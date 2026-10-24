"""Command line entry point for the TreeDar pipeline."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

import numpy as np
import rasterio

from . import (__version__, areas, benchmark, chm, classify, features, io, jev, labeltool,
               ortho, qc, runinfo, segment, terrain, treeids, treemap)

TREE_COLUMNS = ["tree_id", "x", "y", "height_m", "crown_area_m2", "crown_diameter_m"]


def _write_raster(path: Path, array: np.ndarray, transform, crs, nodata=None):
    # Cloud-optimized GeoTIFF: tiled with overviews, streamable by web viewers.
    with rasterio.open(path, "w", driver="COG", height=array.shape[0],
                       width=array.shape[1], count=1, dtype=array.dtype,
                       crs=crs.to_wkt() if crs else None, transform=transform,
                       nodata=nodata, compress="deflate") as dst:
        dst.write(array, 1)


def _parse_bbox(text: str | None):
    if text is None:
        return None
    parts = [float(v) for v in text.split(",")]
    if len(parts) != 4 or parts[0] >= parts[2] or parts[1] >= parts[3]:
        raise ValueError("--bbox must be minx,miny,maxx,maxy with min < max")
    return tuple(parts)


def cmd_qc(args) -> int:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    report = qc.qc_report(args.laz)
    (out / "qc.json").write_text(json.dumps(report, indent=2))
    d = report["density"]
    print(f"{report['point_count']:,} points, EPSG:{report['crs_epsg']}, "
          f"median density {d['median']:.0f} pts/m2")
    return 0


def cmd_run(args) -> int:
    process(args)
    return 0


def process(args) -> dict:
    """LAZ to rasters, trees.csv, tree_features.csv and summary.json in args.out."""
    start = time.perf_counter()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    bbox = _parse_bbox(args.bbox)

    if not args.no_qc:
        report = qc.qc_report(args.laz)
        (out / "qc.json").write_text(json.dumps(report, indent=2))
    crs = io.read_crs(args.laz)

    pts = io.read_points(args.laz, bbox=bbox, step=args.step)
    dtm, transform = terrain.ground_dtm(pts["x"], pts["y"], pts["z"], cell=args.cell,
                                        classification=pts.get("classification"))
    raw = chm.max_height_grid(pts["x"], pts["y"], pts["z"], dtm, transform, cell=args.cell)
    canopy, chm_stats = chm.filter_pits_and_spikes(raw, spike_threshold=args.spike_threshold)
    preset = segment.WINDOW_PRESETS[args.preset]
    window = {k: (getattr(args, k) if getattr(args, k) is not None else v) for k, v in preset.items()}
    tops = segment.detect_treetops(canopy, transform, min_height=args.min_height, **window)
    labels = segment.segment_crowns(canopy, tops, transform)
    trees = segment.tree_table(canopy, labels, transform)
    tree_feats = features.tree_features(pts, dtm, labels, transform, chm=canopy)

    _write_raster(out / "dtm.tif", dtm, transform, crs)
    _write_raster(out / "chm.tif", canopy, transform, crs)
    _write_raster(out / "chm_raw.tif", raw, transform, crs)
    _write_raster(out / "crowns.tif", labels, transform, crs, nodata=0)
    with open(out / "trees.csv", "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=TREE_COLUMNS)
        writer.writeheader()
        writer.writerows(trees)

    with open(out / "tree_features.csv", "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=features.FEATURE_COLUMNS)
        writer.writeheader()
        writer.writerows(tree_feats)

    summary = {
        "n_trees": len(trees),
        "n_points_used": int(pts["x"].size),
        "runtime_s": round(time.perf_counter() - start, 1),
        "params": {"cell": args.cell, "min_height": args.min_height,
                   "bbox": bbox, "step": args.step, "preset": args.preset, **window},
        "chm_filter": chm_stats,
        "version": __version__,
    }
    if getattr(args, "ids_registry", None):
        summary["tree_ids"] = treeids.update_run(out, args.ids_registry)
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(f"{len(trees)} trees from {summary['n_points_used']:,} points "
          f"in {summary['runtime_s']} s -> {out}")
    return summary


def cmd_areas(args) -> int:
    run_dir = Path(args.run_dir)
    with rasterio.open(run_dir / "chm.tif") as ds:
        canopy, transform, crs = ds.read(1), ds.transform, ds.crs
    area_map, names = areas.delineate(canopy, transform, min_height=args.min_height)
    trees = classify._read_csv(run_dir / "trees.csv")
    rows = areas.tree_areas(trees, area_map, names, transform)
    _write_raster(run_dir / "areas.tif", area_map, transform, crs, nodata=0)
    areas.write_csv(run_dir / "tree_areas.csv", rows, ["tree_id", "area"])
    counts = {n: 0 for n in names.values()}
    for r in rows:
        if r["area"]:
            counts[r["area"]] += 1
    template = run_dir / "area_types.csv"
    if not template.exists():  # never overwrite names the owner already filled in
        areas.write_csv(template, [{"area": n, "label": "", "n_trees": c} for n, c in counts.items()],
                        ["area", "label", "n_trees"])
    areas.render(canopy, area_map, names, run_dir / "areas.png",
                 title="Tree areas: name what grows in each (skip if unsure)")
    print(f"{len(names)} areas, {sum(counts.values())} trees assigned -> {run_dir / 'areas.png'}; "
          f"fill in {template}")
    return 0


def make_labels(run_dir: Path, area_types_csv) -> Path:
    types = areas.read_area_types(area_types_csv)
    rows = areas.labels_from_areas(classify._read_csv(run_dir / "tree_areas.csv"), types)
    path = run_dir / "labels.csv"
    areas.write_csv(path, rows, ["tree_id", "label", "group"])
    print(f"{len(rows)} labelled trees from {len(types)} named areas -> {path}")
    return path


def cmd_ids(args) -> int:
    run_dir = Path(args.run_dir)
    counts = treeids.update_run(run_dir, args.registry, args.max_dist, args.max_dz)
    print(f"{counts['trees']} trees: {counts['seed']} seeded, {counts['matched']} matched, "
          f"{counts['new']} new, {counts['missing']} registered trees missing, "
          f"{counts['review']} to review -> {run_dir / 'tree_ids.csv'}")
    if args.labels:
        rows, skipped = treeids.labels_for_run(treeids.read_tree_ids(run_dir / "tree_ids.csv"),
                                               classify._read_csv(args.labels))
        areas.write_csv(run_dir / "labels.csv", rows, ["tree_id", "label", "group"])
        print(f"{len(rows)} labels -> {run_dir / 'labels.csv'}; {len(skipped)} skipped")
        for reason in skipped:
            print(f"  skipped stable_id {reason}")
    return 0


def cmd_labels(args) -> int:
    make_labels(Path(args.run_dir), args.area_types)
    return 0


def make_map(run_dir: Path, predictions_csv=None):
    n = treemap.write_geojson(run_dir, run_dir / "tree_map.geojson", predictions_csv)
    treemap.render_preview(run_dir, run_dir / "tree_map.png", predictions_csv)
    print(f"{n} crowns -> {run_dir / 'tree_map.geojson'}, {run_dir / 'tree_map.png'}")


def cmd_map(args) -> int:
    make_map(Path(args.run_dir), args.predictions)
    return 0


def cmd_ortho(args) -> int:
    info = ortho.build(args.laz, args.out, cell=args.cell, bbox=_parse_bbox(args.bbox))
    print(f"{info['cols']} x {info['rows']} px at {info['cell_m']} m, "
          f"{info['filled_fraction']:.0%} of cells coloured -> {args.out}")
    return 0


def cmd_chips(args) -> int:
    written = ortho.crown_chips(args.run_dir, args.ortho, args.out)
    print(f"{len(written)} crown chips -> {args.out}")
    return 0


def cmd_raw_chips(args) -> int:
    from .raw_chips import generate_raw_chips
    tree_ids = None
    if args.tree_ids:
        tree_ids = json.loads(Path(args.tree_ids).read_text(encoding="utf-8"))
        if not isinstance(tree_ids, list):
            raise ValueError("tree IDs JSON must contain a list")
    result = generate_raw_chips(args.opensfm, args.images, args.run_dir, args.out,
                                tree_ids=tree_ids, views=args.views,
                                search_radius_m=args.search_radius,
                                edge_margin=args.edge_margin)
    print(f"{len(result)} trees, {sum(len(v) for v in result.values())} views -> {args.out}")
    return 0


def cmd_photo_ortho(args) -> int:
    from .photo_ortho import render_photo_ortho
    result = render_photo_ortho(args.opensfm, args.images, args.run_dir, args.out,
                                cell=args.cell, bbox=_parse_bbox(args.bbox),
                                candidates=args.candidates, edge_margin=args.edge_margin)
    print(json.dumps(result))
    return 0


def cmd_labelpage(args) -> int:
    n = labeltool.build(args.run_dir, args.chips, args.out, per_area=args.per_area, seed=args.seed)
    print(f"{n} crowns on the labelling page -> {args.out} (contains property imagery: keep local)")
    return 0


def cmd_benchmark(args) -> int:
    r = benchmark.evaluate_run(args.reference, args.run_dir, max_dist_m=args.max_dist,
                               max_dz_m=args.max_dz, min_height=args.min_height)
    fmt = lambda v: "n/a" if v is None else f"{v:.1%}"  # noqa: E731
    print(f"{r['reference']}: {r['n_reference']} reference trees >= {r['min_height_m']} m, "
          f"{r['n_detected']} detected; recall {fmt(r['recall'])}, precision {fmt(r['precision'])}, "
          f"F1 {fmt(r['f1'])} -> {Path(args.run_dir) / 'benchmark.json'}")
    return 0


def cmd_pipeline(args) -> int:
    """One command: LAZ -> trees, areas, labels and classifier when named, tree map."""
    start = time.perf_counter()
    process(args)
    run_dir = Path(args.out)
    cmd_areas(argparse.Namespace(run_dir=run_dir, min_height=3.0))
    predictions = None
    if args.area_types:
        labels_csv = make_labels(run_dir, args.area_types)
        cmd_classify(argparse.Namespace(
            trees=run_dir / "trees.csv", features=run_dir / "tree_features.csv",
            labels=labels_csv, out=run_dir / "classify", block_m=args.block_m,
            min_accuracy=args.min_accuracy, min_coverage=args.min_coverage))
        predictions = run_dir / "classify" / "predictions.csv"
    make_map(run_dir, predictions)
    params = {k: v for k, v in vars(args).items() if k not in ("func", "laz", "out", "ids_registry")}
    runinfo.write_manifest(run_dir, input_path=args.laz, params=params, command="pipeline")
    report = runinfo.write_report(run_dir)
    print(f"pipeline finished in {time.perf_counter() - start:.0f} s; report: {report}")
    return 0


def cmd_report(args) -> int:
    run_dir = Path(args.run_dir)
    if not (run_dir / "manifest.json").exists():
        runinfo.write_manifest(run_dir, command="report")
    print(f"report -> {runinfo.write_report(run_dir)}")
    return 0


def cmd_classify(args) -> int:
    report = classify.run(args.trees, args.features, args.labels, args.out, block_m=args.block_m,
                          min_accuracy=args.min_accuracy, min_coverage=args.min_coverage)
    cv = report["cross_validation"]
    chosen = cv["chosen"]
    msg = (f"{report['labelled_blocks']} label groups ({report['grouping']}): "
           f"{report['development_groups']} development, {report['final_test_groups']} final test. "
           f"Development CV on {cv['n_labelled']} trees: "
           + ("n/a" if cv["accuracy_all"] is None else f"{cv['accuracy_all']:.1%}")
           + " with no unknowns")
    if cv["n_unscored"]:
        msg += f" ({cv['n_unscored']} trees in folds that could not be scored are left out)"
    if cv.get("note"):
        msg += f"; {cv['note']}"
    if chosen:
        msg += f"; threshold {chosen['threshold']:.2f} chosen"
    else:
        msg += f"; no threshold reaches {args.min_accuracy:.0%}, all predictions set to unknown"
    final = report.get("final_test")
    if final:
        ft = final["at_threshold"]
        acc = "n/a" if ft["accuracy"] is None else f"{ft['accuracy']:.1%}"
        msg += f". FINAL TEST on {ft['n']} held-out trees: {acc} correct, {ft['coverage']:.1%} labelled"
    else:
        msg += ". No independent final test (too few label groups)"
    print(msg + f" -> {args.out}")
    return 0


def cmd_jev(args) -> int:
    report = jev.run(args.trees, args.features, args.labels, args.out, model=args.model,
                     workers=args.workers, include_unlabelled=args.all_trees,
                     min_accuracy=args.min_accuracy, min_coverage=args.min_coverage,
                     split_csv=args.split_from, chips_dir=args.chips)
    msg = (f"Jev ({report['model']}): {report['trees_sent']} trees, "
           f"{report['input_tokens']:,} input tokens, about ${report['estimated_cost_usd']:.4f}")
    ev = report.get("evaluation")
    if ev:
        msg += f"; accuracy {ev['accuracy_all']:.1%} on {ev['n_labelled']} labelled trees"
        if ev["chosen"]:
            msg += (f"; at confidence >= {ev['chosen']['threshold']:.2f}: "
                    f"{ev['chosen']['accuracy']:.1%} correct, {ev['chosen']['coverage']:.1%} labelled")
    ft = report.get("final_test")
    if ft:
        acc = "n/a" if ft["accuracy"] is None else f"{ft['accuracy']:.1%}"
        msg += (f". FINAL TEST on {ft['n']} held-out trees at development threshold "
                f"{ft['threshold']:.2f}: {acc} correct, {ft['coverage']:.1%} labelled")
    print(msg + f" -> {args.out}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="treedar", description=__doc__)
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_qc = sub.add_parser("qc", help="point cloud quality report")
    p_qc.add_argument("laz")
    p_qc.add_argument("--out", required=True)
    p_qc.set_defaults(func=cmd_qc)

    def add_run_args(p):
        p.add_argument("laz")
        p.add_argument("--out", required=True)
        p.add_argument("--cell", type=float, default=0.5)
        p.add_argument("--min-height", type=float, default=5.0)
        p.add_argument("--bbox", default=None, help="minx,miny,maxx,maxy in file CRS")
        p.add_argument("--step", type=int, default=1, help="keep every Nth point")
        p.add_argument("--no-qc", action="store_true",
                       help="skip the whole-file qc.json report (faster tile runs)")
        p.add_argument("--spike-threshold", type=float, default=5.0,
                       help="lower CHM cells this far above their 5x5 median (0 disables)")
        p.add_argument("--preset", choices=sorted(segment.WINDOW_PRESETS), default="broadleaf",
                       help="treetop search settings: broadleaf (wide crowns, original default) or "
                            "conifer (tuned on FOR-instance conifer plots)")
        p.add_argument("--window-base", dest="window_base_m", type=float, default=None,
                       help="override: treetop window diameter at height 0 (m)")
        p.add_argument("--window-slope", dest="window_slope", type=float, default=None,
                       help="override: window diameter increase per metre of height")
        p.add_argument("--smooth", dest="smooth_sigma_m", type=float, default=None,
                       help="override: CHM smoothing sigma before treetop search (m)")
        p.add_argument("--ids-registry", default=None,
                       help="tree ID registry JSON for this dataset; assigns lasting IDs "
                            "(tree_ids.csv), creating the registry on the first run")

    p_run = sub.add_parser("run", help="DTM, CHM, treetops, crowns and tree features")
    add_run_args(p_run)
    p_run.set_defaults(func=cmd_run)

    p_ids = sub.add_parser("ids", help="lasting tree IDs across reruns of one dataset")
    p_ids.add_argument("run_dir", help="output folder of `treedar run` or `pipeline`")
    p_ids.add_argument("--registry", required=True,
                       help="tree ID registry JSON for this dataset (created on the first run)")
    p_ids.add_argument("--max-dist", type=float, default=treeids.MAX_DIST_M,
                       help="max horizontal treetop distance for the same tree (m); "
                            "used only when the registry is created")
    p_ids.add_argument("--max-dz", type=float, default=treeids.MAX_DZ_M,
                       help="max height difference for the same tree (m); "
                            "used only when the registry is created")
    p_ids.add_argument("--labels", default=None,
                       help="CSV with stable_id,label[,group]; writes this run's labels.csv")
    p_ids.set_defaults(func=cmd_ids)

    p_areas = sub.add_parser("areas", help="named tree areas and a labelling template")
    p_areas.add_argument("run_dir", help="output folder of `treedar run`")
    p_areas.add_argument("--min-height", type=float, default=3.0)
    p_areas.set_defaults(func=cmd_areas)

    p_lab = sub.add_parser("labels", help="tree labels from owner-named areas")
    p_lab.add_argument("run_dir")
    p_lab.add_argument("--area-types", required=True, help="CSV with area,label")
    p_lab.set_defaults(func=cmd_labels)

    p_map = sub.add_parser("map", help="crown polygons (GeoJSON) and preview image")
    p_map.add_argument("run_dir")
    p_map.add_argument("--predictions", default=None, help="predictions.csv from classify")
    p_map.set_defaults(func=cmd_map)

    p_or = sub.add_parser("ortho", help="true orthophoto from point colour (GeoTIFF)")
    p_or.add_argument("laz")
    p_or.add_argument("--out", required=True, help="output .tif")
    p_or.add_argument("--cell", type=float, default=0.1)
    p_or.add_argument("--bbox", default=None)
    p_or.set_defaults(func=cmd_ortho)

    p_ch = sub.add_parser("chips", help="per-crown image chips from an ortho")
    p_ch.add_argument("run_dir")
    p_ch.add_argument("--ortho", required=True)
    p_ch.add_argument("--out", required=True)
    p_ch.set_defaults(func=cmd_chips)

    p_rc = sub.add_parser("raw-chips", help="crown views from aligned original photos")
    p_rc.add_argument("run_dir", help="run with dtm.tif, chm_raw.tif and crowns.tif")
    p_rc.add_argument("--opensfm", required=True, help="folder containing reconstruction.json")
    p_rc.add_argument("--images", required=True, help="folder containing aligned source photos")
    p_rc.add_argument("--out", required=True, help="output JPEG views and views.json folder")
    p_rc.add_argument("--tree-ids", help="JSON list of crown IDs (default: all positive IDs)")
    p_rc.add_argument("--views", type=int, default=3)
    p_rc.add_argument("--search-radius", type=float, default=40, help="camera search radius (m)")
    p_rc.add_argument("--edge-margin", type=int, default=40, help="photo edge exclusion (pixels)")
    p_rc.set_defaults(func=cmd_raw_chips)

    p_po = sub.add_parser("photo-ortho", help="project aligned photos onto the LiDAR surface")
    p_po.add_argument("run_dir", help="run with dtm.tif and chm_raw.tif; ellipsoidal Z required")
    p_po.add_argument("--opensfm", required=True, help="folder containing reconstruction.json")
    p_po.add_argument("--images", required=True)
    p_po.add_argument("--out", required=True, help="output RGB GeoTIFF")
    p_po.add_argument("--cell", type=float, default=0.03, help="output resolution in metres")
    p_po.add_argument("--bbox", help="minx,miny,maxx,maxy in the surface CRS")
    p_po.add_argument("--candidates", type=int, default=6, help="nearest cameras tried per pixel")
    p_po.add_argument("--edge-margin", type=int, default=20, help="photo edge exclusion (pixels)")
    p_po.set_defaults(func=cmd_photo_ortho)

    p_lp = sub.add_parser("labelpage", help="offline click-to-label page for the owner")
    p_lp.add_argument("run_dir")
    p_lp.add_argument("--chips", required=True, help="folder from `treedar chips`")
    p_lp.add_argument("--out", required=True, help="output .html")
    p_lp.add_argument("--per-area", type=int, default=15)
    p_lp.add_argument("--seed", type=int, default=0)
    p_lp.set_defaults(func=cmd_labelpage)

    p_bm = sub.add_parser("benchmark", help="score detections against a hand-segmented reference")
    p_bm.add_argument("reference", help="LAS/LAZ with a per-point treeID field (0 = no tree)")
    p_bm.add_argument("run_dir", help="TreeDar run folder for the same area")
    p_bm.add_argument("--max-dist", type=float, default=1.5, help="max horizontal match distance (m)")
    p_bm.add_argument("--max-dz", type=float, default=3.0, help="max height difference for a match (m)")
    p_bm.add_argument("--min-height", type=float, default=5.0)
    p_bm.set_defaults(func=cmd_benchmark)

    p_rep = sub.add_parser("report", help="(re)build report.html for a run folder")
    p_rep.add_argument("run_dir")
    p_rep.set_defaults(func=cmd_report)

    p_pipe = sub.add_parser("pipeline", help="one command from LAZ to tree map")
    add_run_args(p_pipe)
    p_pipe.add_argument("--area-types", default=None,
                        help="CSV with area,label; if given, train and apply the classifier")
    p_pipe.add_argument("--block-m", type=float, default=50.0)
    p_pipe.add_argument("--min-accuracy", type=float, default=0.85)
    p_pipe.add_argument("--min-coverage", type=float, default=0.70)
    p_pipe.set_defaults(func=cmd_pipeline)
    p_cls = sub.add_parser("classify", help="train and cross-validate a tree type classifier")
    p_cls.add_argument("--trees", required=True, help="trees.csv from run")
    p_cls.add_argument("--features", required=True, help="tree_features.csv from run")
    p_cls.add_argument("--labels", required=True, help="CSV with tree_id,label (trusted labels only)")
    p_cls.add_argument("--out", required=True)
    p_cls.add_argument("--block-m", type=float, default=50.0,
                       help="spatial block size for cross-validation (metres)")
    p_cls.add_argument("--min-accuracy", type=float, default=0.85)
    p_cls.add_argument("--min-coverage", type=float, default=0.70)
    p_cls.set_defaults(func=cmd_classify)

    p_jev = sub.add_parser("jev", help="ask TypeSafe Jev for tree types (sends features, plus crown images with --chips; "
                                       "no coordinates; needs TYPESAFE_API_KEY and owner approval)")
    p_jev.add_argument("--trees", required=True)
    p_jev.add_argument("--features", required=True)
    p_jev.add_argument("--labels", required=True)
    p_jev.add_argument("--out", required=True)
    p_jev.add_argument("--model", default="jev-latest")
    p_jev.add_argument("--workers", type=int, default=4)
    p_jev.add_argument("--all-trees", action="store_true",
                       help="also send unlabelled trees (default: labelled trees only)")
    p_jev.add_argument("--min-accuracy", type=float, default=0.85)
    p_jev.add_argument("--min-coverage", type=float, default=0.70)
    p_jev.add_argument("--chips", default=None,
                       help="folder of crown chips (tree_<id>.png) to send as images (experimental)")
    p_jev.add_argument("--split-from", default=None,
                       help="predictions.csv from classify: reuse its development/final test split")
    p_jev.set_defaults(func=cmd_jev)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        print(f"treedar: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
