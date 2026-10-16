"""Command line entry point: `treedar qc` and `treedar run`."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

import numpy as np
import rasterio

from . import __version__, chm, features, io, qc, segment, terrain

TREE_COLUMNS = ["tree_id", "x", "y", "height_m", "crown_area_m2", "crown_diameter_m"]


def _write_raster(path: Path, array: np.ndarray, transform, crs, nodata=None):
    with rasterio.open(path, "w", driver="GTiff", height=array.shape[0],
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
    tops = segment.detect_treetops(canopy, transform, min_height=args.min_height)
    labels = segment.segment_crowns(canopy, tops, transform)
    trees = segment.tree_table(canopy, labels, transform)
    tree_feats = features.tree_features(pts, dtm, labels, transform)

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
                   "bbox": bbox, "step": args.step},
        "chm_filter": chm_stats,
        "version": __version__,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(f"{len(trees)} trees from {summary['n_points_used']:,} points "
          f"in {summary['runtime_s']} s -> {out}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="treedar", description=__doc__)
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_qc = sub.add_parser("qc", help="point cloud quality report")
    p_qc.add_argument("laz")
    p_qc.add_argument("--out", required=True)
    p_qc.set_defaults(func=cmd_qc)

    p_run = sub.add_parser("run", help="DTM, CHM, treetops and crowns")
    p_run.add_argument("laz")
    p_run.add_argument("--out", required=True)
    p_run.add_argument("--cell", type=float, default=0.5)
    p_run.add_argument("--min-height", type=float, default=5.0)
    p_run.add_argument("--bbox", default=None, help="minx,miny,maxx,maxy in file CRS")
    p_run.add_argument("--step", type=int, default=1, help="keep every Nth point")
    p_run.add_argument("--no-qc", action="store_true",
                       help="skip the whole-file qc.json report (faster tile runs)")
    p_run.add_argument("--spike-threshold", type=float, default=5.0,
                       help="lower CHM cells this far above their 5x5 median (0 disables)")
    p_run.set_defaults(func=cmd_run)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (FileNotFoundError, ValueError) as exc:
        print(f"treedar: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
