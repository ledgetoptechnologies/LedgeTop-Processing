"""Field pack: small, ready-to-show files for labelling trees on a phone.

A field pack is one folder per run that a web page can show without any
GIS or LiDAR software:

- `trees.geojson`: crown outlines in WGS84 longitude/latitude, one feature
  per tree, keyed by lasting ID (`treedar ids` must have run first), with the
  treetop, size, area and any prediction as properties.
- `points/<stable_id>.bin`: the tree's points (everything inside its crown
  footprint, ground and stem included), thinned to at most `max_points`, as
  little-endian records of float32 x, y, z and uint8 r, g, b, pad (16 bytes),
  colour brightened with a display gamma (pack.json `colour_gamma`).
  x and y are metres east and north of the treetop; z is metres above the
  ground model at the treetop. A browser reads it straight into typed arrays.
- `photos/<stable_id>/`: original-photo crops (from `treedar raw-chips`) and
  the orthophoto crop, resized for a phone.
- `ortho/overview.jpg`: the orthophoto warped to Web Mercator, with its
  latitude/longitude bounds in `pack.json`, for a simple map image overlay.
- `pack.json`: what the pack holds, the point format, and the size and
  SHA-256 of every file.

The pack contains property imagery and coordinates: keep it out of Git.
"""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import os
import shutil
from pathlib import Path

import numpy as np
import rasterio
from PIL import Image
from pyproj import Transformer
from rasterio import features as rfeatures

from . import __version__, io
from .grid import Grid

SCHEMA = "treedar.fieldpack.v1"
MAX_POINTS = 150_000
PHOTO_PX = 1600
OVERVIEW_PX = 4096
COLOUR_GAMMA = 2.2  # LAS colour is linear and often dark; brighten for display
POINT_DTYPE = np.dtype([("x", "<f4"), ("y", "<f4"), ("z", "<f4"),
                        ("r", "u1"), ("g", "u1"), ("b", "u1"), ("pad", "u1")])


def _read_csv(path) -> list[dict]:
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def _round(values, places=7):
    return [round(float(v), places) for v in values]


def _ring_lonlat(ring, to_ll):
    xs, ys = zip(*ring)
    lon, lat = to_ll.transform(xs, ys)
    return [_round(p) for p in zip(lon, lat)]


def _crowns_lonlat(labels, transform, to_ll) -> dict[int, dict]:
    """One Polygon or MultiPolygon per crown label, in longitude/latitude."""
    pieces: dict[int, list] = {}
    for geom, value in rfeatures.shapes(labels, mask=labels > 0, transform=transform):
        rings = [_ring_lonlat(ring, to_ll) for ring in geom["coordinates"]]
        pieces.setdefault(int(value), []).append(rings)
    return {tid: ({"type": "Polygon", "coordinates": polys[0]} if len(polys) == 1
                  else {"type": "MultiPolygon", "coordinates": polys})
            for tid, polys in pieces.items()}


def _expected_counts(run_dir: Path) -> dict[int, int]:
    path = run_dir / "tree_features.csv"
    if not path.exists():
        return {}
    return {int(r["tree_id"]): int(float(r["n_points"])) for r in _read_csv(path)
            if r.get("n_points")}


def write_points(laz, labels, transform, origins: dict[int, tuple], out_dir: Path,
                 names: dict[int, str], max_points=MAX_POINTS, expected=None, seed=0,
                 gamma=COLOUR_GAMMA) -> dict:
    """Write points/<name>.bin per tree in one pass over the point cloud.

    origins maps crown label -> (x, y, ground z) of the local frame. Points are
    first kept at random with probability max_points / expected count (so the
    pass needs no counting pre-pass), then capped exactly at max_points.
    Colour is scaled to 8 bits with a display gamma (1 keeps it linear).
    Returns {"counts": {label: points written}, "has_colour": bool}.
    """
    if max_points < 1:
        raise ValueError("max_points must be positive")
    out_dir.mkdir(parents=True, exist_ok=True)
    grid = Grid.from_transform(transform, labels.shape)
    rng = np.random.default_rng(seed)
    expected = expected or {}
    size = int(labels.max()) + 1
    keep_p = np.ones(size)
    for tid, n in expected.items():
        if 0 < tid < size and n > max_points:
            # A margin over the cap so the exact cap below, not chance, decides.
            keep_p[tid] = min(1.0, 1.3 * max_points / n)
    origin = np.zeros((size, 3))
    wanted = np.zeros(size, dtype=bool)
    for tid, xyz in origins.items():
        if 0 < tid < size:
            origin[tid], wanted[tid] = xyz, True
    colour_scale = None
    has_colour = False
    for chunk in io.iter_chunks(laz):
        x, y = np.asarray(chunk.x), np.asarray(chunk.y)
        row, col = grid.rowcol(x, y)
        inside = (row >= 0) & (row < grid.rows) & (col >= 0) & (col < grid.cols)
        lab = np.zeros(x.shape, dtype=np.int64)
        lab[inside] = labels[row[inside], col[inside]]
        sel = np.flatnonzero(wanted[lab] & (rng.random(lab.size) < keep_p[lab]))
        if sel.size == 0:
            continue
        sel = sel[np.argsort(lab[sel], kind="stable")]
        lab_s = lab[sel]
        rec = np.zeros(sel.size, dtype=POINT_DTYPE)
        rec["x"] = x[sel] - origin[lab_s, 0]
        rec["y"] = y[sel] - origin[lab_s, 1]
        rec["z"] = np.asarray(chunk.z)[sel] - origin[lab_s, 2]
        dims = set(chunk.point_format.dimension_names)
        if {"red", "green", "blue"} <= dims:
            rgb = [np.asarray(chunk[c])[sel] for c in ("red", "green", "blue")]
            if colour_scale is None:
                # LAS colour is 16-bit, but some writers store 8-bit values unscaled.
                colour_scale = 65535.0 if max(int(c.max()) for c in rgb) > 255 else 255.0
            for name, c in zip(("r", "g", "b"), rgb):
                level = np.clip(c / colour_scale, 0, 1) ** (1 / gamma)
                rec[name] = np.rint(255 * level)
            has_colour = True
        starts = np.flatnonzero(np.r_[True, lab_s[1:] != lab_s[:-1]])
        ends = np.r_[starts[1:], lab_s.size]
        for s, e in zip(starts, ends):
            with open(out_dir / f"{names[int(lab_s[s])]}.part", "ab") as fh:
                fh.write(rec[s:e].tobytes())
    counts = {}
    for tid, name in names.items():
        part = out_dir / f"{name}.part"
        rec = np.fromfile(part, dtype=POINT_DTYPE) if part.exists() else np.zeros(0, POINT_DTYPE)
        if rec.size > max_points:
            rec = rec[np.sort(rng.choice(rec.size, max_points, replace=False))]
        rec.tofile(out_dir / f"{name}.bin")
        if part.exists():
            part.unlink()
        counts[tid] = int(rec.size)
    return {"counts": counts, "has_colour": has_colour}


def _save_photo(src: Path, dst: Path, max_px: int):
    with Image.open(src) as im:
        im = im.convert("RGB")
        im.thumbnail((max_px, max_px), Image.LANCZOS)
        dst.parent.mkdir(parents=True, exist_ok=True)
        im.save(dst, "JPEG", quality=82, optimize=True)


def write_photos(out_dir: Path, run_ids: dict[int, str], raw_dir=None, chips_dir=None,
                 max_px=PHOTO_PX) -> dict[int, list]:
    """Copy each tree's raw-photo views and ortho chip, resized, into photos/<name>/."""
    views = {}
    if raw_dir is not None:
        raw_dir = Path(raw_dir)
        views = json.loads((raw_dir / "views.json").read_text(encoding="utf-8"))
    photos: dict[int, list] = {}
    for tid, name in run_ids.items():
        items = []
        for v in views.get(str(tid), []):
            src = raw_dir / v.get("filename", f"tree_{tid}_v{v['view']}.jpg")
            if not src.exists():
                continue
            rel = f"photos/{name}/v{v['view']}.jpg"
            _save_photo(src, out_dir / rel, max_px)
            items.append({"file": rel, "kind": "raw", "off_nadir_deg": v.get("off_nadir_deg"),
                          "azimuth_deg": v.get("azimuth_deg")})
        if chips_dir is not None:
            src = next((p for p in (Path(chips_dir) / f"tree_{tid}.png",
                                    Path(chips_dir) / f"tree_{tid}.jpg") if p.exists()), None)
            if src is not None:
                rel = f"photos/{name}/ortho.jpg"
                _save_photo(src, out_dir / rel, max_px)
                items.append({"file": rel, "kind": "ortho"})
        photos[tid] = items
    return photos


def write_overview(ortho, out_path: Path, max_px=OVERVIEW_PX) -> dict:
    """Orthophoto warped to Web Mercator (EPSG:3857) as one JPEG, with bounds."""
    from rasterio.vrt import WarpedVRT
    from rasterio.enums import Resampling
    with rasterio.open(ortho) as src, WarpedVRT(src, crs="EPSG:3857",
                                                 resampling=Resampling.bilinear) as vrt:
        scale = max(vrt.width, vrt.height) / max_px
        shape = (3, max(1, round(vrt.height / max(scale, 1))), max(1, round(vrt.width / max(scale, 1))))
        rgb = vrt.read((1, 2, 3), out_shape=shape, resampling=Resampling.average)
        left, bottom, right, top_ = vrt.bounds
    if rgb.dtype != np.uint8:
        rgb = np.clip(rgb / (257.0 if rgb.max() > 255 else 1.0), 0, 255).astype(np.uint8)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(np.moveaxis(rgb, 0, -1)).save(out_path, "JPEG", quality=85, optimize=True)
    to_ll = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    west, south = to_ll.transform(left, bottom)
    east, north = to_ll.transform(right, top_)
    return {"file": "ortho/overview.jpg", "width": shape[2], "height": shape[1],
            "bounds": [_round([south, west]), _round([north, east])]}


def _manifest(out_dir: Path) -> dict:
    files = {}
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file()):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        files[path.relative_to(out_dir).as_posix()] = {"bytes": path.stat().st_size,
                                                       "sha256": digest}
    return files


def build(run_dir, out_dir, laz, raw_photos=None, ortho_chips=None, ortho=None,
          predictions=None, max_points=MAX_POINTS, photo_px=PHOTO_PX,
          overview_px=OVERVIEW_PX, seed=0, gamma=COLOUR_GAMMA) -> dict:
    """Write a field pack for one run folder. Returns pack.json's content.

    The pack is built in `<out_dir>.partial` and renamed when complete, so a
    failed build never leaves a half-written pack under the final name.
    """
    run_dir, out_dir = Path(run_dir), Path(out_dir)
    if out_dir.exists():
        raise ValueError(f"field pack already exists: {out_dir}")
    if not (run_dir / "tree_ids.csv").exists():
        raise ValueError("run has no tree_ids.csv: assign lasting IDs with `treedar ids` first")
    work = out_dir.with_name(out_dir.name + ".partial")
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)

    trees = {int(r["tree_id"]): r for r in _read_csv(run_dir / "trees.csv")}
    ids = {int(r["tree_id"]): r for r in _read_csv(run_dir / "tree_ids.csv")}
    if set(ids) != set(trees):
        raise ValueError("tree_ids.csv does not match trees.csv; rerun `treedar ids`")
    areas = {}
    if (run_dir / "tree_areas.csv").exists():
        areas = {int(r["tree_id"]): r["area"] for r in _read_csv(run_dir / "tree_areas.csv")}
    preds = {int(r["tree_id"]): r for r in _read_csv(predictions)} if predictions else {}
    with rasterio.open(run_dir / "crowns.tif") as ds:
        labels = ds.read(1, masked=True).filled(0)
        transform, crs = ds.transform, ds.crs
    with rasterio.open(run_dir / "dtm.tif") as ds:
        if ds.transform != transform or ds.shape != labels.shape:
            raise ValueError("dtm.tif and crowns.tif grids differ")
        dtm = ds.read(1, masked=True).filled(np.nan)
    if crs is None:
        raise ValueError("crowns.tif has no CRS")
    to_ll = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
    grid = Grid.from_transform(transform, labels.shape)

    names = {tid: str(int(ids[tid]["stable_id"])) for tid in trees}
    origins = {}
    for tid, t in trees.items():
        x, y = float(t["x"]), float(t["y"])
        r, c = grid.rowcol(np.array([x]), np.array([y]))
        ground = float(dtm[r[0], c[0]]) if (0 <= r[0] < grid.rows and 0 <= c[0] < grid.cols) else np.nan
        if not np.isfinite(ground):
            ground = float(np.nanmin(dtm))
        origins[tid] = (x, y, ground)

    pts = write_points(laz, labels, transform, origins, work / "points", names,
                       max_points=max_points, expected=_expected_counts(run_dir), seed=seed,
                       gamma=gamma)
    photos = write_photos(work, names, raw_photos, ortho_chips, photo_px)
    overview = write_overview(ortho, work / "ortho" / "overview.jpg", overview_px) if ortho else None

    crowns = _crowns_lonlat(labels, transform, to_ll)
    feats = []
    for tid in sorted(trees, key=lambda t: int(names[t])):
        t, i = trees[tid], ids[tid]
        lon, lat = to_ll.transform(float(t["x"]), float(t["y"]))
        props = {"stable_id": int(names[tid]), "tree_id": tid,
                 "top": _round([lon, lat]), "ground_z": round(origins[tid][2], 2),
                 "height_m": float(t["height_m"]), "crown_area_m2": float(t["crown_area_m2"]),
                 "crown_diameter_m": float(t["crown_diameter_m"]),
                 "area": areas.get(tid, ""), "review": i.get("review", ""),
                 "points": f"points/{names[tid]}.bin", "n_points": pts["counts"][tid],
                 "photos": photos.get(tid, [])}
        p = preds.get(tid)
        if p:
            props.update({"predicted": p["predicted"], "confidence": float(p["confidence"])})
        feats.append({"type": "Feature", "geometry": crowns.get(tid), "properties": props})
    (work / "trees.geojson").write_text(json.dumps({"type": "FeatureCollection", "features": feats}))

    pack = {
        "schema": SCHEMA, "treedar_version": __version__,
        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "run": run_dir.resolve().name, "source_crs": crs.to_string(),
        "trees": len(feats), "id_key": "stable_id",
        "point_format": {"record_bytes": POINT_DTYPE.itemsize, "byte_order": "little",
                         "fields": ["x:float32", "y:float32", "z:float32", "r:uint8",
                                    "g:uint8", "b:uint8", "pad:uint8"],
                         "frame": "metres; x east and y north of the treetop, "
                                  "z above the ground model at the treetop",
                         "max_points": max_points, "has_colour": pts["has_colour"],
                         "colour_gamma": gamma},
        "photos": {"max_px": photo_px, "trees_with_raw": sum(
            any(p["kind"] == "raw" for p in v) for v in photos.values())},
        "ortho": overview,
        "predictions": bool(preds),
    }
    pack["files"] = _manifest(work)
    (work / "pack.json").write_text(json.dumps(pack, indent=2))
    os.replace(work, out_dir)
    return pack
