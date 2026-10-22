"""True orthophoto from point colour, and per-crown image chips.

The colour of the highest coloured point in each cell is used, so the image is
aligned with the LiDAR canopy by construction (no relief displacement). Cells
with no coloured point are left black. Colour is the LiDAR unit's camera RGB
as stored in the point cloud: uncalibrated, compare within one flight.
"""

from __future__ import annotations

import csv
from pathlib import Path

import numpy as np
import rasterio
from rasterio.windows import from_bounds

from . import io
from .grid import Grid


def build(path, out_tif, cell: float = 0.1, bbox=None) -> dict:
    with io.open_las(path) as reader:
        h = reader.header
        crs = h.parse_crs()
        bounds = bbox or (h.mins[0], h.mins[1], h.maxs[0], h.maxs[1])
        names = set(reader.header.point_format.dimension_names)
    if not {"red", "green", "blue"} <= names:
        raise ValueError("point cloud has no RGB colour")
    grid = Grid.from_bounds(*bounds, cell)
    size = grid.rows * grid.cols
    zmax = np.full(size, -np.inf, dtype=np.float64)
    rgb = np.zeros((size, 3), dtype=np.uint16)
    for chunk in io._bbox_records(path, bbox):
        x, y, z = np.asarray(chunk.x), np.asarray(chunk.y), np.asarray(chunk.z)
        r, g, b = (np.asarray(chunk[c]) for c in ("red", "green", "blue"))
        idx = grid.flat_index(x, y)
        ok = (idx >= 0) & ((r.astype(np.uint32) + g + b) > 0)
        if bbox is not None:  # spatial queries and full scans can both return extra points
            ok &= (x >= bbox[0]) & (x <= bbox[2]) & (y >= bbox[1]) & (y <= bbox[3])
        if not ok.any():
            continue
        idx, z, r, g, b = idx[ok], z[ok], r[ok], g[ok], b[ok]
        order = np.lexsort((z, idx))  # highest point per cell is last in its run
        last = np.r_[idx[order][1:] != idx[order][:-1], True]
        sel = order[last]
        better = z[sel] > zmax[idx[sel]]
        sel = sel[better]
        zmax[idx[sel]] = z[sel]
        rgb[idx[sel]] = np.c_[r[sel], g[sel], b[sel]]
    filled = np.isfinite(zmax)
    # 16-bit camera colour scaled to 8 bits with a robust white point.
    white = np.percentile(rgb[filled], 99.5, axis=0) if filled.any() else np.ones(3)
    img8 = np.clip(rgb / np.maximum(white, 1) * 255, 0, 255).astype(np.uint8)
    img8[~filled] = 0
    img8 = img8.reshape(grid.rows, grid.cols, 3)
    with rasterio.open(out_tif, "w", driver="COG", height=grid.rows, width=grid.cols, count=3,
                       dtype="uint8", crs=crs.to_wkt() if crs else None, transform=grid.transform,
                       compress="deflate") as dst:
        for band in range(3):
            dst.write(img8[:, :, band], band + 1)
    return {"cell_m": cell, "rows": grid.rows, "cols": grid.cols,
            "filled_fraction": round(float(filled.mean()), 4)}


def crown_chips(run_dir, ortho_tif, out_dir, tree_ids=None, margin_m: float = 1.0,
                min_px: int = 320, max_px: int = 1024) -> list[dict]:
    """PNG chip per crown: crown bounding box plus margin, upscaled to at least
    min_px (TypeSafe images need 280 px or more per side), crown outline drawn."""
    from PIL import Image, ImageDraw
    from skimage.segmentation import find_boundaries
    run_dir, out = Path(run_dir), Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    with open(run_dir / "trees.csv", newline="") as fh:
        trees = {r["tree_id"]: r for r in csv.DictReader(fh)}
    wanted = set(map(str, tree_ids)) if tree_ids is not None else set(trees)
    with rasterio.open(run_dir / "crowns.tif") as cds, rasterio.open(ortho_tif) as ods:
        labels = cds.read(1)
        ctf = cds.transform
        written = []
        ids = np.unique(labels)
        from scipy import ndimage
        slices = ndimage.find_objects(labels)
        for tid in ids[ids > 0]:
            if str(tid) not in wanted or slices[tid - 1] is None:
                continue
            rs, cs = slices[tid - 1]
            minx = ctf.c + cs.start * ctf.a - margin_m
            maxx = ctf.c + cs.stop * ctf.a + margin_m
            maxy = ctf.f + rs.start * ctf.e + margin_m
            miny = ctf.f + rs.stop * ctf.e - margin_m
            win = from_bounds(minx, miny, maxx, maxy, ods.transform)
            img = np.moveaxis(ods.read(window=win, boundless=True, fill_value=0), 0, -1)
            if img.size == 0:
                continue
            # Crown outline resampled to the ortho window.
            ly = np.linspace(miny, maxy, img.shape[0])[::-1]
            lx = np.linspace(minx, maxx, img.shape[1])
            rr = np.clip(((ctf.f - ly) / -ctf.e).astype(int), 0, labels.shape[0] - 1)
            cc = np.clip(((lx - ctf.c) / ctf.a).astype(int), 0, labels.shape[1] - 1)
            mask = labels[np.ix_(rr, cc)] == tid
            # Pad very elongated chips (e.g. windbreak rows) to at most 3:1 so
            # shrinking to max_px never pushes the short side below 280 px.
            short = int(np.ceil(max(img.shape[:2]) / 3))
            pad_r, pad_c = max(0, short - img.shape[0]), max(0, short - img.shape[1])
            if pad_r or pad_c:
                pads = ((pad_r // 2, pad_r - pad_r // 2), (pad_c // 2, pad_c - pad_c // 2))
                img = np.pad(img, pads + ((0, 0),))
                mask = np.pad(mask, pads)
            scale = max(1, int(np.ceil(min_px / min(img.shape[:2]))))
            pil = Image.fromarray(img).resize((img.shape[1] * scale, img.shape[0] * scale),
                                              Image.NEAREST)
            edge = np.kron(find_boundaries(mask, mode="inner"), np.ones((scale, scale), bool))
            arr = np.array(pil)
            arr[edge] = (255, 255, 0)
            pil = Image.fromarray(arr)
            if max(pil.size) > max_px:
                pil.thumbnail((max_px, max_px))
            if min(pil.size) < 280:
                continue
            path = out / f"tree_{tid}.png"
            pil.save(path)
            written.append({"tree_id": str(tid), "path": str(path), "width": pil.size[0],
                            "height": pil.size[1]})
    return written
