"""Individual tree detection and crown segmentation from a CHM.

Baseline method: variable-window local maxima for treetops, then
marker-controlled watershed for crowns. Comparable to lidR's lmf() plus
watershed workflow, written here so the pipeline stays in Python.
"""

from __future__ import annotations

import numpy as np
from scipy import ndimage
from skimage.segmentation import watershed

from .grid import Grid


# Window presets. "broadleaf" is the original default (lidR example form) and
# suits wide deciduous crowns; "conifer" was tuned on FOR-instance conifer dev
# plots (hand-segmented drone LiDAR), where it raised test F1 in every region,
# but it splits large broadleaf crowns on Beau's property.
WINDOW_PRESETS = {
    "broadleaf": {"window_base_m": 3.0, "window_slope": 0.1, "smooth_sigma_m": 0.5},
    "conifer": {"window_base_m": 2.0, "window_slope": 0.0, "smooth_sigma_m": 0.25},
}


def window_diameter_m(height, base_m: float = 3.0, slope: float = 0.1):
    """Search window diameter (m) for a treetop of the given height: base + slope * height."""
    return base_m + slope * np.asarray(height)


def _disk(radius_cells: int) -> np.ndarray:
    r = radius_cells
    yy, xx = np.mgrid[-r:r + 1, -r:r + 1]
    return (xx * xx + yy * yy) <= r * r


def detect_treetops(chm, transform, min_height: float = 5.0, smooth_sigma_m: float = 0.5,
                    window_base_m: float = 3.0, window_slope: float = 0.1) -> np.ndarray:
    """Treetops as an (n, 3) array of [x, y, height]."""
    grid = Grid.from_transform(transform, chm.shape)
    smooth = ndimage.gaussian_filter(chm, smooth_sigma_m / grid.cell) \
        if smooth_sigma_m > 0 else chm
    candidates = (smooth >= min_height) & (smooth == ndimage.maximum_filter(smooth, size=3))
    if not candidates.any():
        return np.empty((0, 3))

    # Keep a candidate only if it is the maximum within its height-dependent window.
    radius_cells = np.maximum(
        1, np.round(window_diameter_m(smooth, window_base_m, window_slope) / 2.0 / grid.cell)).astype(int)
    keep = np.zeros_like(candidates)
    for r in np.unique(radius_cells[candidates]):
        sel = candidates & (radius_cells == r)
        local_max = ndimage.maximum_filter(smooth, footprint=_disk(int(r)))
        keep |= sel & (smooth >= local_max)

    # Flat-topped maxima produce several touching cells: merge them.
    labels, n = ndimage.label(keep, structure=np.ones((3, 3)))
    if n == 0:
        return np.empty((0, 3))
    centers = np.array(ndimage.center_of_mass(keep, labels, range(1, n + 1)))
    rows = np.round(centers[:, 0]).astype(int)
    cols = np.round(centers[:, 1]).astype(int)
    x, y = grid.cell_centers(rows, cols)
    heights = np.asarray(ndimage.maximum(chm, labels, range(1, n + 1)))
    return np.column_stack([x, y, heights])


def segment_crowns(chm, treetops, transform, min_height: float = 2.0,
                   smooth_sigma_m: float = 0.5) -> np.ndarray:
    """Crown label raster (int32, 0 = not a tree) from treetop markers."""
    grid = Grid.from_transform(transform, chm.shape)
    markers = np.zeros(chm.shape, dtype=np.int32)
    if len(treetops):
        rows, cols = grid.rowcol(treetops[:, 0], treetops[:, 1])
        rows = np.clip(rows, 0, grid.rows - 1)
        cols = np.clip(cols, 0, grid.cols - 1)
        markers[rows, cols] = np.arange(1, len(treetops) + 1, dtype=np.int32)
    else:
        return markers
    smooth = ndimage.gaussian_filter(chm, smooth_sigma_m / grid.cell) \
        if smooth_sigma_m > 0 else chm
    labels = watershed(-smooth, markers=markers, mask=chm >= min_height)
    return labels.astype(np.int32)


def tree_table(chm, labels, transform) -> list[dict]:
    """Per-crown attributes: apex position, height, crown area and diameter."""
    grid = Grid.from_transform(transform, chm.shape)
    ids = np.unique(labels)
    ids = ids[ids > 0]
    if ids.size == 0:
        return []
    heights = ndimage.maximum(chm, labels, ids)
    apexes = ndimage.maximum_position(chm, labels, ids)
    areas = ndimage.sum(np.ones_like(chm), labels, ids) * grid.cell ** 2
    rows = []
    for tid, h, (r, c), a in zip(ids, heights, apexes, areas):
        x, y = grid.cell_centers(r, c)
        rows.append({
            "tree_id": int(tid),
            "x": float(x),
            "y": float(y),
            "height_m": round(float(h), 2),
            "crown_area_m2": round(float(a), 2),
            "crown_diameter_m": round(float(2.0 * np.sqrt(a / np.pi)), 2),
        })
    return rows
