"""Ground (bare earth) model.

Uses ground-classified points (class 2) when the file has them, otherwise a
progressive morphological filter (Zhang et al. 2003) on a lowest-point grid.
"""

from __future__ import annotations

import numpy as np
from scipy import ndimage

from .grid import Grid, aggregate, fill_nearest

GROUND_CLASS = 2


def _remove_low_outliers(zmin: np.ndarray, threshold: float = 1.0) -> np.ndarray:
    """Drop isolated cells far below their neighbourhood (multipath, noise)."""
    filled = fill_nearest(zmin)
    local = ndimage.median_filter(filled, size=5)
    out = zmin.copy()
    out[filled < local - threshold] = np.nan
    return out


def progressive_morphological_ground(zmin: np.ndarray, cell: float,
                                     max_window_m: float = 40.0, slope: float = 0.3,
                                     dh0: float = 0.3, dh_max: float = 3.0) -> np.ndarray:
    """Return a boolean mask of ground cells on a lowest-point grid."""
    surface = fill_nearest(zmin)
    ground = ~np.isnan(zmin)
    windows, w = [], 1
    while w * cell <= max_window_m:
        windows.append(w)
        w = 2 * w + 1
    prev_w = 1
    for w in windows[1:]:
        opened = ndimage.grey_opening(surface, size=(w, w))
        dh = min(dh0 + slope * (w - prev_w) * cell, dh_max)
        ground &= (surface - opened) <= dh
        surface = opened
        prev_w = w
    return ground


def _interpolate(values: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Fill cells outside mask from mask cells by normalized Gaussian smoothing,
    then nearest neighbour for anything still empty."""
    known = np.where(mask, values, 0.0)
    weight = mask.astype(np.float64)
    out = np.where(mask, values, np.nan)
    for sigma in (1, 2, 4, 8, 16, 32):
        if not np.isnan(out).any():
            break
        num = ndimage.gaussian_filter(known, sigma)
        den = ndimage.gaussian_filter(weight, sigma)
        fill = np.isnan(out) & (den > 0.05)
        out[fill] = num[fill] / den[fill]
    return fill_nearest(out)


def ground_dtm(x, y, z, cell: float = 0.5, classification=None):
    """Bare-earth DTM on a regular grid. Returns (dtm, transform)."""
    grid = Grid.from_points(x, y, cell)
    use_class = classification is not None and np.any(classification == GROUND_CLASS)
    if use_class:
        g = classification == GROUND_CLASS
        zmin = aggregate(grid, x[g], y[g], z[g], "min")
        mask = ~np.isnan(zmin)
    else:
        # Filter on a 1 m lowest-point grid: more cells contain a ground hit
        # under canopy than at finer resolution.
        coarse_cell = max(cell, 1.0)
        coarse = Grid.from_bounds(grid.minx, grid.maxy - grid.rows * cell,
                                  grid.minx + grid.cols * cell, grid.maxy, coarse_cell)
        zmin_c = _remove_low_outliers(aggregate(coarse, x, y, z, "min"))
        ground_c = progressive_morphological_ground(zmin_c, coarse_cell)
        dtm_c = _interpolate(zmin_c, ground_c)
        if coarse_cell == cell:
            return dtm_c.astype(np.float32), coarse.transform
        # Resample coarse DTM to the requested grid by bilinear interpolation.
        rows, cols = np.mgrid[0:grid.rows, 0:grid.cols]
        cx, cy = grid.cell_centers(rows, cols)
        fr = (coarse.maxy - cy) / coarse_cell - 0.5
        fc = (cx - coarse.minx) / coarse_cell - 0.5
        dtm = ndimage.map_coordinates(dtm_c, [fr, fc], order=1, mode="nearest")
        return dtm.astype(np.float32), grid.transform
    return _interpolate(zmin, mask).astype(np.float32), grid.transform
