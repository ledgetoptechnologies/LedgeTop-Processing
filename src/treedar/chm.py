"""Canopy height model."""

from __future__ import annotations

import numpy as np
from scipy import ndimage

from .grid import Grid, aggregate

MAX_TREE_HEIGHT_M = 60.0  # anything higher above ground is treated as noise


def canopy_height_model(x, y, z, dtm, transform, cell: float = 0.5,
                        pit_threshold: float = 1.0,
                        spike_threshold: float = 5.0) -> np.ndarray:
    """Highest normalized point per cell, with pits and empty cells filled."""
    grid = Grid.from_transform(transform, dtm.shape)
    if not np.isclose(grid.cell, cell):
        raise ValueError(f"cell {cell} does not match DTM grid cell {grid.cell}")
    row, col = grid.rowcol(x, y)
    inside = (row >= 0) & (row < grid.rows) & (col >= 0) & (col < grid.cols)
    hag = np.full(x.shape, np.nan)
    hag[inside] = z[inside] - dtm[row[inside], col[inside]]
    keep = inside & (hag <= MAX_TREE_HEIGHT_M)
    chm = aggregate(grid, x[keep], y[keep], hag[keep], "max")

    # Empty cells take the highest neighbour (3x3), then zero if still empty.
    empty = np.isnan(chm)
    if empty.any():
        neighbour_max = ndimage.maximum_filter(np.where(empty, -np.inf, chm), size=3)
        chm = np.where(empty & np.isfinite(neighbour_max), neighbour_max, chm)
    chm = np.nan_to_num(chm, nan=0.0)

    # Pits: cells much lower than their neighbourhood inside the canopy.
    local = ndimage.median_filter(chm, size=3)
    pits = chm < local - pit_threshold
    chm = np.where(pits, local, chm)

    # Spikes: isolated cells far above their neighbourhood (birds, multipath,
    # airborne noise). Real emergent crowns span many cells, so their 5x5
    # median stays high and they are kept.
    local = ndimage.median_filter(chm, size=5)
    spikes = chm > local + spike_threshold
    chm = np.where(spikes, local, chm)
    return np.clip(chm, 0.0, None).astype(np.float32)
