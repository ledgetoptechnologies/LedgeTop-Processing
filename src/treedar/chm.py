"""Canopy height model."""

from __future__ import annotations

import numpy as np
from scipy import ndimage

from .grid import Grid, aggregate

MAX_TREE_HEIGHT_M = 60.0  # anything higher above ground is treated as noise


def max_height_grid(x, y, z, dtm, transform, cell: float = 0.5) -> np.ndarray:
    """Highest height-above-ground per cell, empty cells filled, no smoothing.

    Keep this unfiltered surface when comparing against field-measured heights.
    """
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
    return np.clip(np.nan_to_num(chm, nan=0.0), 0.0, None).astype(np.float32)


def filter_pits_and_spikes(raw: np.ndarray, pit_threshold: float = 1.0,
                           spike_threshold: float | None = 5.0, spike_window: int = 5):
    """Heuristic clean-up of a raw CHM. Returns (chm, stats).

    Pits: cells more than pit_threshold below their 3x3 median are raised to it.
    Spikes: cells more than spike_threshold above their spike_window median are
    lowered to it. This is noise suppression by shape, not a confirmed noise
    classification: a very narrow real apex beside a gap can also be lowered.
    Pass spike_threshold=None or <= 0 to disable it.
    """
    local = ndimage.median_filter(raw, size=3)
    pits = raw < local - pit_threshold
    chm = np.where(pits, local, raw)

    spikes = np.zeros_like(pits)
    if spike_threshold is not None and spike_threshold > 0:
        local = ndimage.median_filter(chm, size=spike_window)
        spikes = chm > local + spike_threshold
        chm = np.where(spikes, local, chm)

    stats = {"pit_cells_raised": int(pits.sum()), "spike_cells_lowered": int(spikes.sum()),
             "pit_threshold_m": pit_threshold, "spike_threshold_m": spike_threshold,
             "spike_window_cells": spike_window}
    return np.clip(chm, 0.0, None).astype(np.float32), stats


def canopy_height_model(x, y, z, dtm, transform, cell: float = 0.5,
                        pit_threshold: float = 1.0,
                        spike_threshold: float | None = 5.0) -> np.ndarray:
    """Filtered CHM: max height per cell with pits and isolated spikes removed."""
    raw = max_height_grid(x, y, z, dtm, transform, cell)
    chm, _ = filter_pits_and_spikes(raw, pit_threshold, spike_threshold)
    return chm
