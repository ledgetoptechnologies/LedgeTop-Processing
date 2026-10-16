"""Regular raster grid helpers shared by terrain, CHM and segmentation."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from affine import Affine
from scipy import ndimage


@dataclass(frozen=True)
class Grid:
    minx: float
    maxy: float
    cell: float
    rows: int
    cols: int

    @classmethod
    def from_bounds(cls, minx, miny, maxx, maxy, cell) -> "Grid":
        # Snap to whole cells so grids built from the same bounds line up.
        minx = np.floor(minx / cell) * cell
        maxy = np.ceil(maxy / cell) * cell
        cols = int(np.ceil((maxx - minx) / cell)) + 1
        rows = int(np.ceil((maxy - miny) / cell)) + 1
        return cls(float(minx), float(maxy), float(cell), rows, cols)

    @classmethod
    def from_points(cls, x, y, cell) -> "Grid":
        return cls.from_bounds(x.min(), y.min(), x.max(), y.max(), cell)

    @classmethod
    def from_transform(cls, transform: Affine, shape) -> "Grid":
        return cls(transform.c, transform.f, transform.a, shape[0], shape[1])

    @property
    def shape(self):
        return (self.rows, self.cols)

    @property
    def transform(self) -> Affine:
        return Affine(self.cell, 0.0, self.minx, 0.0, -self.cell, self.maxy)

    def rowcol(self, x, y):
        col = ((x - self.minx) / self.cell).astype(np.int64)
        row = ((self.maxy - y) / self.cell).astype(np.int64)
        return row, col

    def flat_index(self, x, y):
        """Flat cell index per point, or -1 for points outside the grid."""
        row, col = self.rowcol(x, y)
        inside = (row >= 0) & (row < self.rows) & (col >= 0) & (col < self.cols)
        return np.where(inside, row * self.cols + col, -1)

    def cell_centers(self, rows, cols):
        x = self.minx + (cols + 0.5) * self.cell
        y = self.maxy - (rows + 0.5) * self.cell
        return x, y


def aggregate(grid: Grid, x, y, values, how: str) -> np.ndarray:
    """Per-cell min or max of values. Empty cells are NaN."""
    idx = grid.flat_index(x, y)
    ok = idx >= 0
    idx, values = idx[ok], values[ok]
    if how == "min":
        out = np.full(grid.rows * grid.cols, np.inf)
        np.minimum.at(out, idx, values)
        out[np.isinf(out)] = np.nan
    elif how == "max":
        out = np.full(grid.rows * grid.cols, -np.inf)
        np.maximum.at(out, idx, values)
        out[np.isinf(out)] = np.nan
    else:
        raise ValueError(how)
    return out.reshape(grid.shape)


def count(grid: Grid, x, y) -> np.ndarray:
    idx = grid.flat_index(x, y)
    idx = idx[idx >= 0]
    return np.bincount(idx, minlength=grid.rows * grid.cols).reshape(grid.shape)


def fill_nearest(arr: np.ndarray) -> np.ndarray:
    """Replace NaN cells with the value of the nearest valid cell."""
    missing = np.isnan(arr)
    if not missing.any():
        return arr
    if missing.all():
        raise ValueError("cannot fill a grid with no valid cells")
    _, (ri, ci) = ndimage.distance_transform_edt(missing, return_indices=True)
    return arr[ri, ci]
