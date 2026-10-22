"""Point cloud quality report."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import numpy as np

from . import io
from .grid import Grid


def _density_stats(counts: np.ndarray, cell: float) -> dict:
    per_m2 = counts.astype(np.float64) / (cell * cell)
    occupied = per_m2[counts > 0]
    if occupied.size == 0:
        return {"cell_m": cell, "mean": 0.0, "median": 0.0, "p05": 0.0, "p95": 0.0,
                "empty_fraction": 1.0}
    return {
        "cell_m": cell,
        "mean": float(occupied.mean()),
        "median": float(np.median(occupied)),
        "p05": float(np.percentile(occupied, 5)),
        "p95": float(np.percentile(occupied, 95)),
        # Empty cells include gaps inside the footprint and corners of the
        # bounding box outside it, so treat this as an upper bound.
        "empty_fraction": float((counts == 0).mean()),
    }


class _StripAccumulator:
    """Per (cell, flight line) count, sum and sum of squares of z, grown as new
    point_source_ids appear, so alignment can be measured in the same pass."""

    def __init__(self, grid: Grid, z0: float):
        self.grid, self.z0 = grid, z0
        self.columns: dict[int, int] = {}
        self.n = np.zeros((grid.rows * grid.cols, 0))
        self.s = self.n.copy()
        self.ss = self.n.copy()

    def _column(self, source_ids: np.ndarray) -> np.ndarray:
        for sid in np.unique(source_ids).tolist():
            if sid not in self.columns:
                self.columns[sid] = len(self.columns)
        width = len(self.columns)
        if width > self.n.shape[1]:
            pad = ((0, 0), (0, width - self.n.shape[1]))
            self.n, self.s, self.ss = (np.pad(a, pad) for a in (self.n, self.s, self.ss))
        lookup = np.vectorize(self.columns.get, otypes=[np.int64])
        u, inv = np.unique(source_ids, return_inverse=True)
        return lookup(u)[inv]

    def add(self, x, y, z, source_ids):
        idx = self.grid.flat_index(x, y)
        ok = idx >= 0
        if not ok.any():
            return
        col = self._column(source_ids[ok])
        flat = idx[ok] * self.n.shape[1] + col
        dz = z[ok] - self.z0
        size = self.n.size
        self.n += np.bincount(flat, minlength=size).reshape(self.n.shape)
        self.s += np.bincount(flat, weights=dz, minlength=size).reshape(self.n.shape)
        self.ss += np.bincount(flat, weights=dz * dz, minlength=size).reshape(self.n.shape)

    def report(self, min_points: int = 10, max_std: float = 0.08) -> dict:
        """Offsets between flight lines over smooth surfaces (open ground, roads).

        A (cell, line) sample is usable when it has min_points and a z standard
        deviation of at most max_std, which excludes vegetation and edges. For
        cells with two or more usable lines, each line's mean is compared to
        the median of the line means in that cell.
        """
        n = self.n
        with np.errstate(invalid="ignore", divide="ignore"):
            mean = self.s / n
            std = np.sqrt(np.maximum(self.ss / n - mean * mean, 0.0))
        usable = (n >= min_points) & (std <= max_std)
        cells = usable.sum(axis=1) >= 2
        out = {"cell_m": self.grid.cell, "min_points": min_points, "max_std_m": max_std,
               "flight_lines": len(self.columns), "compared_cells": int(cells.sum())}
        if not cells.any():
            return out
        m = np.where(usable[cells], mean[cells], np.nan)
        dev = m - np.nanmedian(m, axis=1, keepdims=True)
        spread = np.nanmax(m, axis=1) - np.nanmin(m, axis=1)
        absdev = np.abs(dev[~np.isnan(dev)])
        out.update({
            "abs_offset_median_m": round(float(np.median(absdev)), 4),
            "abs_offset_p95_m": round(float(np.percentile(absdev, 95)), 4),
            "cell_spread_median_m": round(float(np.median(spread)), 4),
            "cell_spread_p95_m": round(float(np.percentile(spread, 95)), 4),
        })
        per_line = {}
        ids = {c: sid for sid, c in self.columns.items()}
        for c in range(dev.shape[1]):
            d = dev[:, c][~np.isnan(dev[:, c])]
            if d.size >= 20:
                per_line[str(ids[c])] = {"cells": int(d.size),
                                         "median_offset_m": round(float(np.median(d)), 4)}
        out["per_line"] = per_line
        return out


def qc_report(path: str | Path, cell: float = 1.0, strip_cell: float = 2.0) -> dict:
    """Stream the file once and summarise header, fields, returns and density."""
    with io.open_las(path) as reader:
        h = reader.header
        crs = h.parse_crs()
        dims = list(h.point_format.dimension_names)
        grid = Grid.from_bounds(h.mins[0], h.mins[1], h.maxs[0], h.maxs[1], cell)
        report = {
            "point_count": int(h.point_count),
            "crs_epsg": crs.to_epsg() if crs else None,
            "crs_name": crs.name if crs else None,
            "las_version": str(h.version),
            "point_format": int(h.point_format.id),
            "dimensions": dims,
            "extent": {
                "minx": float(h.mins[0]), "miny": float(h.mins[1]),
                "maxx": float(h.maxs[0]), "maxy": float(h.maxs[1]),
                "minz": float(h.mins[2]), "maxz": float(h.maxs[2]),
            },
        }

    strips = _StripAccumulator(
        Grid.from_bounds(h.mins[0], h.mins[1], h.maxs[0], h.maxs[1], strip_cell), h.mins[2])
    all_counts = np.zeros(grid.rows * grid.cols, dtype=np.int64)
    first_counts = np.zeros_like(all_counts)
    returns, classes, sources = Counter(), Counter(), Counter()
    for chunk in io.iter_chunks(path):
        x, y = np.asarray(chunk.x), np.asarray(chunk.y)
        rn = np.asarray(chunk.return_number)
        idx = grid.flat_index(x, y)
        ok = idx >= 0
        all_counts += np.bincount(idx[ok], minlength=all_counts.size)
        first = ok & (rn == 1)
        first_counts += np.bincount(idx[first], minlength=all_counts.size)
        strips.add(x, y, np.asarray(chunk.z), np.asarray(chunk.point_source_id))
        for counter, values in ((returns, rn),
                                (classes, np.asarray(chunk.classification)),
                                (sources, np.asarray(chunk.point_source_id))):
            u, c = np.unique(values, return_counts=True)
            counter.update(dict(zip(u.tolist(), c.tolist())))

    report["return_number_counts"] = {str(k): v for k, v in sorted(returns.items())}
    report["classification_counts"] = {str(k): v for k, v in sorted(classes.items())}
    report["point_source_count"] = len(sources)
    report["point_source_counts"] = {str(k): v for k, v in sorted(sources.items())}
    report["density"] = _density_stats(all_counts.reshape(grid.shape), cell)
    report["first_return_density"] = _density_stats(first_counts.reshape(grid.shape), cell)
    report["strip_alignment"] = strips.report()
    return report
