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


def qc_report(path: str | Path, cell: float = 1.0) -> dict:
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
    return report
