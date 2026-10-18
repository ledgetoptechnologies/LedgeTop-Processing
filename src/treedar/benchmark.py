"""Tree detection benchmark against hand-segmented reference point clouds.

Reference files carry a per-point tree identifier (e.g. FOR-instance `treeID`,
0 = not a tree). Each reference tree's top is its highest point. Detected
treetops (trees.csv) are paired one-to-one with reference tops by minimum
total distance, accepting a pair only within `max_dist_m` horizontally and
`max_dz_m` in height. Recall = matched / reference, precision = matched /
detected, reported for all reference trees and by height class.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import numpy as np

from . import io


def reference_tops(path, id_field="treeID", min_points=10, dtm=None, transform=None):
    """(n, 4) array [tree_id, x, y, height] of reference tree tops.

    Height is above the supplied DTM when given (same grid as TreeDar runs),
    otherwise the raw top z minus the lowest point of the file.
    """
    tops: dict[int, tuple] = {}
    counts: dict[int, int] = {}
    zmin = np.inf
    with io.open_las(path) as reader:
        if id_field not in reader.header.point_format.dimension_names:
            raise ValueError(f"{path} has no '{id_field}' field")
        for chunk in reader.chunk_iterator(io.CHUNK):
            tid = np.asarray(chunk[id_field]).astype(np.int64)
            z = np.asarray(chunk.z)
            zmin = min(zmin, float(z.min()))
            sel = tid > 0
            if not sel.any():
                continue
            tid, z, x, y = tid[sel], z[sel], np.asarray(chunk.x)[sel], np.asarray(chunk.y)[sel]
            order = np.lexsort((z, tid))
            last = np.r_[tid[order][1:] != tid[order][:-1], True]
            u, c = np.unique(tid, return_counts=True)
            for k, v in zip(u.tolist(), c.tolist()):
                counts[k] = counts.get(k, 0) + v
            for i in order[last]:
                k = int(tid[i])
                if k not in tops or z[i] > tops[k][2]:
                    tops[k] = (float(x[i]), float(y[i]), float(z[i]))
    rows = []
    for k, (x, y, z) in tops.items():
        if counts.get(k, 0) < min_points:
            continue
        if dtm is not None:
            from .grid import Grid
            g = Grid.from_transform(transform, dtm.shape)
            r, c = g.rowcol(np.array([x]), np.array([y]))
            r = int(np.clip(r[0], 0, g.rows - 1))
            c = int(np.clip(c[0], 0, g.cols - 1))
            h = z - float(dtm[r, c])
        else:
            h = z - zmin
        rows.append((k, x, y, h))
    return np.array(rows, dtype=float).reshape(-1, 4)


def match(ref_xyh, det_xyh, max_dist_m=1.5, max_dz_m=3.0):
    """One-to-one pairs (ref_index, det_index) minimising total distance."""
    from scipy.optimize import linear_sum_assignment
    if len(ref_xyh) == 0 or len(det_xyh) == 0:
        return np.empty((0, 2), dtype=int)
    d = np.hypot(ref_xyh[:, None, 0] - det_xyh[None, :, 0], ref_xyh[:, None, 1] - det_xyh[None, :, 1])
    dz = np.abs(ref_xyh[:, None, 2] - det_xyh[None, :, 2])
    ok = (d <= max_dist_m) & (dz <= max_dz_m)
    cost = np.where(ok, d, 1e6)
    r, c = linear_sum_assignment(cost)
    keep = ok[r, c]
    return np.c_[r[keep], c[keep]]


def score(ref_xyh, det_xyh, pairs, min_height=5.0, classes=((5, 10), (10, 20), (20, 999))) -> dict:
    """Recall/precision/F1 overall (reference and detections at or above
    min_height) and recall by reference height class."""
    ref_ok = ref_xyh[:, 2] >= min_height
    det_ok = det_xyh[:, 2] >= min_height
    matched_ref = np.zeros(len(ref_xyh), bool)
    matched_det = np.zeros(len(det_xyh), bool)
    if len(pairs):
        matched_ref[pairs[:, 0]] = True
        matched_det[pairs[:, 1]] = True
    n_ref, n_det = int(ref_ok.sum()), int(det_ok.sum())
    tp_r = int((matched_ref & ref_ok).sum())
    tp_d = int((matched_det & det_ok).sum())
    recall = tp_r / n_ref if n_ref else None
    precision = tp_d / n_det if n_det else None
    f1 = (2 * recall * precision / (recall + precision)) if recall and precision else None
    by_class = []
    for lo, hi in classes:
        m = (ref_xyh[:, 2] >= lo) & (ref_xyh[:, 2] < hi)
        if m.any():
            by_class.append({"height_m": f"{lo}-{hi if hi < 999 else ''}", "n_reference": int(m.sum()),
                             "recall": round(float(matched_ref[m].mean()), 4)})
    dh = None
    if len(pairs):
        dh = det_xyh[pairs[:, 1], 2] - ref_xyh[pairs[:, 0], 2]
    return {"min_height_m": min_height, "n_reference": n_ref, "n_detected": n_det,
            "recall": None if recall is None else round(recall, 4),
            "precision": None if precision is None else round(precision, 4),
            "f1": None if f1 is None else round(f1, 4),
            "height_error_median_m": None if dh is None else round(float(np.median(dh)), 3),
            "height_error_abs_p95_m": None if dh is None else round(float(np.percentile(np.abs(dh), 95)), 3),
            "recall_by_height": by_class}


def outside_mask(reference_las, transform, shape, exclude_class=3):
    """Boolean grid: True where most points of the reference are `exclude_class`
    (FOR-instance class 3 = trees outside the annotated plot)."""
    from .grid import Grid
    g = Grid.from_transform(transform, shape)
    total = np.zeros(g.rows * g.cols, dtype=np.int64)
    excl = np.zeros_like(total)
    for chunk in io.iter_chunks(reference_las):
        idx = g.flat_index(np.asarray(chunk.x), np.asarray(chunk.y))
        ok = idx >= 0
        total += np.bincount(idx[ok], minlength=total.size)
        cls = np.asarray(chunk.classification)[ok]
        excl += np.bincount(idx[ok][cls == exclude_class], minlength=total.size)
    with np.errstate(invalid="ignore", divide="ignore"):
        frac = excl / total
    return (np.nan_to_num(frac) > 0.5).reshape(g.shape)


def evaluate_run(reference_las, run_dir, max_dist_m=1.5, max_dz_m=3.0, min_height=5.0,
                 exclude_class: int | None = 3, detections=None) -> dict:
    """Score a TreeDar run folder against a reference point cloud.

    Detections in cells dominated by `exclude_class` points (trees outside the
    annotated plot) are dropped before scoring. `detections` may supply an
    (n, 3) [x, y, height] array instead of the run's trees.csv.
    """
    import rasterio
    run_dir = Path(run_dir)
    with rasterio.open(run_dir / "dtm.tif") as ds:
        dtm, transform = ds.read(1), ds.transform
    ref = reference_tops(reference_las, dtm=dtm, transform=transform)
    if detections is None:
        with open(run_dir / "trees.csv", newline="") as fh:
            detections = [[float(r["x"]), float(r["y"]), float(r["height_m"])] for r in csv.DictReader(fh)]
    det = np.asarray(detections, dtype=float).reshape(-1, 3)
    n_dropped = 0
    if exclude_class is not None and len(det):
        from .grid import Grid
        mask = outside_mask(reference_las, transform, dtm.shape, exclude_class)
        g = Grid.from_transform(transform, dtm.shape)
        r, c = g.rowcol(det[:, 0], det[:, 1])
        r, c = np.clip(r, 0, g.rows - 1), np.clip(c, 0, g.cols - 1)
        keep = ~mask[r, c]
        n_dropped = int((~keep).sum())
        det = det[keep]
    pairs = match(ref[:, 1:4], det, max_dist_m, max_dz_m)
    result = {"reference": Path(reference_las).name, "max_dist_m": max_dist_m, "max_dz_m": max_dz_m,
              "detections_outside_plot_dropped": n_dropped,
              **score(ref[:, 1:4], det, pairs, min_height)}
    (run_dir / "benchmark.json").write_text(json.dumps(result, indent=2))
    return result
