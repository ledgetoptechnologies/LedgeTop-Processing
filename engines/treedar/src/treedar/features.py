"""Per-tree features for later classification.

Each point is assigned to the crown whose label cell it falls in. Colour and
intensity use only upper-crown points (height above ground at least
`upper_fraction` of the tree's top), so understory and ground do not leak in.
Points without camera colour (R = G = B = 0) are ignored for colour features.
Colour comes from the LiDAR unit's camera and is uncalibrated: compare trees
within one flight, not across flights.

Shape features follow the conifer/broadleaf literature: skewness and kurtosis
of relative heights, the Clark-Evans aggregation index of upper-crown points
in plan view (mean nearest-neighbour distance over its expectation for a
random pattern; below 1 is clustered, above 1 regular), and eigenvalue
linearity, planarity and sphericity of upper-crown points in 3D.
"""

from __future__ import annotations

import numpy as np
from scipy import ndimage

from .grid import Grid

FEATURE_COLUMNS = [
    "tree_id", "n_points", "n_upper_points", "n_colour_points",
    "hag_p50_rel", "hag_p75_rel", "hag_p90_rel", "hag_cv",
    "second_return_frac", "intensity_mean", "intensity_cv",
    "red_mean", "green_mean", "blue_mean",
    "rcc_mean", "gcc_mean", "bcc_mean", "exg_mean", "gcc_std",
    "hag_skew", "hag_kurt", "clark_evans", "linearity", "planarity", "sphericity",
]

SHAPE_SAMPLE = 2000  # upper-crown points per tree used for the shape features


class _Groups:
    """Per-label reductions over arrays already sorted by label."""

    def __init__(self, sorted_labels: np.ndarray):
        self.ids, self.starts = np.unique(sorted_labels, return_index=True)
        self.size = sorted_labels.size
        self.counts = np.diff(np.append(self.starts, self.size))

    def sum(self, values, mask=None):
        v = np.asarray(values, dtype=np.float64)
        if mask is not None:
            v = np.where(mask, v, 0.0)
        return np.add.reduceat(v, self.starts)

    def mean(self, values, mask=None):
        n = self.counts.astype(np.float64) if mask is None else self.sum(mask)
        with np.errstate(invalid="ignore", divide="ignore"):
            return self.sum(values, mask) / n, n

    def std(self, values, mask=None):
        v = np.asarray(values, dtype=np.float64)
        mean, n = self.mean(v, mask)
        sq, _ = self.mean(v * v, mask)
        return np.sqrt(np.maximum(sq - mean * mean, 0.0)), mean, n

    def max(self, values):
        return np.maximum.reduceat(values, self.starts)

    def broadcast(self, per_label):
        return np.repeat(per_label, self.counts)

    def quantiles_unit(self, values, qs):
        """Per-label lower quantiles of values in [0, 1], one float sort for all qs."""
        run = np.repeat(np.arange(self.ids.size, dtype=np.float64), self.counts)
        v = np.asarray(values, dtype=np.float64)
        v = v[np.argsort(run * 2.0 + v)]  # label runs stay apart because v <= 1
        return [v[self.starts + np.floor((self.counts - 1) * q).astype(int)] for q in qs]


def _shape_features(g, x, y, hag, rel, upper, sample=SHAPE_SAMPLE) -> dict:
    """Per-tree skew/kurtosis of relative height, Clark-Evans index and
    eigenvalue shape features. Arrays are in label-sorted order."""
    from scipy.spatial import ConvexHull, QhullError, cKDTree
    n = g.ids.size
    out = {k: np.full(n, np.nan) for k in
           ("hag_skew", "hag_kurt", "clark_evans", "linearity", "planarity", "sphericity")}
    mean, _ = g.mean(rel)
    std = np.sqrt(np.maximum(g.mean(rel * rel)[0] - mean * mean, 0.0))
    d = rel - g.broadcast(mean)
    sd = g.broadcast(np.where(std > 0, std, np.nan))
    with np.errstate(invalid="ignore"):
        out["hag_skew"] = g.mean((d / sd) ** 3)[0]
        out["hag_kurt"] = g.mean((d / sd) ** 4)[0] - 3.0
    ends = np.append(g.starts[1:], g.size)
    for i, (a, b) in enumerate(zip(g.starts, ends)):
        idx = a + np.flatnonzero(upper[a:b])
        if idx.size < 10:
            continue
        if idx.size > sample:  # even subsample keeps runs deterministic
            idx = idx[np.linspace(0, idx.size - 1, sample).astype(int)]
        xy = np.c_[x[idx], y[idx]]
        xy = np.unique(xy, axis=0)  # duplicate XY would make nearest distance 0
        if len(xy) >= 10:
            try:
                area = ConvexHull(xy).volume  # 2D hull "volume" is its area
            except QhullError:
                area = 0.0
            if area > 0:
                dist, _ = cKDTree(xy).query(xy, k=2)
                out["clark_evans"][i] = dist[:, 1].mean() / (0.5 * np.sqrt(area / len(xy)))
        pts = np.c_[x[idx], y[idx], hag[idx]]
        ev = np.sort(np.linalg.eigvalsh(np.cov(pts.T)))[::-1]
        if ev[0] > 0:
            out["linearity"][i] = (ev[0] - ev[1]) / ev[0]
            out["planarity"][i] = (ev[1] - ev[2]) / ev[0]
            out["sphericity"][i] = ev[2] / ev[0]
    return out


def tree_features(points: dict, dtm, labels, transform, upper_fraction: float = 0.5,
                  chm=None, above_top_tolerance: float = 1.0) -> list[dict]:
    """Return one feature row per crown label in `labels` (ids > 0).

    With `chm` (the filtered canopy height model), each tree's top is its
    maximum CHM value, so a raw outlier the CHM filter removed cannot set the
    scale, and points more than `above_top_tolerance` m above that top are
    dropped. Without it, the highest raw point in the crown is used.
    """
    grid = Grid.from_transform(transform, labels.shape)
    x, y, z = points["x"], points["y"], points["z"]
    row, col = grid.rowcol(x, y)
    inside = (row >= 0) & (row < grid.rows) & (col >= 0) & (col < grid.cols)
    lab = np.zeros(x.shape, dtype=np.int64)
    lab[inside] = labels[row[inside], col[inside]]
    sel = np.flatnonzero(lab > 0)
    if sel.size == 0:
        return []
    # Sort the selected points by label once; every array below is in that order.
    sel = sel[np.argsort(lab[sel], kind="stable")]
    hag = z[sel] - dtm[row[sel], col[sel]]
    if chm is not None:
        top_of = np.zeros(int(labels.max()) + 1)
        ids = np.unique(lab[sel])
        top_of[ids] = ndimage.maximum(chm, labels, ids)
        keep = hag <= top_of[lab[sel]] + above_top_tolerance
        sel, hag = sel[keep], hag[keep]
        if sel.size == 0:
            return []
    lab = lab[sel]
    g = _Groups(lab)
    top = g.max(hag) if chm is None else top_of[g.ids]

    rel = np.clip(hag / np.maximum(g.broadcast(top), 1e-6), 0.0, 1.0)
    upper = rel >= upper_fraction

    rel_std, rel_mean, n_points = g.std(rel)
    p50, p75, p90 = g.quantiles_unit(rel, (0.5, 0.75, 0.9))
    cols: dict[str, np.ndarray] = {
        "n_points": n_points,
        "n_upper_points": g.sum(upper),
        "hag_p50_rel": p50, "hag_p75_rel": p75, "hag_p90_rel": p90,
        "hag_cv": rel_std / np.maximum(rel_mean, 1e-6),
    }
    if "return_number" in points:
        cols["second_return_frac"], _ = g.mean(points["return_number"][sel] >= 2)
    if "intensity" in points:
        istd, imean, _ = g.std(points["intensity"][sel], upper)
        cols["intensity_mean"] = imean
        with np.errstate(invalid="ignore", divide="ignore"):
            cols["intensity_cv"] = istd / imean
    if all(c in points for c in ("red", "green", "blue")):
        r, gr, b = (points[c][sel].astype(np.float64) for c in ("red", "green", "blue"))
        total = r + gr + b
        coloured = upper & (total > 0)
        safe = np.where(total > 0, total, 1.0)
        rcc, gcc, bcc = r / safe, gr / safe, b / safe
        cols["n_colour_points"] = g.sum(coloured)
        cols["red_mean"], _ = g.mean(r, coloured)
        cols["green_mean"], _ = g.mean(gr, coloured)
        cols["blue_mean"], _ = g.mean(b, coloured)
        cols["rcc_mean"], _ = g.mean(rcc, coloured)
        cols["bcc_mean"], _ = g.mean(bcc, coloured)
        cols["exg_mean"], _ = g.mean(2 * gcc - rcc - bcc, coloured)
        cols["gcc_std"], cols["gcc_mean"], _ = g.std(gcc, coloured)

    cols.update(_shape_features(g, x[sel], y[sel], hag, rel, upper))

    out = []
    for i, tid in enumerate(g.ids):
        row_ = {"tree_id": int(tid)}
        for name in FEATURE_COLUMNS[1:]:
            v = cols.get(name)
            if v is None or not np.isfinite(v[i]):
                row_[name] = None
            elif name.startswith("n_"):
                row_[name] = int(v[i])
            else:
                row_[name] = round(float(v[i]), 4)
        out.append(row_)
    return out
