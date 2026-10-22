"""Trunk (stem) detection and treetop + trunk fusion.

Above-canopy drone LiDAR samples few points on trunks, but where the canopy
is thinner some pulses reach the lower stem. In a horizontal slice above the
ground (default 1.0 to 2.5 m), a trunk shows up as a tight cluster of points
that also spans much of the slice vertically. Low shrubs and branch tips are
wider or flatter. Each accepted cluster must have canopy above it.

Fusion keeps every treetop and adds a tree for each trunk with no treetop
within `radius_m`, which recovers trees whose crowns the treetop method
merged into a neighbour or missed.
"""

from __future__ import annotations

import numpy as np
from scipy import ndimage

from .grid import Grid


def detect_stems(points: dict, dtm, transform, chm, slice_m=(1.0, 2.5), eps_m=0.15,
                 min_samples=15, max_radius_m=0.25, min_vertical_frac=0.4,
                 merge_dist_m=0.5, min_canopy_m=3.0, canopy_radius_m=1.5) -> np.ndarray:
    """(n, 3) array [x, y, canopy height above the trunk] of trunk candidates."""
    from sklearn.cluster import DBSCAN
    grid = Grid.from_transform(transform, dtm.shape)
    x, y, z = points["x"], points["y"], points["z"]
    r, c = grid.rowcol(x, y)
    inside = (r >= 0) & (r < grid.rows) & (c >= 0) & (c < grid.cols)
    hag = np.full(x.shape, np.nan)
    hag[inside] = z[inside] - dtm[r[inside], c[inside]]
    lo, hi = slice_m
    sel = inside & (hag >= lo) & (hag < hi)
    if sel.sum() < min_samples:
        return np.empty((0, 3))
    xy = np.c_[x[sel], y[sel]]
    hs = hag[sel]
    labels = DBSCAN(eps=eps_m, min_samples=min_samples).fit_predict(xy)
    found = []
    for k in np.unique(labels[labels >= 0]):
        m = labels == k
        q = xy[m]
        centre = q.mean(axis=0)
        radius = np.sqrt(((q - centre) ** 2).sum(axis=1)).mean()
        vertical = (hs[m].max() - hs[m].min()) / (hi - lo)
        if radius <= max_radius_m and vertical >= min_vertical_frac:
            found.append(centre)
    if not found:
        return np.empty((0, 3))
    stems = _merge_close(np.array(found), merge_dist_m)

    # Canopy above each trunk: highest CHM within canopy_radius_m.
    rad = max(1, int(round(canopy_radius_m / grid.cell)))
    yy, xx = np.mgrid[-rad:rad + 1, -rad:rad + 1]
    local_max = ndimage.maximum_filter(chm, footprint=(xx * xx + yy * yy) <= rad * rad)
    sr, sc = grid.rowcol(stems[:, 0], stems[:, 1])
    sr, sc = np.clip(sr, 0, grid.rows - 1), np.clip(sc, 0, grid.cols - 1)
    h = local_max[sr, sc]
    keep = h >= min_canopy_m
    return np.c_[stems[keep], h[keep]]


def _merge_close(xy: np.ndarray, dist: float) -> np.ndarray:
    """Average points closer than `dist` (single linkage)."""
    from scipy.sparse.csgraph import connected_components
    from scipy.spatial import cKDTree
    if len(xy) < 2:
        return xy
    pairs = cKDTree(xy).query_pairs(dist, output_type="ndarray")
    if len(pairs) == 0:
        return xy
    from scipy.sparse import coo_matrix
    n = len(xy)
    adj = coo_matrix((np.ones(len(pairs)), (pairs[:, 0], pairs[:, 1])), shape=(n, n))
    _, comp = connected_components(adj, directed=False)
    return np.array([xy[comp == k].mean(axis=0) for k in range(comp.max() + 1)])


def fuse(treetops: np.ndarray, stems: np.ndarray, radius_m: float = 1.5) -> tuple[np.ndarray, np.ndarray]:
    """Treetops plus trunks with no treetop within radius_m.

    Returns (trees [x, y, height], source) where source is "top" or "stem".
    """
    from scipy.spatial import cKDTree
    tops = np.asarray(treetops, dtype=float).reshape(-1, 3)
    st = np.asarray(stems, dtype=float).reshape(-1, 3)
    if len(st) and len(tops):
        d, _ = cKDTree(tops[:, :2]).query(st[:, :2])
        st = st[d > radius_m]
    trees = np.vstack([tops, st]) if len(st) else tops
    source = np.array(["top"] * len(tops) + ["stem"] * len(st), dtype=object)
    return trees, source
