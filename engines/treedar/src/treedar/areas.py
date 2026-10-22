"""Named tree areas for collecting labels from the landowner.

Contiguous canopy (CHM >= min_height after closing small gaps) is split into
stands; stands larger than `split_area_m2` are divided into compact zones
with k-means on cell positions so a mixed block can be named in parts. Each
area gets a short name (A, B, ... and A1, A2 for zones). The landowner names
what grows in an area, and `labels_from_areas` turns those names into tree
labels with the area as the cross-validation group, so evaluation holds out
whole areas.
"""

from __future__ import annotations

import csv
import string
from pathlib import Path

import numpy as np
from scipy import ndimage

from .grid import Grid


def _names():
    letters = string.ascii_uppercase
    yield from letters
    for a in letters:
        for b in letters:
            yield a + b


def delineate(chm, transform, min_height=3.0, min_area_m2=100.0, split_area_m2=10000.0,
              zone_area_m2=7000.0, seed=0):
    """Return (area_map int array, {area_id: name})."""
    from sklearn.cluster import KMeans
    grid = Grid.from_transform(transform, chm.shape)
    cell_area = grid.cell ** 2
    closing = max(1, int(round(1.0 / grid.cell)))
    mask = ndimage.binary_closing(chm >= min_height, structure=np.ones((2 * closing + 1,) * 2))
    stands, n = ndimage.label(mask)
    if n == 0:
        return np.zeros(chm.shape, dtype=np.int32), {}
    sizes = ndimage.sum(np.ones_like(chm), stands, range(1, n + 1)) * cell_area
    order = [i + 1 for i in np.argsort(-sizes) if sizes[i] >= min_area_m2]

    area_map = np.zeros(chm.shape, dtype=np.int32)
    names: dict[int, str] = {}
    name_iter = _names()
    next_id = 1
    for sid in order:
        base = next(name_iter)
        rows, cols = np.nonzero(stands == sid)
        size = sizes[sid - 1]
        k = int(np.ceil(size / zone_area_m2)) if size > split_area_m2 else 1
        if k == 1:
            area_map[rows, cols] = next_id
            names[next_id] = base
            next_id += 1
            continue
        pts = np.c_[cols, rows].astype(float)
        km = KMeans(n_clusters=k, random_state=seed, n_init=10).fit(pts[:: max(1, len(pts) // 20000)])
        zone = km.predict(pts)
        rank = {z: i for i, z in enumerate(np.argsort(km.cluster_centers_[:, 0]))}  # west to east
        for z in range(k):
            sel = zone == z
            area_map[rows[sel], cols[sel]] = next_id + rank[z]
            names[next_id + rank[z]] = f"{base}{rank[z] + 1}"
        next_id += k
    return area_map, names


def tree_areas(trees: list[dict], area_map, names, transform) -> list[dict]:
    grid = Grid.from_transform(transform, area_map.shape)
    x = np.array([float(t["x"]) for t in trees])
    y = np.array([float(t["y"]) for t in trees])
    r, c = grid.rowcol(x, y)
    inside = (r >= 0) & (r < grid.rows) & (c >= 0) & (c < grid.cols)
    out = []
    for t, ok, rr, cc in zip(trees, inside, r, c):
        aid = int(area_map[rr, cc]) if ok else 0
        out.append({"tree_id": t["tree_id"], "area": names.get(aid, "")})
    return out


def render(chm, area_map, names, path, title="Tree areas"):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from skimage.segmentation import find_boundaries
    fig, ax = plt.subplots(figsize=(12, 11))
    ax.imshow(chm, cmap="Greens", vmin=0, vmax=max(5.0, float(np.percentile(chm, 99.5))))
    edges = find_boundaries(area_map)
    ax.imshow(np.ma.masked_where(~edges, edges), cmap="autumn")
    for aid, name in names.items():
        m = area_map == aid
        if not m.any():
            continue
        cy, cx = ndimage.center_of_mass(m)
        big = m.sum() > 6000
        ax.text(cx, cy, name, fontsize=14 if big else 9, fontweight="bold", ha="center",
                va="center", bbox=dict(facecolor="white", alpha=0.8, pad=1, lw=0))
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(path, dpi=75)
    plt.close(fig)


def read_area_types(path) -> dict[str, str]:
    """CSV with columns area,label. Blank label means unknown or mixed: skipped."""
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    out = {}
    for r in rows:
        area, label = (r.get("area") or "").strip(), (r.get("label") or "").strip()
        if area and label:
            out[area] = label
    return out


def labels_from_areas(tree_area_rows: list[dict], area_types: dict[str, str]) -> list[dict]:
    """tree_id, label, group rows for trees whose area has a named type."""
    return [{"tree_id": r["tree_id"], "label": area_types[r["area"]], "group": r["area"]}
            for r in tree_area_rows if r["area"] in area_types]


def write_csv(path, rows, fields):
    with open(Path(path), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
