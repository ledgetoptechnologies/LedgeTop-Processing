"""Lasting tree IDs across reruns of one dataset.

A run's `tree_id` is a crown label numbered in scan order, so a rerun with
other settings can renumber every tree. A registry file per dataset keeps
lasting IDs. The first run seeds it: each tree's lasting ID is its run number,
so labels already written against that run stay valid. Each later run pairs
its treetops with registered trees one-to-one, closest pairs first, within
`max_dist_m` horizontally and `max_dz_m` in height. A paired tree keeps its
lasting ID; an unpaired treetop gets the next unused ID.

Ambiguous cases are flagged for review, never resolved silently: a registered
tree with two or more treetops in range (`possible_split`), or a treetop with
two or more registered trees in range (`possible_merge`). Labels are not
carried to flagged trees. Registered trees that a run does not find stay in
the registry as `missing`, and their IDs are never reused.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import os
from pathlib import Path

import numpy as np
import rasterio
from pyproj import CRS
from scipy.spatial import cKDTree

SCHEMA = "treedar.tree-ids.v1"
MAX_DIST_M = 1.5
MAX_DZ_M = 3.0
COLUMNS = ["tree_id", "stable_id", "match", "distance_m", "dz_m", "review", "related_ids"]


def _crs_text(crs) -> str | None:
    if crs is None:
        return None
    crs = CRS.from_user_input(crs)
    if not crs.is_projected or any(
            not np.isclose(axis.unit_conversion_factor, 1) for axis in crs.axis_info[:2]):
        raise ValueError("tree IDs need a projected CRS in metres")
    return crs.to_wkt()


def _same_crs(a: str | None, b: str | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return CRS.from_wkt(a) == CRS.from_wkt(b)


def new_registry(crs=None, max_dist_m=MAX_DIST_M, max_dz_m=MAX_DZ_M) -> dict:
    return {"schema": SCHEMA, "crs": _crs_text(crs), "max_dist_m": max_dist_m,
            "max_dz_m": max_dz_m, "next_id": 1, "runs": [], "trees": []}


def load_registry(path) -> dict | None:
    path = Path(path)
    if not path.exists():
        return None
    registry = json.loads(path.read_text())
    if not isinstance(registry, dict) or registry.get("schema") != SCHEMA:
        raise ValueError(f"not a TreeDar tree ID registry: {path}")
    return registry


def save_registry(path, registry: dict) -> None:
    """Write via a temporary file so an interrupted save keeps the old registry."""
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(registry, indent=2))
    os.replace(temporary, path)


def assign(trees: list[dict], registry: dict, run_name: str) -> list[dict]:
    """Lasting ID rows for one run's trees; updates `registry` in place.

    `trees` are trees.csv rows (tree_id, x, y, height_m). Returns one row per
    tree with COLUMNS, ordered by run tree_id.
    """
    order = sorted(trees, key=lambda t: int(t["tree_id"]))
    run_ids = [int(t["tree_id"]) for t in order]
    if len(set(run_ids)) != len(run_ids):
        raise ValueError("duplicate tree_id in trees.csv")
    new = np.array([[float(t["x"]), float(t["y"]), float(t["height_m"])] for t in order],
                   dtype=np.float64).reshape(-1, 3)
    known = registry["trees"]
    stamp = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    if not known:
        rows = [_row(tid, tid, "seed") for tid in run_ids]
        for tid, (x, y, h) in zip(run_ids, new):
            known.append(_entry(tid, x, y, h, run_name))
        registry["next_id"] = max(run_ids, default=0) + 1
        _record_run(registry, run_name, stamp, rows, missing=0)
        return rows

    old = np.array([[t["x"], t["y"], t["height_m"]] for t in known], dtype=np.float64)
    max_dist, max_dz = registry["max_dist_m"], registry["max_dz_m"]
    candidates = []  # (distance, new index, old index, dz)
    if len(new):
        for i, near in enumerate(cKDTree(old[:, :2]).query_ball_point(new[:, :2], max_dist)):
            for j in near:
                dz = new[i, 2] - old[j, 2]
                if abs(dz) <= max_dz:
                    d = float(np.hypot(*(new[i, :2] - old[j, :2])))
                    candidates.append((d, i, j, float(dz)))
    candidates.sort(key=lambda c: (c[0], run_ids[c[1]], known[c[2]]["id"]))

    pair_of_new, pair_of_old = {}, {}
    for d, i, j, dz in candidates:
        if i not in pair_of_new and j not in pair_of_old:
            pair_of_new[i], pair_of_old[j] = (j, d, dz), i
    olds_near_new, news_near_old = {}, {}
    for _, i, j, _ in candidates:
        olds_near_new.setdefault(i, set()).add(j)
        news_near_old.setdefault(j, set()).add(i)

    rows = []
    for i, tid in enumerate(run_ids):
        if i in pair_of_new:
            j, d, dz = pair_of_new[i]
            row = _row(tid, known[j]["id"], "matched", d, dz)
        else:
            row = _row(tid, registry["next_id"], "new")
            registry["next_id"] += 1
        rows.append(row)

    # A registered tree near several treetops may have split; a treetop near
    # several registered trees may be a merge. Flag every tree involved.
    for j, near in news_near_old.items():
        if len(near) > 1:
            for i in near:
                _flag(rows[i], "possible_split", {known[j]["id"]} | {rows[k]["stable_id"] for k in near})
    for i, near in olds_near_new.items():
        if len(near) > 1:
            _flag(rows[i], "possible_merge", {known[j]["id"] for j in near})

    by_id = {t["id"]: t for t in known}
    for i, row in enumerate(rows):
        x, y, h = new[i]
        if row["match"] == "new":
            known.append(_entry(row["stable_id"], x, y, h, run_name))
            continue
        entry = by_id[row["stable_id"]]
        entry["status"], entry["last_run"] = "active", run_name
        if not row["review"]:
            entry["x"], entry["y"], entry["height_m"] = float(x), float(y), round(float(h), 2)
    missing = 0
    for j, entry in enumerate(known[:len(old)]):
        if j not in pair_of_old:
            entry["status"] = "missing"
            missing += 1
    _record_run(registry, run_name, stamp, rows, missing)
    return rows


def _row(tree_id, stable_id, match, distance=None, dz=None) -> dict:
    return {"tree_id": tree_id, "stable_id": stable_id, "match": match,
            "distance_m": "" if distance is None else round(distance, 2),
            "dz_m": "" if dz is None else round(dz, 2), "review": "", "related_ids": ""}


def _entry(stable_id, x, y, h, run_name) -> dict:
    return {"id": int(stable_id), "x": float(x), "y": float(y), "height_m": round(float(h), 2),
            "status": "active", "first_run": run_name, "last_run": run_name}


def _flag(row, reason, related):
    """Add a review reason and the other lasting IDs involved (never the row's own)."""
    reasons = set(filter(None, row["review"].split(";"))) | {reason}
    ids = set(filter(None, str(row["related_ids"]).split(";"))) | {str(r) for r in related}
    ids.discard(str(row["stable_id"]))
    row["review"] = ";".join(sorted(reasons))
    row["related_ids"] = ";".join(sorted(ids, key=int))


def _record_run(registry, run_name, stamp, rows, missing):
    counts = {m: sum(r["match"] == m for r in rows) for m in ("seed", "matched", "new")}
    registry["runs"].append({"run": run_name, "assigned_utc": stamp, "trees": len(rows),
                             **counts, "missing": missing,
                             "review": sum(bool(r["review"]) for r in rows)})


def update_run(run_dir, registry_path, max_dist_m=MAX_DIST_M, max_dz_m=MAX_DZ_M) -> dict:
    """Assign lasting IDs to a run folder and write its tree_ids.csv.

    Returns the run's counts. A run already in the registry is not assigned
    twice; its existing tree_ids.csv is kept.
    """
    run_dir, registry_path = Path(run_dir), Path(registry_path)
    run_name = run_dir.resolve().name
    with rasterio.open(run_dir / "crowns.tif") as ds:
        run_crs = _crs_text(ds.crs)
    registry = load_registry(registry_path)
    if registry is None:
        registry = new_registry(run_crs, max_dist_m, max_dz_m)
    elif not _same_crs(registry["crs"], run_crs):
        raise ValueError("run CRS differs from the tree ID registry; reproject before matching")
    previous = next((r for r in registry["runs"] if r["run"] == run_name), None)
    if previous is not None:
        if not (run_dir / "tree_ids.csv").exists():
            raise ValueError(f"run {run_name} is registered but its tree_ids.csv is missing")
        return previous
    with open(run_dir / "trees.csv", newline="") as fh:
        trees = list(csv.DictReader(fh))
    rows = assign(trees, registry, run_name)
    with open(run_dir / "tree_ids.csv", "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    save_registry(registry_path, registry)
    return registry["runs"][-1]


def read_tree_ids(path) -> list[dict]:
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def labels_for_run(id_rows: list[dict], stable_labels: list[dict]) -> tuple[list[dict], list[str]]:
    """Turn labels keyed by stable_id into this run's tree_id labels.

    Trees flagged for review, and labels whose tree this run did not find,
    are left out and reported, so a label never moves to a different tree.
    """
    by_stable = {}
    for row in id_rows:
        by_stable[str(row["stable_id"])] = row
    out, skipped = [], []
    for label in stable_labels:
        sid = str(label["stable_id"]).strip()
        row = by_stable.get(sid)
        if row is None:
            skipped.append(f"{sid}: not found in this run")
        elif row["review"]:
            skipped.append(f"{sid}: {row['review']}")
        elif label.get("label", "").strip():
            out.append({"tree_id": row["tree_id"], "label": label["label"].strip(),
                        "group": label.get("group", "")})
    return out, skipped
