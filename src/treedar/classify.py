"""Tree type classifier with spatial cross-validation and an unknown option.

Labels are trusted tree types (field checks or the landowner), never model
predictions. Evaluation keeps nearby trees together: trees are grouped into
square spatial blocks and whole blocks are held out, so a test tree never has
a training neighbour from the same block. Probabilities are calibrated on
blocks separate from the test blocks. A tree is labelled only when its top
probability reaches a threshold; otherwise it is reported as unknown.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import numpy as np

UNKNOWN = "unknown"
TREE_NUMERIC = ("height_m", "crown_area_m2", "crown_diameter_m")
SKIP_FEATURES = {"tree_id", "n_points", "n_upper_points", "n_colour_points"}


def _read_csv(path) -> list[dict]:
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def _float(v):
    return float(v) if v not in ("", None) else np.nan


def load_table(trees_csv, features_csv, labels_csv=None):
    """Join trees, features and optional labels on tree_id.

    Returns (ids, xy, X, feature_names, labels) where labels holds the label
    string per tree, or "" when unlabelled.
    """
    trees = {r["tree_id"]: r for r in _read_csv(trees_csv)}
    feats = {r["tree_id"]: r for r in _read_csv(features_csv)}
    labels = {}
    if labels_csv is not None:
        for r in _read_csv(labels_csv):
            lab = (r.get("label") or "").strip()
            if lab:
                labels[r["tree_id"]] = lab
    ids = [tid for tid in trees if tid in feats]
    if not ids:
        raise ValueError("no tree_id appears in both the trees and features tables")
    unknown_ids = set(labels) - set(ids)
    if unknown_ids:
        raise ValueError(f"{len(unknown_ids)} labelled tree_ids are not in the trees/features tables")
    fnames = [c for c in feats[ids[0]] if c not in SKIP_FEATURES] + list(TREE_NUMERIC)
    X = np.array([[_float(feats[t][c]) if c in feats[t] else _float(trees[t][c]) for c in fnames]
                  for t in ids])
    xy = np.array([[float(trees[t]["x"]), float(trees[t]["y"])] for t in ids])
    y = np.array([labels.get(t, "") for t in ids], dtype=object)
    return ids, xy, X, fnames, y


def spatial_blocks(xy: np.ndarray, block_m: float) -> np.ndarray:
    """Integer block id per tree from a square grid of block_m metres."""
    if block_m <= 0:
        raise ValueError("block size must be positive")
    cells = np.floor((xy - xy.min(axis=0)) / block_m).astype(np.int64)
    _, block = np.unique(cells, axis=0, return_inverse=True)
    return block.ravel()


def _model():
    from sklearn.ensemble import HistGradientBoostingClassifier
    return HistGradientBoostingClassifier(max_iter=200, learning_rate=0.08,
                                          max_leaf_nodes=15, l2_regularization=1.0,
                                          random_state=0)


def _fit_calibrated(X, y, groups, inner_folds: int):
    """Gradient boosting with sigmoid calibration fitted on held-out blocks."""
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.model_selection import GroupKFold
    n_groups = len(np.unique(groups))
    k = min(inner_folds, n_groups)
    if k < 2:
        model = _model().fit(X, y)
        return model, False
    splits = list(GroupKFold(n_splits=k).split(X, y, groups))
    # Calibration folds must contain every class in training, else skip calibration.
    if any(len(np.unique(y[tr])) < len(np.unique(y)) for tr, _ in splits):
        return _model().fit(X, y), False
    cal = CalibratedClassifierCV(_model(), method="sigmoid", cv=splits)
    return cal.fit(X, y), True


def cross_validate(X, y, groups, outer_folds: int = 5, inner_folds: int = 3):
    """Out-of-fold class probabilities for labelled trees, by spatial block."""
    from sklearn.model_selection import GroupKFold
    classes = np.unique(y)
    n_groups = len(np.unique(groups))
    k = min(outer_folds, n_groups)
    if k < 2:
        raise ValueError("need labelled trees in at least 2 spatial blocks for cross-validation")
    proba = np.full((len(y), len(classes)), np.nan)
    calibrated_folds = 0
    for tr, te in GroupKFold(n_splits=k).split(X, y, groups):
        if len(np.unique(y[tr])) < 2:
            continue  # a fold whose training side has a single class cannot be scored
        model, calibrated = _fit_calibrated(X[tr], y[tr], groups[tr], inner_folds)
        calibrated_folds += calibrated
        p = model.predict_proba(X[te])
        for j, c in enumerate(model.classes_):
            proba[te, np.searchsorted(classes, c)] = p[:, j]
    proba = np.nan_to_num(proba, nan=0.0)
    return classes, proba, {"outer_folds": k, "calibrated_folds": int(calibrated_folds)}


def coverage_table(y, classes, proba, thresholds=None) -> list[dict]:
    """Accuracy on labelled-as-known trees and coverage at each threshold."""
    if thresholds is None:
        thresholds = np.round(np.arange(0.3, 1.0, 0.05), 2)
    pred = classes[proba.argmax(axis=1)]
    top = proba.max(axis=1)
    rows = []
    for t in thresholds:
        known = top >= t
        n = int(known.sum())
        rows.append({"threshold": float(t), "coverage": round(n / len(y), 4) if len(y) else 0.0,
                     "accuracy": round(float((pred[known] == y[known]).mean()), 4) if n else None,
                     "n_labelled": n})
    return rows


def pick_threshold(table, min_accuracy=0.85, min_coverage=0.70):
    """Lowest threshold meeting min_accuracy, so coverage is as high as possible."""
    ok = [r for r in table if r["accuracy"] is not None and r["accuracy"] >= min_accuracy]
    if not ok:
        return None
    best = min(ok, key=lambda r: r["threshold"])
    return {**best, "meets_coverage_target": best["coverage"] >= min_coverage}


def calibration_table(y, classes, proba, bins: int = 5) -> list[dict]:
    pred = classes[proba.argmax(axis=1)]
    top = proba.max(axis=1)
    edges = np.linspace(1.0 / len(classes), 1.0, bins + 1)
    rows = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        m = (top >= lo) & ((top < hi) | (hi == 1.0))
        if m.any():
            rows.append({"confidence_from": round(float(lo), 3), "confidence_to": round(float(hi), 3),
                         "n": int(m.sum()), "mean_confidence": round(float(top[m].mean()), 4),
                         "observed_accuracy": round(float((pred[m] == y[m]).mean()), 4)})
    return rows


def per_class(y, pred, classes) -> dict:
    out = {}
    for c in classes:
        tp = int(((pred == c) & (y == c)).sum())
        fp = int(((pred == c) & (y != c)).sum())
        fn = int(((pred != c) & (y == c)).sum())
        out[str(c)] = {"n": int((y == c).sum()),
                       "precision": round(tp / (tp + fp), 4) if tp + fp else None,
                       "recall": round(tp / (tp + fn), 4) if tp + fn else None}
    return out


def evaluate(y, classes, proba, min_accuracy=0.85, min_coverage=0.70) -> dict:
    pred = classes[proba.argmax(axis=1)]
    table = coverage_table(y, classes, proba)
    return {
        "n_labelled": int(len(y)),
        "accuracy_all": round(float((pred == y).mean()), 4),
        "per_class": per_class(y, pred, classes),
        "confusion": {str(a): {str(b): int(((y == a) & (pred == b)).sum()) for b in classes}
                      for a in classes},
        "coverage_table": table,
        "chosen": pick_threshold(table, min_accuracy, min_coverage),
        "calibration": calibration_table(y, classes, proba),
    }


def run(trees_csv, features_csv, labels_csv, out_dir, block_m=50.0,
        min_accuracy=0.85, min_coverage=0.70) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    ids, xy, X, fnames, y_all = load_table(trees_csv, features_csv, labels_csv)
    labelled = y_all != ""
    if labelled.sum() < 10:
        raise ValueError("need at least 10 labelled trees")
    classes_present = np.unique(y_all[labelled])
    if len(classes_present) < 2:
        raise ValueError("need at least 2 label classes")
    # Features with no values among labelled trees (e.g. colour without RGB) carry
    # no information and break histogram binning, so drop them.
    usable = np.isfinite(X[labelled]).any(axis=0)
    dropped = [f for f, u in zip(fnames, usable) if not u]
    X, fnames = X[:, usable], [f for f, u in zip(fnames, usable) if u]
    blocks = spatial_blocks(xy, block_m)
    Xl, yl, gl = X[labelled], y_all[labelled], blocks[labelled]

    classes, oof, cv_info = cross_validate(Xl, yl, gl)
    report = {"features": fnames, "dropped_empty_features": dropped, "block_m": block_m,
              "labelled_blocks": int(len(np.unique(gl))), **cv_info,
              "targets": {"min_accuracy": min_accuracy, "min_coverage": min_coverage},
              "cross_validation": evaluate(yl, classes, oof, min_accuracy, min_coverage)}

    chosen = report["cross_validation"]["chosen"]
    threshold = chosen["threshold"] if chosen else 1.01  # nothing confident enough: all unknown
    report["threshold_used"] = threshold

    model, _ = _fit_calibrated(Xl, yl, gl, inner_folds=3)
    proba = model.predict_proba(X)
    model_classes = np.asarray(model.classes_)
    top = proba.max(axis=1)
    best = model_classes[proba.argmax(axis=1)]
    with open(out / "predictions.csv", "w", newline="") as fh:
        cols = ["tree_id", "predicted", "confidence", "label", "is_labelled", "block"] + \
               [f"p_{c}" for c in model_classes]
        w = csv.writer(fh)
        w.writerow(cols)
        for i, tid in enumerate(ids):
            pred = best[i] if top[i] >= threshold else UNKNOWN
            w.writerow([tid, pred, round(float(top[i]), 4), y_all[i], int(labelled[i]), int(blocks[i])]
                       + [round(float(p), 4) for p in proba[i]])
    (out / "classify_report.json").write_text(json.dumps(report, indent=2))
    return report
