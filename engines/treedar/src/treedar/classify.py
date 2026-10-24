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


def read_label_map(path) -> dict[str, str]:
    """CSV with columns label,type mapping field labels (e.g. species) to the
    classes the classifier trains on (e.g. conifer, broadleaf). Matching
    ignores case and surrounding spaces. A blank type means "not a usable
    label" (e.g. not sure): such trees are treated as unlabelled."""
    out = {}
    for r in _read_csv(path):
        key = (r.get("label") or "").strip().lower()
        if key:
            out[key] = (r.get("type") or "").strip()
    if not out:
        raise ValueError(f"label map has no rows: {path}")
    return out


def load_table(trees_csv, features_csv, labels_csv=None, label_map=None):
    """Join trees, features and optional labels on tree_id.

    Returns (ids, xy, X, feature_names, labels, groups) where labels holds the
    label string per tree ("" when unlabelled) and groups the optional "group"
    column of the labels file ("" when absent), e.g. the named area a label
    came from. With `label_map` (from read_label_map), every label is replaced
    by its mapped type; a label missing from the map is an error, so a new
    species is never silently trained as its own class.
    """
    trees = {r["tree_id"]: r for r in _read_csv(trees_csv)}
    feats = {r["tree_id"]: r for r in _read_csv(features_csv)}
    labels, label_groups = {}, {}
    unmapped: set[str] = set()
    if labels_csv is not None:
        for r in _read_csv(labels_csv):
            lab = (r.get("label") or "").strip()
            if lab and label_map is not None:
                if lab.lower() not in label_map:
                    unmapped.add(lab)
                    continue
                lab = label_map[lab.lower()]
            if lab:
                labels[r["tree_id"]] = lab
                label_groups[r["tree_id"]] = (r.get("group") or "").strip()
    if unmapped:
        raise ValueError(f"labels missing from the label map: {', '.join(sorted(unmapped))}")
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
    groups = np.array([label_groups.get(t, "") for t in ids], dtype=object)
    return ids, xy, X, fnames, y, groups


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
    """Out-of-fold class probabilities for labelled trees, by spatial block.

    Returns (classes, proba, scored, info). A fold whose training side has a
    single class cannot be scored: its trees are False in `scored`, their
    probability rows are zero and must be left out of any metric. `info`
    lists the skipped folds' groups.
    """
    from sklearn.model_selection import GroupKFold
    classes = np.unique(y)
    n_groups = len(np.unique(groups))
    k = min(outer_folds, n_groups)
    if k < 2:
        raise ValueError("need labelled trees in at least 2 spatial blocks for cross-validation")
    proba = np.zeros((len(y), len(classes)))
    scored = np.zeros(len(y), dtype=bool)
    calibrated_folds, skipped = 0, []
    for tr, te in GroupKFold(n_splits=k).split(X, y, groups):
        if len(np.unique(y[tr])) < 2:
            skipped.append({"groups": np.unique(groups[te]).tolist(), "n": int(len(te)),
                            "train_classes": [str(c) for c in np.unique(y[tr])]})
            continue
        model, calibrated = _fit_calibrated(X[tr], y[tr], groups[tr], inner_folds)
        calibrated_folds += calibrated
        proba[te] = _align(model, classes, model.predict_proba(X[te]))
        scored[te] = True
    return classes, proba, scored, {"outer_folds": k, "calibrated_folds": int(calibrated_folds),
                                    "skipped_folds": skipped}


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


def _align(model, classes, proba):
    """Reorder predict_proba columns to `classes` (zeros for classes the model lacks)."""
    out = np.zeros((proba.shape[0], len(classes)))
    for j, c in enumerate(model.classes_):
        out[:, np.searchsorted(classes, c)] = proba[:, j]
    return out


def final_test_split(y, groups, fraction=0.3, seed=0, attempts=200):
    """Boolean mask of labelled trees in held-out final test groups, or None.

    Whole groups go to the test set. Every class must appear in at least one
    test group and in at least two development groups (so development
    cross-validation can run). Returns None when no split satisfies this.
    """
    if fraction <= 0:
        return None
    uniq = np.unique(groups)
    rng = np.random.default_rng(seed)
    n_test = max(1, int(round(fraction * len(uniq))))
    for _ in range(attempts):
        test_groups = rng.choice(uniq, size=n_test, replace=False)
        test = np.isin(groups, test_groups)
        ok = True
        for c in np.unique(y):
            if not (test & (y == c)).any() or len(np.unique(groups[~test & (y == c)])) < 2:
                ok = False
                break
        if ok and len(np.unique(groups[~test])) >= 2:
            return test
    return None


def at_threshold(y, classes, proba, threshold) -> dict:
    pred = classes[proba.argmax(axis=1)]
    known = proba.max(axis=1) >= threshold
    n = int(known.sum())
    return {"threshold": float(threshold), "n": int(len(y)), "n_labelled": n,
            "coverage": round(n / len(y), 4) if len(y) else 0.0,
            "accuracy": round(float((pred[known] == y[known]).mean()), 4) if n else None}


def _write_predictions(path, ids, classes, proba, y, threshold):
    top = proba.max(axis=1)
    best = classes[proba.argmax(axis=1)]
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["tree_id", "predicted", "confidence", "label"] + [f"p_{c}" for c in classes])
        for i, tid in enumerate(ids):
            pred = best[i] if top[i] >= threshold else UNKNOWN
            w.writerow([tid, pred, round(float(top[i]), 4), y[i]] + [round(float(v), 4) for v in proba[i]])


def run(trees_csv, features_csv, labels_csv, out_dir, block_m=50.0,
        min_accuracy=0.85, min_coverage=0.70, test_fraction=0.3, seed=0,
        label_map=None) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    mapping = read_label_map(label_map) if label_map is not None else None
    ids, xy, X, fnames, y_all, label_groups = load_table(trees_csv, features_csv, labels_csv,
                                                          mapping)
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
    # Labels given per named area are held out a whole area at a time; otherwise
    # trees are grouped by square spatial blocks.
    use_label_groups = bool(labelled.any() and all(label_groups[labelled]))
    group_names = None
    if use_label_groups:
        group_names, groups = np.unique(np.where(labelled, label_groups, "~unlabelled"),
                                        return_inverse=True)
    else:
        groups = blocks
    Xl, yl, gl = X[labelled], y_all[labelled], groups[labelled]
    test = final_test_split(yl, gl, test_fraction, seed)

    report = {"features": fnames, "dropped_empty_features": dropped,
              "grouping": "label group column" if use_label_groups else f"{block_m} m blocks",
              "block_m": block_m, "labelled_blocks": int(len(np.unique(gl))),
              "targets": {"min_accuracy": min_accuracy, "min_coverage": min_coverage},
              "label_map": Path(label_map).name if label_map is not None else None}
    dev = ~test if test is not None else np.ones(len(yl), dtype=bool)
    report["development_groups"] = int(len(np.unique(gl[dev])))
    report["final_test_groups"] = int(len(np.unique(gl[test]))) if test is not None else 0

    # Development: cross-validation on development groups only; the threshold is
    # chosen here and never on the final test groups.
    classes, oof, scored, cv_info = cross_validate(Xl[dev], yl[dev], gl[dev])
    for fold in cv_info["skipped_folds"]:
        fold["groups"] = [str(group_names[g]) if group_names is not None else int(g)
                          for g in fold["groups"]]
    report.update(cv_info)
    y_dev = yl[dev]
    if scored.any():
        cv = evaluate(y_dev[scored], classes, oof[scored], min_accuracy, min_coverage)
    else:
        # Nothing was tested, so there is no accuracy and no threshold to choose.
        cv = {"n_labelled": 0, "accuracy_all": None,
              "per_class": {str(c): {"n": 0} for c in classes},
              "confusion": {str(a): {str(b): 0 for b in classes} for a in classes},
              "coverage_table": [], "chosen": None, "calibration": []}
    cv["n_unscored"] = int((~scored).sum())
    untested = []
    for c in classes:
        stats = cv["per_class"][str(c)]
        stats["n_labelled"] = int((y_dev == c).sum())
        stats["n_scored"] = int((y_dev[scored] == c).sum())
        if stats["n_scored"] == 0:
            stats["precision"] = stats["recall"] = None
            untested.append(str(c))
    if untested:
        cv["note"] = (f"not tested: {', '.join(untested)}. A class whose labels are all in one "
                      "group can't be held out and still trained on; each class needs labels in "
                      "2 or more groups")
    report["cross_validation"] = cv
    chosen = cv["chosen"]
    threshold = chosen["threshold"] if chosen else 1.01  # nothing confident enough: all unknown
    report["threshold_used"] = threshold

    # Final test: model trained on development groups, scored once on held-out groups.
    if test is not None:
        dev_model, _ = _fit_calibrated(Xl[dev], yl[dev], gl[dev], inner_folds=3)
        p_test = _align(dev_model, classes, dev_model.predict_proba(Xl[test]))
        report["final_test"] = {**evaluate(yl[test], classes, p_test, min_accuracy, min_coverage),
                                "at_threshold": at_threshold(yl[test], classes, p_test, threshold)}
        test_ids = np.asarray(ids, dtype=object)[labelled][test]
        _write_predictions(out / "final_test_predictions.csv", test_ids, classes, p_test,
                           yl[test], threshold)
    else:
        report["final_test"] = None
        report["final_test_note"] = ("not enough labelled groups for an independent test set: each "
                                     "class needs groups on both sides; results are development only")

    # Map predictions: model trained on every labelled tree, development threshold.
    model, _ = _fit_calibrated(Xl, yl, gl, inner_folds=3)
    proba = _align(model, classes, model.predict_proba(X))
    partition = np.full(len(ids), "", dtype=object)
    partition[np.flatnonzero(labelled)[dev]] = "development"
    if test is not None:
        partition[np.flatnonzero(labelled)[test]] = "final_test"
    top = proba.max(axis=1)
    best = classes[proba.argmax(axis=1)]
    with open(out / "predictions.csv", "w", newline="") as fh:
        cols = ["tree_id", "predicted", "confidence", "label", "is_labelled", "partition",
                "cv_group"] + [f"p_{c}" for c in classes]
        w = csv.writer(fh)
        w.writerow(cols)
        for i, tid in enumerate(ids):
            pred = best[i] if top[i] >= threshold else UNKNOWN
            w.writerow([tid, pred, round(float(top[i]), 4), y_all[i], int(labelled[i]),
                        partition[i], int(groups[i])] + [round(float(v), 4) for v in proba[i]])
    (out / "classify_report.json").write_text(json.dumps(report, indent=2))
    return report
