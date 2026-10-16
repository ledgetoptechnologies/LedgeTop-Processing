"""Synthetic spatial classification contracts, without project or field data."""

import csv
import json

import numpy as np
import pytest
from sklearn.model_selection import GroupKFold
from threadpoolctl import threadpool_limits

from treedar.classify import run, spatial_blocks


def _write_csv(path, rows):
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture(scope="module")
def synthetic_tables(tmp_path_factory):
    """Two noisy tree types over a 400 m square, with half field-labelled."""
    directory = tmp_path_factory.mktemp("synthetic_classify")
    rng = np.random.default_rng(42)
    xy = rng.uniform(0, 400, size=(600, 2))
    # Spatial correlation alone is insufficient: classes overlap across blocks.
    broadleaf = rng.random(600) < (0.15 + 0.70 * xy[:, 0] / 400)
    height = 18 + 5 * broadleaf + rng.normal(0, 3, 600)
    gcc = 0.32 + 0.10 * broadleaf + rng.normal(0, 0.05, 600)
    ids = [str(i + 1) for i in range(600)]
    trees = [
        dict(tree_id=tid, x=xy[i, 0], y=xy[i, 1], height_m=height[i],
             crown_area_m2=20 + 0.7 * height[i], crown_diameter_m=5 + height[i] / 10)
        for i, tid in enumerate(ids)
    ]
    features = [dict(tree_id=tid, gcc_mean=gcc[i], empty_feature="", n_points=100)
                for i, tid in enumerate(ids)]
    selected = rng.choice(600, size=300, replace=False)
    labels = [dict(tree_id=ids[i], label="broadleaf" if broadleaf[i] else "conifer")
              for i in selected]
    paths = [directory / name for name in ("trees.csv", "features.csv", "labels.csv")]
    for path, rows in zip(paths, (trees, features, labels)):
        _write_csv(path, rows)
    return dict(paths=paths, ids=ids, labels=labels, xy=xy)


@pytest.fixture(scope="module")
def classified(synthetic_tables, tmp_path_factory):
    out = tmp_path_factory.mktemp("classified")
    # Bound native threads so this small synthetic fit stays cheap on large PCs.
    with threadpool_limits(limits=2):
        report = run(*synthetic_tables["paths"], out, block_m=50, min_accuracy=0.96)
    with (out / "predictions.csv").open(newline="") as fh:
        predictions = list(csv.DictReader(fh))
    return report, predictions, json.loads((out / "classify_report.json").read_text())


def test_classification_reports_spatial_evaluation_and_empty_feature(classified):
    report, _, saved_report = classified
    assert saved_report == report
    assert report["outer_folds"] >= 2
    assert report["labelled_blocks"] >= report["outer_folds"]
    evaluation = report["cross_validation"]
    assert evaluation["n_labelled"] == 300
    assert set(evaluation["per_class"]) == {"broadleaf", "conifer"}
    assert sum(row["n"] for row in evaluation["per_class"].values()) == 300
    coverage = evaluation["coverage_table"]
    assert len(coverage) >= 2
    assert all(a["threshold"] < b["threshold"] for a, b in zip(coverage, coverage[1:]))
    assert all(a["coverage"] >= b["coverage"] for a, b in zip(coverage, coverage[1:]))
    assert "empty_feature" in report["dropped_empty_features"]
    assert "empty_feature" not in report["features"]
    assert "gcc_mean" in report["features"]


def test_predictions_cover_every_tree_and_apply_unknown_threshold(classified, synthetic_tables):
    report, predictions, _ = classified
    assert len(predictions) == len(synthetic_tables["ids"])
    assert {row["tree_id"] for row in predictions} == set(synthetic_tables["ids"])
    labels = {row["tree_id"]: row["label"] for row in synthetic_tables["labels"]}
    threshold = report["threshold_used"]
    assert any(row["predicted"] == "unknown" for row in predictions)
    assert any(row["predicted"] != "unknown" for row in predictions)
    for row in predictions:
        confidence = float(row["confidence"])
        assert 0 <= confidence <= 1
        assert row["label"] == labels.get(row["tree_id"], "")
        assert int(row["is_labelled"]) == (row["tree_id"] in labels)
        assert row["predicted"] in {"unknown", "broadleaf", "conifer"}
        # CSV confidence is rounded to four decimals; a rounded boundary may
        # sit on either side of the exact probability used for abstention.
        if row["predicted"] == "unknown":
            assert confidence < threshold + 0.000051
        else:
            assert confidence >= threshold - 0.000051
            assert float(row[f"p_{row['predicted']}"]) == max(
                float(row["p_broadleaf"]), float(row["p_conifer"]))


def test_spatial_blocks_keep_known_squares_and_groupkfold_disjoint(synthetic_tables):
    xy = np.array([[0, 0], [5, 8], [49.9, 49.9], [50, 0], [51, 40],
                   [0, 50], [40, 51], [50, 50]])
    groups = spatial_blocks(xy, 50)
    assert groups[0] == groups[1] == groups[2]
    assert groups[3] == groups[4]
    assert groups[5] == groups[6]
    assert len({groups[0], groups[3], groups[5], groups[7]}) == 4
    groups = spatial_blocks(synthetic_tables["xy"], 50)
    seen = []
    for train, test in GroupKFold(n_splits=5).split(synthetic_tables["xy"], groups=groups):
        assert set(groups[train]).isdisjoint(groups[test])
        seen.extend(test)
    assert sorted(seen) == list(range(len(groups)))


@pytest.mark.parametrize(
    "case, message",
    [("few_labels", "at least 10 labelled"),
     ("one_class", "at least 2 label classes"),
     ("unknown_id", "labelled tree_ids are not in")],
)
def test_invalid_labels_are_rejected(case, message, synthetic_tables, tmp_path):
    labels = synthetic_tables["labels"]
    if case == "few_labels":
        labels = labels[:9]
    elif case == "one_class":
        labels = [dict(tree_id=row["tree_id"], label="conifer") for row in labels]
    else:
        labels = labels + [dict(tree_id="absent-tree", label="conifer")]
    labels_path = tmp_path / "labels.csv"
    _write_csv(labels_path, labels)
    trees, features, _ = synthetic_tables["paths"]
    with pytest.raises(ValueError, match=message):
        run(trees, features, labels_path, tmp_path / "out")
