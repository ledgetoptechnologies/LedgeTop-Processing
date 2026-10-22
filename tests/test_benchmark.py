"""Analytic benchmark truth; all point clouds are generated locally."""

import csv
import json

import laspy
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from treedar import benchmark, cli, io


def write_reference(path, rows, *, version="1.2", tree_field=True):
    header = laspy.LasHeader(point_format=3 if version == "1.2" else 6, version=version)
    header.scales = [0.001] * 3
    if tree_field:
        header.add_extra_dim(laspy.ExtraBytesParams(name="treeID", type=np.uint32))
    cloud = laspy.LasData(header)
    rows = np.asarray(rows)
    cloud.x, cloud.y, cloud.z = rows[:, 0], rows[:, 1], rows[:, 2]
    cloud.classification = rows[:, 4].astype(np.uint8)
    if tree_field:
        cloud.treeID = rows[:, 3].astype(np.uint32)
    cloud.write(path)
    return path


def test_match_optimizes_one_to_one_instead_of_greedy():
    # Ref 0 is slightly closer to det 0, but taking it strands ref 1.
    ref = np.array([[0, 0, 10], [1, 0, 10]], float)
    det = np.array([[0.4, 0, 10], [-0.6, 0, 10]], float)
    pairs = benchmark.match(ref, det, max_dist_m=1)
    assert set(map(tuple, pairs)) == {(0, 1), (1, 0)}
    assert len(np.unique(pairs[:, 0])) == len(np.unique(pairs[:, 1])) == 2


def test_match_distance_and_height_limits_are_inclusive():
    ref = np.array([[0, 0, 10]], float)
    assert benchmark.match(ref, np.array([[1.5, 0, 13.0]])).tolist() == [[0, 0]]
    assert benchmark.match(ref, np.array([[1.501, 0, 10]])).shape == (0, 2)
    assert benchmark.match(ref, np.array([[0, 0, 13.001]])).shape == (0, 2)
    assert benchmark.match(np.empty((0, 3)), ref).shape == (0, 2)
    assert benchmark.match(ref, np.empty((0, 3))).shape == (0, 2)


def test_score_metrics_height_errors_and_half_open_bins():
    ref = np.array([[0, 0, h] for h in [4, 5, 10, 20, 25]], float)
    det = np.array([[0, 0, h] for h in [4, 6, 8, 22, 30, 35]], float)
    pairs = np.array([[0, 0], [1, 1], [2, 2], [3, 3]])
    result = benchmark.score(ref, det, pairs)
    assert result["n_reference"] == 4
    assert result["n_detected"] == 5
    assert result["recall"] == 0.75
    assert result["precision"] == 0.6
    assert result["f1"] == 0.6667
    assert result["height_error_median_m"] == 1
    assert result["height_error_abs_p95_m"] == 2
    assert result["recall_by_height"] == [
        {"height_m": "5-10", "n_reference": 1, "recall": 1.0},
        {"height_m": "10-20", "n_reference": 1, "recall": 1.0},
        {"height_m": "20-", "n_reference": 2, "recall": 0.5},
    ]
    empty = benchmark.score(np.empty((0, 3)), np.empty((0, 3)), np.empty((0, 2), int))
    assert empty["recall"] is empty["precision"] is empty["f1"] is None
    assert empty["height_error_median_m"] is None


def test_score_no_matches_has_zero_f1():
    points = np.array([[0, 0, 10]], float)
    result = benchmark.score(points, points, np.empty((0, 2), int))
    assert result["recall"] == result["precision"] == 0
    assert result["f1"] == 0


def test_score_discards_pairs_with_either_member_below_threshold():
    ref = np.array([[0, 0, 9], [1, 0, 4]], float)
    det = np.array([[0, 0, 4], [1, 0, 9]], float)
    result = benchmark.score(ref, det, np.array([[0, 0], [1, 1]]))
    assert result["recall"] == result["precision"] == result["f1"] == 0
    assert result["height_error_median_m"] is None


@pytest.mark.parametrize("empty_reference", [True, False])
def test_score_one_empty_population_has_undefined_f1(empty_reference):
    nonempty, empty = np.array([[0, 0, 10]], float), np.empty((0, 3))
    ref, det = (empty, nonempty) if empty_reference else (nonempty, empty)
    result = benchmark.score(ref, det, np.empty((0, 2), int))
    assert result["f1"] is None
    assert result["recall" if empty_reference else "precision"] is None
    assert result["precision" if empty_reference else "recall"] == 0


def test_reference_class_exclusion_is_per_tree_and_aggregated_across_chunks(tmp_path, monkeypatch):
    rows = [(0, 0, 100, 0, 2)]
    rows += [(1, 1, 110 + i, 7, 3 if i < 6 else 5) for i in range(10)]
    rows += [(2, 1, 110 + i, 8, 3 if i < 5 else 5) for i in range(10)]
    reference = write_reference(tmp_path / "class-exclusion.las", rows)
    monkeypatch.setattr(io, "CHUNK", 3)
    assert set(benchmark.reference_tops(reference)[:, 0]) == {7, 8}
    assert benchmark.reference_tops(reference, exclude_class=3)[:, 0].tolist() == [8]


def test_evaluate_run_filters_short_trees_before_matching(tmp_path):
    reference, run = benchmark_scene(tmp_path, "1.2")
    # Reference is 9 m tall. A closer 4 m detection must not consume its match
    # when the scored population is >= 5 m and a valid 10 m detection exists.
    result = benchmark.evaluate_run(reference, run, max_dist_m=1.5, max_dz_m=6,
                                    detections=np.array([[0.5, 0.5, 4], [0.6, 0.5, 10]]))
    assert result["n_reference"] == result["n_detected"] == 1
    assert result["recall"] == result["precision"] == result["f1"] == 1
    assert result["height_error_median_m"] == 1


@pytest.mark.parametrize("version", ["1.2", "1.4"])
def test_reference_tops_highest_point_counts_across_chunks(tmp_path, monkeypatch, version):
    reference = write_reference(tmp_path / "reference.las", [
        (0, 0, 100, 0, 2), (9, 9, 150, 0, 5),
        (1, 1, 110, 7, 5), (2, 1, 113, 7, 5),
        (3, 1, 112, 7, 5), (4, 1, 115, 8, 5),
    ], version=version)
    monkeypatch.setattr(io, "CHUNK", 2)
    tops = benchmark.reference_tops(reference, min_points=3)
    np.testing.assert_allclose(tops, [[7, 2, 1, 13]])
    assert benchmark.reference_tops(reference, min_points=4).shape == (0, 4)
    tops = benchmark.reference_tops(reference, min_points=3, dtm=np.array([[102.0]]),
                                     transform=from_origin(0, 10, 10, 10))
    np.testing.assert_allclose(tops, [[7, 2, 1, 11]])


def test_reference_tops_rejects_missing_id_field(tmp_path):
    reference = write_reference(tmp_path / "missing.las", [(0, 0, 0, 0, 2)], tree_field=False)
    with pytest.raises(ValueError, match="has no 'treeID' field"):
        benchmark.reference_tops(reference)


def benchmark_scene(tmp_path, version):
    # Left cell is annotated; right cell is majority outside-plot class 3.
    rows = [(0.5, 0.5, 100 + i, 1, 5) for i in range(10)]
    rows += [(1.5, 0.5, 112, 0, 3)] * 10
    rows += [(1.5, 0.5, 100, 0, 2)] * 2
    reference = write_reference(tmp_path / "reference.las", rows, version=version)
    run = tmp_path / "run"
    run.mkdir()
    with rasterio.open(run / "dtm.tif", "w", driver="GTiff", width=2, height=1,
                       count=1, dtype="float32", transform=from_origin(0, 1, 1, 1)) as ds:
        ds.write(np.full((1, 2), 100, np.float32), 1)
    with (run / "trees.csv").open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["x", "y", "height_m"])
        writer.writerows([(0.5, 0.5, 10), (1.5, 0.5, 12)])
    return reference, run


@pytest.mark.parametrize("version", ["1.2", "1.4"])
def test_evaluate_run_excludes_class3_detections_and_persists_json(tmp_path, version):
    reference, run = benchmark_scene(tmp_path, version)
    result = benchmark.evaluate_run(reference, run)
    assert result["reference"] == "reference.las"
    assert result["detections_outside_plot_dropped"] == 1
    assert result["n_reference"] == result["n_detected"] == 1
    assert result["recall"] == result["precision"] == result["f1"] == 1
    assert result["height_error_median_m"] == 1
    assert json.loads((run / "benchmark.json").read_text()) == result
    result = benchmark.evaluate_run(reference, run, exclude_class=None)
    assert result["detections_outside_plot_dropped"] == 0
    assert result["n_detected"] == 2
    assert result["precision"] == 0.5
    result = benchmark.evaluate_run(reference, run, detections=np.array([[0.5, 0.5, 9]]))
    assert result["n_detected"] == 1
    assert result["height_error_abs_p95_m"] == 0


def test_benchmark_cli_writes_json(tmp_path, capsys):
    reference, run = benchmark_scene(tmp_path, "1.2")
    assert cli.main(["benchmark", str(reference), str(run), "--max-dist", "0.25",
                     "--max-dz", "2", "--min-height", "6"]) == 0
    result = json.loads((run / "benchmark.json").read_text())
    assert result["max_dist_m"] == 0.25
    assert result["max_dz_m"] == 2
    assert result["min_height_m"] == 6
    assert result["f1"] == 1
    assert "benchmark.json" in capsys.readouterr().out
