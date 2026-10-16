"""Real CLI/LAZ/GeoTIFF tests; synthetic recovery is not field accuracy."""

import csv
import json
import subprocess
import sys

import laspy
import numpy as np
import pytest
import rasterio
from scipy.optimize import linear_sum_assignment

from synthetic_forest import TREES, terrain_z, write_forest


def cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "treedar", *map(str, args)],
        capture_output=True, text=True, timeout=60,
    )


def run_pipeline(source, out, *args):
    result = cli("run", source, "--out", out, "--cell", "0.5", *args)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads((out / "summary.json").read_text())


@pytest.mark.parametrize("classified", [True, False], ids=["classified", "unclassified"])
def test_synthetic_tree_recovery(tmp_path, classified):
    source, out = tmp_path / "forest.laz", tmp_path / "result"
    write_forest(source, classified=classified)
    summary = run_pipeline(source, out)
    with (out / "trees.csv").open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert summary["n_trees"] == len(rows) == len(TREES)
    positions = np.array([[float(row["x"]), float(row["y"])] for row in rows])
    expected = np.array([[tree.x, tree.y] for tree in TREES])
    distances = np.linalg.norm(expected[:, None] - positions[None, :], axis=2)
    truth_ids, prediction_ids = linear_sum_assignment(distances)
    assert np.all(distances[truth_ids, prediction_ids] <= 1.5)
    errors = [abs(float(rows[p]["height_m"]) - TREES[t].height)
              for t, p in zip(truth_ids, prediction_ids)]
    assert np.median(errors) <= 0.5
    assert max(errors) <= 1.0
    assert all(float(row["crown_area_m2"]) > 0 for row in rows)

    with rasterio.open(out / "dtm.tif") as dtm_file:
        dtm = dtm_file.read(1)
        assert dtm_file.crs.to_epsg() == 32616
        assert dtm_file.transform.a == 0.5
        assert dtm_file.transform.e == -0.5
        rr, cc = np.indices(dtm.shape)
        xx, yy = rasterio.transform.xy(dtm_file.transform, rr, cc)
        xx, yy = np.asarray(xx).reshape(dtm.shape), np.asarray(yy).reshape(dtm.shape)
        expected_ground = terrain_z(xx, yy)
        assert np.all(np.isfinite(dtm))
        assert np.median(np.abs(dtm - expected_ground)) <= 0.15
        assert np.max(np.abs(dtm - expected_ground)) <= 0.5
        with rasterio.open(out / "chm.tif") as chm_file, rasterio.open(out / "crowns.tif") as crowns_file:
            assert chm_file.transform == crowns_file.transform == dtm_file.transform
            assert chm_file.shape == crowns_file.shape == dtm_file.shape
            assert chm_file.crs == crowns_file.crs == dtm_file.crs
            chm, labels = chm_file.read(1), crowns_file.read(1)
            assert np.isfinite(chm).all() and np.min(chm) >= 0
            assert set(np.unique(labels)) == {0, *(int(row["tree_id"]) for row in rows)}


def test_bare_ground_has_no_trees(tmp_path):
    source, out = tmp_path / "ground.laz", tmp_path / "result"
    write_forest(source, trees=())
    assert run_pipeline(source, out)["n_trees"] == 0
    with (out / "trees.csv").open(newline="") as stream:
        assert list(csv.DictReader(stream)) == []
    with (out / "tree_features.csv").open(newline="") as stream:
        assert list(csv.DictReader(stream)) == []
    with rasterio.open(out / "crowns.tif") as crowns:
        assert not crowns.read(1).any()


def test_qc_reads_real_laz_point_counts(tmp_path):
    source, out = tmp_path / "forest.laz", tmp_path / "qc"
    write_forest(source)
    result = cli("qc", source, "--out", out)
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads((out / "qc.json").read_text())
    cloud = laspy.read(source)
    assert report["point_count"] == len(cloud.points)
    assert report["crs_epsg"] == 32616
    assert report["point_source_count"] == 2
    assert report["return_number_counts"] == {"1": len(cloud.points)}
    assert report["classification_counts"]["2"] == 64 * 64
    assert report["classification_counts"]["5"] == len(cloud.points) - 64 * 64
    assert report["density"]["mean"] > 0
    assert report["density"]["mean"] == pytest.approx(report["first_return_density"]["mean"])


def test_missing_input_fails_without_success_summary(tmp_path):
    out = tmp_path / "result"
    result = cli("run", tmp_path / "missing.laz", "--out", out)
    assert result.returncode != 0
    assert result.stderr.strip() or result.stdout.strip()
    assert not (out / "summary.json").exists()


def test_no_qc_preserves_pipeline_outputs(tmp_path):
    source = tmp_path / "forest.laz"
    write_forest(source)
    baseline, skipped = tmp_path / "baseline", tmp_path / "skipped"
    normal = run_pipeline(source, baseline)
    without_qc = run_pipeline(source, skipped, "--no-qc")
    assert (baseline / "qc.json").is_file()
    assert not (skipped / "qc.json").exists()
    assert (baseline / "trees.csv").read_bytes() == (skipped / "trees.csv").read_bytes()
    assert (baseline / "tree_features.csv").read_bytes() == (skipped / "tree_features.csv").read_bytes()
    for name in ("dtm.tif", "chm.tif", "chm_raw.tif", "crowns.tif"):
        with rasterio.open(baseline / name) as a, rasterio.open(skipped / name) as b:
            np.testing.assert_array_equal(a.read(1), b.read(1))
            assert a.transform == b.transform and a.crs == b.crs
    for key in ("n_trees", "n_points_used", "params", "chm_filter", "version"):
        assert normal[key] == without_qc[key]


@pytest.mark.parametrize("option,value", [("--cell", "0"), ("--cell", "-1"), ("--step", "0")])
def test_invalid_sampling_parameters_fail(tmp_path, option, value):
    source, out = tmp_path / "forest.laz", tmp_path / "result"
    write_forest(source)
    result = cli("run", source, "--out", out, option, value)
    assert result.returncode != 0
    assert not (out / "summary.json").exists()
