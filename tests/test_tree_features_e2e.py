"""Known point attributes through LAZ input, crown assignment and CSV export."""

import csv

import laspy
import numpy as np
import pytest
import rasterio

from treedar.features import FEATURE_COLUMNS
from synthetic_forest import TREES, write_forest
from test_pipeline_e2e import run_pipeline


COLOUR_COLUMNS = (
    "red_mean", "green_mean", "blue_mean", "rcc_mean", "gcc_mean",
    "bcc_mean", "exg_mean", "gcc_std",
)


def csv_rows(path):
    with path.open(newline="") as stream:
        reader = csv.DictReader(stream)
        rows = list(reader)
        return reader.fieldnames, rows


def write_attributed_forest(path, *, rgb=True, zero_colour=False):
    write_forest(path)
    cloud = laspy.read(path)
    canopy = np.asarray(cloud.classification) == 5
    cloud.intensity[:] = np.where(canopy, 200, 1000).astype(np.uint16)
    cloud.return_number[:] = 1
    cloud.return_number[np.arange(len(cloud.points)) % 3 == 0] = 2
    cloud.number_of_returns[:] = 2
    # Bright ground colour must not contaminate upper-crown features.
    cloud.red[:] = np.where(canopy, 1000, 60000).astype(np.uint16)
    cloud.green[:] = np.where(canopy, 3000, 1).astype(np.uint16)
    cloud.blue[:] = np.where(canopy, 500, 1).astype(np.uint16)
    # A known subset of canopy returns has no camera colour at all.
    missing = canopy & ((np.arange(len(cloud.points)) % 5 == 0) | zero_colour)
    for name in ("red", "green", "blue"):
        cloud[name][missing] = 0
    if not rgb:
        cloud = laspy.convert(cloud, point_format_id=1, file_version="1.2")
    cloud.write(path)
    return cloud


def assert_structural_features(rows):
    assert len(rows) == len(TREES)
    for row in rows:
        assert int(row["n_points"]) >= int(row["n_upper_points"]) > 0
        p50, p75, p90 = (float(row[f"hag_p{p}_rel"]) for p in (50, 75, 90))
        assert 0 <= p50 <= p75 <= p90 <= 1
        assert float(row["hag_cv"]) >= 0
        assert float(row["intensity_mean"]) == pytest.approx(200)
        assert float(row["intensity_cv"]) == pytest.approx(0, abs=1e-4)
        assert 0 <= float(row["second_return_frac"]) <= 1


def test_known_upper_colour_and_return_fractions(tmp_path):
    source, out = tmp_path / "coloured.laz", tmp_path / "result"
    cloud = write_attributed_forest(source)
    run_pipeline(source, out)
    columns, rows = csv_rows(out / "tree_features.csv")
    _, trees = csv_rows(out / "trees.csv")
    assert columns == FEATURE_COLUMNS
    assert len(rows) == len(trees) == len({row["tree_id"] for row in rows})
    assert {row["tree_id"] for row in rows} == {row["tree_id"] for row in trees}
    assert_structural_features(rows)

    with rasterio.open(out / "crowns.tif") as crowns, rasterio.open(out / "dtm.tif") as dtm:
        rr, cc = rasterio.transform.rowcol(crowns.transform, cloud.x, cloud.y)
        rr, cc = np.asarray(rr), np.asarray(cc)
        point_labels = crowns.read(1)[rr, cc]
        heights = np.asarray(cloud.z) - dtm.read(1)[rr, cc]
    for row in rows:
        selected = point_labels == int(row["tree_id"])
        # Derive expected population directly from exported crown/terrain grids
        # and source attributes, independently of the feature reduction code.
        upper = selected & (heights >= heights[selected].max() * 0.5)
        coloured = upper & ((np.asarray(cloud.red) + np.asarray(cloud.green).astype(np.uint32)
                              + np.asarray(cloud.blue)) > 0)
        assert int(row["n_points"]) == selected.sum()
        assert int(row["n_upper_points"]) == upper.sum()
        assert int(row["n_colour_points"]) == coloured.sum()
        assert 0 < coloured.sum() < upper.sum()
        assert float(row["second_return_frac"]) == pytest.approx(
            np.mean(np.asarray(cloud.return_number)[selected] >= 2), abs=1e-4
        )
        ratios = [float(row[name]) for name in ("rcc_mean", "gcc_mean", "bcc_mean")]
        assert ratios == pytest.approx([1000 / 4500, 3000 / 4500, 500 / 4500], abs=1e-3)
        assert sum(ratios) == pytest.approx(1, abs=1e-3)
        assert [float(row[name]) for name in ("red_mean", "green_mean", "blue_mean")] == [1000, 3000, 500]
        assert float(row["gcc_std"]) == pytest.approx(0, abs=1e-4)


@pytest.mark.parametrize("rgb", [True, False], ids=["all-zero-upper-colour", "no-rgb-fields"])
def test_unavailable_colour_is_blank_but_structure_present(tmp_path, rgb):
    source, out = tmp_path / "no_colour.laz", tmp_path / "result"
    write_attributed_forest(source, rgb=rgb, zero_colour=True)
    run_pipeline(source, out)
    columns, rows = csv_rows(out / "tree_features.csv")
    assert columns == FEATURE_COLUMNS
    assert_structural_features(rows)
    for row in rows:
        assert all(row[name] == "" for name in COLOUR_COLUMNS)
        assert row["n_colour_points"] == ("0" if rgb else "")


def test_high_outlier_does_not_rescale_crown_features(tmp_path):
    clean_source, dirty_source = tmp_path / "clean.laz", tmp_path / "outlier.laz"
    original = write_attributed_forest(clean_source)
    # Duplicate a real upper-crown return one metre away from the apex and
    # lift it by 20 m. Ground and all legitimate crown observations survive.
    candidate = np.flatnonzero(
        (np.asarray(original.classification) == 5)
        & np.isclose(np.asarray(original.x), TREES[0].x + 1)
        & np.isclose(np.asarray(original.y), TREES[0].y)
    )
    assert candidate.size == 1
    dirty = laspy.LasData(original.header.copy())
    dirty.points = laspy.ScaleAwarePointRecord.zeros(len(original.points) + 1, header=dirty.header)
    dirty.points.array[:-1] = original.points.array
    index = int(candidate[0])
    dirty.points.array[-1:] = original.points.array[index:index + 1]
    dirty.z[-1] += 20
    dirty.write(dirty_source)

    clean_out, dirty_out = tmp_path / "clean_result", tmp_path / "dirty_result"
    run_pipeline(clean_source, clean_out)
    summary = run_pipeline(dirty_source, dirty_out)
    assert summary["chm_filter"]["spike_cells_lowered"] >= 1
    _, clean = csv_rows(clean_out / "tree_features.csv")
    _, contaminated = csv_rows(dirty_out / "tree_features.csv")
    by_id = {row["tree_id"]: row for row in contaminated}
    assert set(by_id) == {row["tree_id"] for row in clean}
    for row in clean:
        other = by_id[row["tree_id"]]
        for name in ("n_points", "n_upper_points", "n_colour_points"):
            assert other[name] == row[name]
        for name in ("hag_p50_rel", "hag_p75_rel", "hag_p90_rel", "rcc_mean", "gcc_mean", "bcc_mean"):
            assert float(other[name]) == pytest.approx(float(row[name]), abs=1e-4)
