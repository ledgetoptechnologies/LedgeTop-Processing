"""Real point-file regressions for chunking, spatial selection and LAS 1.4."""

import laspy
import numpy as np
import pytest

from treedar import io, qc
from treedar.grid import Grid
from synthetic_forest import write_forest


def test_bbox_and_decimation_agree_across_chunk_boundaries(tmp_path, monkeypatch):
    source = tmp_path / "forest.laz"
    write_forest(source)
    cloud = laspy.read(source)
    x, y = np.asarray(cloud.x), np.asarray(cloud.y)
    selected = (x >= 7) & (x <= 25) & (y >= 7) & (y <= 25)
    monkeypatch.setattr(io, "CHUNK", 137)
    actual = io.read_points(source, bbox=(7, 7, 25, 25), step=7)
    np.testing.assert_array_equal(actual["x"], x[selected][::7])
    np.testing.assert_array_equal(actual["y"], y[selected][::7])
    np.testing.assert_array_equal(actual["z"], np.asarray(cloud.z)[selected][::7])
    assert all(len(values) == len(actual["x"]) for values in actual.values())


def test_bbox_outside_cloud_fails(tmp_path):
    source = tmp_path / "forest.laz"
    write_forest(source)
    with pytest.raises(ValueError, match="no points"):
        io.read_points(source, bbox=(100, 100, 110, 110))


def test_las14_extended_counts_and_rgb(tmp_path):
    source = tmp_path / "forest.laz"
    write_forest(source)
    original = laspy.read(source)
    modern = laspy.convert(original, point_format_id=7, file_version="1.4")
    modern.red[:] = 4000
    modern.green[:] = 8000
    modern.blue[:] = 12000
    modern.return_number[0:100] = 2
    modern.number_of_returns[0:100] = 2
    modern.write(source)
    # LAS 1.4 PF7 writes extended point/return counts; legacy counts are zero.
    report = qc.qc_report(source)
    assert report["las_version"] == "1.4"
    assert report["point_format"] == 7
    assert report["point_count"] == len(modern.points)
    assert report["return_number_counts"] == {"1": len(modern.points) - 100, "2": 100}
    points = io.read_points(source)
    for name, value in (("red", 4000), ("green", 8000), ("blue", 12000)):
        assert np.all(points[name] == value)


def test_grid_excludes_points_just_outside_left_and_top():
    grid = Grid.from_bounds(0, 0, 2, 2, 1)
    indices = grid.flat_index(np.array([-0.01, 0.5]), np.array([1.5, 2.01]))
    np.testing.assert_array_equal(indices, [-1, -1])


def test_grid_does_not_pad_nonboundary_extent():
    grid = Grid.from_bounds(0.25, 0.25, 31.75, 31.75, 0.5)
    assert grid.shape == (64, 64)
