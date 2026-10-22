"""Real LAZ fallback tests and geometry-only COPC reader doubles.

These doubles do not prove COPC decoding or octree query correctness.
"""

from types import SimpleNamespace

import laspy
import numpy as np
import pytest

from treedar import io
from synthetic_forest import write_forest
from test_pipeline_e2e import run_pipeline


def test_non_copc_bbox_scan_returns_exact_selected_records(tmp_path, monkeypatch):
    source = tmp_path / "ordinary.laz"
    write_forest(source)
    with pytest.raises(laspy.errors.LaspyException):
        laspy.CopcReader.open(source)
    cloud = laspy.read(source)
    x, y = np.asarray(cloud.x), np.asarray(cloud.y)
    keep = (x >= 6) & (x <= 12) & (y >= 6) & (y <= 12)
    monkeypatch.setattr(io, "CHUNK", 137)
    points = io.read_points(source, bbox=(6, 6, 12, 12), step=3)
    for name in ("x", "y", "z", "classification", "point_source_id"):
        np.testing.assert_array_equal(points[name], np.asarray(cloud[name])[keep][::3])


def test_non_copc_bbox_pipeline_finds_only_selected_tree(tmp_path):
    source = tmp_path / "ordinary.laz"
    write_forest(source)
    summary = run_pipeline(source, tmp_path / "result", "--bbox", "0,0,16,16")
    assert summary["n_trees"] == 1
    assert summary["n_points_used"] < len(laspy.read(source).points)


def cube_reader(*, inconsistent=False, no_entries=False, degenerate=False):
    """A declared 8 m cube, with one level-2 node at voxel (1, 2, 0)."""
    node = SimpleNamespace(
        point_count=2, key=SimpleNamespace(level=2, x=1, y=2, z=0)
    )
    points = SimpleNamespace(
        x=np.array([2.25, 7.0 if inconsistent else 3.75]),
        y=np.array([4.25, 5.75]), z=np.array([0.25, 1.75]),
    )
    return SimpleNamespace(
        copc_info=SimpleNamespace(halfsize=0.0, center=np.zeros(3)),
        header=SimpleNamespace(
            mins=np.zeros(3), maxs=np.zeros(3) if degenerate else np.array([8., 6., 4.]),
            scales=np.full(3, 0.001),
        ),
        root_page=SimpleNamespace(entries={} if no_entries else {"node": node}),
        _fetch_and_decompress_points_of_nodes=lambda nodes: points,
    )


def test_cube_repair_rejects_node_outside_reconstructed_voxel():
    assert io._repair_copc_cube(cube_reader(inconsistent=True)) is False


@pytest.mark.parametrize("option", ["no_entries", "degenerate"])
def test_cube_repair_rejects_unverifiable_geometry(option):
    assert io._repair_copc_cube(cube_reader(**{option: True})) is False


def test_cube_repair_accepts_consistent_sampled_voxel():
    reader = cube_reader()
    assert io._repair_copc_cube(reader) is True
    assert reader.copc_info.halfsize == 4.0
    np.testing.assert_array_equal(reader.copc_info.center, [4., 4., 4.])


def test_existing_positive_cube_is_preserved():
    reader = cube_reader()
    reader.copc_info.halfsize = 12.0
    reader.copc_info.center = np.array([10., 20., 30.])
    assert io._repair_copc_cube(reader) is True
    assert reader.copc_info.halfsize == 12.0
    np.testing.assert_array_equal(reader.copc_info.center, [10., 20., 30.])
