"""Analytic crown geometries through the public feature extraction API."""

from affine import Affine
import numpy as np
import pytest

from treedar.features import SHAPE_SAMPLE, tree_features


SPATIAL = ("clark_evans", "linearity", "planarity", "sphericity")
SHAPE = ("hag_skew", "hag_kurt", *SPATIAL)


def crown_features(xyz):
    xyz = np.asarray(xyz, dtype=float)
    # One labelled crown, flat terrain, and a generous grid enclosing fixtures.
    points = dict(zip(("x", "y", "z"), xyz.T))
    rows = tree_features(points, np.zeros((40, 40)), np.ones((40, 40), dtype=int),
                         Affine(1, 0, -20, 0, -1, 20))
    assert len(rows) == 1
    return rows[0]


def test_clark_evans_distinguishes_regular_and_clustered_crowns():
    x, y = np.meshgrid(np.linspace(-4, 4, 20), np.linspace(-4, 4, 20))
    regular = crown_features(np.c_[x.ravel(), y.ravel(), np.full(x.size, 10)])
    rng = np.random.default_rng(872)
    centres = np.repeat([[-4, -4], [-4, 4], [4, -4], [4, 4]], 100, axis=0)
    clustered_xy = centres + rng.normal(0, 0.03, (400, 2))
    clustered = crown_features(np.c_[clustered_xy, np.full(400, 10)])
    assert regular["clark_evans"] > 1.5
    assert 0 < clustered["clark_evans"] < 1


def test_vertical_line_has_unit_linearity_and_no_plan_view_area():
    row = crown_features(np.c_[np.zeros(101), np.zeros(101), np.linspace(10, 20, 101)])
    assert row["linearity"] == pytest.approx(1)
    assert row["planarity"] == pytest.approx(0)
    assert row["sphericity"] == pytest.approx(0)
    assert row["clark_evans"] is None


def test_flat_disc_has_unit_planarity():
    # Equally spaced angular samples on concentric rings give equal XY variance.
    angle = np.arange(48) * 2 * np.pi / 48
    radius = np.arange(1, 9) / 2
    x = (radius[:, None] * np.cos(angle)).ravel()
    y = (radius[:, None] * np.sin(angle)).ravel()
    row = crown_features(np.c_[x, y, np.full(x.size, 10)])
    assert row["linearity"] == pytest.approx(0, abs=1e-4)
    assert row["planarity"] == pytest.approx(1, abs=1e-4)
    assert row["sphericity"] == pytest.approx(0, abs=1e-4)


def ball_points(n):
    rng = np.random.default_rng(491)
    directions = rng.normal(size=(n, 3))
    directions /= np.linalg.norm(directions, axis=1)[:, None]
    # Cube-root radius gives uniform density in the volume, not on its surface.
    return directions * (2 * rng.random(n) ** (1 / 3))[:, None] + [0, 0, 20]


def test_uniform_ball_is_spherical_and_large_crown_is_deterministic():
    points = ball_points(SHAPE_SAMPLE * 3)
    first, second = crown_features(points), crown_features(points.copy())
    assert first == second
    assert first["n_upper_points"] == len(points) > SHAPE_SAMPLE
    assert first["sphericity"] > 0.5
    assert first["linearity"] + first["planarity"] + first["sphericity"] == pytest.approx(1, abs=2e-4)
    assert all(np.isfinite(first[name]) for name in SHAPE)


def test_symmetric_relative_heights_have_zero_skew_and_known_excess_kurtosis():
    heights = np.tile([2, 4, 6, 8], 20)
    row = crown_features(np.c_[np.linspace(-4, 4, heights.size), np.zeros(heights.size), heights])
    assert row["hag_skew"] == pytest.approx(0, abs=1e-4)
    # The equally likely four centred heights have population excess kurtosis -1.36.
    assert row["hag_kurt"] == pytest.approx(-1.36, abs=1e-4)


@pytest.mark.parametrize("n", [1, 9])
def test_fewer_than_ten_upper_points_leave_spatial_metrics_empty(n):
    row = crown_features(np.c_[np.linspace(-1, 1, n), np.zeros(n), np.full(n, 10)])
    assert row["n_upper_points"] == n
    assert all(row[name] is None for name in SHAPE)


def test_sparse_upper_crown_preserves_available_height_statistics():
    heights = np.r_[np.full(20, 1), np.linspace(6, 10, 9)]
    row = crown_features(np.c_[np.linspace(-4, 4, heights.size), np.zeros(heights.size), heights])
    assert row["n_upper_points"] == 9
    assert all(row[name] is None for name in SPATIAL)
    assert all(np.isfinite(row[name]) for name in ("hag_skew", "hag_kurt"))


def test_identical_points_leave_degenerate_metrics_empty():
    row = crown_features(np.tile([0, 0, 10], (30, 1)))
    assert all(row[name] is None for name in SHAPE)


def test_duplicate_xy_does_not_create_zero_distance_bias():
    x, y = np.meshgrid(np.linspace(-4, 4, 10), np.linspace(-4, 4, 10))
    points = np.c_[x.ravel(), y.ravel(), np.full(x.size, 10)]
    unique = crown_features(points)
    repeated = crown_features(np.repeat(points, 3, axis=0))
    assert repeated["clark_evans"] == unique["clark_evans"]
    assert repeated["clark_evans"] > 1.5
