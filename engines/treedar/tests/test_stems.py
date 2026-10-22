"""Analytic stems and shrubs, independent of survey data."""

import numpy as np
import pytest
from affine import Affine

from treedar.stems import detect_stems, fuse


TRANSFORM = Affine(0.25, 0, 0, 0, -0.25, 10)


def cylinder(x=5.0, y=5.0, radius=0.15, heights=None):
    angles, z = np.meshgrid(
        np.linspace(0, 2 * np.pi, 48, endpoint=False),
        np.linspace(1.0, 2.45, 12) if heights is None else heights,
    )
    return {"x": (x + radius * np.cos(angles)).ravel(),
            "y": (y + radius * np.sin(angles)).ravel(), "z": z.ravel()}


def detect(points, canopy=12.0, **kwargs):
    return detect_stems(points, np.zeros((40, 40)), TRANSFORM,
                        np.full((40, 40), canopy), **kwargs)


def test_vertical_cylinder_has_one_stem_and_canopy_height():
    result = detect(cylinder())
    np.testing.assert_allclose(result, [[5.0, 5.0, 12.0]], atol=1e-10)


@pytest.mark.parametrize("points", [
    cylinder(radius=0.6),
    cylinder(heights=np.linspace(1.4, 1.5, 12)),
])
def test_wide_or_flat_shrub_is_not_a_stem(points):
    assert detect(points).shape == (0, 3)


def test_stem_requires_canopy_above_minimum_height():
    assert detect(cylinder(), canopy=2.99).shape == (0, 3)
    assert detect(cylinder(), canopy=3.0).shape == (1, 3)


def test_distinct_clusters_thirty_centimetres_apart_merge():
    left, right = cylinder(x=4.85, radius=0.02), cylinder(x=5.15, radius=0.02)
    points = {key: np.concatenate([left[key], right[key]]) for key in left}
    # This epsilon leaves two DBSCAN clusters; merging is a separate stage.
    assert detect(points, eps_m=0.06, merge_dist_m=0.1).shape == (2, 3)
    np.testing.assert_allclose(detect(points, eps_m=0.06), [[5, 5, 12]], atol=1e-10)


def test_fusion_preserves_all_tops_and_only_adds_distant_stems():
    tops = np.array([[2, 2, 10], [8, 8, 14]], dtype=float)
    stems = np.array([[2.1, 2.1, 11], [3.5, 2, 13], [5, 5, 17]], dtype=float)
    trees, source = fuse(tops, stems, radius_m=1.5)
    np.testing.assert_array_equal(trees, np.vstack([tops, stems[-1]]))
    assert source.tolist() == ["top", "top", "stem"]


def test_fusion_accepts_empty_inputs():
    trees, source = fuse([], [[5, 5, 12]])
    np.testing.assert_array_equal(trees, [[5, 5, 12]])
    assert source.tolist() == ["stem"]
    trees, source = fuse([], [])
    assert trees.shape == (0, 3)
    assert source.size == 0
