"""Preset behavior on synthetic crowns and through the public CLI."""

import json

import numpy as np
import pytest
from affine import Affine

from synthetic_forest import TREES, write_forest
from treedar import chm, cli, io, segment, terrain


def test_default_window_retains_original_height_dependent_formula():
    height = np.array([0, 5, 10, 20, 30], dtype=float)
    np.testing.assert_allclose(segment.window_diameter_m(height), [3, 3.5, 4, 5, 6])
    assert segment.WINDOW_PRESETS["broadleaf"] == {
        "window_base_m": 3.0, "window_slope": 0.1, "smooth_sigma_m": 0.5,
    }


def test_default_and_broadleaf_find_original_synthetic_forest(tmp_path):
    laz = tmp_path / "forest.laz"
    write_forest(laz)
    pts = io.read_points(laz)
    dtm, transform = terrain.ground_dtm(pts["x"], pts["y"], pts["z"], cell=0.5,
                                       classification=pts["classification"])
    raw = chm.max_height_grid(pts["x"], pts["y"], pts["z"], dtm, transform, cell=0.5)
    canopy, _ = chm.filter_pits_and_spikes(raw)
    default = segment.detect_treetops(canopy, transform)
    broadleaf = segment.detect_treetops(canopy, transform, **segment.WINDOW_PRESETS["broadleaf"])
    np.testing.assert_array_equal(default, broadleaf)
    assert len(default) == 3
    for tree in TREES:
        nearest = default[np.argmin(np.hypot(default[:, 0] - tree.x, default[:, 1] - tree.y))]
        np.testing.assert_allclose(nearest, [tree.x, tree.y, tree.height], atol=0.05)


def test_conifer_resolves_crowns_two_and_a_half_metres_apart():
    # Unequal peaks prevent an equality tie from masquerading as separation.
    rows, cols = np.mgrid[:80, :80]
    x, y = (cols + 0.5) * 0.25, 20 - (rows + 0.5) * 0.25
    canopy = np.maximum(
        30 * np.exp(-((x - 8.125) ** 2 + (y - 10.125) ** 2) / (2 * 0.5**2)),
        28 * np.exp(-((x - 10.625) ** 2 + (y - 10.125) ** 2) / (2 * 0.5**2)),
    )
    transform = Affine(0.25, 0, 0, 0, -0.25, 20)
    broadleaf = segment.detect_treetops(canopy, transform, **segment.WINDOW_PRESETS["broadleaf"])
    conifer = segment.detect_treetops(canopy, transform, **segment.WINDOW_PRESETS["conifer"])
    assert len(broadleaf) == 1
    assert len(conifer) == 2
    np.testing.assert_allclose(np.sort(conifer[:, 0]), [8.125, 10.625], atol=0.25)


@pytest.mark.parametrize("flags,expected", [
    ([], ("broadleaf", 3.0, 0.1, 0.5)),
    (["--preset", "conifer"], ("conifer", 2.0, 0.0, 0.25)),
    (["--preset", "conifer", "--window-base", "4", "--window-slope", "0.2", "--smooth", "0"],
     ("conifer", 4.0, 0.2, 0.0)),
    (["--preset", "broadleaf", "--window-slope", "0"], ("broadleaf", 3.0, 0.0, 0.5)),
])
def test_cli_summary_records_resolved_preset_and_explicit_overrides(tmp_path, monkeypatch, flags, expected):
    laz, out = tmp_path / "forest.laz", tmp_path / "run"
    write_forest(laz)
    calls = []
    original = segment.detect_treetops

    def capture(*args, **kwargs):
        calls.append(kwargs.copy())
        return original(*args, **kwargs)

    monkeypatch.setattr(segment, "detect_treetops", capture)
    assert cli.main(["run", str(laz), "--out", str(out), "--no-qc", *flags]) == 0
    params = json.loads((out / "summary.json").read_text())["params"]
    keys = ("preset", "window_base_m", "window_slope", "smooth_sigma_m")
    assert tuple(params[key] for key in keys) == expected
    assert len(calls) == 1
    assert tuple(calls[0][key] for key in keys[1:]) == expected[1:]
