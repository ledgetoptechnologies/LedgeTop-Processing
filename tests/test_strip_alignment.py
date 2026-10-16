"""Known relative flight-line offsets on generated overlapping ground."""

import json

import laspy
import numpy as np
import pytest

from treedar.qc import qc_report
from test_pipeline_e2e import cli


def write_strips(path, *, offsets=(0., 0.06, -0.04), z_base=100.,
                 overlap=True, noise=0.02):
    # 144 complete 2 m cells, with 16 returns per line in each cell.
    # Identical XY sampling prevents terrain slope from confounding offsets.
    axis = np.arange(0.25, 24., 0.5)
    xx, yy = np.meshgrid(axis, axis)
    rng = np.random.default_rng(20261016)
    xs, ys, zs, sources = [], [], [], []
    for line, offset in enumerate(offsets):
        x = xx.ravel() + (0 if overlap else line * 30.)
        y = yy.ravel()
        z = z_base + 0.02 * x + 0.01 * y + offset + rng.normal(0, noise, x.size)
        xs.append(x)
        ys.append(y)
        zs.append(z)
        sources.append(np.full(x.size, 101 + line * 17, dtype=np.uint16))
    header = laspy.LasHeader(point_format=3, version="1.2")
    header.scales = np.full(3, 0.001)
    header.offsets = np.array([0., 0., z_base])
    cloud = laspy.LasData(header)
    cloud.x, cloud.y, cloud.z = map(np.concatenate, (xs, ys, zs))
    cloud.point_source_id = np.concatenate(sources)
    cloud.classification[:] = 2
    cloud.return_number[:] = 1
    cloud.number_of_returns[:] = 1
    cloud.write(path)


@pytest.mark.parametrize("z_base", [100., 500000.])
def test_relative_strip_offsets_recovered_from_real_laz(tmp_path, z_base):
    source, out = tmp_path / "strips.laz", tmp_path / "qc"
    write_strips(source, z_base=z_base)
    result = cli("qc", source, "--out", out)
    assert result.returncode == 0, result.stdout + result.stderr
    alignment = json.loads((out / "qc.json").read_text())["strip_alignment"]
    assert alignment["flight_lines"] == 3
    assert alignment["compared_cells"] == 144
    assert set(alignment["per_line"]) == {"101", "118", "135"}
    for source_id, expected in (("101", 0.), ("118", 0.06), ("135", -0.04)):
        line = alignment["per_line"][source_id]
        assert line["cells"] == 144
        assert line["median_offset_m"] == pytest.approx(expected, abs=0.005)
    assert alignment["cell_spread_median_m"] == pytest.approx(0.10, abs=0.005)


@pytest.mark.parametrize("mode", ["single-line", "no-overlap", "rough-surface"])
def test_no_offset_estimate_without_comparable_surfaces(tmp_path, mode):
    source = tmp_path / "strips.laz"
    options = {
        "single-line": {"offsets": (0.,)},
        "no-overlap": {"overlap": False},
        "rough-surface": {"noise": 0.5},
    }[mode]
    write_strips(source, **options)
    alignment = qc_report(source)["strip_alignment"]
    assert alignment["flight_lines"] == (1 if mode == "single-line" else 3)
    assert alignment["compared_cells"] == 0
    for key in ("per_line", "abs_offset_median_m", "abs_offset_p95_m",
                "cell_spread_median_m", "cell_spread_p95_m"):
        assert key not in alignment
