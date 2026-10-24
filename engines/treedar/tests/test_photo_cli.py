"""Photo commands preserve options and surface clear CLI errors."""

import json
import subprocess
import sys

import rasterio

from treedar import cli, photo_ortho, raw_chips


def test_raw_chips_cli_forwards_ids_and_options(tmp_path, monkeypatch, capsys):
    ids = tmp_path / "ids.json"
    ids.write_text('[1, "7"]')
    calls = []
    monkeypatch.setattr(raw_chips, "generate_raw_chips",
                        lambda *a, **kw: calls.append((a, kw)) or {"1": [{"view": 0}]})
    assert cli.main(["raw-chips", "run", "--opensfm", "alignment", "--images", "photos",
                     "--out", "views", "--tree-ids", str(ids), "--views", "2",
                     "--search-radius", "25", "--edge-margin", "0", "--select", "spread"]) == 0
    assert calls == [(("alignment", "photos", "run", "views"),
                      dict(tree_ids=[1, "7"], views=2, search_radius_m=25, edge_margin=0,
                           select="spread"))]
    assert "1 trees, 1 views" in capsys.readouterr().out


def test_raw_chips_cli_rejects_non_list_ids(tmp_path, capsys):
    ids = tmp_path / "ids.json"
    ids.write_text('{"id": 1}')
    assert cli.main(["raw-chips", "run", "--opensfm", "alignment", "--images", "photos",
                     "--out", "views", "--tree-ids", str(ids)]) == 2
    assert "must contain a list" in capsys.readouterr().err


def test_photo_ortho_cli_forwards_bbox_and_resolution(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(photo_ortho, "render_photo_ortho",
                        lambda *a, **kw: calls.append((a, kw)) or {"coverage": 0.75})
    assert cli.main(["photo-ortho", "run", "--opensfm", "alignment", "--images", "photos",
                     "--out", "overview.tif", "--cell", "0.2", "--bbox", "0,0,4,6",
                     "--candidates", "3", "--edge-margin", "1"]) == 0
    assert calls == [(("alignment", "photos", "run", "overview.tif"),
                      dict(cell=0.2, bbox=(0., 0., 4., 6.), candidates=3, edge_margin=1))]
    assert json.loads(capsys.readouterr().out) == {"coverage": 0.75}


def test_photo_ortho_cli_reports_validation_error(monkeypatch, capsys):
    def reject(*args, **kwargs):
        raise ValueError("surface grids differ")
    monkeypatch.setattr(photo_ortho, "render_photo_ortho", reject)
    assert cli.main(["photo-ortho", "run", "--opensfm", "alignment", "--images", "photos",
                     "--out", "overview.tif"]) == 2
    assert "surface grids differ" in capsys.readouterr().err


def test_photo_commands_real_cli_with_synthetic_alignment(tmp_path):
    from test_raw_chips import files
    opensfm, images, run, chips = files(tmp_path)
    before = {p: p.read_bytes() for root in (opensfm, images, run) for p in root.iterdir()}
    common = ["--opensfm", str(opensfm), "--images", str(images), "--edge-margin", "0"]
    chip_run = subprocess.run([sys.executable, "-m", "treedar", "raw-chips", str(run),
                               *common, "--out", str(chips), "--views", "1"],
                              capture_output=True, text=True, timeout=30)
    assert chip_run.returncode == 0, chip_run.stdout + chip_run.stderr
    views = json.loads((chips / "views.json").read_text())
    assert len(views["1"]) == 1 and (chips / views["1"][0]["filename"]).is_file()
    output = tmp_path / "photo-overview.tif"
    ortho_run = subprocess.run([sys.executable, "-m", "treedar", "photo-ortho", str(run),
                                *common, "--out", str(output), "--cell", "1"],
                               capture_output=True, text=True, timeout=30)
    assert ortho_run.returncode == 0, ortho_run.stdout + ortho_run.stderr
    assert json.loads(ortho_run.stdout)["coverage_fraction"] == 1
    with rasterio.open(output) as ds:
        assert ds.count == 3 and ds.shape == (8, 8) and ds.dataset_mask().all()
    assert all(p.read_bytes() == content for p, content in before.items())
