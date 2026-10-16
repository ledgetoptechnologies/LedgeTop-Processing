"""Actual LAZ, RGB GeoTIFF and crown-chip tests using invented coordinates."""

import os
from pathlib import Path
import subprocess
import sys

import laspy
import numpy as np
from PIL import Image
from pyproj import CRS
import pytest
import rasterio

from treedar import io, ortho


def write_points(path, points, rgb=True):
    header = laspy.LasHeader(point_format=3 if rgb else 1, version="1.2")
    header.scales = [0.001] * 3
    header.add_crs(CRS.from_epsg(32616))
    cloud = laspy.LasData(header)
    values = np.asarray(points)
    cloud.x, cloud.y, cloud.z = values[:, :3].T
    if rgb:
        cloud.red, cloud.green, cloud.blue = values[:, 3:].astype(np.uint16).T
    cloud.write(path)


def cli(*args):
    env = {**os.environ, "OMP_NUM_THREADS": "2", "OPENBLAS_NUM_THREADS": "2"}
    result = subprocess.run([sys.executable, "-m", "treedar", *map(str, args)],
                            capture_output=True, text=True, timeout=60, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    return result


@pytest.mark.parametrize("chunk_size", [2, 100])
def test_highest_coloured_point_and_black_cells(tmp_path, monkeypatch, chunk_size):
    source, output = tmp_path / "stacked.laz", tmp_path / "rgb.tif"
    # The second chunk can contain a lower coloured sample or a higher black
    # sample. Neither may replace the highest available coloured return.
    write_points(source, [
        (0.25, 2.75, 4, 0, 60000, 0),
        (1.25, 2.75, 8, 0, 0, 60000),
        (0.25, 2.75, 1, 60000, 0, 0),
        (0.25, 2.75, 20, 0, 0, 0),
        (2.25, 2.75, 6, 60000, 0, 0),
        (0.25, 0.25, 1, 0, 0, 0),
    ])
    monkeypatch.setattr(io, "CHUNK", chunk_size)
    info = ortho.build(source, output, cell=1)
    assert info == {"cell_m": 1, "rows": 3, "cols": 3, "filled_fraction": 0.3333}
    with rasterio.open(output) as ds:
        assert ds.count == 3
        assert ds.dtypes == ("uint8",) * 3
        assert ds.crs.to_epsg() == 32616
        assert ds.transform == rasterio.transform.from_origin(0, 3, 1, 1)
        expected = np.zeros((3, 3, 3), dtype=np.uint8)
        expected[:, 0, 0] = (0, 255, 0)
        expected[:, 0, 1] = (0, 0, 255)
        expected[:, 0, 2] = (255, 0, 0)
        np.testing.assert_array_equal(ds.read(), expected)


def test_all_uncoloured_points_produce_black_ortho(tmp_path):
    source, output = tmp_path / "black.laz", tmp_path / "black.tif"
    write_points(source, [(0.25, 0.25, 1, 0, 0, 0), (1.25, 1.25, 2, 0, 0, 0)])
    info = ortho.build(source, output, cell=1)
    assert info["filled_fraction"] == 0
    with rasterio.open(output) as ds:
        assert not ds.read().any()


def test_missing_rgb_is_rejected_without_output(tmp_path):
    source, output = tmp_path / "no_rgb.laz", tmp_path / "bad.tif"
    write_points(source, [(0.25, 0.25, 1), (1.25, 1.25, 2)], rgb=False)
    with pytest.raises(ValueError, match="no RGB"):
        ortho.build(source, output)
    assert not output.exists()


def test_bbox_fallback_excludes_points_in_snapped_grid_outside_bbox(tmp_path):
    source, output = tmp_path / "ordinary.laz", tmp_path / "subset.tif"
    # Ordinary LAZ forces a chunked scan. Points outside exact bounds still
    # fit the snapped raster, so grid membership alone cannot enforce bbox.
    write_points(source, [(0.25, 0.75, 30, 0, 60000, 0),
                          (0.75, 0.75, 10, 60000, 0, 0),
                          (1.25, 0.75, 20, 0, 0, 60000)])
    info = ortho.build(source, output, cell=1, bbox=(0.5, 0.5, 1.0, 1.0))
    assert info["filled_fraction"] == 0.5
    with rasterio.open(output) as ds:
        assert ds.transform == rasterio.transform.from_origin(0, 1, 1, 1)
        expected = np.zeros((3, 1, 2), dtype=np.uint8)
        expected[:, 0, 0] = (255, 0, 0)
        np.testing.assert_array_equal(ds.read(), expected)


@pytest.fixture
def chip_scene(tmp_path):
    run = tmp_path / "run"
    run.mkdir()
    transform = rasterio.transform.from_origin(0, 12, 1, 1)
    crowns = np.zeros((12, 12), dtype=np.int32)
    crowns[2:5, 2:5] = 1
    crowns[7:10, 7:10] = 3
    with rasterio.open(run / "crowns.tif", "w", driver="GTiff", height=12, width=12,
                       count=1, dtype="int32", transform=transform, crs="EPSG:32616") as ds:
        ds.write(crowns, 1)
    # ID 2 has a tree-table record but no crown pixels and must be skipped.
    (run / "trees.csv").write_text("tree_id,x,y,height_m,crown_area_m2,crown_diameter_m\n"
                                   "1,3.5,8.5,10,9,3.4\n2,5.5,5.5,8,0,0\n3,8.5,3.5,12,9,3.4\n")
    image_path = tmp_path / "ortho.tif"
    image = np.zeros((3, 12, 12), dtype=np.uint8)
    image[2] = 180
    with rasterio.open(image_path, "w", driver="GTiff", height=12, width=12,
                       count=3, dtype="uint8", transform=transform, crs="EPSG:32616") as ds:
        ds.write(image)
    return run, image_path


def assert_chip(path):
    with Image.open(path) as image:
        image.verify()
    with Image.open(path) as image:
        assert image.format == "PNG"
        assert min(image.size) >= 280
        assert max(image.size) <= 1024
        pixels = np.asarray(image)
        assert pixels.shape[2] == 3
        assert ((pixels == (255, 255, 0)).all(axis=2)).any()
        assert ((pixels == (0, 0, 180)).all(axis=2)).any()


def test_crown_chips_subset_and_missing_crown(chip_scene, tmp_path):
    run, image = chip_scene
    output = tmp_path / "chips"
    written = ortho.crown_chips(run, image, output, tree_ids=[3, 2, 999])
    assert [row["tree_id"] for row in written] == ["3"]
    assert {p.name for p in output.glob("*.png")} == {"tree_3.png"}
    assert Path(written[0]["path"]) == output / "tree_3.png"
    assert_chip(output / "tree_3.png")
    with Image.open(output / "tree_3.png") as chip:
        assert chip.size == (written[0]["width"], written[0]["height"])


def test_ortho_and_chips_cli_success(chip_scene, tmp_path):
    run, _ = chip_scene
    source, image, output = tmp_path / "rgb.laz", tmp_path / "cli.tif", tmp_path / "cli_chips"
    # Dense blue points cover both crown windows, giving visible imagery plus
    # the yellow outline in the CLI output as in the direct API test.
    write_points(source, [(x + 0.25, y + 0.25, 5, 0, 0, 60000)
                          for x in range(12) for y in range(12)])
    cli("ortho", source, "--out", image, "--cell", "1")
    with rasterio.open(image) as ds:
        assert ds.count == 3 and ds.crs.to_epsg() == 32616
    result = cli("chips", run, "--ortho", image, "--out", output)
    assert "2 crown chips" in result.stdout
    assert {p.name for p in output.glob("*.png")} == {"tree_1.png", "tree_3.png"}
    for path in output.glob("*.png"):
        with Image.open(path) as chip:
            chip.verify()
        with Image.open(path) as chip:
            assert min(chip.size) >= 280
            pixels = np.asarray(chip)
            assert ((pixels == (255, 255, 0)).all(axis=2)).any()
            assert ((pixels == (0, 0, 255)).all(axis=2)).any()


def test_elongated_crown_keeps_a_usable_chip(tmp_path):
    run, image, output = tmp_path / "run", tmp_path / "ortho.tif", tmp_path / "chips"
    run.mkdir()
    transform = rasterio.transform.from_origin(0, 8, 1, 1)
    crowns = np.zeros((8, 106), dtype=np.int32)
    crowns[3:5, 3:103] = 1
    with rasterio.open(run / "crowns.tif", "w", driver="GTiff", height=8, width=106,
                       count=1, dtype="int32", transform=transform, crs="EPSG:32616") as ds:
        ds.write(crowns, 1)
    (run / "trees.csv").write_text("tree_id,x,y,height_m,crown_area_m2,crown_diameter_m\n"
                                   "1,50.5,4.5,10,200,16\n")
    rgb = np.zeros((3, 8, 106), dtype=np.uint8)
    rgb[2] = 180
    with rasterio.open(image, "w", driver="GTiff", height=8, width=106,
                       count=3, dtype="uint8", transform=transform, crs="EPSG:32616") as ds:
        ds.write(rgb)
    written = ortho.crown_chips(run, image, output)
    assert [row["tree_id"] for row in written] == ["1"]
    assert_chip(output / "tree_1.png")
