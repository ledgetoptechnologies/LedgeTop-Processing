"""Photo orthos from analytic poses, generated RGB and synthetic metre grids."""

import json
from pathlib import Path

import numpy as np
from PIL import Image
import pytest
import rasterio
from rasterio.transform import from_origin

from treedar.photo_ortho import render_photo_ortho


@pytest.fixture
def photo_scene(tmp_path):
    opensfm, images, run = [tmp_path / p for p in ("opensfm", "images", "run")]
    for folder in (opensfm, images, run):
        folder.mkdir()
    transform = from_origin(499996, 4, 1, 1)
    for name in ("dtm.tif", "chm_raw.tif"):
        with rasterio.open(run / name, "w", driver="GTiff", width=8, height=8,
                           count=1, dtype="float32", crs="EPSG:32631",
                           transform=transform, nodata=-9999) as dst:
            dst.write(np.zeros((8, 8), dtype=np.float32), 1)
    rec = {"reference_lla": {"latitude": 0, "longitude": 3, "altitude": 0},
           "cameras": {}, "shots": {}}

    def shot(name="one.png", *, width=32, height=32, colour=(0, 0, 0),
             centre_x=0, down=True):
        cam_id = name + "-camera"
        rec["cameras"][cam_id] = {
            "projection_type": "brown", "width": width, "height": height,
            "focal_x": 0.5, "focal_y": 0.5, "c_x": 0, "c_y": 0,
            "k1": 0, "k2": 0, "k3": 0, "p1": 0, "p2": 0,
        }
        rec["shots"][name] = {
            "camera": cam_id, "rotation": [np.pi, 0, 0] if down else [0, 0, 0],
            "translation": [-centre_x, 0, 10 if down else -10],
        }
        pixels = np.empty((height, width, 3), dtype=np.uint8)
        pixels[:] = colour
        Image.fromarray(pixels).save(images / name)
        (opensfm / "reconstruction.json").write_text(json.dumps([rec]))
        return pixels

    def render(name="ortho.tif", **kwargs):
        output = tmp_path / name
        stats = render_photo_ortho(opensfm, images, run, output,
                                   **dict({"cell": 1, "edge_margin": 0}, **kwargs))
        with rasterio.open(output) as ds:
            return stats, ds.read(), ds.dataset_mask()

    return {"opensfm": opensfm, "images": images, "run": run,
            "root": tmp_path, "shot": shot, "render": render, "transform": transform}


def test_one_camera_black_pixels_are_covered_and_georeferenced(photo_scene):
    photo_scene["shot"]()
    stats, rgb, mask = photo_scene["render"](tile_size=3)
    assert stats["width"] == stats["height"] == 8
    assert stats["total_pixels"] == stats["surface_pixels"] == stats["covered_pixels"] == 64
    assert stats["coverage_fraction"] == 1
    assert rgb.shape == (3, 8, 8) and not rgb.any()
    assert np.all(mask == 255)
    with rasterio.open(stats["output"]) as ds:
        assert ds.crs.to_epsg() == 32631
        assert ds.transform == photo_scene["transform"]
        assert ds.profile["tiled"] and ds.nodata is None
        assert ds.colorinterp == (rasterio.enums.ColorInterp.red,
                                  rasterio.enums.ColorInterp.green,
                                  rasterio.enums.ColorInterp.blue)
    assert not Path(stats["output"] + ".msk").exists()


def test_nearest_camera_falls_back_with_own_dimensions_and_fewer_than_six_shots(photo_scene):
    # The nearest camera to western pixels faces away. The larger green second
    # photo uses different dimensions; its full camera frame covers the grid.
    photo_scene["shot"]("a.png", width=16, height=16, down=False, colour=(255, 0, 0))
    photo_scene["shot"]("b.png", width=64, height=48, centre_x=2, colour=(0, 180, 0))
    limited, _, partial = photo_scene["render"]("nearest.tif", candidates=1)
    stats, rgb, mask = photo_scene["render"]("fallback.tif", candidates=6, tile_size=2)
    assert 0 < limited["covered_pixels"] < 64
    assert (partial == 0).any()
    assert stats["coverage_fraction"] == 1 and np.all(mask == 255)
    assert not rgb[0].any() and np.all(rgb[1] == 180) and not rgb[2].any()


def test_no_visible_photo_reports_zero_coverage(photo_scene):
    photo_scene["shot"](down=False)
    stats, rgb, mask = photo_scene["render"]()
    assert stats["surface_pixels"] == 64
    assert stats["covered_pixels"] == 0 and stats["coverage_fraction"] == 0
    assert not mask.any() and not rgb.any()


def test_missing_surface_is_masked_and_bbox_clipped_to_footprint(photo_scene):
    photo_scene["shot"](colour=(10, 20, 30))
    with rasterio.open(photo_scene["run"] / "dtm.tif", "r+") as ds:
        ground = ds.read(1)
        ground[3, 3] = -9999
        ds.write(ground, 1)
    stats, _, mask = photo_scene["render"](bbox=[499990, -10, 500010, 10])
    assert stats["width"] == stats["height"] == 8
    assert stats["surface_pixels"] == stats["covered_pixels"] == 63
    assert mask[3, 3] == 0 and np.count_nonzero(mask) == 63
    half, _, half_mask = photo_scene["render"]("half.tif", cell=0.5, tile_size=3)
    # Bilinear interpolation may not borrow through nodata; valid outer
    # half-cells retain their edge sample strictly inside the grid footprint.
    assert np.all(half_mask[0] == 255) and np.all(half_mask[-1] == 255)
    assert np.all(half_mask[5:9, 5:9] == 0)
    assert half["covered_pixels"] == 16 * 16 - 16


def test_tile_size_does_not_change_projected_photo_samples(photo_scene):
    pixels = photo_scene["shot"]()
    yy, xx = np.indices(pixels.shape[:2])
    pixels[:, :, 0], pixels[:, :, 1] = xx * 7, yy * 7
    Image.fromarray(pixels).save(photo_scene["images"] / "one.png")
    small, rgb_small, mask_small = photo_scene["render"]("small.tif", cell=0.5, tile_size=1)
    large, rgb_large, mask_large = photo_scene["render"]("large.tif", cell=0.5, tile_size=128)
    np.testing.assert_array_equal(rgb_small, rgb_large)
    np.testing.assert_array_equal(mask_small, mask_large)
    assert small["covered_pixels"] == large["covered_pixels"] == 256
    # A downward pose preserves east/right and reverses north/image-down.
    assert np.all(np.diff(rgb_small[0, 8].astype(int)) >= 0)
    assert np.all(np.diff(rgb_small[1, :, 8].astype(int)) >= 0)
    assert rgb_small[0, 8, -1] > rgb_small[0, 8, 0]


@pytest.mark.parametrize("kwargs,match", [
    ({"cell": 0}, "cell"), ({"cell": float("nan")}, "cell"),
    ({"cell": float("inf")}, "cell"), ({"candidates": 0}, "candidates"),
    ({"candidates": 1.5}, "candidates"), ({"tile_size": 0}, "tile_size"),
    ({"tile_size": True}, "tile_size"), ({"edge_margin": -1}, "edge_margin"),
    ({"bbox": [0, 0, 1]}, "bbox"), ({"bbox": [0, 0, 1, 1]}, "intersect"),
    ({"bbox": [500000, 0, 499999, 1]}, "bbox"),
    ({"bbox": [499996, -4, float("nan"), 4]}, "bbox"),
])
def test_invalid_parameters_fail_before_writing_output(photo_scene, kwargs, match):
    photo_scene["shot"]()
    with pytest.raises(ValueError, match=match):
        photo_scene["render"](**kwargs)
    assert not (photo_scene["root"] / "ortho.tif").exists()


def test_mismatched_surface_grid_is_rejected(photo_scene):
    photo_scene["shot"]()
    with rasterio.open(photo_scene["run"] / "chm_raw.tif", "r+") as ds:
        ds.transform = from_origin(499997, 4, 1, 1)
    with pytest.raises(ValueError, match="match"):
        photo_scene["render"]()


@pytest.mark.parametrize("destination", ["dtm.tif", "chm_raw.tif", "crowns.tif", "reconstruction.json", "photo"])
def test_output_cannot_overwrite_inputs(photo_scene, destination):
    photo_scene["shot"]()
    if destination == "photo":
        output = photo_scene["images"] / "one.png"
    elif destination == "reconstruction.json":
        output = photo_scene["opensfm"] / destination
    else:
        output = photo_scene["run"] / destination
    before = output.read_bytes() if output.exists() else None
    with pytest.raises(ValueError, match="output"):
        render_photo_ortho(photo_scene["opensfm"], photo_scene["images"],
                           photo_scene["run"], output, cell=1, edge_margin=0)
    if before is not None:
        assert output.read_bytes() == before
    else:
        assert not output.exists()
