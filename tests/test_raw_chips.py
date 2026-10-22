"""Synthetic raster crowns projected into synthetic original photographs."""

import json

import numpy as np
import pytest
import rasterio
from PIL import Image
from rasterio.transform import from_origin

from treedar.raw_chips import generate_raw_chips
from treedar.photo_geometry import PhotoScene


def files(tmp_path, *, edge=False, invalid=False, mismatch=None):
    run, osfm, images = [tmp_path / name for name in ("run", "opensfm", "images")]
    for path in (run, osfm, images):
        path.mkdir()
    transform = from_origin(499996, 4, 1, 1)
    labels = np.zeros((8, 8), dtype=np.int32)
    labels[:2, :2] = 1 if edge else 0
    if not edge:
        labels[3:5, 3:5] = 1
    labels[7, 7] = -2  # Background/non-tree IDs never generate filenames.
    for name, values in [("dtm.tif", np.full((8, 8), 100.0)),
                         ("chm_raw.tif", np.full((8, 8), 10.0)), ("crowns.tif", labels)]:
        if invalid and name == "chm_raw.tif":
            values[3, 3] = -999
        tf = from_origin(499997, 4, 1, 1) if mismatch == "transform" and name == "crowns.tif" else transform
        crs = "EPSG:32632" if mismatch == "crs" and name == "crowns.tif" else "EPSG:32631"
        if mismatch == "shape" and name == "crowns.tif":
            values = values[:-1]
        with rasterio.open(run / name, "w", driver="GTiff", height=values.shape[0],
                           width=values.shape[1], count=1, dtype=values.dtype, crs=crs,
                           transform=tf, nodata=-999) as ds:
            ds.write(values, 1)
    camera = dict(projection_type="brown", width=200, height=160, focal_x=.5,
                  focal_y=.5, c_x=0, c_y=0, k1=0, k2=0, k3=0, p1=0, p2=0)
    shots = {}
    for name, centre in [("b.png", [0, 0, 50]), ("a.png", [0, 0, 50]),
                         ("lower.png", [0, 0, 5]), ("far.png", [100, 0, 50])]:
        # Rotation pi around x looks downward; translation is -R @ centre.
        shots[name] = dict(camera="camera", rotation=[np.pi, 0, 0],
                           translation=[-centre[0], centre[1], centre[2]])
        Image.new("RGB", (200, 160), (40, 80, 120)).save(images / name)
    (osfm / "reconstruction.json").write_text(json.dumps([dict(
        cameras={"camera": camera}, shots=shots,
        reference_lla=dict(latitude=0, longitude=3, altitude=100),
    )]))
    return osfm, images, run, tmp_path / "chips"


def test_real_projection_ranks_ties_and_writes_original_colour_and_outline(tmp_path):
    args = files(tmp_path)
    before = {p.name: p.read_bytes() for p in args[1].iterdir()}
    result = generate_raw_chips(*args, views=3)
    assert [view["photo"] for view in result["1"]] == ["a.png", "b.png"]
    assert [view["view"] for view in result["1"]] == [0, 1]
    assert json.loads((args[3] / "views.json").read_text()) == result
    for view in result["1"]:
        with Image.open(args[3] / view["filename"]) as image:
            pixels = np.array(image)
            assert image.width == image.height
            # JPEG compression permits a small colour error, never a mosaic.
            np.testing.assert_allclose(pixels[0, 0], [40, 80, 120], atol=5)
            yellow = (pixels[:, :, 0] > 150) & (pixels[:, :, 1] > 130) & (pixels[:, :, 2] < 100)
            assert yellow.any()
    assert before == {p.name: p.read_bytes() for p in args[1].iterdir()}


def test_edge_crown_has_closed_outline_and_only_positive_requested_ids(tmp_path, monkeypatch):
    args = files(tmp_path, edge=True)
    projected = []
    original = PhotoScene.project

    def capture(self, i, points):
        projected.append(points.copy())
        return original(self, i, points)

    monkeypatch.setattr(PhotoScene, "project", capture)
    result = generate_raw_chips(*args, tree_ids=[-2, 0, 1, 999], edge_margin=0)
    assert list(result) == ["1"]
    assert len(list(args[3].glob("*.jpg"))) == 2
    assert projected
    for ring in projected:
        np.testing.assert_allclose(ring[0], ring[-1])


def test_invalid_crown_surface_is_skipped(tmp_path):
    args = files(tmp_path, invalid=True)
    assert generate_raw_chips(*args) == {}
    assert not list(args[3].glob("*.jpg"))


def test_sparse_large_crown_id_is_preserved(tmp_path):
    args = files(tmp_path)
    with rasterio.open(args[2] / "crowns.tif", "r+") as ds:
        labels = ds.read(1)
        labels[labels == 1] = 2_000_000_000
        ds.write(labels, 1)
    result = generate_raw_chips(*args, views=1)
    assert list(result) == ["2000000000"]
    assert (args[3] / "tree_2000000000_v0.jpg").is_file()


def test_integer_string_tree_ids_are_supported(tmp_path):
    assert list(generate_raw_chips(*files(tmp_path), tree_ids=["1"])) == ["1"]


@pytest.mark.parametrize("value", [1.2, True, np.bool_(True), float("nan"), float("inf"), "1.2"])
def test_noninteger_tree_ids_rejected(tmp_path, value):
    with pytest.raises(ValueError, match="tree ID must be an integer"):
        generate_raw_chips(*files(tmp_path), tree_ids=[value])


@pytest.mark.parametrize("nested", [False, True])
def test_output_cannot_overwrite_or_pollute_original_images(tmp_path, nested):
    osfm, images, run, _ = files(tmp_path)
    out = images / "chips" if nested else images
    before = {p.name: p.read_bytes() for p in images.iterdir()}
    with pytest.raises(ValueError, match="outside the original images"):
        generate_raw_chips(osfm, images, run, out)
    assert before == {p.name: p.read_bytes() for p in images.iterdir()}


def test_margin_and_search_radius_can_reject_all_photos(tmp_path):
    args = files(tmp_path)
    assert generate_raw_chips(*args, edge_margin=100) == {}
    assert generate_raw_chips(*args, search_radius_m=0.1) == {}


@pytest.mark.parametrize("mismatch", ["crs", "shape", "transform"])
def test_crown_grid_must_match_surface(tmp_path, mismatch):
    with pytest.raises(ValueError, match="crowns and surface"):
        generate_raw_chips(*files(tmp_path, mismatch=mismatch))


@pytest.mark.parametrize("kwargs", [{"views": 0}, {"views": 1.5}, {"views": float("inf")},
                                    {"views": float("nan")}, {"views": True}, {"search_radius_m": -1},
                                    {"edge_margin": -1}, {"edge_margin": float("nan")}])
def test_invalid_settings_rejected(tmp_path, kwargs):
    with pytest.raises(ValueError):
        generate_raw_chips(*files(tmp_path), **kwargs)
