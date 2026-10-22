"""Camera and grid truth uses synthetic pixels and analytic geometry only."""

import json

import numpy as np
import pytest
import rasterio
from PIL import Image
from rasterio.transform import from_origin, Affine

from treedar.photo_geometry import PhotoScene, load_surface


def scene_files(tmp_path, mutate=None, names=None):
    osfm, photos = tmp_path / "opensfm", tmp_path / "images"
    osfm.mkdir(); photos.mkdir()
    cam = dict(projection_type="brown", width=100, height=80, focal_x=.5,
               focal_y=.6, c_x=.01, c_y=-.02, k1=0, k2=0, k3=0, p1=0, p2=0)
    names = names or ["b.png", "a.png"]
    rec = dict(cameras={"one": cam, "two": {**cam, "focal_x": 1}},
               shots={n: dict(camera="one" if i == 0 else "two", rotation=[0, 0, 0],
                              translation=[-1, -2, -3]) for i, n in enumerate(names)},
               reference_lla=dict(latitude=0, longitude=3, altitude=100))
    if mutate:
        mutate(rec)
    (osfm / "reconstruction.json").write_text(json.dumps([rec]))
    for name in names:
        if "/" not in name and "\\" not in name and ":" not in name:
            Image.new("RGB", (100, 80), (12, 34, 56)).save(photos / name)
    return osfm, photos


def test_per_shot_camera_projection_and_centres(tmp_path):
    scene = PhotoScene(*scene_files(tmp_path), "EPSG:32631")
    assert scene.names == ["a.png", "b.png"]
    np.testing.assert_allclose(scene.centres, [[1, 2, 3]] * 2)
    # Camera coordinates (1, 2, 10): normalized x=.1, y=.2.
    u, v, front = scene.project(0, [[2, 4, 13], [1, 2, 2], [1, 2, 3], [np.nan, 0, 1]])
    np.testing.assert_allclose([u[0], v[0]], [60.5, 49.5])
    assert front.tolist() == [True, False, False, False]
    assert np.isnan(u[1:]).all() and np.isnan(v[1:]).all()
    np.testing.assert_allclose(scene.project(1, [[2, 4, 13]])[0], [55.5])
    returned = scene.camera(0)
    returned["focal_x"] = 100
    assert scene.camera(0)["focal_x"] == 1


def test_brown_distortion_and_rotation(tmp_path):
    def mutate(rec):
        rec["cameras"]["two"].update(k1=.1, k2=.2, k3=.3, p1=.01, p2=.02)
        rec["shots"]["a.png"].update(rotation=[0, 0, np.pi/2], translation=[0, 0, 0])
    scene = PhotoScene(*scene_files(tmp_path, mutate), "EPSG:32631")
    # Rotation maps (2,-1,10) to (1,2,10).
    x, y, r2 = .1, .2, .05
    rad = 1 + .1*r2 + .2*r2**2 + .3*r2**3
    xd = x*rad + 2*.01*x*y + .02*(r2 + 2*x*x)
    yd = y*rad + .01*(r2 + 2*y*y) + 2*.02*x*y
    u, v, front = scene.project(0, [[2, -1, 10]])
    np.testing.assert_allclose([u[0], v[0]], [(xd+.01)*100+49.5, (.6*yd-.02)*100+39.5])
    assert front.all()


def test_topocentric_origin_and_vertical_ellipsoidal_height(tmp_path):
    scene = PhotoScene(*scene_files(tmp_path), "EPSG:32631")
    points = scene.to_topo([500000, 500000], [0, 0], [100, 110])
    np.testing.assert_allclose(points, [[0, 0, 0], [0, 0, 10]], atol=1e-7)
    assert scene.to_topo(500000, 0, 100).shape == (1, 3)


@pytest.mark.parametrize("mutate,match", [
    (lambda r: r.update(shots={}), "no shots"),
    (lambda r: r["cameras"]["one"].update(projection_type="fisheye"), "unsupported camera"),
    (lambda r: r["shots"]["a.png"].update(rotation=[0, 0]), "invalid shot pose"),
    (lambda r: r["shots"]["a.png"].update(translation=[0, float("nan"), 0]), "invalid shot pose"),
    (lambda r: r["cameras"]["one"].update(width=-1), "dimensions"),
])
def test_invalid_reconstruction(tmp_path, mutate, match):
    with pytest.raises(ValueError, match=match):
        PhotoScene(*scene_files(tmp_path, mutate), "EPSG:32631")


@pytest.mark.parametrize("name", ["../escape.png", "/absolute.png", "C:/absolute.png", "folder\\photo.png"])
def test_reject_shot_path_escape(tmp_path, name):
    with pytest.raises(ValueError, match="shot path"):
        PhotoScene(*scene_files(tmp_path, names=[name]), "EPSG:32631")


def test_empty_reconstruction(tmp_path):
    (tmp_path / "reconstruction.json").write_text("[]")
    with pytest.raises(ValueError, match="nonempty"):
        PhotoScene(tmp_path, tmp_path, "EPSG:32631")


def test_photo_rgb_dimensions_and_bounded_lru(tmp_path):
    dirs = scene_files(tmp_path, names=[f"{i}.png" for i in range(5)])
    scene = PhotoScene(*dirs, "EPSG:32631")
    first = scene.photo(0)
    assert first.shape == (80, 100, 3) and first.dtype == np.uint8
    np.testing.assert_array_equal(first[0, 0], [12, 34, 56])
    assert scene.photo(0) is first
    for i in range(1, 5):
        scene.photo(i)
    assert len(scene._photos) == 3 and 0 not in scene._photos
    Image.new("RGB", (99, 80)).save(dirs[1] / "0.png")
    with pytest.raises(ValueError, match="dimensions"):
        scene.photo(0)


def surface_files(tmp_path, *, chm_crs="EPSG:32631", chm_tf=None, chm_shape=(2, 2), tf=None):
    tf = tf or from_origin(0, 2, 1, 1)
    for name, values, crs, transform in [
        ("dtm.tif", np.array([[100, -999], [100, 100]], float), "EPSG:32631", tf),
        ("chm_raw.tif", np.full(chm_shape, 5.0), chm_crs, chm_tf or tf),
    ]:
        with rasterio.open(tmp_path / name, "w", driver="GTiff", width=values.shape[1],
                           height=values.shape[0], count=1, dtype="float64", crs=crs,
                           transform=transform, nodata=-999) as ds:
            ds.write(values, 1)


def test_surface_combines_and_preserves_nodata(tmp_path):
    surface_files(tmp_path)
    dsm, tf, crs = load_surface(tmp_path)
    np.testing.assert_allclose(dsm, [[105, np.nan], [105, 105]], equal_nan=True)
    assert tf == from_origin(0, 2, 1, 1)
    assert crs.to_epsg() == 32631


@pytest.mark.parametrize("kwargs", [
    {"chm_crs": "EPSG:32632"}, {"chm_tf": from_origin(1, 2, 1, 1)}, {"chm_shape": (1, 2)},
])
def test_surface_rejects_mismatched_grids(tmp_path, kwargs):
    surface_files(tmp_path, **kwargs)
    with pytest.raises(ValueError, match="must match"):
        load_surface(tmp_path)


def test_surface_rejects_rotated_grid(tmp_path):
    surface_files(tmp_path, tf=Affine(1, .1, 0, 0, -1, 2))
    with pytest.raises(ValueError, match="north-up"):
        load_surface(tmp_path)


@pytest.mark.parametrize("crs", ["EPSG:4326", "EPSG:2263"])
def test_scene_rejects_geographic_or_feet_crs(tmp_path, crs):
    with pytest.raises(ValueError, match="projected in metres"):
        PhotoScene(*scene_files(tmp_path), crs)
