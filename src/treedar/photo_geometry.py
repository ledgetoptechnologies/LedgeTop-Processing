"""OpenSfM Brown cameras and ellipsoidal surface projection helpers.

Surface elevations and reconstruction reference altitude must both be WGS84
ellipsoidal heights. This module performs no geoid conversion.
"""

import json
from collections import OrderedDict
from pathlib import Path, PureWindowsPath

import numpy as np
import rasterio
from PIL import Image
from pyproj import CRS, Transformer
from scipy.spatial.transform import Rotation


def _metric_crs(crs):
    crs = CRS.from_user_input(crs)
    if not crs.is_projected or len(crs.axis_info) < 2 or any(
        not np.isclose(axis.unit_conversion_factor, 1) for axis in crs.axis_info[:2]
    ):
        raise ValueError("surface CRS must be projected in metres")
    return crs


def load_surface(run_dir):
    """Return DTM + raw CHM, preserving invalid pixels as NaN.

    The returned DSM z coordinates are assumed ellipsoidal, not orthometric.
    Both input grids must use one projected metric, north-up grid.
    """
    run_dir = Path(run_dir)
    with rasterio.open(run_dir / "dtm.tif") as dtm, rasterio.open(run_dir / "chm_raw.tif") as chm:
        if dtm.crs != chm.crs or dtm.shape != chm.shape or dtm.transform != chm.transform:
            raise ValueError("DTM and raw CHM CRS, shape and transform must match")
        _metric_crs(dtm.crs)
        tf = dtm.transform
        if not np.isfinite(tuple(tf)).all() or tf.b != 0 or tf.d != 0 or tf.a <= 0 or tf.e >= 0:
            raise ValueError("surface grid must be north-up without rotation")
        ground = dtm.read(1, masked=True).astype(np.float64).filled(np.nan)
        canopy = chm.read(1, masked=True).astype(np.float64).filled(np.nan)
        surface = ground + canopy
        surface[~np.isfinite(surface)] = np.nan
        return surface, tf, dtm.crs


class PhotoScene:
    """A deterministic scene using each shot's own Brown camera intrinsics."""

    def __init__(self, opensfm_dir, images_dir, crs):
        self.images_dir = Path(images_dir).resolve()
        self._photos = OrderedDict()
        self.crs = _metric_crs(crs)
        with (Path(opensfm_dir) / "reconstruction.json").open(encoding="utf-8") as fh:
            reconstruction = json.load(fh)
        if not isinstance(reconstruction, list) or len(reconstruction) != 1:
            raise ValueError("expected exactly one nonempty OpenSfM reconstruction")
        rec = reconstruction[0]
        if not rec.get("shots"):
            raise ValueError("OpenSfM reconstruction has no shots")
        self.names = sorted(rec["shots"])
        self._cameras, rotations, translations = [], [], []
        for name in self.names:
            self._photo_path(name)
            shot = rec["shots"][name]
            try:
                cam = dict(rec["cameras"][shot["camera"]])
            except KeyError as exc:
                raise ValueError(f"shot {name!r} has no valid camera") from exc
            if cam.get("projection_type") != "brown":
                raise ValueError(f"unsupported camera model: {cam.get('projection_type')!r}")
            for key in ("width", "height", "focal_x", "focal_y", "c_x", "c_y", "k1", "k2", "k3", "p1", "p2"):
                try:
                    value = float(cam[key])
                except (KeyError, ValueError, TypeError) as exc:
                    raise ValueError(f"invalid Brown camera {key}") from exc
                if not np.isfinite(value):
                    raise ValueError(f"invalid Brown camera {key}")
                cam[key] = value
            if any(cam[k] <= 0 for k in ("width", "height", "focal_x", "focal_y")) or any(
                not cam[k].is_integer() for k in ("width", "height")
            ):
                raise ValueError("camera dimensions and focal lengths must be positive")
            cam["width"], cam["height"] = int(cam["width"]), int(cam["height"])
            try:
                rv, t = np.asarray(shot["rotation"], float), np.asarray(shot["translation"], float)
            except (KeyError, ValueError, TypeError) as exc:
                raise ValueError(f"invalid shot pose: {name!r}") from exc
            if rv.shape != (3,) or t.shape != (3,) or not np.isfinite(rv).all() or not np.isfinite(t).all():
                raise ValueError(f"invalid shot pose: {name!r}")
            rotations.append(Rotation.from_rotvec(rv).as_matrix())
            translations.append(t)
            self._cameras.append(cam)
        self._rotations, self._translations = np.array(rotations), np.array(translations)
        self.centres = -np.einsum("nji,nj->ni", self._rotations, self._translations)
        try:
            ref = rec["reference_lla"]
            lat, lon, altitude = [float(ref[k]) for k in ("latitude", "longitude", "altitude")]
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid reconstruction reference_lla") from exc
        if not np.isfinite([lat, lon, altitude]).all() or abs(lat) > 90 or abs(lon) > 180:
            raise ValueError("invalid reconstruction reference_lla")
        self._to_ll = Transformer.from_crs(self.crs, "EPSG:4326", always_xy=True)
        self._to_ecef = Transformer.from_crs("EPSG:4979", "EPSG:4978", always_xy=True)
        self._origin = np.array(self._to_ecef.transform(lon, lat, altitude))
        lat, lon = np.radians([lat, lon])
        self._enu = np.array([
            [-np.sin(lon), np.cos(lon), 0],
            [-np.sin(lat) * np.cos(lon), -np.sin(lat) * np.sin(lon), np.cos(lat)],
            [np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)],
        ])

    def _photo_path(self, name):
        win = PureWindowsPath(name)
        if not name or "\\" in name or win.is_absolute() or win.drive or Path(name).is_absolute() or ".." in Path(name).parts:
            raise ValueError("shot path must stay inside images directory")
        path = (self.images_dir / name).resolve()
        if not path.is_relative_to(self.images_dir):
            raise ValueError("shot path must stay inside images directory")
        return path

    def camera(self, i):
        return dict(self._cameras[i])

    def project(self, i, topocentric):
        points = np.asarray(topocentric, dtype=float)
        if points.ndim != 2 or points.shape[1] != 3:
            raise ValueError("projection points must have shape (N, 3)")
        cam = self._cameras[i]
        xyz = points @ self._rotations[i].T + self._translations[i]
        front = np.isfinite(xyz).all(axis=1) & (xyz[:, 2] > 0)
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            x, y = xyz[:, 0] / xyz[:, 2], xyz[:, 1] / xyz[:, 2]
            r2 = x*x + y*y
            radial = 1 + cam["k1"]*r2 + cam["k2"]*r2**2 + cam["k3"]*r2**3
            xd = x*radial + 2*cam["p1"]*x*y + cam["p2"]*(r2 + 2*x*x)
            yd = y*radial + cam["p1"]*(r2 + 2*y*y) + 2*cam["p2"]*x*y
            size = max(cam["width"], cam["height"])
            u = (cam["focal_x"]*xd + cam["c_x"])*size + (cam["width"] - 1)/2
            v = (cam["focal_y"]*yd + cam["c_y"])*size + (cam["height"] - 1)/2
        front &= np.isfinite(u) & np.isfinite(v)
        return np.where(front, u, np.nan), np.where(front, v, np.nan), front

    def to_topo(self, e, n, z):
        """Convert projected e/n and WGS84 ellipsoidal z into OpenSfM ENU."""
        e, n, z = np.broadcast_arrays(np.asarray(e, float), np.asarray(n, float), np.asarray(z, float))
        lon, lat = self._to_ll.transform(e.ravel(), n.ravel())
        xyz = np.array(self._to_ecef.transform(lon, lat, z.ravel())).T
        return (xyz - self._origin) @ self._enu.T

    def photo(self, i):
        if i in self._photos:
            self._photos.move_to_end(i)
            return self._photos[i]
        with Image.open(self._photo_path(self.names[i])) as image:
            cam = self._cameras[i]
            if image.size != (cam["width"], cam["height"]):
                raise ValueError("decoded photo dimensions differ from OpenSfM camera")
            pixels = np.array(image.convert("RGB"), dtype=np.uint8)
        pixels.flags.writeable = False
        self._photos[i] = pixels
        if len(self._photos) > 3:
            self._photos.popitem(last=False)
        return pixels
