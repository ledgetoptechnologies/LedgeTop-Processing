"""Survey grid with NAVD88 heights to and from NAD83(2011) ellipsoidal, through PROJ."""
import hashlib
import math
from pathlib import Path

from pyproj import CRS, Transformer
from pyproj.enums import TransformDirection

from .geodesy import GRID_SHA256, GRID_SIZE


class VerticalBridge:
    """Survey XYZ in the profile's unit; ellipsoidal in degrees and metres."""

    def __init__(self, grid_path, profile):
        path = Path(grid_path).resolve()
        if (not path.is_file() or path.stat().st_size != GRID_SIZE
                or hashlib.sha256(path.read_bytes()).hexdigest() != GRID_SHA256):
            raise ValueError('verified_geoid18_grid_required')
        # An absolute verified file avoids any CDN lookup; keep pipeline syntax out of it.
        if any(c in str(path) for c in (' ', '+', chr(10), chr(13))):
            raise ValueError('unsupported_grid_path')
        self.unit = profile.survey_unit_m
        self.xy = Transformer.from_crs(CRS(profile.survey_epsg), CRS(6318), always_xy=True,
                                       allow_ballpark=False)
        self.pipeline = (
            '+proj=pipeline +step +proj=unitconvert +xy_in=deg +xy_out=rad '
            f'+step +proj=vgridshift +grids={path.as_posix()} +multiplier=1 '
            '+step +proj=unitconvert +xy_in=rad +xy_out=deg')
        self.vertical = Transformer.from_pipeline(self.pipeline)

    @staticmethod
    def _finite(values):
        if len(values) != 3 or not all(
                isinstance(v, (int, float)) and math.isfinite(v) for v in values):
            raise ValueError('finite_xyz_required')
        return tuple(values)

    def survey_to_ellipsoid(self, xyz):
        east, north, height = self._finite(xyz)
        lon, lat = self.xy.transform(east, north, errcheck=True)
        return self._finite(self.vertical.transform(lon, lat, height * self.unit, errcheck=True))

    def ellipsoid_to_survey(self, working):
        lon, lat, h = self._finite(working)
        height = self.vertical.transform(lon, lat, h, direction=TransformDirection.INVERSE,
                                         errcheck=True)[2]
        east, north = self.xy.transform(lon, lat, direction=TransformDirection.INVERSE,
                                        errcheck=True)
        return self._finite((east, north, height / self.unit))
