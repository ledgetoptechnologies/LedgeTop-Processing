"""Additive offline survey-frame bridge; no legacy ODX workflow changes.

Not an OpenSfM adapter: NAD83(2011) geographic 3D must not be relabeled WGS84.
No implicit use on point clouds/meshes/rasters lacking coordinate-frame receipts.
"""
import hashlib
import math
from pathlib import Path
from pyproj import CRS,Transformer
from pyproj.enums import TransformDirection

GRID_SHA256='fa9a407ac7ee3f5a3694008e4bcd09ce9cc250452f0c3b11700a4960340abce2'
GRID_SIZE=16742155
class SurveyVerticalBridge:
    survey_frame='EPSG:7600+6360; NAVD88 GEOID18; XYZ ftUS'
    ellipsoidal_frame='EPSG:6319; NAD83(2011); longitude/latitude degrees, h metres'
    def __init__(self,grid_path):
        p=Path(grid_path).resolve()
        if not p.is_file() or p.stat().st_size!=GRID_SIZE or hashlib.sha256(p.read_bytes()).hexdigest()!=GRID_SHA256:
            raise ValueError('verified_geoid18_grid_required')
        # Avoid paths with pipeline syntax. An absolute verified file avoids CDN lookup.
        if any(c in str(p) for c in [' ', '+','\n','\r']):raise ValueError('unsupported_grid_path')
        self.xy=Transformer.from_crs(CRS(7600),CRS(6318),always_xy=True,allow_ballpark=False)
        self.pipeline=f'+proj=pipeline +step +proj=unitconvert +xy_in=deg +xy_out=rad +step +proj=vgridshift +grids={p.as_posix()} +multiplier=1 +step +proj=unitconvert +xy_in=rad +xy_out=deg'
        self.vertical=Transformer.from_pipeline(self.pipeline)
    @staticmethod
    def _finite(values):
        if len(values)!=3 or not all(isinstance(v,(int,float)) and math.isfinite(v) for v in values):raise ValueError('finite_xyz_required')
        return tuple(values)
    def survey_to_ellipsoid(self,xyz_ftUS):
        e,n,H=self._finite(xyz_ftUS)
        lon,lat=self.xy.transform(e,n,errcheck=True)
        return self._finite(self.vertical.transform(lon,lat,H*1200/3937,errcheck=True))
    def ellipsoid_to_survey(self,lon_lat_h_m):
        lon,lat,h=self._finite(lon_lat_h_m)
        _,_,H=self.vertical.transform(lon,lat,h,direction=TransformDirection.INVERSE,errcheck=True)
        e,n=self.xy.transform(lon,lat,direction=TransformDirection.INVERSE,errcheck=True)
        return self._finite((e,n,H*3937/1200))
    def record(self,point,direction):
        """Keep explicit role/ID; never select fitting controls from source names."""
        if point.get('role') not in ['control','checkpoint','excluded']:raise ValueError('explicit_role_required')
        if not isinstance(point.get('id'),str) or not point['id']:raise ValueError('point_id_required')
        if direction=='to_ellipsoid': xyz=self.survey_to_ellipsoid(point['xyz']);frame=self.ellipsoidal_frame
        elif direction=='to_survey':xyz=self.ellipsoid_to_survey(point['xyz']);frame=self.survey_frame
        else:raise ValueError('explicit_direction_required')
        return {**point,'xyz':xyz,'coordinate_frame':frame,'grid_sha256':GRID_SHA256}
