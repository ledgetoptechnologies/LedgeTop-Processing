"""Shared invented fixtures. The bridge double comes from the independent reference."""
import hashlib
import json
import os
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))
sys.dont_write_bytecode = True

from declared_frame import contract as frame_contract  # noqa: E402
from declared_frame import dem_frame, survey_frame  # noqa: E402
from declared_frame.contract import normalize_once, projection, seal  # noqa: E402
from declared_frame.errors import FrameRefused  # noqa: E402
from declared_frame.geodesy import GRID_SHA256, GeoidGrid, LocalFrame  # noqa: E402
from declared_frame.ledger import Ledger  # noqa: E402
from declared_frame.profile import load_profile  # noqa: E402

GRID_PATH = os.environ.get('DECLARED_FRAME_GRID')
NEEDS_GRID = unittest.skipUnless(
    GRID_PATH, 'needs us_noaa_g2018u0.tif, sha256 ' + GRID_SHA256)
RAW_PROFILE = (HERE.parent / 'fixtures/synthetic-profile.json').read_bytes()
PROFILE = load_profile(RAW_PROFILE, hashlib.sha256(RAW_PROFILE).hexdigest())
NONCE = 'a' * 64
PLACES = {'SYN-A': (-71.500, 43.500, 250.0), 'SYN-B': (-71.492, 43.506, 261.5),
          'SYN-C': (-71.508, 43.494, 243.25)}
SIZE = (4000, 3000)


def profile_with(**changes):
    data = json.loads(RAW_PROFILE)
    for key, value in changes.items():
        data[key] = {**data[key], **value} if isinstance(value, dict) else value
    raw = json.dumps(data).encode()
    return load_profile(raw, hashlib.sha256(raw).hexdigest())


def rows():
    result = []
    for label, (lon, lat, height) in PLACES.items():
        east, north = PROFILE.survey.forward(lon, lat)
        for n in range(3):
            result.append([f'{east:.4f}', f'{north:.4f}', f'{height / PROFILE.survey_unit_m:.4f}',
                           str(100 + n), str(200 + n), f'image-{n}.jpg', label])
    return result


def survey_bytes(header=None, data=None):
    lines = [header or survey_frame.header_for(PROFILE)] + [' '.join(r) for r in data or rows()]
    return ('\n'.join(lines) + '\n').encode()


SIZES = {f'image-{n}.jpg': SIZE for n in range(3)}


def declared_contract(profile=PROFILE):
    """A well-formed contract that needs no grid."""
    survey = survey_frame.read_survey(survey_bytes(), profile)
    return seal({
        'schema': frame_contract.SCHEMA, 'kind': profile.kind, 'profile_sha256': profile.sha256,
        'source': {**profile.source, 'raw_sha256': survey.raw_sha256,
                   'raw_header': survey.raw_header},
        'working_frame': profile.working_frame, 'epoch_review': profile.epoch_review,
        'grid_sha256': GRID_SHA256, 'vertical_application_count': 1,
        'points': [{'id': label, 'raw_xyz': list(survey.points()[label]),
                    'working': [lon, lat, height - 27.0],
                    'marks': [{'filename': m.filename, 'raw_pixel': list(m.raw_pixel)}
                              for m in survey.marks if m.id == label]}
                   for label, (lon, lat, height) in PLACES.items()]})


def sink_items(contract):
    return [{'id': p['id'], 'longitude': p['working'][0], 'latitude': p['working'][1],
             'height': p['working'][2], 'has_height': True,
             'observations': [[m['filename'], *projection(m['raw_pixel'], *SIZE)]
                              for m in p['marks']]} for p in contract['points']]


class ReferenceBridge:
    def __init__(self, grid, profile=PROFILE):
        self.grid, self.profile = grid, profile

    def survey_to_ellipsoid(self, xyz):
        lon, lat = self.profile.survey.inverse(xyz[0], xyz[1])
        return lon, lat, xyz[2] * self.profile.survey_unit_m + self.grid.undulation(lon, lat)

    def ellipsoid_to_survey(self, working):
        lon, lat, h = working
        east, north = self.profile.survey.forward(lon, lat)
        return east, north, (h - self.grid.undulation(lon, lat)) / self.profile.survey_unit_m


ORIGIN = (43.5, -71.5, 0.0)
EAST, NORTH = PROFILE.output.forward(ORIGIN[1], ORIGIN[0])
OFFSET = [int(EAST), int(NORTH)]
# A deliberately scaled, rotated and skewed engine matrix with the XY offset removed.
AFFINE = [[0.9998, 0.0021, -0.0001, EAST - OFFSET[0]],
          [-0.0019, 0.9999, 0.0002, NORTH - OFFSET[1]],
          [0.0001, -0.0003, 1.0001, 0.02], [0, 0, 0, 1]]


def chain(contract):
    sink = frame_contract.verify_sink(contract, PROFILE, sink_items(contract), SIZES)
    record = dem_frame.georeference(contract, sink, PROFILE, origin=ORIGIN, affine=AFFINE,
                                    offset=OFFSET)
    engine = [(10.0, 20.0, 230.0), (900.0, -700.0, 260.0), (-1200.0, 1100.0, 215.0)]
    cloud = dem_frame.bind_cloud(
        record, PROFILE, source_sha256='1' * 64, output_sha256='2' * 64, source_count=9,
        output_count=9, witnesses=[(p, dem_frame.exact_point(record, PROFILE, p)) for p in engine])
    dem = dem_frame.bind_dem(cloud, record, PROFILE, cloud_sha256='2' * 64, dem_sha256='3' * 64,
                             epsg=PROFILE.output_epsg,
                             affine=[EAST, 320.0, 17.0, NORTH, 23.0, -270.0],
                             data_type='Float64', unit='m')
    return {'contract': contract, 'sink': sink, 'georeference': record, 'cloud': cloud, 'dem': dem}


class Refusals(unittest.TestCase):
    def refused(self, code, action, *args, **kwargs):
        with self.assertRaises(FrameRefused) as caught:
            action(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)
        return caught.exception


__all__ = [name for name in dir() if not name.startswith('_')]
