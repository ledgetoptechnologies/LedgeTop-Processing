"""The injected profile: every project-specific value, and nothing in code.

A controlled profile is private. It is never committed; a run names it by SHA-256.
"""
import hashlib
import json
import re
from dataclasses import dataclass

from .errors import FrameRefused
from .geodesy import GRID_NAME, GRID_SHA256, GRID_SIZE, TransverseMercator

SCHEMA = 'declared-frame-profile/v1'
KINDS = ('synthetic', 'controlled')
WORKING = {
    'crs': 'EPSG:6319',
    'axes': 'longitude,latitude,ellipsoidal_height',
    'units': 'degree,degree,metre',
    'height': 'GRS80 ellipsoidal',
}
_SOURCE_KEYS = {'crs', 'units', 'axes', 'realization', 'epoch', 'geoid'}
_KEYS = {'schema', 'kind', 'source', 'survey', 'output', 'grid', 'roles', 'epoch_review',
         'camera_gps'}
_TM_KEYS = {'epsg', 'lat0', 'lon0', 'scale', 'false_east_m', 'false_north_m', 'unit_m'}
_SHA = re.compile('[0-9a-f]{64}')
_ID = re.compile('[A-Za-z0-9_-]{1,32}')


@dataclass(frozen=True)
class Profile:
    sha256: str
    kind: str
    source: dict
    survey: TransverseMercator
    survey_epsg: int
    survey_unit_m: float
    output: TransverseMercator
    output_epsg: int
    output_parameters: dict
    grid: dict
    fit: tuple
    marks_per_point: int
    withheld: tuple
    excluded: tuple
    epoch_review: str

    @property
    def working_frame(self):
        return {**WORKING, 'realization': self.source['realization'],
                'epoch': self.source['epoch']}


def load_profile(raw, expected_sha256):
    """`expected_sha256` comes from the sealed manifest, never from the profile."""
    digest = hashlib.sha256(raw).hexdigest()
    if not isinstance(expected_sha256, str) or digest != expected_sha256:
        raise FrameRefused('profile_digest_mismatch')
    try:
        data = json.loads(raw)
        if set(data) != _KEYS or data['schema'] != SCHEMA or data['kind'] not in KINDS:
            raise ValueError
        source, roles, grid = data['source'], data['roles'], data['grid']
        if set(source) != _SOURCE_KEYS or not all(
                isinstance(v, str) and v and not re.search('[ =]', v) for v in source.values()):
            raise ValueError
        if source['realization'] != 'NAD83(2011)' or source['geoid'] != 'GEOID18':
            raise FrameRefused('unsupported_frame')
        if not re.fullmatch('[0-9]{4}[.][0-9]{2}', source['epoch']):
            raise FrameRefused('unsupported_epoch')
        if grid != {'name': GRID_NAME, 'sha256': GRID_SHA256, 'size': GRID_SIZE}:
            raise FrameRefused('verified_geoid18_grid_required')
        for key in ('survey', 'output'):
            if set(data[key]) != _TM_KEYS:
                raise ValueError
        groups = [tuple(roles[key]) for key in ('fit', 'withheld', 'excluded')]
        every = [label for group in groups for label in group]
        if (set(roles) != {'fit', 'withheld', 'excluded', 'marks_per_point'} or not groups[0]
                or len({label.upper() for label in every}) != len(every)
                or not all(_ID.fullmatch(label) for label in every)
                or not isinstance(roles['marks_per_point'], int) or roles['marks_per_point'] < 1):
            raise FrameRefused('role_profile_invalid')
        # Nothing here transforms camera GPS into the declared frame, so it may not
        # constrain a solve in that frame.
        if data['camera_gps'] != 'not_a_constraint':
            raise FrameRefused('camera_gps_frame_unqualified')
        review = data['epoch_review']
        if data['kind'] == 'controlled' and not _SHA.fullmatch(review):
            raise FrameRefused('epoch_review_required')
        return Profile(
            sha256=digest, kind=data['kind'], source=source,
            survey=_projection(data['survey']), survey_epsg=data['survey']['epsg'],
            survey_unit_m=data['survey']['unit_m'],
            output=_projection(data['output']), output_epsg=data['output']['epsg'],
            output_parameters=data['output'], grid=grid, fit=groups[0],
            marks_per_point=roles['marks_per_point'], withheld=groups[1], excluded=groups[2],
            epoch_review=review)
    except FrameRefused:
        raise
    except (ValueError, TypeError, KeyError, AttributeError) as error:
        raise FrameRefused('profile_malformed') from error


def _projection(values):
    return TransverseMercator(values['lat0'], values['lon0'], values['scale'],
                              values['false_east_m'], values['false_north_m'], values['unit_m'])
