"""Normalize once, state the 3D working frame, and refuse a handoff that loses it."""
import hashlib
import json
import math
from pathlib import Path
from types import MappingProxyType

from .grid_reference import EPSG_7600, GRID_NAME, GRID_SHA256, GeoidGrid, US_SURVEY_FOOT_M
from .patch_engine import LOCATION_SHA256, ORIGINAL_SHA256, PATCHED_SHA256
from .survey_frame import HEADER_FIELDS, SurveyFile, SurveyRefused, read_survey

SCHEMA = 'survey-working-frame/v1'
MAX_REFERENCE_DIFFERENCE_M = 0.010
MAX_ROUND_TRIP_M = 0.001
MIN_GEOID_SHIFT_M = 1.0
MAX_HORIZONTAL_DIFFERENCE_DEG = 1e-8
HEIGHT_RANGE_M = (-500.0, 9000.0)
WORKING_FRAME = MappingProxyType({
    'crs': 'EPSG:6319',
    'realization': 'NAD83(2011)',
    'epoch': '2010.00',
    'axes': 'longitude,latitude,ellipsoidal_height',
    'units': 'degree,degree,metre',
    'height': 'GRS80 ellipsoidal',
})
NORMALIZATION_OPERATION = 'EPSG:7600 to EPSG:6318; GEOID18 H to h'
REFUSED = 'engine_import_handoff_cannot_preserve_working_frame'

# Where the engine import path drops the frame. "hash" means the cited file is
# pinned by SHA-256; "retained copy" means the line comes from a saved copy whose
# installed hash was not recorded.
IMPORT_LOSSES = (
    ('modules/location.py:69-76', 'hash',
     'a compound CRS has no single EPSG code, so it goes through to_proj4() and '
     'ImportFromProj4(); the vertical datum is gone before OSR sees it'),
    ('modules/gcp.py:34', 'hash',
     'the header becomes a CRS only; epoch and realization have no field'),
    ('modules/gcp.py:135,147-153', 'hash',
     'every accepted file is rewritten as WGS84 UTM: a 2D CRS with no height datum'),
    ('modules/types.py:194,207', 'retained copy',
     'that rewritten 2D file replaces the caller file for the rest of the run'),
    ('modules/gcp.py:203-209', 'hash',
     'the OpenSfM file is written as EPSG:4979 under a fixed "crs": "WGS84", with '
     'height scaled by the horizontal unit when the CRS has two axes'),
    ('opensfm/dataset.py:641-643', 'hash',
     'the point reader discards the CRS it was given'),
    ('opensfm/dataset.py:458-474', 'hash',
     'the stored reference is latitude, longitude and altitude with no frame'),
)
# Empty on purpose: no transformation into the engine's own frame has been
# authorized. An entry needs a reviewed source change; callers cannot supply one.
AUTHORIZED_FRAME_TRANSFORMS = ()


def canonical_sha256(value):
    text = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(text.encode()).hexdigest()


class Ledger:
    """One vertical application per distinct input within an attempt."""

    def __init__(self):
        self._used = set()

    def claim(self, key):
        if key in self._used:
            raise SurveyRefused('already_normalized')
        self._used.add(key)


def normalize_once(survey, bridge, grid, ledger):
    """Return the declared contract. `grid` is the independent reader, not PROJ."""
    if not isinstance(survey, SurveyFile):
        raise SurveyRefused('already_normalized')
    ledger.claim(survey.raw_sha256)
    points = []
    for point_id, raw in survey.points().items():
        xyz = tuple(float(token) for token in raw)
        height_m = xyz[2] * US_SURVEY_FOOT_M
        if not HEIGHT_RANGE_M[0] <= height_m <= HEIGHT_RANGE_M[1]:
            raise SurveyRefused('height_out_of_range')
        try:
            working = tuple(bridge.survey_to_ellipsoid(xyz))
            back = tuple(bridge.ellipsoid_to_survey(working))
            reference = EPSG_7600.inverse(xyz[0], xyz[1])
            expected = height_m + grid.undulation(*reference)
        except Exception as error:
            raise SurveyRefused('vertical_operation_failed') from error
        _check_point(xyz, height_m, working, back, reference, expected)
        points.append({'id': point_id, 'raw_xyz': list(raw), 'working': list(working)})
    contract = {
        'schema': SCHEMA,
        'source': {key: getattr(survey, key) for key in
                   ('raw_sha256', 'raw_header', 'crs', 'units', 'axes', 'realization',
                    'epoch', 'geoid', 'roles')},
        'working_frame': dict(WORKING_FRAME),
        'operation': NORMALIZATION_OPERATION,
        'grid_sha256': GRID_SHA256,
        'vertical_application_count': 1,
        'points': points,
    }
    return {**contract, 'sha256': canonical_sha256(contract)}


def _check_point(xyz, height_m, working, back, reference, expected):
    lon, lat, h = working
    if not all(math.isfinite(v) for v in (*working, *back, expected)):
        raise SurveyRefused('nonfinite_normalization')
    if abs(h - height_m) < MIN_GEOID_SHIFT_M:
        raise SurveyRefused('vertical_operation_identity')
    if abs(h - expected) > MAX_REFERENCE_DIFFERENCE_M:
        raise SurveyRefused('vertical_operation_mismatch')
    if max(abs(lon - reference[0]), abs(lat - reference[1])) > MAX_HORIZONTAL_DIFFERENCE_DEG:
        raise SurveyRefused('horizontal_operation_mismatch')
    if max(abs(a - b) for a, b in zip(xyz, back)) * US_SURVEY_FOOT_M > MAX_ROUND_TRIP_M:
        raise SurveyRefused('round_trip_mismatch')


def verify_contract(contract):
    """Structure and digest only. It proves consistency, not who produced it."""
    if not isinstance(contract, dict) or contract.get('schema') != SCHEMA:
        raise SurveyRefused('working_frame_required')
    body = {key: value for key, value in contract.items() if key != 'sha256'}
    if contract.get('sha256') != canonical_sha256(body):
        raise SurveyRefused('working_frame_digest_mismatch')
    if body.get('working_frame') != dict(WORKING_FRAME):
        raise SurveyRefused('working_frame_relabelled')
    if (body.get('vertical_application_count') != 1 or not body.get('points')
            or body.get('grid_sha256') != GRID_SHA256):
        raise SurveyRefused('working_frame_operation_mismatch')
    for key, (required, code) in HEADER_FIELDS.items():
        if body.get('source', {}).get(key.lower()) != required:
            raise SurveyRefused(code, {'field': key})
    return contract


def require_preserved(contract, observed):
    """Compare what an engine reader handed on with the contract, field by field."""
    verify_contract(contract)
    lost = sorted(key for key, value in WORKING_FRAME.items() if observed.get(key) != value)
    if lost:
        raise SurveyRefused(REFUSED, {'lost': lost})
    for point in contract['points']:
        seen = observed.get('heights_m', {}).get(point['id'])
        if (not isinstance(seen, float) or not math.isfinite(seen)
                or abs(seen - point['working'][2]) > MAX_REFERENCE_DIFFERENCE_M):
            raise SurveyRefused('engine_height_mismatch', {'id': point['id']})


def engine_modules(engine_root):
    result = {}
    for name in ('modules/gcp.py', 'modules/location.py'):
        path = Path(engine_root) / name
        result[name] = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
    return result


def cross_import_handoff(contract, engine_root):
    """The only door into the engine. It is closed for the pinned main engine."""
    verify_contract(contract)
    found = engine_modules(engine_root)
    if (found['modules/gcp.py'] not in (ORIGINAL_SHA256, PATCHED_SHA256)
            or found['modules/location.py'] != LOCATION_SHA256):
        raise SurveyRefused('engine_import_boundary_unqualified', {'modules': found})
    # The pinned reader is known to drop the frame, and AUTHORIZED_FRAME_TRANSFORMS
    # is empty, so there is nothing to hand over.
    raise SurveyRefused(REFUSED, {'lost': sorted(WORKING_FRAME),
                                  'at': [item[0] for item in IMPORT_LOSSES]})


def import_survey_gcp(gcp_path, engine_root, *, grid_path=None, bridge=None, ledger=None):
    """Called by the opt-in hook in the engine reader. It never falls to legacy."""
    survey = read_survey(Path(gcp_path).read_bytes())
    path = Path(grid_path) if grid_path else Path(__file__).with_name(GRID_NAME)
    try:
        grid = GeoidGrid(path)
    except ValueError as error:
        raise SurveyRefused(str(error)) from error
    if bridge is None:
        from .vertical_bridge import SurveyVerticalBridge
        bridge = SurveyVerticalBridge(path)
    contract = normalize_once(survey, bridge, grid, ledger or Ledger())
    cross_import_handoff(contract, engine_root)
