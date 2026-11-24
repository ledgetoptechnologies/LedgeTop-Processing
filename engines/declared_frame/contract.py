"""Normalize once, declare the 3D working frame, and check it where it is consumed."""
import hashlib
import json
import math

from .errors import FrameRefused
from .geodesy import GRID_SHA256
from .survey_frame import SurveyFile, check_fit_membership

SCHEMA = 'declared-frame-contract/v2'
MAX_REFERENCE_DIFFERENCE_M = 0.010
MAX_ROUND_TRIP_M = 0.001
MIN_GEOID_SHIFT_M = 1.0
MAX_HORIZONTAL_DIFFERENCE_DEG = 1e-8
HEIGHT_RANGE_M = (-500.0, 9000.0)
# The sink only passes numbers along, so it is held to storage precision.
SINK_DEG, SINK_M, SINK_PIXEL = 1e-12, 1e-9, 1e-12


def canonical_sha256(value):
    text = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(text.encode()).hexdigest()


def seal(body):
    body = {key: value for key, value in body.items() if key != 'sha256'}
    return {**body, 'sha256': canonical_sha256(body)}


def check_seal(value, schema, code):
    if not isinstance(value, dict) or value.get('schema') != schema:
        raise FrameRefused(code)
    body = {key: item for key, item in value.items() if key != 'sha256'}
    try:
        intact = value.get('sha256') == canonical_sha256(body)
    except (TypeError, ValueError):
        intact = False
    if not intact:
        raise FrameRefused(code + '_digest_mismatch')
    return value


def normalize_once(survey, profile, bridge, grid, ledger):
    """`grid` is the independent reader. `bridge` is the operation being checked."""
    if not isinstance(survey, SurveyFile):
        raise FrameRefused('already_normalized')
    ledger.claim('normalize', survey.raw_sha256)
    points = []
    for point_id, raw in survey.points().items():
        xyz = tuple(float(token) for token in raw)
        height_m = xyz[2] * profile.survey_unit_m
        if not HEIGHT_RANGE_M[0] <= height_m <= HEIGHT_RANGE_M[1]:
            raise FrameRefused('height_out_of_range')
        try:
            working = tuple(bridge.survey_to_ellipsoid(xyz))
            back = tuple(bridge.ellipsoid_to_survey(working))
            reference = profile.survey.inverse(xyz[0], xyz[1])
            expected = height_m + grid.undulation(*reference)
        except Exception as error:
            raise FrameRefused('vertical_operation_failed') from error
        _check_point(xyz, height_m, working, back, reference, expected, profile.survey_unit_m)
        points.append({
            'id': point_id, 'raw_xyz': list(raw), 'working': list(working),
            'marks': [{'filename': mark.filename, 'raw_pixel': list(mark.raw_pixel)}
                      for mark in survey.marks if mark.id == point_id]})
    return seal({
        'schema': SCHEMA, 'kind': profile.kind, 'profile_sha256': profile.sha256,
        'source': {**profile.source, 'raw_sha256': survey.raw_sha256,
                   'raw_header': survey.raw_header},
        'working_frame': profile.working_frame,
        'epoch_review': profile.epoch_review,
        'grid_sha256': GRID_SHA256,
        'vertical_application_count': 1,
        'points': points,
    })


def _check_point(xyz, height_m, working, back, reference, expected, unit):
    lon, lat, h = working
    if not all(math.isfinite(v) for v in (*working, *back, expected)):
        raise FrameRefused('nonfinite_normalization')
    if abs(h - height_m) < MIN_GEOID_SHIFT_M:
        raise FrameRefused('vertical_operation_identity')
    if abs(h - expected) > MAX_REFERENCE_DIFFERENCE_M:
        raise FrameRefused('vertical_operation_mismatch')
    if max(abs(lon - reference[0]), abs(lat - reference[1])) > MAX_HORIZONTAL_DIFFERENCE_DEG:
        raise FrameRefused('horizontal_operation_mismatch')
    if max(abs(a - b) for a, b in zip(xyz, back)) * unit > MAX_ROUND_TRIP_M:
        raise FrameRefused('round_trip_mismatch')


def verify_contract(contract, profile):
    """Consistency with the sealed profile. Who produced it is provenance.py's job."""
    check_seal(contract, SCHEMA, 'working_frame')
    if contract.get('profile_sha256') != profile.sha256 or contract.get('kind') != profile.kind:
        raise FrameRefused('profile_digest_mismatch')
    if contract.get('working_frame') != profile.working_frame:
        raise FrameRefused('working_frame_relabelled')
    source = contract.get('source', {})
    if any(source.get(key) != value for key, value in profile.source.items()):
        raise FrameRefused('source_frame_mismatch')
    if contract.get('epoch_review') != profile.epoch_review:
        raise FrameRefused('epoch_review_mismatch')
    if (contract.get('vertical_application_count') != 1
            or contract.get('grid_sha256') != GRID_SHA256):
        raise FrameRefused('working_frame_operation_mismatch')
    ids = [point['id'] for point in contract['points'] for _ in point['marks']]
    if len({point['id'] for point in contract['points']}) != len(contract['points']):
        raise FrameRefused('duplicate_point')
    check_fit_membership(ids, profile)
    return contract


def projection(raw_pixel, width, height):
    """The engine's own normalized image coordinates for a mark."""
    size = max(width, height)
    return ((float(raw_pixel[0]) + 0.5 - width / 2.0) / size,
            (float(raw_pixel[1]) + 0.5 - height / 2.0) / size)


def verify_sink(contract, profile, seen, sizes):
    """Compare every field of the objects the solver will read.

    `seen` is a list of {id, longitude, latitude, height, has_height, observations}
    read back from those objects. `sizes` maps an image name to (width, height).
    """
    verify_contract(contract, profile)
    expected = {point['id']: point for point in contract['points']}
    ids = [item['id'] for item in seen]
    if len(ids) != len(set(ids)):
        raise FrameRefused('sink_duplicate_id')
    if len(ids) != len(expected):
        raise FrameRefused('sink_count_mismatch', {'seen': len(ids), 'expected': len(expected)})
    if set(ids) != set(expected):
        raise FrameRefused('sink_id_mismatch')
    for item in seen:
        point = expected[item['id']]
        lon, lat, h = point['working']
        place = (item['longitude'], item['latitude'], item['height'])
        if not all(isinstance(v, float) and math.isfinite(v) for v in place):
            raise FrameRefused('sink_nonfinite', {'id': item['id']})
        if abs(place[0] - lon) > SINK_DEG or abs(place[1] - lat) > SINK_DEG:
            swapped = abs(place[0] - lat) <= SINK_DEG and abs(place[1] - lon) <= SINK_DEG
            raise FrameRefused('sink_axis_order' if swapped else 'sink_horizontal_mismatch',
                               {'id': item['id']})
        if item['has_height'] is not True or abs(place[2] - h) > SINK_M:
            raise FrameRefused('sink_height_mismatch', {'id': item['id']})
        _verify_observations(point, item['observations'], sizes)
    return seal({'schema': 'declared-frame-sink/v1', 'contract_sha256': contract['sha256'],
                 'points': len(ids), 'observations': sum(len(i['observations']) for i in seen),
                 'seen_sha256': canonical_sha256(sorted(seen, key=lambda i: i['id']))})


def _verify_observations(point, observations, sizes):
    wanted = {}
    for mark in point['marks']:
        if mark['filename'] not in sizes:
            raise FrameRefused('sink_image_missing', {'id': point['id']})
        wanted[mark['filename']] = projection(mark['raw_pixel'], *sizes[mark['filename']])
    shots = [shot for shot, _, _ in observations]
    if len(shots) != len(set(shots)) or set(shots) != set(wanted):
        raise FrameRefused('sink_observation_mismatch', {'id': point['id']})
    for shot, x, y in observations:
        if not all(isinstance(v, float) and abs(v - w) <= SINK_PIXEL
                   for v, w in zip((x, y), wanted[shot])):
            raise FrameRefused('sink_observation_mismatch', {'id': point['id']})
