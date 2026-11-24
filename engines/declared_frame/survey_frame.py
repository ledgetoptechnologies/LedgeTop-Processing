"""Opt-in survey input: keep the raw record, refuse anything the profile omits.

The opt-in is the first line of the control file. The engine's own parser rejects
it, so an engine without the hooks stops instead of reading through the legacy path.
"""
import hashlib
import re
from dataclasses import dataclass

from .errors import FrameRefused

HEADER_MARKER = 'SURVEY3D'
HEADER_VERSION = 'SURVEY3D/2'
_FIELDS = {'CRS': 'unsupported_frame', 'UNITS': 'unsupported_units', 'AXES': 'unsupported_axes',
           'REALIZATION': 'unsupported_frame', 'EPOCH': 'unsupported_epoch',
           'GEOID': 'unsupported_geoid'}
MAX_BYTES = 2 * 1024 * 1024
_DECIMAL = re.compile('-?[0-9]+([.][0-9]+)?')


@dataclass(frozen=True)
class Mark:
    id: str
    raw_xyz: tuple
    raw_pixel: tuple
    filename: str


@dataclass(frozen=True)
class SurveyFile:
    raw_sha256: str
    raw_header: str
    profile_sha256: str
    marks: tuple

    def points(self):
        return {mark.id: mark.raw_xyz for mark in self.marks}


def header_for(profile):
    values = {key: profile.source[key.lower()] for key in _FIELDS}
    return ' '.join([HEADER_VERSION, f'PROFILE={profile.sha256}']
                    + [f'{key}={value}' for key, value in values.items()])


def is_opt_in(raw):
    """True for any version or spelling of the marker, so none falls to legacy."""
    text = raw.decode('utf-8', errors='replace').replace(chr(0xFEFF), '').strip()
    return text.upper().startswith(HEADER_MARKER)


def read_survey(raw, profile):
    if not isinstance(raw, bytes):
        raise FrameRefused('raw_bytes_required')
    if len(raw) > MAX_BYTES:
        raise FrameRefused('survey_file_too_large')
    if not is_opt_in(raw):
        raise FrameRefused('survey_opt_in_required')
    try:
        text = raw.decode('utf-8').replace(chr(0xFEFF), '').strip()
    except UnicodeDecodeError as error:
        raise FrameRefused('survey_text_unreadable') from error
    lines = [line.strip() for line in text.splitlines()]
    _check_header(lines[0], profile)
    marks = tuple(_mark(line) for line in lines[1:] if line and not line.startswith('#'))
    if not marks:
        raise FrameRefused('survey_marks_required')
    _check_identity(marks)
    check_fit_membership([mark.id for mark in marks], profile)
    return SurveyFile(raw_sha256=hashlib.sha256(raw).hexdigest(), raw_header=lines[0],
                      profile_sha256=profile.sha256, marks=marks)


def _check_header(line, profile):
    parts = line.split()
    if parts[0] != HEADER_VERSION:
        raise FrameRefused('unsupported_survey_version')
    values = {}
    for part in parts[1:]:
        key, separator, value = part.partition('=')
        if not separator or key in values or key not in (*_FIELDS, 'PROFILE'):
            raise FrameRefused('malformed_survey_header')
        values[key] = value
    for key, code in _FIELDS.items():
        if values.get(key) != profile.source[key.lower()]:
            raise FrameRefused(code, {'field': key})
    if values.get('PROFILE') != profile.sha256:
        raise FrameRefused('profile_digest_mismatch')


def _mark(line):
    parts = line.split()
    if len(parts) != 7:
        raise FrameRefused('malformed_survey_mark')
    # Text, not float(): keeps the surveyed digits and rejects nan, inf and 1_0.
    if not all(_DECIMAL.fullmatch(part) for part in parts[:5]):
        raise FrameRefused('invalid_coordinate')
    return Mark(id=parts[6], raw_xyz=tuple(parts[:3]), raw_pixel=tuple(parts[3:5]),
                filename=parts[5])


def _check_identity(marks):
    seen, shots = {}, set()
    for mark in marks:
        if seen.setdefault(mark.id, mark.raw_xyz) != mark.raw_xyz:
            raise FrameRefused('conflicting_point_coordinates')
        if (mark.id, mark.filename) in shots:
            raise FrameRefused('duplicate_mark')
        shots.add((mark.id, mark.filename))
    # The legacy reader keys points by coordinates, so two IDs there would merge.
    if len({tuple(float(v) for v in xyz) for xyz in seen.values()}) != len(seen):
        raise FrameRefused('coincident_points')


def check_fit_membership(ids, profile):
    """Exactly the profile's fit points and marks. Case and CHK- do not change a role."""
    withheld = {label.upper() for label in profile.withheld}
    excluded = {label.upper() for label in profile.excluded}
    for label in ids:
        bare = label.upper()
        while bare.startswith('CHK-'):
            bare = bare[4:]
        if bare in withheld:
            raise FrameRefused('withheld_point_before_render')
        if bare in excluded:
            raise FrameRefused('excluded_point')
        if label not in profile.fit:
            raise FrameRefused('unknown_point')
    if any(ids.count(label) != profile.marks_per_point for label in profile.fit):
        raise FrameRefused('fit_membership_mismatch')
