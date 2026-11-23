"""Opt-in compound survey input: keep the raw record, refuse anything unstated.

The opt-in is the first line of the control file. The line is not a CRS the
engine's own parser accepts, so an engine without this adapter stops on it
instead of reading the file through the legacy path.
"""
import hashlib
import re
from dataclasses import dataclass

HEADER_MARKER = 'SURVEY3D'
HEADER_VERSION = 'SURVEY3D/1'
HEADER_FIELDS = {
    'CRS': ('EPSG:7600+6360', 'unsupported_frame'),
    'UNITS': ('ftUS,ftUS,ftUS', 'unsupported_units'),
    'AXES': ('E,N,H', 'unsupported_axes'),
    'REALIZATION': ('NAD83(2011)', 'unsupported_frame'),
    'EPOCH': ('2010.00', 'unsupported_epoch'),
    'GEOID': ('GEOID18', 'unsupported_geoid'),
}
ROLE_PROFILES = ('SYNTHETIC', 'SIX-FIT')
FIT_LABELS = ('1a', '5a', '6a', '7a', '8a', '9a')
MARKS_PER_FIT_LABEL = 3
EVALUATION_LABELS = ('2a', '4a')
EXCLUDED_LABELS = ('3a',) + tuple(str(n) for n in range(1, 10))
MAX_BYTES = 2 * 1024 * 1024
_DECIMAL = re.compile('-?[0-9]+([.][0-9]+)?')
_SYNTHETIC_ID = re.compile('SYN-[A-Z0-9]{1,16}')


class SurveyRefused(RuntimeError):
    """A refusal with a stable code. RuntimeError, so the engine stops on it."""

    def __init__(self, code, details=None):
        super().__init__(code)
        self.code = code
        self.details = details or {}


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
    crs: str
    units: str
    axes: str
    realization: str
    epoch: str
    geoid: str
    roles: str
    marks: tuple

    def points(self):
        return {mark.id: mark.raw_xyz for mark in self.marks}


def is_opt_in(raw):
    """True for any version or spelling of the marker, so none falls to legacy."""
    text = raw.decode('utf-8', errors='replace').replace(chr(0xFEFF), '').strip()
    return text.upper().startswith(HEADER_MARKER)


def read_survey(raw):
    if not isinstance(raw, bytes):
        raise SurveyRefused('raw_bytes_required')
    if len(raw) > MAX_BYTES:
        raise SurveyRefused('survey_file_too_large')
    if not is_opt_in(raw):
        raise SurveyRefused('survey_opt_in_required')
    try:
        text = raw.decode('utf-8').replace(chr(0xFEFF), '').strip()
    except UnicodeDecodeError as error:
        raise SurveyRefused('survey_text_unreadable') from error
    lines = [line.strip() for line in text.splitlines()]
    values = _header(lines[0])
    marks = tuple(_mark(line) for line in lines[1:] if line and not line.startswith('#'))
    if not marks:
        raise SurveyRefused('survey_marks_required')
    survey = SurveyFile(
        raw_sha256=hashlib.sha256(raw).hexdigest(), raw_header=lines[0],
        crs=values['CRS'], units=values['UNITS'], axes=values['AXES'],
        realization=values['REALIZATION'], epoch=values['EPOCH'],
        geoid=values['GEOID'], roles=values['ROLES'], marks=marks)
    _check_identity(survey)
    _check_roles(survey)
    return survey


def _header(line):
    parts = line.split()
    if parts[0] != HEADER_VERSION:
        raise SurveyRefused('unsupported_survey_version')
    values = {}
    for part in parts[1:]:
        key, separator, value = part.partition('=')
        if not separator or key in values or key not in (*HEADER_FIELDS, 'ROLES'):
            raise SurveyRefused('malformed_survey_header')
        values[key] = value
    for key, (required, code) in HEADER_FIELDS.items():
        if values.get(key) != required:
            raise SurveyRefused(code, {'field': key})
    if values.get('ROLES') not in ROLE_PROFILES:
        raise SurveyRefused('unsupported_role_profile')
    return values


def _mark(line):
    parts = line.split()
    if len(parts) != 7:
        raise SurveyRefused('malformed_survey_mark')
    # Text, not float(): keeps the surveyed digits and rejects nan, inf and 1_0.
    if not all(_DECIMAL.fullmatch(part) for part in parts[:5]):
        raise SurveyRefused('invalid_coordinate')
    return Mark(id=parts[6], raw_xyz=tuple(parts[:3]), raw_pixel=tuple(parts[3:5]),
                filename=parts[5])


def _check_identity(survey):
    seen, shots = {}, set()
    for mark in survey.marks:
        if seen.setdefault(mark.id, mark.raw_xyz) != mark.raw_xyz:
            raise SurveyRefused('conflicting_point_coordinates')
        if (mark.id, mark.filename) in shots:
            raise SurveyRefused('duplicate_mark')
        shots.add((mark.id, mark.filename))
    # The engine keys points by coordinates, so two IDs there would silently merge.
    if len({tuple(float(v) for v in xyz) for xyz in seen.values()}) != len(seen):
        raise SurveyRefused('coincident_points')


def _check_roles(survey):
    ids = [mark.id for mark in survey.marks]
    if survey.roles == 'SYNTHETIC':
        if not all(_SYNTHETIC_ID.fullmatch(i) for i in ids):
            raise SurveyRefused('synthetic_ids_required')
        return
    for label in ids:
        bare = label[4:] if label.upper().startswith('CHK-') else label
        if bare in EVALUATION_LABELS:
            raise SurveyRefused('evaluation_point_before_render')
        if bare in EXCLUDED_LABELS:
            raise SurveyRefused('excluded_point')
        if label not in FIT_LABELS:
            raise SurveyRefused('unknown_point')
    if any(ids.count(label) != MARKS_PER_FIT_LABEL for label in FIT_LABELS):
        raise SurveyRefused('fit_membership_mismatch')
