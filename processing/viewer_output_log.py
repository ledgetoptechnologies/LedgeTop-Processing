"""Bounded console kept beside a Viewer task's inputs and results.

Node output lives in the node's memory and goes with its restart or cleanup.
Clients hold cursors into this list, so it only ever grows: a retained index
keeps its meaning across truncation and node restarts, and each gap is a marker
line in place. ``cursor`` counts lines read from the engine's current output.

Rereading one line cannot show the node still has this output: a restart can
repeat that line at the same index while earlier lines changed. ``anchor``
chains the digests of the output's first lines, which name one child run, and
``generation`` counts the engine outputs followed so far. Anything unproven is a
marker, so a false marker is possible and silent loss is not. ``stream`` names
this retained list: a cursor means something only for the stream it came from.
"""

import hashlib
import json
import secrets
import zipfile
import zlib

OUTPUT_LOG_NAME = '.ledgetop-output.json'
OUTPUT_LOG_VERSION = 2
OUTPUT_LOG_MAX_FILE_BYTES = 2 * 1024 ** 2
# Lines stop at the head limits. The rest of the total is for the markers and
# the final lines, which are where an engine failure is reported.
OUTPUT_HEAD_LINES = 8000
OUTPUT_HEAD_BYTES = 768 * 1024
OUTPUT_TOTAL_LINES = 10000
OUTPUT_TOTAL_BYTES = 1024 ** 2
OUTPUT_LINE_CHARS = 4096
OUTPUT_TAIL_LINES = 200
# The anchor must fit one bounded read of the output's start.
OUTPUT_ANCHOR_LINES = 8
OUTPUT_ANCHOR_BYTES = 256 * 1024
# NodeODX writes its whole console here, and the file survives in all.zip.
ARCHIVED_OUTPUT_NAME = 'task_output.txt'
ARCHIVED_OUTPUT_MAX_BYTES = 64 * 1024 ** 2
MARKER = '[ledgetop] '
CLIPPED = ' ' + MARKER + 'line truncated'


LEGACY_FIELDS = frozenset(('version', 'engine', 'cursor', 'last', 'omitted', 'omittedFrom', 'snapshot', 'lines'))


def _continuity_fields():
    return {'generation': 1, 'anchor': None, 'anchorLines': 0, 'verified': 0, 'unverifiable': False,
            'snapshotAttempts': 0}


def new_output_log(engine_uuid):
    return {'version': OUTPUT_LOG_VERSION, 'engine': engine_uuid, 'stream': secrets.token_hex(16), 'cursor': 0,
            'last': None, 'omitted': 0, 'omittedFrom': None, 'snapshot': None, 'lines': [], **_continuity_fields()}


def upgrade_output_log(log, engine_uuid):
    """Read a version 1 file. Having no anchor, its cursor proves nothing, and
    the first capture resets it. The stream is derived so reads agree until then.
    """
    if type(log) is dict and log.get('version') == 1 and set(log) == LEGACY_FIELDS and type(engine_uuid) is str:
        stream = hashlib.sha256(('legacy-output:' + engine_uuid).encode()).hexdigest()[:32]
        return {**log, 'version': OUTPUT_LOG_VERSION, 'stream': stream, **_continuity_fields()}
    return log


def valid_output_log(log, engine_uuid):
    return (type(log) is dict and set(log) == set(new_output_log(engine_uuid))
            and log['version'] == OUTPUT_LOG_VERSION and log['engine'] == engine_uuid
            and type(log['stream']) is str and len(log['stream']) == 32
            and type(log['generation']) is int and log['generation'] >= 1
            and (log['anchor'] is None or type(log['anchor']) is str)
            and type(log['anchorLines']) is int and 0 <= log['anchorLines'] <= OUTPUT_ANCHOR_LINES
            and type(log['verified']) is int and log['verified'] >= 0
            and type(log['unverifiable']) is bool
            and type(log['snapshotAttempts']) is int and log['snapshotAttempts'] >= 0
            and type(log['cursor']) is int and log['cursor'] >= 0
            and type(log['omitted']) is int and log['omitted'] >= 0
            and (log['last'] is None or type(log['last']) is str)
            and (log['omittedFrom'] is None or type(log['omittedFrom']) is int)
            and log['snapshot'] in (None, 'complete', 'incomplete')
            and type(log['lines']) is list and all(type(line) is str for line in log['lines']))


def output_digest(line):
    return hashlib.sha256(line.encode('utf-8', 'surrogatepass')).hexdigest()


def _anchor_digest(previous, line):
    return hashlib.sha256(((previous or '') + output_digest(line)).encode()).hexdigest()


def matches_anchor(log, lines):
    if len(lines) < log['anchorLines']:
        return False
    value = None
    for line in lines[:log['anchorLines']]:
        value = _anchor_digest(value, line)
    return value == log['anchor']


def _size(line):
    return len(json.dumps(line)) + 1


def _clip(line):
    return line if len(line) <= OUTPUT_LINE_CHARS else line[:OUTPUT_LINE_CHARS - len(CLIPPED)] + CLIPPED


def _used(log):
    return sum(_size(line) for line in log['lines'])


def _fits(log, used, line):
    return len(log['lines']) < OUTPUT_TOTAL_LINES and used + _size(line) <= OUTPUT_TOTAL_BYTES


def mark_output(log, text):
    """The total bound stops even markers, so repeated gaps stay bounded."""
    if _fits(log, _used(log), MARKER + text):
        log['lines'].append(MARKER + text)


def mark_unverifiable(log):
    """Say once per output that its continuity is not being proven."""
    if not log['unverifiable']:
        log['unverifiable'] = True
        mark_output(log, 'output continuity cannot be verified; lines may be missing')


def _extend_anchor(log, line):
    """Chain a leading line. One too long for the bounded read ends the anchor."""
    if log['anchorLines'] != log['cursor'] or log['cursor'] >= OUTPUT_ANCHOR_LINES:
        return
    if _size(line) > OUTPUT_ANCHOR_BYTES // OUTPUT_ANCHOR_LINES:
        if not log['cursor']:
            mark_unverifiable(log)
        return
    log['anchor'] = _anchor_digest(log['anchor'], line)
    log['anchorLines'] += 1


def consume_output(log, lines):
    used = _used(log)
    for line in lines:
        _extend_anchor(log, line)
        log['cursor'] += 1
        log['last'] = output_digest(line)
        kept = _clip(line)
        size = _size(kept)
        if log['omittedFrom'] is None and (used + size > OUTPUT_HEAD_BYTES
                                           or len(log['lines']) >= OUTPUT_HEAD_LINES):
            log['omittedFrom'] = log['cursor'] - 1
            mark_output(log, 'output limit reached; later lines are omitted')
        if log['omittedFrom'] is not None:
            log['omitted'] += 1
            continue
        log['lines'].append(kept)
        used += size


def skip_oversized_line(log):
    """Step past one line no bounded read can return, so reading cannot stall."""
    if not log['cursor']:
        mark_unverifiable(log)
    log['cursor'] += 1
    log['last'] = None
    log['omitted'] += 1
    mark_output(log, 'engine line over the read limit omitted')


def restart_output(log):
    """Keep everything retained and follow the engine's output from zero.

    Lines since the last proof may already belong to the new output, and lines
    between the two may be gone, so the marker says so rather than where.
    """
    mark_output(log, 'engine output restarted; lines may be missing; lines from index '
                + str(log['verified']) + ' to here are unproven')
    log.update(cursor=0, last=None, omittedFrom=None, generation=log['generation'] + 1, anchor=None,
               anchorLines=0, unverifiable=False, verified=len(log['lines']))


def append_output_tail(log, lines):
    mark_output(log, str(max(log['omitted'] - len(lines), 0)) + ' engine lines omitted; final lines follow')
    used = _used(log)
    for line in lines:
        kept = _clip(line)
        if not _fits(log, used, kept):
            break
        log['lines'].append(kept)
        used += _size(kept)


def archived_output_lines(stream):
    """The console inside an already verified archive, or None if not safely there."""
    try:
        with zipfile.ZipFile(stream) as archive:
            items = [item for item in archive.infolist() if item.filename == ARCHIVED_OUTPUT_NAME]
            if len(items) != 1 or items[0].file_size > ARCHIVED_OUTPUT_MAX_BYTES:
                return None
            with archive.open(items[0]) as member:  # The header's size is only a claim.
                raw = member.read(ARCHIVED_OUTPUT_MAX_BYTES + 1)
    except (zipfile.BadZipFile, OSError, ValueError, EOFError, RuntimeError, NotImplementedError, zlib.error):
        return None
    if not raw or len(raw) > ARCHIVED_OUTPUT_MAX_BYTES:
        return None
    lines = raw.decode('utf-8', 'replace').split('\n')
    return lines[:-1] if lines[-1] == '' else lines
