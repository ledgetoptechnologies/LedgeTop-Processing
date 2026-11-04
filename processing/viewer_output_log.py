"""Bounded console kept beside a Viewer task's inputs and results.

Node output lives in the node's memory and goes with its restart or cleanup.
Clients hold cursors into this list, so it only ever grows: a retained index
keeps its meaning across truncation and node restarts, and each gap is a marker
line in place. ``cursor`` counts lines read from the engine's current output.
"""

import hashlib
import json

OUTPUT_LOG_NAME = '.ledgetop-output.json'
OUTPUT_LOG_VERSION = 1
OUTPUT_LOG_MAX_FILE_BYTES = 2 * 1024 ** 2
# Lines stop at the head limits. The rest of the total is for the markers and
# the final lines, which are where an engine failure is reported.
OUTPUT_HEAD_LINES = 8000
OUTPUT_HEAD_BYTES = 768 * 1024
OUTPUT_TOTAL_LINES = 10000
OUTPUT_TOTAL_BYTES = 1024 ** 2
OUTPUT_LINE_CHARS = 4096
OUTPUT_TAIL_LINES = 200
MARKER = '[ledgetop] '
CLIPPED = ' ' + MARKER + 'line truncated'


def new_output_log(engine_uuid):
    return {'version': OUTPUT_LOG_VERSION, 'engine': engine_uuid, 'cursor': 0, 'last': None,
            'omitted': 0, 'omittedFrom': None, 'snapshot': None, 'lines': []}


def valid_output_log(log, engine_uuid):
    return (type(log) is dict and set(log) == set(new_output_log(engine_uuid))
            and log['version'] == OUTPUT_LOG_VERSION and log['engine'] == engine_uuid
            and type(log['cursor']) is int and log['cursor'] >= 0
            and type(log['omitted']) is int and log['omitted'] >= 0
            and (log['last'] is None or type(log['last']) is str)
            and (log['omittedFrom'] is None or type(log['omittedFrom']) is int)
            and log['snapshot'] in (None, 'complete', 'incomplete')
            and type(log['lines']) is list and all(type(line) is str for line in log['lines']))


def output_digest(line):
    return hashlib.sha256(line.encode('utf-8', 'surrogatepass')).hexdigest()


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


def consume_output(log, lines):
    used = _used(log)
    for line in lines:
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
    log['cursor'] += 1
    log['last'] = None
    log['omitted'] += 1
    mark_output(log, 'engine line over the read limit omitted')


def restart_output(log):
    """Keep everything retained and follow the engine's new output from zero."""
    mark_output(log, 'engine output restarted; lines above are from before the restart')
    log.update(cursor=0, last=None, omittedFrom=None)


def append_output_tail(log, lines):
    mark_output(log, str(max(log['omitted'] - len(lines), 0)) + ' engine lines omitted; final lines follow')
    used = _used(log)
    for line in lines:
        kept = _clip(line)
        if not _fits(log, used, kept):
            break
        log['lines'].append(kept)
        used += _size(kept)
