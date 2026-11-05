"""Bounded, path-free execution output DTO shared by existing read adapters."""
import codecs
import json
import re
from urllib.parse import parse_qs, urlsplit

from . import reference_protocol as p

MAX_LOG_BYTES = 256 * 1024
MAX_CURSOR = 9999999999999
MAX_MESSAGE_CHARS = 4000
# A longer line becomes a placeholder: cutting before redaction could split a
# secret from the pattern that recognizes it, so it is never partly shown.
MAX_LINE_BYTES = 32 * 1024
# Page states. "unavailable" is deliberately absent: it is only ever the 503 body.
STATES = ('pending', 'open', 'complete', 'expired')
# No value here claims continuity; a positive cursor can only be disproved.
CONTINUITY = ('unverified', 'reset', 'unknown')
REASONS = ('engine_unavailable', 'engine_identity', 'engine_task_missing', 'execution_unverified',
           'not_configured', 'not_shared', 'unsupported', 'worker_unavailable')
TERMINAL_JOB_STATUSES = ('succeeded', 'failed', 'cancelled')
PRE_ENGINE_JOB_PHASES = ('queued', 'waiting_capacity', 'awaiting_transfer', 'fetching', 'source_receiving')
ENGINE_PRESTART_PHASES = ('prepared', 'initialize_pending', 'upload_pending')
ENGINE_FINISHED_PHASES = ('downloading', 'archive_open_pending', 'archive_pending', 'archive_ready',
                          'awaiting_delivery', 'delivered')
_REASON_CODES = {'unknown local execution': 'execution_unverified',
                 'execution_logs_binding_mismatch': 'execution_unverified',
                 'execution_logs_unsupported': 'unsupported',
                 'execution_logs_unavailable': 'engine_unavailable',
                 'engine_read_unavailable': 'engine_unavailable',
                 'engine_output_unavailable': 'engine_unavailable',
                 'physical_engine_identity_unavailable': 'engine_identity',
                 'physical_engine_identity_changed': 'engine_identity'}

REDACTED = '[redacted]'
_ANSI = re.compile(r'\x1b(?:\[[0-?]*[ -/]*[@-~]?|\][^\x07\x1b]*(?:\x07|\x1b\\)?'
                   r'|[PX^_][^\x1b]*(?:\x1b\\)?|[ -/]*[0-~]?)|\x9b[0-?]*[ -/]*[@-~]?')
_SPACES = re.compile('[\t\n\x0b\x0c\r\x85\u2028\u2029]')
_INVISIBLE = re.compile('[\x00-\x1f\x7f-\x9f\u061c\u200b-\u200f\u202a-\u202e\u2060-\u2069\ufeff]')
_SURROGATES = re.compile('[\ud800-\udfff]')
_PEM = re.compile(r'-----BEGIN [A-Z0-9 ]{0,40}PRIVATE KEY.*')
# Every prefix is anchored on a fixed word with bounded repeats: redaction runs
# on untrusted lines inside the engine gate, so it must stay near-linear.
_HEADER = re.compile(r"""((?:proxy-)?authorization|(?:set-)?cookie|x-api-key|x-auth-token|x-amz-security-token)"""
                     r"""(\\?["']?\s{0,8}[:=]\s{0,8}).*""", re.I)
_BEARER = re.compile(r"""\b(bearer)\s{1,8}[^\s'"]+""", re.I)
_WORD = re.compile(r'[^\s<>]+')
_KEYS = (r'(?:token|secret|passw(?:or)?d|(?<![a-z])pass(?:phrase)?(?![a-z])|(?<![a-z])pwd|api[_-]?key|access[_-]?key'
         r'|private[_-]?key|auth[_-]?key|credential|signature|(?<![a-z])sig(?![a-z])|session[_-]?id|(?<![a-z])auth(?![a-z]))')
_VALUE = r"""(?:[\[{(].*|\\"(?:(?!\\").)*(?:\\")?|"(?:\\.|[^"\\])*"?|'(?:\\.|[^'\\])*'?|[^\s&'"]+)"""
_ASSIGNED = re.compile('(' + _KEYS + r"""[\w.\-]{0,64})(\\?["']?\s{0,8}(?:=>|:=|=|:)\s{0,8})""" + _VALUE, re.I)
_FLAG = re.compile(r'((?<![\w-])--?[\w\-]{0,64}?' + _KEYS + r'[\w\-]{0,64}\s{1,8})' + _VALUE, re.I)
_BASIC_FLAG = re.compile(r'((?<![\w-])(?:--user|-u)\s{1,8})\S+:\S+')
_SHAPES = re.compile(r'\b(?:AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9]{20,}|github_pat_\w{20,}|xox[abprs]-[\w\-]{10,}'
                     r'|sk-[\w\-]{20,}|eyJ[\w\-]{8,}\.[\w\-]{8,}\.[\w\-]*)')
_PATHS = re.compile(r"""\bfile:/+[^\s'"]*|(?<![A-Za-z0-9])[A-Za-z]:[\\/][^\s'"|]*|\\\\[^\s'"|]+"""
                    r"""|(?<![\w:/.\-~])~?(?:/[\w.@%+\-~]+){2,}/?""")


def encoded_size(value):
    # Match the existing HTTP JSON writer, including Unicode escaping.
    return len(json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(',', ':')).encode('utf-8'))


def omitted(size, *, over=False):
    return '[line omitted: %s%d bytes]' % ('over ' if over else '', size)


def _userinfo(match):
    word = match[0]
    start, end = word.find('://'), word.rfind('@')
    return word if start < 0 or end < start else word[:start + 3] + REDACTED + word[end:]


def sanitize(text):
    """Return the same single line without secrets, host paths or live markup.

    Heuristic by shape, so unusual or multi-line secrets can pass. Rules run on
    text cleaned of escapes first; an escape cannot split a recognized word.
    """
    text = _SURROGATES.sub('\ufffd', _ANSI.sub('', text))
    text = _INVISIBLE.sub('', _SPACES.sub(' ', text))
    text = _PEM.sub(REDACTED, text)
    text = _HEADER.sub(lambda m: m[1] + m[2] + REDACTED, text)
    text = _WORD.sub(_userinfo, text)
    text = _BEARER.sub(lambda m: m[1] + ' ' + REDACTED, text)
    text = _ASSIGNED.sub(lambda m: m[1] + m[2] + REDACTED, text)
    text = _FLAG.sub(lambda m: m[1] + REDACTED, text)
    text = _BASIC_FLAG.sub(lambda m: m[1] + REDACTED, text)
    text = _SHAPES.sub(REDACTED, text)
    return _PATHS.sub('[path]', text).replace('<', '\u2039')


def message(text):
    """One engine line to one displayable message; this never raises."""
    try:
        size = len(text.encode('utf-8', 'surrogatepass'))
        return omitted(size) if size > MAX_LINE_BYTES else sanitize(text)[:MAX_MESSAGE_CHARS]
    except Exception:
        return '[line omitted: unreadable]'


def reason(error, default='worker_unavailable'):
    """Map an internal failure to a closed reason; its text is never exposed."""
    text = str(error)
    return text if text in REASONS else _REASON_CODES.get(text, default)


def worker_reason(status, raw):
    try:
        code = json.loads(raw)['error']
    except (ValueError, TypeError, KeyError):
        code = None
    if type(code) is str and code in REASONS:
        return code
    return {403: 'not_shared', 409: 'execution_unverified'}.get(status, 'worker_unavailable')


def unavailable(cause):
    return dict(error='execution_logs_unavailable', state='unavailable',
                reason=cause if cause in REASONS else 'worker_unavailable')


def read_output_prefix(response, limit):
    """Consume only a bounded JSON string-array prefix, plus one lookahead line.

    NodeODM returns the entire remaining tail. Close its response after a valid
    lookahead line; an EOF page instead requires the closed array and clean EOF.
    A line that outgrows MAX_LINE_BYTES ends the read with one placeholder, so
    the cursor passes it without reading its remainder. Invalid text is kept as
    a replaced line; only broken array structure fails the page.
    The caller owns and closes response on every outcome.
    """
    decoder = codecs.getincrementaldecoder('utf-8')('replace')
    parser = json.JSONDecoder(strict=False)
    buffer, cursor, count, state, eof = '', 0, 0, 'start', False
    lines = []
    page_limit = limit
    while True:
        while cursor < len(buffer) and buffer[cursor] in ' \t\r\n':
            cursor += 1
        if cursor < len(buffer):
            char = buffer[cursor]
            if state == 'start':
                if char != '[':
                    raise ValueError('invalid engine output')
                cursor += 1
                state = 'first'
                continue
            if state == 'done':
                raise ValueError('invalid engine output tail')
            if state == 'separator':
                if char not in ',]':
                    raise ValueError('invalid engine output separator')
                if len(lines) > page_limit:
                    return lines[:page_limit], True
                cursor += 1
                state = 'value' if char == ',' else 'done'
                continue
            if state == 'first' and char == ']':
                cursor += 1
                state = 'done'
                continue
            if char != '"':
                raise ValueError('engine output must contain strings')
            try:
                text, end = parser.raw_decode(buffer, cursor)
            except json.JSONDecodeError:
                if eof:
                    raise ValueError('incomplete engine output') from None
                if len(buffer[cursor:].encode('utf-8', 'replace')) > MAX_LINE_BYTES:
                    if len(lines) >= page_limit:
                        return lines[:page_limit], True
                    return lines + [omitted(MAX_LINE_BYTES, over=True)], True
            else:
                lines.append(text)
                # Leave room for one validated lookahead line. Many moderately
                # long lines must still advance, even at a requested limit of 200.
                if len(lines) <= page_limit and count > MAX_LOG_BYTES // 2:
                    page_limit = len(lines)
                cursor, state = end, 'separator'
                continue
        if eof:
            if state != 'done':
                raise ValueError('incomplete engine output')
            return lines, False
        chunk = response.read(min(4096, MAX_LOG_BYTES + 1 - count))
        if type(chunk) is not bytes:
            raise ValueError('invalid engine output transport')
        count += len(chunk)
        if count > MAX_LOG_BYTES:
            raise ValueError('engine output prefix too large')
        eof = not chunk
        buffer = buffer[cursor:] + decoder.decode(chunk, final=eof)
        cursor = 0


def request(path, prefix):
    parsed = urlsplit(path)
    match = re.fullmatch(re.escape(prefix) + r'/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})/logs', parsed.path)
    if not match:
        return None
    values = parse_qs(parsed.query, strict_parsing=True, keep_blank_values=True)
    if parsed.scheme or parsed.netloc or parsed.fragment or set(values) != {'generation', 'after', 'limit'}:
        raise ValueError('invalid log request')
    if any(len(value) != 1 or not re.fullmatch(r'0|[1-9][0-9]{0,15}', value[0]) for value in values.values()):
        raise ValueError('invalid log request')
    generation, after, limit = (int(values[key][0]) for key in ('generation', 'after', 'limit'))
    bounds(generation, after, limit)
    return match[1], generation, after, limit


def bounds(generation, after, limit):
    p._integer(generation, 'log generation')
    if type(after) is not int or not 0 <= after <= MAX_CURSOR or type(limit) is not int or not 1 <= limit <= 200:
        raise ValueError('invalid log bounds')


def page(job, lines, after, has_more=False, *, state='open', continuity='unverified'):
    return dict(jobId=job['jobId'], attemptId=job['attemptId'], generation=job['generation'],
                environment=job['environment'], nodeId=job['nodeId'], lines=lines,
                nextCursor=0 if continuity == 'reset' else after + len(lines), hasMore=has_more,
                state=state, continuity=continuity)


def idle(job, after, state):
    """Bound empty page where no output can be read."""
    if state == 'expired':
        return page(job, [], after, state=state, continuity='unknown')
    return page(job, [], after, state=state, continuity='reset' if after else 'unverified')


def validate(value, job, after, limit):
    p._closed(value, ('jobId', 'attemptId', 'generation', 'environment', 'nodeId', 'lines', 'nextCursor', 'hasMore',
                      'state', 'continuity'), 'log page')
    if type(value['generation']) is not int or any(value[key] != job[key] for key in ('jobId', 'attemptId', 'generation', 'environment', 'nodeId')):
        raise ValueError('log binding mismatch')
    lines = value['lines']
    if type(lines) is not list or len(lines) > limit or type(value['hasMore']) is not bool:
        raise ValueError('invalid log page')
    if type(value['state']) is not str or value['state'] not in STATES or type(value['continuity']) is not str or value['continuity'] not in CONTINUITY:
        raise ValueError('invalid log state')
    reset = value['continuity'] == 'reset'
    if type(value['nextCursor']) is not int or value['nextCursor'] != (0 if reset else after + len(lines)) or value['nextCursor'] > MAX_CURSOR:
        raise ValueError('invalid log cursor')
    if value['hasMore'] and not lines:
        raise ValueError('nonadvancing log page')
    if (reset and (lines or not after)) or (value['state'] == 'expired' and lines):
        raise ValueError('invalid log state')
    for line in lines:
        p._closed(line, ('level', 'message'), 'log line')
        if line['level'] != 'info' or type(line['message']) is not str or len(line['message']) > MAX_MESSAGE_CHARS:
            raise ValueError('invalid log line')
    if encoded_size(value) > MAX_LOG_BYTES:
        raise ValueError('oversized log page')
    return value
