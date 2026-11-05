"""Bounded, path-free execution output DTO shared by existing read adapters."""
import codecs
import json
import re
from urllib.parse import parse_qs, urlsplit

from . import reference_protocol as p

MAX_LOG_BYTES = 256 * 1024
MAX_CURSOR = 9999999999999


def encoded_size(value):
    # Match the existing HTTP JSON writer, including Unicode escaping.
    return len(json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(',', ':')).encode('utf-8'))


def read_output_prefix(response, limit):
    """Consume only a bounded JSON string-array prefix, plus one lookahead line.

    NodeODM returns the entire remaining tail. Close its response after a valid
    lookahead line; an EOF page instead requires the closed array and clean EOF.
    The caller owns and closes response on every outcome.
    """
    decoder = codecs.getincrementaldecoder('utf-8')('strict')
    parser = json.JSONDecoder()
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
            else:
                text.encode('utf-8', 'strict')
                lines.append(text)
                # Leave room for one validated lookahead line. Many moderately
                # long lines must still advance, even at a requested limit of
                # 200; a huge individual line remains conservatively unavailable.
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


def page(job, lines, after, has_more=False):
    return dict(jobId=job['jobId'], attemptId=job['attemptId'], generation=job['generation'],
                environment=job['environment'], nodeId=job['nodeId'], lines=lines,
                nextCursor=after + len(lines), hasMore=has_more)


def validate(value, job, after, limit):
    p._closed(value, ('jobId', 'attemptId', 'generation', 'environment', 'nodeId', 'lines', 'nextCursor', 'hasMore'), 'log page')
    if type(value['generation']) is not int or any(value[key] != job[key] for key in ('jobId', 'attemptId', 'generation', 'environment', 'nodeId')):
        raise ValueError('log binding mismatch')
    lines = value['lines']
    if type(lines) is not list or len(lines) > limit or type(value['hasMore']) is not bool:
        raise ValueError('invalid log page')
    if type(value['nextCursor']) is not int or value['nextCursor'] != after + len(lines) or value['nextCursor'] > MAX_CURSOR:
        raise ValueError('invalid log cursor')
    if value['hasMore'] and not lines:
        raise ValueError('nonadvancing log page')
    for line in lines:
        p._closed(line, ('level', 'message'), 'log line')
        if line['level'] != 'info' or type(line['message']) is not str or len(line['message']) > 4000:
            raise ValueError('invalid log line')
    if encoded_size(value) > MAX_LOG_BYTES:
        raise ValueError('oversized log page')
    return value
