"""Bounded, contiguous prefix of NodeODM's JSON string-array output.

NodeODM accepts a starting line but no ending line. Closing the response after
a verified prefix avoids buffering or downloading the entire remaining log.
Each returned record represents exactly one upstream line. An individually
oversized but fully validated string gets an explicit placeholder; its original
is never altered on the engine. Unverifiable records never advance the cursor.
"""
import codecs
import json
import time


OVERSIZED_OUTPUT_RECORD = '[LedgeTop: oversized log record omitted; original remains on engine.]'


def read_output_prefix(response, *, max_lines=256, max_bytes=512 * 1024,
                       deadline=None):
    if type(max_lines) is not int or not 1 <= max_lines <= 256:
        raise ValueError('invalid output page bound')
    if type(max_bytes) is not int or not 2 <= max_bytes <= 512 * 1024:
        raise ValueError('invalid output page bound')
    deadline = time.monotonic() + 5 if deadline is None else deadline
    chunk, offset, total, padding = b'', 0, 0, 0
    # JSON escapes may occupy six input bytes for one ASCII output byte.
    wire_limit = 7 * max_bytes + 16384

    def take():
        nonlocal chunk, offset, total
        if time.monotonic() >= deadline:
            raise ValueError('output deadline exceeded')
        if offset == len(chunk):
            chunk = getattr(response, 'read1', response.read)(min(8192, wire_limit - total + 1))
            offset = 0
            total += len(chunk)
            if total > wire_limit:
                raise ValueError('output framing exceeds bound')
            if not chunk:
                return None
        value = chunk[offset]
        offset += 1
        return value

    def significant():
        nonlocal padding
        value = take()
        while value in (9, 10, 13, 32):
            padding += 1
            if padding > 16384:
                raise ValueError('output framing exceeds bound')
            value = take()
        return value

    def finish():
        if significant() is not None:
            raise ValueError('invalid output array suffix')

    if significant() != ord('['):
        raise ValueError('output must be a string array')
    value = significant()
    if value == ord(']'):
        finish()
        return []
    records, page_bytes = [], 2
    while True:
        if value != ord('"'):
            raise ValueError('output record must be a string')
        token, utf8 = bytearray(b'"'), bytearray(b'"')
        decoder = codecs.getincrementaldecoder('utf-8')('strict')
        escaped, unicode_remaining, token_size, oversized = False, 0, 1, False
        while True:
            value = take()
            if value is None:
                raise ValueError('unfinished output record')
            token_size += 1
            utf8.append(value)
            if len(utf8) >= 4096:
                decoder.decode(utf8)
                utf8.clear()
            if not oversized:
                if token_size > max_bytes - 2:
                    if records:
                        return records
                    # Stop retaining the string, but validate all remaining
                    # UTF-8/JSON lexical bytes before representing its line.
                    oversized = True
                    token.clear()
                else:
                    token.append(value)
            if unicode_remaining:
                if value not in b'0123456789abcdefABCDEF':
                    raise ValueError('invalid output Unicode escape')
                unicode_remaining -= 1
            elif escaped:
                if value == ord('u'):
                    unicode_remaining = 4
                elif value not in b'"\\/bfnrt':
                    raise ValueError('invalid output escape')
                escaped = False
            elif value < 32:
                raise ValueError('invalid output control byte')
            elif value == ord('\\'):
                escaped = True
            elif value == ord('"'):
                decoder.decode(utf8, final=True)
                break
        record = OVERSIZED_OUTPUT_RECORD if oversized else json.loads(token.decode('utf-8'))
        separator = significant()
        if separator not in (ord(','), ord(']')):
            raise ValueError('invalid output array separator')
        # Bound the facade's canonical JSON too, including escaped Unicode.
        size = len(json.dumps(record, ensure_ascii=True).encode('ascii')) + bool(records)
        if page_bytes + size > max_bytes:
            if records:
                return records
            # Raw UTF-8 may fit while canonical escaped JSON does not.
            record = OVERSIZED_OUTPUT_RECORD
            size = len(json.dumps(record).encode('ascii'))
            if page_bytes + size > max_bytes:
                raise ValueError('output page cannot represent oversized record')
        records.append(record)
        page_bytes += size
        if separator == ord(']'):
            finish()
            return records
        if len(records) == max_lines:
            return records
        value = significant()
