"""Bounded, contiguous prefix of NodeODM's JSON string-array output.

NodeODM accepts a starting line but no ending line. Closing the response after
a verified prefix avoids buffering or downloading the entire remaining log.
Only returned records are consumed by the caller's ordinary line cursor.
"""
import json
import time


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
        token, escaped = bytearray(b'"'), False
        while True:
            value = take()
            if value is None:
                raise ValueError('unfinished output record')
            token.append(value)
            if len(token) > max_bytes - 2:
                if records:
                    return records
                raise ValueError('individual output record exceeds bound')
            if escaped:
                escaped = False
            elif value == ord('\\'):
                escaped = True
            elif value == ord('"'):
                break
        record = json.loads(token.decode('utf-8'))
        separator = significant()
        if separator not in (ord(','), ord(']')):
            raise ValueError('invalid output array separator')
        # Bound the facade's canonical JSON too, including escaped Unicode.
        size = len(json.dumps(record, ensure_ascii=True).encode('ascii')) + bool(records)
        if page_bytes + size > max_bytes:
            if not records:
                raise ValueError('individual output record exceeds bound')
            return records
        records.append(record)
        page_bytes += size
        if separator == ord(']'):
            finish()
            return records
        if len(records) == max_lines:
            return records
        value = significant()
