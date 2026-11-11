import io
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import time
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from processing.engine_output import OVERSIZED_OUTPUT_RECORD, read_output_prefix
from processing.viewer_service import EngineUnavailable, NativeEngine


class Response(io.BytesIO):
    def __init__(self, raw, fragment=8192, media_type='application/json'):
        super().__init__(raw)
        self.headers = {'Content-Type': media_type, 'Content-Length': str(len(raw))}
        self.fragment, self.consumed, self.largest_read = fragment, 0, 0

    def read1(self, size=-1):
        self.largest_read = max(self.largest_read, size)
        data = super().read(min(size, self.fragment))
        self.consumed += len(data)
        return data


class OutputPrefixTests(unittest.TestCase):
    def test_fragmented_escapes_blank_comma_and_unicode(self):
        records = ['', 'comma,record', '"quoted" \\ slash /', '\n\t\r', 'oak 🌳 café', '\ud800']
        for ascii_only in (True, False):
            # The lone surrogate is represented with a JSON escape in both cases.
            raw = json.dumps(records[:-1], ensure_ascii=ascii_only).encode('utf-8')[:-1] + b',"\\ud800"]'
            for fragment in (1, 2, 3, 8192):
                with self.subTest(ascii_only=ascii_only, fragment=fragment):
                    self.assertEqual(read_output_prefix(Response(raw, fragment)), records)

    def test_byte_pages_are_contiguous_without_dropping_the_next_record(self):
        records = ['', 'a,b', '🌳' * 8, 'quoted "x"', 'tail']
        found, cursor = [], 0
        while cursor < len(records):
            page = read_output_prefix(Response(json.dumps(records[cursor:]).encode()), max_bytes=110)
            self.assertTrue(page)
            self.assertLessEqual(len(json.dumps(page, separators=(',', ':')).encode()), 110)
            self.assertEqual(page, records[cursor:cursor + len(page)])
            found.extend(page)
            cursor += len(page)
        self.assertEqual(found, records)

    def test_bad_records_and_framing_fail_closed(self):
        for raw in (b'{}', b'null', b'[1]', b'[true]', b'["a",null]', b'["a",]',
                    b'["a" "b"]', b'["a"]garbage', b'["unterminated', b'["\\q"]',
                    b'["\xff"]', b'["literal\nnewline"]', b'[', b''):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    read_output_prefix(Response(raw, 1))

    def test_oversized_record_is_explicitly_represented_without_skipping(self):
        raw = json.dumps(['good', 'x' * 200, 'never skip to this']).encode()
        self.assertEqual(read_output_prefix(Response(raw), max_bytes=100), ['good'])
        self.assertEqual(read_output_prefix(
            Response(json.dumps(['x' * 200, 'tail']).encode()), max_bytes=100),
            [OVERSIZED_OUTPUT_RECORD, 'tail'])
        # Encoded Unicode can exceed the facade budget while raw UTF-8 fits.
        self.assertEqual(read_output_prefix(Response(
            json.dumps(['🌳' * 10], ensure_ascii=False).encode()), max_bytes=100),
            [OVERSIZED_OUTPUT_RECORD])

    def test_oversized_discard_still_validates_utf8_escapes_and_separator(self):
        prefix = b'["' + b'x' * 200
        for suffix in (b'\\q"]', b'\\u00xz"]', b'\xff"]', b'\xe2\x82"]',
                       b'\n"]', b'"null]', b'\\', b''):
            with self.subTest(suffix=suffix), self.assertRaises(ValueError):
                read_output_prefix(Response(prefix + suffix, 1), max_bytes=100)
        raw = prefix + b'\\uD83C\\uDF33,\\\\\\"valid"]'
        self.assertEqual(read_output_prefix(Response(raw, 1), max_bytes=100),
                         [OVERSIZED_OUTPUT_RECORD])

    def test_large_first_record_then_normal_line_preserves_exact_cursor(self):
        records = ['prefix 🌳 ' + 'x' * (600 * 1024) + ' \\"ending', '', 'normal,next']
        response = Response(json.dumps(records, ensure_ascii=False).encode(), 8191)
        page = read_output_prefix(response, max_lines=1)
        self.assertEqual(page, [OVERSIZED_OUTPUT_RECORD])
        self.assertLessEqual(response.largest_read, 8192)
        self.assertEqual(read_output_prefix(Response(json.dumps(records[len(page):]).encode())), records[1:])

    def test_oversized_discard_cannot_escape_wire_or_time_bounds(self):
        response = Response(b'["' + b'x' * 20000 + b'"]')
        with self.assertRaises(ValueError):
            read_output_prefix(response, max_bytes=100)
        self.assertLessEqual(response.consumed, 7 * 100 + 16384 + 1)
        with self.assertRaises(ValueError):
            read_output_prefix(Response(b'["' + b'x' * 200 + b'"]'),
                               max_bytes=100, deadline=time.monotonic() - 1)

    def test_verified_page_does_not_claim_to_validate_unread_suffix(self):
        self.assertEqual(read_output_prefix(Response(b'["good",malformed'), max_lines=1), ['good'])

    def test_empty_array_padding_deadline_and_bounds(self):
        self.assertEqual(read_output_prefix(Response(b' \r\n[ ]\t')), [])
        with self.assertRaises(ValueError):
            read_output_prefix(Response(b' ' * 16385 + b'[]'))
        with self.assertRaises(ValueError):
            read_output_prefix(Response(b'[]'), deadline=time.monotonic() - 1)
        for kwargs in ({'max_lines': 0}, {'max_lines': True}, {'max_lines': 257},
                       {'max_bytes': 1}, {'max_bytes': True}, {'max_bytes': 524289}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                read_output_prefix(Response(b'[]'), **kwargs)


class NativeOutputTests(unittest.TestCase):
    def test_real_http_content_length_over_four_mib_returns_prefix_and_tail(self):
        records = ['synthetic ' + str(i) + ':' + 'x' * 400 for i in range(12000)]
        records[-3:] = ['', 'comma,value', 'unicode 🌳']
        requested = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                query = parse_qs(urlsplit(self.path).query)
                cursor = int(query['line'][0])
                requested.append(cursor)
                raw = json.dumps(records[cursor:]).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                try:
                    for offset in range(0, len(raw), 8192):
                        self.wfile.write(raw[offset:offset + 8192])
                except (BrokenPipeError, ConnectionResetError):
                    pass  # The bounded client intentionally closes its prefix.

            def log_message(self, *_):
                pass

        server = HTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            engine = NativeEngine('http://127.0.0.1:' + str(server.server_port))
            self.assertGreater(len(json.dumps(records).encode()), 4 * 1024 ** 2)
            self.assertEqual(engine.output('synthetic'), records[:256])
            self.assertEqual(engine.output('synthetic', len(records) - 3), records[-3:])
            self.assertEqual(requested, [0, len(records) - 3])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        self.assertFalse(thread.is_alive())

    def test_many_megabyte_log_pages_reconstruct_exact_cursor_sequence(self):
        records = [(str(i) + ': ' + 'synthetic text,' * 28) for i in range(12000)]
        records[1:5] = ['', 'comma,record', 'unicode 🌳', 'quoted "value" \\']
        self.assertGreater(len(json.dumps(records).encode()), 4 * 1024 ** 2)
        engine, responses, cursors = NativeEngine('http://synthetic.invalid:3000', 'private-test'), [], []

        class Opener:
            def open(self, request, timeout):
                query = parse_qs(urlsplit(request.full_url).query)
                self_test.assertEqual(query['token'], ['private-test'])
                self_test.assertEqual(timeout, 5)
                cursor = int(query.get('line', ['0'])[0])
                cursors.append(cursor)
                response = Response(json.dumps(records[cursor:]).encode())
                responses.append(response)
                return response

        self_test, found, cursor = self, [], 0
        with patch('processing.viewer_service.build_opener', return_value=Opener()):
            # Keep the generic four-MiB response bound unchanged.
            with self.assertRaises(EngineUnavailable):
                engine.read('/task/synthetic/output', line=0)
            while cursor < len(records):
                page = engine.output('synthetic', cursor)
                self.assertTrue(page)
                self.assertLessEqual(len(page), 256)
                self.assertEqual(page, records[cursor:cursor + len(page)])
                found.extend(page)
                cursor += len(page)
            self.assertEqual(engine.output('synthetic', cursor), [])
        self.assertEqual(found, records)
        self.assertEqual(cursors[1:], list(range(0, 12000, 256)) + [12000])
        for response in responses[1:]:
            self.assertTrue(response.closed)
            self.assertLessEqual(response.largest_read, 8192)
            self.assertLess(response.consumed, 512 * 1024)

    def test_media_type_invalid_json_and_cursor_errors_are_redacted(self):
        engine = NativeEngine('http://synthetic.invalid:3000', 'private-test')
        for response in (Response(b'{"error":"secret"}'), Response(b'["ok"]', media_type='text/plain')):
            with patch('processing.viewer_service.build_opener') as opener:
                opener.return_value.open.return_value = response
                with self.assertRaisesRegex(EngineUnavailable, '^engine_output_unavailable$'):
                    engine.output('synthetic')
                self.assertTrue(response.closed)
        for cursor in (-1, True, 1.5, 2 ** 53):
            with self.assertRaises(ValueError):
                engine.output('synthetic', cursor)


if __name__ == '__main__':
    unittest.main()
