"""Native NodeODM HTTP boundary for the durable Viewer service.

The service owns state and execution. This boundary never redirects, logs query
credentials, interprets task names as environments, or loads image batches into
memory. Each multipart part is streamed to an unlinked private temporary file.
"""

from email.parser import BytesHeaderParser
from email.policy import default as email_policy
import hmac
import json
import hashlib
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import parse_qs, unquote, urlsplit
import uuid as uuid_module

from .maintenance import Conflict
from .viewer_tasks import InputLimitExceeded
from .viewer_limits import MAX_UPLOAD_BYTES


MAX_FORM_BYTES = 64 * 1024
MAX_PART_HEADERS = 16 * 1024
MAX_PARTS = 64
CHUNK = 64 * 1024
BOUNDARY = re.compile(r"[0-9A-Za-z'()+_,./:=?-]{1,70}\Z")


class RequestError(Exception):
    def __init__(self, status=400, code='invalid_request'):
        self.status, self.code = status, code


def safe_filename(value):
    if (not isinstance(value, str) or not value or len(value.encode('utf-8')) > 255
            or value in ('.', '..') or value[-1:] in ('.', ' ')
            or any(ord(c) < 32 or ord(c) == 127 or c in '/\\:' for c in value)):
        raise RequestError(400, 'invalid_filename')
    return value


def task_uuid(value):
    try:
        parsed = uuid_module.UUID(value)
        if str(parsed) != value.lower():
            raise ValueError()
        return str(parsed)
    except (ValueError, TypeError, AttributeError):
        raise RequestError(400, 'invalid_task_uuid') from None


class _Body:
    def __init__(self, stream, length):
        self.stream, self.remaining, self.buffer = stream, length, b''

    def fill(self):
        if not self.remaining:
            return False
        value = self.stream.read(min(CHUNK, self.remaining))
        if not value:
            raise RequestError(400, 'truncated_body')
        self.remaining -= len(value)
        self.buffer += value
        return True

    def line(self, limit):
        while True:
            index = self.buffer.find(b'\r\n')
            if index >= 0:
                if index + 2 > limit:
                    raise RequestError(400, 'multipart_headers_too_large')
                line, self.buffer = self.buffer[:index + 2], self.buffer[index + 2:]
                return line
            if len(self.buffer) >= limit:
                raise RequestError(400, 'multipart_headers_too_large')
            if not self.fill():
                raise RequestError(400, 'truncated_multipart')

    def finish(self):
        # No epilogue or additional bytes may be smuggled behind the delimiter.
        while self.remaining and len(self.buffer) <= 2:
            self.fill()
        if self.remaining or self.buffer not in (b'', b'\r\n'):
            raise RequestError(400, 'invalid_multipart_ending')
        self.buffer = b''


def _copy_part(body, boundary, output):
    marker = b'\r\n--' + boundary
    written = 0
    while True:
        index = body.buffer.find(marker)
        if index >= 0:
            needed = index + len(marker) + 2
            while len(body.buffer) < needed:
                if not body.fill():
                    raise RequestError(400, 'truncated_multipart')
            suffix = body.buffer[index + len(marker):needed]
            if suffix in (b'\r\n', b'--'):
                output.write(body.buffer[:index])
                written += index
                body.buffer = body.buffer[needed:]
                return written, suffix == b'--'
            # A boundary-like byte sequence inside binary content is data.
            count = index + 2
        else:
            count = max(0, len(body.buffer) - len(marker) - 1)
        if count:
            output.write(body.buffer[:count])
            written += count
            body.buffer = body.buffer[count:]
            if index >= 0:
                continue
        if not body.fill():
            raise RequestError(400, 'truncated_multipart')


def _multipart(stream, length, content_type, temp_dir=None):
    message = BytesHeaderParser(policy=email_policy).parsebytes(
        b'Content-Type: ' + content_type.encode('ascii') + b'\r\n\r\n')
    boundary = message.get_boundary()
    if message.get_content_type() != 'multipart/form-data' or not isinstance(boundary, str) or not BOUNDARY.fullmatch(boundary):
        raise RequestError(415, 'multipart_required')
    boundary = boundary.encode('ascii')
    body = _Body(stream, length)
    first = body.line(len(boundary) + 8)
    if first == b'--' + boundary + b'--\r\n':
        body.finish()
        return
    if first != b'--' + boundary + b'\r\n':
        raise RequestError(400, 'invalid_multipart')
    for _ in range(MAX_PARTS):
        raw = bytearray()
        while True:
            line = body.line(MAX_PART_HEADERS - len(raw))
            raw.extend(line)
            if line == b'\r\n':
                break
        headers = BytesHeaderParser(policy=email_policy).parsebytes(bytes(raw))
        if (headers.defects or len(headers.get_all('Content-Disposition', [])) != 1
                or headers.get_content_disposition() != 'form-data'
                or headers.get('Content-Transfer-Encoding')
                or '\\' in str(headers.get('Content-Disposition', ''))
                or b'\\' in bytes(raw)):
            raise RequestError(400, 'invalid_multipart_headers')
        name = headers.get_param('name', header='Content-Disposition')
        # get_filename() strips whitespace, which would silently normalize an
        # unsafe basename. Preserve the decoded parameter exactly for validation.
        filename = headers.get_param('filename', header='Content-Disposition')
        with tempfile.TemporaryFile(mode='w+b', dir=temp_dir) as output:
            size, final = _copy_part(body, boundary, output)
            output.seek(0)
            yield name, filename, output, size
        if final:
            body.finish()
            return
    raise RequestError(400, 'too_many_parts')


def _length(handler, maximum, *, allow_empty=False):
    values = handler.headers.get_all('Content-Length', [])
    if handler.headers.get('Transfer-Encoding'):
        raise RequestError(400, 'content_length_required')
    if not values and allow_empty:
        return 0
    if len(values) != 1 or not re.fullmatch(r'0|[1-9][0-9]{0,12}', values[0]):
        raise RequestError(411, 'content_length_required')
    length = int(values[0])
    if length > maximum:
        raise RequestError(413, 'body_too_large')
    if length == 0 and not allow_empty:
        raise RequestError(400, 'empty_body')
    return length


class ViewerFacade:
    def __init__(self, service, token, staging_token='', temp_dir=None):
        if not isinstance(token, str) or not token or not isinstance(staging_token, str) or staging_token == token:
            raise ValueError('distinct Viewer credentials required')
        self.service, self.token, self.staging_token = service, token, staging_token
        if temp_dir is not None:
            directory = Path(temp_dir)
            if (directory.is_symlink() or not directory.is_dir()
                    or (os.name != 'nt' and directory.stat().st_mode & 0o077)):
                raise ValueError('private existing spool directory required')
            temp_dir = str(directory)
        self.temp_dir = temp_dir

    @staticmethod
    def _reply(handler, status, value):
        raw = json.dumps(value, allow_nan=False, separators=(',', ':')).encode('utf-8')
        handler.send_response(status)
        handler.send_header('Content-Type', 'application/json')
        handler.send_header('Content-Length', str(len(raw)))
        handler.send_header('Cache-Control', 'no-store')
        handler.end_headers()
        handler.wfile.write(raw)

    def _environment(self, query):
        tokens = query.get('token', [])
        if len(tokens) != 1:
            raise RequestError(401, 'unauthorized')
        candidate = tokens[0].encode('utf-8')
        production = hmac.compare_digest(candidate, self.token.encode('utf-8'))
        staging = bool(self.staging_token) and hmac.compare_digest(candidate, self.staging_token.encode('utf-8'))
        if not production and not staging:
            raise RequestError(401, 'unauthorized')
        return 'production' if production else 'staging'

    def handle(self, handler):
        parsed = urlsplit(handler.path)
        path = parsed.path
        if path not in ('/info', '/options') and not path.startswith('/task/'):
            return False
        try:
            query = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=4)
            environment = self._environment(query)
            output_route = re.fullmatch(r'/task/([^/]+)/output', path)
            inventory_route = re.fullmatch(r'/task/([^/]+)/ledgetop/input-inventory', path)
            allowed_query = {'token', 'line'} if output_route else ({'token', 'cursor'} if inventory_route else {'token'})
            if set(query) - allowed_query or any(len(values) != 1 for values in query.values()):
                raise RequestError(400, 'invalid_query')
            if handler.command == 'GET':
                self._get(handler, path, query, environment)
            elif handler.command == 'POST':
                self._post(handler, path, environment)
            else:
                raise RequestError(405, 'method_not_allowed')
        except RequestError as error:
            handler.close_connection = True
            self._reply(handler, error.status, {'error': error.code})
        except KeyError:
            handler.close_connection = True
            self._reply(handler, 404, {'error': 'task_not_found'})
        except Conflict:
            handler.close_connection = True
            self._reply(handler, 409, {'error': 'conflict'})
        except InputLimitExceeded:
            handler.close_connection = True
            self._reply(handler, 413, {'error': 'task_input_limit_exceeded'})
        except (ValueError, TypeError, UnicodeError, RecursionError):
            handler.close_connection = True
            self._reply(handler, 400, {'error': 'invalid_request'})
        except (OSError, TimeoutError):
            handler.close_connection = True
            # Broken clients and upstream errors must not expose URLs or tokens.
            try:
                self._reply(handler, 503, {'error': 'service_unavailable'})
            except OSError:
                pass
        return True

    @staticmethod
    def _capability(handler):
        values=handler.headers.get_all('X-LedgeTop-Capability',[])
        if len(values)>1:raise RequestError(400,'invalid_capability')
        return values[0] if values else None

    def _authorize(self, identifier, environment, handler):
        authorize = getattr(self.service, 'authorize', None)
        if callable(authorize):
            capability=self._capability(handler)
            if capability is None:authorize(identifier, environment)
            else:authorize(identifier, environment, capability)

    def _get(self, handler, path, query, environment):
        lengths = handler.headers.get_all('Content-Length', [])
        if handler.headers.get('Transfer-Encoding') or lengths not in ([], ['0']):
            raise RequestError(400, 'unexpected_body')
        if path == '/info':
            value = self.service.capabilities()
        elif path == '/options':
            value = self.service.options()
        else:
            inventory = re.fullmatch(r'/task/([^/]+)/ledgetop/input-inventory', path)
            if inventory:
                identifier = task_uuid(unquote(inventory[1]))
                capability = self._capability(handler)
                if capability is None:
                    raise KeyError('unknown reservation')
                value = self.service.input_inventory(identifier, environment, capability, query.get('cursor', [None])[0])
                self._reply(handler, 200, value)
                return
            route = re.fullmatch(r'/task/([^/]+)/(info|output|download/all\.zip)', path)
            if not route:
                raise RequestError(404, 'not_found')
            identifier, action = task_uuid(unquote(route[1])), route[2]
            self._authorize(identifier, environment, handler)
            if action == 'info':
                value = self.service.info(identifier)
            elif action == 'output':
                line = query.get('line', ['0'])[0]
                if not re.fullmatch(r'0|[1-9][0-9]{0,14}', line) or int(line) > 2 ** 53 - 1:
                    raise RequestError(400, 'invalid_output_cursor')
                value = self.service.output(identifier, int(line))
            else:
                stream, length = self.service.download(identifier)
                try:
                    if type(length) is not int or length < 0:
                        raise ValueError('invalid download length')
                    digest = getattr(stream, 'ledgetop_archive_sha256', None)
                    if digest is not None and (type(digest) is not str or
                            not re.fullmatch(r'[a-f0-9]{64}', digest)):
                        raise ValueError('invalid archive identity')
                    handler.send_response(200)
                    handler.send_header('Content-Type', 'application/zip')
                    handler.send_header('Content-Length', str(length))
                    if digest is not None:
                        handler.send_header('X-LedgeTop-Archive-SHA256', digest)
                    handler.send_header('Cache-Control', 'no-store')
                    handler.end_headers()
                    remaining = length
                    calculated = hashlib.sha256() if digest is not None else None
                    last = None
                    try:
                        while remaining:
                            data = stream.read(min(CHUNK, remaining))
                            if not data or len(data) > remaining:
                                handler.close_connection = True
                                return
                            remaining -= len(data)
                            if calculated is not None:
                                calculated.update(data)
                                if last is not None: handler.wfile.write(last)
                                last = data
                            else:
                                handler.wfile.write(data)
                        # Withhold the last chunk until the complete served
                        # bytes match the receipt. A mismatch closes a
                        # truncated 200 response, never a complete false body.
                        if calculated is not None:
                            if calculated.hexdigest() != digest:
                                handler.close_connection = True
                                return
                            if last is not None: handler.wfile.write(last)
                    except OSError:
                        handler.close_connection = True
                finally:
                    try:
                        stream.close()
                    except OSError:
                        handler.close_connection = True
                return
        self._reply(handler, 200, value)

    def _post(self, handler, path, environment):
        if path in ('/info', '/options'):
            raise RequestError(405, 'method_not_allowed')
        if path in ('/task/ledgetop/reserve','/task/ledgetop/cancel'):
            length=_length(handler,MAX_FORM_BYTES)
            if handler.headers.get_content_type()!='application/x-www-form-urlencoded':raise RequestError(415,'form_required')
            body=handler.rfile.read(length)
            if len(body)!=length:raise RequestError(400,'truncated_body')
            values=parse_qs(body.decode('utf-8'),keep_blank_values=True,max_num_fields=2)
            if set(values)!={'uuid'} or len(values['uuid'])!=1:raise RequestError(400,'invalid_action_form')
            identifier=task_uuid(values['uuid'][0]);capability=self._capability(handler)
            if capability is None:raise RequestError(400,'invalid_capability')
            action=self.service.reserve if path.endswith('/reserve') else self.service.cancel_reserved
            self._reply(handler,200,action(identifier,environment,capability));return
        if path == '/task/new/init':
            length = _length(handler, MAX_FORM_BYTES)
            identifier = task_uuid(handler.headers.get('set-uuid', ''))
            if len(handler.headers.get_all('set-uuid', [])) != 1:
                raise RequestError(400, 'invalid_task_uuid')
            values = {}
            for name, filename, stream, size in _multipart(handler.rfile, length, handler.headers.get('Content-Type', ''), self.temp_dir):
                if name not in ('name', 'options', 'outputs') or filename is not None or name in values:
                    raise RequestError(400, 'invalid_init_form')
                values[name] = stream.read(size).decode('utf-8')
            options = json.loads(values.get('options', '[]'))
            outputs = json.loads(values['outputs']) if 'outputs' in values else None
            capability=self._capability(handler)
            if capability is None:result=self.service.initialize(identifier, values.get('name', ''), options, outputs, environment)
            else:result=self.service.initialize(identifier, values.get('name', ''), options, outputs, environment, capability)
            self._reply(handler, 200, result)
            return
        upload = re.fullmatch(r'/task/new/upload/([^/]+)', path)
        if upload:
            length = _length(handler, MAX_UPLOAD_BYTES)
            identifier = task_uuid(unquote(upload[1]))
            self._authorize(identifier, environment, handler)
            count, seen = 0, set()
            for name, filename, stream, size in _multipart(handler.rfile, length, handler.headers.get('Content-Type', ''), self.temp_dir):
                if name != 'images':
                    raise RequestError(400, 'invalid_upload_form')
                filename = safe_filename(filename)
                if filename.casefold() in seen or not size:
                    raise RequestError(400, 'duplicate_or_empty_upload')
                seen.add(filename.casefold())
                capability=self._capability(handler)
                if capability is None:self.service.upload(identifier, filename, stream, size)
                else:self.service.upload(identifier, filename, stream, size, environment, capability)
                count += 1
            if not count:
                raise RequestError(400, 'empty_upload')
            self._reply(handler, 200, {'success': True})
            return
        commit = re.fullmatch(r'/task/new/commit/([^/]+)', path)
        if commit:
            identifier = task_uuid(unquote(commit[1]))
            self._authorize(identifier, environment, handler)
            if _length(handler, MAX_FORM_BYTES, allow_empty=True):
                raise RequestError(400, 'unexpected_body')
            capability=self._capability(handler)
            if capability is None:result=self.service.commit(identifier)
            else:result=self.service.commit(identifier,environment,capability)
            self._reply(handler, 200, result)
            return
        if path in ('/task/cancel', '/task/remove'):
            length = _length(handler, MAX_FORM_BYTES)
            if handler.headers.get_content_type() != 'application/x-www-form-urlencoded':
                raise RequestError(415, 'form_required')
            body = handler.rfile.read(length)
            if len(body) != length:
                raise RequestError(400, 'truncated_body')
            values = parse_qs(body.decode('utf-8'), keep_blank_values=True, max_num_fields=2)
            if set(values) != {'uuid'} or len(values['uuid']) != 1:
                raise RequestError(400, 'invalid_action_form')
            identifier = task_uuid(values['uuid'][0])
            self._authorize(identifier, environment, handler)
            action = self.service.cancel if path.endswith('cancel') else self.service.remove
            self._reply(handler, 200, action(identifier))
            return
        raise RequestError(404, 'not_found')
