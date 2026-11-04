"""Loopback-only NodeODM output gateway; an experimental opt-in component.

Docker container incarnation is checked on both sides of each bounded read.
It is evidence of container continuity, not a durable task/log incarnation:
the engine may replace a task or restart inside a running container. Do not
enable as a production start/cleanup witness without those additional checks.
"""
import argparse
import hashlib
import hmac
import http.client
import json
import os
import posixpath
import re
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlsplit

LIMIT = 1024 * 1024
TASK = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')
CONTAINER = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}\Z')


class Unavailable(Exception):
    pass


def docker_incarnation(container, host_port, *, docker='/usr/bin/docker'):
    """Inspect a fixed local Engine; reject unpinned/wildcard port mappings."""
    if not CONTAINER.fullmatch(container) or not posixpath.isabs(docker):
        raise ValueError('invalid container configuration')
    env = {k: v for k, v in os.environ.items() if k.upper() not in
           ('DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH')}
    try:
        result = subprocess.run([docker, '--host', 'unix:///var/run/docker.sock', 'inspect', container],
                                capture_output=True, timeout=3, env=env, check=True)
        if len(result.stdout) > LIMIT:
            raise ValueError()
        value = json.loads(result.stdout)
        if not isinstance(value, list) or len(value) != 1:
            raise ValueError()
        obj = value[0]
        state = obj['State']
        mappings = obj['NetworkSettings']['Ports'].get('3000/tcp') or []
        if (not re.fullmatch(r'[0-9a-f]{64}', obj['Id']) or state['Running'] is not True
                or type(state['Pid']) is not int or state['Pid'] <= 0
                or not isinstance(state['StartedAt'], str) or not state['StartedAt'].endswith('Z')
                or len(mappings) != 1 or mappings[0].get('HostIp') != '127.0.0.1'
                or mappings[0].get('HostPort') != str(host_port)):
            raise ValueError()
        return obj['Id'], state['Pid'], state['StartedAt'], host_port
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        raise Unavailable('engine identity unavailable') from None


class OutputGateway:
    def __init__(self, container, port, token, engine_token='', inspect=docker_incarnation):
        if (not CONTAINER.fullmatch(container) or type(port) is not int or not 1024 <= port <= 65535
                or not isinstance(token, str) or not 32 <= len(token) <= 1024
                or not token.isascii() or any(ord(c) < 33 or ord(c) > 126 for c in token)):
            raise ValueError('invalid gateway configuration')
        self.container, self.port, self._token = container, port, token
        self._engine_token, self._inspect = engine_token, inspect

    def read(self, task, line):
        if not TASK.fullmatch(task) or type(line) is not int or not 0 <= line <= 2**53 - 1:
            raise ValueError('invalid output request')
        before = self._inspect(self.container, self.port)
        query = {'line': line}
        if self._engine_token:
            query['token'] = self._engine_token
        client = http.client.HTTPConnection('127.0.0.1', self.port, timeout=5)
        try:
            deadline = time.monotonic() + 5
            client.request('GET', '/task/' + task + '/output?' + urlencode(query), headers={'Accept': 'application/json'})
            response = client.getresponse()
            if response.status != 200 or 'application/json' not in response.getheader('Content-Type', ''):
                raise Unavailable('engine output unavailable')
            chunks = []
            remaining_bytes = LIMIT + 1
            while remaining_bytes:
                remaining_time = deadline - time.monotonic()
                if remaining_time <= 0:
                    raise Unavailable('engine output unavailable')
                if response.fp is None:
                    break
                response.fp.raw._sock.settimeout(remaining_time)
                part = response.read1(min(65536, remaining_bytes))
                if not part:
                    break
                chunks.append(part)
                remaining_bytes -= len(part)
            raw = b''.join(chunks)
            if len(raw) > LIMIT:
                raise Unavailable('engine output unavailable')
            lines = json.loads(raw)
            if not isinstance(lines, list) or any(not isinstance(item, str) for item in lines):
                raise Unavailable('engine output unavailable')
            after = self._inspect(self.container, self.port)
            if before != after:
                raise Unavailable('engine changed during read')
            raw_epoch = json.dumps(before, separators=(',', ':'), ensure_ascii=True).encode('ascii')
            return {'containerEpoch': hashlib.sha256(raw_epoch).hexdigest(), 'taskId': task,
                    'fromLine': line, 'lines': lines}
        except (OSError, http.client.HTTPException, ValueError, json.JSONDecodeError):
            raise Unavailable('engine output unavailable') from None
        finally:
            client.close()

    def handler(self):
        gateway = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                code, result = 503, {'error': 'output_unavailable'}
                try:
                    if not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + gateway._token):
                        code, result = 401, {'error': 'unauthorized'}
                    else:
                        parsed = urlsplit(self.path)
                        match = re.fullmatch(r'/task/([^/]+)/output', parsed.path)
                        query = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=1)
                        cursor = query.get('line', ['0'])
                        if not match or set(query) - {'line'} or len(cursor) != 1 or not re.fullmatch(r'0|[1-9][0-9]{0,15}', cursor[0]):
                            code, result = 400, {'error': 'invalid_request'}
                        else:
                            result = gateway.read(match[1], int(cursor[0])); code = 200
                except ValueError:
                    code, result = 400, {'error': 'invalid_request'}
                except Unavailable:
                    pass
                raw = json.dumps(result, ensure_ascii=True, separators=(',', ':')).encode('ascii')
                self.send_response(code)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(raw)))
                self.send_header('Cache-Control', 'no-store')
                self.end_headers()
                self.wfile.write(raw)
        return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--container', required=True)
    parser.add_argument('--engine-port', required=True, type=int)
    parser.add_argument('--listen-port', required=True, type=int)
    args = parser.parse_args()
    gateway = OutputGateway(args.container, args.engine_port, os.environ.get('LT_OUTPUT_GATEWAY_TOKEN', ''),
                            os.environ.get('LT_NODEODM_TOKEN', ''))
    if not 1024 <= args.listen_port <= 65535:
        parser.error('invalid listen port')
    server = ThreadingHTTPServer(('127.0.0.1', args.listen_port), gateway.handler())
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
