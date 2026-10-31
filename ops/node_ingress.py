"""Host-owned NodeODX ingress gate. Never accepts job-supplied configuration.

Close drains the old nginx workers before attesting closed ingress. The active
configuration stays closed across container restart and ambiguous operations.
This does not reserve the Docker Engine against administrators/direct writers.
"""
import argparse
from contextlib import contextmanager
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import build_opener, ProxyHandler, HTTPRedirectHandler

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from processing.maintenance_authority import maintenance_authority, MaintenanceAuthorityBusy, local_engine_id


class GateError(Exception):
    pass


def save(path, value, mode=0o600):
    data = value if isinstance(value, str) else json.dumps(value, sort_keys=True) + '\n'
    temporary = path.with_suffix('.tmp')
    with os.fdopen(os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, mode), 'w') as stream:
        os.fchmod(stream.fileno(), mode)
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def locked(root):
    import fcntl
    with (root / 'ingress.lock').open('a+b') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def _allowed_clients(values):
    """Canonical fixed IPs/CIDRs only; never interpolate names or nginx syntax."""
    if not isinstance(values, (tuple, list)) or len(values) > 64:
        raise GateError('bounded fixed client IP/CIDR list required')
    result = []
    for value in values:
        if not isinstance(value, str) or not value or len(value) > 64 or '%' in value:
            raise GateError('canonical fixed client IP/CIDR required')
        try:
            address = (ipaddress.ip_network(value, strict=True) if '/' in value
                       else ipaddress.ip_address(value))
        except ValueError:
            raise GateError('canonical fixed client IP/CIDR required') from None
        if str(address) != value or value in result:
            raise GateError('unique canonical fixed client IP/CIDR required')
        result.append(value)
    return result


def configuration(listen, backend, closed, allowed_clients=()):
    if any(type(p) is not int or not 1024 <= p <= 65535 for p in (listen, backend)) or listen == backend:
        raise GateError('invalid fixed ingress ports')
    allowed = _allowed_clients(allowed_clients)
    # Empty preserves the legacy unrestricted gate. Restricted gates use the
    # actual peer address; neither X-Forwarded-For nor any other header is trusted.
    access = ''
    if allowed:
        addresses = dict.fromkeys(('127.0.0.1', '::1', *allowed))
        access = '\n        '.join(f'allow {address};' for address in addresses) + '\n        deny all;'
    route = 'return 503;' if closed else f'''
        proxy_pass http://127.0.0.1:{backend};
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_request_buffering off;
        proxy_buffering off;
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
    '''
    return f'''pid /tmp/nginx.pid;
error_log /dev/null crit;
worker_processes 1;
events {{ worker_connections 256; }}
http {{
    access_log off;
    client_body_temp_path /tmp/body;
    proxy_temp_path /tmp/proxy;
    fastcgi_temp_path /tmp/fastcgi;
    uwsgi_temp_path /tmp/uwsgi;
    scgi_temp_path /tmp/scgi;
    client_max_body_size 0;
    server {{
        listen 0.0.0.0:{listen};
        listen [::]:{listen} ipv6only=on;
        {access}
        add_header Cache-Control no-store always;
        location / {{ {route} }}
    }}
}}
'''


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise GateError('redirect refused')


class Ingress:
    def __init__(self, root):
        self.root = root
        self.config = json.loads((root / 'gateway.json').read_text())
        for key in ('proxyName', 'backendName'):
            if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}', self.config[key]):
                raise GateError('invalid owned container identity')
        if not re.fullmatch(r'sha256:[a-f0-9]{64}', self.config['proxyImage']):
            raise GateError('immutable gateway image required')
        configuration(self.config['listenPort'], self.config['backendPort'], True,
                      self.config.get('allowedClients', ()))
        if self.config.get('backendEngine', 'odx') not in ('odm', 'odx'):
            raise GateError('explicit ODM-family backend engine required')
        token = self.config.get('backendToken', '')
        if (not isinstance(token, str) or len(token) > 4096
                or any(ord(character) < 33 or ord(character) > 126 for character in token)):
            raise GateError('invalid private backend credential')
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def docker(self, args):
        try:
            result = subprocess.run(['docker', *args], capture_output=True, text=True, timeout=20)
        except (OSError, subprocess.SubprocessError):
            raise GateError('Docker response uncertain; inspect saved ingress state') from None
        if result.returncode:
            raise GateError('Docker operation failed; inspect owned gateway privately')
        return result.stdout

    def inspect(self):
        context = os.environ.get('DOCKER_CONTEXT')
        endpoint = os.environ.get('DOCKER_HOST') if not context else None
        if not endpoint:
            command = ['context', 'inspect', '--format', '{{json .Endpoints.docker.Host}}']
            if context:
                command.append(context)
            endpoint = json.loads(self.docker(command).strip())
        if not isinstance(endpoint, str) or not endpoint.startswith('unix:///'):
            raise GateError('ingress requires the local Unix Docker endpoint')
        if self.docker(['info', '--format', '{{.ID}}']).strip() != self.config['engineId']:
            raise GateError('wrong Docker Engine')
        item = json.loads(self.docker(['inspect', self.config['proxyName']]))[0]
        if (item['Image'] != self.config['proxyImage'] or not item['State']['Running']
                or item['HostConfig']['NetworkMode'] != 'host'
                or item['Config'].get('Entrypoint') != ['nginx']
                or item['Config'].get('Cmd') != ['-c', '/config/nginx.conf', '-g', 'daemon off;']):
            raise GateError('gateway identity or state changed')
        mounts = [m for m in item['Mounts'] if m['Destination'] == '/config']
        if (len(mounts) != 1 or mounts[0]['Type'] != 'bind' or mounts[0]['RW']
                or mounts[0]['Source'] != str(self.root / 'public-config')):
            raise GateError('gateway configuration is not this exact read-only bind')
        return item

    def workers(self, item):
        raw = self.docker(['top', item['Id'], '-eo', 'pid,comm'])
        result = set()
        for line in raw.splitlines()[1:]:
            pid, command = line.split()
            if not pid.isdigit() or command != 'nginx':
                raise GateError('unexpected gateway process')
            result.add(int(pid))
        master = item['State']['Pid']
        if master not in result or len(result) < 2:
            raise GateError('gateway master/workers unavailable')
        return result - {master}

    def http(self, port):
        try:
            token = self.config.get('backendToken', '')
            query = '?' + urlencode({'token': token}) if token else ''
            with self.opener.open(f'http://127.0.0.1:{port}/info' + query, timeout=3) as response:
                raw = response.read(65537)
                if len(raw) > 65536:
                    raise GateError('oversized health response')
                return response.status, json.loads(raw)
        except HTTPError as error:
            code = error.code
            error.close()
            return code, None

    def backend(self, expected):
        if not re.fullmatch(r'sha256:[a-f0-9]{64}', expected or ''):
            raise GateError('expected immutable backend image required')
        item = json.loads(self.docker(['inspect', self.config['backendName']]))[0]
        bindings = item['NetworkSettings']['Ports'].get('3000/tcp')
        if (item['Image'] != expected or not item['State']['Running']
                or bindings != [{'HostIp': '127.0.0.1', 'HostPort': str(self.config['backendPort'])}]):
            raise GateError('backend image/state/private binding mismatch')
        status, info = self.http(self.config['backendPort'])
        if (status != 200 or not isinstance(info, dict)
                or info.get('engine') != self.config.get('backendEngine', 'odx')):
            raise GateError('backend health refused')
        return info

    def change(self, closed, expected=None, seconds=60):
        item = self.inspect()
        if not closed:
            state = json.loads((self.root / 'state.json').read_text())
            if state.get('state') != 'closed':
                raise GateError('verified closed ingress required before opening')
            info = self.backend(expected)
        old_workers = self.workers(item)
        state = {'state': 'closing' if closed else 'opening', 'containerId': item['Id'],
                 'oldWorkers': sorted(old_workers), 'expectedBackendImage': expected}
        save(self.root / 'state.json', state)
        save(self.root / 'public-config/nginx.conf',
             configuration(self.config['listenPort'], self.config['backendPort'], closed,
                           self.config.get('allowedClients', ())), mode=0o644)
        self.docker(['exec', item['Id'], 'nginx', '-t', '-c', '/config/nginx.conf'])
        self.docker(['kill', '--signal', 'HUP', item['Id']])
        deadline = time.monotonic() + seconds
        while True:
            current = self.inspect()
            if current['Id'] != item['Id'] or current['State']['Pid'] != item['State']['Pid']:
                raise GateError('gateway changed while draining')
            workers = self.workers(current)
            try:
                status, response = self.http(self.config['listenPort'])
                proven = status == 503 if closed else status == 200 and isinstance(response, dict) and all(
                    response.get(key) == info.get(key) for key in ('engine', 'engineVersion', 'version'))
            except (OSError, ValueError):
                proven = False
            if not (old_workers & workers) and proven:
                if not closed:
                    self.backend(expected)
                state['state'] = 'closed' if closed else 'open'
                save(self.root / 'state.json', state)
                return {'state': state['state'], 'oldWorkersDrained': True, 'containerId': item['Id']}
            if time.monotonic() >= deadline:
                raise GateError('drain/health unproved; ingress intent retained, do not update or replay')
            time.sleep(.2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('action', choices=('close', 'open'))
    parser.add_argument('--expected-image')
    parser.add_argument('--wait-seconds', type=int, choices=range(1, 86401), default=60)
    args = parser.parse_args()
    try:
        engine_id = local_engine_id()
        with maintenance_authority(engine_id):
            root = Path(args.root).absolute()
            if root.resolve() != root or not root.is_dir() or root.stat().st_mode & 0o077:
                raise GateError('real private operator root required')
            with locked(root):
                ingress = Ingress(root)
                if ingress.config['engineId'] != engine_id:
                    raise GateError('physical engine identity changed')
                print(json.dumps(ingress.change(args.action == 'close', args.expected_image, args.wait_seconds)))
        return 0
    except (GateError, MaintenanceAuthorityBusy, OSError, ValueError, KeyError):
        print('Ingress operation stopped. Inspect private state and exact containers; no automatic reopen.')
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
