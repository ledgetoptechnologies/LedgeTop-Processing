"""SOURCE CANDIDATE: isolated staging-only native swap mechanics fixture.

Default prints the plan. --run is explicit and must be separately reviewed.
No coordinator, real node, host HTTP port, image build/pull, or GPU is used.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import subprocess
import tarfile
import threading
import time
import uuid


HELPER_SHA = '49437cc636a865c646645bdccfa6df07d19eaf5744b5a8bdf21ab5620ef9ff5e'
BASE = 'sha256:5ce31be03711d280e7f97ee3820dd982b68227181165b3f430b037447c3d13b6'
TARGET = 'sha256:9c9f7ed05e0a8d2e4a370a75dd7b8d8524a0603205e79dfc9046239818237940'
NGINX_IMAGES = (
    'sha256:84ed8c0dd296c188bfc24ed506f46589e9285fb0b11cf3a04d91d524d3067e11',
    'sha256:697b0b156c70b91130853fd0675c6be36665939d5f93dce2686be04b4c3f98a8',
)
OWNER = 'ledgetop.retention.native-fixture'
RESERVE = 8 * 1024 ** 3
NODE_BANNER = 'Now using node v14.21.3 (npm v6.14.18)'
NGINX_CONFIG = b'''pid /tmp/nginx.pid;
error_log /dev/stderr warn;
worker_processes 1;
events { worker_connections 64; }
http {
 access_log off;
 client_body_temp_path /tmp/body;
 proxy_temp_path /tmp/proxy;
 fastcgi_temp_path /tmp/fastcgi;
 uwsgi_temp_path /tmp/uwsgi;
 scgi_temp_path /tmp/scgi;
 server { listen 0.0.0.0:3001;
  location / { proxy_pass http://fixture-engine:3000;
   proxy_http_version 1.1; proxy_set_header Connection "";
   proxy_buffering off; proxy_request_buffering off;
  }
 }
}
'''
HTTP_JS = r"""
const http=require('http'),p=JSON.parse(process.env.FIXTURE_REQUEST);
const q=p.token?'?token='+encodeURIComponent(p.token):'';
const req=http.get({hostname:p.host,port:p.port,path:p.route+q},r=>{
 let body='',size=0;r.setEncoding('utf8');r.on('data',x=>{
  size+=Buffer.byteLength(x);if(size>4194304)req.destroy(Error('bound'));else body+=x;
 });r.on('end',()=>console.log(JSON.stringify({status:r.statusCode,body})));
});req.setTimeout(5000,()=>req.destroy(Error('deadline')));
req.on('error',()=>{process.exitCode=1});
"""


def check(ok, code):
    if not ok:
        raise RuntimeError(code)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def namespace(nonce):
    check(isinstance(nonce, str) and re.fullmatch('[0-9a-f]{32}', nonce), 'invalid fixture nonce')
    return 'lt-retention-native-' + nonce[:12]


def owned_object(item, nonce):
    prefix = namespace(nonce)
    labels = item.get('Config', {}).get('Labels') if 'Config' in item else item.get('Labels')
    check((labels or {}).get(OWNER) == nonce, 'resource lacks exact fixture owner label')
    check(item.get('Name', '').lstrip('/').startswith(prefix + '-'), 'resource name outside fixture namespace')


def node_reply(raw):
    lines = raw.splitlines()
    if lines and lines[0] == NODE_BANNER:
        lines = lines[1:]
    check(len(lines) == 1, 'unexpected Node wrapper output')
    reply = json.loads(lines[0])
    check(isinstance(reply, dict) and set(reply) == {'status', 'body'} and
          type(reply['status']) is int and 100 <= reply['status'] <= 599 and
          isinstance(reply['body'], str), 'invalid synthetic HTTP envelope')
    return reply


def failure_reason(error):
    allowed = {'fixture proxy startup failed', 'initial synthetic engine startup failed',
               'bounded host command failed', 'unexpected Node wrapper output',
               'invalid synthetic HTTP envelope', 'fixture engine is not running',
               'fixture HTTP request failed'}
    reason = 'invalid_json' if isinstance(error, json.JSONDecodeError) else str(error)
    return {'class': type(error).__name__, 'reason': reason if reason in allowed or reason == 'invalid_json'
            else 'redacted_exception'}


def bounded_logs(identifier, path, *, seconds=8, max_bytes=65536):
    check(re.fullmatch('[0-9a-f]{64}', identifier), 'invalid diagnostic container ID')
    check(0 < seconds <= 8 and 0 < max_bytes <= 65536, 'invalid diagnostic bounds')
    command = ['docker', '--host', 'unix:///var/run/docker.sock', 'logs', '--tail', '80', identifier]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    expired = threading.Event()
    def expire():
        expired.set()
        if process.poll() is None:
            process.kill()  # Only this owned, bounded log-reader CLI process.
    timer = threading.Timer(seconds, expire)
    timer.start()
    raw, truncated = b'', False
    try:
        raw = process.stdout.read(max_bytes + 1)
        truncated = len(raw) > max_bytes
        if truncated and process.poll() is None:
            process.kill()
        process.wait(timeout=3)
    finally:
        timer.cancel(); timer.join(timeout=1)
        if process.poll() is None:
            process.kill(); process.wait(timeout=3)
        process.stdout.close()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw[:max_bytes])
    return {'file': path.name, 'bytes': min(len(raw), max_bytes),
            'truncated': truncated, 'timedOut': expired.is_set(), 'exit': process.returncode}


def binding_view(container):
    return {'HostConfig': container['HostConfig'], 'Mounts': container['Mounts'],
            'networkKeys': sorted(container.get('NetworkSettings', {}).get('Networks', {}))}


def differing_field_paths(before, after):
    """Names/indices only; never stringify changed values into safe metadata."""
    found = []
    def visit(left, right, path, depth=0):
        if len(found) >= 256:
            return
        if depth > 16 or type(left) is not type(right):
            found.append(path)
        elif isinstance(left, dict):
            for key in sorted(set(left) | set(right)):
                component = key if re.fullmatch('[A-Za-z0-9_.-]{1,80}', key) else 'key-' + sha(key.encode())[:12]
                field = path + ('.' if path else '') + component
                if key not in left or key not in right:
                    found.append(field)
                else:
                    visit(left[key], right[key], field, depth + 1)
                if len(found) >= 256:
                    break
        elif isinstance(left, list):
            if len(left) != len(right):
                found.append(path + '.length')
            for i, (a, b) in enumerate(zip(left, right)):
                visit(a, b, path + '[' + str(i) + ']', depth + 1)
                if len(found) >= 256:
                    break
        elif left != right:
            found.append(path)
    visit(before, after, '')
    return found


def record_failure_diagnostics(runtime, nonce, root, error, context):
    """Private bounded evidence before cleanup; never stdout/config/environment."""
    try:
        check(root.resolve() == root and root.name == namespace(nonce) and
              (root / '.fixture-marker').read_text() == nonce + '\n', 'diagnostic root ownership changed')
        report = {'nonce': nonce, 'exception': failure_reason(error), 'context': context,
                  'containers': [], 'diagnosticErrors': []}
        old_binding = None
        prepared_path = root / 'backup' / 'prepared.private.json'
        if prepared_path.exists():
            try:
                check(prepared_path.resolve() == prepared_path, 'prepared diagnostic source alias')
                old = json.loads(prepared_path.read_text())['oldContainer']
                owned_object(old, nonce)
                old_binding = binding_view(old)
            except Exception as old_error:
                report['diagnosticErrors'].append(failure_reason(old_error))
        safe_binding = {'comparisonAvailable': old_binding is not None,
                        'candidateBindingsCompared': 0, 'differingFieldPaths': []}
        for identifier in runtime.docker('ps', '--all', '--quiet', '--filter',
                                         'label=' + OWNER + '=' + nonce).split():
            try:
                c = runtime.inspect(identifier)
                owned_object(c, nonce)
                row = {'id': c['Id'], 'name': c['Name'], 'image': c['Image'],
                       'state': {k: c['State'].get(k) for k in
                                 ('Status', 'Running', 'ExitCode', 'OOMKilled', 'Restarting')}}
                if c['Image'] == TARGET and c['Config'].get('Labels', {}).get('com.docker.compose.service') == 'nodeodm':
                    check(re.fullmatch('[0-9a-f]{64}', c['Id']), 'invalid candidate diagnostic identity')
                    binding = binding_view(c)
                    path = root / ('diagnostic-' + c['Id'] + '.binding.private.json')
                    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    with os.fdopen(fd, 'w') as stream:
                        json.dump(binding, stream, indent=2)
                    row['bindingFile'] = path.name
                    if old_binding is not None:
                        paths = differing_field_paths(old_binding, binding)
                        row['differingBindingFieldPaths'] = paths
                        safe_binding['candidateBindingsCompared'] += 1
                        safe_binding['differingFieldPaths'].extend(paths)
                try:
                    row['logs'] = bounded_logs(c['Id'], root / ('diagnostic-' + c['Id'][:12] + '.log'))
                except Exception as log_error:
                    row['logsError'] = failure_reason(log_error)
                report['containers'].append(row)
            except Exception as diagnostic_error:
                report['diagnosticErrors'].append(failure_reason(diagnostic_error))
        fd = os.open(root / 'failure-diagnostics.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(report, stream, indent=2)
        fd = os.open(root / 'binding-differences.safe.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(safe_binding, stream, indent=2)
        return True
    except Exception:
        return False  # Diagnostic failure must not suppress owned cleanup.


def load_helper():
    path = Path(__file__).with_name('retained_small_engine_swap.py')
    check(sha(path.read_bytes()) == HELPER_SHA, 'helper differs from reviewed Git-LF source')
    spec = importlib.util.spec_from_file_location('_retained_fixture_helper', path)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper


def engine_compose(prefix, network, volume, image, configuration, token, nonce):
    check(prefix == namespace(nonce), 'fixture namespace differs from owner')
    return {'name': prefix, 'services': {'nodeodm': {
        'image': image, 'container_name': prefix + '-engine', 'pull_policy': 'never',
        'command': ['--max-images', '1000', '--token', token], 'restart': 'no', 'runtime': 'runc',
        'cpus': .5, 'mem_limit': '768m', 'memswap_limit': '768m', 'pids_limit': 128,
        'cap_drop': ['ALL'], 'security_opt': ['no-new-privileges'],
        'environment': {'NVIDIA_VISIBLE_DEVICES': 'void', 'OMP_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1'},
        'tmpfs': ['/tmp:rw,nosuid,nodev,size=64m'],
        'labels': {OWNER: nonce},
        'volumes': [{'type': 'volume', 'source': 'data', 'target': '/var/www/data'},
                    {'type': 'bind', 'source': str(configuration), 'target': '/var/www/config-default.json', 'read_only': True}],
        'networks': {'isolated': {'aliases': ['fixture-engine']}}}},
        'volumes': {'data': {'external': True, 'name': volume}},
        'networks': {'isolated': {'external': True, 'name': network}}}


class FixtureClient:
    def __init__(self, runtime, origin, token='', node=False):
        check(origin in ('http://127.0.0.1:13001', 'http://127.0.0.1:3001'), 'unexpected fixture HTTP origin')
        self.runtime, self.origin, self.token = runtime, origin, token

    def request(self, method, route, **kwargs):
        check(method == 'GET', 'fixture adapter permits no HTTP mutation')
        status, body = self.runtime.internal_http(self.origin.endswith(':3001'), route, self.token)
        check(status == 200, 'fixture HTTP request failed')
        return body.encode()

    def json(self, method, route, **kwargs):
        return json.loads(self.request(method, route, **kwargs))


def fixture_runtime(helper, nonce, network, nginx_image):
    class Runtime(helper.Runtime):
        def __init__(self):
            # No production credential file, API client or live queue exists.
            self.nonce, self.network, self.nginx_image = nonce, network, nginx_image
            self.current_engine = None

        def run(self, args, **kwargs):
            if args and args[0] == 'curl':
                check(args[-1] == 'http://127.0.0.1:3001/info', 'unexpected host curl route')
                status, _ = self.internal_http(True, '/info', '')
                return str(status)
            return super().run(args, **kwargs)

        def internal_http(self, public, route, token):
            check(route in ('/info', '/options', '/task/list') or
                  re.fullmatch('/task/[0-9a-f-]{36}/info', route), 'unexpected synthetic HTTP route')
            c = self.inspect(self.current_engine)
            owned_object(c, nonce)
            check(c['State']['Running'], 'fixture engine is not running')
            packet = {'host': namespace(nonce) + '-ingress' if public else '127.0.0.1',
                      'port': 3001 if public else 3000, 'route': route, 'token': token}
            reply = node_reply(self.docker('exec', '--env', 'FIXTURE_REQUEST=' + json.dumps(packet),
                                           c['Id'], 'node', '-e', HTTP_JS, timeout=8))
            return reply['status'], reply['body']

        def engine_idle(self, identifier, command):
            self.current_engine = identifier
            return super().engine_idle(identifier, command)

        def ingress_inspect(self):
            c = self.inspect(helper.INGRESS)
            owned_object(c, nonce)
            check(c['Id'] == helper.INGRESS and c['Image'] == self.nginx_image and c['State']['Running'] and
                  c['Config']['Entrypoint'] == ['nginx'] and
                  c['Config']['Cmd'] == ['-c', '/config/nginx.conf', '-g', 'daemon off;'] and
                  c['HostConfig']['NetworkMode'] == network and
                  not c['HostConfig']['PortBindings'] and
                  set(c['NetworkSettings']['Networks']) == {network}, 'fixture ingress binding changed')
            mounts = [m for m in c['Mounts'] if m['Destination'] == '/config']
            check(len(mounts) == 1 and mounts[0]['Type'] == 'bind' and not mounts[0]['RW'] and
                  mounts[0]['Source'] == str(helper.NGINX.parent), 'fixture ingress source changed')
            return c

    runtime = Runtime()
    # Only a private imported module is rebound. Production helper bytes stay
    # unchanged; callers cannot accidentally reuse its real-host identities.
    helper.Client = lambda origin, token='', node=False: FixtureClient(runtime, origin, token, node)
    return runtime


def cleanup_resources(runtime, nonce, network, volume):
    identifiers = runtime.docker('ps', '--all', '--quiet', '--filter', 'label=' + OWNER + '=' + nonce).split()
    for identifier in identifiers:
        c = runtime.inspect(identifier)
        owned_object(c, nonce)
        if c['State']['Running']:
            runtime.docker('stop', '--time', '15', identifier, timeout=25)
        check(not runtime.inspect(identifier)['State']['Running'], 'owned test container remains running')
        runtime.docker('rm', identifier)
    volumes = runtime.docker('volume', 'ls', '--format', '{{.Name}}').splitlines()
    if volume in volumes:
        v = json.loads(runtime.docker('volume', 'inspect', volume))[0]
        owned_object(v, nonce)
        data = Path(v['Mountpoint'])
        check(data.is_absolute() and data.resolve() == data and data.name == '_data' and
              (data / '.fixture-marker').read_text() == nonce + '\n', 'fixture volume marker changed')
        check(not runtime.docker('ps', '--all', '--quiet', '--filter', 'volume=' + volume).strip(),
              'volume still has a container user')
        runtime.docker('volume', 'rm', volume)
    networks = runtime.docker('network', 'ls', '--format', '{{.Name}}').splitlines()
    if network in networks:
        n = json.loads(runtime.docker('network', 'inspect', network))[0]
        owned_object(n, nonce)
        check(not n.get('Containers'), 'fixture network still has peers')
        runtime.docker('network', 'rm', network)


def remove_owned_backup(runtime, path, nonce):
    check(path.is_absolute() and path.resolve() == path and path.name == 'backup' and
          path.parent.name == namespace(nonce), 'backup cleanup path changed')
    check(path.stat().st_uid == 0 and stat.S_IMODE(path.stat().st_mode) == 0o700 and
          (path / '.fixture-marker').read_text() == nonce + '\n', 'backup cleanup marker changed')
    for identifier in runtime.docker('ps', '--all', '--quiet').split():
        for mount in runtime.inspect(identifier)['Mounts']:
            source = mount.get('Source', '')
            if source.startswith('/'):
                common = os.path.commonpath([source, path])
                check(not (common == str(path) or (mount.get('RW') and common == source)),
                      'another container still references fixture backup')
    # Docker effects never add mounts beneath this host-private archive root.
    mounts = json.loads(runtime.run(['findmnt', '--json', '--list', '--output', 'TARGET']))
    check(not any(os.path.commonpath([entry['target'], path]) == str(path)
                  for entry in mounts['filesystems']), 'backup has a host submount')
    for base, dirs, files in os.walk(path):
        check(not any((Path(base) / p).is_symlink() for p in dirs), 'backup has unexpected directory alias')
    shutil.rmtree(path)


def _run_fixture(recovery):
    check(os.name == 'posix' and os.geteuid() == 0, 'fixture requires root on staging90')
    addresses = json.loads(subprocess.check_output(['ip', '-j', 'address', 'show']))
    ips = {a.get('local') for n in addresses for a in n.get('addr_info', [])}
    check('192.168.50.90' in ips and '192.168.50.89' not in ips and '192.168.50.88' not in ips,
          'fixture refuses processing hosts')
    helper, nonce = load_helper(), uuid.uuid4().hex
    prefix = namespace(nonce)
    root = Path('/var/lib') / prefix
    network, volume = prefix + '-network', prefix + '-data'
    runtime = fixture_runtime(helper, nonce, network, NGINX_IMAGES[0])
    # Inspect cached images before creating resources. Never pull/build images.
    images = {p: json.loads(runtime.docker('image', 'inspect', p))[0] for p in (BASE, TARGET)}
    nginx_image = None
    for image in NGINX_IMAGES:
        try:
            if json.loads(runtime.docker('image', 'inspect', image))[0]['Id'] == image:
                nginx_image = image
                break
        except Exception:
            pass
    check(nginx_image is not None, 'no reviewed cached fixture nginx image')
    runtime.nginx_image = nginx_image
    check(all(v['Id'] == k and v['Architecture'] == 'amd64' for k, v in images.items()), 'image pin changed')
    check(shutil.disk_usage('/').free > RESERVE + images[BASE]['Size'] + 256 * 1024 ** 2,
          'one full export would breach staging reserve')
    os.umask(0o077); root.mkdir(mode=0o700)
    (root / '.fixture-marker').write_text(nonce + '\n')
    backup, work, public = root / 'backup', root / 'work', root / 'public-config'
    backup.mkdir(mode=0o700); work.mkdir(mode=0o700); public.mkdir(mode=0o755)
    public.chmod(0o755)
    (backup / '.fixture-marker').write_text(nonce + '\n')
    configuration = work / 'fixture-config.json'
    configuration.write_text(json.dumps({'logger': {'logDirectory': '/tmp'}})); configuration.chmod(0o444)
    compose_path, nginx = work / 'candidate-compose.json', public / 'nginx.conf'
    proxy_pass = b'proxy_pass http://fixture-engine:3000;'
    nginx.write_bytes(NGINX_CONFIG); nginx.chmod(0o644)
    recovery.update(runtime=runtime, nonce=nonce, network=network, volume=volume, root=root)
    runtime.docker('network', 'create', '--internal', '--label', OWNER + '=' + nonce, network)
    runtime.docker('volume', 'create', '--label', OWNER + '=' + nonce, volume)
    data = Path(json.loads(runtime.docker('volume', 'inspect', volume))[0]['Mountpoint'])
    check(data.resolve() == data and data.name == '_data', 'fixture data alias')
    (data / '.fixture-marker').write_text(nonce + '\n')
    os.chown(data, 1001, 1001)
    ids = [str(uuid.uuid5(uuid.UUID(nonce), str(i))) for i in range(3)]
    tasks, now = [], int(time.time() * 1000)
    for i, identifier in enumerate(ids):
        d = data / identifier; d.mkdir(mode=0o700); os.chown(d, 1001, 1001)
        for sub in ('images', 'gcp'):
            p = d / sub; p.mkdir(mode=0o700); os.chown(p, 1001, 1001)
        for name, content in [('source.bin', b'synthetic input '), ('result.bin', b'synthetic result ')]:
            p = d / name; p.write_bytes(content + nonce.encode()); os.chown(p, 1001, 1001)
        tasks.append(dict(uuid=identifier, name='synthetic', options=[], outputs=[], skipPostProcessing=True,
                          dateCreated=now, processingTime=1, status={'code': (30, 40, 50)[i]}))
    index = data / 'tasks.json'; index.write_text(json.dumps(tasks)); os.chown(index, 1001, 1001)
    token = secrets.token_hex(32)
    config = engine_compose(prefix, network, volume, BASE, configuration, token, nonce)
    compose_path.write_text(json.dumps(config))
    compose = ['compose', '-p', prefix, '--project-directory', str(work), '-f', str(compose_path)]
    result, success, phase = {}, False, 'engine_startup'
    proxy_observation = {'lastStatus': None, 'lastException': None}
    try:
        runtime.docker(*compose, 'up', '-d', '--no-deps', '--no-build', '--pull', 'never', 'nodeodm', timeout=90)
        engine = runtime.docker(*compose, 'ps', '--all', '--quiet', 'nodeodm').strip()
        check(engine, 'fixture engine identity unavailable')
        runtime.current_engine = engine
        for name, value in dict(PROJECT=prefix, VOLUME=volume, DATA=data, COMPOSE=compose_path,
                                NGINX=nginx, PASS=proxy_pass, ENGINE=engine,
                                INGRESS_IMAGE=nginx_image).items():
            setattr(helper, name, value)
        deadline = time.monotonic() + 90
        while True:
            try:
                before = runtime.engine_idle(engine, config['services']['nodeodm']['command'])
                break
            except Exception:
                check(time.monotonic() < deadline, 'initial synthetic engine startup failed')
                time.sleep(1)
        # Distinguish restored tmp metadata from the child image's defaults.
        saved_tmp = work / 'fixture-gitignore'
        saved_tmp.write_text('*'); os.chown(saved_tmp, 1001, 1001); saved_tmp.chmod(0o640)
        runtime.docker('cp', '--archive', str(saved_tmp), engine + ':/var/www/tmp/.gitignore')
        root_sentinel = 'retained-root-' + nonce
        saved_root = work / 'fixture-root-retained.bin'
        saved_root.write_text(root_sentinel)
        runtime.docker('cp', str(saved_root), engine + ':/var/www/fixture-root-retained.bin')
        ingress = runtime.docker('create', '--name', prefix + '-ingress', '--label', OWNER + '=' + nonce,
            '--network', network, '--runtime', 'runc', '--entrypoint', 'nginx', '--user', '101:101',
            '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
            '--cpus', '.25', '--memory', '64m', '--memory-swap', '64m', '--pids-limit', '32',
            '--tmpfs', '/tmp:rw,nosuid,nodev,size=64m', '--mount',
            'type=bind,src=' + str(public) + ',dst=/config,readonly', nginx_image,
            '-c', '/config/nginx.conf', '-g', 'daemon off;').strip()
        helper.INGRESS = ingress
        runtime.docker('start', ingress)
        phase = 'proxy_startup'
        deadline = time.monotonic() + 30
        while True:
            try:
                status, _ = runtime.internal_http(True, '/info', token)
                proxy_observation['lastStatus'] = status
                if status == 200:
                    break
            except Exception as error:
                proxy_observation['lastException'] = failure_reason(error)
            check(time.monotonic() < deadline, 'fixture proxy startup failed')
            time.sleep(.25)
        old = runtime.inspect(engine)
        owned_object(old, nonce)
        normalized = json.loads(runtime.docker(*compose, 'config', '--format', 'json'))
        prepared = dict(requestId=str(uuid.uuid4()), nginxHex=nginx.read_bytes().hex(),
                        command=config['services']['nodeodm']['command'], oldContainer=old,
                        engine=before, composeSha256=sha(compose_path.read_bytes()),
                        candidate=helper.candidate_config(normalized, old['Config']['Cmd']))
        helper.save(backup / 'prepared.private.json', prepared)
        phase = 'ingress_close'
        runtime.change_ingress(prepared, closed=True)
        check(stat.S_IMODE(nginx.stat().st_mode) == 0o644, 'closed nginx mode changed under umask077')
        runtime.engine_idle(engine, prepared['command'])
        phase = 'backup'
        runtime.stop_and_backup(prepared, backup)
        # The actual Docker export must include old unmounted writable state.
        with tarfile.open(backup / 'engine-rootfs.tar', 'r:') as archive:
            stream = archive.extractfile('var/www/fixture-root-retained.bin')
            check(stream is not None and stream.read(1024).decode() == root_sentinel,
                  'full-root export omitted original writable state')
        phase = 'recreate_and_verify'
        runtime.recreate_and_verify(prepared, backup)
        current = runtime.current_engine
        check(current != engine, 'Compose did not actually recreate the engine')
        check(runtime.docker('exec', current, 'stat', '-c', '%a %u %g', '/var/www/tmp/.gitignore').strip() ==
              '640 1001 1001', 'saved tmp ownership/mode was not preserved')
        runtime.persist_pin(prepared)
        phase = 'ingress_reopen'
        runtime.change_ingress(prepared, closed=False)
        check(stat.S_IMODE(nginx.stat().st_mode) == 0o644, 'reopened nginx mode changed under umask077')
        phase = 'candidate_stop'
        runtime.docker('stop', '--time', '15', current, timeout=25)
        stopped = runtime.inspect(current)['State']
        check(stopped['ExitCode'] == 0 and not stopped['OOMKilled'], 'candidate shutdown was not clean')
        helper.verify_retained(json.loads((backup / 'retained-before.json').read_text()),
                               helper.snapshot_tree(data, deadline=time.monotonic() + 30))
        result = dict(passed=True, helperSha256=HELPER_SHA, nonce=nonce, base=BASE, target=TARGET,
                      nginxImage=nginx_image, recreated=True, terminalTasks=len(before['tasks']),
                      volumeBackupCompared=True, rootWritableExportVerified=True, tmpMetadataPreserved=True,
                      retainedFilesVerified=True, ingressClosedReopenedAndWorkersDrained=True,
                      candidateExitCode=stopped['ExitCode'], candidateOom=stopped['OOMKilled'],
                      sourceHashes=helper.SOURCE_HASHES, publishedPorts=[], imageBuilds=0, imagePulls=0,
                      scope='isolated native mechanics; no live-host/queue/client/GPU acceptance')
        helper.save(root / 'result.json', result)
        success = True
    except Exception as error:
        record_failure_diagnostics(runtime, nonce, root, error,
                                   {'phase': phase, 'proxyStartup': proxy_observation})
        raise
    finally:
        recovery['cleanupStarted'] = True
        cleanup_resources(runtime, nonce, network, volume)
        if success:
            remove_owned_backup(runtime, backup, nonce)
            result.update(ownedContainersVolumesNetworksRemoved=True, bulkBackupRemoved=True,
                          evidenceRoot=str(root), freeBytes=shutil.disk_usage('/').free)
            helper.save(root / 'result.json', result)
    return result


def run_fixture():
    recovery = {}
    try:
        return _run_fixture(recovery)
    except Exception as error:
        if recovery and not recovery.get('cleanupStarted') and recovery.get('root'):
            record_failure_diagnostics(recovery['runtime'], recovery['nonce'], recovery['root'],
                                       error, {'phase': 'partial_setup'})
        raise
    finally:
        # Setup can fail before the main rehearsal's try/finally is entered.
        # Inspect exact labels/names then clean only those partial resources.
        # Never automatically retry a cleanup whose daemon response was lost.
        if recovery and not recovery.get('cleanupStarted'):
            recovery['cleanupStarted'] = True
            cleanup_resources(recovery['runtime'], recovery['nonce'], recovery['network'], recovery['volume'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    if not args.run:
        print(json.dumps({'mode': 'source-plan', 'writes': False, 'requiresSeparateReview': True,
                          'helperSha256': HELPER_SHA, 'host': 'staging90 only',
                          'imagesMustAlreadyBeCached': [BASE, TARGET, list(NGINX_IMAGES)],
                          'minimumFreeBytes': 'base image Size plus 8GiB reserve plus 256MiB',
                          'publishedPorts': [], 'imageBuilds': 0, 'imagePulls': 0}))
        return 0
    try:
        print(json.dumps(run_fixture()))
        return 0
    except Exception:
        print('Owned native fixture failed; retain its private artifacts for inspection. No live node was selected.')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
