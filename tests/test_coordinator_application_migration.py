"""Opt-in complete schema1 application cold lifecycle; synthetic local state only."""
from contextlib import closing
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import sqlite3
import ssl
import subprocess
import sys
import threading
import time
import unittest
import uuid
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

LEGACY_COMMIT = '671fd2a6bde40088bdfad91ccf67eab45ee3d8d7'
CANDIDATE_COMMIT = '6bbd468e1177746cb695e43195e75355b0681f23'
SOURCE_PINS = {
    LEGACY_COMMIT: (49, 'c5d141da519e4787e26fb2c01dfc6506d4834f730885c802a940c36d87ddbb77'),
    CANDIDATE_COMMIT: (63, '04a14d73f7f1652b82a61428be51f9dd751b9bc957094500f8eb776458387add'),
}
GIT_LF_PINS = {
    LEGACY_COMMIT: 'a0dc9950d3d4dc33aa513524cb04ced518df7efb4f67b7c55a5566becb122e12',
    CANDIDATE_COMMIT: '06905adbf065ef2f4d3d3e027aa6ba90a49c99c806f4b15d629cdff7cfe73109',
}
IMAGES = {
    LEGACY_COMMIT: 'sha256:649f53f2b5dc32cd9eb229c42b6bbd7b479249fba5a8d65c48d8afc4f157b249',
    CANDIDATE_COMMIT: 'sha256:e96d957f3eff3f6a33fa673fa9f8ce211d82969c3724d16ba24721f6b376f42b',
}
API = 'synthetic-migration-native-' + 'a'*32
VIEWER = 'synthetic-migration-reference-' + 'b'*32
WORKER = 'synthetic-migration-worker-' + 'c'*32


def packed(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def source_pin(root, commit):
    root = Path(root).resolve()
    files = {'processing/'+p.relative_to(root/'processing').as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in sorted((root/'processing').rglob('*.py'))}
    assert (len(files), hashlib.sha256(packed(files)).hexdigest()) == SOURCE_PINS[commit], 'source closure differs'
    normalized = {'processing/'+p.relative_to(root/'processing').as_posix():
        hashlib.sha256(p.read_bytes().replace(b'\r\n', b'\n')).hexdigest()
        for p in sorted((root/'processing').rglob('*.py'))}
    assert hashlib.sha256(packed(normalized)).hexdigest() == GIT_LF_PINS[commit], 'Git closure differs'


def logical_state(path):
    with closing(sqlite3.connect('file:'+str(path)+'?mode=ro', uri=True)) as db:
        schema = db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name').fetchall()
        rows = {}
        for kind, name, _, _ in schema:
            if kind == 'table':
                assert name.replace('_', '').isalnum()
                rows[name] = sorted(db.execute('SELECT * FROM "'+name+'"').fetchall(), key=packed)
        return dict(schema=schema, rows=rows)


def child(action, root, source, commit):
    source_pin(source, commit)
    # No Processing imports precede the exact source closure and subprocess path check.
    from processing.app import make_handler, CoordinatorService
    from processing.config import Config
    from processing.prototype import Queue
    import processing.app as app
    assert Path(app.__file__).resolve().parent == Path(source).resolve()/'processing'
    root = Path(root)
    settings = json.loads((root/'test-settings.json').read_text())
    config = Config(api_token=API, local_node_url=settings['backend'], main_node_url=settings['backend'],
        local_max_photos=1000, local_max_input_bytes=20*1024**3,
        prototype_state_dir=str(root/'state'), prototype_node_ids=('cluster', 'main'),
        treedar_node_ids=('main',), viewer_enabled=True, viewer_token='synthetic-legacy-viewer-'+'d'*32,
        reference_config_path=str(root/'private-synthetic.json'))
    handler = make_handler(config)
    queue = handler.prototype_queue
    if action == 'seed':
        try:
            with queue._db() as db:
                for index in range(12):
                    db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,'staging','treedar-synthetic-v1',?)",
                        ('synthetic-history-'+str(index), 'synthetic-request-'+str(index), 'failed' if index < 2 else 'succeeded'))
            import uuid
            for node in ('cluster', 'main'):
                node_state = queue.get_node(node)
                receipt = queue.schedule_node_owned(node, dict(schemaVersion=1, requestId=str(uuid.uuid4()),
                    authorityId=queue.authority_id, expectedRevision=node_state['revision'],
                    drainAt='2026-01-01T00:00:00Z', notBefore='2026-01-01T00:00:00Z', afterJobId=None,
                    targetImage='sha256:'+'a'*64))
                queue.cancel_node(node, dict(expectedRevision=receipt['revision'], planId=receipt['planId']))
            result = dict(authority=queue.authority_id)
        finally:
            handler.reference_listener.close()
            handler.viewer_service.close()
            queue.close()
    else:
        before = logical_state(root/'state/jobs.sqlite3')
        original_authority = queue.authority_id
        assert len(before['rows']['jobs']) == 12
        assert len(before['rows']['owned_maintenance_schedules']) == 2
        assert all(not values for name, values in before['rows'].items() if name.startswith(('reference_', 'viewer_')))
        assert not any('recover' in name or 'pipeline' in name or 'control_charge' in name for name in before['rows'])
        assert getattr(handler.reference_listener.control.worker, 'recovery', None) is None
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        service = CoordinatorService(server, handler)
        stop = threading.Event()
        outcomes = []
        thread = threading.Thread(target=lambda: outcomes.append(service.run(stop)))
        thread.start()
        native_port = server.server_port
        ref_port = settings['tlsPort']
        def get(path, token=None, secure=False):
            port = ref_port if secure else native_port
            url = ('https' if secure else 'http')+'://127.0.0.1:'+str(port)+path
            headers = {} if token is None else {'Authorization':'Bearer '+token}
            context = ssl.create_default_context(cafile=str(root/'cert.pem'))
            opener = build_opener(ProxyHandler({}), HTTPSHandler(context=context))
            try:
                with opener.open(Request(url, headers=headers), timeout=3) as response:
                    return response.status, json.loads(response.read(262144))
            except HTTPError as error:
                try: return error.code, json.loads(error.read(262144))
                finally: error.close()
        try:
            deadline = time.monotonic()+5
            while not service.healthy() and thread.is_alive() and time.monotonic() < deadline:
                stop.wait(0.01)
            assert service.healthy(), service.health()
            assert get('/healthz')[0] == 200
            assert get('/v1/prototype/authority', API)[1]['authorityId'] == original_authority
            assert get('/v1/prototype/jobs', API)[0] == 200
            assert get('/ltds/v1/capabilities', secure=True)[0] == 401
            status, capabilities = get('/ltds/v1/capabilities', VIEWER, secure=True)
            assert status == 200, capabilities
            assert capabilities['engines']['odm']['available'] is True
            assert capabilities['engines']['treedar']['available'] is False
            assert capabilities['engines']['gaussian_splat']['enabled'] is False
            assert get('/ltds/v1/capabilities', WORKER, secure=True)[0] == 401
            assert handler.reference_listener.queue is queue is handler.viewer_service.queue
            # A second actual Queue cannot acquire the live application's state lease.
            try:
                other = Queue(root/'state', node_ids=('cluster', 'main'), reference_enabled=True)
            except (RuntimeError, ValueError):
                pass
            else:
                other.close()
                raise AssertionError('live application state admitted a second owner')
        finally:
            stop.set(); thread.join(10)
        assert not thread.is_alive() and outcomes == [0], (outcomes, service.health())
        assert not service.health()['nativeAlive'] and not service.health()['referenceAlive']
        assert not handler.viewer_service._thread.is_alive()
        for port in (native_port, ref_port):
            with socket.socket() as probe:
                probe.settimeout(1)
                assert probe.connect_ex(('127.0.0.1', port)) != 0, 'listener remained open'
        assert logical_state(root/'state/jobs.sqlite3') == before, 'application changed preserved state'
        # Shutdown must release the real OS state lock, not merely return health.
        reopened = Queue(root/'state', node_ids=('cluster', 'main'), reference_enabled=True)
        assert reopened.authority_id == original_authority
        reopened.close()
        result = dict(authority=original_authority, nativeClosed=True, tlsClosed=True,
            statePreserved=True, workerRecoveryEnabled=False, applicationModule=app.__file__)
    # Every loaded Processing module is from this one exact closure.
    loaded = [module.__file__ for name, module in sys.modules.items()
              if name.startswith('processing') and getattr(module, '__file__', '').endswith('.py')]
    assert all(Path(path).resolve().is_relative_to(Path(source).resolve()/'processing') for path in loaded)
    result.update(sourceCommit=commit, loadedProcessingModules=len(loaded))
    return result


def image_phase(action, commit):
    """Runs inside one exact local image; only /qualification is persistent."""
    assert os.geteuid() == 10001 and os.getegid() == 10001, 'actual application user differs'
    root = Path('/qualification')
    requests = []
    class Backend(BaseHTTPRequestHandler):
        def log_message(self, *_): pass
        def do_GET(self):
            requests.append(('GET', self.path))
            value = (dict(version='2.2.0', engine='odm', engineVersion='synthetic-idle',
                maxImages=None, taskQueueCount=0) if self.path == '/info' else
                [dict(name=name, type='bool', value=False) for name in ('pc-ept', 'gltf', '3d-tiles')]
                if self.path == '/options' else [])
            body = packed(value); self.send_response(200)
            self.send_header('Content-Type', 'application/json'); self.send_header('Content-Length', str(len(body)))
            self.end_headers(); self.wfile.write(body)
        def do_POST(self):
            requests.append(('POST', self.path)); self.send_error(409)
    backend = ThreadingHTTPServer(('127.0.0.1', 19001), Backend)
    backend_thread = threading.Thread(target=backend.serve_forever, kwargs={'poll_interval':0.02})
    backend_thread.start()
    try:
        private_path = root/'private-synthetic.json'
        if action == 'seed':
            assert not list(root.iterdir()), 'qualification volume must be new'
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                '-subj', '/CN=synthetic-migration', '-addext', 'subjectAltName=IP:127.0.0.1',
                '-addext', 'basicConstraints=critical,CA:TRUE', '-keyout', str(root/'key.pem'),
                '-out', str(root/'cert.pem')], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            (root/'key.pem').chmod(0o600)
            (root/'test-settings.json').write_bytes(packed(dict(backend='http://127.0.0.1:19001', tlsPort=19443)))
            private = dict(schemaVersion=1, listener=dict(host='127.0.0.1', port=19443,
                certificate=str(root/'cert.pem'), privateKey=str(root/'key.pem'), clientCA=str(root/'cert.pem')),
                viewerBearers={'staging':VIEWER}, workers={node:dict(
                    bearer=WORKER if node == 'cluster' else 'synthetic-main-worker-'+'e'*32,
                    workerId='worker-'+node, instanceId='instance-'+node,
                    resultOrigin='https://127.0.0.1:19999', resultBearer='synthetic-result-'+node+'-'+'f'*32)
                    for node in ('cluster', 'main')}, streamBudget=dict(maxConcurrent=1, maxObjectBytes=4194304))
            private_path.write_bytes(packed(private)); private_path.chmod(0o600)
        else:
            assert packed(logical_state(root/'state/jobs.sqlite3')) == (root/'baseline.json').read_bytes()
        original_config = private_path.read_bytes()
        result = child(action, root, '/app', commit)
        assert private_path.read_bytes() == original_config
        current = packed(logical_state(root/'state/jobs.sqlite3'))
        if action == 'seed': (root/'baseline.json').write_bytes(current)
        else:
            assert current == (root/'baseline.json').read_bytes()
            assert requests, 'actual dynamic discovery did not contact fixture'
        assert all(method == 'GET' and path in ('/info', '/options') for method, path in requests)
        result.update(logicalSha256=hashlib.sha256(current).hexdigest(),
            configurationSha256=hashlib.sha256(original_config).hexdigest(),
            backendRequests=len(requests), backendMutations=0)
        return result
    finally:
        backend.shutdown(); backend.server_close(); backend_thread.join(5)
        assert not backend_thread.is_alive()


@unittest.skipUnless(os.environ.get('MIGRATION_IMAGE_ORACLE') == '1',
    'explicit local Docker image application lifecycle oracle required')
class CoordinatorApplicationMigrationTests(unittest.TestCase):
    def test_actual_old_candidate_old_schema1_application_lifecycle_preserves_history(self):
        repo = Path(__file__).resolve().parents[1]
        volume = 'ltds-migration-oracle-'+uuid.uuid4().hex
        def docker(*args):
            result = subprocess.run(['docker', *args], capture_output=True, text=True, timeout=45)
            self.assertEqual(result.returncode, 0, result.stderr)
            return result.stdout.strip()
        for image in IMAGES.values():
            self.assertEqual(docker('image', 'inspect', image, '--format', '{{.Id}}'), image)
        docker('volume', 'create', '--label', 'com.ledgetop.qualifier=coordinator-application-migration', volume)
        try:
            common = ['run', '--rm', '--pull', 'never', '--network', 'none', '--read-only',
                '--cpus', '2', '--memory', '2g', '--pids-limit', '128', '--cap-drop', 'ALL',
                '--security-opt', 'no-new-privileges', '--tmpfs', '/tmp:rw,nosuid,nodev,size=256m',
                '--mount', 'type=volume,source='+volume+',target=/qualification']
            # Initialize only this new empty local volume root, without recursive operations.
            docker(*common, '--user', '0', '--cap-add', 'CHOWN', '--entrypoint', 'python',
                IMAGES[LEGACY_COMMIT], '-c', 'import os;os.chmod("/qualification",0o700);os.chown("/qualification",10001,10001)')
            reports = []
            for action, commit in (('seed', LEGACY_COMMIT), ('lifecycle', LEGACY_COMMIT),
                    ('lifecycle', CANDIDATE_COMMIT), ('lifecycle', LEGACY_COMMIT)):
                report = json.loads(docker(*common, '--mount', 'type=bind,source='+str(repo)+',target=/review,readonly',
                    '--workdir', '/app', '--env', 'PYTHONDONTWRITEBYTECODE=1', '--env', 'PYTHONPATH=/app',
                    '--entrypoint', 'python', IMAGES[commit], '-B', '-W', 'error::ResourceWarning',
                    '/review/tests/test_coordinator_application_migration.py', '--image-phase', action, commit))
                reports.append(report)
            for report in reports[1:]:
                self.assertEqual(report['authority'], reports[0]['authority'])
                self.assertEqual(report['logicalSha256'], reports[0]['logicalSha256'])
                self.assertEqual(report['configurationSha256'], reports[0]['configurationSha256'])
                self.assertTrue(report['statePreserved'])
                self.assertFalse(report['workerRecoveryEnabled'])
                self.assertEqual(report['backendMutations'], 0)
            print(json.dumps(dict(migration='actual-old-image_to_candidate_to_actual-old-image', reports=reports)))
        finally:
            docker('volume', 'rm', volume)


if __name__ == '__main__':
    if len(sys.argv) == 6 and sys.argv[1] == '--child':
        print(json.dumps(child(*sys.argv[2:])))
    elif len(sys.argv) == 4 and sys.argv[1] == '--image-phase':
        print(json.dumps(image_phase(*sys.argv[2:])))
    else:
        unittest.main()
