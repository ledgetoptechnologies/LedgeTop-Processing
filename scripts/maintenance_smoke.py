"""Disposable real-Compose update/rollback acceptance with a NodeODX API fixture.

No real engine processing or existing service is used. This tests the container
maintenance mechanism; it is not acceptance of a particular NodeODX release.
"""
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
import uuid
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from processing.app import make_handler
from processing.config import Config
from processing.container_update import ComposeNode, execute
from processing.odx_smoke_worker import Client

BASE = 'sha256:9c234ac9f7be3dd7579747280afb7584c3eb85c3a59825cd2992e378aa36b2b0'
SERVER = '''import json,os
from pathlib import Path
from http.server import BaseHTTPRequestHandler,HTTPServer
p=Path('/owned/marker')
if not p.exists(): p.write_text('keep-me')
class H(BaseHTTPRequestHandler):
 def log_message(self,*a):pass
 def do_GET(self):
  bad=os.environ.get('FIXTURE_TIER')=='bad' and self.path=='/info'
  body=json.dumps([] if self.path=='/task/list' else {'engine':'odx','version':'fixture','engineVersion':'fixture','taskQueueCount':0}).encode()
  self.send_response(503 if bad else 200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
HTTPServer(('0.0.0.0',3000),H).serve_forever()
'''


def main():
    name = 'lt-maintenance-smoke-' + uuid.uuid4().hex[:10]
    evidence_path = Path(sys.argv[1]).absolute()
    if evidence_path.exists() or not evidence_path.parent.is_dir():
        raise ValueError('new metadata-only evidence path required')
    with tempfile.TemporaryDirectory(prefix=name + '-') as temporary:
        root = Path(temporary)
        root.chmod(0o700)
        def command(args, timeout=90):
            result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
            if result.returncode:
                raise RuntimeError('isolated fixture operation failed: ' + result.stderr[-1000:])
            return result.stdout.strip()
        engine_id = command(['docker', 'info', '--format', '{{.ID}}'])
        assert command(['docker', 'image', 'inspect', '--format', '{{.Id}}', BASE]) == BASE
        base_tag = name + '-base:local'
        tags, images = [], {}
        original_base_tags = json.loads(command(['docker', 'image', 'inspect', '--format', '{{json .RepoTags}}', BASE])) or []
        command(['docker', 'tag', BASE, base_tag])
        (root / 'server.py').write_text(SERVER)
        (root / 'Dockerfile').write_text('FROM ' + base_tag + '''
USER root
RUN mkdir -p /var/www/tmp /owned && chown 10001:10001 /owned && ln -s /usr/local/bin/python /usr/bin/node
COPY server.py /var/www/index.js
ARG TIER
ENV FIXTURE_TIER=$TIER
USER 10001:10001
HEALTHCHECK NONE
ENTRYPOINT ["/usr/bin/node", "/var/www/index.js"]
''')
        compose_file = root / 'compose.json'
        compose = ['docker', 'compose', '-p', name, '-f', str(compose_file)]
        handler = server = None
        try:
            for tier in ('old', 'new', 'bad'):
                tag = name + ':' + tier
                command(['docker', 'build', '--pull=false', '--build-arg', 'TIER=' + tier,
                         '--tag', tag, str(root)])
                tags.append(tag)
                images[tier] = command(['docker', 'image', 'inspect', '--format', '{{.Id}}', tag])
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', 0))
                port = listener.getsockname()[1]
            common = {'image': images['old'], 'read_only': True, 'init': True,
                      'cap_drop': ['ALL'], 'cpus': .25, 'mem_limit': '64m', 'pids_limit': 32,
                      'security_opt': ['no-new-privileges:true'],
                      'tmpfs': ['/var/www/tmp:rw,size=16m,mode=1777']}
            compose_file.write_text(json.dumps({'services': {
                'node': {**common, 'ports': ['127.0.0.1:' + str(port) + ':3000'], 'volumes': ['proof:/owned']},
                'sibling': {**common, 'volumes': ['sibling-proof:/owned']}},
                'volumes': {'proof': {}, 'sibling-proof': {}}}))
            command([*compose, 'up', '-d', '--no-build', '--pull', 'never'])
            sibling = command([*compose, 'ps', '-q', 'sibling'])
            token, operator = secrets.token_urlsafe(36), secrets.token_urlsafe(36)
            handler = make_handler(Config(token, '', '', 800, 10000,
                prototype_state_dir=str(root / 'queue'), prototype_operator_token=operator))
            server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            origin = 'http://127.0.0.1:' + str(server.server_port)
            api, control = Client(origin, token), Client(origin, operator)
            queue = handler.prototype_queue
            args = SimpleNamespace(project=name, service='node', compose_file=str(compose_file),
                node_origin='http://127.0.0.1:' + str(port), docker_engine_id=engine_id, node_port=3000)
            def plan(image, after=None):
                now = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
                return queue.schedule_node('cluster', {'expectedRevision': queue.get_node('cluster')['revision'],
                    'drainAt': now, 'notBefore': now, 'afterJobId': after, 'targetImage': image})
            job, _ = queue.submit({'requestId': 'running', 'environment': 'staging', 'profile': 'treedar-synthetic-v1'})
            claimed = queue.claim({'workerId': 'fixture'})
            queued, _ = queue.submit({'requestId': 'waiting', 'environment': 'production', 'profile': 'treedar-synthetic-v1'})
            plan(images['new'], job['id'])
            first = root / 'first'
            first.mkdir()
            adapter = ComposeNode(args, first)
            original_docker = adapter.docker
            def traced_docker(command_args, **kwargs):
                try:
                    return original_docker(command_args, **kwargs)
                except Exception:
                    # Fixture commands carry no credentials; diagnose only this
                    # disposable namespace, never a configured real node.
                    print('fixture-failed-command', command_args, flush=True)
                    raise
            adapter.docker = traced_docker
            # Wait for fixture readiness independently before testing the drain.
            from processing.container_update import wait_healthy
            if not wait_healthy(adapter, images['old'], 15):
                try:
                    print('fixture-idle-check', adapter.idle())
                    item = adapter.inspect()
                    print('fixture-processes', command(['docker', 'top', item['Id'], '-eo', 'args']))
                except Exception as exc:
                    print('fixture-readiness-category', type(exc).__name__, str(exc))
                    identifier = command([*compose, 'ps', '--all', '-q', 'node'])
                    if identifier:
                        print('fixture-container-log', command(['docker', 'logs', identifier]))
                raise AssertionError('isolated fixture did not satisfy adapter readiness')
            before = adapter.inspect()['Id']
            assert execute(api, control, adapter, 'cluster', first)['status'] == 'waiting'
            assert adapter.inspect()['Id'] == before and queue.claim({'workerId': 'blocked'}) is None
            queue.complete(job['id'], {'claimToken': claimed['claimToken'], 'status': 'succeeded', 'result': {'apiFixtureOnly': True}})
            updated = execute(api, control, adapter, 'cluster', first, health_seconds=15)
            assert updated['status'] == 'succeeded' and queue.get_node('cluster')['state'] == 'active'
            claim = queue.claim({'workerId': 'after'})
            assert claim['id'] == queued['id']
            queue.complete(claim['id'], {'claimToken': claim['claimToken'], 'status': 'succeeded', 'result': {'apiFixtureOnly': True}})
            second = root / 'second'
            second.mkdir()
            plan(images['bad'])
            adapter = ComposeNode(args, second)
            failed = execute(api, control, adapter, 'cluster', second, health_seconds=2)
            assert failed['status'] == 'failed' and failed['rollbackHealthy']
            assert queue.get_node('cluster')['state'] == 'failed'
            assert adapter.inspect()['Image'] == images['new']
            assert command([*compose, 'ps', '-q', 'sibling']) == sibling
            assert command(['docker', 'exec', adapter.inspect()['Id'], 'cat', '/owned/marker']) == 'keep-me'
            evidence = {'scope': 'disposable NodeODX API fixture; no real engine processing',
                'images': images, 'drainBlockedClaim': True, 'busyJobDelayedUpdate': True,
                'healthyUpdateResumedAdmission': True, 'failedHealthRestoredPinnedImage': True,
                'failedUpdateKeptNodeBlocked': True, 'unrelatedContainerUnchanged': True,
                'persistentVolumeMarkerPreserved': True, 'success': updated, 'failure': failed}
            evidence_path.write_text(json.dumps(evidence, indent=2) + '\n')
        finally:
            if server:
                server.shutdown();server.server_close()
            if handler:
                handler.prototype_queue.close()
            if compose_file.exists():
                command([*compose, 'down', '--volumes', '--remove-orphans'])
            # Removing an imported image's last tag also deletes that preexisting
            # image. Keep the small base-cache tag when it originally had none.
            for tag in tags + ([base_tag] if original_base_tags else []):
                command(['docker', 'image', 'rm', tag])
        evidence['preexistingBaseImageRetained'] = command(['docker', 'image', 'inspect', '--format', '{{.Id}}', BASE]) == BASE
        evidence['retainedBaseTag'] = None if original_base_tags else base_tag
        evidence['testResourcesRemoved'] = True
        evidence_path.write_text(json.dumps(evidence, indent=2) + '\n')
        print(json.dumps({'status': 'PASS', 'drain': True, 'update': True, 'rollback': True, 'cleanup': True}))


if __name__ == '__main__':
    main()
