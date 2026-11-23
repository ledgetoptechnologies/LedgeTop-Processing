"""Coordinator and TreeDar runner adapters, kind refusal and first adoption.

Docker is a scripted fake; the coordinator is the real in-process HTTP
service, so plan, begin, finish and rollback go through the real `execute`.
"""
from contextlib import contextmanager, ExitStack
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import threading
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import uuid

from ops import auto_deploy as agent
from ops.auto_deploy import DeployError
from processing import release_packages, service_adapters
from processing.app import make_handler
from processing.config import Config
from processing.container_update import (ComposeNode, PreflightError, UpdateError, Uncertain, execute,
                                         save, wait_healthy)
from processing.maintenance import Conflict
from processing.odx_smoke_worker import Client, HTTPRefused, HTTPUncertain, SmokeError
from processing import prototype
from tests.test_service import TOKEN, start_server

OLD = 'sha256:' + '1' * 64
NEW = 'sha256:' + '2' * 64
OPERATOR = 'maintenance-operator-private-test-credential'
COMMIT = 'f' * 40


def sql(path, *statements):
    """Run statements on a coordinator database and close it (Windows locks)."""
    db = sqlite3.connect(path)
    try:
        with db:
            for statement, *args in statements:
                db.execute(statement, *args) if not (args and isinstance(args[0], list)) else db.executemany(statement, args[0])
        return db.execute('SELECT 1').fetchone()
    finally:
        db.close()


def scalar(path, statement):
    db = sqlite3.connect(path)
    try:
        return db.execute(statement).fetchone()
    finally:
        db.close()


def labels(package, *, source=release_packages.SOURCE_URL, commit=COMMIT, run=200):
    return {release_packages.SOURCE_LABEL: source, release_packages.PACKAGE_LABEL: package,
            release_packages.REVISION_LABEL: commit, release_packages.RUN_LABEL: str(run),
            release_packages.REF_LABEL: 'refs/heads/main', release_packages.EVENT_LABEL: 'push'}


class FakeDocker:
    """Answers exactly the Docker commands the adapters issue."""

    def __init__(self, project, service, port, *, current=OLD):
        self.project, self.service, self.port = project, service, port
        self.current = current
        self.images = {OLD: {}, NEW: labels('coordinator')}
        self.running = True
        self.health = {OLD: 'healthy', NEW: 'healthy'}
        self.ps = {'service': [], 'ancestor': []}
        self.versions = {OLD: '0.9.0\n', NEW: '1.0.0\n'}
        self.uncertain = set()
        self.commands = []
        self.on_up = None  # called after `compose up`, e.g. to restart the coordinator

    def __call__(self, command, timeout=30):
        self.commands.append(command)
        if command[:2] == ['docker', 'info']:
            return 'engine\n'
        if command[:2] == ['docker', 'compose']:
            if command[-2:] == ['--quiet', self.service] and 'ps' in command:
                return 'cid\n'
            if 'config' in command:
                override = json.loads(Path(command[command.index('config') - 1]).read_text())
                return json.dumps({'services': {self.service: override['services'][self.service]}})
            if 'up' in command:
                resolved = json.loads(Path(command[command.index('-f') + 1]).read_text())
                self.current = resolved['services'][self.service]['image']
                if self.on_up is not None:
                    self.on_up(self.current)
                return ''
        if command[:2] == ['docker', 'inspect'] and command[2] == 'cid':
            if self.current in self.uncertain:
                raise Uncertain('Docker response uncertain; inspect service and receipt')
            return json.dumps([{
                'Id': 'cid', 'Image': self.current,
                'State': {'Running': self.running, **({} if self.health.get(self.current, 'healthy') is None
                          else {'Health': {'Status': self.health.get(self.current, 'healthy')}})},
                'Config': {'Labels': {'com.docker.compose.project': self.project,
                                      'com.docker.compose.service': self.service}},
                'NetworkSettings': {'Ports': {'8080/tcp': [{'HostIp': '127.0.0.1', 'HostPort': str(self.port)}]}}}])
        if command[:3] == ['docker', 'image', 'inspect']:
            image = command[-1]
            if image not in self.images:
                raise UpdateError('Docker operation failed; private operator inspection required')
            return image + '\n' if command[4] == '{{.Id}}' else json.dumps(self.images[image])
        if command[:2] == ['docker', 'ps']:
            filters = [command[i + 1] for i, value in enumerate(command) if value == '--filter']
            key = 'ancestor' if filters[0].startswith('ancestor=') else 'service'
            return '\n'.join(self.ps[key])
        if command[:2] == ['docker', 'run']:
            image = command[command.index('--version') - 1]
            if image in self.uncertain:
                raise Uncertain('Docker response uncertain; inspect service and receipt')
            return self.versions[image]
        raise AssertionError('unexpected Docker command: ' + ' '.join(command))


@contextmanager
def local_docker():
    env = {key: value for key, value in os.environ.items() if key != 'DOCKER_CONTEXT'}
    env['DOCKER_HOST'] = 'unix:///var/run/docker.sock'
    with patch.dict(os.environ, env, clear=True):
        yield


class Coordinated(unittest.TestCase):
    """A real coordinator with nodes cluster and main, plus a private root."""
    kind = 'coordinator'
    service = 'coordinator'

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.work = self.root / 'receipt'
        self.work.mkdir()
        self.config = Config(TOKEN, '', '', 800, 20000,
            prototype_state_dir=str(self.root / 'queue'), prototype_operator_token=OPERATOR,
            prototype_node_ids=('cluster', 'main'))
        handler = make_handler(self.config)
        self.queue = handler.prototype_queue
        self.server = start_server(handler)
        self.base = 'http://127.0.0.1:' + str(self.server.server_port)
        self.api, self.operator = Client(self.base, TOKEN), Client(self.base, OPERATOR)
        self.authority = self.api.json('GET', '/v1/prototype/authority')['authorityId']
        self.settings = {'nodeId': 'cluster', 'project': 'owned', 'service': self.service,
                         'kind': self.kind, 'dockerEngineId': 'engine'}
        save(self.root / 'compose.json', {'services': {self.service: {'image': OLD}}})
        self.docker = FakeDocker('owned', self.service, self.server.server_port)
        stack = ExitStack()
        stack.enter_context(local_docker())
        self.addCleanup(stack.close)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.queue.close()
        self.temp.cleanup()

    def adapter(self, baseline=OLD, authority='same'):
        made = service_adapters.adapter(self.kind, self.root, self.settings, self.work, api=self.api,
                                        authority=self.authority if authority == 'same' else authority,
                                        baseline=baseline)
        made.docker = self.docker
        return made

    def restart(self, image=None):
        """Stop and start the coordinator process on the same database and port,
        as `compose up` does when it recreates the coordinator container."""
        port = self.server.server_port
        self.server.shutdown()
        self.server.server_close()
        self.queue.close()
        handler = make_handler(self.config)
        self.queue = handler.prototype_queue
        self.server = ThreadingHTTPServer(('127.0.0.1', port), handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.restarts = getattr(self, 'restarts', 0) + 1

    def reserve(self, adapter=None):
        """Reserve a self-update on cluster and bind `adapter` to it."""
        node = self.plan()
        begun = self.operator.json('POST', '/v1/prototype/nodes/cluster/begin',
                                   {'planId': node['planId'], 'expectedRevision': node['revision'],
                                    'selfUpdate': True})
        if adapter is not None:
            adapter.bind_reservation(node_id='cluster', plan_id=node['planId'], target=NEW,
                                     revision=begun['node']['revision'])
        return begun

    def plan(self, future=False):
        stamp = datetime.now(timezone.utc) + timedelta(seconds=60 if future else -1)
        return self.operator.json('POST', '/v1/prototype/nodes/cluster/schedule', {
            'expectedRevision': self.queue.get_node('cluster')['revision'],
            'drainAt': stamp.isoformat(), 'notBefore': stamp.isoformat(),
            'afterJobId': None, 'targetImage': NEW})['node']

    def update(self, adapter, wait_seconds=0):
        return execute(self.api, self.operator, adapter, 'cluster', self.work,
                       wait_seconds=wait_seconds, health_seconds=0)

    def submit(self, name):
        return self.queue.submit({'requestId': name, 'environment': 'staging',
                                  'profile': 'treedar-synthetic-v1'})[0]


class SelfUpdateQueueTests(unittest.TestCase):
    """The coordinator's own reservation, fence and restart behaviour."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.queue = prototype.Queue(self.directory, node_ids=('cluster', 'main'))

    def tearDown(self):
        self.queue.close()
        self.temp.cleanup()

    def reopen(self):
        self.queue.close()
        self.queue = prototype.Queue(self.directory, node_ids=('cluster', 'main'))

    def begin(self, **extra):
        stamp = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        node = self.queue.schedule_node('cluster', {
            'expectedRevision': self.queue.get_node('cluster')['revision'], 'drainAt': stamp,
            'notBefore': stamp, 'afterJobId': None, 'targetImage': NEW})
        return self.queue.begin_node_update('cluster', {'planId': node['planId'],
                                                        'expectedRevision': node['revision'], **extra})

    def submit(self, name):
        return self.queue.submit({'requestId': name, 'environment': 'staging',
                                  'profile': 'treedar-synthetic-v1'})[0]

    def test_self_update_reservation_survives_restart_and_finishes(self):
        begun = self.begin(selfUpdate=True)
        self.reopen()
        node = self.queue.get_node('cluster')
        self.assertEqual(node['state'], 'updating')
        self.assertTrue(self.queue.unsettled_summary()['selfUpdateFence'])
        done = self.queue.finish_node_update('cluster', {'planId': node['planId'], 'updateToken': begun['updateToken'],
                                                         'status': 'succeeded', 'result': {'healthy': True}})
        self.assertEqual(done['state'], 'active')
        self.assertFalse(self.queue.unsettled_summary()['selfUpdateFence'])
        self.reopen()
        self.assertEqual(self.queue.get_node('cluster')['state'], 'active')

    def test_engine_update_is_still_held_on_restart(self):
        begun = self.begin()
        self.reopen()
        node = self.queue.get_node('cluster')
        self.assertEqual(node['state'], 'held')
        with self.assertRaises(Conflict):
            self.queue.finish_node_update('cluster', {'planId': node['planId'], 'updateToken': begun['updateToken'],
                                                      'status': 'succeeded', 'result': {'healthy': True}})

    def test_self_update_needs_no_unsettled_job_on_any_node(self):
        job = self.submit('elsewhere')  # queued, on no node yet
        stamp = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        node = self.queue.schedule_node('cluster', {
            'expectedRevision': self.queue.get_node('cluster')['revision'], 'drainAt': stamp,
            'notBefore': stamp, 'afterJobId': None, 'targetImage': NEW})
        request = {'planId': node['planId'], 'expectedRevision': node['revision'], 'selfUpdate': True}
        with self.assertRaisesRegex(Conflict, 'unsettled'):
            self.queue.begin_node_update('cluster', request)
        sql(self.queue.path, ("UPDATE jobs SET status='cancelled' WHERE id=?", (job['id'],)))
        self.assertEqual(self.queue.begin_node_update('cluster', request)['node']['state'], 'updating')

    def test_second_self_update_reservation_is_refused(self):
        self.begin(selfUpdate=True)
        stamp = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        other = self.queue.schedule_node('main', {
            'expectedRevision': self.queue.get_node('main')['revision'], 'drainAt': stamp,
            'notBefore': stamp, 'afterJobId': None, 'targetImage': NEW})
        with self.assertRaisesRegex(Conflict, 'another coordinator self-update'):
            self.queue.begin_node_update('main', {'planId': other['planId'], 'expectedRevision': other['revision'],
                                                  'selfUpdate': True})
        self.assertEqual(self.queue.get_node('main')['state'], 'draining')
        self.assertEqual(len(self.queue.unsettled_summary()['selfUpdates']), 1)

    def test_failed_self_update_finish_needs_proven_restoration(self):
        begun = self.begin(selfUpdate=True)
        node = self.queue.get_node('cluster')
        claim = {'planId': node['planId'], 'updateToken': begun['updateToken'], 'status': 'failed'}
        for result in ({'healthy': False}, {'healthy': False, 'rollbackHealthy': False},
                       {'healthy': False, 'rollbackHealthy': 'true'}):
            with self.assertRaisesRegex(Conflict, 'rollback health unproven'):
                self.queue.finish_node_update('cluster', {**claim, 'result': result})
        self.assertTrue(self.queue.unsettled_summary()['selfUpdateFence'])
        done = self.queue.finish_node_update('cluster', {**claim, 'result': {
            'healthy': False, 'rollbackHealthy': True, 'restoredImage': OLD}})
        self.assertEqual(done['state'], 'failed')
        self.assertFalse(self.queue.unsettled_summary()['selfUpdateFence'])

    def test_unchanged_self_update_may_finish_failed(self):
        begun = self.begin(selfUpdate=True)
        node = self.queue.get_node('cluster')
        done = self.queue.finish_node_update('cluster', {
            'planId': node['planId'], 'updateToken': begun['updateToken'], 'status': 'failed',
            'result': {'healthy': False, 'rollbackPerformed': False, 'reason': 'validation_changed_before_mutation'}})
        self.assertEqual(done['state'], 'failed')

    def test_engine_failed_finish_is_unchanged(self):
        begun = self.begin()
        node = self.queue.get_node('cluster')
        done = self.queue.finish_node_update('cluster', {
            'planId': node['planId'], 'updateToken': begun['updateToken'], 'status': 'failed',
            'result': {'healthy': False, 'rollbackHealthy': False}})
        self.assertEqual(done['state'], 'failed')

    def test_self_update_flag_must_be_true(self):
        stamp = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        node = self.queue.schedule_node('cluster', {
            'expectedRevision': self.queue.get_node('cluster')['revision'], 'drainAt': stamp,
            'notBefore': stamp, 'afterJobId': None, 'targetImage': NEW})
        for value in (False, 1, 'true'):
            with self.assertRaises(ValueError):
                self.queue.begin_node_update('cluster', {'planId': node['planId'],
                                                         'expectedRevision': node['revision'], 'selfUpdate': value})
        self.assertEqual(self.queue.get_node('cluster')['state'], 'draining')

    def test_fence_blocks_claims_on_every_node_until_finish_or_hold(self):
        begun = self.begin(selfUpdate=True)
        self.submit('queued-during-update')
        for node in ('main', 'cluster'):
            self.assertIsNone(self.queue.claim({'workerId': 'w', 'nodeId': node}))
        node = self.queue.get_node('cluster')
        self.queue.hold_node('cluster', {'planId': node['planId'], 'expectedRevision': node['revision'],
                                         'note': 'inspect'})
        self.assertFalse(self.queue.unsettled_summary()['selfUpdateFence'])
        self.assertEqual(self.queue.claim({'workerId': 'w', 'nodeId': 'main'})['requestId'], 'queued-during-update')
        self.reopen()
        self.assertEqual(self.queue.get_node('cluster')['state'], 'held')
        self.assertIsNotNone(begun['updateToken'])

    def test_summary_counts_only_unsettled_states(self):
        for name in ('a', 'b', 'c'):
            self.submit(name)
        sql(self.queue.path, ("UPDATE jobs SET status='cancelled' WHERE request_id='a'",),
            ("UPDATE jobs SET status='held' WHERE request_id='b'",))
        result = self.queue.unsettled_summary()
        self.assertEqual(result['jobs'], {'queued': 1, 'running': 0, 'held': 1})
        self.assertEqual(result['unsettledJobs'], 2)


class SchemaCompatibilityTests(unittest.TestCase):
    """Rollback safety: an older coordinator never misreads a newer database."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_floor_is_recorded_and_never_lowered(self):
        prototype.Queue(self.directory).close()
        path = self.directory / 'jobs.sqlite3'
        self.assertEqual(scalar(path, 'SELECT min_reader FROM coordinator_schema')[0], prototype.SCHEMA_MIN_READER)
        with patch.object(prototype, 'SCHEMA_READER', 2), patch.object(prototype, 'SCHEMA_MIN_READER', 2):
            prototype.Queue(self.directory).close()
        # An older reader now refuses instead of misreading.
        with self.assertRaises(prototype.SchemaTooNew):
            prototype.Queue(self.directory)
        self.assertEqual(scalar(path, 'SELECT min_reader FROM coordinator_schema')[0], 2)

    def test_current_migrations_are_additive(self):
        """A non-additive migration must raise SCHEMA_MIN_READER (and READER)."""
        source = Path(prototype.__file__).parent
        risky = []
        # Separate worker-local databases, not the coordinator's jobs.sqlite3.
        other_databases = {'scratch_policy.py'}
        for path in sorted(source.glob('*.py')):
            if path.name in other_databases:
                continue
            text = path.read_text(encoding='utf-8')
            for match in re.finditer(r'(?i)\b(DROP\s+(TABLE|COLUMN|INDEX)|RENAME\s+(TO|COLUMN)|ALTER\s+TABLE\s+\w+\s+(?!ADD\s+COLUMN))',
                                     text):
                risky.append(path.name + ': ' + match.group(0))
        if prototype.SCHEMA_MIN_READER == 1:
            self.assertEqual(risky, [])
        self.assertLessEqual(prototype.SCHEMA_MIN_READER, prototype.SCHEMA_READER)

    def test_self_update_column_is_additive_for_older_readers(self):
        prototype.Queue(self.directory).close()
        db = sqlite3.connect(self.directory / 'jobs.sqlite3')
        try:
            column = [row for row in db.execute('PRAGMA table_info(nodes)') if row[1] == 'self_update'][0]
        finally:
            db.close()
        self.assertEqual((column[3], column[4]), (1, '0'))  # NOT NULL DEFAULT 0


class DirectCoordinator:
    """The prototype API surface `execute` uses, answered by the queue itself."""
    origin = 'http://127.0.0.1:18080'

    def __init__(self, queue):
        self.queue = queue

    def json(self, method, path, payload=None):
        parts = path.split('/')
        if method == 'GET' and path == '/v1/prototype/jobs':
            return {'jobs': self.queue.list()}
        if method == 'GET' and len(parts) == 5 and parts[3] == 'nodes':
            return {'node': self.queue.get_node(parts[4])}
        if method == 'POST' and parts[-1] == 'begin':
            return self.queue.begin_node_update(parts[4], payload)
        if method == 'POST' and parts[-1] == 'finish':
            return {'node': self.queue.finish_node_update(parts[4], payload)}
        raise AssertionError('unexpected coordinator call ' + method + ' ' + path)


class StoppedSelfUpdateAdapter:
    """A coordinator whose candidate and rollback both stay unhealthy."""
    begin_fields = {'selfUpdate': True}
    finish_requires_rollback_health = True

    def __init__(self, during_swap):
        self.current, self.during_swap, self.calls = OLD, during_swap, []

    def inspect(self):
        return {'Image': self.current}

    def idle(self):
        return True

    def image(self, image):
        pass

    def prepare(self, image):
        pass

    def replace(self, image):
        self.calls.append(image)
        self.current = image
        if image == NEW:
            self.during_swap()

    def healthy(self, image):
        return False


class ReferenceClaimFenceTests(unittest.TestCase):
    """Reference claims honour the self-update fence, including after an
    unproven rollback that leaves the reservation for an operator. The hold at
    the end changes only the updating node, so the successful claim after it
    shows the fence was what refused."""

    def setUp(self):
        from processing.reference_control import ReferenceControlAPI
        from processing.reference_http import create_reference_http_server
        from processing.reference_store import ReferenceStore, PROFILE
        from tests.test_reference_http import STAGING, WORKER
        self.directory = tempfile.TemporaryDirectory()
        self.queue = prototype.Queue(self.directory.name, node_ids=('cluster', 'main'))
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        self.fixture = json.loads((Path(__file__).resolve().parents[1] /
                                   'contracts/reference-v1.contract.json').read_text())
        self.assertEqual(self.store._required_node(self.fixture['submission']), 'cluster')
        api = ReferenceControlAPI(self.store, viewer_bearers={STAGING: 'staging'},
            worker_bearers={WORKER: dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')},
            capability_fingerprint=self.fixture['capabilities']['capabilityFingerprint'],
            options_fingerprint=self.fixture['capabilities']['engines']['odm']['optionsFingerprint'])
        self.server = create_reference_http_server(api, request_timeout=0.2)
        threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': 0.02}, daemon=True).start()
        self.staging = STAGING
        self.work = Path(self.directory.name) / 'receipt'
        self.work.mkdir()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.queue.close()
        self.directory.cleanup()

    def submit_reference(self):
        import http.client
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        connection.request('POST', '/ltds/v1/jobs', body=json.dumps(self.fixture['submission']).encode(),
                           headers={'Authorization': 'Bearer ' + self.staging})
        response = connection.getresponse()
        body = json.loads(response.read())
        connection.close()
        self.assertIn(response.status, (200, 201), body)
        return body['jobId']

    def reference_claim(self, job_id, request_id):
        return self.store.claim(job_id, node_id='cluster', worker_id='worker',
                                worker_instance_id='instance', claim_request_id=request_id)

    def test_reference_and_prototype_claims_refuse_until_an_operator_releases(self):
        jobs = []

        def during_swap():
            jobs.append(self.submit_reference())
            self.assertIsNone(self.reference_claim(jobs[0], 'claim-during-swap'))
        stamp = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        self.queue.schedule_node('main', {'expectedRevision': self.queue.get_node('main')['revision'],
                                          'drainAt': stamp, 'notBefore': stamp, 'afterJobId': None,
                                          'targetImage': NEW})
        coordinator = DirectCoordinator(self.queue)
        adapter = StoppedSelfUpdateAdapter(during_swap)
        result = execute(coordinator, coordinator, adapter, 'main', self.work, health_seconds=0)
        self.assertEqual((result['status'], result['reason']), ('stopped', 'rollback_unhealthy'))
        self.assertEqual(adapter.calls, [NEW, OLD])
        node = self.queue.get_node('main')
        self.assertEqual(node['state'], 'updating')
        # Cluster itself is active: only the coordinator-wide fence refuses.
        self.assertEqual(self.queue.get_node('cluster')['state'], 'active')
        # (Queue.claim after the same stop is covered by
        # test_unhealthy_rollback_with_a_reachable_api_keeps_the_fence.)
        self.assertIsNone(self.reference_claim(jobs[0], 'claim-after-stop'))
        # Explicit operator recovery lifts the fence; the queued work proceeds.
        self.queue.hold_node('main', {'planId': node['planId'], 'expectedRevision': node['revision'],
                                      'note': 'coordinator inspected and restored by hand'})
        claimed = self.reference_claim(jobs[0], 'claim-after-hold')
        self.assertIsNotNone(claimed)


class KindTests(unittest.TestCase):
    def test_missing_kind_is_the_unchanged_odx_engine(self):
        self.assertEqual(service_adapters.service_kind({}), 'odx')
        self.assertIsNone(service_adapters.package_for('odx'))

    def test_declared_kinds_map_to_their_own_package(self):
        self.assertEqual(service_adapters.package_for('coordinator'), 'coordinator')
        self.assertEqual(service_adapters.package_for('treedar'), 'treedar')
        for key in service_adapters.PACKAGES.values():
            release_packages.package(key)

    def test_unknown_kind_refuses(self):
        for kind in ('nodeodx', 'Coordinator', '', None, 3):
            with self.assertRaisesRegex(UpdateError, 'unknown service kind'):
                service_adapters.service_kind({'kind': kind})

    def test_no_adapter_for_odx_or_unknown(self):
        for kind in ('odx', 'other'):
            with self.assertRaises(UpdateError):
                service_adapters.adapter(kind, Path('.'), {'project': 'p', 'service': 's',
                                                           'dockerEngineId': 'e'}, Path('.'))

    def test_new_kinds_require_exact_service_identity(self):
        good = {'project': 'owned', 'service': 'coordinator', 'dockerEngineId': 'engine'}
        for change in ({'dockerEngineId': None}, {'dockerEngineId': 'bad id'}, {'project': '../x'},
                       {'service': 'A B'}, {'containerPort': 0}, {'containerPort': '8080'}):
            with self.assertRaises(UpdateError):
                service_adapters.adapter('coordinator', Path('.'), {**good, **change}, Path('.'))

    def test_existing_odx_adapter_is_untouched(self):
        self.assertFalse(issubclass(ComposeNode, service_adapters._PackageImage))
        self.assertNotIn('odx', service_adapters.ADAPTERS)


def summary(queued=0, running=0, held=0, fence=False):
    reservations = [{'nodeId': 'cluster', 'planId': str(uuid.UUID(int=1)), 'targetImage': NEW,
                     'revision': 3}] if fence else []
    return {'schemaVersion': 1, 'jobs': {'queued': queued, 'running': running, 'held': held},
            'unsettledJobs': queued + running + held, 'selfUpdateFence': fence, 'selfUpdates': reservations}


class CoordinatorAdapterTests(Coordinated):
    def test_idle_only_when_no_job_is_queued_running_or_held_on_any_node(self):
        adapter = self.adapter()
        self.assertTrue(adapter.idle())
        self.submit('queued')
        self.assertFalse(adapter.idle())

    def test_idle_from_the_bounded_summary(self):
        adapter = self.adapter()
        adapter.api = MagicMock()
        for value, idle in ((summary(), True), (summary(running=1), False), (summary(held=1), False),
                            (summary(queued=2), False),
                            # Queued work cannot be claimed once the fence is up.
                            (summary(queued=2, fence=True), True), (summary(running=1, fence=True), False)):
            adapter.api.json.return_value = value
            self.assertEqual(adapter.idle(), idle, value)

    def test_unreadable_summary_or_old_coordinator_is_refused(self):
        adapter = self.adapter()
        adapter.api = MagicMock()
        for value in ({}, {**summary(), 'schemaVersion': 2}, {**summary(), 'jobs': {'queued': '0'}},
                      {**summary(), 'selfUpdateFence': None}, {**summary(), 'selfUpdates': None},
                      {**summary(), 'selfUpdateFence': True}):
            adapter.api.json.return_value = value
            with self.assertRaises(UpdateError):
                adapter.idle()
        adapter.api.json.side_effect = HTTPRefused('HTTP request rejected', 404, 'not_found')
        with self.assertRaisesRegex(UpdateError, 'self-update API'):
            adapter.idle()

    def test_cancelled_and_other_settled_jobs_do_not_block(self):
        for name in ('done', 'cancelled', 'failed'):
            self.submit(name)
        sql(self.queue.path, ("UPDATE jobs SET status=CASE request_id WHEN 'done' THEN 'succeeded' "
                              "WHEN 'cancelled' THEN 'cancelled' ELSE 'failed' END",))
        self.assertTrue(self.adapter().idle())

    def test_long_history_stays_readable(self):
        # The full /v1/prototype/jobs history outgrows the client's 64 KiB cap.
        sql(self.queue.path, ("INSERT INTO jobs(id,request_id,environment,profile,status,result) "
                              "VALUES(?,?,'staging','treedar-synthetic-v1','succeeded',?)",
                              [(str(uuid.uuid4()), 'history-' + str(n), json.dumps({'note': 'x' * 64}))
                               for n in range(999)]))
        with self.assertRaises(SmokeError):
            self.api.json('GET', '/v1/prototype/jobs')
        self.assertTrue(self.adapter().idle())
        self.submit('late')
        self.assertFalse(self.adapter().idle())
        self.assertEqual(self.api.json('GET', '/v1/prototype/unsettled')['jobs']['queued'], 1)

    def test_stopped_coordinator_is_not_an_idle_proof(self):
        self.docker.running = False
        with self.assertRaises(UpdateError):
            self.adapter().idle()

    def test_healthy_requires_image_health_healthz_authority_fence_and_labels(self):
        adapter = self.adapter()
        self.docker.current = NEW
        self.assertFalse(adapter.healthy(NEW))  # no reservation fence yet
        self.reserve(adapter)
        self.assertTrue(adapter.healthy(NEW))
        self.assertFalse(adapter.healthy(OLD))  # a different image is running
        self.docker.health[NEW] = 'starting'
        self.assertFalse(adapter.healthy(NEW))
        self.docker.health[NEW] = None  # no HEALTHCHECK is not proof
        self.assertFalse(adapter.healthy(NEW))
        self.docker.health[NEW] = 'healthy'
        self.docker.images[NEW] = labels('treedar')
        self.assertFalse(adapter.healthy(NEW))
        self.docker.images[NEW] = labels('coordinator', source='https://example.invalid/other')
        self.assertFalse(adapter.healthy(NEW))
        self.docker.images[NEW] = labels('coordinator')
        for other in (self.adapter(authority=str(uuid.UUID(int=9))), self.adapter(authority=None)):
            other.reservation = adapter.reservation
            self.assertFalse(other.healthy(NEW))

    def test_health_is_bound_to_the_exact_reservation(self):
        adapter = self.adapter()
        self.docker.current = NEW
        self.reserve(adapter)
        bound = dict(adapter.reservation)
        self.assertTrue(adapter.healthy(NEW))
        unbound = self.adapter()  # never reserved: some fence existing is not enough
        self.assertFalse(unbound.healthy(NEW))
        for change in ({'planId': str(uuid.UUID(int=5))}, {'revision': bound['revision'] + 1},
                       {'nodeId': 'main'}, {'targetImage': OLD}):
            adapter.reservation = {**bound, **change}
            self.assertFalse(adapter.healthy(NEW), change)
        # Once an operator holds it, it is no longer this reservation.
        adapter.reservation = bound
        node = self.queue.get_node('cluster')
        self.operator.json('POST', '/v1/prototype/nodes/cluster/hold',
                           {'planId': node['planId'], 'expectedRevision': node['revision'], 'note': 'inspect'})
        self.assertFalse(adapter.healthy(NEW))

    def test_rollback_target_needs_no_labels(self):
        # The old pin may be an adopted, unlabelled image.
        self.assertEqual(self.docker.images[OLD], {})
        adapter = self.adapter()
        self.reserve(adapter)
        self.assertTrue(adapter.healthy(OLD))

    def test_adoptable_needs_the_self_update_api_and_no_open_update(self):
        adapter = self.adapter()
        self.assertTrue(adapter.adoptable(OLD))
        self.reserve()
        self.assertFalse(service_adapters.safe(adapter.adoptable, OLD))
        adapter.api = MagicMock()
        adapter.api.json.side_effect = lambda method, path: (
            {'status': 'ok'} if path == '/healthz' else {'authorityId': self.authority}
            if path.endswith('/authority') else (_ for _ in ()).throw(HTTPRefused('x', 404, 'not_found')))
        self.assertFalse(service_adapters.safe(adapter.adoptable, OLD))

    def test_failing_healthz_or_timeout_is_unhealthy_not_a_crash(self):
        adapter = self.adapter()
        adapter.api = MagicMock()
        adapter.api.json.side_effect = HTTPRefused('HTTP request rejected', 503)
        self.assertFalse(wait_healthy(adapter, OLD, 0))
        adapter.api.json.side_effect = HTTPUncertain('HTTP response uncertain')
        self.assertFalse(wait_healthy(adapter, OLD, 0))

    def test_docker_timeout_during_health_is_unhealthy(self):
        self.docker.uncertain.add(OLD)
        self.assertFalse(wait_healthy(self.adapter(), OLD, 0))

    def test_healthy_update_through_real_coordinator(self):
        self.plan()
        result = self.update(self.adapter())
        self.assertEqual((result['status'], result['image']), ('succeeded', NEW))
        self.assertEqual(self.docker.current, NEW)
        self.assertEqual(self.queue.get_node('cluster')['state'], 'active')

    def test_update_finishes_after_the_coordinator_restarts_itself(self):
        self.plan()
        self.docker.on_up = self.restart
        result = self.update(self.adapter())
        self.assertEqual((result['status'], result['image']), ('succeeded', NEW))
        self.assertEqual(self.restarts, 1)
        node = self.queue.get_node('cluster')
        self.assertEqual(node['state'], 'active')
        self.assertFalse(self.api.json('GET', '/v1/prototype/unsettled')['selfUpdateFence'])
        self.assertEqual(json.loads((self.work / 'update.json').read_text())['stage'], 'completed')

    def test_rollback_finishes_after_both_restarts(self):
        self.plan()
        self.docker.on_up = self.restart
        self.docker.health[NEW] = 'unhealthy'
        result = self.update(self.adapter())
        self.assertEqual((result['status'], result['restoredImage']), ('failed', OLD))
        self.assertTrue(result['rollbackHealthy'])
        self.assertEqual(self.restarts, 2)
        self.assertEqual(self.docker.current, OLD)
        self.assertEqual(self.queue.get_node('cluster')['state'], 'failed')
        self.assertFalse(self.api.json('GET', '/v1/prototype/unsettled')['selfUpdateFence'])

    def test_without_self_update_a_restart_drops_the_reservation(self):
        # The failure the selfUpdate reservation exists to prevent.
        self.plan()
        self.docker.on_up = self.restart
        adapter = self.adapter()
        adapter.begin_fields = {}
        result = self.update(adapter)
        # Neither image can prove the reservation after the restart.
        self.assertEqual((result['status'], result['reason']), ('stopped', 'rollback_unhealthy'))
        self.assertEqual(self.queue.get_node('cluster')['state'], 'held')

    def test_unhealthy_rollback_with_a_reachable_api_keeps_the_fence(self):
        self.plan()
        self.docker.on_up = self.restart
        self.docker.health = {NEW: 'unhealthy', OLD: 'unhealthy'}
        result = self.update(self.adapter())
        self.assertEqual((result['status'], result['reason'], result['rollbackHealthy']),
                         ('stopped', 'rollback_unhealthy', False))
        self.assertEqual(self.restarts, 2)
        self.assertEqual(self.docker.current, OLD)
        # No finish: the reservation and global fence remain for an operator.
        node = self.queue.get_node('cluster')
        self.assertEqual(node['state'], 'updating')
        unsettled = self.api.json('GET', '/v1/prototype/unsettled')
        self.assertTrue(unsettled['selfUpdateFence'])
        self.assertEqual([r['planId'] for r in unsettled['selfUpdates']], [node['planId']])
        receipt = json.loads((self.work / 'update.json').read_text())
        self.assertEqual((receipt['stage'], receipt['status']), ('rollback_unhealthy', 'stopped'))
        self.submit('waiting-for-recovery')
        self.assertIsNone(self.queue.claim({'workerId': 'w', 'nodeId': 'main'}))
        # The coordinator itself also refuses an unproven failed finish.
        with self.assertRaises(HTTPRefused):
            self.operator.json('POST', '/v1/prototype/nodes/cluster/finish', {
                'planId': node['planId'], 'updateToken': receipt['updateToken'], 'status': 'failed',
                'result': {'healthy': False, 'rollbackHealthy': False, 'restoredImage': OLD}})
        self.assertTrue(self.api.json('GET', '/v1/prototype/unsettled')['selfUpdateFence'])
        # Never replayed: the receipt blocks another run.
        with self.assertRaises(UpdateError):
            self.update(self.adapter())

    def test_no_job_is_claimed_anywhere_between_reservation_and_finish(self):
        self.plan()
        claims = []

        def during_swap(image):
            self.submit('arrived-during-swap')
            claims.append(self.queue.claim({'workerId': 'main-worker', 'nodeId': 'main'}))
            self.restart()
            claims.append(self.queue.claim({'workerId': 'main-worker', 'nodeId': 'main'}))
        self.docker.on_up = during_swap
        self.assertEqual(self.update(self.adapter())['status'], 'succeeded')
        self.assertEqual(claims, [None, None])
        # Main may claim while cluster is updating; only the fence stopped it.
        job = self.queue.claim({'workerId': 'cluster-worker', 'nodeId': 'cluster'})
        self.assertEqual(job['requestId'], 'arrived-during-swap')

    def test_unhealthy_candidate_rolls_back_to_the_old_pin(self):
        self.plan()
        self.docker.images[NEW] = {}  # not this package: never accepted as healthy
        result = self.update(self.adapter())
        self.assertEqual(result['status'], 'failed')
        self.assertTrue(result['rollbackHealthy'])
        self.assertEqual(self.docker.current, OLD)
        self.assertEqual(self.queue.get_node('cluster')['state'], 'failed')

    def test_unhealthy_docker_health_rolls_back(self):
        self.plan()
        self.docker.health[NEW] = 'unhealthy'
        result = self.update(self.adapter())
        self.assertEqual((result['status'], result['restoredImage']), ('failed', OLD))
        self.assertEqual(self.docker.current, OLD)

    def test_busy_queue_times_out_waiting_without_reserving(self):
        self.plan()
        self.submit('backlog')
        self.assertEqual(self.update(self.adapter())['status'], 'waiting')
        self.assertEqual(self.queue.get_node('cluster')['state'], 'draining')
        self.assertFalse((self.work / 'update.json').exists())
        self.assertFalse(any('up' in command for command in self.docker.commands))


class TreeDarAdapterTests(Coordinated):
    kind = 'treedar'
    service = 'treedar'

    def setUp(self):
        super().setUp()
        self.docker.images[NEW] = labels('treedar')

    def test_selected_image_is_the_compose_pin(self):
        self.assertEqual(self.adapter().inspect()['Image'], OLD)

    def test_idle_only_without_running_service_or_pinned_image_container(self):
        adapter = self.adapter()
        self.assertTrue(adapter.idle())
        self.docker.ps['service'] = ['c1']
        self.assertFalse(adapter.idle())
        self.docker.ps = {'service': [], 'ancestor': ['c2']}
        self.assertFalse(adapter.idle())
        filters = [c for c in self.docker.commands if c[:2] == ['docker', 'ps']]
        self.assertIn('ancestor=' + OLD, filters[-1])
        self.assertIn('label=com.docker.compose.service=treedar', filters[0])

    def test_healthy_runs_a_locked_down_version_self_check(self):
        adapter = self.adapter()
        self.assertTrue(adapter.healthy(NEW))
        run = [c for c in self.docker.commands if c[:2] == ['docker', 'run']][-1]
        for flag in (['--network', 'none'], ['--pull', 'never'], ['--cap-drop', 'ALL'],
                     ['--user', '10001:10001'], ['--entrypoint', 'treedar']):
            self.assertIn(flag[1], run[run.index(flag[0]) + 1:run.index(flag[0]) + 2])
        self.assertIn('--read-only', run)
        self.assertEqual(run[-2:], [NEW, '--version'])

    def test_bad_version_output_or_labels_is_unhealthy(self):
        adapter = self.adapter()
        self.docker.versions[NEW] = 'Traceback (most recent call last):\n'
        self.assertFalse(adapter.healthy(NEW))
        self.docker.versions[NEW] = '1.0.0\n'
        self.docker.images[NEW] = labels('coordinator')
        self.assertFalse(adapter.healthy(NEW))
        self.docker.images[NEW] = {}
        self.assertFalse(adapter.healthy(NEW))
        self.assertTrue(adapter.healthy(OLD))  # unlabelled baseline for rollback

    def test_self_check_timeout_is_unhealthy(self):
        adapter = self.adapter()
        self.docker.uncertain.add(NEW)
        self.assertFalse(wait_healthy(adapter, NEW, 0))

    def test_real_subprocess_timeout_maps_to_uncertain(self):
        adapter = self.adapter()
        del adapter.docker  # use ComposeNode.docker with a timed-out subprocess
        with patch('processing.container_update.subprocess.run',
                   side_effect=subprocess.TimeoutExpired('docker', 90)):
            with self.assertRaises(Uncertain):
                adapter.self_check(NEW)
            self.assertFalse(wait_healthy(adapter, NEW, 0))

    def test_replace_mutates_no_container(self):
        adapter = self.adapter()
        adapter.replace(NEW)
        self.assertEqual(adapter.selected, NEW)
        self.assertFalse(any('up' in command for command in self.docker.commands))

    def test_invalid_configuration_is_a_preflight_refusal(self):
        adapter = self.adapter()
        with self.assertRaises(PreflightError):
            adapter.replace('sha256:' + '9' * 64)

    def test_healthy_update_through_real_coordinator(self):
        self.plan()
        result = self.update(self.adapter())
        self.assertEqual((result['status'], result['image']), ('succeeded', NEW))
        self.assertEqual(self.queue.get_node('cluster')['state'], 'active')
        self.assertFalse(any('up' in command for command in self.docker.commands))

    def test_failed_self_check_rolls_back_by_keeping_the_old_pin(self):
        self.plan()
        self.docker.versions[NEW] = ''
        result = self.update(self.adapter())
        self.assertEqual(result['status'], 'failed')
        self.assertTrue(result['rollbackHealthy'])
        self.assertEqual(self.queue.get_node('cluster')['state'], 'failed')

    def test_running_job_container_times_out_waiting(self):
        self.plan()
        self.docker.ps['ancestor'] = ['job-container']
        self.assertEqual(self.update(self.adapter())['status'], 'waiting')
        self.assertFalse((self.work / 'update.json').exists())


@unittest.skipIf(os.name == 'nt', 'Installed node automation uses Linux host locks')
class ServiceRunnerTests(Coordinated):
    """`node_maintenance.run` for the new kinds: no ingress, pin after success."""

    def run_service(self, wait=0, **bound):
        from ops import node_maintenance
        save(self.root / 'maintenance.json', self.settings)
        factory = service_adapters.adapter

        def made(*args, **kwargs):
            adapter = factory(*args, **kwargs)
            adapter.docker = self.docker
            adapter.health_seconds = 0
            return adapter
        with patch('ops.node_maintenance.Ingress', side_effect=AssertionError('no ingress for this kind')), \
                patch('ops.node_maintenance.NodeGate', side_effect=AssertionError('no engine gate')), \
                patch('ops.node_maintenance.service_adapters.adapter', side_effect=made):
            return node_maintenance.run(self.root, self.api, self.operator, self.work, wait, 60, **bound)

    def pin(self):
        return json.loads((self.root / 'compose.json').read_text())['services'][self.service]['image']

    def test_success_persists_the_compose_pin(self):
        node = self.plan()
        result = self.run_service(expected_plan_id=node['planId'], expected_target=NEW)
        self.assertEqual(result['status'], 'succeeded')
        self.assertFalse(result['composePinUpdateRequired'])
        self.assertEqual(self.pin(), NEW)
        receipt = json.loads((self.work / 'managed-update.json').read_text())
        self.assertEqual((receipt['stage'], receipt['kind']), ('completed', 'coordinator'))

    def test_failure_keeps_the_old_pin(self):
        self.plan()
        self.docker.health[NEW] = 'unhealthy'
        self.assertEqual(self.run_service()['status'], 'failed')
        self.assertEqual(self.pin(), OLD)
        self.assertEqual(self.docker.current, OLD)

    def test_unproven_rollback_stops_without_finish_and_keeps_the_pin(self):
        self.plan()
        self.docker.health = {NEW: 'unhealthy', OLD: 'unhealthy'}
        result = self.run_service()
        self.assertEqual((result['status'], result['reason']), ('stopped', 'rollback_unhealthy'))
        self.assertEqual(self.pin(), OLD)
        receipt = json.loads((self.work / 'managed-update.json').read_text())
        self.assertEqual((receipt['stage'], receipt['result']['status']), ('update_returned', 'stopped'))
        self.assertEqual(self.queue.get_node('cluster')['state'], 'updating')

    def test_waiting_writes_no_receipt_so_the_next_pass_resumes(self):
        self.plan()
        self.submit('backlog')
        self.assertEqual(self.run_service()['status'], 'waiting')
        self.assertFalse((self.work / 'managed-update.json').exists())
        self.assertFalse((self.work / 'update.json').exists())

    def test_existing_receipt_is_never_replayed(self):
        self.plan()
        save(self.work / 'update.json', {'stage': 'replace_pending'})
        from ops.node_ingress import GateError
        with self.assertRaisesRegex(GateError, 'existing operation receipt'):
            self.run_service()

    def test_running_container_not_the_pin_stops_before_any_reservation(self):
        self.plan()
        self.docker.current = NEW
        from ops.node_ingress import GateError
        with self.assertRaisesRegex(GateError, 'not the Compose pin'):
            self.run_service()
        self.assertEqual(self.queue.get_node('cluster')['state'], 'draining')

    def test_another_plan_is_never_processed(self):
        self.plan()
        from ops.node_ingress import GateError
        with self.assertRaisesRegex(GateError, 'bound plan'):
            self.run_service(expected_plan_id=str(uuid.uuid4()), expected_target=NEW)
        self.assertEqual(self.docker.commands, [])

    def test_unknown_kind_refuses_before_any_call(self):
        self.settings['kind'] = 'mystery'
        api = MagicMock()
        with self.assertRaisesRegex(UpdateError, 'unknown service kind'):
            from ops import node_maintenance
            save(self.root / 'maintenance.json', self.settings)
            node_maintenance.run(self.root, api, MagicMock(), self.work, 0, 60)
        api.json.assert_not_called()

    def test_odx_without_kind_still_uses_ingress_and_compose_node(self):
        from ops import node_maintenance
        settings = {'nodeId': 'cluster', 'project': 'owned', 'service': 'node'}
        save(self.root / 'maintenance.json', settings)
        with patch('ops.node_maintenance.Ingress', side_effect=RuntimeError('odx ingress')) as ingress, \
                patch('ops.node_maintenance.service_adapters.adapter') as factory:
            with self.assertRaisesRegex(RuntimeError, 'odx ingress'):
                node_maintenance.run(self.root, self.api, self.operator, self.work, 0, 60)
        ingress.assert_called_once_with(self.root / 'ingress')
        factory.assert_not_called()


class StagedImageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_odx_staging_check_is_the_original_compose_node_check(self):
        from ops import node_maintenance
        ingress = MagicMock()
        ingress.config = {'engineId': 'engine'}
        with patch('ops.node_maintenance.Ingress', return_value=ingress), \
                patch('ops.node_maintenance.ComposeNode') as node, \
                patch('ops.node_maintenance.service_adapters.adapter') as factory:
            node_maintenance.staged_image(self.root, {'project': 'owned', 'service': 'node'}, NEW)
        args = node.call_args.args[0]
        self.assertEqual((args.docker_engine_id, args.node_origin, args.node_port),
                         ('engine', 'http://127.0.0.1:13004', 3000))
        node.return_value.image.assert_called_once_with(NEW)
        factory.assert_not_called()

    def test_new_kinds_check_through_their_adapter_without_ingress(self):
        from ops import node_maintenance
        settings = {'project': 'owned', 'service': 'treedar', 'kind': 'treedar', 'dockerEngineId': 'engine'}
        with patch('ops.node_maintenance.Ingress', side_effect=AssertionError('no ingress')), \
                patch('ops.node_maintenance.service_adapters.adapter') as factory:
            node_maintenance.staged_image(self.root, settings, NEW)
        self.assertEqual(factory.call_args.args[0], 'treedar')
        factory.return_value.image.assert_called_once_with(NEW)


REPO = release_packages.repository('coordinator')
DIGEST = 'sha256:' + 'c' * 64


def published(identifier, run, package='coordinator', digests=()):
    return {'Id': identifier, 'Os': 'linux', 'Architecture': 'amd64', 'RepoDigests': list(digests),
            'Config': {'User': '10001:10001', 'Labels': labels(package, commit=format(run, 'x').rjust(40, '0'),
                                                               run=run)}}


class AgentKindAndAdoptionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.settings = {'nodeId': 'cluster', 'project': 'owned', 'service': 'coordinator',
                         'kind': 'coordinator', 'dockerEngineId': 'engine'}
        save(self.root / 'maintenance.json', self.settings)
        save(self.root / 'compose.json', {'services': {'coordinator': {'image': OLD}}})
        self.node = {'state': 'active', 'revision': 4, 'planId': None, 'targetImage': None}
        self.api = MagicMock()
        self.api.json.side_effect = lambda method, path: (
            {'authorityId': str(uuid.UUID(int=7))} if path.endswith('/authority') else {'node': dict(self.node)})
        self.images = {OLD: {'Id': OLD, 'Config': {'Labels': {}}},
                       REPO + ':verified': published(NEW, 200, digests=[REPO + '@' + DIGEST]),
                       REPO + '@' + DIGEST: published(NEW, 200, digests=[REPO + '@' + DIGEST])}
        self.adapter = MagicMock()
        self.adapter.inspect.return_value = {'Image': OLD}
        self.adapter.adoptable.return_value = True
        self.schedule = MagicMock(side_effect=self.scheduled)

    def tearDown(self):
        self.temp.cleanup()

    def scheduled(self, request):
        plan = str(uuid.uuid4())
        self.node.update(state='draining', planId=plan, targetImage=request['targetImage'], revision=5)
        return {'requestId': request['requestId'], 'nodeId': 'cluster', 'planId': plan,
                'authorityId': request['authorityId'], 'expectedRevision': request['expectedRevision'],
                'targetImage': request['targetImage']}

    @contextmanager
    def patched(self):
        @contextmanager
        def acquired(directory):
            yield True
        with patch.object(agent, 'docker', return_value=''), \
                patch.object(agent, 'inspect', side_effect=lambda ref: json.loads(json.dumps(self.images[ref]))), \
                patch.object(agent, 'agent_lock', acquired):
            yield

    def deploy(self, settings=None):
        with self.patched():
            return agent.auto_deploy(self.root, settings or self.current(), self.api, schedule=self.schedule,
                                     lookup=MagicMock(return_value=None),
                                     run=MagicMock(return_value={'status': 'succeeded', 'image': NEW}),
                                     wait_seconds=0, drain_seconds=60)

    def adopt(self, **change):
        arguments = {'kind': 'coordinator', 'image': OLD, 'drain_delay': 0,
                     'make_adapter': MagicMock(return_value=self.adapter), **change}
        with self.patched():
            return agent.adopt(self.root, self.current(), self.api, **arguments)

    def current(self):
        return json.loads((self.root / 'maintenance.json').read_text())

    def test_unknown_kind_refuses(self):
        result = self.deploy({**self.settings, 'kind': 'mystery', 'autoDeploy': {'package': 'coordinator'}})
        self.assertEqual(result['status'], 'refused')
        self.assertIn('unknown service kind', result['reason'])
        self.schedule.assert_not_called()

    def test_package_must_match_a_declared_kind(self):
        for kind in (None, 'odx', 'treedar'):
            settings = {**self.settings, 'autoDeploy': {'package': 'coordinator'}}
            if kind is None:
                del settings['kind']
            else:
                settings['kind'] = kind
            result = self.deploy(settings)
            self.assertEqual(result['status'], 'refused', kind)
            self.assertIn('declared service kind', result['reason'])
        self.schedule.assert_not_called()

    def test_unlabelled_pin_without_adoption_still_needs_manual_adoption(self):
        result = self.deploy({**self.settings, 'autoDeploy': {'package': 'coordinator'}})
        self.assertEqual(result['status'], 'refused')
        self.assertIn('adopt it manually', result['reason'])

    def test_adoption_records_the_pin_and_opts_in_then_the_agent_upgrades(self):
        result = self.adopt(drain_delay=600)
        self.assertEqual((result['status'], result['image'], result['labelledRunId']), ('adopted', OLD, None))
        record = json.loads((self.root / 'auto-deploy' / 'adoption.json').read_text())
        self.assertEqual((record['kind'], record['package'], record['image'], record['service']),
                         ('coordinator', 'coordinator', OLD, 'coordinator'))
        settings = self.current()
        self.assertEqual(settings['autoDeploy'], {'package': 'coordinator', 'drainDelaySeconds': 600})
        self.assertEqual(settings['dockerEngineId'], 'engine')  # existing settings kept
        self.adapter.adoptable.assert_called_once_with(OLD)
        self.schedule.assert_not_called()  # adoption never schedules
        self.assertEqual(self.deploy()['status'], 'succeeded')
        self.assertEqual(self.schedule.call_args.args[0]['targetImage'], NEW)

    def test_adoption_covers_only_that_exact_pin(self):
        self.adopt()
        other = 'sha256:' + '3' * 64
        self.images[other] = {'Id': other, 'Config': {'Labels': {}}}
        save(self.root / 'compose.json', {'services': {'coordinator': {'image': other}}})
        result = self.deploy()
        self.assertEqual(result['status'], 'refused')
        self.assertIn('adopt it manually', result['reason'])

    def test_adoption_refusals(self):
        cases = [
            ({'image': NEW}, 'current Compose pin'),
            ({'kind': 'odx'}, 'coordinator or treedar'),
            ({'kind': 'treedar'}, 'another service kind'),
            ({'drain_delay': -1}, 'drain delay'),
        ]
        for change, message in cases:
            with self.assertRaisesRegex(DeployError, message):
                self.adopt(**change)
        self.adapter.adoptable.return_value = False
        with self.assertRaisesRegex(DeployError, 'not healthy'):
            self.adopt()
        self.adapter.adoptable.side_effect = UpdateError('boom')
        with self.assertRaisesRegex(DeployError, 'not healthy'):
            self.adopt()
        self.adapter.adoptable.side_effect, self.adapter.adoptable.return_value = None, True
        self.adapter.inspect.return_value = {'Image': NEW}
        with self.assertRaisesRegex(DeployError, 'not the Compose pin'):
            self.adopt()
        self.adapter.inspect.return_value = {'Image': OLD}
        self.node['state'] = 'draining'
        with self.assertRaisesRegex(DeployError, 'active'):
            self.adopt()
        self.node['state'] = 'active'
        self.images[OLD] = {'Id': OLD, 'Config': {'Labels': {release_packages.PACKAGE_LABEL: 'treedar'}}}
        with self.assertRaisesRegex(DeployError, 'another Processing package'):
            self.adopt()
        self.assertFalse((self.root / 'auto-deploy' / 'adoption.json').exists())
        self.assertNotIn('autoDeploy', self.current())

    def test_adoption_refuses_open_agent_state_and_repeat(self):
        directory = agent.state_directory(self.root)
        save(directory / agent.REFUSAL_FILE, {'stage': 'schedule'})
        with self.assertRaisesRegex(DeployError, 'refused.json'):
            self.adopt()
        (directory / agent.REFUSAL_FILE).unlink()
        self.adopt()
        with self.assertRaisesRegex(DeployError, 'already opted'):
            self.adopt()

    def test_adoption_resumes_after_an_interruption_before_opt_in(self):
        directory = agent.state_directory(self.root)
        save(directory / agent.ADOPTION_FILE, {'schemaVersion': 1, 'kind': 'coordinator', 'package': 'coordinator',
                                               'nodeId': 'cluster', 'project': 'owned', 'service': 'coordinator',
                                               'image': OLD, 'labelledRunId': None, 'adoptedAt': 'earlier'})
        self.assertEqual(self.adopt()['status'], 'adopted')
        self.assertEqual(json.loads((directory / agent.ADOPTION_FILE).read_text())['adoptedAt'], 'earlier')
        save(self.root / 'maintenance.json', self.settings)
        save(directory / agent.ADOPTION_FILE, {'schemaVersion': 1, 'kind': 'coordinator', 'package': 'coordinator',
                                               'nodeId': 'cluster', 'project': 'owned', 'service': 'coordinator',
                                               'image': NEW, 'labelledRunId': None, 'adoptedAt': 'earlier'})
        with self.assertRaisesRegex(DeployError, 'different adoption'):
            self.adopt()

    def test_labelled_pin_adoption_keeps_its_run_floor(self):
        self.images[OLD] = published(OLD, 150)
        self.assertEqual(self.adopt()['labelledRunId'], 150)
        self.images[REPO + ':verified'] = self.images[REPO + '@' + DIGEST] = published(
            NEW, 120, digests=[REPO + '@' + DIGEST])
        self.assertEqual(self.deploy()['status'], 'not_newer')


if __name__ == '__main__':
    unittest.main()
