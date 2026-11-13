"""Synthetic durable-mapping and real facade boundary checks; no engine calls."""
import hashlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.viewer_http import ViewerFacade
from processing.viewer_service import ViewerService
from processing.viewer_tasks import TaskStore, VIEWER_PROFILE, canonical
from tests.test_viewer_http import MemoryHandler, TOKEN, STAGING


class ProducerBindingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.queue = Queue(root / 'queue', node_ids=('cluster', 'main'))
        self.addCleanup(self.queue.close)
        self.queue.allowed_profiles.add(VIEWER_PROFILE)
        self.store = TaskStore(self.queue, root / 'inputs')
        self.identifier, self.native = str(uuid.uuid4()), str(uuid.uuid4())
        self.store.initialize(self.identifier, 'synthetic', [], None, 'staging')
        self.store.upload(self.identifier, 'photo.jpg', io.BytesIO(b'photo'), 5)
        self.store.commit(self.identifier)
        claim = self.queue.claim({'workerId': 'synthetic', 'nodeId': 'cluster', 'profiles': [VIEWER_PROFILE]})
        self.job = claim['id']
        self.store.update(self.identifier, node_id='cluster', engine_uuid=self.native,
                          claim_token='never-return-this-secret', stage='running', phase='processing')
        result = {'engineTaskId': self.native, 'nodeId': 'cluster',
                  'environment': 'staging', 'cancelled': False}
        self.queue.complete(self.job, {'claimToken': claim['claimToken'], 'status': 'succeeded', 'result': result})
        task = self.store.task(self.identifier)
        expected = {key: task[key] for key in ('generation', 'job_id', 'engine_uuid', 'node_id', 'directory')}
        self.store.update(self.identifier, expected_result=expected, stage='succeeded',
                          phase='complete', archive_sha256='a' * 64)

    def test_mapping_is_read_only_minimal_and_digest_bound(self):
        with patch.object(self.store, '_task', side_effect=AssertionError('no input enumeration')), \
                patch.object(self.store, '_directory', side_effect=AssertionError('no filesystem lookup')):
            value = self.store.producer_binding(self.identifier, 'staging')
            self.assertEqual(value, self.store.producer_binding(self.identifier, 'staging'))
        expected = {'schemaVersion': 1, 'providerTaskId': self.identifier, 'producerGeneration': 1,
                    'environment': 'staging', 'jobId': self.job, 'nativeTaskId': self.native,
                    'nodeId': 'cluster', 'archiveSha256': 'a' * 64,
                    'archiveEvidence': 'producer-record-not-fresh-byte-verification'}
        self.assertEqual(value['bindingSha256'], hashlib.sha256(
            b'ltds-producer-binding-v1\0' + canonical(expected).encode()).hexdigest())
        self.assertEqual(value, {**expected, 'bindingSha256': value['bindingSha256']})
        self.assertNotIn('never-return', json.dumps(value))

    def test_other_environment_unknown_and_removed_are_not_found(self):
        for identifier, environment in ((self.identifier, 'production'), (str(uuid.uuid4()), 'staging')):
            with self.assertRaises(KeyError):
                self.store.producer_binding(identifier, environment)
        with self.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET removed=1 WHERE uuid=?', (self.identifier,))
        with self.assertRaises(KeyError):
            self.store.producer_binding(self.identifier, 'staging')

    def test_incomplete_or_inconsistent_durable_records_reject(self):
        mutations = [('viewer_tasks', 'stage', 'failed'), ('viewer_tasks', 'phase', 'processing'),
                     ('viewer_tasks', 'archive_sha256', None), ('viewer_tasks', 'generation', 0),
                     ('viewer_tasks', 'engine_uuid', 'invalid'), ('viewer_tasks', 'node_id', 'main'),
                     ('jobs', 'status', 'running'), ('jobs', 'environment', 'production'),
                     ('jobs', 'profile', 'other'), ('jobs', 'result', '{}'),
                     ('jobs', 'result', '{invalid'), ('jobs', 'request_id', str(uuid.uuid4()))]
        for table, field, value in mutations:
            with self.subTest(table=table, field=field), self.queue._db() as db:
                key, identity = ('uuid', self.identifier) if table == 'viewer_tasks' else ('id', self.job)
                before = db.execute(f'SELECT {field} FROM {table} WHERE {key}=?', (identity,)).fetchone()[0]
                db.execute(f'UPDATE {table} SET {field}=? WHERE {key}=?', (value, identity))
            with self.assertRaises(Conflict):
                self.store.producer_binding(self.identifier, 'staging')
            with self.queue._db() as db:
                db.execute(f'UPDATE {table} SET {field}=? WHERE {key}=?', (before, identity))

    def test_generation_without_matching_job_incarnation_rejects(self):
        first = self.store.producer_binding(self.identifier, 'staging')
        with self.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET generation=2 WHERE uuid=?', (self.identifier,))
        with self.assertRaises(Conflict):
            self.store.producer_binding(self.identifier, 'staging')
        self.assertEqual(first['producerGeneration'], 1)

    def test_real_reinitialization_binds_new_job_and_generation(self):
        first = self.store.producer_binding(self.identifier, 'staging')
        self.store.remove(self.identifier)
        self.store.initialize(self.identifier, 'synthetic', [], None, 'staging')
        self.store.upload(self.identifier, 'photo.jpg', io.BytesIO(b'photo'), 5)
        self.store.commit(self.identifier)
        claim = self.queue.claim({'workerId': 'synthetic', 'nodeId': 'cluster', 'profiles': [VIEWER_PROFILE]})
        native = str(uuid.uuid4())
        self.store.update(self.identifier, node_id='cluster', engine_uuid=native)
        result = {'engineTaskId': native, 'nodeId': 'cluster', 'environment': 'staging', 'cancelled': False}
        self.queue.complete(claim['id'], {'claimToken': claim['claimToken'], 'status': 'succeeded', 'result': result})
        task = self.store.task(self.identifier)
        expected = {key: task[key] for key in ('generation', 'job_id', 'engine_uuid', 'node_id', 'directory')}
        self.store.update(self.identifier, expected_result=expected, stage='succeeded',
                          phase='complete', archive_sha256='b' * 64)
        second = self.store.producer_binding(self.identifier, 'staging')
        self.assertEqual(second['producerGeneration'], 2)
        self.assertNotEqual(first['jobId'], second['jobId'])
        self.assertNotEqual(first['nativeTaskId'], second['nativeTaskId'])
        self.assertNotEqual(first['bindingSha256'], second['bindingSha256'])

    def test_tls_environment_and_query_boundaries(self):
        def authorize(identifier, environment):
            if self.store.task_state(identifier)['environment'] != environment:
                raise KeyError('unknown task')
        service = SimpleNamespace(producer_binding=self.store.producer_binding, authorize=authorize,
                                  capabilities=lambda: {})
        facade = ViewerFacade(service, TOKEN, STAGING, temp_dir=self.tmp.name)
        path = '/task/' + self.identifier + '/producer-binding'
        for token, tls, suffix, status in ((STAGING, True, '', 200), (TOKEN, True, '', 404),
                                           (STAGING, False, '', 403), ('wrong', True, '', 401),
                                           (STAGING, True, '&environment=production', 400)):
            with self.subTest(token=token, tls=tls, suffix=suffix):
                handler = MemoryHandler(path + '?token=' + token + suffix, method='GET', viewer_tls=tls)
                self.assertTrue(facade.handle(handler))
                self.assertEqual(handler.status, status)
                self.assertEqual(handler.response_headers['Cache-Control'], 'no-store')
        for tls in (False, True):
            handler = MemoryHandler('/info?token=' + STAGING, method='GET', viewer_tls=tls)
            facade.handle(handler)
            self.assertEqual('producerBinding' in json.loads(handler.wfile.getvalue()), tls)

    def test_real_service_route_never_resolves_original_input_directories(self):
        service = ViewerService.__new__(ViewerService)
        service.store = self.store
        facade = ViewerFacade(service, TOKEN, STAGING, temp_dir=self.tmp.name)
        path = '/task/' + self.identifier + '/producer-binding?token='
        with patch.object(self.store, '_task', side_effect=AssertionError('input scan')), \
                patch.object(self.store, '_directory', side_effect=AssertionError('input directory')):
            for token, status in ((STAGING, 200), (TOKEN, 404)):
                handler = MemoryHandler(path + token, method='GET', viewer_tls=True)
                facade.handle(handler)
                self.assertEqual(handler.status, status)


if __name__ == '__main__':
    unittest.main()
