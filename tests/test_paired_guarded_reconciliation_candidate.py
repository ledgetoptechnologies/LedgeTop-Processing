import io
import shutil
import ssl
import unittest
from pathlib import Path
from unittest.mock import patch

from processing.guarded_native_client import GuardedNativeClient
from processing.guarded_reconciliation_candidate import inspect_checkpoint, ReconciliationHeld
from processing import reference_protocol as protocol
from tests.test_bound_node_job_candidate import BoundJobTests, KEY
from tests.test_guarded_native_https import TOKEN
from tests.test_viewer_service import ViewerServiceTests
from tests.guarded_test_signer import operation_signer


class PairedReconciliation(BoundJobTests):
    def test_real_coordinator_checkpoint_and_bound_mtls_journal_are_compared_without_resume(self):
        coordinator = ViewerServiceTests(methodName='runTest')
        coordinator.setUp()
        self.addCleanup(coordinator.tearDown)
        identifier = coordinator.initialize(options=[])
        coordinator.upload(identifier, name='photo.jpg', content=b'abc')
        coordinator.service.commit(identifier)
        coordinator.engines['cluster'].node_guarded = True
        coordinator.engines['cluster'].fail = 'upload'
        coordinator.service.tick()
        task = coordinator.task(identifier)
        self.assertEqual(task['job']['status'], 'held')
        self.assertEqual(coordinator.service.store.guarded_dispatch_checkpoint(identifier)['state'], 'pending')
        # The synthetic node gate is provisioned as "main". Rebind only this
        # isolated fixture's held coordinator rows to that node.
        with coordinator.queue._db() as db:
            db.execute("UPDATE viewer_tasks SET node_id='main' WHERE uuid=?", (identifier,))
            db.execute("UPDATE jobs SET node_id='main' WHERE id=?", (task['job_id'],))
            db.execute("UPDATE viewer_guarded_dispatch_checkpoint SET node_id='main' WHERE uuid=?", (identifier,))
        task = coordinator.task(identifier)
        self.job = task['engine_uuid']
        self.task = {k: task[k] for k in ('name', 'engine_uuid', 'outputs', 'options')}
        env = self.envelope(viewerJobId=task['job_id'], viewerGeneration=task['generation'],
                            requestSha256=protocol.digest(self.task))
        server = self.start_listener()
        self.assertEqual(self.request(server, '/v1/initialize-bound', {'task': self.task, 'jobClaim': env})[0], 200)
        item = task['files'][0]
        source = self.spool / 'paired.jpg'
        shutil.copyfile(item['path'], source)
        source.chmod(0o600)
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        client = GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10,
                                   job_claim=env, operation_signer=operation_signer(self.proxy, KEY, env))
        client.upload(self.job, {'path': str(source), **{k: item[k] for k in ('name', 'bytes', 'sha256')}})
        kwargs = dict(claim_key=KEY, node='main', authority=self.authority,
                      gate_generation=1, origin='https://coordinator.invalid')
        before = coordinator.service.store.guarded_dispatch_checkpoint(identifier)
        result = inspect_checkpoint(coordinator.service.store, identifier, env, client, **kwargs)
        self.assertEqual(result, {'state': 'done', 'held': True, 'operation': 'upload'})
        self.assertEqual(coordinator.service.store.guarded_dispatch_checkpoint(identifier), before)
        self.assertEqual(coordinator.task(identifier)['job']['status'], 'held')
        with self.assertRaises((ReconciliationHeld, ValueError)):
            inspect_checkpoint(coordinator.service.store, identifier,
                self.envelope(viewerJobId='00000000-0000-4000-8000-000000000001',
                              viewerGeneration=task['generation'], requestSha256=protocol.digest(self.task)),
                client, **kwargs)
        # Even a syntactically valid wrong ACK from a peer cannot pass.
        original = client.inspect_bound_mutation
        with patch.object(client, 'inspect_bound_mutation', side_effect=lambda *args: {**original(*args), 'ackDigest': '0'*64}):
            with self.assertRaises(ReconciliationHeld):
                inspect_checkpoint(coordinator.service.store, identifier, env, client, **kwargs)
        # A changed coordinator checkpoint cannot be accepted merely because a
        # valid node response was in flight.
        def changed_during_query(*args):
            value = original(*args)
            with coordinator.queue._db() as db:
                db.execute('UPDATE viewer_guarded_dispatch_checkpoint SET sequence=sequence+1 WHERE uuid=?', (identifier,))
            return value
        with patch.object(client, 'inspect_bound_mutation', side_effect=changed_during_query):
            with self.assertRaises(ReconciliationHeld):
                inspect_checkpoint(coordinator.service.store, identifier, env, client, **kwargs)
        self.assertEqual(self.backend.counts()['upload'], 1)


if __name__ == '__main__':
    unittest.main()
