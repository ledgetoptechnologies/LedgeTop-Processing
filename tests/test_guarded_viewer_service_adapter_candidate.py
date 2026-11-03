"""ViewerService reads against a task-bound guarded mTLS node, synthetically."""
import shutil
import ssl
import unittest
import uuid

from ops.native_node_fixture import ARCHIVE
from processing import reference_protocol as protocol
from processing.guarded_native_client import GuardedNativeClient
from processing.viewer_service import EngineUnavailable
from tests.guarded_test_signer import operation_signer
from tests.test_bound_node_job_candidate import BoundJobTests, KEY
from tests.test_guarded_native_https import TOKEN
from tests.test_viewer_service import ViewerServiceTests


class GuardedViewerAdapterTests(BoundJobTests):
    def test_service_status_output_archive_use_bound_guard_and_hold_pending(self):
        coordinator = ViewerServiceTests(methodName='runTest')
        coordinator.setUp()
        self.addCleanup(coordinator.tearDown)
        identifier = coordinator.initialize(options=[])
        coordinator.upload(identifier, name='photo.jpg', content=b'abc')
        coordinator.service.commit(identifier)
        coordinator.service.tick()
        task = coordinator.task(identifier)
        # The synthetic guard fixture is provisioned as main. Align this
        # isolated coordinator record before creating its signed claim.
        with coordinator.queue._db() as db:
            db.execute("UPDATE viewer_tasks SET node_id='main' WHERE uuid=?", (identifier,))
            db.execute("UPDATE jobs SET node_id='main' WHERE id=?", (task['job_id'],))
        task = coordinator.task(identifier)
        self.job = task['engine_uuid']
        self.task = {k: task[k] for k in ('name', 'engine_uuid', 'outputs', 'options')}
        claim = self.envelope(viewerJobId=task['job_id'], viewerGeneration=task['generation'],
                              requestSha256=protocol.digest(self.task))
        self.commit_reply(True)
        server = self.start_listener()
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        client = GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10,
            job_claim=claim, operation_signer=operation_signer(self.proxy, KEY, claim))
        client.initialize(self.task)
        coordinator.service.engines['main'] = client
        # Until the node has a durable commit, a poll must not report it as
        # running or complete, nor fetch its archive.
        coordinator.service._poll(task)
        self.assertEqual(coordinator.service.info(identifier)['status']['code'], 10)
        self.assertEqual(self.backend.counts()['download'], 0)
        with self.assertRaises(EngineUnavailable):
            client.read('/task/' + str(uuid.uuid4()) + '/info')
        for route, line in (('/info', 0), ('/task/' + self.job + '/remove', 0),
                            ('/task/' + self.job + '/output', -1),
                            ('/task/' + self.job + '/info', 1)):
            with self.subTest(route=route, line=line), self.assertRaises(EngineUnavailable):
                client.read(route, line=line)
        with self.assertRaises(EngineUnavailable):
            with client.download(str(uuid.uuid4())):
                self.fail('other task archive')
        source = self.spool / 'viewer-source.jpg'
        shutil.copyfile(task['files'][0]['path'], source)
        source.chmod(0o600)
        item = task['files'][0]
        client.upload(self.job, {'path': str(source), **{k: item[k] for k in ('name', 'bytes', 'sha256')}})
        client.action('commit', self.job)
        coordinator.service._backoff.clear()
        coordinator.service._poll(coordinator.task(identifier))
        self.assertEqual(coordinator.service.info(identifier)['status']['code'], 40)
        self.assertEqual(coordinator.service.output(identifier, 1), ['', 'synthetic phase two'])
        stream, size = coordinator.service.download(identifier)
        with stream:
            self.assertEqual(stream.read(), ARCHIVE)
        self.assertEqual(size, len(ARCHIVE))
        self.assertEqual(self.backend.counts()['download'], 1)


if __name__ == '__main__':
    unittest.main()
