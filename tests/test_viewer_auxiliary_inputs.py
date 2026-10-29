"""Synthetic multipart -> durable store -> scheduler -> native adapter contract.

This proves auxiliary-byte transport, not ODM GCP syntax, CRS or survey accuracy.
No engine process, live network, client files or installed state is used.
"""
import hashlib
import json
from pathlib import Path
import unittest
import uuid

from processing.viewer_http import ViewerFacade
from tests.test_viewer_http import BOUNDARY, MemoryHandler, multipart
from tests import test_viewer_service as service_fixture


class AuxiliaryInputContractTests(unittest.TestCase):
    def setUp(self):
        self.fixture = service_fixture.ViewerServiceTests('test_exact_upload_bytes_use_small_node_and_main_is_untouched')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.fixture.config.local_max_photos = 2
        self.facade = ViewerFacade(self.fixture.service, 'synthetic-production-token')
        self.identifier = str(uuid.uuid4())
        self.assertEqual(self.post('/task/new/init', multipart([('name', None, b'auxiliary transport')]),
                                   set_uuid=self.identifier).status, 200)

    def post(self, route, body=b'', **headers):
        headers.setdefault('Content_Type', 'multipart/form-data; boundary=' + BOUNDARY)
        handler = MemoryHandler(route+'?token=synthetic-production-token', body,
                                Content_Length=str(len(body)), **headers)
        self.assertTrue(self.facade.handle(handler))
        return handler

    def upload(self, files):
        return self.post('/task/new/upload/'+self.identifier,
                         multipart([('images', name, content) for name, content in files]))

    def test_gcp_snapshot_keeps_name_hash_and_bytes_without_counting_as_photo(self):
        photos = [('image-a.jpg', b'\xff\xd8synthetic-a'), ('image-b.jpg', b'\xff\xd8synthetic-b')]
        gcp = b'EPSG:32616\r\n100 200 300 10 20 image-a.jpg marker-1\r\n'
        self.assertEqual(self.upload(photos).status, 200)
        self.assertEqual(self.upload([('gcp_list.txt', gcp)]).status, 200)
        before = self.fixture.service.store.task(self.identifier)
        self.assertEqual(before['images_count'], 2)
        self.assertEqual(before['input_bytes'], sum(len(content) for _, content in photos)+len(gcp))
        item = next(file for file in before['files'] if file['name'] == 'gcp_list.txt')
        self.assertEqual(item['sha256'], hashlib.sha256(gcp).hexdigest())
        self.assertEqual(Path(item['path']).read_bytes(), gcp)
        self.assertEqual(self.post('/task/new/commit/'+self.identifier).status, 200)
        self.fixture.service.tick()
        current = self.fixture.service.store.task(self.identifier)
        self.assertEqual(current['node_id'], 'cluster')
        actual = self.fixture.engines['cluster'].tasks[current['engine_uuid']]['files']
        self.assertEqual(actual, dict(photos+[('gcp_list.txt', gcp)]))
        self.assertEqual(self.fixture.engines['main'].calls, [])
        self.assertEqual(self.fixture.fleet.max_active, 1)

    def test_auxiliary_only_task_never_enters_processing_queue(self):
        self.assertEqual(self.upload([('gcp_list.txt', b'EPSG:32616\n')]).status, 200)
        self.assertEqual(self.post('/task/new/commit/'+self.identifier).status, 400)
        self.assertEqual(self.fixture.queue.list(), [])
        self.assertTrue(all(not engine.calls for engine in self.fixture.engines.values()))

    def test_exact_auxiliary_replay_is_safe_but_changed_snapshot_is_refused(self):
        original = b'EPSG:32616\n1 2 3 4 5 image-a.jpg marker-1\n'
        self.assertEqual(self.upload([('gcp_list.txt', original)]).status, 200)
        self.assertEqual(self.upload([('gcp_list.txt', original)]).status, 200)
        changed = self.upload([('gcp_list.txt', original+b'changed\n')])
        self.assertEqual(changed.status, 409)
        self.assertEqual(json.loads(changed.wfile.getvalue()), {'error': 'conflict'})
        files = self.fixture.service.store.task(self.identifier)['files']
        self.assertEqual(len(files), 1)
        self.assertEqual(Path(files[0]['path']).read_bytes(), original)


if __name__ == '__main__':
    unittest.main()
