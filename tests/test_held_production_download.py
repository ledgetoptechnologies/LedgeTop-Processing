"""A held production facade must not let a GET change production state.

Archive download is a GET, but it can publish intent/receipt markers, write or
restore the archive, contact an engine and record archive_path. These tests use
the real ViewerService and TaskStore with synthetic engines.
"""

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid

from processing.app import make_handler
from processing.config import Config
from processing.viewer_http import ViewerFacade
from tests.test_viewer_http import BOUNDARY, STAGING, TOKEN, MemoryHandler, multipart
from tests.test_viewer_service import FakeEngine, FakeFleet, OPTIONS


def tree(root):
    """Every path, size and content hash under the state directory."""
    entries = {}
    for path in sorted(Path(root).rglob('*')):
        relative = path.relative_to(root).as_posix()
        if path.is_file() and not path.is_symlink():
            entries[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            entries[relative] = 'dir' if path.is_dir() else 'other'
    return entries


def rows(root):
    db = sqlite3.connect('file:' + str(Path(root) / 'jobs.sqlite3') + '?mode=ro', uri=True)
    try:
        return {name: db.execute('SELECT * FROM "%s"' % name).fetchall() for (name,) in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name")}
    finally:
        db.close()


class HeldProductionDownloadTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = self.temporary.name
        config = Config('test-api-token', 'http://127.0.0.1:19001', 'http://127.0.0.1:19002', 1000, 20 * 1024 ** 3,
                        prototype_state_dir=self.root, prototype_node_ids=('cluster', 'main'),
                        viewer_enabled=True, viewer_token=TOKEN, viewer_staging_token=STAGING)
        self.handler = make_handler(config)
        service = self.service = self.handler.viewer_service
        fleet = FakeFleet()
        self.engines = {name: FakeEngine(name, fleet) for name in ('cluster', 'main')}
        service.engines = self.engines
        service._catalog = lambda: {'ready': True, 'commonOptions': [dict(x) for x in OPTIONS]}
        service._selected_options = lambda _node: [dict(x) for x in OPTIONS]
        service.probe = lambda role, origin, token: {'ready': True, 'taskQueueCount': 0,
                                                     'maxImages': 1000 if role == 'local' else None}
        self.spool = Path(self.root) / 'viewer-spool'
        self.open = ViewerFacade(service, TOKEN, STAGING, temp_dir=str(self.spool))
        self.held = ViewerFacade(service, TOKEN, STAGING, temp_dir=str(self.spool), production_held=True)

    def tearDown(self):
        self.service.close()
        self.handler.prototype_queue.close()
        self.temporary.cleanup()

    def call(self, facade, method, path, token, body=b'', **headers):
        handler = MemoryHandler(path + '?token=' + token, body, method=method, **headers)
        facade.handle(handler)
        return handler

    def completed_task(self, token):
        """A task that has finished on the engine but whose archive was never downloaded."""
        identifier = str(uuid.uuid4())
        init = multipart([('name', None, b'synthetic'), ('options', None, b'[]')])
        content = {'content_type': 'multipart/form-data; boundary=' + BOUNDARY}
        self.assertEqual(self.call(self.open, 'POST', '/task/new/init', token, init, set_uuid=identifier,
                                   content_length=str(len(init)), **content).status, 200)
        upload = multipart([('images', 'photo.jpg', b'synthetic photo')])
        self.assertEqual(self.call(self.open, 'POST', '/task/new/upload/' + identifier, token, upload,
                                   content_length=str(len(upload)), **content).status, 200)
        self.assertEqual(self.call(self.open, 'POST', '/task/new/commit/' + identifier, token,
                                   content_length='0').status, 200)
        self.service.tick()
        task = self.service.store.task(identifier)
        self.engines[task['node_id']].finish(task['engine_uuid'])
        self.service.tick()
        self.assertEqual(self.service.store.task(identifier)['job']['status'], 'succeeded')
        self.assertIsNone(self.service.store.task(identifier)['archive_path'])
        return identifier

    def test_held_production_download_refuses_before_any_state_change(self):
        identifier = self.completed_task(TOKEN)
        before_tree, before_rows = tree(self.root), rows(self.root)
        calls_before = sum(engine.download_calls for engine in self.engines.values())
        response = self.call(self.held, 'GET', '/task/' + identifier + '/download/all.zip', TOKEN)
        self.assertEqual(response.status, 503)
        self.assertEqual(json.loads(response.wfile.getvalue()), {'error': 'production_admission_held'})
        self.assertEqual(tree(self.root), before_tree, 'no marker, partial, archive or link was written')
        self.assertEqual(rows(self.root), before_rows, 'archive_path and every other row unchanged')
        self.assertEqual(sum(engine.download_calls for engine in self.engines.values()), calls_before,
                         'no engine was contacted')

    def test_held_production_metadata_reads_still_work_without_changing_state(self):
        identifier = self.completed_task(TOKEN)
        before_tree, before_rows = tree(self.root), rows(self.root)
        for path in ('/task/' + identifier + '/info', '/task/' + identifier + '/output'):
            self.assertEqual(self.call(self.held, 'GET', path, TOKEN).status, 200, path)
        self.assertEqual(tree(self.root), before_tree)
        self.assertEqual(rows(self.root), before_rows)

    def test_held_fails_closed_for_any_other_production_route(self):
        identifier = self.completed_task(TOKEN)
        for method, path in (('GET', '/task/' + identifier + '/download/other.zip'),
                             ('GET', '/task/' + identifier + '/unknown'), ('DELETE', '/task/' + identifier + '/info'),
                             ('POST', '/task/' + identifier + '/info')):
            response = self.call(self.held, method, path, TOKEN, content_length='0')
            self.assertEqual(response.status, 503, (method, path))

    def test_staging_download_is_not_held(self):
        identifier = self.completed_task(STAGING)
        response = self.call(self.held, 'GET', '/task/' + identifier + '/download/all.zip', STAGING)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.wfile.getvalue(), self.engines['cluster'].archive)
        self.assertIsNotNone(self.service.store.task(identifier)['archive_path'])

    def test_the_same_download_does_change_state_once_production_is_open(self):
        # Shows the held test above is meaningful: this GET writes when allowed.
        identifier = self.completed_task(TOKEN)
        before_tree = tree(self.root)
        response = self.call(self.open, 'GET', '/task/' + identifier + '/download/all.zip', TOKEN)
        self.assertEqual(response.status, 200)
        self.assertNotEqual(tree(self.root), before_tree)
        self.assertIsNotNone(self.service.store.task(identifier)['archive_path'])


if __name__ == '__main__':
    unittest.main()
