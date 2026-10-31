"""Synthetic POSIX coverage of bounded, receipt-backed background capture."""
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import patch

from processing import authority_lease
from processing.config import from_environment
from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.viewer_service import ViewerService
from tests import test_viewer_service as fixtures


class ViewerAutoCaptureConfigTests(unittest.TestCase):
    def test_default_off_and_explicit_enable_requires_authority_and_literal_nodes(self):
        env = {'PROCESSING_API_TOKEN': 'p' * 32}
        with patch.dict(os.environ, env, clear=True):
            self.assertFalse(from_environment().viewer_archive_autocapture)
        enabled = {**env, 'PROCESSING_VIEWER_ENABLED': '1', 'PROCESSING_VIEWER_TOKEN': 'v' * 32,
                   'PROCESSING_PROTOTYPE_STATE_DIR': '/state', 'PROCESSING_PROTOTYPE_NODE_IDS': 'cluster,main',
                   'PROCESSING_VIEWER_ARCHIVE_AUTOCAPTURE': '1', 'LOCAL_NODE_URL': 'http://127.0.0.1:3001',
                   'MAIN_NODE_URL': 'http://127.0.0.1:3004'}
        with patch.dict(os.environ, enabled, clear=True):
            with self.assertRaises(ValueError): from_environment()
        enabled['PROCESSING_AUTHORITY_LEASE_DIR'] = '/leases'
        enabled['LOCAL_NODE_URL'] = 'http://node.example:3001'
        with patch.dict(os.environ, enabled, clear=True):
            with self.assertRaises(ValueError): from_environment()
        if os.name == 'posix' and hasattr(os, 'posix_fallocate'):
            enabled['LOCAL_NODE_URL'] = 'http://127.0.0.1:3001'
            with patch.dict(os.environ, enabled, clear=True):
                self.assertTrue(from_environment().viewer_archive_autocapture)


@unittest.skipUnless(os.name == 'posix' and hasattr(os, 'posix_fallocate'), 'POSIX flock and allocation required')
class ViewerAutoCaptureTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ViewerServiceTests('runTest')
        self.fixture.setUp()
        self.root = self.fixture.root
        self.queue, self.service = self.fixture.queue, self.fixture.service
        self.engine_response = fixtures.synthetic_zip()
        self.requests = 0
        self.ready = threading.Event()
        self.release = threading.Event()
        self.release.set()
        self.include_length = True
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_GET(self):
                owner.requests += 1
                self.send_response(200)
                self.send_header('Content-Type', 'application/zip')
                if owner.include_length:
                    self.send_header('Content-Length', str(len(owner.engine_response)))
                self.end_headers()
                owner.ready.set()
                if owner.release.wait(15):
                    try: self.wfile.write(owner.engine_response)
                    except OSError: pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.fixture.config.local_node_url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.fixture.config.viewer_archive_free_floor = 1
        self.lease_dir = self.root / 'leases'; self.lease_dir.mkdir()
        self.lease = authority_lease.AuthorityLease(self.lease_dir, self.queue.authority_id)
        self.queue.authority_lease = self.lease
        self.identifier = self.fixture.submit()
        self.service.tick()
        self.fixture.finish(self.identifier)
        self.service.tick()
        self.assertEqual(self.fixture.task(self.identifier)['job']['status'], 'succeeded')

    def tearDown(self):
        self.release.set()
        self.server.shutdown(); self.server.server_close(); self.server_thread.join()
        self.service.close()
        if not self.queue._lock.closed:
            self.queue.close()
        self.lease.close()
        self.fixture.temporary.cleanup()

    def test_capture_persists_receipt_and_reuses_without_engine_mutation(self):
        self.service._archive_once(self.identifier)
        task = self.fixture.task(self.identifier)
        self.assertEqual(Path(task['archive_path']).read_bytes(), self.engine_response)
        self.assertTrue(Path(task['directory'], 'all.zip.receipt').is_file())
        self.assertEqual(self.requests, 1)
        self.service._archive_once(self.identifier)
        self.assertEqual(self.requests, 1)
        self.assertEqual(task['job']['status'], 'succeeded')

    def test_unknown_length_and_low_space_preserve_partial_and_engine(self):
        self.include_length = False
        with self.assertRaises(Exception): self.service._archive_once(self.identifier)
        task = self.fixture.task(self.identifier)
        self.assertIsNone(task['archive_path'])
        self.assertTrue(self.fixture.engine_for(self.identifier).tasks)
        self.include_length = True
        self.fixture.config.viewer_archive_free_floor = 10**18
        with self.assertRaises(Exception): self.service._archive_once(self.identifier)
        self.assertIsNone(self.fixture.task(self.identifier)['archive_path'])
        partial = Path(task['directory'], 'all.zip.partial')
        self.assertEqual(partial.stat().st_size, 0)
        self.fixture.config.viewer_archive_free_floor = 1
        with self.assertRaises(Exception): self.service._archive_once(self.identifier)
        self.assertEqual(partial.stat().st_size, 0)
        self.assertIsNone(self.fixture.task(self.identifier)['archive_path'])

    def test_allocation_failure_keeps_input_and_allows_later_retry(self):
        # A child whose filesystem cannot reserve bytes refuses before copying;
        # in-process reservation unit check makes the failure explicit.
        class File:
            def fileno(self): return 8
        statvfs = SimpleNamespace(f_bavail=1024, f_frsize=1024)
        with patch('processing.viewer_service.os.fstatvfs', return_value=statvfs), \
             patch('processing.viewer_service.os.posix_fallocate', side_effect=OSError('no space')):
            with self.assertRaises(OSError): self.service._reserve_archive(File(), 512)
        self.assertIsNone(self.fixture.task(self.identifier)['archive_path'])
        self.service._archive_once(self.identifier)
        self.assertEqual(Path(self.fixture.task(self.identifier)['archive_path']).read_bytes(), self.engine_response)

    def test_archive_fsync_failure_preserves_partial_for_inspection(self):
        actual_fsync = os.fsync
        def fail_archive(fd):
            if os.fstat(fd).st_size == len(self.engine_response):
                raise OSError('synthetic archive fsync failure')
            return actual_fsync(fd)
        with patch('processing.viewer_service.os.fsync', side_effect=fail_archive):
            with self.assertRaises(OSError): self.service.download(self.identifier, automatic=True)
        task = self.fixture.task(self.identifier)
        self.assertIsNone(task['archive_path'])
        self.assertFalse(Path(task['directory'], 'all.zip.receipt').exists())
        self.assertTrue(self.fixture.engine_for(self.identifier).tasks)
        partial = Path(task['directory'], 'all.zip.partial')
        before = partial.read_bytes()
        with self.assertRaises(Exception): self.service._archive_once(self.identifier)
        self.assertEqual(partial.read_bytes(), before)
        self.assertIsNone(self.fixture.task(self.identifier)['archive_path'])

    def test_background_discovers_completed_task_without_viewer_download(self):
        self.fixture.config.viewer_archive_autocapture = True
        self.service.start()
        deadline = time.monotonic() + 10
        while self.fixture.task(self.identifier)['archive_path'] is None:
            if time.monotonic() >= deadline: self.fail('automatic capture did not complete')
            time.sleep(.05)
        self.assertEqual(Path(self.fixture.task(self.identifier)['archive_path']).read_bytes(), self.engine_response)
        self.assertEqual(self.requests, 1)

    def test_inspection_status_survives_service_restart_and_is_sanitized(self):
        self.fixture.config.viewer_archive_autocapture = True
        self.fixture.config.viewer_archive_free_floor = 10**18
        self.service.start()
        deadline = time.monotonic() + 10
        while self.service.info(self.identifier).get('ledgetopArchive', {}).get('state') != 'inspection':
            if time.monotonic() >= deadline: self.fail('inspection status not persisted')
            time.sleep(.05)
        info = self.service.info(self.identifier)
        self.assertEqual(info['status']['code'], 40)
        self.assertEqual(info['ledgetopArchive'], {'state': 'inspection', 'reason': 'partial_requires_inspection'})
        self.service.close()
        restarted = ViewerService(self.queue, self.fixture.config, engines=self.fixture.engines, probe=self.fixture.probe)
        self.assertEqual(restarted.info(self.identifier)['ledgetopArchive'], info['ledgetopArchive'])
        self.assertTrue(Path(self.fixture.task(self.identifier)['directory'], 'all.zip.partial').exists())

    def test_capture_status_distinguishes_processing_and_custody(self):
        self.fixture.config.viewer_archive_autocapture = True
        self.assertEqual(self.service.info(self.identifier)['ledgetopArchive']['state'], 'pending')
        self.service._archive_once(self.identifier)
        info = self.service.info(self.identifier)
        self.assertEqual(info['status']['code'], 40)
        self.assertEqual(info['ledgetopArchive'], {'state': 'archived', 'reason': None})

    def test_shutdown_kills_and_reaps_blocked_capture_before_authority_release(self):
        self.release.clear()
        self.fixture.config.viewer_archive_autocapture = True
        self.service.start()
        self.assertTrue(self.ready.wait(10))
        before = time.monotonic()
        self.service.close()
        self.assertLess(time.monotonic() - before, 10)
        self.assertFalse(self.service._archive_thread.is_alive())
        self.assertIsNone(self.fixture.task(self.identifier)['archive_path'])
        self.release.set()
        # Interrupted bytes remain untouched; an operator must inspect them.
        partial = Path(self.fixture.task(self.identifier)['directory'], 'all.zip.partial')
        self.service._stop.clear()
        if partial.exists():
            before = partial.read_bytes()
            with self.assertRaises(Exception): self.service._archive_once(self.identifier)
            self.assertEqual(partial.read_bytes(), before)
        else:
            self.service._archive_once(self.identifier)
            self.assertEqual(Path(self.fixture.task(self.identifier)['archive_path']).read_bytes(), self.engine_response)

    def test_parent_death_leaves_coordinator_flocks_held_until_capture_child_exits(self):
        # A slow fixture keeps the actual capture child alive while its parent
        # process dies. A replacement must fail to acquire both flocks.
        self.release.clear()
        authority = self.queue.authority_id
        self.queue.close(); self.lease.close()
        code = r'''
import sys
from types import SimpleNamespace
from processing.authority_lease import AuthorityLease
from processing.prototype import Queue
from processing.viewer_service import ViewerService
root, lease_dir, authority, identifier, origin = sys.argv[1:]
q=Queue(root,node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
q.authority_lease=AuthorityLease(lease_dir,authority)
c=SimpleNamespace(prototype_state_dir=root,viewer_max_input_bytes=20*1024**3,
 viewer_max_archive_bytes=4*1024**2,viewer_archive_free_floor=1,
 local_node_url=origin,local_node_token='synthetic',main_node_url=origin,main_node_token='synthetic')
ViewerService(q,c)._archive_once(identifier)
'''
        parent = subprocess.Popen([sys.executable, '-c', code, str(self.root / 'state'), str(self.lease_dir),
                                   authority, self.identifier, self.fixture.config.local_node_url],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            self.assertTrue(self.ready.wait(10))
            self.assertIsNone(parent.poll())
            parent.kill(); parent.wait(timeout=5)
            with self.assertRaises(RuntimeError):
                Queue(self.root / 'state', node_ids=('cluster','main'), treedar_node_ids=('main',), viewer_enabled=True)
            with self.assertRaises(authority_lease.LeaseUnavailable):
                authority_lease.AuthorityLease(self.lease_dir, authority)
            self.release.set()
            deadline = time.monotonic() + 15
            while True:
                try:
                    recovered = Queue(self.root / 'state', node_ids=('cluster','main'),
                                      treedar_node_ids=('main',), viewer_enabled=True)
                    break
                except RuntimeError:
                    if time.monotonic() >= deadline: raise
                    time.sleep(.1)
            self.queue = recovered
            self.lease = authority_lease.AuthorityLease(self.lease_dir, authority)
            self.queue.authority_lease = self.lease
            self.service = ViewerService(self.queue, self.fixture.config, engines=self.fixture.engines, probe=self.fixture.probe)
            # Child finished the receipt but parent died before SQLite update;
            # a later capture revalidates existing bytes and records the path.
            self.service._archive_once(self.identifier)
            task = self.service.store.task(self.identifier)
            self.assertEqual(Path(task['archive_path']).read_bytes(), self.engine_response)
            self.assertEqual(self.requests, 1)
        finally:
            self.release.set()
            if parent.poll() is None:
                parent.kill(); parent.wait(timeout=5)


if __name__ == '__main__':
    unittest.main()
