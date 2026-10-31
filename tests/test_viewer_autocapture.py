"""Synthetic POSIX coverage of bounded, receipt-backed background capture."""
import json
import os
from pathlib import Path
import subprocess
import sqlite3
import tempfile
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


@unittest.skipUnless(os.name == 'posix', 'process kill semantics require POSIX')
class ArchiveSchemaAtomicityTests(unittest.TestCase):
    def test_real_method_transaction_survives_kill_and_ddl_failure(self):
        script = r"""
import contextlib, os, signal, sqlite3, sys, threading
from processing.viewer_tasks import TaskStore
path, stage = sys.argv[1:]
class Queue:
    @contextlib.contextmanager
    def _db(self):
        db = sqlite3.connect(path)
        def trace(sql):
            if (stage == 'first' and 'ADD COLUMN archive_state' in sql) or (stage == 'second' and 'ADD COLUMN archive_reason' in sql) or (stage == 'commit' and sql == 'COMMIT'):
                os.kill(os.getpid(), signal.SIGKILL)
        if stage == 'error':
            def authorizer(action, a, b, c, d):
                if action == sqlite3.SQLITE_ALTER_TABLE and seen[0]: return sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_ALTER_TABLE: seen[0] = True
                return sqlite3.SQLITE_OK
            seen=[False]; db.set_authorizer(authorizer)
        db.set_trace_callback(trace)
        try:
            with db: yield db
        finally: db.close()
store=object.__new__(TaskStore); store._lock=threading.RLock(); store.queue=Queue()
store.enable_archive_status()
"""
        for stage in ('first', 'second', 'commit', 'error', 'success'):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as root:
                dbpath = str(Path(root) / 'tasks.sqlite3')
                with sqlite3.connect(dbpath) as db:
                    db.execute('CREATE TABLE viewer_tasks (uuid TEXT PRIMARY KEY)')
                    db.execute("INSERT INTO viewer_tasks VALUES ('synthetic')")
                run = subprocess.run([sys.executable, '-c', script, dbpath, stage],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
                with sqlite3.connect(dbpath) as db:
                    cols=[row[1] for row in db.execute('PRAGMA table_info(viewer_tasks)')]
                    self.assertEqual(db.execute('SELECT uuid FROM viewer_tasks').fetchone()[0], 'synthetic')
                self.assertEqual(cols, ['uuid', 'archive_state', 'archive_reason'] if stage == 'success' else ['uuid'])
                self.assertEqual(run.returncode == 0, stage == 'success')

    def test_partial_and_unrecognized_schema_are_refused_without_mutation(self):
        # Exercise the real TaskStore method against owned synthetic SQLite.
        from contextlib import contextmanager
        from processing.viewer_tasks import TaskStore
        class Queue:
            def __init__(self, path): self.path=path
            @contextmanager
            def _db(self):
                db=sqlite3.connect(self.path)
                try:
                    with db: yield db
                finally: db.close()
        for fragment in ("archive_state TEXT NOT NULL DEFAULT 'pending'",
                         "archive_state TEXT, archive_reason TEXT"):
            with self.subTest(fragment=fragment), tempfile.TemporaryDirectory() as root:
                path=str(Path(root)/'tasks.sqlite3')
                with sqlite3.connect(path) as db: db.execute('CREATE TABLE viewer_tasks (uuid TEXT, '+fragment+')')
                store=object.__new__(TaskStore); store._lock=threading.RLock(); store.queue=Queue(path)
                with sqlite3.connect(path) as db: before=db.execute('SELECT sql FROM sqlite_master').fetchall()
                with self.assertRaises(ValueError): store.enable_archive_status()
                with sqlite3.connect(path) as db: self.assertEqual(db.execute('SELECT sql FROM sqlite_master').fetchall(),before)


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
        self.engine_http_status = 200
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_GET(self):
                owner.requests += 1
                if owner.engine_http_status != 200:
                    self.send_response(owner.engine_http_status); self.send_header('Content-Length','0'); self.end_headers(); return
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

    def test_same_incarnation_retains_backoff_then_retries(self):
        self.engine_http_status=503;self.fixture.config.viewer_archive_autocapture=True
        self.service.start();deadline=time.monotonic()+10
        while self.identifier not in self.service._archive_retry:
            if time.monotonic()>=deadline:self.fail('failure was not recorded')
            time.sleep(.02)
        task=self.fixture.task(self.identifier)
        identity,due,_=self.service._archive_retry[self.identifier]
        self.assertEqual(identity,(task['generation'],task['job_id']))
        self.engine_http_status=200
        self.assertGreater(due,time.monotonic());self.assertEqual(self.requests,1)
        while self.fixture.task(self.identifier)['archive_path'] is None:
            if time.monotonic()>=deadline:self.fail('retry did not complete')
            time.sleep(.05)
        self.assertEqual(self.requests,2);self.assertEqual(len(self.fixture.fleet.commits),1)

    def test_new_incarnation_does_not_inherit_old_capture_backoff(self):
        # The first generation fails against a real loopback HTTP endpoint.
        self.engine_http_status=503;self.fixture.config.viewer_archive_autocapture=True
        old=self.fixture.task(self.identifier);old_dir=Path(old['directory']);old_input=Path(old['files'][0]['path']);old_bytes=old_input.read_bytes()
        self.service.start();deadline=time.monotonic()+10
        while self.identifier not in self.service._archive_retry:
            if time.monotonic()>=deadline:self.fail('old capture failure was not recorded')
            time.sleep(.02)
        identity,due,failures=self.service._archive_retry[self.identifier]
        self.assertEqual(identity,(old['generation'],old['job_id']))
        self.assertEqual(self.requests,1)
        # Widen the observed retry deadline for a deterministic check; the old
        # failure itself and the replacement capture use real worker processes.
        self.service._archive_retry[self.identifier]=(identity,time.monotonic()+60,failures)
        self.service.remove(self.identifier)
        self.service.initialize(self.identifier,'Synthetic survey',fixtures.REQUEST_OPTIONS,None,'production')
        self.fixture.upload(self.identifier);self.service.commit(self.identifier)
        self.engine_http_status=200;deadline=time.monotonic()+10
        while True:
            current=self.fixture.task(self.identifier);node=current['node_id'];engine_id=current['engine_uuid']
            if node is not None and engine_id in self.fixture.engines[node].tasks:break
            if time.monotonic()>=deadline:self.fail('replacement did not dispatch')
            time.sleep(.02)
        self.fixture.finish(self.identifier)
        while self.fixture.task(self.identifier)['archive_path'] is None:
            if time.monotonic()>=deadline:self.fail('replacement inherited old backoff')
            time.sleep(.05)
        new=self.fixture.task(self.identifier)
        self.assertEqual(new['generation'],old['generation']+1);self.assertNotEqual(new['job_id'],old['job_id'])
        self.assertEqual(Path(new['archive_path']).read_bytes(),self.engine_response)
        self.assertTrue((old_dir/'all.zip.intent').exists());self.assertEqual(old_input.read_bytes(),old_bytes)
        self.assertEqual(self.requests,2);self.assertEqual(len(self.fixture.fleet.commits),2)

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

    def test_real_http_503_before_restore_slot_allows_retry_after_process_restart(self):
        import hashlib
        self.service._archive_once(self.identifier)
        task=self.fixture.task(self.identifier);path=Path(task['archive_path']);receipt=path.with_suffix('.zip.receipt');receipt_before=receipt.read_bytes()
        source=Path(task['files'][0]['path']);source_before=source.read_bytes();path.unlink();authority=self.queue.authority_id
        self.service.close();self.queue.close();self.lease.close()
        code=r"""
import hashlib,sys
from types import SimpleNamespace
from urllib.error import HTTPError
from processing.authority_lease import AuthorityLease
from processing.prototype import Queue
from processing.viewer_service import ViewerService
root,lease_dir,authority,identifier,origin,mode,expected=sys.argv[1:]
q=Queue(root,node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
lease=AuthorityLease(lease_dir,authority);q.authority_lease=lease
c=SimpleNamespace(prototype_state_dir=root,viewer_max_input_bytes=20*1024**3,viewer_max_archive_bytes=4*1024**2,viewer_archive_free_floor=1,local_node_url=origin,local_node_token='',main_node_url=origin,main_node_token='')
s=ViewerService(q,c)
try:
 if mode=='fail':
  try:s.download(identifier)
  except HTTPError as error:
   if error.code!=503:raise
  else:raise AssertionError('expected synthetic 503')
 else:
  stream,size=s.download(identifier)
  with stream:data=stream.read()
  if len(data)!=size or hashlib.sha256(data).hexdigest()!=expected:raise AssertionError('result mismatch')
finally:s.close();lease.close();q.close()
"""
        args=[sys.executable,'-c',code,str(self.root/'state'),str(self.lease_dir),authority,self.identifier,self.fixture.config.local_node_url]
        digest=hashlib.sha256(self.engine_response).hexdigest();self.engine_http_status=503
        first=subprocess.run(args+['fail',digest],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=10)
        self.assertEqual(first.returncode,0)
        pending=path.parent/'.all.zip.restore-pending';self.assertFalse(pending.exists());self.assertEqual(receipt.read_bytes(),receipt_before)
        self.engine_http_status=200
        second=subprocess.run(args+['recover',digest],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=10)
        self.assertEqual(second.returncode,0);self.assertEqual(path.read_bytes(),self.engine_response)
        self.assertEqual(receipt.read_bytes(),receipt_before);self.assertEqual(source.read_bytes(),source_before)
        self.assertEqual(self.requests,3);self.assertEqual(len(self.fixture.fleet.commits),1)
        self.queue=Queue(self.root/'state',node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
        self.lease=authority_lease.AuthorityLease(self.lease_dir,authority);self.queue.authority_lease=self.lease
        self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)

    def test_interrupted_restore_is_retained_and_reported_for_inspection(self):
        # Simulate a child that published a receipt but whose result vanished
        # before the parent recorded archive_path. Only synthetic files change.
        self.service._archive_once(self.identifier)
        task=self.fixture.task(self.identifier);directory=Path(task['directory'])
        archive=directory/'all.zip';receipt=directory/'all.zip.receipt'
        archive.unlink()
        with self.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET archive_path=NULL WHERE uuid=?',(self.identifier,))
        pending=directory/'.all.zip.restore-pending';pending.write_bytes(b'owned incomplete synthetic restore')
        before=pending.read_bytes();receipt_before=receipt.read_bytes();requests_before=self.requests
        with self.assertRaisesRegex(Conflict,'partial_requires_inspection'):
            self.service._archive_once(self.identifier)
        self.assertEqual(pending.read_bytes(),before);self.assertEqual(receipt.read_bytes(),receipt_before)
        self.assertEqual(self.requests,requests_before)
        # Exercise the real background status writer and a separate worker child.
        self.fixture.config.viewer_archive_autocapture=True;self.service.start()
        deadline=time.monotonic()+10
        while self.service.info(self.identifier).get('ledgetopArchive',{}).get('state')!='inspection':
            if time.monotonic()>=deadline:self.fail('restore inspection not persisted')
            time.sleep(.05)
        self.assertEqual(self.service.info(self.identifier)['ledgetopArchive'],
                         {'state':'inspection','reason':'partial_requires_inspection'})
        self.assertEqual(pending.read_bytes(),before);self.assertEqual(receipt.read_bytes(),receipt_before)
        self.assertEqual(self.requests,requests_before)
        self.assertTrue(self.fixture.engine_for(self.identifier).tasks)

    def test_capture_status_distinguishes_processing_and_custody(self):
        self.fixture.config.viewer_archive_autocapture = True
        self.assertEqual(self.service.info(self.identifier)['ledgetopArchive']['state'], 'pending')
        self.service._archive_once(self.identifier)
        info = self.service.info(self.identifier)
        self.assertEqual(info['status']['code'], 40)
        self.assertEqual(info['ledgetopArchive'], {'state': 'recorded', 'reason': None})

    def test_recorded_is_not_verified_custody_when_bytes_missing_or_corrupt(self):
        self.fixture.config.viewer_archive_autocapture = True
        self.service._archive_once(self.identifier)
        path = Path(self.fixture.task(self.identifier)['archive_path'])
        path.write_bytes(b'corrupt')
        self.assertEqual(self.service.info(self.identifier)['ledgetopArchive']['state'], 'recorded')
        path.unlink()
        self.assertEqual(self.service.info(self.identifier)['ledgetopArchive']['state'], 'recorded')

    def test_status_cas_rejects_old_generation_and_job(self):
        store = self.service.store
        store.enable_archive_status()
        task = store.task(self.identifier)
        self.assertFalse(store.set_archive_status(self.identifier, task['generation'] + 1, task['job_id'], 'inspection', 'partial_requires_inspection'))
        self.assertFalse(store.set_archive_status(self.identifier, task['generation'], 'old-job', 'inspection', 'partial_requires_inspection'))
        self.assertEqual(store.task(self.identifier)['archive_state'], 'pending')
        store.remove(self.identifier)
        self.service.initialize(self.identifier, 'Synthetic survey', fixtures.REQUEST_OPTIONS, None, 'production')
        replacement = store.task(self.identifier)
        self.assertEqual(replacement['generation'], task['generation'] + 1)
        self.assertFalse(store.set_archive_status(self.identifier, task['generation'], task['job_id'], 'inspection', 'partial_requires_inspection'))
        self.assertEqual(store.task(self.identifier)['archive_state'], 'pending')

    def test_status_store_failure_is_visible_without_error_text(self):
        self.fixture.config.viewer_archive_autocapture = True
        self.fixture.config.viewer_archive_free_floor = 10**18
        with patch.object(self.service.store, 'set_archive_status', side_effect=RuntimeError('secret-token-for-test')):
            self.service.start()
            deadline = time.monotonic() + 10
            while self.service.info(self.identifier)['ledgetopArchiveHealth']['reason'] != 'status_persistence_failed':
                if time.monotonic() >= deadline: self.fail('health signal not raised')
                time.sleep(.05)
            info = self.service.info(self.identifier)
            self.assertEqual(info['ledgetopArchiveHealth'], {'state': 'inspection', 'reason': 'status_persistence_failed'})
            self.assertNotIn('secret-token-for-test', str(info))
            self.service.close()

    def test_discovery_capture_identity_race_does_not_touch_replacement(self):
        self.fixture.config.viewer_archive_autocapture = True
        self.service.store.enable_archive_status()
        old = self.fixture.task(self.identifier)
        original = self.service._archive_once
        def reset_before_capture(identifier, expected_identity=None):
            self.service.store.remove(identifier)
            self.service.initialize(identifier, 'Synthetic survey', fixtures.REQUEST_OPTIONS, None, 'production')
            try:
                return original(identifier, expected_identity=expected_identity)
            finally:
                self.service._stop.set()
        with patch.object(self.service, '_archive_once', side_effect=reset_before_capture):
            self.service._archive_run()
        new = self.fixture.task(self.identifier)
        self.assertEqual(new['generation'], old['generation'] + 1)
        self.assertEqual(new['archive_state'], 'pending')
        self.assertIsNone(new['archive_reason'])
        self.assertEqual(self.requests, 0)
        self.assertNotIn(self.identifier, self.service._archive_retry)

    def test_discovery_identity_read_failure_signals_health_without_stale_status_write(self):
        self.fixture.config.viewer_archive_autocapture = True
        self.service.store.enable_archive_status()
        def fail_identity(_identifier):
            self.service._stop.set()
            raise RuntimeError('private detail')
        with patch.object(self.service.store, 'task', side_effect=fail_identity), \
             patch.object(self.service.store, 'set_archive_status') as persist:
            self.service._archive_run()
        persist.assert_not_called()
        self.assertEqual(self.service._archive_health(), {'state': 'inspection', 'reason': 'status_persistence_failed'})
        self.assertNotIn('private detail', str(self.service._archive_health()))

    def test_restart_health_never_claims_failure_history_is_verified(self):
        self.fixture.config.viewer_archive_autocapture = True
        self.assertEqual(self.service.info(self.identifier)['ledgetopArchiveHealth'],
                         {'state': 'inspection', 'reason': 'status_unverified_after_restart'})
        self.service._archive_status_persistence_failed = True
        restarted = ViewerService(self.queue, self.fixture.config, engines=self.fixture.engines, probe=self.fixture.probe)
        self.assertEqual(restarted.info(self.identifier)['ledgetopArchiveHealth'],
                         {'state': 'inspection', 'reason': 'status_unverified_after_restart'})

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
