"""Synthetic POSIX coverage of bounded, receipt-backed background capture."""
import json
import errno
import os
import signal
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
    def test_unqualified_direct_service_fails_before_store_construction(self):
        with tempfile.TemporaryDirectory() as root:
            config=SimpleNamespace(prototype_state_dir=root,viewer_archive_registry=True,viewer_max_input_bytes=100)
            with patch('processing.viewer_service.TaskStore',side_effect=AssertionError('must not construct')) as store:
                with self.assertRaisesRegex(RuntimeError,'archive registry is not qualified'):
                    ViewerService(None,config)
            store.assert_not_called();self.assertEqual(list(Path(root).iterdir()),[])

    def test_direct_handler_configuration_cannot_bypass_qualification(self):
        from processing.app import make_handler
        from processing.config import Config
        config=Config('p'*32,'','',1000,100000,viewer_enabled=True,
                      authority_lease_dir='/synthetic',viewer_archive_registry=True)
        with self.assertRaisesRegex(RuntimeError,'archive_registry_requires_authority'):
            make_handler(config)

    def test_registry_cannot_be_enabled_by_environment_without_qualification(self):
        env={'PROCESSING_API_TOKEN':'p'*32,'PROCESSING_VIEWER_ARCHIVE_REGISTRY':'1'}
        with patch.dict(os.environ,env,clear=True), \
             self.assertRaisesRegex(ValueError,'archive registry is not qualified'):
            from_environment()

    def test_registry_marker_blocks_candidate_start_with_feature_disabled(self):
        from processing.app import make_handler
        from processing.config import Config
        with tempfile.TemporaryDirectory() as root:
            queue=Queue(root)
            try:
                with queue._db() as db:
                    db.execute('CREATE TABLE viewer_archive_registry (synthetic TEXT)')
                    db.execute("INSERT INTO viewer_archive_registry VALUES ('retained')")
            finally:queue.close()
            with self.assertRaisesRegex(RuntimeError,'archive_registry_configuration_required'):
                make_handler(Config('p'*32,'','',1000,100000,prototype_state_dir=root))
            queue=Queue(root)
            try:
                with queue._db() as db:
                    self.assertEqual(db.execute('SELECT synthetic FROM viewer_archive_registry').fetchone()[0],'retained')
            finally:queue.close()

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
    def allow_registry_for_test(self):
        qualification=patch('processing.config.ARCHIVE_REGISTRY_QUALIFIED',True)
        qualification.start();self.addCleanup(qualification.stop)
        self.fixture.config.viewer_archive_registry=True

    def submit_completed_for_registry_test(self):
        self.fixture.service=self.service
        self.service._catalog=lambda:{'ready':True,'commonOptions':[dict(item) for item in self.fixture.option_catalog]}
        self.service._selected_options=lambda _node:[dict(item) for item in self.fixture.option_catalog]
        identifier=self.fixture.submit()
        self.service.tick();self.fixture.finish(identifier);self.service.tick()
        return identifier
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
        self.duplicate_length = False
        self.conflicting_transfer = False
        self.engine_http_status = 200
        self.response_prefix_bytes = 0
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
                    if owner.duplicate_length:
                        self.send_header('Content-Length', str(len(owner.engine_response)))
                if owner.conflicting_transfer:
                    self.send_header('Transfer-Encoding','chunked')
                self.end_headers()
                prefix = owner.engine_response[:owner.response_prefix_bytes]
                if prefix:
                    self.wfile.write(prefix)
                    self.wfile.flush()
                owner.ready.set()
                if owner.release.wait(15):
                    try: self.wfile.write(owner.engine_response[len(prefix):])
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

    @unittest.skipUnless(hasattr(os, 'pidfd_open') and hasattr(signal, 'pidfd_send_signal'), 'Linux pidfd required')
    def test_real_killed_capture_preserves_partial_and_reports_inspection(self):
        self.release.clear()
        self.engine_response = b'S' * (1024 ** 2) + self.engine_response
        self.response_prefix_bytes = 1024 ** 2
        children = []
        pidfds = []
        errors = []
        real_popen = subprocess.Popen

        def start_child(*args, **kwargs):
            child = real_popen(*args, **kwargs)
            children.append(child)
            pidfds.append(os.pidfd_open(child.pid))
            return child

        def capture():
            try:
                self.service._archive_once(self.identifier)
            except Exception as error:
                errors.append(error)

        partial = Path(self.fixture.task(self.identifier)['directory']) / 'all.zip.partial'
        thread = threading.Thread(target=capture)
        with patch('processing.viewer_service.subprocess.Popen', side_effect=start_child):
            thread.start()
            try:
                self.assertTrue(self.ready.wait(10), 'child did not contact synthetic engine')
                deadline = time.monotonic() + 10
                prefix = self.engine_response[:self.response_prefix_bytes]
                while not partial.exists() or not partial.read_bytes().startswith(prefix):
                    if time.monotonic() >= deadline:
                        self.fail('child did not persist synthetic response prefix')
                    time.sleep(.02)
                self.assertEqual(len(children), 1)
                before = partial.read_bytes()
                signal.pidfd_send_signal(pidfds[0], signal.SIGKILL)
                thread.join(10)
                self.assertFalse(thread.is_alive(), 'parent did not reap killed capture child')
                self.assertEqual(children[0].returncode, -9)
                self.assertEqual(len(errors), 1)
                self.assertIsInstance(errors[0], Conflict)
                self.assertEqual(str(errors[0]), 'partial_requires_inspection')
                self.assertEqual(partial.read_bytes(), before)
                self.assertIsNone(self.fixture.task(self.identifier)['archive_path'])
                self.assertTrue(self.fixture.engine_for(self.identifier).tasks)
            finally:
                self.release.set()
                if thread.is_alive():
                    self.service._stop.set()
                    thread.join(10)
                for pidfd in pidfds:
                    os.close(pidfd)
                self.assertFalse(thread.is_alive(), 'capture thread survived cleanup')

    def test_manual_and_automatic_capture_serialize_for_same_task(self):
        entered=threading.Event();release=threading.Event();auto_done=threading.Event();errors=[]
        engine=self.fixture.engines['cluster'];original=engine.download
        def blocked(*args):
            entered.set()
            if not release.wait(5):raise RuntimeError('fixture timed out')
            return original(*args)
        def manual():
            try:
                stream,_=self.service.download(self.identifier)
                with stream:stream.read()
            except Exception as error:errors.append(error)
        def automatic():
            try:self.service._archive_once(self.identifier)
            except Exception as error:errors.append(error)
            finally:auto_done.set()
        first=threading.Thread(target=manual);second=threading.Thread(target=automatic)
        with patch.object(engine,'download',side_effect=blocked), patch('processing.viewer_service.subprocess.Popen') as spawn:
            try:
                first.start();self.assertTrue(entered.wait(3));second.start()
                self.assertFalse(auto_done.wait(.2));spawn.assert_not_called()
            finally:
                release.set();first.join(8)
                if second.ident is not None:second.join(8)
            self.assertFalse(first.is_alive());self.assertFalse(second.is_alive())
            self.assertEqual(errors,[]);spawn.assert_not_called()

    def test_auto_child_captures_while_later_upload_is_blocked(self):
        later=self.fixture.submit();entered=threading.Event();release=threading.Event();errors=[]
        engine=self.fixture.engines['cluster'];original=engine.upload
        def upload(*args):
            entered.set()
            if not release.wait(15):raise RuntimeError('fixture upload timed out')
            return original(*args)
        def run(call):
            try:call()
            except Exception as error:errors.append(error)
        dispatch=threading.Thread(target=run,args=(self.service.tick,))
        capture=threading.Thread(target=run,args=(lambda:self.service._archive_once(self.identifier),))
        with patch.object(engine,'upload',side_effect=upload):
            try:
                dispatch.start();self.assertTrue(entered.wait(5));capture.start()
                capture.join(10);self.assertFalse(capture.is_alive(),'capture blocked behind upload')
                self.assertEqual(errors,[])
                archived=self.fixture.task(self.identifier)
                self.assertEqual(Path(archived['archive_path']).read_bytes(),self.engine_response)
                self.assertEqual(self.fixture.task(later)['job']['status'],'running')
            finally:
                release.set();dispatch.join(15)
                if capture.ident is not None:capture.join(15)
            self.assertFalse(dispatch.is_alive());self.assertFalse(capture.is_alive());self.assertEqual(errors,[])

    def test_old_capture_cannot_publish_to_recreated_task(self):
        old=self.fixture.task(self.identifier);errors=[]
        self.release.clear()
        def capture():
            try:self.service._archive_once(self.identifier)
            except Exception as error:errors.append(error)
        thread=threading.Thread(target=capture)
        try:
            thread.start();self.assertTrue(self.ready.wait(5))
            self.service.remove(self.identifier)
            self.service.initialize(self.identifier,'Synthetic survey',fixtures.REQUEST_OPTIONS,None,'production')
            replacement=self.fixture.task(self.identifier)
            self.assertNotEqual(old['generation'],replacement['generation'])
        finally:
            self.release.set();thread.join(15)
        self.assertFalse(thread.is_alive());self.assertEqual(len(errors),1)
        self.assertIsInstance(errors[0],Conflict)
        self.assertIsNone(self.fixture.task(self.identifier)['archive_path'])
        self.assertEqual((Path(old['directory'])/'all.zip').read_bytes(),self.engine_response)
        self.assertTrue((Path(old['directory'])/'all.zip.receipt').is_file())

    def test_unreaped_capture_child_prevents_clean_shutdown(self):
        class Input:
            closed=False
            def fileno(self):return 42
            def close(self):self.closed=True
        class Child:
            def __init__(self):self.stdin=Input();self.killed=False
            def poll(self):return None
            def kill(self):self.killed=True
            def wait(self,timeout):raise subprocess.TimeoutExpired('synthetic-child',timeout)
        child=Child()
        try:
            with patch('processing.viewer_service.subprocess.Popen',return_value=child), \
                 patch('processing.viewer_service.os.set_blocking',side_effect=OSError('synthetic input failure')):
                with self.assertRaisesRegex(RuntimeError,'child termination uncertain'):
                    self.service._archive_once(self.identifier)
            self.assertTrue(child.killed)
            self.assertIs(self.service._archive_uncertain_process,child)
            with self.assertRaisesRegex(Conflict,'child termination uncertain'):
                self.service.download(self.identifier)
            self.assertEqual(self.requests,0)
            self.assertFalse((Path(self.fixture.task(self.identifier)['directory'])/'all.zip.partial').exists())
            with patch('processing.viewer_service.subprocess.Popen',side_effect=AssertionError('must not spawn')):
                with self.assertRaisesRegex(RuntimeError,'child termination uncertain'):
                    self.service._archive_once(self.identifier)
            with self.assertRaisesRegex(RuntimeError,'child termination uncertain'):
                self.service.close()
            self.assertIsNone(self.fixture.task(self.identifier)['archive_path'])
        finally:
            # Only a fake child was used; clear its fixture-only sticky marker.
            self.service._archive_child_unreaped.clear()
            self.service._archive_uncertain_process=None

    def test_registry_precedes_markers_and_marker_loss_holds_after_restart(self):
        self.allow_registry_for_test()
        self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
        task=self.fixture.task(self.identifier);directory=Path(task['directory'])
        with patch.object(self.service,'_publish_archive_metadata',side_effect=OSError('synthetic crash boundary')):
            with self.assertRaises(OSError):self.service.download(self.identifier,automatic=True)
        with self.queue._db() as db:
            row=db.execute('SELECT uuid,generation,job_id,node_id,engine_uuid,environment FROM viewer_archive_registry').fetchone()
        self.assertEqual(tuple(row),tuple(task[k] for k in ('uuid','generation','job_id','node_id','engine_uuid','environment')))
        self.assertFalse((directory/'all.zip.intent').exists())
        self.service._archive_once(self.identifier)
        archive=Path(self.fixture.task(self.identifier)['archive_path']);before=archive.read_bytes()
        archive.with_suffix('.zip.intent').unlink();archive.with_suffix('.zip.receipt').unlink()
        self.service.close()
        self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
        with self.assertRaisesRegex(Conflict,'archive receipt requires inspection'):
            self.service.download(self.identifier)
        self.assertEqual(archive.read_bytes(),before)

    def test_registry_holds_unclassified_historical_recorded_archive(self):
        self.service._archive_once(self.identifier)
        archive=Path(self.fixture.task(self.identifier)['archive_path']);before=archive.read_bytes()
        self.allow_registry_for_test()
        self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
        with self.assertRaisesRegex(Conflict,'historical archive format requires inspection'):
            self.service.download(self.identifier)
        self.assertEqual(archive.read_bytes(),before)

    def test_registry_commits_before_marker_even_if_process_is_killed(self):
        import signal
        self.allow_registry_for_test()
        task=self.fixture.task(self.identifier);directory=Path(task['directory']);authority=self.queue.authority_id
        self.service.close();self.queue.close();self.lease.close()
        script=r'''
import os,signal,sys
from types import SimpleNamespace
from unittest.mock import patch
from processing.authority_lease import AuthorityLease
from processing.prototype import Queue
from processing.viewer_service import ViewerService
import processing.config as runtime_config
runtime_config.ARCHIVE_REGISTRY_QUALIFIED=True
root,lease_dir,authority,identifier,origin=sys.argv[1:]
q=Queue(root,node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
q.authority_lease=AuthorityLease(lease_dir,authority)
c=SimpleNamespace(prototype_state_dir=root,viewer_max_input_bytes=20*1024**3,viewer_max_archive_bytes=4*1024**2,viewer_archive_free_floor=1,viewer_archive_registry=True,local_node_url=origin,local_node_token='',main_node_url=origin,main_node_token='')
s=ViewerService(q,c)
with patch.object(s,'_publish_archive_metadata',side_effect=lambda *_:os.kill(os.getpid(),signal.SIGKILL)):
 s.download(identifier,automatic=True)
'''
        try:
            result=subprocess.run([sys.executable,'-c',script,str(self.root/'state'),str(self.lease_dir),authority,self.identifier,self.fixture.config.local_node_url],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=10)
            self.assertEqual(result.returncode,-signal.SIGKILL)
        finally:
            self.queue=Queue(self.root/'state',node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
            self.lease=authority_lease.AuthorityLease(self.lease_dir,authority);self.queue.authority_lease=self.lease
            self.fixture.config.viewer_archive_registry=True
            self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
        self.assertFalse((directory/'all.zip.intent').exists())
        self.assertFalse((directory/'all.zip.partial').exists())
        with self.queue._db() as db:
            row=db.execute('SELECT uuid,generation,job_id,node_id,engine_uuid,environment FROM viewer_archive_registry').fetchone()
        self.assertEqual(tuple(row),tuple(task[k] for k in ('uuid','generation','job_id','node_id','engine_uuid','environment')))
        self.service._archive_once(self.identifier)
        self.assertEqual(Path(self.service.store.task(self.identifier)['archive_path']).read_bytes(),self.engine_response)

    def test_registry_refuses_old_identity_after_uuid_reincarnation(self):
        self.allow_registry_for_test()
        self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
        old=self.service.store.task(self.identifier)
        self.service.store.archive_registry_check(old,register=True)
        self.service.remove(self.identifier)
        with patch.object(self.service,'_check_options',return_value=[]):
            self.service.initialize(self.identifier,'Synthetic survey',[],None,'production')
        with self.assertRaises(Conflict):self.service.store.archive_registry_check(old,register=True)
        with self.queue._db() as db:
            rows=db.execute('SELECT generation,job_id FROM viewer_archive_registry').fetchall()
        self.assertEqual([tuple(row) for row in rows],[(old['generation'],old['job_id'])])

    def test_registry_concurrent_exact_registration_is_idempotent(self):
        self.allow_registry_for_test()
        self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
        task=self.service.store.task(self.identifier);barrier=threading.Barrier(3);results=[];errors=[]
        def register():
            try:
                barrier.wait(timeout=3);results.append(self.service.store.archive_registry_check(task,register=True))
            except Exception as exc:errors.append(type(exc).__name__)
        threads=[threading.Thread(target=register) for _ in range(2)]
        for thread in threads:thread.start()
        try:
            barrier.wait(timeout=3)
            for thread in threads:thread.join(timeout=3)
            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual(errors,[]);self.assertEqual(results,[True,True])
            with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_archive_registry').fetchone()[0],1)
        finally:
            barrier.abort()
            for thread in threads:thread.join(timeout=3)

    def test_registry_capacity_preserves_recorded_and_unknown_after_restart(self):
        self.service._archive_once(self.identifier)
        historical=self.service.store.task(self.identifier);historical_path=Path(historical['archive_path']);historical_bytes=historical_path.read_bytes()
        self.allow_registry_for_test()
        self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
        with patch('processing.viewer_tasks.ARCHIVE_REGISTRY_LIMIT',1):
            recorded_id=self.submit_completed_for_registry_test()
            self.service._archive_once(recorded_id)
            recorded=self.service.store.task(recorded_id);recorded_path=Path(recorded['archive_path']);recorded_bytes=recorded_path.read_bytes()
            rejected_id=self.submit_completed_for_registry_test()
            rejected=self.service.store.task(rejected_id);directory=Path(rejected['directory'])
            with self.assertRaisesRegex(Conflict,'registry capacity reached'):
                self.service.download(rejected_id,automatic=True)
            self.assertFalse((directory/'all.zip.intent').exists());self.assertFalse((directory/'all.zip.partial').exists())
            self.service.close()
            self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
            with self.service.download(recorded_id)[0] as stream:self.assertEqual(stream.read(),recorded_bytes)
            with self.assertRaisesRegex(Conflict,'historical archive format requires inspection'):
                self.service.download(self.identifier)
            with self.assertRaisesRegex(Conflict,'registry capacity reached'):
                self.service.download(rejected_id,automatic=True)
            self.assertEqual(historical_path.read_bytes(),historical_bytes)
            self.assertEqual(recorded_path.read_bytes(),recorded_bytes)
            with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_archive_registry').fetchone()[0],1)
            authority=self.queue.authority_id
            self.service.close();self.queue.close();self.lease.close()
            script=r'''
import hashlib,sys
from types import SimpleNamespace
import processing.config as config
import processing.viewer_tasks as tasks
from processing.authority_lease import AuthorityLease
from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.viewer_service import ViewerService
root,lease_dir,authority,recorded,historical,rejected,expected=sys.argv[1:]
config.ARCHIVE_REGISTRY_QUALIFIED=True;tasks.ARCHIVE_REGISTRY_LIMIT=1
q=Queue(root,node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
q.authority_lease=AuthorityLease(lease_dir,authority)
c=SimpleNamespace(prototype_state_dir=root,viewer_max_input_bytes=20*1024**3,viewer_max_archive_bytes=4*1024**2,viewer_archive_free_floor=1,viewer_archive_registry=True,local_node_url='http://127.0.0.1:1',local_node_token='',main_node_url='http://127.0.0.1:2',main_node_token='')
s=ViewerService(q,c)
try:
 with s.download(recorded)[0] as stream:
  assert hashlib.sha256(stream.read()).hexdigest()==expected
 for identifier,reason in ((historical,'historical archive format requires inspection'),(rejected,'archive registry capacity reached')):
  try:s.download(identifier,automatic=True)
  except Conflict as error:assert str(error)==reason
  else:raise AssertionError('expected hold')
finally:s.close();q.authority_lease.close();q.close()
'''
            try:
                import hashlib
                result=subprocess.run([sys.executable,'-c',script,str(self.root/'state'),str(self.lease_dir),authority,recorded_id,self.identifier,rejected_id,hashlib.sha256(recorded_bytes).hexdigest()],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=10)
                self.assertEqual(result.returncode,0)
            finally:
                self.queue=Queue(self.root/'state',node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
                self.lease=authority_lease.AuthorityLease(self.lease_dir,authority);self.queue.authority_lease=self.lease
                self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)

    def test_registry_last_capacity_slot_is_transactionally_exclusive(self):
        self.allow_registry_for_test()
        self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
        second=self.submit_completed_for_registry_test()
        tasks=[self.service.store.task(identifier) for identifier in (self.identifier,second)]
        barrier=threading.Barrier(3);results=[]
        def register(task):
            try:
                barrier.wait(timeout=3);self.service.store.archive_registry_check(task,register=True);results.append('inserted')
            except Conflict as exc:results.append(str(exc))
        with patch('processing.viewer_tasks.ARCHIVE_REGISTRY_LIMIT',1):
            threads=[threading.Thread(target=register,args=(task,)) for task in tasks]
            for thread in threads:thread.start()
            try:
                barrier.wait(timeout=3)
                for thread in threads:thread.join(timeout=3)
                self.assertFalse(any(thread.is_alive() for thread in threads))
                self.assertCountEqual(results,['inserted','archive registry capacity reached'])
                with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_archive_registry').fetchone()[0],1)
            finally:
                barrier.abort()
                for thread in threads:thread.join(timeout=3)

    def test_existing_entries_remain_usable_when_already_over_limit(self):
        self.allow_registry_for_test()
        self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
        second=self.submit_completed_for_registry_test()
        tasks=[self.service.store.task(identifier) for identifier in (self.identifier,second)]
        for task in tasks:self.assertTrue(self.service.store.archive_registry_check(task,register=True))
        with patch('processing.viewer_tasks.ARCHIVE_REGISTRY_LIMIT',1):
            for task in tasks:self.assertTrue(self.service.store.archive_registry_check(task,register=True))
            self.service._archive_once(self.identifier)
        archive=Path(self.service.store.task(self.identifier)['archive_path'])
        self.assertEqual(archive.read_bytes(),self.engine_response)
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_archive_registry').fetchone()[0],2)

    def test_invalid_new_registry_identity_is_not_inserted(self):
        self.allow_registry_for_test()
        self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
        task=self.service.store.task(self.identifier)
        oversized=dict(task,generation=2**63)
        with self.assertRaises(Conflict):self.service.store.archive_registry_check(oversized,register=True)
        with self.queue._db() as db:
            original=db.execute('SELECT result FROM jobs WHERE id=?',(task['job_id'],)).fetchone()[0]
            result=json.loads(original);result['engineTaskId']='not-a-uuid'
            db.execute('UPDATE viewer_tasks SET engine_uuid=? WHERE uuid=?',('not-a-uuid',self.identifier))
            db.execute('UPDATE jobs SET result=? WHERE id=?',(json.dumps(result,sort_keys=True,separators=(',',':')),task['job_id']))
        try:
            invalid=self.service.store.task(self.identifier)
            with self.assertRaisesRegex(Conflict,'identity requires inspection'):
                self.service.store.archive_registry_check(invalid,register=True)
            with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_archive_registry').fetchone()[0],0)
        finally:
            with self.queue._db() as db:
                db.execute('UPDATE viewer_tasks SET engine_uuid=? WHERE uuid=?',(task['engine_uuid'],self.identifier))
                db.execute('UPDATE jobs SET result=? WHERE id=?',(original,task['job_id']))

    def test_registry_rejects_unrecognized_schema_without_mutation(self):
        with self.queue._db() as db:
            db.execute('CREATE TABLE viewer_archive_registry (unexpected TEXT)')
            db.execute("INSERT INTO viewer_archive_registry VALUES ('synthetic')")
        self.allow_registry_for_test()
        with self.assertRaisesRegex(Conflict,'schema requires inspection'):
            ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT unexpected FROM viewer_archive_registry').fetchone()[0],'synthetic')

    def test_registry_rejects_mismatched_terminal_job_without_insertion(self):
        self.allow_registry_for_test()
        self.service=ViewerService(self.queue,self.fixture.config,engines=self.fixture.engines,probe=self.fixture.probe)
        task=self.service.store.task(self.identifier)
        with self.queue._db() as db:
            original=db.execute('SELECT result FROM jobs WHERE id=?',(task['job_id'],)).fetchone()[0]
            db.execute('UPDATE jobs SET result=? WHERE id=?',('{"engineTaskId":"wrong"}',task['job_id']))
        try:
            with patch('processing.viewer_service.subprocess.Popen',side_effect=AssertionError('must not launch')):
                with self.assertRaisesRegex(Conflict,'job binding requires inspection'):
                    self.service._archive_once(self.identifier)
            with self.queue._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_archive_registry').fetchone()[0],0)
        finally:
            with self.queue._db() as db:db.execute('UPDATE jobs SET result=? WHERE id=?',(original,task['job_id']))

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

    def test_automatic_capture_rejects_ambiguous_framing_before_partial_and_retries(self):
        directory=Path(self.fixture.task(self.identifier)['directory'])
        for setting in ('duplicate_length','conflicting_transfer'):
            with self.subTest(setting=setting):
                setattr(self,setting,True)
                with self.assertRaises(Exception):self.service._archive_once(self.identifier)
                self.assertFalse((directory/'all.zip.partial').exists())
                self.assertFalse((directory/'all.zip.receipt').exists())
                self.assertIsNone(self.fixture.task(self.identifier)['archive_path'])
                setattr(self,setting,False)
        self.service._archive_once(self.identifier)
        self.assertEqual(Path(self.fixture.task(self.identifier)['archive_path']).read_bytes(),self.engine_response)
        self.assertEqual(self.requests,3)

    def test_preallocation_enospc_keeps_partial_and_refuses_replacement(self):
        with patch('processing.viewer_service.os.posix_fallocate',side_effect=OSError(errno.ENOSPC,'synthetic')):
            with self.assertRaises(OSError):self.service.download(self.identifier,automatic=True)
        task=self.fixture.task(self.identifier);directory=Path(task['directory'])
        partial=directory/'all.zip.partial';before=partial.read_bytes();identity=partial.stat().st_ino
        self.assertFalse((directory/'all.zip.receipt').exists())
        self.assertIsNone(task['archive_path'])
        with self.assertRaises(Exception):self.service._archive_once(self.identifier)
        self.assertEqual(partial.stat().st_ino,identity);self.assertEqual(partial.read_bytes(),before)
        self.assertFalse((directory/'all.zip.receipt').exists())
        self.assertTrue(self.fixture.engine_for(self.identifier).tasks)

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
        old_due=time.monotonic()+60
        self.service._archive_retry[self.identifier]=(identity,old_due,failures)
        # Keep the documented unknown-identity remove/recreate gap out of this
        # test: here the scheduler must see the replacement's known identity.
        # Both locks are reentrant and follow the service's normal lock order.
        with self.service._lock, self.service.store._lock:
            self.service.remove(self.identifier)
            self.service.initialize(self.identifier,'Synthetic survey',fixtures.REQUEST_OPTIONS,None,'production')
        self.fixture.upload(self.identifier);self.service.commit(self.identifier)
        self.engine_http_status=200;deadline=time.monotonic()+10
        while True:
            current=self.fixture.task(self.identifier);node=current['node_id'];engine_id=current['engine_uuid']
            if (node is not None and engine_id in self.fixture.engines[node].tasks
                    and current['phase']=='processing' and current['job']['status']=='running'):break
            if time.monotonic()>=deadline:self.fail('replacement did not dispatch')
            time.sleep(.02)
        self.fixture.finish(self.identifier)
        # Dispatch and capture are distinct asynchronous phases; retain a
        # full bounded window for capture after the engine reports completion.
        # This is still well before the 60-second old-generation deadline.
        deadline=time.monotonic()+10
        while self.fixture.task(self.identifier)['archive_path'] is None:
            if time.monotonic()>=deadline:
                current=self.fixture.task(self.identifier)
                self.fail('replacement capture timed out: phase=%s job=%s archive=%s retry=%r requests=%s archive_thread_alive=%s' % (
                    current['phase'],current['job']['status'],current.get('archive_state'),
                    self.service._archive_retry.get(self.identifier),self.requests,
                    bool(self.service._archive_thread and self.service._archive_thread.is_alive())))
            time.sleep(.05)
        new=self.fixture.task(self.identifier)
        self.assertLess(time.monotonic(),old_due,'replacement capture must precede old retry')
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

    def test_automatic_restore_rejects_ambiguous_framing_without_slot(self):
        self.service._archive_once(self.identifier)
        task=self.fixture.task(self.identifier);directory=Path(task['directory'])
        archive=directory/'all.zip';receipt=directory/'all.zip.receipt';before=receipt.read_bytes()
        archive.unlink()
        with self.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET archive_path=NULL WHERE uuid=?',(self.identifier,))
        self.duplicate_length=True
        with self.assertRaises(Exception):self.service._archive_once(self.identifier)
        self.assertFalse((directory/'.all.zip.restore-pending').exists())
        self.assertEqual(receipt.read_bytes(),before)
        self.duplicate_length=False
        self.service._archive_once(self.identifier)
        self.assertEqual(archive.read_bytes(),self.engine_response)
        self.assertEqual(receipt.read_bytes(),before)

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

    def test_inspection_hold_excludes_capture_and_stale_scan_after_restart(self):
        store = self.service.store
        store.enable_archive_status()
        task = store.task(self.identifier)
        selected = store.next_unarchived_success()
        self.assertIsNotNone(selected)
        self.assertEqual(selected[1], self.identifier)
        partial = Path(task['directory']) / 'all.zip.partial'
        partial.write_bytes(b'synthetic interrupted capture')
        self.assertTrue(store.set_archive_status(self.identifier, task['generation'], task['job_id'],
                                                 'inspection', 'partial_requires_inspection'))
        self.assertIsNone(store.next_unarchived_success())
        self.assertIsNone(store.next_unarchived_success(selected[0]))
        other = self.submit_completed_for_registry_test()
        self.assertEqual(store.next_unarchived_success()[1], other)
        self.assertEqual(store.next_unarchived_success(selected[0])[1], other)
        # A stale scan result also cannot start a child after the hold.
        with patch('processing.viewer_service.subprocess.Popen') as spawn:
            with self.assertRaisesRegex(Conflict, 'archive requires inspection'):
                self.service._archive_once(self.identifier, expected_identity=(task['generation'], task['job_id']))
            spawn.assert_not_called()
        actual_once = self.service._archive_once
        def stale_attempt(*args, **kwargs):
            try:
                return actual_once(*args, **kwargs)
            finally:
                self.service._stop.set()
        with patch.object(store, 'next_unarchived_success', return_value=selected), \
             patch.object(self.service, '_archive_once', side_effect=stale_attempt):
            self.service._archive_run()
        self.assertEqual(store.task(self.identifier)['archive_state'], 'inspection')
        self.assertEqual(store.task(self.identifier)['archive_reason'], 'partial_requires_inspection')
        self.assertNotIn(self.identifier, self.service._archive_retry)
        self.service.close()
        restarted = ViewerService(self.queue, self.fixture.config, engines=self.fixture.engines, probe=self.fixture.probe)
        try:
            self.assertEqual(restarted.store.next_unarchived_success()[1], other)
            with patch('processing.viewer_service.subprocess.Popen') as spawn:
                with self.assertRaisesRegex(Conflict, 'archive requires inspection'):
                    restarted._archive_once(self.identifier, expected_identity=(task['generation'], task['job_id']))
                spawn.assert_not_called()
            self.assertEqual(restarted.store.task(self.identifier)['archive_state'], 'inspection')
            self.assertEqual(partial.read_bytes(), b'synthetic interrupted capture')
        finally:
            restarted.close()

    def test_manual_download_cannot_resolve_inspection_even_with_matching_receipt(self):
        self.service._archive_once(self.identifier)
        store = self.service.store
        store.enable_archive_status()
        task = store.task(self.identifier)
        archive = Path(task['archive_path'])
        partial = archive.with_suffix('.zip.partial')
        receipt = archive.with_suffix('.zip.receipt')
        archive.rename(partial)
        expected, expected_receipt = partial.read_bytes(), receipt.read_bytes()
        with self.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET archive_path=NULL WHERE uuid=?', (self.identifier,))
        self.assertTrue(store.set_archive_status(self.identifier, task['generation'], task['job_id'],
                                                 'inspection', 'partial_requires_inspection'))
        with self.assertRaisesRegex(Conflict, 'archive requires inspection'):
            self.service.download(self.identifier)
        self.assertEqual(partial.read_bytes(), expected)
        self.assertEqual(receipt.read_bytes(), expected_receipt)
        self.assertFalse(archive.exists())
        self.assertIsNone(store.task(self.identifier)['archive_path'])

    def test_failed_capture_persists_hold_before_competing_download(self):
        store = self.service.store
        store.enable_archive_status()
        candidate = store.next_unarchived_success()
        task = store.task(self.identifier)
        partial = Path(task['directory']) / 'all.zip.partial'
        partial.write_bytes(b'synthetic interrupted capture')
        at_persist, allow_persist, download_done = threading.Event(), threading.Event(), threading.Event()
        result = []
        original_set = store.set_archive_status
        def pause_persist(*args, **kwargs):
            at_persist.set()
            if not allow_persist.wait(5):
                raise RuntimeError('test persistence wait timed out')
            try:
                return original_set(*args, **kwargs)
            finally:
                self.service._stop.set()
        def compete():
            try:
                self.service.download(self.identifier)
                result.append('unexpected success')
            except Exception as exc:
                result.append(exc)
            finally:
                download_done.set()
        with patch.object(store, 'next_unarchived_success', return_value=candidate), \
             patch.object(store, 'set_archive_status', side_effect=pause_persist), \
             patch.object(self.service, '_archive_once', side_effect=Conflict('partial_requires_inspection')):
            worker = threading.Thread(target=self.service._archive_run)
            worker.start()
            competitor = None
            try:
                self.assertTrue(at_persist.wait(5))
                competitor = threading.Thread(target=compete)
                competitor.start()
                self.assertFalse(download_done.wait(.2), 'download passed an unpersisted hold')
                allow_persist.set()
                self.assertTrue(download_done.wait(5))
            finally:
                allow_persist.set()
                worker.join(5)
                if competitor is not None:
                    competitor.join(5)
            self.assertFalse(worker.is_alive())
            self.assertFalse(competitor.is_alive())
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], Conflict)
        self.assertEqual(str(result[0]), 'archive requires inspection')
        self.assertEqual(store.task(self.identifier)['archive_state'], 'inspection')
        self.assertEqual(partial.read_bytes(), b'synthetic interrupted capture')

    def test_failed_inspection_persistence_holds_download_in_process(self):
        store = self.service.store
        store.enable_archive_status()
        candidate = store.next_unarchived_success()
        def fail_status(*args, **kwargs):
            self.service._stop.set()
            raise OSError('synthetic database failure')
        with patch.object(store, 'next_unarchived_success', return_value=candidate), \
             patch.object(store, 'set_archive_status', side_effect=fail_status), \
             patch.object(self.service, '_archive_once', side_effect=Conflict('partial_requires_inspection')):
            self.service._archive_run()
        self.assertEqual(store.task(self.identifier)['archive_state'], 'pending')
        self.assertTrue(self.service._archive_status_persistence_failed)
        with self.assertRaisesRegex(Conflict, 'archive requires inspection'):
            self.service.download(self.identifier)

    def test_status_setter_uses_same_task_lock_as_download(self):
        store = self.service.store
        store.enable_archive_status()
        task = store.task(self.identifier)
        entered, completed = threading.Event(), threading.Event()
        outcomes = []
        def set_status():
            entered.set()
            try:
                outcomes.append(store.set_archive_status(self.identifier, task['generation'], task['job_id'],
                                                         'inspection', 'partial_requires_inspection'))
            finally:
                completed.set()
        with self.service._archive_task_lock(self.identifier):
            setter = threading.Thread(target=set_status)
            setter.start()
            self.assertTrue(entered.wait(5))
            self.assertFalse(completed.wait(.2))
        setter.join(5)
        self.assertFalse(setter.is_alive())
        self.assertEqual(outcomes, [True])

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

    def test_unknown_identity_backoff_defers_discovery_until_retry(self):
        self.fixture.config.viewer_archive_autocapture=True
        self.service.store.enable_archive_status()
        due=time.monotonic()+60
        self.service._archive_retry[self.identifier]=(None,due,6)
        def stop_after_wait(_seconds):
            self.service._stop.set();return True
        with patch.object(self.service.store,'task',wraps=self.service.store.task) as read_task, \
             patch.object(self.service._stop,'wait',side_effect=stop_after_wait):
            self.service._archive_run()
        read_task.assert_not_called()
        self.assertEqual(self.service._archive_retry[self.identifier],(None,due,6))
        self.assertEqual(self.requests,0)

    def test_remove_gap_unknown_identity_defers_replacement_without_capture(self):
        self.service.store.enable_archive_status()
        old=self.service.store.task(self.identifier)
        self.service._archive_retry[self.identifier]=((old['generation'],old['job_id']),time.monotonic()-1,1)
        def stop_after_wait(_seconds):
            self.service._stop.set();return True
        # An identity read failing during the gap replaces the old known
        # incarnation's retry with an explicitly unknown identity delay.
        with patch.object(self.service.store,'next_unarchived_success',return_value=(1,self.identifier)), \
             patch.object(self.service.store,'task',side_effect=KeyError('synthetic gap')), \
             patch.object(self.service._stop,'wait',side_effect=stop_after_wait):
            self.service._archive_run()
        retry=self.service._archive_retry[self.identifier]
        self.assertIsNone(retry[0]);self.assertGreater(retry[1],time.monotonic())
        # Keep the observed unknown-identity state stable across slow test
        # scheduling; this adjusts only the fixture's process-local deadline.
        retry=(None,time.monotonic()+60,retry[2])
        self.service._archive_retry[self.identifier]=retry
        self.service.remove(self.identifier)
        with patch.object(self.service,'_check_options',return_value=[]):
            self.service.initialize(self.identifier,'replacement',[],None,'production')
        replacement=self.service.store.task(self.identifier)
        self.assertEqual(replacement['generation'],old['generation']+1)
        self.service._stop.clear()
        with patch.object(self.service.store,'next_unarchived_success',return_value=(1,self.identifier)), \
             patch.object(self.service.store,'task',wraps=self.service.store.task) as read_task, \
             patch.object(self.service,'_archive_once') as capture, \
             patch.object(self.service._stop,'wait',side_effect=stop_after_wait):
            self.service._archive_run()
        read_task.assert_not_called();capture.assert_not_called()
        self.assertEqual(self.service._archive_retry[self.identifier],retry)
        self.assertEqual(self.requests,0)

    def test_deferred_backlog_does_not_delay_later_real_capture(self):
        self.service.store.enable_archive_status()
        delayed=['synthetic-deferred-'+str(i) for i in range(128)]
        due=time.monotonic()+60
        for identifier in delayed:
            self.service._archive_retry[identifier]=(None,due,1)
        candidates=delayed+[self.identifier]
        def next_candidate(cursor):
            index=cursor if cursor < len(candidates) else 0
            return index+1,candidates[index]
        capture=self.service._archive_once
        def capture_and_stop(identifier,expected_identity=None):
            try:return capture(identifier,expected_identity=expected_identity)
            finally:self.service._stop.set()
        waits=[]
        original_wait=self.service._stop.wait
        def wait(seconds):
            waits.append(seconds)
            return original_wait(seconds)
        with patch.object(self.service.store,'next_unarchived_success',side_effect=next_candidate), \
             patch.object(self.service,'_archive_once',side_effect=capture_and_stop), \
             patch.object(self.service._stop,'wait',side_effect=wait):
            self.service._archive_run()
        self.assertEqual(Path(self.fixture.task(self.identifier)['archive_path']).read_bytes(),self.engine_response)
        self.assertEqual(self.requests,1)
        self.assertEqual([x for x in waits if x == .1],[.1,.1])
        self.assertTrue(all(self.service._archive_retry[i]==(None,due,1) for i in delayed))

    def test_all_deferred_scan_waits_after_wrap_without_attempts(self):
        candidates=['delay-'+str(i) for i in range(130)];due=time.monotonic()+60
        for identifier in candidates:self.service._archive_retry[identifier]=(None,due,1)
        visits=[];waits=[]
        def next_candidate(cursor):
            index=cursor if cursor < len(candidates) else 0
            visits.append(candidates[index]);return index+1,candidates[index]
        def stop_on_wait(seconds):
            waits.append(seconds)
            if seconds==.25:self.service._stop.set();return True
            return False
        with patch.object(self.service.store,'next_unarchived_success',side_effect=next_candidate), \
             patch.object(self.service,'_archive_once') as capture, \
             patch.object(self.service._stop,'wait',side_effect=stop_on_wait):
            self.service._archive_run()
        self.assertEqual(visits,candidates+[candidates[0]])
        self.assertEqual(waits,[.1,.1,.25]);capture.assert_not_called()
        self.assertTrue(all(self.service._archive_retry[i]==(None,due,1) for i in candidates))

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
        self.assertIsNone(self.service._archive_retry[self.identifier][0])
        self.assertGreater(self.service._archive_retry[self.identifier][1],time.monotonic())
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
