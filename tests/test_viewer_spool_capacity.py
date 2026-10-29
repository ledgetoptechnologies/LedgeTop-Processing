"""Small real streams and conservative accounting; no client data or cap lift."""
from contextlib import closing
from email.message import Message
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.viewer_http import ViewerFacade
from processing.viewer_service import ViewerService
from processing.viewer_spool_capacity import CONTROL_ALLOWANCE,SpoolCapacity,SpoolPolicy,startup_guard


class Handler:
    def __init__(self,body):
        self.rfile=io.BytesIO(body);self.wfile=io.BytesIO();self.headers=Message()
        self.headers['Content-Length']=str(len(body))
        self.headers['Content-Type']='multipart/form-data; boundary=owned'
        self.status=None
    def send_response(self,status):self.status=status
    def send_header(self,*args):pass
    def end_headers(self):pass


class SpoolCapacityTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)/'state'
        self.queue=Queue(self.root,viewer_enabled=True);self.addCleanup(self.queue.close)
        self.spool=self.root/'viewer-spool';self.spool.mkdir(mode=0o700)
        self.tasks=self.root/'viewer-tasks'
        config=SimpleNamespace(prototype_state_dir=str(self.root),viewer_max_input_bytes=8)
        self.service=ViewerService(self.queue,config,engines={'cluster':object(),'main':object()})
        self.addCleanup(self.service.close)
        self.available=CONTROL_ALLOWANCE+100000
        self.total=self.available
        self.policy=SpoolPolicy(100000,1,2,20)
        self.identifier=str(uuid.uuid4())
        self.service.store.initialize(self.identifier,'',[],None,'staging')
        self.capacity=self.make_capacity()
        self.service.enable_spool_admission(self.capacity)
        self.service._catalog=lambda:dict(ready=True,commonOptions=[])

    def make_capacity(self):
        return SpoolCapacity(self.queue,self.root,self.spool,self.tasks,self.policy,
            probe=lambda path:(self.total,self.available))

    def rows(self):
        with closing(sqlite3.connect(self.capacity.path)) as db:
            return db.execute('SELECT kind,charge,state FROM spool_operations ORDER BY rowid').fetchall()

    def test_direct_write_without_real_window_is_refused(self):
        with self.assertRaises(Conflict):
            self.service.upload(self.identifier,'x.jpg',io.BytesIO(b'abc'),3)
        with self.assertRaises(Conflict):
            self.service.store.initialize(str(uuid.uuid4()),'',[],None,'staging')
        self.assertEqual(self.service.store.task(self.identifier)['files'],[])

    def test_same_ledger_serializes_distinct_instances_and_kinds(self):
        other=self.make_capacity();self.available=CONTROL_ALLOWANCE+26
        entered=threading.Event();release=threading.Event();errors=[]
        def writer():
            try:
                with self.capacity.window('upload',self.identifier,1,10):
                    entered.set();release.wait(5)
            except BaseException as error:errors.append(error)
        thread=threading.Thread(target=writer);thread.start()
        try:
            self.assertTrue(entered.wait(3))
            with self.assertRaises(Conflict):
                with other.window('archive',str(uuid.uuid4()),1,10):self.fail('oversubscribed shared device')
        finally:release.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
        self.assertEqual(self.rows(),[('upload',10,'retained')])

    def test_error_duplicate_and_reset_never_release_retained_upper_bounds(self):
        with self.service.upload_window(self.identifier,10):
            self.service.upload(self.identifier,'x.jpg',io.BytesIO(b'abc'),3)
        with self.service.upload_window(self.identifier,10):
            self.service.upload(self.identifier,'x.jpg',io.BytesIO(b'abc'),3)
        with self.assertRaises(ValueError):
            with self.service.upload_window(self.identifier,10):
                self.service.upload(self.identifier,'short.jpg',io.BytesIO(b'x'),3)
        old=self.service.store.task(self.identifier)['directory']
        self.service.store.remove(self.identifier)
        with self.service.init_window(self.identifier,10):
            self.service.store.initialize(self.identifier,'',[],None,'staging')
        self.assertTrue(Path(old).is_dir())
        self.assertEqual(self.rows(),[('upload',10,'retained')]*3+[('form',10,'retained')])
        with self.capacity._db() as db:
            with self.assertRaises(sqlite3.IntegrityError):db.execute('DELETE FROM spool_operations')

    def test_pre_parser_capacity_refusal_does_not_read_body(self):
        self.available=CONTROL_ALLOWANCE+2
        handler=Handler(b'x'*10)
        class Unreadable:
            def read(self,*args):raise AssertionError('body consumed before capacity grant')
        handler.rfile=Unreadable()
        facade=ViewerFacade(self.service,'synthetic-production-token',temp_dir=self.spool)
        with self.assertRaises(Conflict):facade._post(handler,'/task/new/upload/'+self.identifier,'staging')
        self.assertEqual(self.rows(),[]);self.assertEqual(list(self.spool.iterdir()),[])

    def test_real_multipart_closes_temporaries_before_settlement(self):
        body=b'--owned\r\nContent-Disposition: form-data; name="images"; filename="x.jpg"\r\n\r\nabc\r\n--owned--\r\n'
        handler=Handler(body);facade=ViewerFacade(self.service,'synthetic-production-token',temp_dir=self.spool)
        facade._post(handler,'/task/new/upload/'+self.identifier,'staging')
        self.assertEqual(handler.status,200)
        task=self.service.store.task(self.identifier)
        self.assertEqual(len(task['files']),1);self.assertEqual(task['files'][0]['bytes'],3)
        self.assertEqual(list(self.spool.iterdir()),[])
        self.assertEqual(self.rows(),[('upload',len(body),'retained')])

    def test_init_form_also_reserves_before_body_and_closes_spool(self):
        body=b'--owned\r\nContent-Disposition: form-data; name="name"\r\n\r\nSynthetic\r\n--owned--\r\n'
        identifier=str(uuid.uuid4());handler=Handler(body);handler.headers['set-uuid']=identifier
        facade=ViewerFacade(self.service,'synthetic-production-token',temp_dir=self.spool)
        facade._post(handler,'/task/new/init','staging')
        self.assertEqual(handler.status,200);self.assertEqual(list(self.spool.iterdir()),[])
        self.assertEqual(self.rows(),[('form',len(body),'retained')])
        self.available=CONTROL_ALLOWANCE+2
        handler=Handler(body);handler.headers['set-uuid']=str(uuid.uuid4())
        class Unreadable:
            def read(self,*args):raise AssertionError('init body consumed before reservation')
        handler.rfile=Unreadable()
        with self.assertRaises(Conflict):facade._post(handler,'/task/new/init','staging')
        self.assertEqual(self.rows(),[('form',len(body),'retained')])

    def test_policy_and_root_changes_are_held_and_caps_remain_independent(self):
        with self.assertRaises(Conflict):
            SpoolCapacity(self.queue,self.root,self.spool,self.tasks,SpoolPolicy(200000,1,2,20),probe=lambda p:(self.total,self.available))
        with self.service.upload_window(self.identifier,20):
            with self.assertRaises(ValueError):self.service.upload(self.identifier,'big.jpg',io.BytesIO(b'x'*9),9)
        self.assertEqual(self.service.store.max_input_bytes,8)
        self.assertEqual(self.rows(),[('upload',20,'retained')])
        with self.assertRaises(ValueError):ViewerFacade(self.service,'synthetic-production-token')

    def test_stream_consumption_cannot_exceed_the_durable_bound(self):
        with self.capacity.window('upload',self.identifier,1,4):
            self.capacity.consume('upload',self.identifier,1,3)
            with self.assertRaises(Conflict):self.capacity.consume('upload',self.identifier,1,2)
            with self.assertRaises(Conflict):self.capacity.consume('archive',self.identifier,1,1)
        with self.assertRaises(Conflict):self.capacity.consume('upload',self.identifier,1,1)

    def test_unavailable_or_changed_probe_holds_before_new_effect(self):
        self.available=-1
        with self.assertRaises(Conflict):
            with self.capacity.window('upload',self.identifier,1,3):self.fail('invalid probe admitted')
        self.assertEqual(self.rows(),[])

    def test_later_external_space_loss_holds_writer_without_reclaiming_charge(self):
        with self.assertRaises(Conflict):
            with self.capacity.window('upload',self.identifier,1,10):
                self.available=CONTROL_ALLOWANCE
                self.capacity.check_floor()
        self.assertEqual(self.rows(),[('upload',10,'retained')])

    def test_disabled_or_changed_latched_policy_holds_before_queue_construction(self):
        from processing import app
        for active in (True,False):
            with self.subTest(active=active):
                window=self.capacity.window('upload',self.identifier,1,3)
                if active:window.__enter__()
                try:
                    before=self.capacity.path.read_bytes()
                    config=SimpleNamespace(prototype_state_dir=str(self.root),viewer_spool_admission=None)
                    with patch.object(app,'Queue',side_effect=AssertionError('Queue must not open')) as queue:
                        with self.assertRaises(Conflict):app.make_handler(config)
                    queue.assert_not_called();self.assertEqual(before,self.capacity.path.read_bytes())
                    changed=dict(self.policy.payload(),retainedBudgetBytes=99999)
                    with self.assertRaises(Conflict):startup_guard(self.root,changed)
                finally:
                    if active:window.__exit__(None,None,None)

    def test_missing_either_latch_artifact_never_creates_a_fresh_ledger(self):
        for path in (self.capacity.marker,self.capacity.path):
            with self.subTest(path=path.name):
                held=path.with_name(path.name+'.owned-test-held');path.rename(held)
                try:
                    with self.assertRaises(Conflict):startup_guard(self.root,self.policy.payload())
                    self.assertFalse(path.exists());self.assertTrue(held.exists())
                finally:held.rename(path)

    def test_restart_retains_unknown_active_promise_without_ttl_or_reclaim(self):
        # Simulate the durable state observed after a process dies between
        # reservation commit and its writer boundary. No cleanup is invented.
        with self.capacity._db() as db:
            db.execute('INSERT INTO spool_operations VALUES(?,?,?,?,?,?)',
                (str(uuid.uuid4()),self.identifier,1,'upload',10,'active'))
        before=self.rows();restarted=self.make_capacity()
        self.available=CONTROL_ALLOWANCE+20
        with self.assertRaises(Conflict):
            with restarted.window('archive',str(uuid.uuid4()),1,3):self.fail('unknown promise released')
        self.assertEqual(self.rows(),before)

    @unittest.skipUnless(os.name=='posix','actual independent process death and owner lease')
    def test_actual_process_death_keeps_active_promise_after_restart(self):
        self.service.close();self.queue.close()
        code='''import json,os,sys,uuid
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_spool_capacity import SpoolCapacity,SpoolPolicy
root=Path(sys.argv[1]);total=int(sys.argv[2]);queue=Queue(root,viewer_enabled=True)
capacity=SpoolCapacity(queue,root,root/'viewer-spool',root/'viewer-tasks',SpoolPolicy(100000,1,2,20),probe=lambda p:(total,total))
with capacity.window('upload',str(uuid.uuid4()),1,10):os._exit(77)
'''
        result=subprocess.run([sys.executable,'-B','-c',code,str(self.root),str(self.total)],
            cwd=str(Path(__file__).resolve().parents[1]),capture_output=True,text=True,timeout=15)
        self.assertEqual(result.returncode,77,result.stderr)
        self.queue=Queue(self.root,viewer_enabled=True);self.addCleanup(self.queue.close)
        self.capacity=self.make_capacity();self.available=CONTROL_ALLOWANCE+20
        with self.assertRaises(Conflict):
            with self.capacity.window('archive',str(uuid.uuid4()),1,3):self.fail('crashed writer reclaimed')
        self.assertEqual(self.rows(),[('upload',10,'active')])


class SpoolFactoryTests(unittest.TestCase):
    def config(self,root,policy=None):
        from processing.config import Config
        return Config('synthetic-api-token','http://127.0.0.1:1','http://127.0.0.1:2',1000,20*1024**3,
            prototype_state_dir=str(root),prototype_node_ids=('cluster','main'),viewer_enabled=True,
            viewer_token='synthetic-viewer-token',viewer_spool_admission=policy)

    def test_actual_factory_is_default_off_and_shares_enabled_guard(self):
        from processing.app import make_handler
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'state';handler=make_handler(self.config(root))
            try:
                self.assertIsNone(handler.viewer_service.spool_admission)
                self.assertFalse((root/'viewer-spool-capacity.sqlite').exists())
                self.assertEqual(handler.viewer_service.store.max_input_bytes,20*1024**3)
            finally:handler.viewer_service.close();handler.prototype_queue.close()
            policy=SpoolPolicy(10**15,1,2,100).payload()
            handler=make_handler(self.config(root,policy))
            try:
                capacity=handler.viewer_service.spool_admission
                self.assertIs(capacity,handler.viewer_service.store.spool_admission)
                self.assertEqual(capacity.spool,root/'viewer-spool')
                self.assertTrue(capacity.marker.exists())
                self.assertEqual(handler.viewer_service.config.viewer_max_archive_bytes,100*1024**3)
            finally:handler.viewer_service.close();handler.prototype_queue.close()
            with patch('processing.app.Queue',side_effect=AssertionError('legacy startup wrote')) as queue:
                with self.assertRaises(Conflict):make_handler(self.config(root))
            queue.assert_not_called()

    def test_operator_policy_is_closed_default_off_and_duplicate_rejected(self):
        from processing.config import _spool_admission
        with patch.dict(os.environ,{},clear=True):self.assertIsNone(_spool_admission(True))
        raw=json.dumps(SpoolPolicy(1000,1,2,20).payload())
        with patch.dict(os.environ,{'PROCESSING_VIEWER_SPOOL_ADMISSION':raw},clear=True):
            self.assertEqual(_spool_admission(True)['retainedBudgetBytes'],1000)
            with self.assertRaises(ValueError):_spool_admission(False)
        for value in ('{"schemaVersion":1,"schemaVersion":1}',raw[:-1]+',"retainedBudgetBytes":1001}',
                      json.dumps(dict(SpoolPolicy(1000,1,2,20).payload(),maxRecords=True))):
            with patch.dict(os.environ,{'PROCESSING_VIEWER_SPOOL_ADMISSION':value},clear=True):
                with self.assertRaises(ValueError):_spool_admission(True)


class ArchiveAdmissionTests(unittest.TestCase):
    def setUp(self):
        from tests.test_viewer_service import ViewerServiceTests
        self.f=ViewerServiceTests();self.f.setUp();self.addCleanup(self.f.tearDown)
        spool=self.f.root/'state'/'viewer-spool';spool.mkdir(mode=0o700)
        self.free=CONTROL_ALLOWANCE+1000000
        self.capacity=SpoolCapacity(self.f.queue,self.f.root/'state',spool,self.f.service.store.root,
            SpoolPolicy(1000000,1,2,100),probe=lambda p:(CONTROL_ALLOWANCE+1000000,self.free))
        self.f.service.enable_spool_admission(self.capacity)
        self.identifier=str(uuid.uuid4())
        with self.f.service.init_window(self.identifier,100):
            self.f.service.initialize(self.identifier,'Synthetic',[],None,'production')
        with self.f.service.upload_window(self.identifier,100):self.f.upload(self.identifier,content=b'abc')
        self.f.service.commit(self.identifier);self.f.service.tick();self.f.finish(self.identifier);self.f.service.tick()

    def test_result_download_and_upload_share_capacity_and_keep_native_result(self):
        self.free=CONTROL_ALLOWANCE+2
        with self.assertRaises(Conflict):self.f.service.download(self.identifier)
        task=self.f.task(self.identifier)
        self.assertIsNone(task['archive_path'])
        self.assertIn(task['engine_uuid'],self.f.engine_for(self.identifier).tasks)
        self.free=CONTROL_ALLOWANCE+1000000
        stream,length=self.f.service.download(self.identifier)
        with stream:self.assertEqual(stream.read(),self.f.engine_for(self.identifier).archive)
        with closing(sqlite3.connect(self.capacity.path)) as db:
            kinds=db.execute('SELECT kind,state FROM spool_operations ORDER BY rowid').fetchall()
        self.assertEqual(kinds,[('form','retained'),('upload','retained'),('archive','retained')])

    def test_untrusted_short_declared_archive_never_overruns_reserved_bound(self):
        engine=self.f.engine_for(self.identifier);original=engine.download
        def wrong_length(identifier):
            response=original(identifier);response.headers['Content-Length']='1';return response
        engine.download=wrong_length
        with self.assertRaises(Conflict):self.f.service.download(self.identifier)
        partial=Path(self.f.task(self.identifier)['directory'])/'all.zip.partial'
        self.assertEqual(partial.stat().st_size,0)
        self.assertIsNone(self.f.task(self.identifier)['archive_path'])


if __name__=='__main__':unittest.main()
