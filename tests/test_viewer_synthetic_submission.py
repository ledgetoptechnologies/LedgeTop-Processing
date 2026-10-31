from http.server import BaseHTTPRequestHandler,HTTPServer
import io
import json
import signal
import time
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch
import zipfile
from concurrent.futures import ThreadPoolExecutor

from ops.reference_worker_daemon import DaemonStateLock
from processing.node_gate import NodeGate
from processing.viewer_service import NativeEngine
from processing.viewer_synthetic_submission import SubmissionHeld,submit
from processing.viewer_local_result import capture,register,LocalResultHeld
from processing.viewer_node_enrollment import ViewerNodeEnrollment
import processing.viewer_local_result as local_result
import processing.direct_slot_capture as slot_capture
from tests.test_viewer_node_inputs import NodeInputsTests


class SyntheticSubmissionTests(unittest.TestCase):
    def setUp(self):
        self.base=NodeInputsTests('test_tls_inventory_then_actual_verified_transfer_and_restart')
        self.base.setUp();self.addCleanup(self.base.doCleanups)
        self.calls=[];self.drop=None;self.bad=None;self.pause=None;self.arrived=threading.Event();self.release=threading.Event();outer=self
        archive=io.BytesIO()
        with zipfile.ZipFile(archive,'w') as value:value.writestr('synthetic.txt','test result')
        self.archive=archive.getvalue();self.engine_status=40;self.truncate_archive=False;self.bad_info=False
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body=self.rfile.read(int(self.headers.get('Content-Length','0')))
                stage='init' if self.path=='/task/new/init' else 'upload' if '/upload/' in self.path else 'commit'
                outer.calls.append((stage,self.path,dict(self.headers),body))
                if outer.pause==stage:outer.arrived.set();outer.release.wait(8)
                if outer.drop==stage:self.close_connection=True;return
                value={'uuid':self.headers['set-uuid']} if stage=='init' else {'success':True}
                if outer.bad==stage:value={'success':False,'uuid':self.headers.get('set-uuid','')}
                data=json.dumps(value).encode();self.send_response(200);self.send_header('Content-Length',str(len(data)))
                self.end_headers()
                try:self.wfile.write(data)
                except OSError:pass
            def do_GET(self):
                identifier=self.path.split('/')[2]
                data=(json.dumps({'uuid':identifier,'status':{'code':outer.engine_status}}).encode()
                      if self.path.endswith('/info') else outer.archive)
                if outer.bad_info and self.path.endswith('/info'):
                    self.connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: '+str(len(data)+10).encode()+b'\r\n\r\n'+data)
                    self.close_connection=True;return
                self.send_response(200);self.send_header('Content-Length',str(len(data)))
                self.send_header('Content-Type','application/json' if self.path.endswith('/info') else 'application/zip')
                self.end_headers()
                if outer.truncate_archive and not self.path.endswith('/info'):
                    self.wfile.write(data[:8]);self.close_connection=True;return
                self.wfile.write(data)
            def log_message(self,*args):pass
        self.server=HTTPServer(('127.0.0.1',0),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.05},daemon=True);self.thread.start()
        self.addCleanup(self.close);self.origin='http://127.0.0.1:'+str(self.server.server_port)

    def close(self):
        self.release.set();self.server.shutdown();self.server.server_close();self.thread.join(5);self.assertFalse(self.thread.is_alive())

    def prepare(self,owner):
        inputs=self.base.make(owner);inputs.freeze_inventory();inputs.transfer(self.base.object_id(inputs));return inputs

    def test_supervisor_never_replays_ambiguous_engine_init(self):
        from processing.viewer_synthetic_supervisor import tick
        from processing.viewer_synthetic_submission import SubmissionHeld
        self.drop='init'
        with DaemonStateLock(self.base.base.worker) as owner:
            worker=self.base.base.make(owner)
            kwargs=dict(input_root=self.base.inputs,input_root_identity=self.base.input_id,
                        max_total=100000,max_object=100000,synthetic_fixture=True)
            with self.assertRaises(Exception):tick(worker,NativeEngine(self.origin),self.base.base.fixture.job,'enroll-1','production',**kwargs)
            with self.assertRaises(SubmissionHeld):tick(worker,NativeEngine(self.origin),self.base.base.fixture.job,'enroll-1','production',**kwargs)
        self.assertEqual([item[0] for item in self.calls],['init'])
        self.assertEqual(len(list(self.base.inputs.rglob('*.alloc'))),1)
        self.assertEqual(len(list(self.base.base.output.glob('*.alloc'))),1)

    def test_supervisor_restart_offline_then_reconnect(self):
        from processing.direct_output_slot import _identity
        from processing.viewer_archive_spool import ViewerArchiveSpool
        from processing.reference_http import create_reference_http_server
        fixture=self.base.base.fixture
        root=fixture.root/'supervisor-spool';root.mkdir(mode=0o700);fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
        try:root_id=_identity(fd)
        finally:os.close(fd)
        spool=ViewerArchiveSpool(fixture.claims,root=root,root_identity=root_id,max_archive_bytes=65536,enabled=True)
        fixture.api.viewer_spool=spool
        config=dict(worker=str(self.base.base.worker),workerConfig=self.base.base.config,origin=self.origin,
                    job=fixture.job,inputRoot=str(self.base.inputs),inputIdentity=self.base.input_id)
        code='''import json,os,signal,sys
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
from processing.viewer_service import NativeEngine
import processing.viewer_synthetic_supervisor as supervisor
c=json.loads(sys.argv[1]);mode=sys.argv[2]
def stop(*args,**kwargs):os.kill(os.getpid(),signal.SIGKILL)
with DaemonStateLock(c['worker']) as owner:
 worker=ViewerNodeEnrollment(owner,**c['workerConfig'])
 kwargs=dict(input_root=c['inputRoot'],input_root_identity=c['inputIdentity'],max_total=100000,max_object=100000,synthetic_fixture=True)
 if mode=='kill':
  with patch.object(supervisor,'send_completion',stop):supervisor.tick(worker,NativeEngine(c['origin']),c['job'],'enroll-1','production',**kwargs)
 elif mode=='archive-offline':
  from processing.viewer_local_result import send_archive,ArchiveStatusTransportUnavailable
  try:send_archive(worker,synthetic_fixture=True)
  except ArchiveStatusTransportUnavailable:print('archive-status-transient')
  else:raise AssertionError('offline lookup did not fail')
 elif mode=='retry':print(json.dumps(supervisor.run(worker,NativeEngine(c['origin']),c['job'],'enroll-1','production',max_attempts=8,initial_delay=.1,max_delay=.5,**kwargs)))
 else:print(json.dumps(supervisor.tick(worker,NativeEngine(c['origin']),c['job'],'enroll-1','production',**kwargs)))
'''
        args=[sys.executable,'-c',code,json.dumps(config)]
        killed=subprocess.run(args+['kill'],capture_output=True,timeout=20)
        self.assertEqual(killed.returncode,-signal.SIGKILL,killed.stderr)
        self.assertEqual([item[0] for item in self.calls],['init','upload','commit'])
        self.assertEqual(self.calls[0][2]['set-uuid'],fixture.store.task('22222222-2222-4222-8222-222222222222')['engine_uuid'])
        port=fixture.server.server_port;fixture.stop_server()
        archive_offline=subprocess.run(args+['archive-offline'],capture_output=True,text=True,timeout=20)
        self.assertEqual(archive_offline.returncode,0,archive_offline.stderr)
        self.assertEqual(archive_offline.stdout.strip(),'archive-status-transient')
        offline=subprocess.run(args+['resume'],capture_output=True,timeout=20)
        self.assertNotEqual(offline.returncode,0)
        self.assertEqual([item[0] for item in self.calls],['init','upload','commit'])
        retry=subprocess.Popen(args+['retry'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
        try:
            time.sleep(.5)
            self.assertIsNone(retry.poll(),'retry stopped during coordinator outage')
            fixture.server=create_reference_http_server(fixture.api,port=port,ssl_context=fixture.tls)
            fixture.thread=threading.Thread(target=fixture.server.serve_forever,kwargs={'poll_interval':.05},daemon=True);fixture.thread.start()
            stdout,stderr=retry.communicate(timeout=20)
        finally:
            if retry.poll() is None:os.killpg(retry.pid,signal.SIGKILL);retry.communicate(timeout=5)
        self.assertEqual(retry.returncode,0,stderr)
        self.assertEqual(json.loads(stdout)['kind'],'viewer-completion-attestation-receipt')
        self.assertEqual([item[0] for item in self.calls],['init','upload','commit'])
        self.assertEqual(fixture.queue.get(fixture.job)['status'],'running')
        self.assertEqual(len(list(root.glob('*.alloc'))),1)
        self.assertEqual(len(list(self.base.inputs.rglob('*.alloc'))),1)
        self.assertEqual(len(list(self.base.base.output.glob('*.alloc'))),1)

    def test_missing_capture_guard_is_not_migrated_or_recreated(self):
        import sqlite3
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
        path=self.base.base.worker/'viewer-enrollment.sqlite'
        # Discover the fixture's actual enrollment journal name without assuming it.
        candidates=list(self.base.base.worker.glob('*.sqlite'))
        path=next(candidate for candidate in candidates if 'enrollment' in candidate.name)
        with sqlite3.connect(path) as db:db.execute('DROP TABLE capture_guard')
        with DaemonStateLock(self.base.base.worker) as owner:
            with self.assertRaises(Exception):self.base.base.make(owner)
        with sqlite3.connect(path) as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='capture_guard'").fetchone())

    def test_first_capture_after_process_restart_with_coordinator_offline(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
        self.base.base.fixture.stop_server()
        code='''import json,sys
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
from processing.viewer_local_result import capture,resume_capture_status
from processing.viewer_service import NativeEngine
root,config,origin=sys.argv[1:]
with DaemonStateLock(root) as owner:
 worker=ViewerNodeEnrollment(owner,**json.loads(config))
 assert resume_capture_status(worker,synthetic_fixture=True)['state']=='absent_no_dispatch'
 print(json.dumps(capture(worker,NativeEngine(origin),synthetic_fixture=True)))
'''
        child=subprocess.run([sys.executable,'-c',code,str(self.base.base.worker),json.dumps(self.base.base.config),self.origin],capture_output=True,text=True,timeout=15)
        self.assertEqual(child.returncode,0,child.stderr)
        self.assertEqual(json.loads(child.stdout)['archiveBytes'],len(self.archive))

    def test_offline_capture_guard_holds_after_kill_before_output(self):
        from processing.viewer_local_result import resume_capture_status
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
        self.base.base.fixture.stop_server()
        with DaemonStateLock(self.base.base.worker) as owner:
            status=resume_capture_status(self.base.base.make(owner),synthetic_fixture=True)
            self.assertEqual(status['state'],'absent_no_dispatch')
        code='''import json,os,signal,sys
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
from processing.viewer_local_result import capture
from processing.viewer_service import NativeEngine
root,config,origin=sys.argv[1:]
def stop(*args,**kwargs):os.kill(os.getpid(),signal.SIGKILL)
with DaemonStateLock(root) as owner:
 with patch.object(NativeEngine,'download',stop):capture(ViewerNodeEnrollment(owner,**json.loads(config)),NativeEngine(origin),synthetic_fixture=True)
'''
        child=subprocess.run([sys.executable,'-c',code,str(self.base.base.worker),json.dumps(self.base.base.config),self.origin],capture_output=True,timeout=15)
        self.assertEqual(child.returncode,-signal.SIGKILL)
        with DaemonStateLock(self.base.base.worker) as owner:
            worker=self.base.base.make(owner)
            with self.assertRaises(Exception):resume_capture_status(worker,synthetic_fixture=True)
            with self.assertRaises(Exception):capture(worker,NativeEngine(self.origin),synthetic_fixture=True)

    def test_exact_submission_once_and_replay_without_cleanup(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);result=submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
            self.assertEqual(submit(inputs,NativeEngine(self.origin),synthetic_fixture=True),result)
            self.assertEqual([c[0] for c in self.calls],['init','upload','commit'])
            self.assertEqual(self.calls[0][2]['set-uuid'],inputs.claim['engineTaskId'])
            self.assertIn(b'\r\n\r\nsynthetic\r\n',self.calls[0][3])
            self.assertIn(b'\r\n\r\nsynthetic\r\n',self.calls[1][3])
        self.assertEqual(len(list(self.base.inputs.rglob('*.alloc'))),1)

    def lost_ack(self,stage):
        self.drop=stage
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner)
            with self.assertRaises(SubmissionHeld):submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
            before=len(self.calls)
            with self.assertRaisesRegex(SubmissionHeld,'requires_inspection'):submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
            self.assertEqual(len(self.calls),before)

    def test_lost_init_ack_holds(self):self.lost_ack('init')
    def test_lost_upload_ack_holds(self):self.lost_ack('upload')
    def test_lost_commit_ack_holds(self):self.lost_ack('commit')

    def test_malformed_ack_holds_without_repeat(self):
        self.bad='init'
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner)
            with self.assertRaises(SubmissionHeld):submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
            with self.assertRaisesRegex(SubmissionHeld,'requires_inspection'):submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
        self.assertEqual([c[0] for c in self.calls],['init'])

    def test_missing_gate_blocks_before_engine(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner)
            with NodeGate('fixture-engine') as gate:
                state=gate.snapshot();state['reservation']=None;gate._save(state)
            with self.assertRaises(RuntimeError):submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
        self.assertEqual(self.calls,[])

    def test_competing_state_owner_is_refused(self):
        with DaemonStateLock(self.base.base.worker):
            with self.assertRaisesRegex(ValueError,'already owned'):
                with DaemonStateLock(self.base.base.worker):pass
        self.assertEqual(self.calls,[])

    def test_hold_drains_inflight_and_blocks_next_mutation(self):
        self.pause='upload'
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner)
            with ThreadPoolExecutor(max_workers=1) as pool:
                future=pool.submit(submit,inputs,NativeEngine(self.origin),synthetic_fixture=True)
                try:
                    self.assertTrue(self.arrived.wait(8))
                    hold=inputs.worker._request({},job_id=inputs.claim['jobId'],claim_token=inputs.claim['claimToken'],kind='mutation-hold')
                    self.assertEqual(hold['state'],'draining')
                    with self.assertRaises(sqlite3.IntegrityError):
                        with self.base.base.fixture.queue._db() as db:
                            db.execute("UPDATE jobs SET status='held' WHERE id=?",(inputs.claim['jobId'],))
                    self.release.set()
                    with self.assertRaises(SubmissionHeld):future.result(timeout=8)
                finally:self.release.set()
            self.assertEqual(self.base.base.fixture.queue.get(inputs.claim['jobId'])['status'],'held')
        self.assertEqual([c[0] for c in self.calls],['init','upload'])

    def test_coordinator_restart_with_active_permit_fails_closed(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);inputs.request_execution_grant()
            payload=dict(operationId='init',operation='init')
            permit=inputs.worker._request(payload,job_id=inputs.claim['jobId'],claim_token=inputs.claim['claimToken'],kind='mutation-permit')
            self.assertEqual(permit['state'],'active')
        fixture=self.base.base.fixture;fixture.stop_server();fixture.queue.close();fixture.queue=None
        with self.assertRaises(sqlite3.IntegrityError):fixture.start_queue()
        with self.assertRaises(sqlite3.IntegrityError):fixture.start_queue()

    def test_hold_without_active_permit_blocks_new_permits(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);inputs.request_execution_grant()
            result=inputs.worker._request({},job_id=inputs.claim['jobId'],claim_token=inputs.claim['claimToken'],kind='mutation-hold')
            self.assertEqual(result['state'],'held')
            from processing.viewer_node_enrollment import EnrollmentHeld
            with self.assertRaises(EnrollmentHeld):
                inputs.worker._request(dict(operationId='init',operation='init'),job_id=inputs.claim['jobId'],
                                       claim_token=inputs.claim['claimToken'],kind='mutation-permit')
        self.assertEqual(self.calls,[])

    def test_corrupt_journal_state_does_not_claim_success(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
            with inputs._db() as db:
                db.execute("UPDATE synthetic_submission SET ordinal=0 WHERE singleton=1")
            with self.assertRaisesRegex(SubmissionHeld,'state_invalid'):
                submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
        self.assertEqual([c[0] for c in self.calls],['init','upload','commit'])

    def test_capture_and_replay_local_intent_while_coordinator_offline(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
            self.base.base.fixture.stop_server()
            result=capture(inputs.worker,NativeEngine(self.origin),synthetic_fixture=True)
            self.assertEqual(result['archiveBytes'],len(self.archive))
            self.assertEqual(result['kind'],'viewer-local-result-intent')
        with DaemonStateLock(self.base.base.worker) as owner:
            worker=ViewerNodeEnrollment(owner,**self.base.base.config)
            self.assertEqual(register(worker),result)
        self.assertEqual(len(list(self.base.inputs.rglob('*.alloc'))),1)

    def test_nonterminal_engine_result_does_not_capture(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
            self.engine_status=20
            with self.assertRaisesRegex(LocalResultHeld,'not_confirmed_complete'):
                capture(inputs.worker,NativeEngine(self.origin),synthetic_fixture=True)

    def test_truncated_status_response_does_not_create_completion_observation(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
            self.bad_info=True
            with self.assertRaisesRegex(LocalResultHeld,'status_unavailable'):
                capture(inputs.worker,NativeEngine(self.origin),synthetic_fixture=True)
            self.assertEqual(list(Path(inputs.worker.config['outputRoot']).glob('*.viewer-completion-observation')),[])

    def failed_intent_sync(self,fail_at):
        real_persist=local_result._persist;real_fsync=os.fsync;counter=[0]
        def fail(fd):
            counter[0]+=1
            if counter[0]==fail_at:raise OSError('injected sync failure')
            return real_fsync(fd)
        def persist(*args):
            if not args[2].endswith('.viewer-outbox'):return real_persist(*args)
            with patch.object(slot_capture.os,'fsync',fail):return real_persist(*args)
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
            with patch.object(local_result,'_persist',persist):
                with self.assertRaises(OSError):capture(inputs.worker,NativeEngine(self.origin),synthetic_fixture=True)
            paths=list(Path(inputs.worker.config['outputRoot']).glob('*.viewer-outbox'))
            self.assertEqual(len(paths),1)
            file_stat=paths[0].stat();root_stat=paths[0].parent.stat();synced=[]
            file_id=(file_stat.st_dev,file_stat.st_ino);root_id=(root_stat.st_dev,root_stat.st_ino)
            def still_fails(fd):
                info=os.fstat(fd)
                if (info.st_dev,info.st_ino)==file_id:raise OSError('injected replay sync failure')
                return real_fsync(fd)
            with patch.object(local_result.os,'fsync',still_fails):
                with self.assertRaises(OSError):register(inputs.worker)
            def observe(fd):
                info=os.fstat(fd);synced.append((info.st_dev,info.st_ino));return real_fsync(fd)
            with patch.object(local_result.os,'fsync',observe):register(inputs.worker)
            index=synced.index(file_id);self.assertIn(root_id,synced[index+1:])

    def test_result_intent_file_sync_failure_requires_resync(self):self.failed_intent_sync(1)
    def test_result_intent_directory_sync_failure_requires_resync(self):self.failed_intent_sync(2)

    def test_truncated_archive_holds_without_local_intent_or_cleanup(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
            self.truncate_archive=True
            with self.assertRaises(Exception):capture(inputs.worker,NativeEngine(self.origin),synthetic_fixture=True)
            with self.assertRaises(Exception):register(inputs.worker)
        self.assertEqual(len(list(self.base.inputs.rglob('*.alloc'))),1)
        self.assertEqual(self.archive[:2],b'PK')

    def test_restart_registers_sealed_result_after_kill_before_outbox(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
        self.base.base.fixture.stop_server()
        code='''import json,os,signal,sys
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
from processing.viewer_service import NativeEngine
import processing.viewer_local_result as result
root,config,origin=sys.argv[1:]
def kill(worker):os.kill(os.getpid(),signal.SIGKILL)
result.register=kill
with DaemonStateLock(root) as owner:
 worker=ViewerNodeEnrollment(owner,**json.loads(config))
 result.capture(worker,NativeEngine(origin),synthetic_fixture=True)
'''
        child=subprocess.run([sys.executable,'-c',code,str(self.base.base.worker),json.dumps(self.base.base.config),self.origin],
                             stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
        self.assertEqual(child.returncode,-9)
        with DaemonStateLock(self.base.base.worker) as owner:
            worker=ViewerNodeEnrollment(owner,**self.base.base.config)
            result=register(worker);self.assertEqual(result['archiveBytes'],len(self.archive))
            self.assertEqual(register(worker),result)

    def test_kill_during_capture_holds_partial_without_deletion(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
        self.base.base.fixture.stop_server()
        code='''import json,os,signal,sys
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
from processing.viewer_service import NativeEngine
from processing.direct_slot_capture import Capture
from processing.viewer_local_result import capture
root,config,origin=sys.argv[1:]
original=Capture._write
def write(self,data):original(self,data);os.kill(os.getpid(),signal.SIGKILL)
Capture._write=write
with DaemonStateLock(root) as owner:
 worker=ViewerNodeEnrollment(owner,**json.loads(config))
 capture(worker,NativeEngine(origin),synthetic_fixture=True)
'''
        child=subprocess.run([sys.executable,'-c',code,str(self.base.base.worker),json.dumps(self.base.base.config),self.origin],
                             stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
        self.assertEqual(child.returncode,-9)
        with DaemonStateLock(self.base.base.worker) as owner:
            worker=ViewerNodeEnrollment(owner,**self.base.base.config)
            with self.assertRaises(Exception):register(worker)
        self.assertEqual(len(list(self.base.inputs.rglob('*.alloc'))),1)

    def kill_stage(self,stage):
        with DaemonStateLock(self.base.base.worker) as owner:self.prepare(owner)
        self.pause=stage
        code='''import json,sys
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
from processing.viewer_node_inputs import ViewerNodeInputs
from processing.viewer_service import NativeEngine
from processing.viewer_synthetic_submission import submit
root,config,job,input_root,identity,origin=sys.argv[1:]
with DaemonStateLock(root) as owner:
 worker=ViewerNodeEnrollment(owner,**json.loads(config))
 inputs=ViewerNodeInputs(worker,job,'enroll-1','production',input_root=input_root,input_root_identity=json.loads(identity),max_total=100000,max_object=100000)
 submit(inputs,NativeEngine(origin),synthetic_fixture=True)
'''
        child=subprocess.Popen([sys.executable,'-c',code,str(self.base.base.worker),json.dumps(self.base.base.config),
            self.base.base.fixture.job,str(self.base.inputs),json.dumps(self.base.input_id),self.origin],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            self.assertTrue(self.arrived.wait(8));child.kill();child.wait(timeout=5)
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=5)
            self.release.set()
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.base.make(owner)
            with self.assertRaisesRegex(SubmissionHeld,'requires_inspection'):
                submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
        expected={'init':['init'],'upload':['init','upload'],'commit':['init','upload','commit']}
        self.assertEqual([c[0] for c in self.calls],expected[stage])

    def test_kill_after_init_side_effect_holds(self):self.kill_stage('init')
    def test_kill_after_upload_side_effect_holds(self):self.kill_stage('upload')
    def test_kill_after_commit_side_effect_holds(self):self.kill_stage('commit')
