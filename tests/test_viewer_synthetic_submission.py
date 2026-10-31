from http.server import BaseHTTPRequestHandler,HTTPServer
import json
import os
import sqlite3
import subprocess
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from ops.reference_worker_daemon import DaemonStateLock
from processing.node_gate import NodeGate
from processing.viewer_service import NativeEngine
from processing.viewer_synthetic_submission import SubmissionHeld,submit
from tests.test_viewer_node_inputs import NodeInputsTests


class SyntheticSubmissionTests(unittest.TestCase):
    def setUp(self):
        self.base=NodeInputsTests('test_tls_inventory_then_actual_verified_transfer_and_restart')
        self.base.setUp();self.addCleanup(self.base.doCleanups)
        self.calls=[];self.drop=None;self.bad=None;self.pause=None;self.arrived=threading.Event();self.release=threading.Event();outer=self
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
            def log_message(self,*args):pass
        self.server=HTTPServer(('127.0.0.1',0),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.05},daemon=True);self.thread.start()
        self.addCleanup(self.close);self.origin='http://127.0.0.1:'+str(self.server.server_port)

    def close(self):
        self.release.set();self.server.shutdown();self.server.server_close();self.thread.join(5);self.assertFalse(self.thread.is_alive())

    def prepare(self,owner):
        inputs=self.base.make(owner);inputs.freeze_inventory();inputs.transfer(self.base.object_id(inputs));return inputs

    def test_exact_submission_once_and_replay_without_cleanup(self):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.prepare(owner);result=submit(inputs,NativeEngine(self.origin),synthetic_fixture=True)
            self.assertEqual(submit(inputs,NativeEngine(self.origin),synthetic_fixture=True),result)
            self.assertEqual([c[0] for c in self.calls],['init','upload','commit'])
            self.assertEqual(self.calls[0][2]['set-uuid'],inputs.claim['engineTaskId'])
            self.assertIn(b'\r\n\r\nsynthetic\r\n',self.calls[0][3])
            self.assertIn(b'\r\n\r\nsynthetic\r\n',self.calls[1][3])
        self.assertEqual(len(list(self.base.inputs.glob('*.alloc'))),1)

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
