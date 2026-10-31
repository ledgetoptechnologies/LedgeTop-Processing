import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import HTTPSHandler,ProxyHandler,Request,build_opener

from ops.reference_worker_daemon import DaemonStateLock
from processing.direct_output_slot import _identity
from processing.viewer_node_inputs import InputHeld, ViewerNodeInputs
from processing.viewer_node_enrollment import EnrollmentHeld, ViewerNodeEnrollment
from tests.test_viewer_node_enrollment import NodeEnrollmentTests
from processing.viewer_worker_claim import _InputStream


@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux allocation required')
class NodeInputsTests(unittest.TestCase):
    def setUp(self):
        self.base=NodeEnrollmentTests('test_authenticated_tls_gate_and_allocation_idempotent')
        self.base.setUp();self.addCleanup(self.base.doCleanups)
        self.root=self.base.root;self.inputs=self.root/'node-inputs';self.inputs.mkdir(mode=0o700)
        fd=os.open(self.inputs,os.O_RDONLY|os.O_DIRECTORY)
        try:self.input_id=_identity(fd)
        finally:os.close(fd)

    def make(self,owner):
        worker=self.base.make(owner)
        return ViewerNodeInputs(worker,self.base.fixture.job,'enroll-1','production',
            input_root=self.inputs,input_root_identity=self.input_id,max_total=100000,max_object=100000)

    def object_id(self,inputs):
        with inputs._db() as db:return db.execute('SELECT object_id FROM objects WHERE ordinal=0').fetchone()[0]

    def test_tls_inventory_then_actual_verified_transfer_and_restart(self):
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner)
            self.assertEqual(inputs.fetch_plan(),dict(options=[],outputs=[]))
            self.assertEqual(inputs.fetch_plan(),dict(options=[],outputs=[]))
            self.assertEqual(inputs.freeze_inventory(),dict(state='inventory_verified_no_dispatch',count=1,total=9))
            object_id=self.object_id(inputs)
            self.assertEqual(inputs.transfer(object_id)['state'],'input_verified_no_dispatch')
            self.assertEqual(inputs.transfer(object_id)['state'],'input_verified_no_dispatch')
            with inputs._db() as db:row=db.execute('SELECT slot_key,phase FROM objects WHERE object_id=?',(object_id,)).fetchone()
            self.assertEqual(row[1],'verified')
            self.assertEqual((self.inputs/('object-'+object_id)/('slot-'+row[0]+'.alloc')).read_bytes(),b'synthetic')
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner)
            self.assertEqual(inputs.fetch_plan(),dict(options=[],outputs=[]))
            with self.assertRaisesRegex(InputHeld,'verified_inventory_required'):inputs.transfer(object_id)
            self.assertEqual(inputs.freeze_inventory()['count'],1)
            self.assertEqual(inputs.transfer(object_id)['state'],'input_verified_no_dispatch')
        self.assertEqual(self.base.fixture.store.task('22222222-2222-4222-8222-222222222222')['phase'],'worker_claimed')
        self.assertEqual(len(list(self.inputs.rglob('*.alloc'))),1)

    def test_changed_source_is_held_without_dispatch_or_cleanup(self):
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner);inputs.freeze_inventory();object_id=self.object_id(inputs)
            source=Path(self.base.fixture.store.task('22222222-2222-4222-8222-222222222222')['files'][0]['path'])
            source.write_bytes(b'different')
            with self.assertRaises(InputHeld):inputs.transfer(object_id)
            with self.assertRaisesRegex(InputHeld,'requires_inspection'):inputs.transfer(object_id)
        self.assertEqual(source.read_bytes(),b'different')
        self.assertEqual(len(list(self.inputs.rglob('*.alloc'))),1)

    def test_disconnected_stream_leaves_partial_and_holds_retry(self):
        def broken(stream):
            stream._check();yield os.pread(stream.fd,4,0)
            raise OSError('synthetic disconnect')
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner);inputs.freeze_inventory();object_id=self.object_id(inputs)
            with patch.object(_InputStream,'chunks',broken):
                with self.assertRaises(InputHeld):inputs.transfer(object_id)
            with self.assertRaisesRegex(InputHeld,'requires_inspection'):inputs.transfer(object_id)
        self.assertEqual(len(list(self.inputs.rglob('*.alloc'))),1)

    def test_missing_output_reservation_blocks_inventory(self):
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner)
            allocation=Path(inputs.worker.config['outputRoot'])/('slot-'+inputs.binding_sha+'.alloc')
            allocation.unlink()
            with self.assertRaises((InputHeld,OSError,RuntimeError)):
                inputs.freeze_inventory()
            with inputs._db() as db:
                self.assertIsNone(db.execute('SELECT phase FROM manifest').fetchone())

    def test_grant_requires_verified_inputs_and_replays_after_restart(self):
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner)
            with self.assertRaisesRegex(InputHeld,'input_not_verified'):inputs.request_execution_grant()
            object_id=self.object_id(inputs);inputs.transfer(object_id)
            with self.base.fixture.queue._db() as db:
                db.execute("UPDATE viewer_tasks SET name='changed' WHERE uuid=?",(inputs.claim['viewerTaskId'],))
            grant=inputs.request_execution_grant()
            self.assertEqual(grant['taskName'],'synthetic')
            self.assertEqual(grant['kind'],'synthetic_execution_grant')
            self.assertEqual(inputs.request_execution_grant(),grant)
            with self.base.fixture.queue._db() as db:
                db.execute("UPDATE viewer_tasks SET name='synthetic' WHERE uuid=?",(inputs.claim['viewerTaskId'],))
        with DaemonStateLock(self.base.worker) as owner:
            self.assertEqual(self.make(owner).request_execution_grant(),grant)
        self.assertEqual(self.base.fixture.queue.get(self.base.fixture.job)['status'],'running')

    def test_grant_rejects_plan_name_tampering_before_and_after_issuance(self):
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner);inputs.freeze_inventory();inputs.transfer(self.object_id(inputs))
            with self.base.fixture.queue._db() as db:
                db.execute("UPDATE viewer_worker_plans SET task_name='forged' WHERE job_id=?",(inputs.claim['jobId'],))
            with self.assertRaises(EnrollmentHeld):inputs.request_execution_grant()
            with self.base.fixture.queue._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_grants').fetchone()[0],0)
                db.execute("UPDATE viewer_worker_plans SET task_name='synthetic' WHERE job_id=?",(inputs.claim['jobId'],))
            inputs.request_execution_grant()
            with self.base.fixture.queue._db() as db:
                db.execute("UPDATE viewer_worker_plans SET task_name='forged' WHERE job_id=?",(inputs.claim['jobId'],))
            with self.assertRaises(EnrollmentHeld):inputs.request_execution_grant()

    def test_grant_holds_after_input_mutation(self):
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner);inputs.freeze_inventory();object_id=self.object_id(inputs);inputs.transfer(object_id)
            with inputs._db() as db:key=db.execute('SELECT slot_key FROM objects WHERE object_id=?',(object_id,)).fetchone()[0]
            (self.inputs/('object-'+object_id)/('slot-'+key+'.alloc')).write_bytes(b'different')
            with self.assertRaises(InputHeld):inputs.request_execution_grant()
        with self.base.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_grants').fetchone()[0],0)

    def test_worker_rejects_changed_grant_name(self):
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner);inputs.freeze_inventory();inputs.transfer(self.object_id(inputs))
            original=ViewerNodeEnrollment._request
            def change(worker,request,**kwargs):
                value=original(worker,request,**kwargs)
                if kwargs.get('kind')=='execution-grant':value['taskName']='forged'
                return value
            with patch.object(ViewerNodeEnrollment,'_request',change):
                with self.assertRaisesRegex(InputHeld,'execution_grant_changed'):inputs.request_execution_grant()

    def test_lost_grant_response_replays_without_new_identity(self):
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner);inputs.freeze_inventory();inputs.transfer(self.object_id(inputs))
            original=ViewerNodeEnrollment._request
            def lose(worker,request,**kwargs):
                value=original(worker,request,**kwargs)
                if kwargs.get('kind')=='execution-grant':raise EnrollmentHeld('synthetic_response_lost')
                return value
            with patch.object(ViewerNodeEnrollment,'_request',lose):
                with self.assertRaisesRegex(EnrollmentHeld,'synthetic_response_lost'):inputs.request_execution_grant()
            grant=inputs.request_execution_grant()
            self.assertEqual(inputs.request_execution_grant(),grant)
        with self.base.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_grants').fetchone()[0],1)

    def test_grant_rejects_wrong_claim_token_and_evidence(self):
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner);inputs.freeze_inventory();inputs.transfer(self.object_id(inputs))
            grant=inputs.request_execution_grant()
            payload={k:grant[k] for k in ('enrollmentReceipt','inputInventorySha256','pipelineSha256','inputCount','inputBytes')}
            request=Request(self.base.fixture.origin+'/ltds/worker/v1/viewer-claims/'+inputs.claim['jobId']+'/execution-grant',
                            data=json.dumps(payload).encode(),method='POST',headers={'Authorization':'Bearer '+'b'*32,
                            'X-Claim-Token':inputs.claim['claimToken'],'Content-Type':'application/json'})
            with self.assertRaises(HTTPError) as error:
                build_opener(ProxyHandler({}),HTTPSHandler(context=self.base.fixture.client)).open(request,timeout=3)
            self.assertEqual(error.exception.code,409);error.exception.close()
            with self.assertRaises(EnrollmentHeld):
                inputs.worker._request(payload,job_id=inputs.claim['jobId'],claim_token='wrong',kind='execution-grant')
            payload['inputBytes']+=1
            with self.assertRaises(EnrollmentHeld):
                inputs.worker._request(payload,job_id=inputs.claim['jobId'],claim_token=inputs.claim['claimToken'],kind='execution-grant')
            payload['inputBytes']-=1
            with self.base.fixture.queue._db() as db:
                db.execute("UPDATE jobs SET status='held',held_reason='coordinator_restart' WHERE id=?",(inputs.claim['jobId'],))
            with self.assertRaises(EnrollmentHeld):
                inputs.worker._request(payload,job_id=inputs.claim['jobId'],claim_token=inputs.claim['claimToken'],kind='execution-grant')

    def test_grant_replays_after_node_killed_after_coordinator_response(self):
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner);inputs.freeze_inventory();inputs.transfer(self.object_id(inputs))
        code='''import json,os,signal,sys
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
from processing.viewer_node_inputs import ViewerNodeInputs
root,config,job,input_root,identity=sys.argv[1:]
original=ViewerNodeEnrollment._request
def request(self,payload,**kwargs):
 result=original(self,payload,**kwargs)
 if kwargs.get('kind')=='execution-grant':os.kill(os.getpid(),signal.SIGKILL)
 return result
ViewerNodeEnrollment._request=request
with DaemonStateLock(root) as owner:
 worker=ViewerNodeEnrollment(owner,**json.loads(config))
 inputs=ViewerNodeInputs(worker,job,'enroll-1','production',input_root=input_root,input_root_identity=json.loads(identity),max_total=100000,max_object=100000)
 inputs.request_execution_grant()
'''
        result=subprocess.run([sys.executable,'-c',code,str(self.base.worker),json.dumps(self.base.config),self.base.fixture.job,
                               str(self.inputs),json.dumps(self.input_id)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
        self.assertEqual(result.returncode,-9)
        with DaemonStateLock(self.base.worker) as owner:
            self.assertEqual(self.make(owner).request_execution_grant()['taskName'],'synthetic')

    def test_lost_ack_replays_exact_enrollment_after_commit(self):
        original=ViewerNodeEnrollment._request
        def lose_response(worker,request,**kwargs):
            result=original(worker,request,**kwargs)
            if kwargs.get('job_id') is not None:raise EnrollmentHeld('synthetic_response_lost')
            return result
        with DaemonStateLock(self.base.worker) as owner:
            with patch.object(ViewerNodeEnrollment,'_request',lose_response):
                with self.assertRaisesRegex(EnrollmentHeld,'synthetic_response_lost'):self.make(owner)
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner)
            self.assertEqual(inputs.freeze_inventory()['count'],1)

    def test_process_exit_after_ack_commit_replays(self):
        code='''import json,os,signal,sys
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
worker_root,config,job=sys.argv[1:]
original=ViewerNodeEnrollment._request
def request(self,payload,**kwargs):
 result=original(self,payload,**kwargs)
 if kwargs.get('job_id') is not None:os.kill(os.getpid(),signal.SIGKILL)
 return result
ViewerNodeEnrollment._request=request
with DaemonStateLock(worker_root) as owner:
 ViewerNodeEnrollment(owner,**json.loads(config)).enroll(job,'enroll-1','production')
'''
        result=subprocess.run([sys.executable,'-c',code,str(self.base.worker),json.dumps(self.base.config),self.base.fixture.job],
                              stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
        self.assertEqual(result.returncode,-9)
        with DaemonStateLock(self.base.worker) as owner:
            self.assertEqual(self.make(owner).freeze_inventory()['count'],1)

    def test_killed_midwrite_holds_persistent_state_after_restart(self):
        ready=self.root/'input-write-ready'
        config=json.dumps(self.base.config);identity=json.dumps(self.input_id)
        code='''import json,sys,time
from pathlib import Path
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
from processing.viewer_node_inputs import ViewerNodeInputs
from processing.direct_output_slot import OutputSlot
worker_root,config,job,input_root,identity,ready=sys.argv[1:]
def pause():Path(ready).write_text('ready');time.sleep(30)
with DaemonStateLock(worker_root) as owner:
 worker=ViewerNodeEnrollment(owner,**json.loads(config))
 inputs=ViewerNodeInputs(worker,job,'enroll-1','production',input_root=input_root,input_root_identity=json.loads(identity),max_total=100000,max_object=100000)
 inputs.freeze_inventory()
 with inputs._db() as db:object_id=db.execute('SELECT object_id FROM objects WHERE ordinal=0').fetchone()[0]
 original=OutputSlot.write_at
 def intercept(self,*args):
  original(self,*args);pause()
 OutputSlot.write_at=intercept
 inputs.transfer(object_id)
'''
        child=subprocess.Popen([sys.executable,'-c',code,str(self.base.worker),config,self.base.fixture.job,str(self.inputs),identity,str(ready)],
                               stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            deadline=time.monotonic()+8
            while not ready.exists() and time.monotonic()<deadline:
                if child.poll() is not None:self.fail('input child exited before barrier')
                time.sleep(.02)
            self.assertTrue(ready.exists());child.kill();child.wait(timeout=5)
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=5)
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner);inputs.freeze_inventory();object_id=self.object_id(inputs)
            with self.assertRaisesRegex(InputHeld,'requires_inspection'):inputs.transfer(object_id)
        self.assertEqual(len(list(self.inputs.rglob('*.alloc'))),1)
    def test_legacy_flat_layout_policy_is_held(self):
     import json
     with DaemonStateLock(self.base.worker) as owner:
      inputs=self.make(owner)
      with inputs._db() as db:
       value=json.loads(db.execute('SELECT value FROM policy').fetchone()[0]);value.pop('inputLayout')
       from processing import reference_protocol as p
       db.execute('UPDATE policy SET value=?',(p.canonical_json(value).decode(),))
      with self.assertRaises(InputHeld):self.make(owner)
    def test_killed_after_mkdir_before_identity_is_held_after_restart(self):
        import signal
        code='''import json,os,signal,sys
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
from processing.viewer_node_inputs import ViewerNodeInputs,InputHeld
root,config,job,input_root,identity,mode=sys.argv[1:]
with DaemonStateLock(root) as owner:
 worker=ViewerNodeEnrollment(owner,**json.loads(config))
 inputs=ViewerNodeInputs(worker,job,'enroll-1','production',input_root=input_root,input_root_identity=json.loads(identity),max_total=100000,max_object=100000)
 inputs.freeze_inventory()
 with inputs._db() as db:object_id=db.execute('SELECT object_id FROM objects WHERE ordinal=0').fetchone()[0]
 if mode=='kill':
  original=os.mkdir
  def intercept(path,*args,**kwargs):
   result=original(path,*args,**kwargs)
   if str(path).startswith('object-'):os.kill(os.getpid(),signal.SIGKILL)
   return result
  os.mkdir=intercept
  inputs.transfer(object_id)
  raise AssertionError('mkdir kill point not reached')
 else:
  with inputs._db() as db:phase,slot_identity=db.execute('SELECT phase,slot_identity FROM objects WHERE object_id=?',(object_id,)).fetchone()
  assert phase=='creating' and slot_identity is None
  try:inputs.transfer(object_id)
  except InputHeld as error:assert str(error)=='input_requires_inspection'
  else:raise AssertionError('ambiguous creation was retried')
'''
        args=[sys.executable,'-c',code,str(self.base.worker),json.dumps(self.base.config),self.base.fixture.job,
              str(self.inputs),json.dumps(self.input_id)]
        killed=subprocess.run(args+['kill'],capture_output=True,timeout=15)
        self.assertEqual(killed.returncode,-signal.SIGKILL,killed.stderr)
        dirs=list(self.inputs.glob('object-*'));self.assertEqual(len(dirs),1)
        before=(dirs[0].stat().st_dev,dirs[0].stat().st_ino);self.assertEqual(list(dirs[0].iterdir()),[])
        resumed=subprocess.run(args+['check'],capture_output=True,timeout=15)
        self.assertEqual(resumed.returncode,0,resumed.stderr)
        self.assertEqual((dirs[0].stat().st_dev,dirs[0].stat().st_ino),before)
        self.assertEqual(list(dirs[0].iterdir()),[])
