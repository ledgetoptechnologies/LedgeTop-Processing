import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

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
            self.assertEqual(inputs.freeze_inventory(),dict(state='inventory_verified_no_dispatch',count=1,total=9))
            object_id=self.object_id(inputs)
            self.assertEqual(inputs.transfer(object_id)['state'],'input_verified_no_dispatch')
            self.assertEqual(inputs.transfer(object_id)['state'],'input_verified_no_dispatch')
            with inputs._db() as db:row=db.execute('SELECT slot_key,phase FROM objects WHERE object_id=?',(object_id,)).fetchone()
            self.assertEqual(row[1],'verified')
            self.assertEqual((self.inputs/('slot-'+row[0]+'.alloc')).read_bytes(),b'synthetic')
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner)
            with self.assertRaisesRegex(InputHeld,'verified_inventory_required'):inputs.transfer(object_id)
            self.assertEqual(inputs.freeze_inventory()['count'],1)
            self.assertEqual(inputs.transfer(object_id)['state'],'input_verified_no_dispatch')
        self.assertEqual(self.base.fixture.store.task('22222222-2222-4222-8222-222222222222')['phase'],'worker_claimed')
        self.assertEqual(len(list(self.inputs.glob('*.alloc'))),1)

    def test_changed_source_is_held_without_dispatch_or_cleanup(self):
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner);inputs.freeze_inventory();object_id=self.object_id(inputs)
            source=Path(self.base.fixture.store.task('22222222-2222-4222-8222-222222222222')['files'][0]['path'])
            source.write_bytes(b'different')
            with self.assertRaises(InputHeld):inputs.transfer(object_id)
            with self.assertRaisesRegex(InputHeld,'requires_inspection'):inputs.transfer(object_id)
        self.assertEqual(source.read_bytes(),b'different')
        self.assertEqual(len(list(self.inputs.glob('*.alloc'))),1)

    def test_disconnected_stream_leaves_partial_and_holds_retry(self):
        def broken(stream):
            stream._check();yield os.pread(stream.fd,4,0)
            raise OSError('synthetic disconnect')
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner);inputs.freeze_inventory();object_id=self.object_id(inputs)
            with patch.object(_InputStream,'chunks',broken):
                with self.assertRaises(InputHeld):inputs.transfer(object_id)
            with self.assertRaisesRegex(InputHeld,'requires_inspection'):inputs.transfer(object_id)
        self.assertEqual(len(list(self.inputs.glob('*.alloc'))),1)

    def test_missing_output_reservation_blocks_inventory(self):
        with DaemonStateLock(self.base.worker) as owner:
            inputs=self.make(owner)
            allocation=Path(inputs.worker.config['outputRoot'])/('slot-'+inputs.binding_sha+'.alloc')
            allocation.unlink()
            with self.assertRaises((InputHeld,OSError,RuntimeError)):
                inputs.freeze_inventory()
            with inputs._db() as db:
                self.assertIsNone(db.execute('SELECT phase FROM manifest').fetchone())

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
        self.assertEqual(len(list(self.inputs.glob('*.alloc'))),1)
