import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from ops.reference_worker_daemon import DaemonStateLock
from processing.direct_output_slot import SlotHeld, _identity
from processing.node_gate import NodeGate
from processing.viewer_node_enrollment import EnrollmentHeld, ViewerNodeEnrollment
from tests.test_viewer_worker_claim import ViewerWorkerClaimTests


@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux allocation required')
class NodeEnrollmentTests(unittest.TestCase):
    def setUp(self):
        self.fixture=ViewerWorkerClaimTests('test_wrong_identity_environment_node_and_request_fail')
        self.fixture.setUp();self.addCleanup(self.fixture.tearDown)
        self.root=self.fixture.root;self.nodehome=self.root/'nodehome';self.nodehome.mkdir(mode=0o700)
        self.env=patch.dict(os.environ,{'XDG_STATE_HOME':str(self.nodehome)});self.env.start();self.addCleanup(self.env.stop)
        self.worker=self.root/'node-worker';self.worker.mkdir(mode=0o700)
        self.output=self.root/'node-output';self.output.mkdir(mode=0o700)
        fd=os.open(self.output,os.O_RDONLY|os.O_DIRECTORY)
        try:self.output_id=_identity(fd)
        finally:os.close(fd)
        self.authority=self.fixture.queue.authority_id
        with NodeGate('fixture-engine') as gate:
            gate.provision('cluster','http://127.0.0.1:18080',self.authority)
            self.gate_id=_identity(gate._stream.fileno())
        self.config=dict(origin=self.fixture.origin,ca_file=str(self.fixture.cert),
            ca_sha256=hashlib.sha256(self.fixture.cert.read_bytes()).hexdigest(),bearer='a'*32,
            node_id='cluster',worker_id='worker-a',instance_id='instance-a',engine_id='fixture-engine',
            authority_id=self.authority,gate_origin='http://127.0.0.1:18080',gate_generation=1,
            gate_lock_identity=self.gate_id,output_root=str(self.output),output_root_identity=self.output_id,
            output_budget=65536,max_budget=65536,allowed_environments=['production'])

    def make(self,owner,**override):return ViewerNodeEnrollment(owner,**{**self.config,**override})
    def enroll(self,worker,request='enroll-1',environment='production'):
        return worker.enroll(self.fixture.job,request,environment)

    def test_authenticated_tls_gate_and_allocation_idempotent(self):
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner);first=self.enroll(worker)
            self.assertEqual(first['state'],'enrolled_no_dispatch');self.assertEqual(self.enroll(worker),first)
            with self.assertRaises(EnrollmentHeld):self.enroll(worker,'other-request')
        with NodeGate('fixture-engine') as gate:
            state=gate.snapshot();self.assertEqual(state['reservation']['job']['id'],self.fixture.job)
            self.assertIsNone(state['reservation']['completionDigest'])
        names=[p.name for p in self.output.iterdir()]
        self.assertEqual(len([v for v in names if v.endswith('.alloc')]),1)
        self.assertEqual(len([v for v in names if v.endswith('.json')]),1)
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'running')
        self.assertEqual(self.fixture.store.task('22222222-2222-4222-8222-222222222222')['phase'],'worker_claimed')

    def test_wrong_environment_and_worker_or_authority_do_not_bind_gate(self):
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner)
            with self.assertRaises(EnrollmentHeld):self.enroll(worker,environment='staging')
        with NodeGate('fixture-engine') as gate:self.assertIsNone(gate.snapshot()['reservation'])

    def test_wrong_tls_ca_or_worker_identity_is_rejected(self):
        with DaemonStateLock(self.worker) as owner:
            with self.assertRaises(EnrollmentHeld):self.make(owner,ca_sha256='0'*64)
            worker=self.make(owner,worker_id='wrong-worker')
            with self.assertRaises(EnrollmentHeld):self.enroll(worker)
        with NodeGate('fixture-engine') as gate:self.assertIsNone(gate.snapshot()['reservation'])

    def test_wrong_local_node_does_not_bind_gate(self):
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner,node_id='main')
            with self.assertRaises(EnrollmentHeld):self.enroll(worker)
        with NodeGate('fixture-engine') as gate:self.assertIsNone(gate.snapshot()['reservation'])

    def test_wrong_gate_identity_does_not_reserve(self):
        wrong=[self.gate_id[0],self.gate_id[1]+1,self.gate_id[2]]
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner,gate_lock_identity=wrong)
            with self.assertRaisesRegex(EnrollmentHeld,'gate_binding_changed'):self.enroll(worker)
        with NodeGate('fixture-engine') as gate:self.assertIsNone(gate.snapshot()['reservation'])

    def test_wrong_worker_bearer_is_rejected_before_gate_reservation(self):
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner,bearer='z'*32)
            with self.assertRaises(EnrollmentHeld):self.enroll(worker)
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'queued')
        with NodeGate('fixture-engine') as gate:self.assertIsNone(gate.snapshot()['reservation'])

    def test_untrusted_journal_sidecar_is_held(self):
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner)
            sidecar=self.worker/'viewer-enrollment.sqlite-journal'
            sidecar.symlink_to(self.fixture.cert)
            with self.assertRaisesRegex(EnrollmentHeld,'journal_sidecar_invalid'):self.enroll(worker)
            sidecar.unlink()  # test-owned synthetic link only
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'queued')

    def test_disk_failure_retains_gate_and_uncommitted_slot(self):
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner)
            with patch('processing.direct_output_slot.os.posix_fallocate',side_effect=OSError(28,'synthetic ENOSPC')):
                with self.assertRaises(SlotHeld):self.enroll(worker)
            self.assertEqual(len(list(self.output.glob('*.alloc'))),1)
            self.assertEqual(len(list(self.output.glob('*.json'))),0)
            with self.assertRaises(SlotHeld):self.enroll(worker)
        with NodeGate('fixture-engine') as gate:
            self.assertEqual(gate.snapshot()['reservation']['job']['id'],self.fixture.job)
        self.assertEqual(len(list(self.output.glob('*.alloc'))),1)

    def test_concurrent_same_intent_has_one_reservation(self):
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner)
            with ThreadPoolExecutor(max_workers=2) as pool:
                values=[future.result(timeout=10) for future in [pool.submit(self.enroll,worker),pool.submit(self.enroll,worker)]]
            self.assertEqual(values[0],values[1])
        self.assertEqual(len(list(self.output.glob('*.alloc'))),1)

    def test_completed_binding_does_not_recreate_disappeared_gate_reservation(self):
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner);self.enroll(worker)
            with NodeGate('fixture-engine') as gate:
                state=gate.snapshot();state['reservation']=None;gate._save(state)
            with self.assertRaisesRegex(EnrollmentHeld,'reservation_disappeared'):self.enroll(worker)
        with NodeGate('fixture-engine') as gate:self.assertIsNone(gate.snapshot()['reservation'])
        self.assertEqual(len(list(self.output.glob('*.alloc'))),1)

    def test_completed_binding_does_not_rebind_missing_gate_job(self):
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner);self.enroll(worker)
            with NodeGate('fixture-engine') as gate:
                state=gate.snapshot();state['reservation']['job']=None;gate._save(state)
            with self.assertRaisesRegex(EnrollmentHeld,'binding_disappeared'):self.enroll(worker)
        with NodeGate('fixture-engine') as gate:self.assertIsNone(gate.snapshot()['reservation']['job'])

    def test_committed_slot_deleted_never_recreated(self):
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner);self.enroll(worker)
            for path in self.output.iterdir():path.unlink()  # only this test's synthetic slot
            with self.assertRaises(SlotHeld):self.enroll(worker)
            self.assertEqual(list(self.output.iterdir()),[])
        with NodeGate('fixture-engine') as gate:self.assertIsNotNone(gate.snapshot()['reservation'])

    def test_committed_slot_replaced_with_valid_fresh_pair_is_rejected(self):
        from processing.direct_output_slot import open_slot
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner);result=self.enroll(worker)
            for path in self.output.iterdir():path.rename(path.with_name(path.name+'.retained'))
            slot=open_slot(self.output,self.output_id,result['bindingSha256'],65536,max_budget=65536,create=True);slot.close()
            with self.assertRaisesRegex(EnrollmentHeld,'slot_identity_changed'):self.enroll(worker)

    def _kill_boundary(self,phase):
        ready=self.root/('child-'+phase)
        code='''import json,sys,time
from pathlib import Path
from ops.reference_worker_daemon import DaemonStateLock
import processing.viewer_node_enrollment as module
from processing.node_gate import NodeGate
root,config,job,ready,phase=sys.argv[1:];config=json.loads(config)
def pause():Path(ready).write_text('ready');time.sleep(30)
with DaemonStateLock(root) as owner:
 worker=module.ViewerNodeEnrollment(owner,**config)
 if phase=='slot-committed':
  from contextlib import contextmanager
  actual=worker._db
  @contextmanager
  def intercept(*args,**kwargs):
   phase_now=None
   with actual(*args,**kwargs) as db:
    yield db
    row=db.execute('SELECT slot_phase FROM intent WHERE singleton=1').fetchone()
    if row:phase_now=row[0]
   if phase_now=='committed':pause()
  worker._db=intercept
 if phase=='journal':
  from contextlib import contextmanager
  actual=worker._db
  @contextmanager
  def intercept(*args,**kwargs):
   with actual(*args,**kwargs) as db:
    yield db
    row=db.execute('SELECT claim,binding FROM intent WHERE singleton=1').fetchone()
    if row and row[0] is not None and row[1] is None:pause()
  worker._db=intercept
 if phase=='claim':
  actual=worker._validate_claim
  def intercept(*args):
   result=actual(*args);pause();return result
  worker._validate_claim=intercept
 if phase=='gate-reserve':
  actual=NodeGate.reserve
  def intercept(self,*args):
   result=actual(self,*args);pause();return result
  NodeGate.reserve=intercept
 if phase=='gate':
  actual=NodeGate.bind
  def intercept(self,*args):
   result=actual(self,*args);pause();return result
  NodeGate.bind=intercept
 if phase in ('slot','slot-intent'):
  actual=module.open_slot
  def intercept(*args,**kwargs):
   if phase=='slot-intent':pause()
   result=actual(*args,**kwargs);pause();return result
  module.open_slot=intercept
 worker.enroll(job,'enroll-1','production')
'''
        child=subprocess.Popen([sys.executable,'-c',code,str(self.worker),json.dumps(self.config),self.fixture.job,str(ready),phase],
                               stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            end=time.monotonic()+8
            while not ready.exists() and time.monotonic()<end:
                if child.poll() is not None:self.fail('node child exited before barrier')
                time.sleep(.02)
            self.assertTrue(ready.exists());child.kill();child.wait(timeout=5)
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=5)
        with DaemonStateLock(self.worker) as owner:
            worker=self.make(owner)
            if phase=='slot-intent':
                with self.assertRaises(SlotHeld):self.enroll(worker)
                self.assertEqual(list(self.output.iterdir()),[])
            else:
                result=self.enroll(worker)
                self.assertEqual(result['state'],'enrolled_no_dispatch');self.assertEqual(self.enroll(worker),result)
        with NodeGate('fixture-engine') as gate:
            self.assertEqual(gate.snapshot()['reservation']['job']['id'],self.fixture.job)
        self.assertEqual(len(list(self.output.glob('*.alloc'))),0 if phase=='slot-intent' else 1)

    def test_kill_after_claim_before_local_record(self):self._kill_boundary('claim')
    def test_kill_during_local_claim_commit(self):self._kill_boundary('journal')
    def test_kill_after_gate_reserve_before_bind(self):self._kill_boundary('gate-reserve')
    def test_kill_after_gate_bind_before_allocation(self):self._kill_boundary('gate')
    def test_kill_after_slot_intent_before_creation_holds(self):self._kill_boundary('slot-intent')
    def test_kill_after_allocation_before_return(self):self._kill_boundary('slot')
    def test_kill_after_committed_slot_marker_revalidates(self):self._kill_boundary('slot-committed')
