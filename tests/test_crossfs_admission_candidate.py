"""Synthetic cooperative accounting tests; no live mounts or NAS."""
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from processing.crossfs_admission_candidate import CrossFsAdmission,CrossFsHeld
from processing.scratch_policy import ScratchLedger

@unittest.skipUnless(os.name=='posix' and Path('/dev/shm').is_dir(), 'two local filesystems required')
class CrossFsAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.qualify=patch('processing.crossfs_admission_candidate.CROSSFS_QUALIFIED',True)
        self.qualify.start(); self.addCleanup(self.qualify.stop)
        self.a=Path(tempfile.mkdtemp(prefix='lt-crossfs-a-'))
        self.b=Path(tempfile.mkdtemp(prefix='lt-crossfs-b-',dir='/dev/shm'))
        print('Retained synthetic roots:',self.a,self.b,flush=True)
        if self.a.stat().st_dev==self.b.stat().st_dev:self.skipTest('distinct local devices required')
        self.now=[1000.0]
        self.la=ScratchLedger(self.a/'ledger.sqlite',self.a,capacity_probe=lambda _:10000,safety_floor_bytes=100,clock=lambda:self.now[0])
        self.lb=ScratchLedger(self.b/'ledger.sqlite',self.b,capacity_probe=lambda _:10000,safety_floor_bytes=100,clock=lambda:self.now[0])
        self.journal=self.a/'cross.sqlite'
        self.canonical=patch('processing.crossfs_admission_candidate.CROSSFS_CANONICAL_JOURNAL',self.journal)
        self.canonical.start(); self.addCleanup(self.canonical.stop)
        self.gate=CrossFsAdmission(self.journal,{'a':self.la,'b':self.lb})
        self.args=dict(attempt_id='test-attempt',job_id='job',generation=1,manifest_sha256='a'*64,
                       environment='staging',ttl_seconds=60,demands=[{'ledger':'a','bytes':100},{'ledger':'a','bytes':200},{'ledger':'b','bytes':400}])

    def test_replaced_journal_holds(self):
        # Both paths belong to this retained synthetic root; no file deleted.
        os.rename(self.journal,self.a/'retained-original.sqlite')
        self.journal.touch(mode=0o600)
        with self.assertRaisesRegex(CrossFsHeld,'identity changed'):
            self.gate.acquire(**self.args)

    def test_group_and_idempotent_replay(self):
        self.assertEqual(self.gate.acquire(**self.args)['state'],'admitted')
        self.assertEqual(self.gate.acquire(**self.args)['state'],'admitted')
        with self.gate._db() as db:
            self.assertEqual(sorted(x[0] for x in db.execute('select bytes from parts')), [300,400])
        with self.assertRaises(CrossFsHeld):self.gate.acquire(**dict(self.args,demands=[{'ledger':'a','bytes':9}]))

    def test_lost_ack_replays_and_low_capacity_keeps_partial(self):
        real=self.lb.reserve
        def lost(**kwargs):
            real(**kwargs)
            raise OSError('synthetic lost acknowledgement')
        with patch.object(self.lb,'reserve',side_effect=lost):
            with self.assertRaises(CrossFsHeld):self.gate.acquire(**self.args)
        with self.gate._db() as db:
            self.assertEqual(db.execute('select state from attempts').fetchone()[0],'acquiring')
        self.assertEqual(self.gate.acquire(**self.args)['state'],'admitted')
        other=dict(self.args,attempt_id='other',job_id='other',demands=[{'ledger':'a','bytes':9999}])
        with self.assertRaises(CrossFsHeld):self.gate.acquire(**other)

    def test_expired_before_pin_holds_instead_of_admission(self):
        real=self.la.set_engine_state
        def expire_before_pin(*args,**kwargs):
            self.now[0]+=61
            return real(*args,**kwargs)
        with patch.object(self.la,'set_engine_state',side_effect=expire_before_pin):
            with self.assertRaises(CrossFsHeld):self.gate.acquire(**self.args)
        with self.gate._db() as db:
            self.assertEqual(db.execute('select state from attempts').fetchone()[0],'acquiring')

    def test_profile_disabled_without_explicit_test_patch(self):
        with patch('processing.crossfs_admission_candidate.CROSSFS_QUALIFIED',False):
            with self.assertRaises(CrossFsHeld):CrossFsAdmission(self.a/'disabled.sqlite',{'a':self.la})

    def test_independent_instances_share_device_authority(self):
        other=ScratchLedger(self.a/'other-ledger.sqlite',self.a,capacity_probe=lambda _:10000,safety_floor_bytes=100,clock=lambda:self.now[0])
        first=CrossFsAdmission(self.journal,{'a':self.la})
        second=CrossFsAdmission(self.journal,{'a':other})
        args=dict(self.args,attempt_id='one',job_id='one',demands=[{'ledger':'a','bytes':6000}])
        self.assertEqual(first.acquire(**args)['state'],'admitted')
        with self.assertRaisesRegex(CrossFsHeld,'another ledger'):
            second.acquire(**dict(args,attempt_id='two',job_id='two'))
        with self.assertRaisesRegex(CrossFsHeld,'canonical'):
            CrossFsAdmission(self.a/'second.sqlite',{'a':other})

    def test_competing_attempts_cannot_overcommit_one_ledger(self):
        results=[]
        def run(number):
            try:
                args=dict(self.args,attempt_id='t'+str(number),job_id='j'+str(number),
                          demands=[{'ledger':'a','bytes':6000}])
                self.gate.acquire(**args);results.append('admitted')
            except CrossFsHeld:results.append('held')
        threads=[threading.Thread(target=run,args=(i,)) for i in range(2)]
        for thread in threads:thread.start()
        for thread in threads:thread.join(15)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertCountEqual(results,['admitted','held'])
