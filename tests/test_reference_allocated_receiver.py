import hashlib
import os
from pathlib import Path
import tempfile
import unittest

from processing.scratch_policy import ScratchLedger, PolicyConflict


class AllocatedLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name); self.now=1
        self.kw=dict(capacity_probe=lambda _:10**9,clock=lambda:self.now,safety_floor_bytes=0,
                     input_copy_accounting=True,allocated_receiver=True)
        self.ledger=ScratchLedger(self.root/'ledger.sqlite',self.root,**self.kw)
        self.ledger.enable_input_copy_accounting(); self.ledger.enable_allocated_receiver()
        self.ledger.reserve(job_id='job',generation=1,reservation_id='res',manifest_sha256='a'*64,
            input_bytes=3,output_budget=3,environment='staging',ttl_seconds=2,worker_ready=True)
        digest=hashlib.sha256(b'abc').hexdigest()
        self.identity=dict(jobId='job',generation=1,environment='staging',manifestSha256='a'*64,
            path='jobs/res/input',sha256=digest,byteSize=3)
        self.ledger.register_input_binding('res',job_id='job',generation=1,environment='staging',
            manifest_sha256='a'*64,pipeline_sha256='b'*64,
            objects=[dict(object_id='input',byte_size=3,sha256=digest,relative_path='jobs/res/input')])

    def test_intent_holds_capacity_across_expiry_and_restart(self):
        self.ledger.begin_allocated_input('res','input','1'*32,self.identity)
        self.now=100
        ledger=ScratchLedger(self.root/'ledger.sqlite',self.root,**self.kw)
        with ledger._transaction() as db:
            self.assertEqual(ledger._reservation(db,'res')['state'],'receiving')
        with self.assertRaises(PolicyConflict): ledger.set_engine_state('res','engine_started')
        with self.assertRaises(PolicyConflict): ledger.begin_allocated_input('res','input','1'*32,self.identity)

    def test_completion_accounts_before_releasing_hold(self):
        self.ledger.begin_allocated_input('res','input','1'*32,self.identity)
        self.ledger.record_allocated_input_inode('res','input','1'*32,3,4,5,6,7,5)
        self.now=100
        self.ledger.finish_allocated_input('res','input','1'*32,3,4,5,6,7,5,3)
        with self.ledger._transaction() as db:
            row=self.ledger._reservation(db,'res')
            self.assertEqual((row['state'],row['allocated'],row['remaining']),('reserved',3,3))

    def test_profile_required_after_irreversible_latch(self):
        with self.assertRaises(PolicyConflict):
            ScratchLedger(self.root/'ledger.sqlite',self.root,input_copy_accounting=True,
                capacity_probe=lambda _:10**9,clock=lambda:self.now,safety_floor_bytes=0)

    @unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux receiver required')
    def test_actual_allocated_inode_success_and_interruption(self):
        from processing.reference_allocated_receiver import receive
        parent=self.root/'jobs'/'res'; parent.mkdir(parents=True,mode=0o700)
        class Broker:
            def download_indexed_object_fd(self,grant,object_id,fd):
                self.inode=os.fstat(fd).st_ino
                os.pwrite(fd,b'abc',0); os.fsync(fd)
        broker=Broker()
        receive(self.ledger,broker,None,'res','input','1'*32,self.identity,3)
        self.assertEqual((parent/'input').read_bytes(),b'abc')
        self.assertEqual((parent/'input').stat().st_ino,broker.inode)
        self.assertFalse((parent/('.allocated-'+'1'*32+'.part')).exists())

    @unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux receiver required')
    def test_failed_transfer_retains_part_and_receiving_hold(self):
        from processing.reference_allocated_receiver import receive
        parent=self.root/'jobs'/'res'; parent.mkdir(parents=True,mode=0o700)
        class Broker:
            def download_indexed_object_fd(self,grant,object_id,fd):
                os.pwrite(fd,b'a',0); os.fsync(fd); raise OSError('synthetic interruption')
        with self.assertRaises(OSError):
            receive(self.ledger,Broker(),None,'res','input','1'*32,self.identity,3)
        self.assertTrue((parent/('.allocated-'+'1'*32+'.part')).exists())
        self.assertFalse((parent/'input').exists())
        with self.ledger._transaction() as db:
            self.assertEqual(self.ledger._reservation(db,'res')['state'],'receiving')
        with self.assertRaises(PolicyConflict):
            self.ledger.begin_allocated_input('res','input','1'*32,self.identity)

    @unittest.skipUnless(os.name=='posix' and hasattr(os,'fork') and hasattr(os,'posix_fallocate'),'Linux fork required')
    def test_process_death_after_partial_write_holds_on_restart(self):
        from processing.reference_allocated_receiver import receive
        parent=self.root/'jobs'/'res'; parent.mkdir(parents=True,mode=0o700)
        class Broker:
            def download_indexed_object_fd(self,grant,object_id,fd):
                os.pwrite(fd,b'a',0); os.fsync(fd); os._exit(73)
        child=os.fork()
        if child==0:
            try:
                receive(self.ledger,Broker(),None,'res','input','1'*32,self.identity,3)
            finally:
                os._exit(74)
        _,status=os.waitpid(child,0)
        self.assertEqual(os.waitstatus_to_exitcode(status),73)
        self.now=100
        ledger=ScratchLedger(self.root/'ledger.sqlite',self.root,**self.kw)
        with ledger._transaction() as db:
            self.assertEqual(ledger._reservation(db,'res')['state'],'receiving')
        self.assertTrue((parent/('.allocated-'+'1'*32+'.part')).exists())
        with self.assertRaises(PolicyConflict):
            ledger.begin_allocated_input('res','input','1'*32,self.identity)


if __name__=='__main__': unittest.main()
