"""Synthetic process-death checks for a provisional, non-adopting receiver."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from processing.scratch_policy import ScratchLedger,PolicyConflict
from processing.reference_allocated_receiver import receive,root_proof,validate_root_proof
from ops import bootstrap_worker_input_accounting as bootstrap
from ops import reference_worker_daemon as daemon
from tests import test_reference_worker_input_bootstrap as fixtures
from tests import test_reference_driver_indexed_fetch as driver_fixtures


@unittest.skipUnless(os.name=='posix' and hasattr(os,'fork') and hasattr(os,'posix_fallocate'),'Linux process and allocation required')
class PublicationCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700)
        self.kw=dict(capacity_probe=lambda _:10**9,clock=lambda:1,safety_floor_bytes=0,
                     input_copy_accounting=True,allocated_receiver=True,allocated_receiver_version=2)
        self.ledger=ScratchLedger(self.root/'ledger.sqlite',self.root,**self.kw)
        self.ledger.enable_input_copy_accounting();self.ledger.enable_allocated_receiver()
        self.ledger.reserve(job_id='job',generation=1,reservation_id='res',manifest_sha256='a'*64,
            input_bytes=3,output_budget=3,environment='staging',ttl_seconds=300,worker_ready=True)
        self.digest=hashlib.sha256(b'abc').hexdigest()
        self.identity=dict(jobId='job',generation=1,environment='staging',manifestSha256='a'*64,
            path='jobs/res/input',sha256=self.digest,byteSize=3)
        self.ledger.register_input_binding('res',job_id='job',generation=1,environment='staging',
            manifest_sha256='a'*64,pipeline_sha256='b'*64,
            objects=[dict(object_id='input',byte_size=3,sha256=self.digest,relative_path='jobs/res/input')])
        self.parent=self.root/'jobs'/'res';self.parent.mkdir(parents=True,mode=0o700)

    class Broker:
        def download_indexed_object_fd(self,grant,object_id,fd):
            os.pwrite(fd,b'abc',0);os.fsync(fd)

    def receipt(self):
        with closing(sqlite3.connect(self.root/'ledger.sqlite')) as db:
            row=db.execute('SELECT identity,state,dev,ino,mount,dirdev,dirino,dirmount FROM allocated_input_receipts').fetchone()
            return json.loads(row[0]),row[1:]

    def test_root_identity_and_publication_checkpoints_persist(self):
        receive(self.ledger,self.Broker(),None,'res','input','1'*32,self.identity,3)
        identity,row=self.receipt()
        self.assertEqual(row[0],'published');self.assertEqual(identity['rootProof'],root_proof(self.root))
        self.assertEqual((self.parent/'input').read_bytes(),b'abc')
        with self.assertRaises(PolicyConflict):
            receive(self.ledger,self.Broker(),None,'res','input','1'*32,self.identity,3)

    def test_changed_root_identity_is_rejected(self):
        proof=root_proof(self.root);held=self.root.with_name(self.root.name+'-held')
        self.root.rename(held);self.root.mkdir(mode=0o700)
        try:
            with self.assertRaises(ValueError):validate_root_proof(proof,self.root)
        finally:self.root.rmdir();held.rename(self.root)

    def assert_crash(self,phase,expected,part_exists,final_exists):
        child=os.fork()
        if child==0:
            try:
                if phase=='link':
                    real=os.link
                    def stop(*args,**kwargs):real(*args,**kwargs);os._exit(71)
                    with patch('processing.reference_allocated_receiver.os.link',side_effect=stop):
                        receive(self.ledger,self.Broker(),None,'res','input','1'*32,self.identity,3)
                elif phase in ('linked','unlinked'):
                    real=self.ledger.checkpoint_allocated_publication
                    def stop(*args):
                        result=real(*args)
                        if args[-1]==phase:os._exit(71)
                        return result
                    with patch.object(self.ledger,'checkpoint_allocated_publication',side_effect=stop):
                        receive(self.ledger,self.Broker(),None,'res','input','1'*32,self.identity,3)
                else:
                    real=self.ledger.finish_allocated_input
                    def stop(*args):real(*args);os._exit(71)
                    with patch.object(self.ledger,'finish_allocated_input',side_effect=stop):
                        receive(self.ledger,self.Broker(),None,'res','input','1'*32,self.identity,3)
            finally:os._exit(72)
        _,status=os.waitpid(child,0);self.assertEqual(os.waitstatus_to_exitcode(status),71)
        ledger=ScratchLedger(self.root/'ledger.sqlite',self.root,**self.kw)
        identity,row=self.receipt();self.assertEqual(row[0],expected)
        self.assertEqual(identity['rootProof'],root_proof(self.root))
        self.assertEqual((self.parent/('.allocated-'+'1'*32+'.part')).exists(),part_exists)
        self.assertEqual((self.parent/'input').exists(),final_exists)
        if final_exists:self.assertEqual((self.parent/'input').read_bytes(),b'abc')
        with self.assertRaises(PolicyConflict):receive(ledger,self.Broker(),None,'res','input','1'*32,self.identity,3)

    def test_death_after_link_before_checkpoint_holds(self):self.assert_crash('link','publishing',True,True)
    def test_death_after_linked_checkpoint_holds(self):self.assert_crash('linked','linked',True,True)
    def test_death_after_unlinked_checkpoint_holds(self):self.assert_crash('unlinked','unlinked',False,True)
    def test_lost_finish_reply_holds(self):self.assert_crash('finished','published',False,True)


@unittest.skipUnless(os.name=='posix','Linux bootstrap identity required')
class PublicationBootstrapTests(unittest.TestCase):
    def setUp(self):
        fixtures.InputBootstrapTests.setUp(self)
        self.config['inputCopyAccounting']={'schemaVersion':6,'fetchJournal':{'schemaVersion':1,
            'policyId':'indexed-worker-fetch-three-retained-v1'},'allocatedReceiver':{'schemaVersion':2}}

    def test_fresh_profile_and_previous_profile_cannot_be_interchanged(self):
        bootstrap.bootstrap_input_copy_accounting(self.config)
        before=(self.state/'scratch.sqlite').read_bytes()
        with closing(sqlite3.connect(self.state/'scratch.sqlite')) as db:
            value=json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])
            self.assertEqual(value['allocatedReceiverVersion'],2)
        previous=dict(self.config,inputCopyAccounting={'schemaVersion':5,'fetchJournal':self.config['inputCopyAccounting']['fetchJournal'],
            'allocatedReceiver':{'schemaVersion':1}})
        with daemon.DaemonStateLock(self.state) as owner:
            self.assertTrue(bootstrap.require_worker_input_profile(self.config,owner))
            with self.assertRaises(ValueError):bootstrap.require_worker_input_profile(previous,owner)
        self.assertEqual((self.state/'scratch.sqlite').read_bytes(),before)

    def test_unknown_or_bool_receiver_version_refused(self):
        for value in (True,0,3):
            candidate=dict(self.config['inputCopyAccounting'],allocatedReceiver={'schemaVersion':value})
            with self.subTest(value=value),self.assertRaises(ValueError):bootstrap.input_profile({'inputCopyAccounting':candidate})


@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux indexed receiver required')
class PublicationDriverTests(unittest.TestCase):
    make_driver=driver_fixtures.IndexedDriverTests.make_driver
    def setUp(self):driver_fixtures.IndexedDriverTests.setUp(self)

    def test_actual_indexed_driver_uses_versioned_receiver(self):
        self.f.ledger.allocated_receiver=True;self.f.ledger.allocated_receiver_version=2
        self.f.ledger.enable_allocated_receiver()
        job=driver_fixtures.IndexedDriverTests.fetching(self,contents=(b'hello',))
        self.assertEqual(self.f.driver.step()['state'],'fetching')
        with self.f.ledger._transaction() as db:
            row=db.execute('SELECT identity,state FROM allocated_input_receipts').fetchone()
        self.assertEqual(row['state'],'published')
        self.assertEqual(json.loads(row['identity'])['rootProof'],root_proof(self.f.scratch))
        self.assertEqual(self.f.driver.step()['state'],'processing_pending')
