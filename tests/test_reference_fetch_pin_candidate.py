"""Synthetic fetch pin tests. No service, external disk or client data."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing.scratch_policy import ScratchLedger, PolicyConflict
from processing import reference_fetch_pin_candidate as pin

@unittest.skipUnless(os.name=='posix','POSIX owned pin required')
class ReferenceFetchPinCandidateTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.scratch=self.root/'scratch';self.scratch.mkdir(mode=0o700)
        self.now=100
        self.ledger=ScratchLedger(self.root/'ledger.sqlite',self.scratch,capacity_probe=lambda _:10**9,clock=lambda:self.now,safety_floor_bytes=0)
        self.owner=DaemonStateLock(self.root);self.owner.__enter__();self.addCleanup(self.owner.__exit__,None,None,None)
        self.patcher=patch.object(pin,'QUALIFIED',True);self.patcher.start();self.addCleanup(self.patcher.stop)
        pin.install(self.ledger,self.owner,enabled=True)
        self.ledger.reserve(job_id='job-1',generation=1,reservation_id='reservation-1',manifest_sha256='a'*64,
            input_bytes=100,output_budget=50,environment='staging',ttl_seconds=10,worker_ready=True)
        directory=self.ledger.ensure_job_directory('reservation-1');self.directory=directory;info=directory.stat()
        self.args=dict(reservation='reservation-1',job='job-1',generation=1,environment='staging',manifest_sha256='a'*64,
                       directory_device=info.st_dev,directory_inode=info.st_ino)
    def test_pin_survives_ttl_and_refuses_changed_directory_or_identity(self):
        identity=pin.acquire(self.ledger,self.owner,**self.args)
        self.now=200
        with self.ledger._transaction() as db:
            self.ledger._expire(db,self.ledger._now(db))
            self.assertEqual(db.execute('SELECT state FROM reservations WHERE id=?',('reservation-1',)).fetchone()[0],'reserved')
            self.assertEqual(db.execute('SELECT state FROM reference_fetch_pins').fetchone()[0],'active')
        self.assertEqual(pin.acquire(self.ledger,self.owner,**self.args),identity)
        with self.assertRaises(PolicyConflict):pin.acquire(self.ledger,self.owner,**dict(self.args,directory_inode=self.args['directory_inode']+1))
        with self.assertRaises(PolicyConflict):pin.acquire(self.ledger,self.owner,**dict(self.args,manifest_sha256='b'*64))
    def test_release_remains_unqualified(self):
        identity=pin.acquire(self.ledger,self.owner,**self.args)
        with self.assertRaises(PolicyConflict):pin.release_after_custody(self.ledger,self.owner,None,reservation='reservation-1')
    def test_disabled_support_fails_closed(self):
        with patch.object(pin,'QUALIFIED',False),self.assertRaises(PolicyConflict):
            with self.ledger._transaction() as db:self.ledger._expire(db,200)

    def test_active_pin_blocks_controlled_cleanup_and_schema_loss_holds(self):
        pin.acquire(self.ledger,self.owner,**self.args)
        with self.ledger._transaction() as db:
            db.execute("UPDATE reservations SET state='terminal',terminal_at=1 WHERE id='reservation-1'")
            db.execute("INSERT INTO objects VALUES('reservation-1','synthetic-output',1,?,'jobs/reservation-1/output',1,NULL)",('b'*64,))
        self.assertEqual(self.ledger.cleanup_candidates(),[])
        with self.assertRaises(PolicyConflict):
            self.ledger.begin_cleanup('reservation-1','synthetic-output',generation=1,sha256='b'*64)
        with patch.object(pin,'QUALIFIED',False),self.assertRaises(PolicyConflict):
            self.ledger.cleanup_candidates()
        with self.ledger._transaction() as db:db.execute('DROP TABLE reference_fetch_pins')
        with self.assertRaises(PolicyConflict):self.ledger.cleanup_candidates()
        with self.assertRaises(PolicyConflict):
            with self.ledger._transaction() as db:self.ledger._expire(db,200)
