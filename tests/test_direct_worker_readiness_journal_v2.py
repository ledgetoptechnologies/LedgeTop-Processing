import os
import unittest
import uuid
import subprocess
import sys
import json
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch

from processing import direct_worker_readiness_journal_v2 as journal
from processing.scratch_policy import ScratchLedger, PolicyConflict
from tests.test_direct_worker_receiver_v2 import WorkerV2Tests


@unittest.skipIf(os.name=='nt','Linux scratch fixture required')
class ReadinessJournalTests(unittest.TestCase):
    def setup_fixture(self):
        f=WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        f.ledger._retire_direct_writer(f.observation['scratchReservationId'],f.readiness['body']['pinId'])
        kwargs=dict(principal='coordinator-a',expected_worker=f.worker,engine_id='fixture-v2',
            coordinator_origin='https://127.0.0.1:8443',gate_generation=1,challenge_id=str(uuid.uuid4()),
            validity_seconds=10,issuer_id='worker-issuer',key_id='worker-key',private_key=f.proof_key)
        return f,kwargs

    def test_exact_proof_replayed_after_ledger_reopen(self):
        f,kwargs=self.setup_fixture()
        proof=journal.issue(f.ledger,f.spool,f.item,**kwargs)
        from cryptography.hazmat.primitives import serialization
        private=f.proof_key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,
                                          serialization.NoEncryption()).decode()
        payload=dict(root=str(f.root),spool=str(f.spool.root),marker=f.spool.marker.name,
                     item=f.item,worker=f.worker,challenge=kwargs['challenge_id'],key=private)
        script=Path(__file__).parent/'fixtures'/'direct_readiness_journal_replay.py'
        result=subprocess.run([sys.executable,str(script)],input=json.dumps(payload),text=True,
            capture_output=True,timeout=10,check=True,env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[1])))
        self.assertEqual(json.loads(result.stdout),proof)
        ledger=ScratchLedger(f.root/'private'/'ledger.sqlite',f.root,
            capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)
        self.assertEqual(journal.issue(ledger,f.spool,f.item,**kwargs),proof)
        with self.assertRaises(PolicyConflict):journal.issue(ledger,f.spool,f.item,**dict(kwargs,principal='coordinator-b'))
        with ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_writer_pins').fetchone()[0],1)

    def test_issue_crash_retains_uncertain_intent_and_pin(self):
        f,kwargs=self.setup_fixture();original=journal.issue_fenced
        def issue_then_fail(*args,**opts):
            original(*args,**opts)
            raise RuntimeError('lost before journal commit')
        with patch.object(journal,'issue_fenced',side_effect=issue_then_fail):
            with self.assertRaises(journal.ReadinessIssueUncertain):journal.issue(f.ledger,f.spool,f.item,**kwargs)
        ledger=ScratchLedger(f.root/'private'/'ledger.sqlite',f.root,
            capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)
        with self.assertRaises(journal.ReadinessIssueUncertain):journal.issue(ledger,f.spool,f.item,**kwargs)
        with ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_writer_pins').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT state FROM direct_readiness_issues').fetchone()[0],'issuing')

    def test_replay_refuses_missing_pin_and_expired_proof(self):
        f,kwargs=self.setup_fixture();proof=journal.issue(f.ledger,f.spool,f.item,**kwargs)
        with patch('processing.direct_worker_fence_proof_v2.time.time',return_value=1000):
            with self.assertRaises(journal.ReadinessIssueUncertain):journal.issue(f.ledger,f.spool,f.item,**kwargs)
        with f.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_writer_pins').fetchone()[0],1)
        f.ledger._retire_direct_writer(f.observation['scratchReservationId'],proof['body']['pinId'])
        with self.assertRaises(journal.ReadinessIssueUncertain):journal.issue(f.ledger,f.spool,f.item,**kwargs)

    def test_intent_commit_outcome_is_uncertain_and_replay_holds(self):
        f,kwargs=self.setup_fixture();original=f.ledger._transaction;calls=[]
        @contextmanager
        def lose_commit_reply(*args,**options):
            with original(*args,**options) as db:
                yield db
            calls.append(True)
            if len(calls)==2:raise RuntimeError('synthetic committed intent teardown failure')
        with patch.object(f.ledger,'_transaction',side_effect=lose_commit_reply):
            with self.assertRaises(journal.ReadinessIssueUncertain):journal.issue(f.ledger,f.spool,f.item,**kwargs)
        with self.assertRaises(journal.ReadinessIssueUncertain):journal.issue(f.ledger,f.spool,f.item,**kwargs)
        with f.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT state FROM direct_readiness_issues').fetchone()[0],'issuing')
