"""Synthetic journal identity tests; no service, live data or filesystem mutation."""
import json
import sqlite3
import unittest
from unittest.mock import patch
from processing import reference_transfer_journal_candidate as journal
from processing.reference_protocol import canonical_json

class ReferenceTransferJournalCandidateTests(unittest.TestCase):
    def test_ten_thousand_distinct_bound_records_and_idempotent_charge(self):
        db=sqlite3.connect(':memory:')
        self.addCleanup(db.close)
        with patch.object(journal,'QUALIFIED',True), db:
            journal.install(db,enabled=True)
            binding=dict(job='synthetic-job',generation=1,manifest_sha='a'*64,reservation='synthetic-reservation',directory_device=1,directory_inode=2)
            journal.bind(db,**binding)
            for i in range(10000):
                self.assertEqual(journal.pending(db,job='synthetic-job',object_id=f'image-{i:05}',size=3,sha256='b'*64,path=f'jobs/file-{i:05}'),'pending')
            identity=canonical_json(dict(objectId='image-09999',size=3,sha256='b'*64,path='jobs/file-09999')).decode()
            self.assertEqual(journal.verified(db,job='synthetic-job',object_id='image-09999',identity=identity),3)
            self.assertEqual(journal.verified(db,job='synthetic-job',object_id='image-09999',identity=identity),3)
            with self.assertRaises(journal.TransferJournalHeld):
                journal.charged(db,job='synthetic-job',object_id='image-09999',identity=identity,observed_allocated=2)
            journal.charged(db,job='synthetic-job',object_id='image-09999',identity=identity,observed_allocated=3)
            with self.assertRaises(journal.TransferJournalHeld):
                journal.bind(db,**dict(binding,generation=2))
            with self.assertRaises(journal.TransferJournalHeld):
                journal.pending(db,job='synthetic-job',object_id='image-09999',size=4,sha256='b'*64,path='jobs/file-09999')
            self.assertEqual(db.execute('SELECT COUNT(*) FROM transfer_journal_object').fetchone()[0],10000)
