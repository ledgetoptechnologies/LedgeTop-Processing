"""Experimental journal tests; no worker runtime or ledger integration."""
import copy
import hashlib
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

from processing import reference_protocol as p
from processing.reference_input_journal import InputJournal, InputJournalError
from processing.scratch_policy import ScratchLedger


class InputJournalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'journal.sqlite'
        self.db = sqlite3.connect(self.path)
        self.journal = InputJournal(self.db)
        self.objects = [dict(id='image-'+str(i), byteSize=i+1, sha256=('%064x' % (i+1))) for i in range(3)]
        self.binding = dict(jobId=str(uuid.uuid4()), environment='staging', generation=1, maxAttempts=3,
                            manifestSha256='a'*64, objectsSha256=p.digest(self.objects))

    def tearDown(self):
        self.db.close(); self.tmp.cleanup()

    def reopen(self):
        self.db.close(); self.db = sqlite3.connect(self.path); self.journal = InputJournal(self.db)
        self.assertFalse(self.journal.seed(self.binding, self.objects))

    def test_v2_manifest_digest_and_projection_are_bound_on_seed(self):
        from processing.reference_manifest_v2 import decode_manifest_v2
        raw = (Path(__file__).resolve().parents[1] / 'contracts/input-v2-manifest-canonical.json').read_bytes().strip()
        manifest = decode_manifest_v2(raw)
        objects = [{key: obj[key] for key in ('id', 'byteSize', 'sha256')} for obj in manifest['objects']]
        self.binding.update(manifestSha256=manifest['sha256'], objectsSha256=p.digest(objects))
        with self.assertRaises(InputJournalError):
            self.journal.seed_manifest(dict(self.binding, manifestSha256='f'*64), raw)
        with self.assertRaises(InputJournalError):
            self.journal.seed_manifest(dict(self.binding, objectsSha256='f'*64), raw)
        self.assertTrue(self.journal.seed_manifest(self.binding, raw))
        self.assertEqual(self.journal.current(self.binding)['byteSize'], 0)
        with self.assertRaises(InputJournalError):
            self.journal.seed_manifest(self.binding, raw.replace(b'empty.jpg', b'alter.jpg'))

    def test_v2_zero_size_reopens_after_attempt_and_completion(self):
        self.objects = [dict(id='empty', byteSize=0, sha256=hashlib.sha256(b'').hexdigest())]
        self.binding['objectsSha256'] = p.digest(self.objects)
        self.journal.seed(self.binding, self.objects)
        item = self.journal.current(self.binding)
        self.assertEqual(item['target'], 0)
        self.journal.attempt(self.binding, 0)
        self.reopen()
        self.assertEqual(self.journal.current(self.binding)['attempts'], 1)
        self.journal.complete(self.binding, 0, 'empty', 0, self.objects[0]['sha256'], 0)
        self.reopen()
        self.assertIsNone(self.journal.current(self.binding))

    def test_v2_maximum_object_count_and_overflow(self):
        self.objects = [dict(id='i'+str(i), byteSize=0, sha256=hashlib.sha256(b'').hexdigest()) for i in range(32768)]
        self.binding['objectsSha256'] = p.digest(self.objects)
        self.assertTrue(self.journal.seed(self.binding, self.objects))
        self.reopen()
        self.assertEqual(self.journal.current(self.binding)['objectId'], 'i0')
        extra = self.objects + [dict(id='extra', byteSize=0, sha256='0'*64)]
        with self.assertRaises(InputJournalError):
            self.journal.seed(dict(self.binding, objectsSha256=p.digest(extra)), extra)

    def test_trigger_and_extra_index_fail_closed_after_validated_seed(self):
        self.journal.seed(self.binding, self.objects)
        self.db.execute('CREATE TRIGGER malicious AFTER UPDATE ON input_journal_objects BEGIN UPDATE input_journal_jobs SET allocated=999; END')
        self.db.commit()
        with self.assertRaisesRegex(InputJournalError, 'schema mismatch'):
            self.journal.attempt(self.binding, 0)
        self.assertEqual(self.db.execute('SELECT allocated FROM input_journal_jobs').fetchone()[0], 0)
        self.db.execute('DROP TRIGGER malicious')
        self.db.execute('CREATE INDEX unexpected ON input_journal_objects(size)')
        self.db.commit()
        with self.assertRaisesRegex(InputJournalError, 'schema mismatch'):
            self.journal.current(self.binding)

    def test_altered_existing_table_rejected_before_initialization(self):
        db = sqlite3.connect(Path(self.tmp.name) / 'tampered.sqlite')
        try:
            db.execute('CREATE TABLE input_journal_jobs (job TEXT PRIMARY KEY)')
            db.commit()
            with self.assertRaisesRegex(InputJournalError, 'schema mismatch'):
                InputJournal(db)
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='input_journal_objects'").fetchone())
        finally:
            db.close()

    def test_seed_replay_and_conflict_do_not_change_rows(self):
        self.assertTrue(self.journal.seed(self.binding, self.objects))
        self.assertFalse(self.journal.seed(self.binding, self.objects))
        changed = copy.deepcopy(self.objects); changed[0]['byteSize'] += 1
        changed_binding = dict(self.binding, objectsSha256=p.digest(changed))
        with self.assertRaises(InputJournalError): self.journal.seed(changed_binding, changed)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM input_journal_objects').fetchone()[0], 3)

    def test_restart_after_ledger_before_journal_replays_absolute_target(self):
        self.journal.seed(self.binding, self.objects)
        self.assertEqual(self.journal.attempt(self.binding, 0), 1)
        item = self.journal.current(self.binding)
        # Simulate an external ledger having applied item.target, then worker exit.
        ledger_target = item['target']; self.reopen()
        again = self.journal.current(self.binding)
        self.assertEqual(again, item)
        self.assertEqual(ledger_target, again['target'])
        args = (self.binding, 0, item['objectId'], item['byteSize'], item['sha256'], ledger_target)
        self.assertTrue(self.journal.complete(*args)); self.reopen()
        self.assertFalse(self.journal.complete(*args))
        self.assertEqual(self.journal.current(self.binding)['allocatedBefore'], 1)

    def test_attempts_survive_reopen_and_conflicting_completion_is_rolled_back(self):
        self.journal.seed(self.binding, self.objects)
        for n in range(1,4):
            self.assertEqual(self.journal.attempt(self.binding, 0), n); self.reopen()
        with self.assertRaises(InputJournalError): self.journal.attempt(self.binding, 0)
        with self.assertRaises(InputJournalError):
            self.journal.complete(self.binding, 0, 'image-0', 1, self.objects[0]['sha256'], 2)
        self.assertEqual(self.journal.current(self.binding)['attempts'], 3)
        self.assertEqual(self.journal.current(self.binding)['allocatedBefore'], 0)

    def test_all_objects_and_wrong_binding(self):
        self.journal.seed(self.binding, self.objects)
        for n, obj in enumerate(self.objects):
            item = self.journal.current(self.binding)
            self.journal.attempt(self.binding, n)
            self.assertTrue(self.journal.complete(self.binding,n,obj['id'],obj['byteSize'],obj['sha256'],item['target']))
        self.assertIsNone(self.journal.current(self.binding))
        with self.assertRaises(InputJournalError): self.journal.current(dict(self.binding, environment='production'))

    def test_replay_rejects_mutated_or_missing_object_rows_and_attempt_policy(self):
        self.journal.seed(self.binding, self.objects)
        with self.assertRaises(InputJournalError):
            self.journal.seed(dict(self.binding, maxAttempts=4), self.objects)
        self.db.execute('UPDATE input_journal_objects SET size=99 WHERE job=? AND ordinal=1', (self.binding['jobId'],))
        self.db.commit()
        with self.assertRaises(InputJournalError): self.journal.seed(self.binding, self.objects)
        self.db.execute('DELETE FROM input_journal_objects WHERE job=?', (self.binding['jobId'],))
        self.db.commit()
        with self.assertRaises(InputJournalError): self.journal.seed(self.binding, self.objects)

    def test_completion_requires_persisted_attempt(self):
        self.journal.seed(self.binding, self.objects)
        item = self.journal.current(self.binding)
        with self.assertRaises(InputJournalError):
            self.journal.complete(self.binding,0,item['objectId'],item['byteSize'],item['sha256'],item['target'])

    def test_reopen_and_external_mutation_require_revalidation(self):
        self.journal.seed(self.binding, self.objects)
        self.db.close(); self.db = sqlite3.connect(self.path); self.journal = InputJournal(self.db)
        with self.assertRaises(InputJournalError): self.journal.current(self.binding)
        self.assertFalse(self.journal.seed(self.binding, self.objects))
        other = sqlite3.connect(self.path)
        try:
            other.execute('UPDATE input_journal_objects SET size=99 WHERE job=? AND ordinal=0', (self.binding['jobId'],))
            other.commit()
        finally:
            other.close()
        with self.assertRaises(InputJournalError): self.journal.current(self.binding)
        with self.assertRaises(InputJournalError): self.journal.attempt(self.binding, 0)
        with self.assertRaises(InputJournalError): self.journal.complete(self.binding,0,'image-0',1,self.objects[0]['sha256'],1)
        with self.assertRaises(InputJournalError): self.journal.seed(self.binding, self.objects)

    def test_caller_stale_wal_snapshot_rejected_without_rolling_it_back(self):
        self.db.execute('PRAGMA journal_mode=WAL')
        self.journal.seed(self.binding, self.objects)
        self.db.execute('BEGIN')
        self.db.execute('SELECT COUNT(*) FROM input_journal_objects').fetchone()
        other = sqlite3.connect(self.path)
        try:
            other.execute('UPDATE input_journal_objects SET size=99 WHERE job=? AND ordinal=0', (self.binding['jobId'],))
            other.commit()
        finally:
            other.close()
        with self.assertRaises(InputJournalError): self.journal.current(self.binding)
        with self.assertRaises(InputJournalError): self.journal.attempt(self.binding, 0)
        with self.assertRaises(InputJournalError): self.journal.complete(self.binding, 0, 'image-0', 1, self.objects[0]['sha256'], 1)
        with self.assertRaises(InputJournalError): self.journal.seed(self.binding, self.objects)
        self.assertTrue(self.db.in_transaction)
        self.db.rollback()
        with self.assertRaises(InputJournalError): self.journal.current(self.binding)

    def test_actual_scratch_allocation_survives_abrupt_process_exit(self):
        """The two real SQLite journals recover one absolute target after exit.

        The caller verifies the file; this is not a live worker or power-loss test.
        """
        content = b'synthetic-input-bytes'
        objects = [dict(id='image-0', byteSize=len(content), sha256=hashlib.sha256(content).hexdigest())]
        binding = dict(self.binding, objectsSha256=p.digest(objects))
        root = Path(self.tmp.name) / 'scratch'
        root.mkdir(mode=0o700)
        ledger_path = Path(self.tmp.name) / 'ledger.sqlite'
        ledger = ScratchLedger(ledger_path, root, capacity_probe=lambda _: 100000,
                               safety_floor_bytes=0, cleanup_grace_seconds=0)
        ledger.reserve(job_id=binding['jobId'],generation=1,reservation_id='r1',
            manifest_sha256=binding['manifestSha256'],input_bytes=len(content),output_budget=100,
            environment='staging',ttl_seconds=300,worker_ready=True)
        self.journal.seed(binding, objects)
        self.db.close()
        code = '''
import json, os, sqlite3, sys
from pathlib import Path
from processing.reference_input_journal import InputJournal
from processing.scratch_policy import ScratchLedger
root=Path(sys.argv[1]); binding=json.loads(sys.argv[2]); objects=json.loads(sys.argv[3])
db=sqlite3.connect(root/'journal.sqlite'); journal=InputJournal(db)
journal.seed(binding,objects); journal.attempt(binding,0)
data=b'synthetic-input-bytes'; f=root/'scratch'/'test-input'
with f.open('xb') as output:
 output.write(data); output.flush(); os.fsync(output.fileno())
ledger=ScratchLedger(root/'ledger.sqlite',root/'scratch',capacity_probe=lambda _:100000,safety_floor_bytes=0,cleanup_grace_seconds=0)
ledger.record_allocated('r1',journal.current(binding)['target'])
os._exit(73)
'''
        result = subprocess.run([sys.executable,'-c',code,self.tmp.name,p.canonical_json(binding).decode(),
                                 p.canonical_json(objects).decode()],timeout=20)
        self.assertEqual(result.returncode,73)
        self.db = sqlite3.connect(self.path); self.journal = InputJournal(self.db)
        self.assertFalse(self.journal.seed(binding,objects))
        item=self.journal.current(binding)
        stored=(root/'test-input').read_bytes()
        self.assertEqual((len(stored),hashlib.sha256(stored).hexdigest()),(item['byteSize'],item['sha256']))
        ledger=ScratchLedger(ledger_path,root,capacity_probe=lambda _:100000,safety_floor_bytes=0,cleanup_grace_seconds=0)
        ledger.record_allocated('r1',item['target'])
        self.assertTrue(self.journal.complete(binding,0,item['objectId'],item['byteSize'],item['sha256'],item['target']))
        with ledger._transaction() as db:
            row=ledger._reservation(db,'r1')
            self.assertEqual((row['allocated'],row['remaining']),(len(content),100))
        self.assertIsNone(self.journal.current(binding))

