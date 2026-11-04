import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid

from processing import reference_protocol as p
from processing.admission_family_candidate import initialize_empty
from processing.prototype import Queue
from processing.reference_store import PROFILE, ReferenceStore


class EarlyFamilyPreflight(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        queue=Queue(self.tmp.name);queue.allowed_profiles.add(PROFILE)
        ReferenceStore(queue)
        with queue._db() as db:
            db.execute('BEGIN IMMEDIATE');initialize_empty(db);db.commit()
        store=ReferenceStore(queue,admission_family_candidate=True)
        fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        raw=fixture['submission'];raw['submissionId']=str(uuid.uuid4())
        value=p.validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint'])
        job,_=store.submit(value);self.job=job['jobId']
        with queue._db() as db:
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status,node_id) VALUES(?,?,?,?,?,?)",(str(uuid.uuid4()),'fixture-running','staging','photo','running','cluster'))
        self.path=Path(queue.path);queue.close()

    def state(self):
        db=sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True)
        try:return db.execute('SELECT request_id,status,node_id FROM jobs ORDER BY seq').fetchall()
        finally:db.close()

    def reject_unchanged(self):
        before=(self.path.read_bytes(),self.path.stat().st_mode,self.state())
        with self.assertRaisesRegex(RuntimeError,'candidate_family_preflight_unavailable'):
            Queue(self.tmp.name,family_preflight_candidate=True)
        self.assertEqual(before,(self.path.read_bytes(),self.path.stat().st_mode,self.state()))
        # Failure releases the actual lease, allowing an authorized retry.
        with (Path(self.tmp.name)/'coordinator.lock').open('a+b') as lock:
            if __import__('os').name != 'nt':
                import fcntl
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)

    def test_missing_marker_holds_before_queue_recovery(self):
        db=sqlite3.connect(self.path);db.execute('DELETE FROM reference_admission_family_candidate');db.commit();db.close()
        self.reject_unchanged()

    def test_malformed_marker_holds_before_queue_recovery(self):
        db=sqlite3.connect(self.path);db.execute("UPDATE reference_admission_family_candidate SET request_digest=?",('b'*64,));db.commit();db.close()
        self.reject_unchanged()

    def test_existing_owner_lease_blocks_second_queue(self):
        first=Queue(self.tmp.name,family_preflight_candidate=True)
        self.addCleanup(first.close)
        before=(self.path.read_bytes(),self.path.stat().st_mode,self.state())
        with self.assertRaisesRegex(RuntimeError,'already owned'):
            Queue(self.tmp.name,family_preflight_candidate=True)
        # A separate cooperative coordinator cannot pass the OS ownership lock.
        code = ("from processing.prototype import Queue; import sys; "
                "Queue(sys.argv[1],family_preflight_candidate=True)")
        other=subprocess.run([sys.executable,'-c',code,self.tmp.name],capture_output=True,text=True,timeout=10)
        self.assertNotEqual(other.returncode,0)
        self.assertIn('already owned',other.stderr)
        self.assertEqual(before,(self.path.read_bytes(),self.path.stat().st_mode,self.state()))


if __name__=='__main__':unittest.main()
