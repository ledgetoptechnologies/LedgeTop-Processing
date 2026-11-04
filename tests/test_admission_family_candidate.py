import hashlib
import sqlite3
import unittest
import uuid

from processing.admission_family_candidate import FamilyHeld, initialize_empty, admit_new_job, validate_family


class FamilyCandidateTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.executescript('''
          CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY,authority_id TEXT);
          CREATE TABLE reference_jobs(job_id TEXT PRIMARY KEY,generation INTEGER,environment TEXT,request_digest TEXT);
          CREATE TABLE reference_viewer_submission_owners(job_id TEXT PRIMARY KEY,principal_id TEXT,environment TEXT,request_digest TEXT,protocol TEXT);
          CREATE TABLE reference_viewer_envelopes_v2(job_id TEXT PRIMARY KEY,principal_id TEXT,environment TEXT,viewer_attempt_id TEXT,envelope_sha256 TEXT);
          CREATE TABLE reference_viewer_activation(job_id TEXT PRIMARY KEY);
          CREATE TABLE reference_viewer_v3_policy(job_id TEXT PRIMARY KEY,policy_json TEXT,policy_sha256 TEXT);
        ''')
        self.authority = str(uuid.uuid4())
        self.db.execute('INSERT INTO coordinator_identity VALUES(1,?)', (self.authority,))
        initialize_empty(self.db)
        self.db.commit()

    def admit(self, prepared=False, fail=False):
        job = str(uuid.uuid4()); digest='a'*64
        def create(db):
            db.execute('INSERT INTO reference_jobs VALUES(?,1,?,?)', (job,'production',digest))
            if prepared:
                db.execute('INSERT INTO reference_viewer_submission_owners VALUES(?,?,?,?,?)',
                    (job,'viewer','production',digest,'ltds-viewer-prepared-envelope-jcs-v3'))
                db.execute('INSERT INTO reference_viewer_envelopes_v2 VALUES(?,?,?,?,?)',
                    (job,'viewer','production',str(uuid.uuid4()),'b'*64))
                db.execute('INSERT INTO reference_viewer_v3_policy VALUES(?,?,?)', (job,'{}',hashlib.sha256(b'{}').hexdigest()))
            if fail: raise ValueError('synthetic interruption')
        admit_new_job(self.db,job,self.authority,'prepared-v3' if prepared else 'legacy-v1',create)
        return job

    def test_marker_and_job_commit_or_roll_back_together(self):
        with self.assertRaises(ValueError): self.admit(fail=True)
        self.assertEqual(self.db.execute('SELECT count(*) FROM reference_jobs').fetchone()[0],0)
        job=self.admit()
        self.db.execute('BEGIN')
        self.assertEqual(validate_family(self.db,job,self.authority),'legacy-v1')

    def test_explicit_v3_and_missing_or_spoofed_evidence_hold(self):
        job=self.admit(True)
        self.db.execute('BEGIN')
        self.assertEqual(validate_family(self.db,job,self.authority),'prepared-v3')
        self.db.execute('DELETE FROM reference_viewer_submission_owners WHERE job_id=?',(job,))
        with self.assertRaises(FamilyHeld):validate_family(self.db,job,self.authority)
        self.db.execute('DELETE FROM reference_admission_family_candidate WHERE job_id=?',(job,))
        with self.assertRaises(FamilyHeld):validate_family(self.db,job,self.authority)
        self.db.execute('DELETE FROM reference_viewer_envelopes_v2 WHERE job_id=?',(job,))
        self.db.execute('DELETE FROM reference_viewer_v3_policy WHERE job_id=?',(job,))
        self.db.commit()
        with self.assertRaises(FamilyHeld):admit_new_job(self.db,job,self.authority,'legacy-v1',lambda db:None)

    def test_wrong_authority_or_changed_job_holds(self):
        job=self.admit();self.db.execute('BEGIN')
        with self.assertRaises(FamilyHeld):validate_family(self.db,job,str(uuid.uuid4()))
        self.db.execute('UPDATE reference_jobs SET request_digest=? WHERE job_id=?',('c'*64,job))
        with self.assertRaises(FamilyHeld):validate_family(self.db,job,self.authority)

    def test_existing_unmarked_rows_are_never_migrated_or_classified(self):
        job=self.admit();self.db.execute('BEGIN')
        self.db.execute('DROP TABLE reference_admission_family_candidate')
        with self.assertRaises(FamilyHeld):initialize_empty(self.db)
        with self.assertRaises(FamilyHeld):validate_family(self.db,job,self.authority)


if __name__ == '__main__': unittest.main()
