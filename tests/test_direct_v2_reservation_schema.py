"""Isolated schema/census review fixture; no live reservation or custody."""
import unittest
import uuid
import os
import subprocess
import sys
import sqlite3
from contextlib import closing
from pathlib import Path
from processing import direct_v2_reservation_schema as schema
from processing.maintenance import Conflict
from processing.coordinator_freeze import CoordinatorFrozen
from tests.test_direct_v2_admission import DirectV2AdmissionTests

class ReservationSchemaTests(unittest.TestCase):
    def setUp(self):
        self.f=DirectV2AdmissionTests('runTest');self.f.setUp();self.addCleanup(self.f.doCleanups)
    def test_exact_schema_and_existing_row_census_and_replay_integrity(self):
        self.f.test_verified_worker_reservation_preserves_held_source()
        with self.f.queue._db() as db:
            rid=db.execute('SELECT reservation_id FROM direct_v2_reservations').fetchone()[0]
            row=schema.validate_receiving(db,rid)
            self.assertEqual(row['state'],'receiving')
            self.assertGreater(schema.metadata_bytes(db),0)
            db.execute("UPDATE reference_jobs SET node_id='other' WHERE job_id=?",(row['job_id'],))
        with self.f.queue._db() as db:
            with self.assertRaisesRegex(Conflict,'requires inspection'):schema.validate_receiving(db,rid)
    def test_old_or_weakened_schema_is_not_migrated_or_accounted(self):
        with self.f.queue._db() as db:
            db.execute('''CREATE TABLE direct_v2_reservations (reservation_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,node_id TEXT NOT NULL,principal TEXT NOT NULL,request_digest TEXT NOT NULL,
                item_json BLOB NOT NULL,proof_json BLOB NOT NULL,challenge_id TEXT NOT NULL,state TEXT NOT NULL)''')
        with self.f.queue._db() as db:
            with self.assertRaisesRegex(Conflict,'schema requires inspection'):self.f.store._metadata_used_bytes(db)
            db.execute('BEGIN IMMEDIATE')
            with self.assertRaisesRegex(Conflict,'schema requires inspection'):schema.ensure_schema(db)
    def test_changed_principal_or_challenge_never_validates_as_replay(self):
        self.f.test_verified_worker_reservation_preserves_held_source()
        with self.f.queue._db() as db:
            row=db.execute('SELECT reservation_id,principal,challenge_id FROM direct_v2_reservations').fetchone()
            rid=row['reservation_id'];original_principal=row['principal'];original_challenge=row['challenge_id']
            db.execute("UPDATE direct_v2_reservations SET principal='other' WHERE reservation_id=?",(rid,))
        with self.f.queue._db() as db:
            with self.assertRaisesRegex(Conflict,'requires inspection'):schema.validate_receiving(db,rid)
            db.execute('UPDATE direct_v2_reservations SET principal=?,challenge_id=? WHERE reservation_id=?',
                       (original_principal,str(uuid.uuid4()),rid))
        with self.f.queue._db() as db:
            with self.assertRaisesRegex(Conflict,'requires inspection'):schema.validate_receiving(db,rid)
            db.execute('UPDATE direct_v2_reservations SET challenge_id=? WHERE reservation_id=?',(original_challenge,rid))
            self.assertEqual(schema.validate_receiving(db,rid)['reservation_id'],rid)
            job_id=db.execute('SELECT job_id FROM direct_v2_reservations WHERE reservation_id=?',(rid,)).fetchone()[0]
            db.execute("UPDATE reference_source_intakes SET source_manifest_sha=? WHERE job_id=?",('f'*64,job_id))
        with self.f.queue._db() as db:
            with self.assertRaisesRegex(Conflict,'requires inspection'):schema.validate_receiving(db,rid)
    def test_null_primary_key_cannot_disappear_from_byte_census(self):
        with self.f.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');schema.ensure_schema(db)
            db.execute("INSERT INTO direct_v2_reservations VALUES(NULL,'job','node','principal','digest',x'7b7d',x'7b7d','challenge','receiving')")
        with self.f.queue._db() as db:
            with self.assertRaisesRegex(Conflict,'census requires inspection'):schema.metadata_bytes(db)
    def test_new_schema_creation_respects_real_freeze_gate(self):
        gate=self.f.queue._coordinator_freeze
        gate.transition(dict(requestId=str(uuid.uuid4()),planId=str(uuid.uuid4()),action='intent',
            expectedRevision=gate.status()['revision'],startupId=self.f.queue.startup_id),owner_secret='synthetic-owner')
        with self.assertRaises(CoordinatorFrozen):
            with self.f.queue.coordinator_operation('source-io-admission'),self.f.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');schema.ensure_schema(db)

    def test_lost_reservation_table_is_not_recreated_after_fence(self):
        with self.f.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');schema.ensure_schema(db)
            db.execute('DROP TABLE direct_v2_reservations')
        with self.f.queue._db() as db:
            with self.assertRaisesRegex(Conflict,'fence mismatch'):schema.metadata_bytes(db)
            db.execute('BEGIN IMMEDIATE')
            with self.assertRaisesRegex(Conflict,'fence mismatch'):schema.ensure_schema(db)

    def test_lost_table_and_fence_are_not_treated_as_first_install(self):
        with self.f.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');schema.ensure_schema(db)
            db.execute('DROP TABLE direct_v2_reservations')
            db.execute('DROP TABLE legacy_fence_requirement')
        with self.f.queue._db() as db:
            with self.assertRaisesRegex(Conflict,'fence mismatch'):schema.metadata_bytes(db)
            db.execute('BEGIN IMMEDIATE')
            with self.assertRaisesRegex(Conflict,'fence mismatch'):schema.ensure_schema(db)

    def test_queue_reopen_refuses_when_table_and_fence_both_disappear(self):
        from processing.prototype import Queue
        with self.f.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');schema.ensure_schema(db)
        path=self.f.temp.name;self.f.queue.close()
        with closing(sqlite3.connect(Path(path)/'jobs.sqlite3')) as db,db:
            db.execute('DROP TABLE direct_v2_reservations');db.execute('DROP TABLE legacy_fence_requirement')
        with self.assertRaises(Conflict):Queue(path,node_ids=('cluster','main'),reference_enabled=True)

    @unittest.skipUnless(os.environ.get('LTDS_PRE_FENCE_SOURCE'),'pre-fence source not supplied')
    def test_actual_prior_queue_rejects_v2_reservation_fence(self):
        with self.f.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');schema.ensure_schema(db)
        path=self.f.temp.name;self.f.queue.close()
        old=os.environ['LTDS_PRE_FENCE_SOURCE']
        code='from processing.prototype import Queue; import sys; Queue(sys.argv[1],node_ids=("cluster","main"),reference_enabled=True)'
        result=subprocess.run([sys.executable,'-c',code,path],cwd=old,
            env=dict(os.environ,PYTHONPATH=old),capture_output=True,text=True,timeout=10)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('reviewed_legacy_barrier_conversion_required',result.stderr)

if __name__=='__main__':unittest.main()
