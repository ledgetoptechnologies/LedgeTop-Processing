"""Isolated schema/census review fixture; no live reservation or custody."""
import unittest
import uuid
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

if __name__=='__main__':unittest.main()
