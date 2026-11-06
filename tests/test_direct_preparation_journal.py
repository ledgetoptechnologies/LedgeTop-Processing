"""Synthetic queue transaction and uncertainty tests; no live activation."""
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
from processing.direct_preparation_client import DirectPreparationClient
from processing.direct_preparation_journal import prepare_once
from processing.direct_v2_reservation import DirectV2Reservations
from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.reference_source_intake import validate_source_intake
from processing.reference_store import PROFILE,ReferenceStore
from tests import test_direct_worker_receiver_v2 as fixture

class PreparationJournalTests(unittest.TestCase):
    def test_exact_prepared_proof_enters_existing_durable_reservation(self):
        f=fixture.WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        with tempfile.TemporaryDirectory() as directory:
            with closing(sqlite3.connect(Path(directory)/'jobs.sqlite3')) as db,db:
                db.execute('CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY,authority_id TEXT NOT NULL)')
                db.execute('INSERT INTO coordinator_identity VALUES(1,?)',(f.worker['authorityId'],))
            queue=Queue(directory,reference_enabled=True);queue.allowed_profiles.add(PROFILE)
            store=ReferenceStore(queue,source_routing_keys=f.options['trusted_source_keys']);opts=f.options
            checked=validate_source_intake(f.payload,environment='staging',capability_fingerprint=opts['capability_fingerprint'],
                options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'],
                trusted_source_keys=opts['trusted_source_keys'],authority_id=opts['authority_id'])
            store.submit_source_intake(checked,capability_fingerprint=opts['capability_fingerprint'],
                options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'],source_spool=None,
                trusted_source_keys=opts['trusted_source_keys'],authority_id=opts['authority_id'],ingress_mode='direct-v2',
                authenticated_principal='viewer-staging')
            workers={n:dict(identity=dict(f.worker,nodeId=n),issuerId='worker-issuer',keyId='worker-key',
                            publicKey=f.proof_key.public_key()) for n in queue.node_ids}
            reservations=DirectV2Reservations(store,trusted_workers=workers)
            client=object.__new__(DirectPreparationClient)
            client._worker,client._issuer,client._key=f.worker,'worker-issuer','worker-key'
            client._public=f.proof_key.public_key()
            args=dict(payload=f.payload,item=f.item,principal='viewer-staging',challenge_id=f.challenge,gate_generation=1,
                capability_fingerprint=opts['capability_fingerprint'],options_fingerprint=opts['options_fingerprint'],
                allowed_odm_options=opts['allowed_odm_options'])
            def assert_pin(**kwargs):
                with queue._db() as db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM coordinator_operation_pins WHERE kind='source-io-admission' AND state='active'").fetchone()[0],1)
                return f.readiness
            with patch.object(client,'prepare',side_effect=assert_pin):
                outcome=prepare_once(reservations,client,**args)
            self.assertEqual(outcome['state'],'proof_observed')
            with self.assertRaises(ValueError):
                reservations.reserve(payload=f.payload,authenticated_principal='viewer-staging',item=f.item,
                    expected_worker=f.worker,readiness_proof=f.readiness,challenge_id=f.challenge,gate_generation=2,
                    issuer_id='worker-issuer',key_id='worker-key',worker_public_key=f.proof_key.public_key(),
                    trusted_source_keys=opts['trusted_source_keys'],capability_fingerprint=opts['capability_fingerprint'],
                    options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'])
            with queue._db() as db:db.execute("UPDATE nodes SET state='draining' WHERE id=?",(f.item['nodeId'],))
            with self.assertRaisesRegex(Conflict,'maintenance'):
                reservations.reserve(payload=f.payload,authenticated_principal='viewer-staging',item=f.item,
                    expected_worker=f.worker,readiness_proof=f.readiness,challenge_id=f.challenge,gate_generation=1,
                    issuer_id='worker-issuer',key_id='worker-key',worker_public_key=f.proof_key.public_key(),
                    trusted_source_keys=opts['trusted_source_keys'],capability_fingerprint=opts['capability_fingerprint'],
                    options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'])
            with queue._db() as db:
                self.assertEqual(db.execute('SELECT count(*) FROM direct_v2_reservations').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT held_reason FROM jobs WHERE id=?',(f.item['jobId'],)).fetchone()[0],'direct_preparing')
                db.execute("UPDATE nodes SET state='active' WHERE id=?",(f.item['nodeId'],))
            result=reservations.reserve(payload=f.payload,authenticated_principal='viewer-staging',item=f.item,
                expected_worker=f.worker,readiness_proof=f.readiness,challenge_id=f.challenge,gate_generation=1,
                issuer_id='worker-issuer',key_id='worker-key',worker_public_key=f.proof_key.public_key(),
                trusted_source_keys=opts['trusted_source_keys'],capability_fingerprint=opts['capability_fingerprint'],
                options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'])
            self.assertEqual(result['state'],'receiving');self.assertFalse(result['sourceCustodyAccepted'])
            with queue._db() as db:
                db.execute('UPDATE coordinator_identity SET direct_preparation_count=0 WHERE singleton=1')
            from processing.direct_v2_reservation import V2ReservationUncertain
            with self.assertRaises(V2ReservationUncertain):
                reservations.reserve(payload=f.payload,authenticated_principal='viewer-staging',item=f.item,
                    expected_worker=f.worker,readiness_proof=f.readiness,challenge_id=f.challenge,gate_generation=1,
                    issuer_id='worker-issuer',key_id='worker-key',worker_public_key=f.proof_key.public_key(),
                    trusted_source_keys=opts['trusted_source_keys'],capability_fingerprint=opts['capability_fingerprint'],
                    options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'])
            with queue._db() as db:db.execute('UPDATE coordinator_identity SET direct_preparation_count=1 WHERE singleton=1')
            queue.close()

    def test_held_node_survives_uncertain_send_without_retransmission(self):
        f=fixture.WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        with tempfile.TemporaryDirectory() as directory:
            with closing(sqlite3.connect(Path(directory)/'jobs.sqlite3')) as db,db:
                db.execute('CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY,authority_id TEXT NOT NULL)')
                db.execute('INSERT INTO coordinator_identity VALUES(1,?)',(f.worker['authorityId'],))
            queue=Queue(directory,reference_enabled=True)
            queue.allowed_profiles.add(PROFILE);store=ReferenceStore(queue,source_routing_keys=f.options['trusted_source_keys'])
            opts=f.options
            validated=validate_source_intake(f.payload,environment='staging',
                capability_fingerprint=opts['capability_fingerprint'],options_fingerprint=opts['options_fingerprint'],
                allowed_odm_options=opts['allowed_odm_options'],trusted_source_keys=opts['trusted_source_keys'],authority_id=opts['authority_id'])
            job,_=store.submit_source_intake(validated,capability_fingerprint=opts['capability_fingerprint'],
                options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'],
                source_spool=None,trusted_source_keys=opts['trusted_source_keys'],authority_id=opts['authority_id'],
                ingress_mode='direct-v2',authenticated_principal='viewer-staging')
            workers={node:dict(identity=dict(f.worker,nodeId=node),issuerId='worker-issuer',keyId='worker-key',
                               publicKey=f.proof_key.public_key()) for node in queue.node_ids}
            reservations=DirectV2Reservations(store,trusted_workers=workers)
            client=object.__new__(DirectPreparationClient)
            client._worker,client._issuer,client._key=f.worker,'worker-issuer','worker-key'
            client._public=f.proof_key.public_key()
            args=dict(payload=f.payload,item=f.item,principal='viewer-staging',challenge_id=f.challenge,gate_generation=1,
                capability_fingerprint=opts['capability_fingerprint'],options_fingerprint=opts['options_fingerprint'],
                allowed_odm_options=opts['allowed_odm_options'])
            with patch.object(client,'prepare',side_effect=OSError('lost response')) as send:
                with self.assertRaisesRegex(Conflict,'uncertain'):prepare_once(reservations,client,**args)
                with self.assertRaises(Conflict):prepare_once(reservations,client,**args)
                self.assertEqual(send.call_count,1)
            with queue._db() as db:
                intent=db.execute('SELECT state FROM direct_preparation_intents').fetchone()
                state=db.execute('SELECT status,held_reason,node_id FROM jobs WHERE id=?',(job['jobId'],)).fetchone()
                self.assertEqual(intent['state'],'pending')
                self.assertEqual(tuple(state),('held','direct_preparing',f.item['nodeId']))
                self.assertFalse(store._node_has_slot(db,f.item['nodeId']))
            with self.assertRaises(Conflict):queue.operator(job['jobId'],'resolve',dict(note='synthetic',workerStopped=True))
            from processing.direct_preparation_journal import metadata_bytes,TRIGGERS,SCHEMA
            with queue._db() as db:
                record=tuple(db.execute('SELECT * FROM direct_preparation_intents').fetchone())
                db.execute('DROP TRIGGER direct_preparation_no_delete')
                db.execute('DELETE FROM direct_preparation_intents')
                db.execute(TRIGGERS['direct_preparation_no_delete'])
                with self.assertRaisesRegex(Conflict,'count changed'):metadata_bytes(db)
                db.execute('UPDATE coordinator_identity SET direct_preparation_count=0 WHERE singleton=1')
                with self.assertRaisesRegex(Conflict,'intent missing'):metadata_bytes(db)
                db.execute("UPDATE jobs SET held_reason='direct_v2_receiving' WHERE id=?",(job['jobId'],))
                with self.assertRaisesRegex(Conflict,'intent missing'):metadata_bytes(db)
                db.execute("UPDATE jobs SET held_reason='direct_preparing' WHERE id=?",(job['jobId'],))
                db.execute('INSERT INTO direct_preparation_intents VALUES(?,?,?,?,?,?,?,?,?,?)',record)
                db.execute('UPDATE coordinator_identity SET direct_preparation_count=1 WHERE singleton=1')
                self.assertGreater(metadata_bytes(db),0)
                db.execute('UPDATE coordinator_identity SET direct_preparation_version=1 WHERE singleton=1')
                with self.assertRaisesRegex(Conflict,'marker changed'):metadata_bytes(db)
                db.execute('UPDATE coordinator_identity SET direct_preparation_version=2 WHERE singleton=1')
                db.execute('DROP TABLE direct_preparation_intents')
                db.execute('UPDATE coordinator_identity SET direct_preparation_version=0 WHERE singleton=1')
                with self.assertRaisesRegex(Conflict,'count changed'):metadata_bytes(db)
                db.execute('UPDATE coordinator_identity SET direct_preparation_count=0 WHERE singleton=1')
                with self.assertRaisesRegex(Conflict,'intent missing'):metadata_bytes(db)
                db.execute(SCHEMA)
                for sql in TRIGGERS.values():db.execute(sql)
                db.execute('INSERT INTO direct_preparation_intents VALUES(?,?,?,?,?,?,?,?,?,?)',record)
                db.execute('UPDATE coordinator_identity SET direct_preparation_version=2,direct_preparation_count=1 WHERE singleton=1')
            queue.close()
