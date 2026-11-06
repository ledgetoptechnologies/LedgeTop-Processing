"""Unwired V2 direct admission, never a source-custody ACK."""
import tempfile
import unittest
import base64
import json
import time
import uuid
from contextlib import closing, contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.reference_store import PROFILE, ReferenceStore
from tests import test_reference_source_intake as fixtures


class DirectV2AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.queue=Queue(self.temp.name,node_ids=('cluster','main'),reference_enabled=True);self.addCleanup(self.queue.close)
        self.queue.allowed_profiles.add(PROFILE);self.store=ReferenceStore(self.queue)
        self.fixture=fixtures.ReferenceSourceIntakeTests();self.fixture.setUp()
        self.fixture.authority_id=self.queue.authority_id
        self.validated=self.fixture.validate()
        self.options=dict(capability_fingerprint='a'*64,
            options_fingerprint=self.fixture.payload['pipeline']['odm']['optionsFingerprint'],
            allowed_odm_options=set(),source_spool=None,trusted_source_keys=self.fixture.trusted_keys,
            authority_id=self.queue.authority_id,ingress_mode='direct-v2',authenticated_principal='viewer-staging')

    def test_direct_mode_persists_full_signed_input_without_spool_or_claim(self):
        job,created=self.store.submit_source_intake(self.validated,**self.options)
        self.assertTrue(created);self.assertEqual(job['status'],'held')
        self.assertIsNone(self.store.next_for_node('cluster'))
        with self.queue._db() as db:
            row=db.execute('''SELECT s.reserved_bytes,s.source_state,s.derived_state,m.mode,j.held_reason
                FROM reference_source_intakes s JOIN reference_source_intake_modes m ON m.job_id=s.job_id
                JOIN jobs j ON j.id=s.job_id WHERE s.job_id=?''',(job['jobId'],)).fetchone()
            self.assertEqual(tuple(row),(0,'receiving','receiving','direct-v2','direct_admission_pending'))
        replay,created=self.store.submit_source_intake(self.validated,**self.options)
        self.assertFalse(created);self.assertEqual(replay['jobId'],job['jobId'])
        with self.assertRaisesRegex(Conflict,'custody reconciliation'):
            self.queue.operator(job['jobId'],'resolve',dict(note='synthetic',workerStopped=True))
        with self.assertRaises(Conflict):
            self.store.submit_source_intake(self.validated,**dict(self.options,authenticated_principal='other'))
        with self.assertRaises(Conflict):
            self.store.submit_source_intake(self.validated,**dict(self.options,ingress_mode='spool-v1',
                source_spool=SimpleNamespace(check_capacity=lambda *args,**kwargs:None)))

    def test_persisted_source_attestation_consumes_metadata_quota(self):
        self.store.submit_source_intake(self.validated,**self.options)
        with self.queue._db() as db:
            attestation_bytes=db.execute('SELECT length(CAST(source_attestation_json AS BLOB)) FROM reference_source_intakes').fetchone()[0]
            self.assertGreater(attestation_bytes,0)
            used=self.store._metadata_used_bytes(db)
            self.store.max_metadata_bytes=used-attestation_bytes
            with self.assertRaisesRegex(Conflict,'metadata quota'):
                self.store._quota(db)

    def test_scheduling_counts_full_retained_source_bytes(self):
        job,_=self.store.submit_source_intake(self.validated,**self.options)
        pipeline_bytes=self.validated['manifest']['totalBytes']
        source_bytes=self.validated['sourceManifest']['totalBytes']+sum(
            obj['byteSize'] for obj in self.validated['derivedManifest']['objects'])
        self.assertGreater(source_bytes,pipeline_bytes)
        self.store._configured_node_limits['cluster']['maxInputBytes']=pipeline_bytes
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self.assertIsNone(self.store._candidate_any_row(db,'cluster'))
            self.assertEqual(self.store._candidate_any_row(db,'main')['id'],job['jobId'])

    def test_rehashed_tampered_source_remains_queue_barrier(self):
        from processing import reference_protocol as p
        job,_=self.store.submit_source_intake(self.validated,**self.options)
        with self.queue._db() as db:
            row=db.execute('SELECT source_manifest_json FROM reference_source_intakes WHERE job_id=?',(job['jobId'],)).fetchone()
            source=json.loads(row[0]);source['objects'][0]['byteSize']=0
            source['totalBytes']=sum(obj['byteSize'] for obj in source['objects'])
            source['sha256']=p.digest({key:value for key,value in source.items() if key!='sha256'})
            db.execute('UPDATE reference_source_intakes SET source_manifest_json=? WHERE job_id=?',
                       (p.canonical_json(source).decode(),job['jobId']))
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self.assertEqual(self.store._candidate_any_row(db,'cluster')['id'],job['jobId'])

    def test_pending_job_reopens_unassigned_without_cluster_node(self):
        with tempfile.TemporaryDirectory() as directory:
            queue=Queue(directory,node_ids=('main',),reference_enabled=True)
            queue.allowed_profiles.add(PROFILE);store=ReferenceStore(queue)
            self.fixture.authority_id=queue.authority_id;validated=self.fixture.validate()
            options=dict(self.options,authority_id=queue.authority_id)
            job,_=store.submit_source_intake(validated,**options)
            queue.close()
            queue=Queue(directory,node_ids=('main',),reference_enabled=True)
            try:
                self.assertIsNone(queue.get(job['jobId'])['nodeId'])
                self.assertEqual(queue.get(job['jobId'])['status'],'held')
            finally:queue.close()

    def test_pending_job_with_missing_mode_fails_restart_closed(self):
        job,_=self.store.submit_source_intake(self.validated,**self.options)
        self.queue.close()
        import sqlite3
        from pathlib import Path
        with closing(sqlite3.connect(Path(self.temp.name)/'jobs.sqlite3')) as db, db:
            db.execute('DROP TABLE reference_source_intake_modes')
        with self.assertRaises(Conflict):
            Queue(self.temp.name,reference_enabled=True)

    def test_verified_worker_reservation_preserves_held_source(self):
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from processing import direct_v2_inventory as inventory, direct_worker_scratch_v2 as scratch
        from processing import direct_worker_fence_proof_v2 as fence, reference_protocol as p
        from processing.direct_v2_reservation import DirectV2Reservations
        from processing.reference_source_intake import INTAKE_FIELDS
        job,_=self.store.submit_source_intake(self.validated,**self.options)
        payload={key:self.validated[key] for key in INTAKE_FIELDS}
        worker=dict(authorityId=self.queue.authority_id,nodeId='cluster',workerId='worker',workerInstanceId='instance')
        prepared=inventory.prepare(payload,authenticated_principal='viewer-staging',environment='staging',
            capability_fingerprint='a'*64,options_fingerprint=self.options['options_fingerprint'],
            allowed_odm_options=set(),trusted_source_keys=self.fixture.trusted_keys,authority_id=self.queue.authority_id)
        item=dict(reservationId=str(uuid.uuid4()),jobId=job['jobId'],attemptId=payload['submissionId'],generation=1,
            environment='staging',**worker,sourceManifestSha256=prepared['sourceManifestSha256'],
            derivedManifestSha256=prepared['derivedManifestSha256'],pipelineManifestSha256=prepared['pipelineManifestSha256'],
            inventorySha256=prepared['inventorySha256'],objectCount=3,overheadBytesPerObject=65536,
            inputBytes=sum(x['byteSize'] for k in ('sourceManifest','derivedManifest') for x in prepared[k]['objects']),
            outputBudget=100,ttlSeconds=30)
        digest=scratch.identity(item,expected_worker=worker)[1];challenge=str(uuid.uuid4());now=int(time.time())
        body=dict(protocol=fence.PROTOCOL,challengeId=challenge,scratchBindingSha256=digest,pinId='a'*32,
            gateGeneration=1,issuedAt=now,expiresAt=now+30,sourceCustodyAccepted=False,
            transferAuthorized=False,processingAuthorized=False)
        envelope=dict(protocol=fence.PROTOCOL,issuerId='issuer',keyId='key',body=body)
        key=Ed25519PrivateKey.generate()
        proof=dict(envelope,signature=base64.urlsafe_b64encode(key.sign(fence.DOMAIN+p.canonical_json(envelope))).decode().rstrip('='))
        options=dict(payload=payload,authenticated_principal='viewer-staging',item=item,expected_worker=worker,
            readiness_proof=proof,challenge_id=challenge,gate_generation=1,issuer_id='issuer',key_id='key',
            worker_public_key=key.public_key(),trusted_source_keys=self.fixture.trusted_keys,
            capability_fingerprint='a'*64,options_fingerprint=self.options['options_fingerprint'],allowed_odm_options=set())
        reservations=DirectV2Reservations(self.store,trusted_workers={
            node:dict(identity=dict(worker,nodeId=node),issuerId='issuer',keyId='key',publicKey=key.public_key())
            for node in self.queue.node_ids})
        with self.assertRaisesRegex(Conflict,'not pinned'):
            reservations.reserve(**dict(options,worker_public_key=Ed25519PrivateKey.generate().public_key()))
        with self.queue._db() as db:
            original=db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?',(job['jobId'],)).fetchone()[0]
            changed=json.loads(original);changed['manifest']['inputCounts']['images']=0
            db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?',(p.canonical_json(changed).decode(),job['jobId']))
        with self.assertRaisesRegex(Conflict,'metadata changed'):
            reservations.reserve(**options)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM direct_v2_reservations').fetchone()[0],0)
            db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?',(original,job['jobId']))
        with patch('processing.direct_v2_reservation.time.monotonic',side_effect=[0,0,1000]):
            with self.assertRaisesRegex(Conflict,'readiness expired'):reservations.reserve(**options)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM direct_v2_reservations').fetchone()[0],0)
        from processing.direct_v2_reservation import V2ReservationUncertain
        with patch('processing.direct_v2_reservation.time.monotonic',side_effect=[0,0,0,1000]):
            with self.assertRaises(V2ReservationUncertain):reservations.reserve(**options)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM direct_v2_reservations').fetchone()[0],1)
        observation=reservations.reserve(**options)
        self.assertFalse(observation['sourceCustodyAccepted']);self.assertFalse(observation['transferAuthorized'])
        self.assertEqual(observation['nodeId'],'cluster');self.assertTrue(observation['replayed'])
        original_operation=self.queue.coordinator_operation
        @contextmanager
        def failing_teardown(kind):
            with original_operation(kind):yield
            raise RuntimeError('synthetic teardown uncertainty')
        with patch.object(self.queue,'coordinator_operation',side_effect=failing_teardown):
            with self.assertRaises(V2ReservationUncertain):reservations.reserve(**options)
        self.assertEqual(self.queue.get(job['jobId'])['status'],'held')
        with self.assertRaises(Conflict):
            self.queue.operator(job['jobId'],'resolve',dict(note='synthetic',workerStopped=True))

