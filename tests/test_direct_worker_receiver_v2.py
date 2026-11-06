"""Synthetic V2 local worker boundary; no runtime listener or custody ACK."""
import hashlib
from contextlib import closing
import http.client
import importlib.util
import io
import json
import os
import socket
import ssl
import sqlite3
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from processing import direct_v2_inventory as inventory
from processing import direct_worker_scratch_v2 as scratch
from processing.direct_worker_receiver_v2 import DirectWorkerBoundaryV2
from processing.direct_worker_boundary import DirectBoundaryDenied
from processing.node_gate import NodeGate, GateError
from processing.scratch_policy import ScratchLedger, PolicyConflict
from processing.source_spool import SourceSpool, SourceSpoolError
from processing.reference_source_intake import INTAKE_FIELDS
from tests import test_reference_source_intake as fixtures


class WorkerV2Tests(unittest.TestCase):
    def setUp(self):
        clock_patch=patch('processing.direct_worker_receiver_v2.time.time',return_value=110)
        self.clock=clock_patch.start();self.addCleanup(clock_patch.stop)
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.root = Path(temp.name); self.root.chmod(0o700)
        fixture = fixtures.ReferenceSourceIntakeTests(); fixture.setUp()
        fixture.payload['sourceManifest']['objects'][1]['byteSize'] = 0
        fixture.payload['sourceManifest']['objects'][1]['sha256'] = hashlib.sha256(b'').hexdigest()
        fixture.payload['sourceManifest']['totalBytes'] = 3
        fixture.rehash(); signed = fixture.validate()
        payload = {key:signed[key] for key in INTAKE_FIELDS}
        self.payload=payload
        options = dict(authenticated_principal='viewer-staging', environment='staging',
            capability_fingerprint='a'*64, options_fingerprint=payload['pipeline']['odm']['optionsFingerprint'],
            allowed_odm_options=set(), trusted_source_keys=fixture.trusted_keys, authority_id=fixtures.AUTHORITY)
        prepared = inventory.prepare(payload, **options)
        keys = ('authorityId','principalId','environment','submissionId','viewerTaskId','providerTaskId',
            'sourceManifestSha256','derivedManifestSha256','pipelineManifestSha256','pipelineDigest',
            'attestationSha256','sourceSnapshotSha256','preparedEnvelopeSha256','activationId')
        expected = {key:prepared[key] for key in keys}
        self.worker = dict(authorityId=fixtures.AUTHORITY,nodeId='cluster',workerId='worker',workerInstanceId='instance')
        self.reservation = dict(reservationId=str(uuid.uuid4()),jobId=prepared['providerTaskId'],
            attemptId=prepared['submissionId'],generation=1,environment='staging',**self.worker)
        index = inventory.prepare_worker_index(prepared, expected_binding=expected,
            reservation_binding=self.reservation, **options)
        self.index,self.expected,self.options=index,expected,options
        self.item = dict(self.reservation, sourceManifestSha256=prepared['sourceManifestSha256'],
            derivedManifestSha256=prepared['derivedManifestSha256'],
            pipelineManifestSha256=prepared['pipelineManifestSha256'],inventorySha256=prepared['inventorySha256'],
            inputBytes=sum(x['byteSize'] for name in ('sourceManifest','derivedManifest') for x in prepared[name]['objects']),
            objectCount=3,overheadBytesPerObject=65536,outputBudget=100,ttlSeconds=10)
        self.ledger = ScratchLedger(self.root/'private'/'ledger.sqlite',self.root,
            capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)
        self.observation = scratch.prepare(self.ledger,self.item,expected_worker=self.worker,worker_ready=True)
        directory = self.ledger.prepare_job_directory(self.observation['scratchReservationId'])/'direct-source'
        directory.mkdir(mode=0o700)
        marker_name='.ltds-v2-'+uuid.uuid4().hex; marker=Path('/tmp')/marker_name
        marker.write_text('a'*32); marker.chmod(0o600); self.addCleanup(lambda:marker.unlink(missing_ok=True))
        self.spool = SourceSpool(directory,mount_path='/tmp',mount_id='a'*32,marker_name=marker_name,
            safety_floor_bytes=0,require_mount=True)
        gate_patch=patch('processing.node_gate.canonical_gate_path',return_value=self.root/'gate')
        gate_patch.start();self.addCleanup(gate_patch.stop)
        origin='https://127.0.0.1:8443'
        with NodeGate('fixture-v2') as gate:
            gate.provision('cluster',origin,self.worker['authorityId'])
            gate.reserve('cluster',origin,self.worker['authorityId'],1,self.observation['bindingSha256'][:32])
            gate.bind(self.observation['bindingSha256'][:32],dict(id=self.item['jobId'],workerId='worker',
                nodeId='cluster',authorityId=self.worker['authorityId']))
        self.bearer='synthetic-coordinator-token-0123456789'
        self.headers={'Authorization':'Bearer '+self.bearer,'X-Coordinator-Authority':self.worker['authorityId']}
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from processing import direct_worker_fence_proof_v2 as proof
        self.proof_key=Ed25519PrivateKey.generate();self.challenge=str(uuid.uuid4())
        self.readiness=proof.issue_fenced(self.ledger,self.spool,self.item,expected_worker=self.worker,
            engine_id='fixture-v2',coordinator_origin=origin,gate_generation=1,challenge_id=self.challenge,
            validity_seconds=10,issuer_id='worker-issuer',key_id='worker-key',private_key=self.proof_key)
        from processing.direct_worker_receipt_v2 import WorkerClosureSignerV2
        self.signer=WorkerClosureSignerV2(issuer_id='worker-issuer',key_id='worker-key',
                                          identity=self.worker,private_key=self.proof_key)
        self.boundary = DirectWorkerBoundaryV2(ledger=self.ledger,spool=self.spool,inventory=index,
            item=self.item,reservation_binding=self.reservation,expected_worker=self.worker,engine_id='fixture-v2',
            coordinator_origin=origin,gate_generation=1,bearer=self.bearer,
            readiness_proof=self.readiness,challenge_id=self.challenge,issuer_id='worker-issuer',
            key_id='worker-key',public_key=self.proof_key.public_key(),
            closure_expected_binding=self.expected,closure_verification=self.options,worker_signer=self.signer)

    def put_zero(self, headers=None):
        return self.boundary.put(headers or self.headers,protected_transport=True,object_id=fixtures.GCP_ID,
            content_length=0,content_sha256=hashlib.sha256(b'').hexdigest(),reader=io.BytesIO(b''))

    def test_zero_object_auth_durable_replay_and_no_custody(self):
        with self.ledger._transaction() as db:
            row=self.ledger._reservation(db,self.observation['scratchReservationId'])
            self.assertEqual(json.loads(row['settings'])['input_bytes'],
                             self.item['inputBytes'] + 3*65536)
        with self.assertRaises(DirectBoundaryDenied):
            self.put_zero({'Authorization':'Bearer wrong','X-Coordinator-Authority':self.worker['authorityId']})
        result=self.put_zero(); self.assertTrue(result['localObjectDurable'])
        self.assertFalse(result['sourceCustodyAccepted']); self.assertFalse(result['transferComplete'])
        with NodeGate('fixture-v2') as gate:
            self.assertEqual(gate.snapshot()['schema'],2)
            with self.assertRaisesRegex(GateError,'direct transfer hold'):
                gate.resolve(self.observation['bindingSha256'][:32],1,old_worker_stopped=True,
                             coordinator_resolved=True,note='synthetic')
        self.assertTrue(self.put_zero()['replayed'])
        path=self.spool.root/self.spool.storage_key(self.item['jobId'],1,fixtures.GCP_ID)
        self.assertEqual(path.read_bytes(),b'')

    def test_zero_publication_uncertainty_keeps_writer_pinned(self):
        target=self.spool.root/self.spool.storage_key(self.item['jobId'],1,fixtures.GCP_ID)
        original=self.spool._fsync_directory
        def fail_after_publish(path):
            if target.exists(): raise OSError('synthetic sync failure')
            return original(path)
        with patch.object(self.spool,'_fsync_directory',side_effect=fail_after_publish):
            with self.assertRaises(SourceSpoolError): self.put_zero()
        self.assertTrue(target.exists())
        with self.assertRaisesRegex(PolicyConflict,'active or uncertain'): self.put_zero()

    def test_observation_directory_sync_failure_never_reports_durable(self):
        self.put_zero()
        with patch.object(self.spool,'_fsync_directory',side_effect=OSError('synthetic observation sync failure')):
            with self.assertRaises(OSError):
                self.boundary.observe(self.headers,protected_transport=True,object_id=fixtures.GCP_ID)
        with self.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_writer_pins').fetchone()[0],1)

    def test_v2_failure_after_hold_before_pin_still_blocks_resolution(self):
        with patch.object(self.ledger,'_handover_v2_writer',side_effect=RuntimeError('synthetic crash')):
            with self.assertRaises(RuntimeError):self.put_zero()
        with NodeGate('fixture-v2') as gate:
            with self.assertRaisesRegex(GateError,'direct transfer hold'):
                gate.resolve(self.observation['bindingSha256'][:32],1,old_worker_stopped=True,
                             coordinator_resolved=True,note='synthetic')

    def test_v2_signed_readiness_holds_gate_and_transfers_pin_to_writer(self):
        from processing import direct_worker_fence_proof_v2 as proof
        kwargs=dict(expected_item=self.item,expected_worker=self.worker,challenge_id=self.challenge,
            gate_generation=1,issuer_id='worker-issuer',key_id='worker-key',public_key=self.proof_key.public_key())
        pin=proof.verify(self.readiness,now=110,**kwargs)['pinId']
        with self.assertRaises(proof.FenceProofRejected):proof.verify(self.readiness,now=120,**kwargs)
        with NodeGate('fixture-v2') as gate:
            with self.assertRaisesRegex(GateError,'direct transfer hold'):
                gate.resolve(self.observation['bindingSha256'][:32],1,old_worker_stopped=True,
                             coordinator_resolved=True,note='synthetic')
        self.boundary._readiness_pin=pin
        self.assertTrue(self.put_zero()['localObjectDurable'])
        self.assertIsNone(self.boundary._readiness_pin)
        self.assertTrue(self.put_zero()['replayed'])

    def test_fresh_boundary_without_readiness_pin_is_rejected(self):
        self.clock.return_value=120
        with self.assertRaisesRegex(DirectBoundaryDenied,'readiness proof required'):
            DirectWorkerBoundaryV2(ledger=self.ledger,spool=self.spool,inventory=self.boundary.inventory,
                item=self.item,reservation_binding=self.reservation,expected_worker=self.worker,
                engine_id='fixture-v2',coordinator_origin='https://127.0.0.1:8443',
                gate_generation=1,bearer=self.bearer)
        with self.assertRaisesRegex(DirectBoundaryDenied,'readiness proof required'):
            DirectWorkerBoundaryV2(ledger=self.ledger,spool=self.spool,inventory=self.boundary.inventory,
                item=self.item,reservation_binding=self.reservation,expected_worker=self.worker,
                engine_id='fixture-v2',coordinator_origin='https://127.0.0.1:8443',
                gate_generation=1,bearer=self.bearer,readiness_proof=self.readiness,
                challenge_id=self.challenge,issuer_id='worker-issuer',key_id='worker-key',
                public_key=self.proof_key.public_key())

    def test_readiness_expired_after_construction_cannot_handover(self):
        self.clock.return_value=120
        with self.assertRaises(ValueError):self.put_zero()
        self.assertIsNotNone(self.boundary._readiness_pin)
        with NodeGate('fixture-v2') as gate:
            with self.assertRaisesRegex(GateError,'direct transfer hold'):
                gate.resolve(self.observation['bindingSha256'][:32],1,old_worker_stopped=True,
                             coordinator_resolved=True,note='synthetic')

    def test_monotonic_deadline_prevents_clock_rollback_extension(self):
        self.boundary._proof_deadline=time.monotonic()-1
        with self.assertRaisesRegex(DirectBoundaryDenied,'proof expired'):self.put_zero()
        self.assertIsNotNone(self.boundary._readiness_pin)

    def test_initial_sampling_gap_cannot_extend_monotonic_deadline(self):
        with patch('processing.direct_worker_receiver_v2.time.monotonic',return_value=100) as mono:
            def wall():
                mono.return_value=115
                return 110
            self.clock.side_effect=wall
            boundary=DirectWorkerBoundaryV2(ledger=self.ledger,spool=self.spool,inventory=self.boundary.inventory,
                item=self.item,reservation_binding=self.reservation,expected_worker=self.worker,
                engine_id='fixture-v2',coordinator_origin='https://127.0.0.1:8443',gate_generation=1,
                bearer=self.bearer,readiness_proof=self.readiness,challenge_id=self.challenge,
                issuer_id='worker-issuer',key_id='worker-key',public_key=self.proof_key.public_key())
            self.assertEqual(boundary._proof_deadline,110)
            with self.assertRaisesRegex(DirectBoundaryDenied,'proof expired'):
                boundary.put(self.headers,protected_transport=True,object_id=fixtures.GCP_ID,
                    content_length=0,content_sha256=hashlib.sha256(b'').hexdigest(),reader=io.BytesIO(b''))

    def test_challenge_is_consumed_once_across_boundary_reconstruction(self):
        self.put_zero()
        replay=DirectWorkerBoundaryV2(ledger=self.ledger,spool=self.spool,inventory=self.boundary.inventory,
            item=self.item,reservation_binding=self.reservation,expected_worker=self.worker,
            engine_id='fixture-v2',coordinator_origin='https://127.0.0.1:8443',gate_generation=1,
            bearer=self.bearer,readiness_proof=self.readiness,challenge_id=self.challenge,
            issuer_id='worker-issuer',key_id='worker-key',public_key=self.proof_key.public_key())
        with self.assertRaisesRegex(PolicyConflict,'challenge already consumed'):
            replay.put(self.headers,protected_transport=True,object_id=fixtures.GCP_ID,
                content_length=0,content_sha256=hashlib.sha256(b'').hexdigest(),reader=io.BytesIO(b''))

    def test_missing_readiness_guard_and_record_fail_closed(self):
        self.put_zero()
        with closing(sqlite3.connect(self.ledger.path)) as db, db:
            db.execute('DROP TRIGGER direct_v2_readiness_no_delete')
        with self.assertRaisesRegex(PolicyConflict,'guards changed'):
            self.ledger.begin_direct_writer(self.observation['scratchReservationId'],
                scratch.settings(self.item,self.observation['scratchReservationId']))
        with self.assertRaisesRegex(PolicyConflict,'guards changed'):
            ScratchLedger(self.ledger.path,self.root,capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)
        from processing.scratch_policy import V2_READY_TRIGGERS
        with closing(sqlite3.connect(self.ledger.path)) as db, db:
            db.execute('DELETE FROM direct_v2_readiness_uses')
            db.execute(V2_READY_TRIGGERS[2])
        with self.assertRaisesRegex(PolicyConflict,'records missing'):
            ScratchLedger(self.ledger.path,self.root,capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)

    @unittest.skipUnless(os.environ.get('LTDS_OLD_PROCESSING_SOURCE'),'old processing source not supplied')
    def test_previous_scratch_refuses_consumed_v2_readiness(self):
        self.put_zero()
        source=Path(os.environ['LTDS_OLD_PROCESSING_SOURCE'])/'processing'/'scratch_policy.py'
        spec=importlib.util.spec_from_file_location('previous_scratch_v2',source)
        old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
        with self.assertRaisesRegex(old.PolicyConflict,'unknown accounting version'):
            old.ScratchLedger(self.root/'private'/'ledger.sqlite',self.root,
                capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)

    def test_expiry_after_atomic_handover_retains_pin_without_reading(self):
        original=self.ledger._handover_v2_writer
        def delayed(*args,**kwargs):
            pin=original(*args,**kwargs);self.clock.return_value=120;return pin
        reader=io.BytesIO(b'')
        with patch.object(self.ledger,'_handover_v2_writer',side_effect=delayed):
            with self.assertRaises(ValueError):
                self.boundary.put(self.headers,protected_transport=True,object_id=fixtures.GCP_ID,
                    content_length=0,content_sha256=hashlib.sha256(b'').hexdigest(),reader=reader)
        self.assertFalse(reader.closed)
        self.assertFalse((self.spool.root/self.item['jobId']).exists())
        with self.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_writer_pins').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_v2_readiness_uses').fetchone()[0],1)

    def test_database_lock_wait_past_expiry_never_handover_or_read(self):
        lock=sqlite3.connect(self.ledger.path,isolation_level=None,timeout=3)
        started=threading.Event();proceed=threading.Event();errors=[];reader=io.BytesIO(b'')
        original=self.ledger._handover_v2_writer
        def enter(*args,**kwargs):
            started.set();proceed.wait(2);return original(*args,**kwargs)
        def upload():
            try:
                self.boundary.put(self.headers,protected_transport=True,object_id=fixtures.GCP_ID,
                    content_length=0,content_sha256=hashlib.sha256(b'').hexdigest(),reader=reader)
            except Exception as error:errors.append(error)
        with patch.object(self.ledger,'_handover_v2_writer',side_effect=enter):
            thread=threading.Thread(target=upload);thread.start()
            try:
                self.assertTrue(started.wait(2));lock.execute('BEGIN IMMEDIATE');proceed.set();time.sleep(.1)
                self.clock.return_value=120
            finally:
                proceed.set();lock.rollback();lock.close();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(len(errors),1)
        self.assertIsInstance(errors[0],ValueError)
        self.assertFalse(reader.closed)
        with self.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_v2_readiness_uses').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_writer_pins').fetchone()[0],1)

    def test_full_v2_closure_requires_raw_and_derived_objects(self):
        from processing.direct_worker_closure_v2 import verify_closure
        args=dict(expected_binding=self.expected,reservation_binding=self.reservation,
            verification=self.options,expected_worker=self.worker,engine_id='fixture-v2',
            coordinator_origin='https://127.0.0.1:8443',gate_generation=1)
        self.put_zero()
        with self.assertRaisesRegex(PolicyConflict,'closure incomplete'):
            verify_closure(self.ledger,self.spool,self.index,self.item,**args)
        for object_id,content in ((fixtures.IMAGE_ID,b'abc'),('gcp_'+self.item['attemptId'],b'1,2,3\n')):
            self.boundary.put(self.headers,protected_transport=True,object_id=object_id,
                content_length=len(content),content_sha256=hashlib.sha256(content).hexdigest(),
                reader=io.BytesIO(content))
        result=verify_closure(self.ledger,self.spool,self.index,self.item,**args)
        self.assertEqual((result['sourceObjectCount'],result['derivedObjectCount']),(2,1))
        self.assertTrue(result['localClosureComplete'])
        self.assertFalse(result['sourceCustodyAccepted']);self.assertFalse(result['processingAuthorized'])
        self.assertEqual(verify_closure(self.ledger,self.spool,self.index,self.item,**args),result)
        # Rebuild all process-local state from retained signed inventory and
        # persistent scratch/files; an exact receipt may be reobserved only
        # after rehashing every object under the still-held node gate.
        recovered = inventory.prepare_worker_index(json.loads(self.index._wire),
            expected_binding=self.expected,reservation_binding=self.reservation,**self.options)
        reopened = ScratchLedger(self.root/'private'/'ledger.sqlite',self.root,
            capacity_probe=lambda _:1000000,clock=lambda:1000,safety_floor_bytes=100)
        self.assertEqual(verify_closure(reopened,self.spool,recovered,self.item,**args),result)
        from processing.direct_worker_receipt_v2 import WorkerClosureSignerV2, verify
        signer=WorkerClosureSignerV2(issuer_id='worker-issuer',key_id='worker-key',
                                     identity=self.worker,private_key=self.proof_key)
        signed=verify_closure(reopened,self.spool,recovered,self.item,worker_signer=signer,**args)
        self.assertEqual(verify(signed,expected_closure=result,issuer_id='worker-issuer',
            key_id='worker-key',public_key=self.proof_key.public_key()),signed)
        self.assertEqual(verify_closure(reopened,self.spool,recovered,self.item,worker_signer=signer,**args),signed)

    @unittest.skipUnless(os.environ.get('LTDS_OLD_GATE_SOURCE'),'old gate source fixture not supplied')
    def test_old_gate_refuses_v2_upload_hold(self):
        self.put_zero()
        spec=importlib.util.spec_from_file_location('old_gate_v2',os.environ['LTDS_OLD_GATE_SOURCE'])
        old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
        with patch.object(old,'canonical_gate_path',return_value=self.root/'gate'):
            with old.NodeGate('fixture-v2') as gate:
                with self.assertRaises(old.GateError):gate.snapshot()

    def test_zero_object_over_bounded_tls_and_replay(self):
        from ops.qualify_enrolled_phase_transport import configuration_material
        from processing.direct_worker_http import create_direct_worker_server
        from processing.reference_tls import verified_client_context
        material=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))['configurationFiles']
        for name in ('fixture-ca.pem','fixture-cert.pem','fixture-key.pem'):
            (self.root/name).write_bytes(material[name])
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version=ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(str(self.root/'fixture-cert.pem'),str(self.root/'fixture-key.pem'))
        server=create_direct_worker_server(self.boundary,'127.0.0.1',0,ssl_context=context,idle_timeout=2)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            client_context=verified_client_context(str(self.root/'fixture-ca.pem'))
            headers=dict(self.headers,**{'Content-SHA256':hashlib.sha256(b'').hexdigest()})
            for expected_replay in (False,True):
                connection=http.client.HTTPSConnection('127.0.0.1',server.server_port,context=client_context,timeout=3)
                try:
                    connection.request('PUT','/direct/v2/objects/'+fixtures.GCP_ID,body=b'',headers=headers)
                    response=connection.getresponse();result=json.loads(response.read())
                    self.assertEqual(response.status,200,result)
                    self.assertEqual(result['replayed'],expected_replay)
                    self.assertFalse(result['sourceCustodyAccepted'])
                finally:connection.close()
            from processing.direct_worker_sender_v2 import DirectWorkerSenderV2
            sender=DirectWorkerSenderV2(origin='https://127.0.0.1:'+str(server.server_port),
                connect_ip='127.0.0.1',ca_file=str(self.root/'fixture-ca.pem'),bearer=self.bearer,
                inventory=self.index,item=self.item,reservation_binding=self.reservation,
                expected_worker=self.worker,issuer_id='worker-issuer',key_id='worker-key',
                worker_public_key=self.proof_key.public_key(),idle_timeout=2,total_timeout=5)
            observed=sender.put(fixtures.IMAGE_ID,io.BytesIO(b'abc'))
            self.assertTrue(observed['localObjectDurable']);self.assertFalse(observed['sourceCustodyAccepted'])
            self.assertTrue(sender.put(fixtures.GCP_ID,io.BytesIO(b''))['replayed'])
            sender.put('gcp_'+self.item['attemptId'],io.BytesIO(b'1,2,3\n'))
            from processing.direct_worker_sender import DirectDeliveryUncertain
            original_reply=server.RequestHandlerClass._reply; dropped=[]
            def drop_first_complete(handler,status,payload):
                if handler.path=='/direct/v2/complete' and status==200 and not dropped:
                    dropped.append(True);handler.close_connection=True
                    handler.connection.shutdown(socket.SHUT_RDWR);handler.connection.close();return
                return original_reply(handler,status,payload)
            with patch.object(server.RequestHandlerClass,'_reply',new=drop_first_complete):
                with self.assertRaises(DirectDeliveryUncertain):sender.complete()
                closure=sender.complete()
            self.assertEqual(dropped,[True])
            self.assertTrue(closure['closure']['localClosureComplete'])
            self.assertFalse(closure['closure']['sourceCustodyAccepted'])
        finally:
            server.shutdown();server.server_close();thread.join(3)
            self.assertFalse(thread.is_alive())

    def test_tls_post_handover_expiry_is_uncertain(self):
        from ops.qualify_enrolled_phase_transport import configuration_material
        from processing.direct_worker_http import create_direct_worker_server
        from processing.direct_worker_sender import DirectDeliveryUncertain
        from processing.direct_worker_sender_v2 import DirectWorkerSenderV2
        material=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))['configurationFiles']
        for name in ('fixture-ca.pem','fixture-cert.pem','fixture-key.pem'):
            (self.root/name).write_bytes(material[name])
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(str(self.root/'fixture-cert.pem'),str(self.root/'fixture-key.pem'))
        server=create_direct_worker_server(self.boundary,'127.0.0.1',0,ssl_context=context,idle_timeout=2)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            sender=DirectWorkerSenderV2(origin='https://127.0.0.1:'+str(server.server_port),
                connect_ip='127.0.0.1',ca_file=str(self.root/'fixture-ca.pem'),bearer=self.bearer,
                inventory=self.index,item=self.item,reservation_binding=self.reservation,
                expected_worker=self.worker,issuer_id='worker-issuer',key_id='worker-key',
                worker_public_key=self.proof_key.public_key(),idle_timeout=2,total_timeout=5)
            original=self.ledger._handover_v2_writer
            def delayed(*args,**kwargs):
                pin=original(*args,**kwargs);self.clock.return_value=120;return pin
            with patch.object(self.ledger,'_handover_v2_writer',side_effect=delayed):
                with self.assertRaises(DirectDeliveryUncertain):sender.put(fixtures.GCP_ID,io.BytesIO(b''))
            with self.ledger._transaction() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_writer_pins').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_v2_readiness_uses').fetchone()[0],1)
            self.assertFalse((self.spool.root/self.item['jobId']).exists())
        finally:
            server.shutdown();server.server_close();thread.join(3)
            self.assertFalse(thread.is_alive())

    def test_tls_fsync_failure_and_reopened_ledger_do_not_claim_durability(self):
        from ops.qualify_enrolled_phase_transport import configuration_material
        from processing.direct_worker_http import create_direct_worker_server
        from processing.direct_worker_sender import DirectDeliveryUncertain
        from processing.direct_worker_sender_v2 import DirectWorkerSenderV2
        material=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))['configurationFiles']
        for name in ('fixture-ca.pem','fixture-cert.pem','fixture-key.pem'):
            (self.root/name).write_bytes(material[name])
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(str(self.root/'fixture-cert.pem'),str(self.root/'fixture-key.pem'))
        server=create_direct_worker_server(self.boundary,'127.0.0.1',0,ssl_context=context,idle_timeout=2)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            sender=DirectWorkerSenderV2(origin='https://127.0.0.1:'+str(server.server_port),
                connect_ip='127.0.0.1',ca_file=str(self.root/'fixture-ca.pem'),bearer=self.bearer,
                inventory=self.index,item=self.item,reservation_binding=self.reservation,
                expected_worker=self.worker,issuer_id='worker-issuer',key_id='worker-key',
                worker_public_key=self.proof_key.public_key(),idle_timeout=2,total_timeout=5)
            target=self.spool.root/self.spool.storage_key(self.item['jobId'],1,fixtures.GCP_ID)
            original=self.spool._fsync_directory
            def fail_after_link(path):
                if target.exists():raise OSError('synthetic directory sync failure')
                return original(path)
            with patch.object(self.spool,'_fsync_directory',side_effect=fail_after_link):
                with self.assertRaises(DirectDeliveryUncertain):sender.put(fixtures.GCP_ID,io.BytesIO(b''))
            self.assertTrue(target.exists())
            reopened=ScratchLedger(self.root/'private'/'ledger.sqlite',self.root,
                capacity_probe=lambda _:1000000,clock=lambda:1000,safety_floor_bytes=100)
            self.boundary.ledger=reopened
            with self.assertRaises(DirectDeliveryUncertain):sender.observe(fixtures.GCP_ID)
            with reopened._transaction() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_writer_pins').fetchone()[0],1)
        finally:
            server.shutdown();server.server_close();thread.join(3)
            self.assertFalse(thread.is_alive())
