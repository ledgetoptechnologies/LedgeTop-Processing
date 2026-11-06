"""Synthetic V2 local worker boundary; no runtime listener or custody ACK."""
import hashlib
import http.client
import importlib.util
import io
import json
import os
import ssl
import tempfile
import threading
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
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.root = Path(temp.name); self.root.chmod(0o700)
        fixture = fixtures.ReferenceSourceIntakeTests(); fixture.setUp()
        fixture.payload['sourceManifest']['objects'][1]['byteSize'] = 0
        fixture.payload['sourceManifest']['objects'][1]['sha256'] = hashlib.sha256(b'').hexdigest()
        fixture.payload['sourceManifest']['totalBytes'] = 3
        fixture.rehash(); signed = fixture.validate()
        payload = {key:signed[key] for key in INTAKE_FIELDS}
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
            issued_at=100,expires_at=120,issuer_id='worker-issuer',key_id='worker-key',private_key=self.proof_key)
        self.boundary = DirectWorkerBoundaryV2(ledger=self.ledger,spool=self.spool,inventory=index,
            item=self.item,reservation_binding=self.reservation,expected_worker=self.worker,engine_id='fixture-v2',
            coordinator_origin=origin,gate_generation=1,bearer=self.bearer,
            readiness_proof=self.readiness,challenge_id=self.challenge,issuer_id='worker-issuer',
            key_id='worker-key',public_key=self.proof_key.public_key(),now=110)

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

    def test_v2_failure_after_hold_before_pin_still_blocks_resolution(self):
        with patch.object(self.ledger,'_handover_direct_writer',side_effect=RuntimeError('synthetic crash')):
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
                public_key=self.proof_key.public_key(),now=120)

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
        finally:
            server.shutdown();server.server_close();thread.join(3)
            self.assertFalse(thread.is_alive())
