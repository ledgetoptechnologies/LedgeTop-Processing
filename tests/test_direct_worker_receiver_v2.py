"""Synthetic V2 local worker boundary; no runtime listener or custody ACK."""
import hashlib
import io
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from processing import direct_v2_inventory as inventory
from processing import direct_worker_scratch_v2 as scratch
from processing.direct_worker_receiver_v2 import DirectWorkerBoundaryV2
from processing.direct_worker_boundary import DirectBoundaryDenied
from processing.node_gate import NodeGate
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
        self.item = dict(self.reservation, sourceManifestSha256=prepared['sourceManifestSha256'],
            derivedManifestSha256=prepared['derivedManifestSha256'],
            pipelineManifestSha256=prepared['pipelineManifestSha256'],inventorySha256=prepared['inventorySha256'],
            inputBytes=sum(x['byteSize'] for name in ('sourceManifest','derivedManifest') for x in prepared[name]['objects']),
            outputBudget=100,ttlSeconds=10)
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
        self.boundary = DirectWorkerBoundaryV2(ledger=self.ledger,spool=self.spool,inventory=index,
            item=self.item,reservation_binding=self.reservation,expected_worker=self.worker,engine_id='fixture-v2',
            coordinator_origin=origin,gate_generation=1,bearer=self.bearer)

    def put_zero(self, headers=None):
        return self.boundary.put(headers or self.headers,protected_transport=True,object_id=fixtures.GCP_ID,
            content_length=0,content_sha256=hashlib.sha256(b'').hexdigest(),reader=io.BytesIO(b''))

    def test_zero_object_auth_durable_replay_and_no_custody(self):
        with self.assertRaises(DirectBoundaryDenied):
            self.put_zero({'Authorization':'Bearer wrong','X-Coordinator-Authority':self.worker['authorityId']})
        result=self.put_zero(); self.assertTrue(result['localObjectDurable'])
        self.assertFalse(result['sourceCustodyAccepted']); self.assertFalse(result['transferComplete'])
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
