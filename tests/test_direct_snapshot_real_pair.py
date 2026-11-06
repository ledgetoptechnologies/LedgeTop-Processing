"""Real synthetic admission + mTLS source-page route; no custody ACK."""
import base64
import copy
import hashlib
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing import reference_protocol as p, viewer_source_attestation as attest
from processing import direct_v2_inventory as inventory, direct_worker_scratch_v2 as scratch
from processing import direct_worker_fence_proof_v2 as fence
from processing.direct_v2_reservation import DirectV2Reservations
from processing.direct_snapshot_page_journal import SnapshotPageJournal
from processing.direct_v2_snapshot_verifier import SNAPSHOT_DOMAIN,PAGE_DOMAIN
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_source_intake import validate_source_intake
from processing.reference_store import PROFILE,ReferenceStore
from processing.scratch_policy import ScratchLedger
from processing.source_spool import SourceSpool
from tests import test_direct_snapshot_http as http_fixture
from tests import test_reference_source_intake as source_fixture

def b64(value):return base64.urlsafe_b64encode(value).decode().rstrip('=')

class SnapshotRealPair(unittest.TestCase):
    def test_signed_snapshot_through_real_reservation_and_private_mtls(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);root=Path(temp.name);root.chmod(0o700)
        queue=Queue(root/'coordinator',reference_enabled=True);self.addCleanup(queue.close);queue.allowed_profiles.add(PROFILE)
        src=source_fixture.ReferenceSourceIntakeTests();src.setUp();src.authority_id=queue.authority_id
        image=src.payload['sourceManifest']['objects'][0]
        src.payload['sourceManifest'].update(objects=[image],objectCount=1,totalBytes=image['byteSize'])
        src.payload['derivedManifest']['objects']=[]
        src.payload['manifest']['objects']=src.payload['manifest']['objects'][:1]
        src.payload['manifest'].update(objectCount=1,totalBytes=image['byteSize']);src.rehash()
        signed=src.validate();payload=copy.deepcopy(src.payload)
        record=dict(type='dataset_file',datasetFileId=image['id'],relativePath='flight/'+image['name'],
                    byteSize=image['byteSize'],sha256=image['sha256'],processingRole='image',mediaType='image/jpeg',kind='image')
        frame=p.canonical_json(dict(version='ltds-viewer-source-frames-v1',index=0,records=[record]))
        selected=p.canonical_json(dict(version='ltds-viewer-selected-frames-v1',index=0,records=[
            dict(type='dataset_file',ordinal=0,pageObjectId=image['id'],datasetFileId=image['id'])]))
        snapshot_id=str(uuid.uuid4())
        intent=dict(protocol='ltds-viewer-held-source-snapshot-v1',snapshotId=snapshot_id,issuerId='snapshot-issuer',keyId='snapshot-key',
            createdAt='2026-11-06T12:00:00.000Z',datasetId=payload['sourceManifest']['id'],
            datasetManifestSha256=payload['sourceManifest']['datasetManifestSha256'],taskId=payload['viewerTaskId'],
            attemptId=payload['submissionId'],inputManifestSha256=payload['manifest']['sha256'],sourceCount=1,datasetFileCount=1,
            sourcePageHashes=[hashlib.sha256(PAGE_DOMAIN+frame).hexdigest()],sourceFrameBytes=len(frame)+1,
            selectedCount=1,selectedPageHashes=[hashlib.sha256(b'ltds-viewer-selected-frame-v1\0'+selected).hexdigest()],
            selectedFrameBytes=len(selected)+1,gcp=None)
        raw=p.canonical_json(intent);snapshot_key=Ed25519PrivateKey.generate();signature=snapshot_key.sign(SNAPSHOT_DOMAIN+raw)
        proof=copy.deepcopy(signed['sourceAttestation']['proof']);proof['sourceSnapshotSha256']=hashlib.sha256(raw).hexdigest()
        message=attest.DOMAIN+p.canonical_json(proof)
        payload['sourceAttestation']=dict(proof=proof,signature=b64(src.signing_key.sign(message)),attestationSha256=hashlib.sha256(message).hexdigest())
        opts=dict(authenticated_principal='viewer-staging',environment='staging',capability_fingerprint='a'*64,
            options_fingerprint=payload['pipeline']['odm']['optionsFingerprint'],allowed_odm_options=set(),
            trusted_source_keys=src.trusted_keys,authority_id=queue.authority_id)
        prepared=inventory.prepare(payload,**opts)
        worker=dict(authorityId=queue.authority_id,nodeId='cluster',workerId='worker',workerInstanceId='instance')
        binding=dict(reservationId=str(uuid.uuid4()),jobId=payload['providerTaskId'],attemptId=payload['submissionId'],
                     generation=1,environment='staging',**worker)
        item=dict(binding,sourceManifestSha256=prepared['sourceManifestSha256'],derivedManifestSha256=prepared['derivedManifestSha256'],
            pipelineManifestSha256=prepared['pipelineManifestSha256'],inventorySha256=prepared['inventorySha256'],
            inputBytes=3,objectCount=1,overheadBytesPerObject=65536,outputBudget=100,ttlSeconds=300)
        storage=root/'worker';storage.mkdir(mode=0o700)
        ledger=ScratchLedger(storage/'ledger.sqlite',storage,capacity_probe=lambda _:10000000,safety_floor_bytes=100)
        observation=scratch.prepare(ledger,item,expected_worker=worker,worker_ready=True)
        source_dir=ledger.prepare_job_directory(observation['scratchReservationId'])/'direct-source';source_dir.mkdir(mode=0o700)
        marker_name='.ltds-snapshot-pair-'+uuid.uuid4().hex;marker=Path('/tmp')/marker_name
        marker.write_text('a'*32);marker.chmod(0o600);self.addCleanup(lambda:marker.unlink(missing_ok=True))
        spool=SourceSpool(source_dir,mount_path='/tmp',mount_id='a'*32,marker_name=marker_name,safety_floor_bytes=0,require_mount=True)
        gate_patch=patch('processing.node_gate.canonical_gate_path',return_value=root/'node-gate');gate_patch.start();self.addCleanup(gate_patch.stop)
        origin='https://127.0.0.1:8443'
        with NodeGate('snapshot-pair') as gate:
            gate.provision('cluster',origin,worker['authorityId'])
            gate.reserve('cluster',origin,worker['authorityId'],1,observation['bindingSha256'][:32])
            gate.bind(observation['bindingSha256'][:32],dict(id=item['jobId'],workerId='worker',nodeId='cluster',authorityId=worker['authorityId']))
        worker_key=Ed25519PrivateKey.generate();challenge=str(uuid.uuid4())
        ready=fence.issue_fenced(ledger,spool,item,expected_worker=worker,engine_id='snapshot-pair',coordinator_origin=origin,
            gate_generation=1,challenge_id=challenge,validity_seconds=60,issuer_id='worker-issuer',key_id='worker-key',private_key=worker_key)
        store=ReferenceStore(queue,source_routing_keys=src.trusted_keys)
        validated=validate_source_intake(payload,environment='staging',capability_fingerprint='a'*64,
            options_fingerprint=opts['options_fingerprint'],allowed_odm_options=set(),trusted_source_keys=src.trusted_keys,authority_id=queue.authority_id)
        job,_=store.submit_source_intake(validated,capability_fingerprint='a'*64,options_fingerprint=opts['options_fingerprint'],
            allowed_odm_options=set(),source_spool=None,trusted_source_keys=src.trusted_keys,authority_id=queue.authority_id,
            ingress_mode='direct-v2',authenticated_principal='viewer-staging')
        workers={node:dict(identity=dict(worker,nodeId=node),issuerId='worker-issuer',keyId='worker-key',publicKey=worker_key.public_key()) for node in queue.node_ids}
        pins=[dict(principal='viewer-staging',environment='staging',issuerId='snapshot-issuer',keyId='snapshot-key',publicKey=snapshot_key.public_key())]
        reservations=DirectV2Reservations(store,trusted_workers=workers,trusted_snapshot_signers=pins)
        reservations.reserve(payload=payload,authenticated_principal='viewer-staging',item=item,expected_worker=worker,
            readiness_proof=ready,challenge_id=challenge,gate_generation=1,issuer_id='worker-issuer',key_id='worker-key',
            worker_public_key=worker_key.public_key(),trusted_source_keys=src.trusted_keys,capability_fingerprint='a'*64,
            options_fingerprint=opts['options_fingerprint'],allowed_odm_options=set())
        backend=SnapshotPageJournal(store,reservations)
        fixture=http_fixture.SnapshotListenerTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        fixture.scopes[fixture.fingerprint]={'principal':'viewer-staging','environment':'staging'}
        fixture.backend=backend;server=fixture.start()
        prefix=f"/internal/v2/reservations/{binding['reservationId']}/source-snapshots/{snapshot_id}/"
        def post(part,body):return fixture.request(server,cert=fixture.good,key=fixture.goodkey,path=prefix+part,body=body)
        begin=p.canonical_json(dict(intentBase64url=b64(raw),signatureBase64url=b64(signature)))
        self.assertEqual(post('begin',begin)[0],200)
        self.assertEqual(post('pages/0',frame)[0],200)
        self.assertEqual(post('complete-selected',b'')[0],409)
        self.assertEqual(post('selected-pages/0',selected)[0],200)
        self.assertEqual(post('selected-pages/0',selected)[0],200)
        self.assertEqual(post('selected-pages/0',selected+b'x')[0],409)
        selected_status,selected_response=post('complete-selected',b'')
        self.assertEqual(selected_status,200,selected_response)
        self.assertEqual(selected_response['state'],'source_and_selected_pages_verified_not_custody')
        self.assertFalse(selected_response['sourceCustodyAccepted'])
        self.assertFalse(selected_response['processingAuthorized'])
        status,response=post('complete',b'');self.assertEqual(status,200,response)
        self.assertEqual(response['state'],'source_pages_verified_not_custody')
        self.assertFalse(response['sourceCustodyAccepted']);self.assertFalse(response['processingAuthorized'])
        checked=reservations.inspect_held_snapshot(binding['reservationId'],authenticated_principal='viewer-staging',
            issuer_id='snapshot-issuer',key_id='snapshot-key',intent_bytes=raw,signature=signature,
            source_frames=frame+b'\n',selected_frames=selected+b'\n')
        self.assertEqual(checked['state'],'source_and_selected_pages_verified_not_custody')
        self.assertEqual(checked['selectedCount'],1)
        from processing.maintenance import Conflict
        with self.assertRaises(Conflict):
            reservations.inspect_held_snapshot(binding['reservationId'],authenticated_principal='viewer-staging',
                issuer_id='snapshot-issuer',key_id='snapshot-key',intent_bytes=raw,signature=signature,
                source_frames=frame+b'\n',selected_frames=selected+b'x\n')
        self.assertEqual(queue.get(job['jobId'])['status'],'held')
        self.assertEqual(post('complete',b'')[1],response)

if __name__=='__main__':unittest.main()
