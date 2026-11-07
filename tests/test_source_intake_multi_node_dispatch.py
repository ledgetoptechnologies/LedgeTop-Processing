"""Signed V2 intake through real spool receipts and per-node claim admission.

Photo counts are scaled to a one-image cluster ceiling. No native engines or
services run; mount qualification is simulated over task-owned temporary bytes.
"""
import copy
import base64
import hashlib
import io
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

from processing import reference_protocol as protocol
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_store import PROFILE, ReferenceStore
from processing.source_spool import SourceSpool
from processing import viewer_source_attestation as attest
from tests import test_reference_source_intake as intake_fixture


class SourceIntakeMultiNodeDispatchTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='source-multi-node-')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        spool_root = root / 'spool'
        spool_root.mkdir(mode=0o700)
        marker = spool_root / '.ltds-source-spool-id'
        marker.write_text('a' * 64, encoding='ascii')
        if os.name != 'nt':marker.chmod(0o600)
        mount = patch('processing.source_spool.os.path.ismount', return_value=True)
        mount.start()
        self.addCleanup(mount.stop)
        spool = SourceSpool(spool_root, mount_id='a' * 64, safety_floor_bytes=0,
            max_object_bytes=1024, chunk_bytes=3)
        self.queue = Queue(root / 'state', node_ids=('cluster','main'), reference_enabled=True)
        self.addCleanup(self.queue.close)
        self.queue.allowed_profiles.add(PROFILE)
        self.fixture = intake_fixture.ReferenceSourceIntakeTests()
        self.fixture.setUp()
        self.fixture.authority_id = self.queue.authority_id
        self.fixture.trusted_keys.append(dict(self.fixture.trusted_keys[0], environment='production',
            principalId='viewer-production'))
        self.store = ReferenceStore(self.queue, local_max_photos=1)
        self.viewer_headers = {'Authorization':'Bearer synthetic-multi-node-viewer-token'}
        self.production_headers = {'Authorization':'Bearer synthetic-multi-node-production-token'}
        self.worker_headers = {node:{'Authorization':'Bearer synthetic-multi-node-'+node+'-token'}
            for node in self.queue.node_ids}
        self.control = ReferenceControlAPI(self.store,
            viewer_bearers={'synthetic-multi-node-viewer-token':'staging',
                'synthetic-multi-node-production-token':'production'},
            worker_bearers={'synthetic-multi-node-'+node+'-token':dict(node_id=node,
                worker_id='worker-'+node,worker_instance_id='instance-'+node) for node in self.queue.node_ids},
            capability_fingerprint='a'*64,
            options_fingerprint=self.fixture.payload['pipeline']['odm']['optionsFingerprint'],
            source_spool=spool, source_attestation_keys=self.fixture.trusted_keys,
            source_bearer_principals={'synthetic-multi-node-viewer-token': 'viewer-staging',
                'synthetic-multi-node-production-token': 'viewer-production'},
            authority_id=self.queue.authority_id)

    def admit(self, images, environment='staging'):
        payload = copy.deepcopy(self.fixture.payload)
        payload.update(submissionId=str(uuid.uuid4()),providerTaskId=str(uuid.uuid4()),
            viewerTaskId=str(uuid.uuid4()))
        photo = payload['manifest']['objects'][0]
        objects = sorted((dict(photo,id=str(uuid.uuid4()),name='photo-%04d.jpg'%index)
            for index in range(images)),key=lambda item:item['id'])
        payload['sourceManifest'].update(id=str(uuid.uuid4()),objects=[dict(
            id=item['id'],name=item['name'],byteSize=item['byteSize'],sha256=item['sha256'],
            sourceType='raw_input',retentionClass='release_after_acceptance',retentionHoldId=None)
            for item in objects],objectCount=images,totalBytes=images*3)
        payload['derivedManifest'].update(attemptId=payload['submissionId'],objects=[])
        payload['manifest'].update(objects=objects,objectCount=images,totalBytes=images*3,
            inputCounts=dict(images=images,lidarObjects=0,points=0))
        for name in ('sourceManifest','derivedManifest'):
            payload[name]['sha256'] = protocol.digest({key:value for key,value in payload[name].items() if key!='sha256'})
        payload['manifest']['sha256'] = protocol.manifest_sha256(payload['manifest'])
        payload['sourceAttestation'] = self.fixture.validate(payload)['sourceAttestation']
        if environment == 'production':
            proof = payload['sourceAttestation']['proof']
            proof.update(environment='production', principalId='viewer-production')
            message = attest.DOMAIN + protocol.canonical_json(proof)
            payload['sourceAttestation'].update(
                signature=base64.urlsafe_b64encode(self.fixture.signing_key.sign(message)).decode('ascii').rstrip('='),
                attestationSha256=hashlib.sha256(message).hexdigest())
        headers = self.production_headers if environment == 'production' else self.viewer_headers
        posted = self.control.handle('POST','/ltds/v2/jobs',headers,
            protocol.canonical_json(payload),protected_transport=True)
        self.assertEqual(posted.status,201,posted.body)
        job_id = posted.body['jobId']
        for item in objects:
            uploaded = self.control.stream_source_object(
                '/ltds/v2/jobs/%s/source/%s?generation=1'%(job_id,item['id']),
                dict(headers,**{'Content-SHA256':item['sha256']}),
                io.BytesIO(b'abc'),3,protected_transport=True)
            self.assertEqual(uploaded.status,201,uploaded.body)
        completion = dict(generation=1,sourceManifestSha256=payload['sourceManifest']['sha256'],
            derivedManifestSha256=payload['derivedManifest']['sha256'],
            pipelineManifestSha256=payload['manifest']['sha256'])
        completed = self.control.handle('POST','/ltds/v2/jobs/'+job_id+'/source/complete',
            headers,protocol.canonical_json(completion),protected_transport=True)
        self.assertEqual(completed.status,201,completed.body)
        queued = self.store.get(job_id)
        self.assertEqual((queued['status'],queued['phase'],queued['nodeId']),('queued','queued',None))
        return job_id

    def next_job(self,node):
        response = self.control.handle('GET','/ltds/worker/v1/next',self.worker_headers[node],
            protected_transport=True)
        self.assertEqual(response.status,200,response.body)
        return response.body['job']

    def claim(self,node,job_id):
        response = self.control.handle('POST','/ltds/worker/v1/claim',self.worker_headers[node],
            protocol.canonical_json(dict(jobId=job_id,claimRequestId='claim-'+str(uuid.uuid4()))),
            protected_transport=True)
        return response

    def test_source_intake_production_over_the_small_cap_leaves_it_to_staging(self):
        job_id = self.admit(2, environment='production')
        self.assertEqual(self.store.get(job_id)['environment'], 'production')
        self.assertTrue(self.queue.staging_open('cluster'))
        self.assertFalse(self.queue.staging_open('main'))

    def test_signed_production_job_takes_priority_over_older_staging_job(self):
        staging = self.admit(1)
        production = self.admit(1, environment='production')
        self.assertEqual(self.next_job('cluster')['jobId'], production)
        self.assertEqual(self.claim('cluster', production).status, 200)
        self.assertEqual(self.next_job('main')['jobId'], staging)

    def test_signed_production_does_not_preempt_a_running_staging_job(self):
        staging = self.admit(1)
        self.assertEqual(self.claim('cluster', staging).status, 200)
        production = self.admit(1, environment='production')
        self.assertEqual(self.next_job('main')['jobId'], production)
        self.assertEqual(self.claim('main', production).status, 200)
        self.assertEqual((self.store.get(staging)['status'], self.store.get(staging)['nodeId']),
                         ('running', 'cluster'))

    def test_two_small_jobs_claim_cluster_then_main_without_sharing_a_slot(self):
        first,second,third = self.admit(1),self.admit(1),self.admit(1)
        self.assertIsNone(self.next_job('main'))
        self.assertEqual(self.next_job('cluster')['jobId'],first)
        self.assertEqual(self.claim('cluster',first).status,200)
        self.assertEqual(self.next_job('main')['jobId'],second)
        self.assertEqual(self.claim('main',second).status,200)
        self.assertIsNone(self.next_job('cluster'))
        self.assertIsNone(self.next_job('main'))
        self.assertEqual(self.claim('cluster',third).status,409)
        self.assertEqual(self.claim('main',third).status,409)
        for job_id,node in ((first,'cluster'),(second,'main')):
            job = self.store.get(job_id,private=True)
            self.assertEqual((job['status'],job['nodeId']),('running',node))
            self.assertNotEqual(job['phase'],'processing')
            self.assertEqual(job['sourceAcceptanceBinding']['viewerAttemptId'],job['attemptId'])

    def test_busy_main_leaves_large_job_queued_while_cluster_takes_later_small_job(self):
        running = self.admit(2)
        self.assertIsNone(self.next_job('cluster'))
        self.assertEqual(self.next_job('main')['jobId'],running)
        self.assertEqual(self.claim('main',running).status,200)
        large,small = self.admit(2),self.admit(1)
        self.assertEqual(self.claim('cluster',large).status,409)
        self.assertEqual(self.next_job('cluster')['jobId'],small)
        self.assertEqual(self.claim('cluster',small).status,200)
        self.assertIsNone(self.next_job('main'))
        job = self.store.get(large)
        self.assertEqual((job['status'],job['phase'],job['nodeId']),('queued','queued',None))


if __name__ == '__main__':
    unittest.main()
