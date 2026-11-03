"""Cross-language contract checks for the Viewer attempt binding on source-range v2.

Opt-in and synthetic. It loads the Viewer source broker and reference provider
modules from ``LEDGETOP_VIEWER_SOURCE_DIR`` with Node and compares them with the
Processing source-range client on the same values. Nothing is served, minted
against real data, or deployed.

The Viewer source-grant dispatch candidate (c92deb5) adds ``viewerAttemptId`` to
the queued transfer request, the grant, and the broker's canonical source
identity. This commit carries the matching Processing proposal: a submission's
``viewerAttemptId`` is echoed into the transfer request and bound into the
source identity. Each test states a property both sides must share.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import unittest

from processing import reference_protocol as p
from processing.source_range_transfer import (
    CHUNK_BYTES, SourceIdentity, SourceObject, TransferError, _request_id,
    build_source_transfer_request, source_version_for_manifest, validate_source_grant,
)

VIEWER_SOURCE_DIR = os.environ.get('LEDGETOP_VIEWER_SOURCE_DIR')
NODE = shutil.which('node')
ATTEMPT = '00000000-0000-4000-8000-000000000001'
NODE_PROBE = r'''
const path = require('node:path');
const server = path.join(process.env.LEDGETOP_VIEWER_SOURCE_DIR, 'apps/viewer/server');
const broker = require(path.join(server, 'sourceRangeV2Candidate.js'));
const provider = require(path.join(server, 'referenceProvider.js'));
const input = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
const verdict = fn => { try { fn(); return 'accepted'; } catch (error) { return error.code || error.message; } };
const identityJson = broker.canonicalIdentity(input.identity);
process.stdout.write(JSON.stringify({
  identityJson,
  requestId: broker.requestIdFor(identityJson, input.object, input.start, input.length),
  transferRequest: verdict(() => provider.sourceRangeTransferRequest(input.transferRequest)),
}));
'''


def manifest():
    body = b'synthetic attempt binding object'
    value = {'id': 'dataset-attempt-binding', 'revision': 1, 'totalBytes': len(body), 'objectCount': 1,
             'inputCounts': {'images': 1, 'lidarObjects': 0, 'points': 0},
             'objects': [{'id': 'photo-1', 'name': 'photo-1.jpg', 'byteSize': len(body),
                          'sha256': hashlib.sha256(body).hexdigest(), 'kind': 'image', 'mediaType': 'image/jpeg'}]}
    value['sha256'] = p.manifest_sha256(value)
    return value


@unittest.skipUnless(VIEWER_SOURCE_DIR and NODE, 'set LEDGETOP_VIEWER_SOURCE_DIR to a Viewer checkout with the attempt binding')
class ViewerAttemptBindingContractTests(unittest.TestCase):
    def setUp(self):
        self.manifest = manifest()
        self.job = {'jobId': 'job-1', 'generation': 1, 'environment': 'staging',
                    'manifestSha256': self.manifest['sha256'],
                    'submission': {'manifest': self.manifest, 'viewerAttemptId': ATTEMPT}}
        expires = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime(time.time() + 3600))
        self.transfer = build_source_transfer_request(self.job, reservation_id='reservation-1', expires_at=expires,
                                                      nonce='nonce-1')
        self.job['transferRequest'] = self.transfer
        self.source_version = source_version_for_manifest(self.manifest)

    def node(self, **values):
        result = subprocess.run([NODE, '-e', NODE_PROBE], input=json.dumps(values), capture_output=True, text=True,
                                env={**os.environ, 'LEDGETOP_VIEWER_SOURCE_DIR': str(Path(VIEWER_SOURCE_DIR).resolve())},
                                check=True)
        return json.loads(result.stdout)

    def viewer_identity(self):
        return {'environment': 'staging', 'generation': 1, 'jobId': 'job-1',
                'manifestSha256': self.manifest['sha256'], 'reservationId': 'reservation-1',
                'sourceVersion': self.source_version, 'viewerAttemptId': ATTEMPT}

    def processing_identity_json(self):
        identity = SourceIdentity('staging', 'job-1', 'reservation-1', 1, self.manifest['sha256'], self.source_version,
                                  ATTEMPT)
        return json.dumps(identity.canonical(), sort_keys=True, separators=(',', ':'))

    def viewer_grant(self):
        return {**self.transfer, 'jobId': 'job-1', 'environment': 'staging', 'token': 'A' * 43}

    def test_range_request_ids_agree(self):
        # Without the attempt in Processing's identity, every range request ID
        # differs and the broker answers INVALID_REQUEST_ID (HTTP 400).
        obj = self.manifest['objects'][0]
        start, length = 0, obj['byteSize']
        viewer = self.node(identity=self.viewer_identity(), transferRequest=self.transfer, start=start, length=length,
                           object={'id': obj['id'], 'byteSize': obj['byteSize'], 'sha256': obj['sha256']})
        processing = _request_id(self.processing_identity_json(),
                                 SourceObject(obj['id'], obj['byteSize'], obj['sha256']), start, length)
        self.assertEqual(viewer['requestId'], processing)

    def test_viewer_accepts_the_transfer_request_processing_queues(self):
        # The Viewer reference provider requires viewerAttemptId in its closed field set.
        obj = self.manifest['objects'][0]
        verdict = self.node(identity=self.viewer_identity(), transferRequest=self.transfer, start=0, length=1,
                            object={'id': obj['id'], 'byteSize': obj['byteSize'], 'sha256': obj['sha256']})
        self.assertEqual(verdict['transferRequest'], 'accepted')

    def test_processing_accepts_the_grant_viewer_mints(self):
        grant = validate_source_grant(self.viewer_grant(), job=self.job)
        self.assertEqual(grant.identity.viewer_attempt_id, ATTEMPT)

    def test_attempt_binding_must_match_on_both_sides(self):
        for grant in ({**self.viewer_grant(), 'viewerAttemptId': '00000000-0000-4000-8000-000000000002'},
                      {k: v for k, v in self.viewer_grant().items() if k != 'viewerAttemptId'}):
            with self.subTest(grant=grant.get('viewerAttemptId')), self.assertRaises(TransferError):
                validate_source_grant(grant, job=self.job)
        legacy = {**self.job, 'submission': {'manifest': self.manifest}}
        expires = self.transfer['expiresAt']
        legacy['transferRequest'] = build_source_transfer_request(legacy, reservation_id='reservation-1',
                                                                  expires_at=expires, nonce='nonce-1')
        self.assertNotIn('viewerAttemptId', legacy['transferRequest'])
        with self.assertRaises(TransferError):  # an attempt-bound grant cannot satisfy an unbound job
            validate_source_grant(self.viewer_grant(), job=legacy)

    def test_both_sides_keep_the_same_chunk_and_identity(self):
        self.assertEqual(self.transfer['chunkBytes'], CHUNK_BYTES)
        self.assertEqual(json.loads(self.processing_identity_json()), self.viewer_identity())
        obj = self.manifest['objects'][0]
        viewer = self.node(identity=self.viewer_identity(), transferRequest=self.transfer, start=0, length=1,
                           object={'id': obj['id'], 'byteSize': obj['byteSize'], 'sha256': obj['sha256']})
        self.assertEqual(viewer['identityJson'], self.processing_identity_json())


if __name__ == '__main__':
    unittest.main()
