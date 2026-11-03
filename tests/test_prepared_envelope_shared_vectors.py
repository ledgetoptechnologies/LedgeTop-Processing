"""Provisional vectors; set VIEWER_PROTOCOL_JS to the pinned Viewer source."""
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from processing import reference_protocol as p
from processing.prototype import Queue
from processing.reference_store import ReferenceStore, PROFILE

ROOT = Path(__file__).resolve().parents[1]
VECTORS = json.loads((ROOT / 'contracts/prepared-envelope-v2-vectors.json').read_text(encoding='utf8'))
DOMAIN = b'ltds-viewer-prepared-envelope-v2\0'
VIEWER_BLOB = '354192ea1a6d902171d3e78c47a96c87ca125896'


class SharedPreparedVectors(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.queue = Queue(self.temp.name, node_ids=('cluster',))
        self.addCleanup(self.queue.close)
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        self.policy = dict(capabilityFingerprint='a' * 64, optionsFingerprint='b' * 64,
                           allowedOdmOptions=['projection', 'pc-quality'], availableEngines=['odm'])

    def prepare(self, envelope):
        return self.store.prepare_viewer_envelope_v2(envelope, authenticated_principal='viewer-prod',
            authenticated_environment='production', trusted_policy=self.policy)

    def test_vectors_accepted_stored_and_digest_match(self):
        for vector in VECTORS['vectors']:
            with self.subTest(vector=vector['name']):
                envelope = copy.deepcopy(vector['envelope'])
                expected = bytes.fromhex(vector['canonicalHex'])
                self.assertEqual(p.canonical_json(envelope), expected)
                self.assertEqual(hashlib.sha256(DOMAIN + expected).hexdigest(), vector['digest'])
                job, created = self.prepare(envelope)
                self.assertTrue(created)
                with self.queue._db() as db:
                    row = db.execute('SELECT envelope_json,envelope_sha256 FROM reference_viewer_envelopes_v2 WHERE job_id=?', (job['jobId'],)).fetchone()
                self.assertEqual(row['envelope_json'].encode('utf8'), expected)
                self.assertEqual(row['envelope_sha256'], vector['digest'])

    def test_actual_pinned_viewer_canonical_bytes(self):
        source = os.environ.get('VIEWER_PROTOCOL_JS')
        if not source:
            self.skipTest('set VIEWER_PROTOCOL_JS to the pinned Viewer referenceProtocol.js')
        source_path = Path(source).resolve(strict=True)
        # Git stores LF while a Windows checkout may have CRLF worktree endings.
        source_bytes = source_path.read_bytes().replace(b'\r\n', b'\n')
        blob = hashlib.sha1(b'blob ' + str(len(source_bytes)).encode() + b'\0' + source_bytes).hexdigest()
        self.assertEqual(blob, VIEWER_BLOB, 'Viewer file does not match pinned Git blob')
        script = "const p=require(process.argv[1]);const c=require('node:crypto');let s='';process.stdin.setEncoding('utf8');process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{const a=JSON.parse(s);console.log(JSON.stringify(a.map(v=>{const b=p.canonicalBytes(v);return {canonicalHex:b.toString('hex'),digest:c.createHash('sha256').update(Buffer.concat([Buffer.from('ltds-viewer-prepared-envelope-v2\\0'),b])).digest('hex')}})))});"
        result = subprocess.run(['node', '-e', script, str(source_path)], input=json.dumps([v['envelope'] for v in VECTORS['vectors']], ensure_ascii=True), text=True, encoding='utf8', capture_output=True, check=True)
        actual = json.loads(result.stdout)
        self.assertEqual(actual, [dict(canonicalHex=v['canonicalHex'], digest=v['digest']) for v in VECTORS['vectors']])

    def test_wrong_missing_and_extra_fields_rejected(self):
        base = VECTORS['vectors'][0]['envelope']
        bad = []
        for field in base:
            value = copy.deepcopy(base); del value[field]; bad.append(value)
        value = copy.deepcopy(base); value['unexpected'] = True; bad.append(value)
        for field, replacement in [('schemaVersion', True), ('schemaVersion', 1), ('protocol', 'wrong'),
                                   ('viewerTaskId', 'NOT-A-UUID'), ('viewerAttemptId', None),
                                   ('activationId', 'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF')]:
            value = copy.deepcopy(base); value[field] = replacement; bad.append(value)
        for mutate in (lambda x: x['submission'].pop('manifest'),
                       lambda x: x['submission'].update(extra=1),
                       lambda x: x['submission']['manifest'].update(revision=0),
                       lambda x: x['submission']['manifest'].update(revision=9007199254740992),
                       lambda x: x['submission']['manifest'].update(id='x'*129)):
            value = copy.deepcopy(base); mutate(value); bad.append(value)
        for value in bad:
            with self.subTest(value=value), self.assertRaises(ValueError): self.prepare(value)
        self.assertEqual(self.queue.list(), [])

    def test_known_number_canonicalization_gap(self):
        # This is a compatibility blocker, not an approval to send such a value.
        for number in (-1, 1.5):
            envelope = copy.deepcopy(VECTORS['vectors'][0]['envelope'])
            envelope['submission']['submissionId'] = ('10000000-0000-4000-8000-000000000001' if number == -1 else '10000000-0000-4000-8000-000000000002')
            envelope['activationId'] = ('20000000-0000-4000-8000-000000000001' if number == -1 else '20000000-0000-4000-8000-000000000002')
            envelope['viewerAttemptId'] = envelope['activationId']
            envelope['submission']['pipeline']['odm']['options']['pc-quality'] = number
            self.assertTrue(self.prepare(envelope)[1])
            source = os.environ.get('VIEWER_PROTOCOL_JS')
            if source:
                script = "const p=require(process.argv[1]);try{p.canonicalBytes(JSON.parse(process.argv[2]));process.exit(1)}catch(e){process.exit(0)}"
                subprocess.run(['node', '-e', script, str(Path(source).resolve()), json.dumps(envelope)], check=True, capture_output=True)


if __name__ == '__main__':
    unittest.main()
