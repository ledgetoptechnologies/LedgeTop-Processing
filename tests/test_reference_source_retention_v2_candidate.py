import base64
import copy
import hashlib
import json
from pathlib import Path
import unittest

from processing import reference_protocol as p
from processing.reference_source_retention_v2_candidate import (
    SourceRetentionV2CandidateError,
    CLOSURE_DOMAIN,
    RECEIPT_DOMAIN,
    validate_source_retention_v2_candidate,
)

FIXTURE = Path(__file__).resolve().parents[1] / 'contracts' / 'source-retention-v2-proposal-vector.json'


class SourceRetentionV2CandidateTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads(FIXTURE.read_text(encoding='utf-8'))
        self.envelope = copy.deepcopy(self.fixture['envelope'])
        self.payload = self.envelope['payload']
        self.manifest = copy.deepcopy(self.fixture['inputManifest'])
        self.trusted_key = {
            'issuerId': self.payload['issuerId'],
            'keyId': self.payload['keyId'],
            'keyEpoch': self.payload['keyEpoch'],
            'ownerInstanceId': self.payload['ownerInstanceId'],
            'environment': self.payload['environment'],
            'algorithm': 'Ed25519',
            'datasetId': self.payload['datasetId'],
            'revoked': False,
            'notBefore': '2026-10-01T00:00:00.000Z',
            'notAfter': '2026-12-01T00:00:00.000Z',
            'publicKeySpkiDer': base64.b64decode(self.fixture['publicKeySpkiDerBase64']),
        }

    def validate(self, envelope=None, **changes):
        args = dict(
            job_id=self.payload['jobId'], generation=self.payload['generation'],
            environment=self.payload['environment'], input_manifest=self.manifest,
            pipeline_digest=self.payload['pipelineDigest'], trusted_key=self.trusted_key,
            trusted_now='2026-11-02T12:00:00.000Z',
            verify_lineage=lambda owner, epoch: owner == 'owner-1' and epoch == 'epoch-1',
            scope='synthetic-only',
        )
        args.update(changes)
        return validate_source_retention_v2_candidate(
            self.envelope if envelope is None else envelope, **args)

    def test_signed_vector_and_full_manifest_closure_validate(self):
        result = self.validate()
        self.assertEqual(result, self.envelope)
        result['payload']['objects'].clear()
        self.assertEqual(len(self.envelope['payload']['objects']), 2)

    def test_every_published_canonical_byte_vector_matches_independently(self):
        payload = self.envelope['payload']
        objects = p.canonical_json(payload['objects'])
        closure_message = CLOSURE_DOMAIN + objects
        payload_bytes = p.canonical_json(payload)
        signed_message = RECEIPT_DOMAIN + payload_bytes
        self.assertEqual(base64.b64decode(self.fixture['objectsCanonicalUtf8Base64']), objects)
        self.assertEqual(base64.b64decode(self.fixture['closureMessageBase64']), closure_message)
        self.assertEqual(self.fixture['closureSha256'], hashlib.sha256(closure_message).hexdigest())
        self.assertEqual(base64.b64decode(self.fixture['payloadCanonicalUtf8Base64']), payload_bytes)
        self.assertEqual(base64.b64decode(self.fixture['signedMessageBase64']), signed_message)
        self.assertEqual(self.fixture['signedMessageSha256'], hashlib.sha256(signed_message).hexdigest())
        self.assertEqual(
            base64.b64decode(self.fixture['publicKeySpkiDerBase64']),
            self.trusted_key['publicKeySpkiDer'],
        )

    def test_trusted_key_must_be_valid_at_signing_and_verification_times(self):
        now = '2026-11-02T12:00:00.000Z'
        valid_at_boundary = dict(self.trusted_key, notBefore=self.payload['retainedAt'],
                                 notAfter=now)
        self.validate(trusted_key=valid_at_boundary)
        expired_now = dict(self.trusted_key, notAfter='2026-11-02T11:59:59.999Z')
        with self.assertRaises(SourceRetentionV2CandidateError):
            self.validate(trusted_key=expired_now)
        invalid_at_signing = dict(self.trusted_key, notAfter='2026-10-27T00:00:00.000Z')
        with self.assertRaises(SourceRetentionV2CandidateError):
            self.validate(trusted_key=invalid_at_signing)

    def test_context_and_environment_are_exactly_bound(self):
        for name, value in (
            ('job_id', 'other-job'), ('generation', 2),
            ('environment', 'production'), ('pipeline_digest', 'f' * 64),
        ):
            with self.subTest(name=name), self.assertRaises(p.ProtocolError):
                self.validate(**{name: value})

    def test_manifest_digest_and_full_object_closure_are_required(self):
        changed = copy.deepcopy(self.manifest)
        changed['objects'][0]['byteSize'] += 1
        with self.assertRaises(p.ProtocolError):
            self.validate(input_manifest=changed)
        changed = copy.deepcopy(self.manifest)
        changed['objects'].pop()
        changed['objectCount'] -= 1
        changed['totalBytes'] -= 3
        changed['inputCounts']['images'] -= 1
        changed['sha256'] = p.manifest_sha256(changed)
        with self.assertRaises(p.ProtocolError):
            self.validate(input_manifest=changed)

    def test_signature_algorithm_and_signature_are_verified(self):
        with self.assertRaises(p.ProtocolError):
            self.validate({**self.envelope, 'signatureAlgorithm': 'none'})
        changed = copy.deepcopy(self.envelope)
        changed['signature'] = ('A' if changed['signature'][0] != 'A' else 'B') + changed['signature'][1:]
        lineage_calls = []
        with self.assertRaises(SourceRetentionV2CandidateError):
            self.validate(changed, verify_lineage=lambda *args: lineage_calls.append(args) or True)
        self.assertEqual(lineage_calls, [])
        changed = copy.deepcopy(self.envelope)
        changed['payload']['retainedAt'] = '2026-12-02T12:00:00.000Z'
        with self.assertRaises(SourceRetentionV2CandidateError):
            self.validate(changed)

    def test_trusted_key_and_rollback_lineage_are_mandatory(self):
        for field, value in (('issuerId', 'other'), ('keyEpoch', 2),
                             ('datasetId', 'tank/other'), ('revoked', True)):
            key = dict(self.trusted_key, **{field: value})
            with self.subTest(field=field), self.assertRaises(SourceRetentionV2CandidateError):
                self.validate(trusted_key=key)
        with self.assertRaises(SourceRetentionV2CandidateError):
            self.validate(verify_lineage=lambda *_: False)
        with self.assertRaises(SourceRetentionV2CandidateError):
            self.validate(trusted_key=None)

    def test_snapshot_identity_and_uint64_txg_rules(self):
        changed = copy.deepcopy(self.envelope)
        changed['payload']['snapshotId'] += '-other'
        with self.assertRaises(SourceRetentionV2CandidateError):
            self.validate(changed)
        for txg in ('0', '01', '18446744073709551616', 1):
            changed = copy.deepcopy(self.envelope)
            changed['payload']['snapshotCreateTxg'] = txg
            with self.subTest(txg=txg), self.assertRaises(SourceRetentionV2CandidateError):
                self.validate(changed)

    def test_closed_schema_and_role_uniqueness(self):
        changed = copy.deepcopy(self.envelope)
        changed['unexpected'] = True
        with self.assertRaises(SourceRetentionV2CandidateError):
            self.validate(changed)
        changed = copy.deepcopy(self.envelope)
        changed['payload']['storageReceiptId'] = changed['payload']['receiptId']
        with self.assertRaises(SourceRetentionV2CandidateError):
            self.validate(changed)

    def test_path_dataset_hold_and_real_calendar_dates_are_strict(self):
        for dataset in ('tank/..', 'tank//spool', '/tank/spool', 'tank/spool/', 'tank/spool@x'):
            changed = copy.deepcopy(self.envelope)
            changed['payload']['datasetId'] = dataset
            with self.subTest(dataset=dataset), self.assertRaises(SourceRetentionV2CandidateError):
                self.validate(changed)
        changed = copy.deepcopy(self.envelope)
        changed['payload']['holdId'] = 'other'
        with self.assertRaises(SourceRetentionV2CandidateError):
            self.validate(changed)
        changed = copy.deepcopy(self.envelope)
        changed['payload']['retainedAt'] = '2026-02-30T12:00:00.000Z'
        with self.assertRaises(SourceRetentionV2CandidateError):
            self.validate(changed)

    def test_duplicate_wire_keys_and_production_scope_are_rejected(self):
        wire = p.canonical_json(self.envelope).decode('utf-8')
        with self.assertRaises(p.ProtocolError):
            self.validate('{"payload":{},' + wire[1:])
        with self.assertRaises(SourceRetentionV2CandidateError):
            self.validate(scope='production')


if __name__ == '__main__':
    unittest.main()
