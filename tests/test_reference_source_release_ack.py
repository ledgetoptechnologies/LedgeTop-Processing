"""Candidate ACK wire semantics only; no mocked cleanup authority is installed."""
import copy
import json
from pathlib import Path
import unittest

from processing import reference_protocol as p
from processing.reference_source_release_ack import MAX_ACK_BYTES, validate_source_release_ack

ROOT = Path(__file__).resolve().parents[1]


class SourceReleaseAckTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads((ROOT/'contracts/source-release-ack-v1.candidate.json').read_text())
        source = json.loads((ROOT/'contracts/source-retention-v1.contract.json').read_text())
        self.context = dict(source['binding'], source_receipt=source['receipt'],
            pipeline_closure_sha256=self.fixture['expectedCommitments']['pipelineClosureSha256'],
            local_cleanup_closure_sha256=self.fixture['expectedCommitments']['localCleanupClosureSha256'])
        self.ack = copy.deepcopy(self.fixture['ack'])

    def validate(self, payload=None, **context):
        return validate_source_release_ack(self.ack if payload is None else payload,
            **{**self.context, **context})

    def test_exact_golden_detached_and_wire_bytes(self):
        result = self.validate()
        self.assertEqual(p.canonical_json(result).decode(), self.fixture['canonicalAck'])
        self.assertEqual(p.digest(result), self.fixture['ackSha256'])
        result['ackId'] = 'detached'
        self.assertNotEqual(result, self.ack)
        self.assertEqual(self.validate(p.canonical_json(self.ack)), self.ack)

    def test_every_identity_and_expected_commitment_is_bound(self):
        for field, value in dict(sourceRetentionReceiptId='wrong-source', sourceStorageReceiptId='wrong-storage',
            jobId='wrong-job', generation=2, environment='production', inputManifestSha256='e'*64,
            pipelineDigest='e'*64, pipelineClosureSha256='e'*64, localCleanupClosureSha256='e'*64).items():
            with self.subTest(field=field), self.assertRaises(p.ProtocolError):
                self.validate({**self.ack, field:value})
        for name in ('pipeline_closure_sha256', 'local_cleanup_closure_sha256'):
            with self.subTest(context=name), self.assertRaises(p.ProtocolError):
                self.validate(**{name:'f'*64})

    def test_missing_extra_family_types_and_role_substitution(self):
        for field in self.ack:
            payload = dict(self.ack); payload.pop(field)
            with self.subTest(missing=field), self.assertRaises(p.ProtocolError):self.validate(payload)
        for field, value in (('schemaVersion', True), ('generation', True), ('generation', 0),
                ('generation', 2**53), ('receiptType', 'source_retention'),
                ('validationVersion', 'ltds-reference-v1'), ('ackId', '../ack'), ('ackId', 'caf\u00e9'),
                ('ackId', self.ack['sourceRetentionReceiptId']),
                ('ackId', self.ack['sourceStorageReceiptId']), ('environment', ['staging'])):
            with self.subTest(field=field,value=value), self.assertRaises(p.ProtocolError):
                self.validate({**self.ack,field:value})
        with self.assertRaises(p.ProtocolError):self.validate({**self.ack,'cleanupComplete':True})

    def test_unvalidated_or_partial_source_receipt_cannot_substitute(self):
        for change in ({'retainedObjectIds':[]}, {'receiptType':'result_acceptance'},
                {'retentionGuarantee':'expires_in_24h'}, {'generation':2}):
            receipt = {**self.context['source_receipt'], **change}
            with self.subTest(change=change), self.assertRaises(p.ProtocolError):
                self.validate(source_receipt=receipt)

    def test_dates_hashes_duplicate_keys_and_compact_wire_bound(self):
        for timestamp in ('2026-02-30T00:00:00.000Z', '2026-10-29T24:00:00.000Z',
                '2026-10-29T00:00:00Z', '2026-10-29T00:00:00.000+00:00', True):
            with self.subTest(timestamp=timestamp), self.assertRaises(p.ProtocolError):
                self.validate({**self.ack,'acknowledgedAt':timestamp})
        for field in ('inputManifestSha256','pipelineDigest','pipelineClosureSha256','localCleanupClosureSha256'):
            for value in ('A'*64,'a'*63,True):
                with self.subTest(field=field,value=value), self.assertRaises(p.ProtocolError):
                    self.validate({**self.ack,field:value})
        wire = p.canonical_json(self.ack).decode()
        with self.assertRaises(p.ProtocolError):self.validate('{"schemaVersion":1,'+wire[1:])
        with self.assertRaises(p.ProtocolError):self.validate(' '*MAX_ACK_BYTES+wire)
        with self.assertRaises(p.ProtocolError):self.validate(b' '*(MAX_ACK_BYTES+1))
        with self.assertRaises(p.ProtocolError):self.validate('\ud800')


if __name__ == '__main__':unittest.main()
