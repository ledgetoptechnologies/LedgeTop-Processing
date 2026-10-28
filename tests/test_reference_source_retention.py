import copy
import json
from pathlib import Path
import unittest

from processing import reference_protocol as p
from processing.reference_source_retention import validate_source_retention

FIXTURE = Path(__file__).resolve().parents[1] / 'contracts' / 'source-retention-v1.contract.json'


class SourceRetentionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads(FIXTURE.read_text(encoding='utf-8'))
        self.receipt = copy.deepcopy(self.fixture['receipt'])
        self.binding = copy.deepcopy(self.fixture['binding'])

    def validate(self, receipt=None, **binding):
        return validate_source_retention(self.receipt if receipt is None else receipt,
            **{**self.binding, **binding})

    def test_golden_roundtrip_detached_and_canonical(self):
        result = self.validate()
        self.assertEqual(p.canonical_json(result).decode(), self.fixture['canonicalReceipt'])
        self.assertEqual(p.digest(result), self.fixture['receiptSha256'])
        result['retainedObjectIds'].clear()
        self.assertTrue(self.receipt['retainedObjectIds'])
        self.assertEqual(self.validate(p.canonical_json(self.receipt)), self.receipt)

    def test_identity_environment_generation_and_manifest_are_bound(self):
        changes = dict(job_id='other-job', generation=2, environment='production',
                       pipeline_digest='f' * 64)
        for name, value in changes.items():
            with self.subTest(name=name), self.assertRaises(p.ProtocolError):
                self.validate(**{name: value})
        manifest = copy.deepcopy(self.binding['input_manifest'])
        manifest['objects'][0]['byteSize'] += 1
        with self.assertRaises(p.ProtocolError):
            self.validate(input_manifest=manifest)

    def test_full_sorted_closure_only(self):
        expected = self.receipt['retainedObjectIds']
        for ids in ([], expected[:-1], expected + ['unknown'], list(reversed(expected)),
                    [expected[0]] * len(expected), 'not-a-list'):
            with self.subTest(ids=ids), self.assertRaises(p.ProtocolError):
                self.validate({**self.receipt, 'retainedObjectIds': ids})

    def test_receipt_families_guarantees_and_extensions_cannot_substitute(self):
        for field, value in (('receiptType', 'result_acceptance'),
                ('retentionGuarantee', 'expires_in_24h'), ('validationVersion', 'ltds-reference-v1'),
                ('schemaVersion', True), ('generation', True), ('receiptId', '../receipt'),
                ('storageReceiptId', self.receipt['receiptId']), ('environment', 'staging ')):
            with self.subTest(field=field), self.assertRaises(p.ProtocolError):
                self.validate({**self.receipt, field: value})
        for field in ('path', 'url', 'expiresAt', 'acceptedObjectIds'):
            with self.subTest(field=field), self.assertRaises(p.ProtocolError):
                self.validate({**self.receipt, field: 'unexpected'})

    def test_strict_utc_millisecond_timestamp_and_real_dates(self):
        for value in ('2026-10-27T12:00:00Z', '2026-10-27T12:00:00.000+00:00',
                      '2026-02-30T12:00:00.000Z', '2026-10-27T24:00:00.000Z',
                      '2026-10-27T12:00:00.000000Z', True):
            with self.subTest(value=value), self.assertRaises(p.ProtocolError):
                self.validate({**self.receipt, 'retainedAt': value})

    def test_duplicate_json_keys_and_oversized_wire_rejected(self):
        wire = p.canonical_json(self.receipt).decode()
        with self.assertRaises(p.ProtocolError):
            self.validate('{"schemaVersion":1,' + wire[1:])
        with self.assertRaises(p.ProtocolError):
            self.validate(b' ' * (p.MAX_BODY_BYTES + 1))

    def test_non_ascii_ids_and_uppercase_hashes_rejected(self):
        for field, value in (('receiptId', 'caf\u00e9'),
                             ('inputManifestSha256', self.receipt['inputManifestSha256'].upper())):
            with self.subTest(field=field), self.assertRaises(p.ProtocolError):
                self.validate({**self.receipt, field: value})


if __name__ == '__main__':
    unittest.main()
