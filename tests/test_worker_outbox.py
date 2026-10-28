import concurrent.futures
import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from processing.worker_outbox import WorkerOutbox, OutboxConflict
from processing.reference_protocol import digest


class WorkerOutboxTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'private' / 'outbox.sqlite'
        self.now = 100
        self.outbox = self.open()
        self.fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts' /
            'reference-v1-fixtures.json').read_text(encoding='utf-8'))
        self.manifest = copy.deepcopy(self.fixture['resultManifest'])
        self.local_objects = {'result-1': 'jobs/r1/result.bin', 'result-2': 'jobs/r1/cloud.bin'}

    def open(self):
        return WorkerOutbox(self.path, clock=lambda: self.now, random_source=lambda: 1,
                            retry_base_seconds=4, retry_max_seconds=16)

    def persist(self, **updates):
        values = dict(job_id=self.manifest['jobId'], generation=1, environment='staging',
            input_manifest_sha256=self.fixture['manifestSha256'], pipeline_digest=self.manifest['pipelineDigest'],
            result_manifest_sha256=self.manifest['resultManifestSha256'],
            result_manifest=self.manifest, owned_prefix='jobs/r1', local_objects=self.local_objects)
        values.update(updates)
        return self.outbox.persist_terminal(**values)

    def receipt(self, claim, **updates):
        payload = claim['payload']
        value = {key: payload[key] for key in ('jobId', 'generation', 'environment',
            'inputManifestSha256', 'pipelineDigest', 'resultManifestSha256')}
        value.update(type='durable-result-receipt', durable=True, receiptId='ack1',
            storageReceiptId='storage1', outboxId=claim['outboxId'],
            payloadSha256=claim['payloadSha256'], acceptedObjectIds=['result-1', 'result-2'])
        value.update(updates)
        return value

    def test_offline_restart_resends_identical_immutable_payload(self):
        outbox_id = self.persist()
        first = self.outbox.claim_next()
        self.assertEqual(first['outboxId'], outbox_id)
        self.assertEqual(self.outbox.failed_attempt(outbox_id, first['claimToken']), 104)
        self.assertIsNone(self.open().claim_next())
        self.now = 104
        second = self.open().claim_next()
        self.assertEqual(first['payload'], second['payload'])
        self.assertEqual(first['payloadSha256'], second['payloadSha256'])
        self.assertNotEqual(first['claimToken'], second['claimToken'])
        self.assertEqual(second['attempt'], 2)
        self.assertEqual(self.persist(), outbox_id)
        with self.assertRaises(ValueError):
            self.persist(pipeline_digest='f'*64)
        with self.assertRaises(OutboxConflict):
            self.persist(terminal_state='failed')
        with self.assertRaises(OutboxConflict):
            self.persist(local_objects={'result-1': 'jobs/r1/moved.bin', 'result-2': 'jobs/r1/cloud.bin'})

    def test_shared_manifest_and_wire_payload_exclude_local_paths(self):
        outbox_id = self.persist()
        claim = self.outbox.claim_next()
        payload = claim['payload']
        self.assertEqual(payload['resultManifest'], self.fixture['resultManifest'])
        self.assertEqual(set(payload), {'jobId', 'generation', 'environment',
            'inputManifestSha256', 'pipelineDigest', 'resultManifestSha256', 'resultManifest', 'terminalState'})
        self.assertEqual(digest(payload), claim['payloadSha256'])
        self.assertEqual(self.open().get(outbox_id)['local_metadata']['localObjects'], self.local_objects)

    def test_manifest_validation_prevents_credentials_urls_and_aliases(self):
        self.manifest['objects'][0]['url'] = 'https://example.invalid/output'
        with self.assertRaises(ValueError):
            self.persist()
        self.manifest = copy.deepcopy(self.fixture['resultManifest'])
        for path in ('../nas/source', 'jobs/r1/result.bin.'):
            with self.assertRaises(ValueError):
                self.persist(local_objects={'result-1': path, 'result-2': 'jobs/r1/cloud.bin'})
        with self.assertRaises(ValueError):
            self.persist(local_objects={'result-1': 'jobs/r1/result.bin', 'result-2': 'jobs/r1/RESULT.BIN'})
        with self.assertRaises(ValueError):
            self.persist(generation=0)

    def test_wrong_or_generic_ack_retains_payload_and_retry(self):
        outbox_id = self.persist()
        claim = self.outbox.claim_next()
        invalid = [dict(status=200), self.receipt(claim, durable=False),
            self.receipt(claim, generation=2), self.receipt(claim, payloadSha256='f'*64),
            self.receipt(claim, pipelineDigest='f'*64),
            self.receipt(claim, resultManifestSha256='f'*64),
            self.receipt(claim, acceptedObjectIds=[])]
        for receipt in invalid:
            with self.assertRaises(OutboxConflict):
                self.outbox.acknowledge(outbox_id, receipt)
            self.assertEqual(self.open().get(outbox_id)['state'], 'sending')
        self.outbox.failed_attempt(outbox_id, claim['claimToken'])
        self.now = 104
        self.assertEqual(self.open().claim_next()['payload'], claim['payload'])

    def test_simultaneous_claims_have_one_holder(self):
        self.persist()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(lambda _: self.open().claim_next(), range(8)))
        self.assertEqual(sum(c is not None for c in claims), 1)

    def test_crash_lease_expiry_and_stale_holder(self):
        outbox_id = self.persist()
        first = self.outbox.claim_next(lease_seconds=10)
        self.now = 109
        self.assertIsNone(self.open().claim_next())
        self.now = 110
        second = self.open().claim_next(lease_seconds=10)
        self.assertEqual(second['payload'], first['payload'])
        self.assertEqual(second['attempt'], 2)
        with self.assertRaises(OutboxConflict):
            self.outbox.failed_attempt(outbox_id, first['claimToken'])
        self.assertEqual(self.outbox.failed_attempt(outbox_id, second['claimToken']), 118)
        self.now = 117
        self.assertIsNone(self.open().claim_next())
        self.now = 118
        third = self.open().claim_next()
        self.assertEqual(self.outbox.failed_attempt(outbox_id, third['claimToken']), 134)
        self.now = 134
        fourth = self.open().claim_next()
        self.assertEqual(self.outbox.failed_attempt(outbox_id, fourth['claimToken']), 150)

    def test_acknowledged_terminal_result_is_retained_not_cleaned(self):
        outbox_id = self.persist()
        claim = self.outbox.claim_next()
        receipt = self.receipt(claim)
        self.outbox.acknowledge(outbox_id, receipt)
        self.open().acknowledge(outbox_id, receipt)
        row = self.open().get(outbox_id)
        self.assertEqual(row['state'], 'acknowledged')
        self.assertEqual(row['payload'], claim['payload'])
        self.assertEqual(row['receipt'], receipt)
        self.assertIsNone(self.open().claim_next())
        self.assertEqual(self.persist(), outbox_id)
        with self.assertRaises(OutboxConflict):
            self.outbox.acknowledge(outbox_id, self.receipt(claim, receiptId='other'))

    def test_environment_binding_survives_restart_and_generation_change(self):
        self.persist()
        self.outbox = self.open()
        self.manifest.update(generation=2, environment='production')
        self.manifest['resultManifestSha256'] = digest({k: v for k, v in self.manifest.items() if k != 'resultManifestSha256'})
        with self.assertRaises(OutboxConflict):
            self.persist(generation=2, environment='production')

    def test_durable_receipt_identity_cannot_bind_another_generation(self):
        first_id = self.persist()
        first = self.outbox.claim_next()
        self.outbox.acknowledge(first_id, self.receipt(first))
        self.manifest['generation'] = 2
        self.manifest['resultManifestSha256'] = digest({k: v for k, v in self.manifest.items() if k != 'resultManifestSha256'})
        second_id = self.persist(generation=2)
        second = self.outbox.claim_next()
        with self.assertRaises(OutboxConflict):
            self.outbox.acknowledge(second_id, self.receipt(second))
        self.assertEqual(self.open().get(second_id)['state'], 'sending')

    def test_clock_rollback_cannot_reopen_expired_attempt(self):
        outbox_id = self.persist()
        first = self.outbox.claim_next(lease_seconds=10)
        self.now = 120
        with self.assertRaises(OutboxConflict):
            self.outbox.failed_attempt(outbox_id, first['claimToken'])
        self.now = 105
        with self.assertRaises(OutboxConflict):
            self.open().failed_attempt(outbox_id, first['claimToken'])
        self.assertEqual(self.open().claim_next()['attempt'], 2)

    def test_real_sqlite_connection_uses_full_durability_and_transactional_schema(self):
        connect = sqlite3.connect
        observed = []
        class InspectConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                if sql.startswith('CREATE'):
                    observed.append((self.in_transaction, super().execute('PRAGMA synchronous').fetchone()[0]))
                return super().execute(sql, parameters)
        with patch('processing.worker_outbox.sqlite3.connect',
                   lambda *args, **kwargs: connect(*args, **kwargs, factory=InspectConnection)):
            self.open()
        self.assertTrue(observed)
        self.assertTrue(all(transaction and sync == 2 for transaction, sync in observed))


if __name__ == '__main__':
    unittest.main()
