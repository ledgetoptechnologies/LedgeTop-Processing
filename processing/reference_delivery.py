"""Private worker-result receipt barrier, separate from Viewer acceptance.

The transport authenticates the configured worker identity before calling this
service. This receipt proves durable metadata registration, not file replication
or permission to clean worker files. Surviving workers can replay it after loss.
"""
import json

from . import reference_protocol as protocol
from .maintenance import Conflict


BINDING = ('jobId', 'generation', 'environment', 'inputManifestSha256',
           'pipelineDigest', 'resultManifestSha256')


class DeliveryReceiver:
    def __init__(self, store):
        self.store = store
        with store.queue._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS reference_worker_receipts (
              outbox_id TEXT PRIMARY KEY, payload_sha TEXT NOT NULL,
              job_id TEXT NOT NULL, receipt_json TEXT NOT NULL)''')

    def receive(self, envelope, *, node_id, worker_id, worker_instance_id,
                claim_request_id, claim_token):
        protocol._closed(envelope, ('outboxId', 'payloadSha256', 'payload'), 'delivery')
        protocol._hash(envelope['outboxId'], 'outbox ID')
        protocol._hash(envelope['payloadSha256'], 'payload digest')
        payload = envelope['payload']
        protocol._closed(payload, (*BINDING, 'resultManifest', 'terminalState'), 'worker result')
        if protocol.digest(payload) != envelope['payloadSha256']:
            raise Conflict('delivery payload digest mismatch')
        binding = {key: payload[key] for key in BINDING}
        if protocol.digest(binding) != envelope['outboxId']:
            raise Conflict('delivery outbox identity mismatch')
        job = self.store.get_v1(payload['jobId'], private=True)
        if (payload['generation'], payload['environment'], payload['inputManifestSha256'],
                payload['pipelineDigest']) != (job['generation'], job['environment'],
                job['manifestSha256'], job['pipelineDigest']):
            raise Conflict('worker result identity mismatch')
        if payload['terminalState'] != 'succeeded':
            raise Conflict('failure reconciliation is required')
        result = protocol.validate_result_manifest(payload['resultManifest'],
            job_id=job['jobId'], generation=job['generation'], environment=job['environment'],
            pipeline_digest=job['pipelineDigest'])
        if result['resultManifestSha256'] != payload['resultManifestSha256']:
            raise Conflict('worker manifest digest mismatch')
        requested = {engine for engine in ('odm', 'treedar')
                     if job['submission']['pipeline'][engine]['requested']}
        if set(result['engineVersions']) != requested:
            raise Conflict('result engine versions do not match the requested pipeline')
        if 'odm' in requested and sum(
                obj['role'] == 'odm_archive' and obj['byteSize'] > 0 for obj in result['objects']) != 1:
            raise Conflict('ODM delivery requires the original archive')
        # Authenticate even exact delivery replays, before exposing old receipts.
        self.store.record_worker_result(job['jobId'], result, claim_token=claim_token,
            node_id=node_id, worker_id=worker_id, worker_instance_id=worker_instance_id,
            claim_request_id=claim_request_id)
        self.store.queue.complete(job['jobId'], {'claimToken': claim_token,
            'status': 'succeeded', 'result': {'resultManifestSha256': result['resultManifestSha256']}})
        if self.store.get(job['jobId'])['phase'] == 'waiting_for_delivery':
            self.store.update_phase(job['jobId'], 'awaiting_acceptance')
        # Each preceding FULL transaction has committed before issuing a receipt.
        # A crash between them retains files and exact replay safely finishes.
        receipt = dict(type='durable-result-receipt', durable=True,
            receiptId='delivery-' + envelope['outboxId'],
            storageReceiptId='coordinator-' + envelope['outboxId'],
            outboxId=envelope['outboxId'], payloadSha256=envelope['payloadSha256'],
            acceptedObjectIds=sorted(obj['id'] for obj in result['objects']), **binding)
        encoded = protocol.canonical_json(receipt).decode()
        with self.store.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT * FROM reference_worker_receipts WHERE outbox_id=?',
                             (envelope['outboxId'],)).fetchone()
            if old:
                if old['payload_sha'] != envelope['payloadSha256'] or old['receipt_json'] != encoded:
                    raise Conflict('worker delivery receipt changed')
                return json.loads(old['receipt_json'])
            self.store._quota(db, len(encoded.encode()))
            db.execute('INSERT INTO reference_worker_receipts VALUES(?,?,?,?)',
                (envelope['outboxId'], envelope['payloadSha256'], job['jobId'], encoded))
        return receipt
