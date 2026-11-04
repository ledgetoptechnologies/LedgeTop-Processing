"""Unmounted coordinator-owned ODM proof journal. Not cleanup authority."""
from contextlib import contextmanager
import copy
import json
import os

from . import reference_protocol as p
from .reference_store import ReferenceStore
from .viewer_import_confirmation_store import ImportConfirmationStoreHeld
from .viewer_odm_import_confirmation_store_v2 import OdmImportConfirmationStoreV2
from .viewer_result_manifest import derive_viewer_result_manifest


class _VersionBoundOdmJournal(OdmImportConfirmationStoreV2):
    def _trusted(self, db, scope):
        context, digest = super()._trusted(db, scope)
        prepared = db.execute('''SELECT o.protocol,e.envelope_sha256 FROM reference_viewer_submission_owners o
            JOIN reference_viewer_envelopes_v2 e ON e.job_id=o.job_id WHERE o.job_id=?''', (scope['jobId'],)).fetchone()
        if prepared is None: raise ImportConfirmationStoreHeld('trusted_binding_unavailable')
        # First receipt must remain tied to the exact authenticated prepared
        # version and bytes, even if two versions carry identical identities.
        digest = p.digest(dict(bindingSha256=digest, preparedProtocol=prepared['protocol'],
                               preparedEnvelopeSha256=prepared['envelope_sha256']))
        return context, digest


class ReferenceOdmImportJournalCandidate:
    """Same-DB synthetic integration. Config and coordinator ownership are trusted.

    No route mounts this class. The caller must configure pins for exactly one
    principal/environment/issuer and must not accept them from request data.
    """
    def __init__(self, reference, *, principal, environment, issuer, import_keys, copy_keys, prepared_protocol):
        if not isinstance(reference, ReferenceStore): raise ValueError('owned reference store required')
        p._slug(principal, 'principal'); p._slug(issuer, 'issuer'); p._environment(environment)
        if type(import_keys) is not dict or not import_keys or type(copy_keys) is not dict or not copy_keys:
            raise ValueError('explicit Viewer pins required')
        if prepared_protocol not in ('ltds-viewer-prepared-envelope-v2','ltds-viewer-prepared-envelope-jcs-v3'):
            raise ValueError('explicit prepared protocol required')
        self.reference, self.queue = reference, reference.queue
        self.principal, self.environment, self.issuer = principal, environment, issuer
        self.prepared_protocol = prepared_protocol
        self.import_keys, self.copy_keys = copy.deepcopy(import_keys), copy.deepcopy(copy_keys)
        with self._owner():
            self.journal = _VersionBoundOdmJournal(self.queue.path, trusted_loader=self._load)

    @contextmanager
    def _owner(self):
        queue = self.queue
        if os.getpid() != queue._creator_pid: raise ImportConfirmationStoreHeld('coordinator_owner_unavailable')
        with queue._lifecycle_lock:
            if queue._lock.closed: raise ImportConfirmationStoreHeld('coordinator_owner_unavailable')
            yield

    def _load(self, db, scope):
        """Read all durable facts through the journal's existing transaction."""
        if scope['environment'] != self.environment: raise ValueError('environment differs')
        row = self.reference._row(db, scope['jobId'])
        if (row['generation'], row['environment'], row['queue_environment']) != (
                scope['generation'], self.environment, self.environment): raise ValueError('job differs')
        owner = self.reference._submission_owner(db, row)
        if (owner is None or owner['principal_id'] != self.principal
                or owner['protocol'] != self.prepared_protocol):
            raise ValueError('untrusted job owner')
        prepared = db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?', (scope['jobId'],)).fetchone()
        activation = self.reference._activation(db, row)
        if not prepared or not activation: raise ValueError('Viewer activation missing')
        if not row['result_json']: raise ValueError('result missing')
        result = json.loads(row['result_json'])
        manifest = derive_viewer_result_manifest(result, job_id=scope['jobId'], generation=scope['generation'],
            environment=self.environment, pipeline_digest=row['pipeline_digest'])
        transfer = db.execute('SELECT * FROM reference_viewer_transfers WHERE job_id=?', (scope['jobId'],)).fetchone()
        if not transfer or not transfer['completed_at'] or not transfer['viewer_manifest_json']:
            raise ValueError('accepted Viewer copy missing')
        if p.canonical_json(json.loads(transfer['viewer_manifest_json'])) != p.canonical_json(manifest):
            raise ValueError('Viewer manifest differs')
        copies = db.execute('''SELECT a.receipt_json AS acceptance, v.receipt_json AS receipt
            FROM reference_acceptances a JOIN reference_viewer_receipts v
            ON v.acceptance_receipt_id=a.receipt_id AND v.job_id=a.job_id
            WHERE a.job_id=? AND a.receipt_id=?''', (scope['jobId'],transfer['receipt_id'])).fetchall()
        if len(copies) != 1: raise ValueError('signed copy missing')
        envelope = dict(acceptance=json.loads(copies[0]['acceptance']), viewerReceipt=json.loads(copies[0]['receipt']))
        if p.canonical_json(envelope['acceptance']) != p.canonical_json(json.loads(transfer['acceptance_json'])):
            raise ValueError('acceptance changed')
        expected = dict(jobId=scope['jobId'], generation=scope['generation'], environment=self.environment,
            inputManifestSha256=row['manifest_sha'], pipelineDigest=row['pipeline_digest'],
            resultManifestSha256=manifest['resultManifestSha256'], viewerTaskId=prepared['viewer_task_id'],
            viewerAttemptId=prepared['viewer_attempt_id'], viewerIssuerId=self.issuer,
            acceptedEnvelopeSha256=[p.digest(envelope)])
        return dict(expected=expected, result_manifest=manifest, accepted_envelopes=[envelope],
                    trusted_import_keys=self.import_keys, trusted_copy_keys=self.copy_keys)

    def record(self, scope, confirmation):
        with self._owner(): return self.journal.record(scope, confirmation)

    def load_verified(self, scope):
        with self._owner(): return self.journal.load_verified(scope)
