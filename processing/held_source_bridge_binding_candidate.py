"""Held-only job-scoped v2 journal binding; no route or custody authorization."""
import hashlib
import json
from types import MappingProxyType

from . import reference_protocol as p
from . import prepared_envelope_jcs_candidate as jcs
from .held_source_bridge_journal_candidate import _stage
from .paged_snapshot_journal_candidate import ServerSnapshotTrust, pin_registry
from .viewer_paged_frame_candidate import _pairs


class BoundBridgeTrust:
    """Provision only from trusted server configuration, never request data."""
    def __init__(self, *, outer, local_key_entries, revocation_epoch, max_selected_bytes, max_object_bytes):
        if type(outer) is not ServerSnapshotTrust:
            raise ValueError('configured scoped outer trust required')
        p._integer(revocation_epoch, 'revocation epoch', allow_zero=True)
        p._integer(max_selected_bytes, 'maximum selected bytes')
        p._integer(max_object_bytes, 'maximum object bytes')
        if max_object_bytes > max_selected_bytes:
            raise ValueError('object limit exceeds selected total limit')
        if type(local_key_entries) is not list or any(
                type(entry) is not dict or entry.get('purpose') != 'held-viewer-snapshot' for entry in local_key_entries):
            raise ValueError('configured local Viewer trust required')
        converted = [{**entry, 'purpose': 'paged-source-snapshot'} for entry in local_key_entries]
        registry = pin_registry(converted)
        if any(scope[0] != outer.authority_id for scope in registry):
            raise ValueError('local key authority differs')
        self.outer = outer
        self.local_keys = MappingProxyType(registry)
        self.revocation_epoch = revocation_epoch
        self.max_selected_bytes = max_selected_bytes
        self.max_object_bytes = max_object_bytes


def inspect(store, submission_id, *, authenticated_principal, authenticated_environment, trust,
            statement_bytes, intent_bytes, local_signature, source_frames, selected_frames,
            evidence_frames, file_frames, processing_closure, processing_pages, _fault=None):
    """Verify and journal exact evidence within the prepared-job transaction.

    No network route calls this candidate. It does not activate a job, grant
    source access, prove custody, or permit cleanup.
    """
    try:
        if (type(trust) is not BoundBridgeTrust or type(statement_bytes) is not bytes
                or not 0 < len(statement_bytes) <= 65536):
            raise ValueError()
        p._uuid(submission_id, 'submission'); p._slug(authenticated_principal, 'principal')
        p._environment(authenticated_environment)
        # Use the same detached pages for prepared-manifest comparison and
        # journal verification; mutable caller objects cannot change between.
        closure_wire, pages_wire = p.canonical_json(processing_closure), p.canonical_json(processing_pages)
        if len(closure_wire) > 65536 or len(pages_wire) > 12*1024*1024:
            raise ValueError()
        processing_closure = json.loads(closure_wire.decode())
        processing_pages = json.loads(pages_wire.decode())
        statement = json.loads(statement_bytes.decode('utf8'), object_pairs_hook=_pairs,
                               parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        bridge = statement['viewerHeldSource']
        authority = trust.outer.authority_id
        scope = (authority, authenticated_principal, authenticated_environment)
        outer_key = trust.outer.registry.get((*scope, statement['issuerId'], statement['keyId']))
        local_key = trust.local_keys.get((*scope, bridge['issuerId'], bridge['keyId']))
        if outer_key is None or local_key is None or trust.outer.clock_provider is None:
            raise ValueError()
        now = trust.outer.clock_provider()
        if type(now) is not int or not 0 <= now <= p.MAX_SAFE_INTEGER:
            raise ValueError()
        with store.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            store._check_jcs_capability(db); store._require_viewer_authority(db, authority)
            found = store._scoped_prepared_job_id(db, submission_id, authenticated_principal,
                                                  authenticated_environment, jcs.PROTOCOL)
            if found is None:
                raise ValueError()
            row = store._row(db, found['job_id']); owner = store._submission_owner(db, row)
            if owner is None or (owner['principal_id'], owner['environment'], owner['protocol']) != (
                    authenticated_principal, authenticated_environment, jcs.PROTOCOL):
                raise ValueError()
            envelope = db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?', (row['job_id'],)).fetchone()
            policy = db.execute('SELECT * FROM reference_viewer_v3_policy WHERE job_id=?', (row['job_id'],)).fetchone()
            if envelope is None or policy is None:
                raise ValueError()
            pol = json.loads(policy['policy_json'])
            if (p.canonical_json(pol).decode() != policy['policy_json']
                    or hashlib.sha256(policy['policy_json'].encode()).hexdigest() != policy['policy_sha256']):
                raise ValueError()
            _, digest, normalized = jcs.validate(envelope['envelope_json'].encode(), trusted_policy=pol,
                                                  environment=authenticated_environment)
            if (digest != envelope['envelope_sha256'] or p.canonical_json(normalized).decode() != row['input_json']):
                raise ValueError()
            # The signed closure must describe exactly the prepared job's
            # objects, not merely some valid Viewer selection.
            objects = [obj for page in processing_pages for obj in page['body']['objects']]
            if p.canonical_json(objects) != p.canonical_json(normalized['manifest']['objects']):
                raise ValueError()
            context = dict(issuerId=statement['issuerId'], keyId=statement['keyId'], principalId=authenticated_principal,
                environment=authenticated_environment, coordinatorAuthorityId=authority, jobId=row['job_id'],
                generation=row['generation'], submissionId=submission_id, activationId=envelope['activation_id'],
                viewerTaskId=envelope['viewer_task_id'], viewerAttemptId=envelope['viewer_attempt_id'],
                preparedEnvelopeSha256=digest, datasetId=normalized['manifest']['id'],
                datasetManifestSha256=statement['datasetManifestSha256'], processingManifestSha256=normalized['manifestSha256'],
                pipelineDigest=normalized['pipelineDigest'], processingClosureSha256=statement['processingClosureSha256'],
                revocationEpoch=trust.revocation_epoch, localIssuerId=bridge['issuerId'], localKeyId=bridge['keyId'],
                maxSelectedBytes=trust.max_selected_bytes, maxObjectBytes=trust.max_object_bytes)
            arguments = dict(statement_bytes=statement_bytes, outer_key=outer_key, local_key=local_key,
                context=context, now_ms=now, intent_bytes=intent_bytes, local_signature=local_signature,
                source_frames=source_frames, selected_frames=selected_frames, evidence_frames=evidence_frames,
                file_frames=file_frames, processing_closure=processing_closure, processing_pages=processing_pages,
                max_future_skew_ms=trust.outer.max_future_skew_ms)
            return _stage(db, arguments=arguments, _fault=_fault, _owned=True)
    except Exception:
        raise ValueError('bound_bridge_unavailable') from None
