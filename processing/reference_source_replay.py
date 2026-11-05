"""Read-only reconciliation of an already admitted, exact V2 source request.

Live catalogs authorize new admissions, not a second admission for an existing
immutable request. Current source signer trust still applies to every replay.
No source receipt, reservation, execution or deletion is created here.
"""
import json

from . import reference_protocol as p
from .maintenance import Conflict
from .reference_limits import V2_SOURCE_INTAKE_BYTES
from .reference_source_intake import INTAKE_FIELDS, validate_source_intake


_LOOKUP = """SELECT r.job_id,r.submission_id,r.environment,r.input_json,
    r.request_digest AS pipeline_request_digest,r.manifest_sha,r.pipeline_digest,
    j.environment AS queue_environment,
    r.generation AS reference_generation,
    s.request_digest AS source_request_digest,s.viewer_task_id,s.provider_task_id,
    s.source_manifest_json,s.derived_manifest_json,s.source_attestation_json,
    s.source_attestation_sha,s.source_manifest_sha,s.derived_manifest_sha,
    s.pipeline_manifest_sha
    FROM reference_jobs r JOIN jobs j ON j.id=r.job_id
    LEFT JOIN reference_source_intakes s ON s.job_id=r.job_id
    WHERE r.submission_id=?"""


def replay_source_intake(store, payload, *, environment, authority_id,
                         trusted_source_keys):
    """Return existing public metadata or None; never admit a new request.

    Validate expensive manifest/signature work outside the Queue transaction,
    then recheck the exact persisted binding before returning current metadata.
    Unknown requests must continue through ordinary fresh-catalog validation.
    """
    p._closed(payload, INTAKE_FIELDS, 'source intake replay')
    p._require(type(payload['schemaVersion']) is int and payload['schemaVersion'] == 2,
               'unsupported source intake version')
    p._uuid(payload['submissionId'], 'submission ID')
    p._environment(environment)
    with store.queue._db() as db:
        saved = db.execute(_LOOKUP, (payload['submissionId'],)).fetchone()
        if saved is None:
            return None
        saved = dict(saved)
    p._require(authority_id == store.queue.authority_id, 'source authority changed')
    if (saved['environment'] != environment or saved['queue_environment'] != environment
            or not saved['source_request_digest']
            or p.digest(payload) != saved['source_request_digest']):
        raise Conflict('submissionId belongs to a different reference request')
    submission = json.loads(saved['input_json'])
    original = dict(schemaVersion=2, submissionId=saved['submission_id'],
        providerTaskId=saved['provider_task_id'], viewerTaskId=saved['viewer_task_id'],
        sourceManifest=json.loads(saved['source_manifest_json']),
        derivedManifest=json.loads(saved['derived_manifest_json']),
        manifest=submission['manifest'], pipeline=submission['pipeline'],
        capabilityFingerprint=submission['capabilityFingerprint'],
        sourceAttestation=json.loads(saved['source_attestation_json']))
    if p.digest(original) != saved['source_request_digest']:
        raise Conflict('persisted source replay binding changed')
    p._integer(saved['reference_generation'], 'reference generation')
    options = submission['pipeline']['odm']
    # This allowlist is safe only for the exact persisted request above; it
    # must never be used to authorize a new or changed submission.
    allowed = frozenset(options['options'])
    checked = validate_source_intake(original, environment=environment,
        capability_fingerprint=submission['capabilityFingerprint'],
        options_fingerprint=options['optionsFingerprint'], allowed_odm_options=allowed,
        available_engines=('odm', 'treedar'), trusted_source_keys=trusted_source_keys,
        authority_id=authority_id)
    expected = p.validate_submission({name: original[name] for name in
        ('schemaVersion', 'submissionId', 'manifest', 'pipeline', 'capabilityFingerprint')}
        | {'schemaVersion': 1}, environment=environment,
        capability_fingerprint=submission['capabilityFingerprint'],
        options_fingerprint=options['optionsFingerprint'], allowed_odm_options=allowed,
        available_engines=('odm', 'treedar'), max_body_bytes=V2_SOURCE_INTAKE_BYTES)
    expected.update(pipelineDigest=checked['pipelineDigest'], sourceIntakeVersion=2)
    if (p.canonical_json(expected) != p.canonical_json(submission)
            or saved['pipeline_request_digest'] != expected['requestDigest']
            or saved['manifest_sha'] != checked['manifestSha256']
            or saved['pipeline_digest'] != checked['pipelineDigest']
            or saved['source_manifest_sha'] != checked['sourceManifestSha256']
            or saved['derived_manifest_sha'] != checked['derivedManifestSha256']
            or saved['pipeline_manifest_sha'] != checked['manifestSha256']
            or saved['source_attestation_sha'] != checked['sourceAttestation']['attestationSha256']):
        raise Conflict('persisted source replay binding changed')
    with store.queue._db() as db:
        db.execute('BEGIN')
        current = db.execute(_LOOKUP, (payload['submissionId'],)).fetchone()
        if current is None or dict(current) != saved:
            raise Conflict('source replay binding changed during verification')
        return store._public(store._row(db, saved['job_id']))
