"""Candidate compact source-release ACK validator; no authority or side effects.

Caller context must come from independently authenticated durable stores. Neither
this validator nor a matching closure digest proves custody, cleanup, finality,
inventory completeness, replay protection or permission to release a pin.
"""
import copy
from datetime import datetime
import re

from . import reference_protocol as p
from .reference_source_retention import validate_source_retention

MAX_ACK_BYTES = 16384
VALIDATION_VERSION = 'ltds-source-release-ack-v1'
FIELDS = ('schemaVersion', 'receiptType', 'validationVersion', 'ackId',
          'sourceRetentionReceiptId', 'sourceStorageReceiptId', 'jobId',
          'generation', 'environment', 'inputManifestSha256', 'pipelineDigest',
          'pipelineClosureSha256', 'localCleanupClosureSha256', 'acknowledgedAt')


def validate_source_release_ack(payload, *, source_receipt, job_id, generation,
        environment, input_manifest, pipeline_digest, pipeline_closure_sha256,
        local_cleanup_closure_sha256):
    """Check detached syntax and exact bindings; never resolve or release anything.

    The two expected closure commitments must be independently resolved. A caller
    must not obtain them by copying fields out of the ACK being validated. Actual
    authenticated issuer identity, durable receipt-role uniqueness, replay/CAS
    and late-assignment fencing remain required outside this pure function.
    """
    if isinstance(payload, (str, bytes)):
        try:
            size = len(payload.encode('utf-8')) if isinstance(payload, str) else len(payload)
        except UnicodeError:
            raise p.ProtocolError('ACK must be finite UTF-8 JSON') from None
        p._require(size <= MAX_ACK_BYTES, 'ACK exceeds body limit')
        payload = p.decode_submission(payload)
    p._closed(payload, FIELDS, 'source release ACK')
    p._require(len(p.canonical_json(payload)) <= MAX_ACK_BYTES, 'ACK exceeds body limit')
    p._require(type(payload['schemaVersion']) is int and payload['schemaVersion'] == 1,
               'unsupported source release ACK schema')
    p._require(payload['receiptType'] == 'source_release_ack', 'wrong ACK family')
    p._require(payload['validationVersion'] == VALIDATION_VERSION,
               'unsupported source release ACK validation version')
    source = validate_source_retention(source_receipt, job_id=job_id,
        generation=generation, environment=environment, input_manifest=input_manifest,
        pipeline_digest=pipeline_digest)
    for field in ('ackId', 'sourceRetentionReceiptId', 'sourceStorageReceiptId', 'jobId'):
        p._slug(payload[field], field)
    roles = (payload['ackId'], payload['sourceRetentionReceiptId'], payload['sourceStorageReceiptId'])
    p._require(len(set(roles)) == len(roles), 'source and ACK receipt roles must differ')
    p._integer(payload['generation'], 'ACK generation')
    p._environment(payload['environment'])
    p._hash(pipeline_closure_sha256, 'expected parent closure digest')
    p._hash(local_cleanup_closure_sha256, 'expected local cleanup closure digest')
    for field in ('inputManifestSha256', 'pipelineDigest', 'pipelineClosureSha256',
                  'localCleanupClosureSha256'):
        p._hash(payload[field], field)
    expected = dict(sourceRetentionReceiptId=source['receiptId'],
        sourceStorageReceiptId=source['storageReceiptId'], jobId=source['jobId'],
        generation=source['generation'], environment=source['environment'],
        inputManifestSha256=source['inputManifestSha256'], pipelineDigest=source['pipelineDigest'],
        pipelineClosureSha256=pipeline_closure_sha256,
        localCleanupClosureSha256=local_cleanup_closure_sha256)
    p._require(all(payload[key] == value for key, value in expected.items()),
               'source release ACK identity mismatch')
    timestamp = payload['acknowledgedAt']
    p._require(type(timestamp) is str and re.fullmatch(
        r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z', timestamp),
        'acknowledgedAt requires canonical UTC milliseconds')
    try:
        datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    except ValueError:
        raise p.ProtocolError('invalid acknowledgedAt') from None
    return copy.deepcopy(payload)
