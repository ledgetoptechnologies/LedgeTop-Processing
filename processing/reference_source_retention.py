"""Source-only NAS retention receipt contract; no authentication or cleanup.

The issuer must enforce durable retention pins against every purge path (or use
separate immutable storage) before issuing this receipt. The coordinator must
authenticate the issuer and enforce receipt-ID role uniqueness across source,
result and terminal receipts. Validation alone proves neither of those facts.
"""
import copy
from datetime import datetime
import re

from . import reference_protocol as p

VALIDATION_VERSION = 'ltds-source-retention-v1'
RETENTION_GUARANTEE = 'until_release_ack_v1'
FIELDS = ('schemaVersion', 'receiptType', 'receiptId', 'storageReceiptId', 'jobId',
          'generation', 'environment', 'inputManifestSha256', 'pipelineDigest',
          'retainedObjectIds', 'retainedAt', 'validationVersion', 'retentionGuarantee')


def validate_source_retention(payload, *, job_id, generation, environment,
                              input_manifest, pipeline_digest):
    """Bind a detached full-closure receipt to an already validated submission.

    Authenticated durable worker/outbox ownership remains valid after execution
    capacity is released; this contract does not require a current execution
    lease or grant a new one. No worker file is accepted or deleted here.
    """
    if isinstance(payload, (str, bytes)):
        payload = p.decode_submission(payload)
    p._require(len(p.canonical_json(payload)) <= p.MAX_BODY_BYTES, 'receipt exceeds body limit')
    p._closed(payload, FIELDS, 'source retention receipt')
    p._require(type(payload['schemaVersion']) is int and payload['schemaVersion'] == 1,
               'unsupported source retention schema')
    p._require(payload['receiptType'] == 'source_retention', 'wrong receipt family')
    p._require(payload['validationVersion'] == VALIDATION_VERSION,
               'unsupported source retention validation version')
    p._require(payload['retentionGuarantee'] == RETENTION_GUARANTEE,
               'durable source retention guarantee required')
    for field in ('receiptId', 'storageReceiptId', 'jobId'):
        p._slug(payload[field], field)
    p._require(payload['receiptId'] != payload['storageReceiptId'], 'receipt roles must differ')
    p._slug(job_id, 'expected job ID')
    p._integer(generation, 'expected generation')
    p._integer(payload['generation'], 'generation')
    p._environment(environment)
    p._environment(payload['environment'])
    p._hash(pipeline_digest, 'expected pipeline digest')
    for field in ('inputManifestSha256', 'pipelineDigest'):
        p._hash(payload[field], field)
    p._require((payload['jobId'], payload['generation'], payload['environment'], payload['pipelineDigest'])
               == (job_id, generation, environment, pipeline_digest), 'source retention identity mismatch')
    p._require(type(input_manifest) is dict, 'validated input manifest required')
    p._require(input_manifest.get('sha256') == p.manifest_sha256(input_manifest),
               'input manifest digest mismatch')
    p._require(payload['inputManifestSha256'] == input_manifest['sha256'],
               'source retention input digest mismatch')
    objects = input_manifest.get('objects')
    p._require(type(objects) is list and 0 < len(objects) <= p.MAX_OBJECTS,
               'validated input object closure required')
    ids = []
    for obj in objects:
        p._require(type(obj) is dict and 'id' in obj, 'validated input object required')
        p._slug(obj['id'], 'input object ID')
        ids.append(obj['id'])
    p._require(len(set(ids)) == len(ids), 'duplicate input object ID')
    retained = payload['retainedObjectIds']
    p._require(type(retained) is list and len(retained) == len(ids), 'full retained closure required')
    for value in retained:
        p._slug(value, 'retained object ID')
    # ASCII order is intentional and matches the Node contract; no locale sort.
    p._require(retained == sorted(ids), 'retained objects must be the exact sorted closure')
    timestamp = payload['retainedAt']
    p._require(type(timestamp) is str and re.fullmatch(
        r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z', timestamp),
        'retainedAt requires canonical UTC milliseconds')
    try:
        datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    except ValueError:
        raise p.ProtocolError('invalid retainedAt') from None
    return copy.deepcopy(payload)
