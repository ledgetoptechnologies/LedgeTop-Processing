"""Inactive ODM-only Viewer import confirmation proposal (version 2).

This verifies signed assertions; it does not publish a model, persist replay
state, authorize cleanup, or expose a route. Viewer task/attempt and Processing
job/copy bindings must come from independent trusted durable state. Viewer
model/output IDs and inventory digests are post-ingest signed assertions, not
independent Processing expectations. Contract pairing remains required.
"""
from datetime import datetime
import hashlib
import re

from cryptography.exceptions import InvalidSignature

from . import reference_protocol as protocol
from .viewer_acceptance_receipt import (
    ViewerReceiptRejected, _canonical_signature, _load_key,
    verify_viewer_acceptance_receipt,
)
from .viewer_import_confirmation import (
    ImportConfirmationRejected, VerifiedImportConfirmation, _hash, _id, _snapshot_inputs,
)
from .viewer_result_manifest import validate_viewer_acceptance, validate_viewer_result_manifest


PROTOCOL = 'ltds-viewer-odm-import-confirmation-v2'
SIGNATURE_DOMAIN = b'LTDS-VIEWER-ODM-IMPORT-CONFIRMATION-V2\0'
ARCHIVE_INVENTORY_FORMAT = 'viewer-processing-archive-evidence-v1'
OUTPUT_INVENTORY_FORMAT = 'viewer-model-output-inventory-v1'
BOUND_FIELDS = ('jobId', 'generation', 'environment', 'inputManifestSha256',
                'pipelineDigest', 'resultManifestSha256', 'viewerTaskId', 'viewerAttemptId')
EXPECTED_FIELDS = frozenset((*BOUND_FIELDS, 'viewerIssuerId', 'acceptedEnvelopeSha256'))
FIELDS = frozenset(('protocol', 'schemaVersion', 'issuerId', 'keyId', 'confirmationId',
    *BOUND_FIELDS, 'modelId', 'modelVersionId', 'outputId', 'outputInventoryFormat',
    'outputInventorySha256', 'acceptedCopies', 'objects', 'archive', 'milestone', 'readyAt', 'signature'))
_TIME = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z\Z')


def _need(condition, code):
    if not condition:
        raise ImportConfirmationRejected(code)


def verify_odm_import_confirmation(confirmation, *, trusted_import_keys, trusted_copy_keys,
                                   expected, result_manifest, accepted_envelopes,
                                   prior_confirmation_sha256=None):
    """Verify the narrow signed assertion against independently trusted inputs.

    Never accept a caller-created VerifiedImportConfirmation instance as proof.
    Request handlers must bound raw bytes before JSON parsing. The caller must
    atomically persist the first verified digest and require it on every replay;
    this pure function cannot enforce that rule or create the trusted mapping.
    """
    (confirmation, trusted_import_keys, trusted_copy_keys, expected, result_manifest,
     accepted_envelopes, prior_confirmation_sha256) = _snapshot_inputs(
        confirmation, trusted_import_keys, trusted_copy_keys, expected,
        result_manifest, accepted_envelopes, prior_confirmation_sha256)
    _need(type(expected) is dict and set(expected) == EXPECTED_FIELDS, 'invalid_expected_binding')
    for field in ('jobId', 'viewerTaskId', 'viewerAttemptId', 'viewerIssuerId'):
        _need(_id(expected[field]), 'invalid_expected_identity')
    _need(type(expected['generation']) is int and 0 < expected['generation'] <= 9007199254740991,
          'invalid_expected_generation')
    _need(expected['environment'] in ('production', 'staging'), 'invalid_expected_environment')
    for field in ('inputManifestSha256', 'pipelineDigest', 'resultManifestSha256'):
        _need(_hash(expected[field]), 'invalid_expected_digest')
    envelope_digests = expected['acceptedEnvelopeSha256']
    _need(type(envelope_digests) is list and len(envelope_digests) == 1
          and _hash(envelope_digests[0]), 'invalid_expected_copy_digest')
    _need(type(accepted_envelopes) is list and len(accepted_envelopes) == 1,
          'invalid_accepted_envelopes')
    try:
        manifest = validate_viewer_result_manifest(result_manifest, job_id=expected['jobId'],
            generation=expected['generation'], environment=expected['environment'],
            pipeline_digest=expected['pipelineDigest'])
    except (TypeError, ValueError, KeyError):
        raise ImportConfirmationRejected('invalid_expected_manifest') from None
    _need(type(manifest['schemaVersion']) is int and manifest['schemaVersion'] == 1
          and set(manifest['engineVersions']) == {'odm'}
          and len(manifest['objects']) == 1
          and manifest['objects'][0]['role'] == 'odm_archive'
          and manifest['objects'][0]['byteSize'] > 0, 'unsupported_result_closure')
    _need(manifest['resultManifestSha256'] == expected['resultManifestSha256'], 'manifest_binding_mismatch')
    obj = manifest['objects'][0]
    envelope = accepted_envelopes[0]
    _need(type(envelope) is dict and set(envelope) == {'acceptance', 'viewerReceipt'}, 'invalid_copy_envelope')
    copy_expected = {field: expected[field] for field in ('jobId', 'generation', 'environment',
        'inputManifestSha256', 'pipelineDigest', 'resultManifestSha256')}
    try:
        acceptance = validate_viewer_acceptance(envelope['acceptance'], result_manifest=manifest,
            input_manifest_sha256=expected['inputManifestSha256'])
        _need(acceptance['acceptedObjectIds'] == [obj['id']], 'incomplete_copy_coverage')
        signed = verify_viewer_acceptance_receipt(envelope['viewerReceipt'],
            trusted_keys=trusted_copy_keys, expected=copy_expected, result_manifest=manifest,
            accepted_object_ids=[obj['id']]).receipt
    except ImportConfirmationRejected:
        raise
    except (TypeError, ValueError, KeyError):
        raise ImportConfirmationRejected('invalid_accepted_copy') from None
    _need(signed['issuerId'] == expected['viewerIssuerId'], 'copy_issuer_mismatch')
    receipt_ids = (acceptance['receiptId'], acceptance['storageReceiptId'],
                   signed['receiptId'], signed['storageReceiptId'])
    _need(len(set(receipt_ids)) == 4, 'copy_identity_collision')
    envelope_sha256 = protocol.digest(envelope)
    _need(envelope_digests == [envelope_sha256], 'copy_digest_binding_mismatch')
    copies = [dict(acceptanceReceiptId=receipt_ids[0], acceptanceStorageReceiptId=receipt_ids[1],
        viewerReceiptId=receipt_ids[2], viewerStorageReceiptId=receipt_ids[3], envelopeSha256=envelope_sha256)]

    _need(type(confirmation) is dict and set(confirmation) == FIELDS, 'invalid_confirmation_fields')
    _need(confirmation['protocol'] == PROTOCOL and type(confirmation['schemaVersion']) is int
          and confirmation['schemaVersion'] == 2, 'unsupported_confirmation_version')
    _need(all(_id(confirmation[field]) for field in ('issuerId', 'keyId', 'confirmationId',
          'modelId', 'modelVersionId', 'outputId')), 'invalid_confirmation_identity')
    _need(confirmation['confirmationId'] not in receipt_ids, 'confirmation_identity_collision')
    _need(confirmation['issuerId'] == expected['viewerIssuerId'], 'confirmation_issuer_mismatch')
    _need(all(confirmation[field] == expected[field] and type(confirmation[field]) is type(expected[field])
              for field in BOUND_FIELDS), 'confirmation_binding_mismatch')
    _need(confirmation['milestone'] == 'ready_for_review', 'invalid_import_milestone')
    _need(confirmation['outputInventoryFormat'] == OUTPUT_INVENTORY_FORMAT
          and _hash(confirmation['outputInventorySha256']), 'invalid_output_inventory')
    archive = confirmation['archive']
    _need(type(archive) is dict and set(archive) == {'objectId', 'byteSize', 'sha256',
          'inventoryFormat', 'inventorySha256'} and _hash(archive['inventorySha256']),
          'invalid_archive_assertion')
    expected_archive = dict(objectId=obj['id'], byteSize=obj['byteSize'], sha256=obj['sha256'],
        inventoryFormat=ARCHIVE_INVENTORY_FORMAT, inventorySha256=archive['inventorySha256'])
    try:
        closure_matches = all(protocol.canonical_json(confirmation[name]) == protocol.canonical_json(value)
            for name, value in (('acceptedCopies', copies), ('objects', manifest['objects']),
                                ('archive', expected_archive)))
    except (TypeError, ValueError, RecursionError):
        raise ImportConfirmationRejected('confirmation_closure_mismatch') from None
    _need(closure_matches, 'confirmation_closure_mismatch')
    ready_at = confirmation['readyAt']
    _need(type(ready_at) is str and _TIME.fullmatch(ready_at) is not None, 'invalid_ready_at')
    try:
        datetime.fromisoformat(ready_at.replace('Z', '+00:00'))
    except ValueError:
        raise ImportConfirmationRejected('invalid_ready_at') from None
    _need(type(trusted_import_keys) is dict and confirmation['keyId'] in trusted_import_keys,
          'untrusted_import_key')
    key_record = trusted_import_keys[confirmation['keyId']]
    _need(type(key_record) is dict and set(key_record) == {'issuerId', 'publicKeyPem'}
          and key_record['issuerId'] == confirmation['issuerId'], 'untrusted_import_issuer')
    try:
        key = _load_key(key_record['publicKeyPem'])
        signature = _canonical_signature(confirmation['signature'])
        body = {key: value for key, value in confirmation.items() if key != 'signature'}
        key.verify(signature, SIGNATURE_DOMAIN + protocol.canonical_json(body))
    except (ViewerReceiptRejected, InvalidSignature, TypeError, ValueError):
        raise ImportConfirmationRejected('invalid_import_signature') from None
    canonical = protocol.canonical_json(confirmation)
    digest = hashlib.sha256(canonical).hexdigest()
    if prior_confirmation_sha256 is not None:
        _need(_hash(prior_confirmation_sha256) and prior_confirmation_sha256 == digest,
              'confirmation_replay_changed')
    return VerifiedImportConfirmation(canonical, digest)
