"""Inactive candidate verification for a Viewer import confirmation.

This module is not wired to any API or cleanup path. Verification alone does
not authorize deletion. Callers must derive ``expected`` and accepted-copy
envelopes from authenticated, durable state, and atomically persist the first
verified digest before treating subsequent calls as replays. The wire contract
is provisional until paired with the Viewer owner.
"""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
import re

from cryptography.exceptions import InvalidSignature

from . import reference_protocol as protocol
from .viewer_acceptance_receipt import (
    ViewerReceiptRejected, _canonical_signature, _load_key,
    verify_viewer_acceptance_receipt,
)
from .viewer_result_manifest import validate_viewer_acceptance, validate_viewer_result_manifest


PROTOCOL = 'ltds-viewer-import-confirmation-v1'
SIGNATURE_DOMAIN = b'LTDS-VIEWER-IMPORT-CONFIRMATION-V1\0'
IDENTITY_FIELDS = ('jobId', 'generation', 'environment', 'inputManifestSha256',
                   'pipelineDigest', 'resultManifestSha256', 'viewerTaskId',
                   'viewerAttemptId', 'modelId', 'modelVersionId', 'outputId',
                   'outputManifestSha256')
FIELDS = frozenset(('protocol', 'schemaVersion', 'issuerId', 'keyId',
                    'confirmationId', *IDENTITY_FIELDS, 'acceptedCopies',
                    'objects', 'archive', 'milestone', 'readyAt', 'signature'))
EXPECTED_FIELDS = frozenset((*IDENTITY_FIELDS, 'viewerIssuerId', 'acceptedEnvelopeSha256',
                             'archiveInventorySha256'))
_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z')
_HASH = re.compile(r'[a-f0-9]{64}\Z')
_TIME = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z\Z')


class ImportConfirmationRejected(ValueError):
    """A confirmation was malformed, untrusted, incomplete, or changed."""


@dataclass(frozen=True)
class VerifiedImportConfirmation:
    confirmation: dict
    sha256: str


def _need(condition, code):
    if not condition:
        raise ImportConfirmationRejected(code)


def _id(value):
    return type(value) is str and _ID.fullmatch(value) is not None


def _hash(value):
    return type(value) is str and _HASH.fullmatch(value) is not None


def verify_import_confirmation(confirmation, *, trusted_import_keys,
                               trusted_copy_keys, expected, result_manifest,
                               accepted_envelopes, prior_confirmation_sha256=None):
    """Validate exact signed bindings; never performs an import or deletion.

    ``expected`` includes Viewer identities and archive evidence from a trusted
    pre-established mapping, not values taken from this confirmation. Accepted
    envelopes are authenticated durable accepted-copy records. On replay the
    caller must supply the first atomically persisted confirmation digest as
    ``prior_confirmation_sha256``; omitting it is valid only for initial receipt.
    """
    _need(type(expected) is dict and set(expected) == EXPECTED_FIELDS, 'invalid_expected_binding')
    for field in ('jobId', 'viewerIssuerId', 'viewerTaskId', 'viewerAttemptId', 'modelId', 'modelVersionId', 'outputId'):
        _need(_id(expected[field]), 'invalid_expected_identity')
    _need(type(expected['generation']) is int and 0 < expected['generation'] <= 9007199254740991,
          'invalid_expected_generation')
    _need(expected['environment'] in ('production', 'staging'), 'invalid_expected_environment')
    for field in ('inputManifestSha256', 'pipelineDigest', 'resultManifestSha256', 'outputManifestSha256'):
        _need(_hash(expected[field]), 'invalid_expected_digest')
    envelope_digests = expected['acceptedEnvelopeSha256']
    _need(type(envelope_digests) is list and 0 < len(envelope_digests) <= protocol.MAX_OBJECTS
          and all(_hash(item) for item in envelope_digests)
          and envelope_digests == sorted(set(envelope_digests)), 'invalid_expected_copy_digests')
    _need(type(accepted_envelopes) is list and len(accepted_envelopes) == len(envelope_digests),
          'invalid_accepted_envelopes')
    try:
        manifest = validate_viewer_result_manifest(result_manifest, job_id=expected['jobId'],
            generation=expected['generation'], environment=expected['environment'],
            pipeline_digest=expected['pipelineDigest'])
    except (TypeError, ValueError, KeyError):
        raise ImportConfirmationRejected('invalid_expected_manifest') from None
    _need(manifest['resultManifestSha256'] == expected['resultManifestSha256'], 'manifest_binding_mismatch')
    objects = manifest['objects']
    _need(objects, 'empty_object_closure')
    copy_expected = {field: expected[field] for field in ('jobId', 'generation', 'environment',
        'inputManifestSha256', 'pipelineDigest', 'resultManifestSha256')}
    copies, covered, identities = [], set(), set()
    for envelope in accepted_envelopes:
        _need(type(envelope) is dict and set(envelope) == {'acceptance', 'viewerReceipt'}, 'invalid_copy_envelope')
        try:
            acceptance = validate_viewer_acceptance(envelope['acceptance'], result_manifest=manifest,
                input_manifest_sha256=expected['inputManifestSha256'])
            ids = acceptance['acceptedObjectIds']
            _need(ids and not (covered & set(ids)), 'duplicate_or_empty_copy_coverage')
            signed = verify_viewer_acceptance_receipt(envelope['viewerReceipt'],
                trusted_keys=trusted_copy_keys, expected=copy_expected, result_manifest=manifest,
                accepted_object_ids=ids).receipt
        except ImportConfirmationRejected:
            raise
        except (TypeError, ValueError, KeyError):
            raise ImportConfirmationRejected('invalid_accepted_copy') from None
        receipt_ids = (acceptance['receiptId'], acceptance['storageReceiptId'],
                       signed['receiptId'], signed['storageReceiptId'])
        _need(signed['issuerId'] == expected['viewerIssuerId'], 'copy_issuer_mismatch')
        _need(len(set(receipt_ids)) == 4 and not (identities & set(receipt_ids)), 'copy_identity_collision')
        identities.update(receipt_ids)
        covered.update(ids)
        copies.append({'acceptanceReceiptId': receipt_ids[0], 'acceptanceStorageReceiptId': receipt_ids[1],
                       'viewerReceiptId': receipt_ids[2], 'viewerStorageReceiptId': receipt_ids[3],
                       'envelopeSha256': protocol.digest(envelope)})
    _need(covered == {item['id'] for item in objects}, 'incomplete_copy_coverage')
    copies.sort(key=lambda item: item['envelopeSha256'])
    _need([item['envelopeSha256'] for item in copies] == envelope_digests, 'copy_digest_binding_mismatch')

    odm = 'odm' in manifest['engineVersions']
    archives = [item for item in objects if item['role'] == 'odm_archive']
    if odm:
        _need(len(archives) == 1 and archives[0]['byteSize'] > 0
              and _hash(expected['archiveInventorySha256']), 'invalid_archive_binding')
        item = archives[0]
        archive = {'objectId': item['id'], 'byteSize': item['byteSize'], 'sha256': item['sha256'],
                   'inventorySha256': expected['archiveInventorySha256']}
    else:
        _need(not archives and expected['archiveInventorySha256'] is None, 'invalid_archive_binding')
        archive = None

    _need(type(confirmation) is dict and set(confirmation) == FIELDS, 'invalid_confirmation_fields')
    _need(confirmation['protocol'] == PROTOCOL and type(confirmation['schemaVersion']) is int
          and confirmation['schemaVersion'] == 1, 'unsupported_confirmation_version')
    _need(all(_id(confirmation[field]) for field in ('issuerId', 'keyId', 'confirmationId')),
          'invalid_confirmation_identity')
    _need(confirmation['issuerId'] == expected['viewerIssuerId'], 'confirmation_issuer_mismatch')
    _need(all(confirmation[field] == expected[field] and type(confirmation[field]) is type(expected[field])
              for field in IDENTITY_FIELDS), 'confirmation_binding_mismatch')
    _need(confirmation['milestone'] == 'ready_for_review', 'invalid_import_milestone')
    ready_at = confirmation['readyAt']
    _need(type(ready_at) is str and _TIME.fullmatch(ready_at) is not None, 'invalid_ready_at')
    try:
        datetime.fromisoformat(ready_at.replace('Z', '+00:00'))
    except ValueError:
        raise ImportConfirmationRejected('invalid_ready_at') from None
    try:
        closure_matches = all(protocol.canonical_json(confirmation[name]) == protocol.canonical_json(value)
            for name, value in (('acceptedCopies', copies), ('objects', objects), ('archive', archive)))
    except (TypeError, ValueError, RecursionError):
        raise ImportConfirmationRejected('confirmation_closure_mismatch') from None
    _need(closure_matches, 'confirmation_closure_mismatch')
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
    digest = protocol.digest(confirmation)
    if prior_confirmation_sha256 is not None:
        _need(_hash(prior_confirmation_sha256) and prior_confirmation_sha256 == digest,
              'confirmation_replay_changed')
    return VerifiedImportConfirmation(deepcopy(confirmation), digest)
