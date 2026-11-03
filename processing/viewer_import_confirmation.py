"""Inactive candidate verification for a Viewer import confirmation.

This module is not wired to any API or cleanup path. Verification alone does
not authorize deletion. Callers must derive ``expected`` and accepted-copy
envelopes from authenticated, durable state, and atomically persist the first
verified digest before treating subsequent calls as replays. The wire contract
is provisional until paired with the Viewer owner.
"""
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
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
# Shared pre-serialization limits for *all* arguments, including key registries.
MAX_INPUT_BYTES = 16 * 1024 * 1024
MAX_INPUT_NODES = 500_000
MAX_INPUT_DEPTH = 12
MAX_STRING_BYTES = 8192
MAX_CONTAINER_ITEMS = 10_000
MAX_MAP_ITEMS = 64
MAX_SAFE_INTEGER = 9_007_199_254_740_991


class ImportConfirmationRejected(ValueError):
    """A confirmation was malformed, untrusted, incomplete, or changed."""


@dataclass(frozen=True)
class VerifiedImportConfirmation:
    canonical_bytes: bytes
    sha256: str

    def __post_init__(self):
        if (type(self.canonical_bytes) is not bytes or len(self.canonical_bytes) > MAX_INPUT_BYTES
                or type(self.sha256) is not str
                or _HASH.fullmatch(self.sha256) is None
                or hashlib.sha256(self.canonical_bytes).hexdigest() != self.sha256):
            raise ValueError('invalid immutable confirmation snapshot')

    @property
    def confirmation(self):
        """Return a fresh detached copy; retained proof bytes stay immutable."""
        return json.loads(self.canonical_bytes)


def _need(condition, code):
    if not condition:
        raise ImportConfirmationRejected(code)


def _id(value):
    return type(value) is str and _ID.fullmatch(value) is not None


def _hash(value):
    return type(value) is str and _HASH.fullmatch(value) is not None


def _snapshot_inputs(*values):
    """Bound and detach JSON trees before hashing or inspecting bindings.

    Exact builtin types only; no hooks from mapping/list/string subclasses.
    Charging six bytes per input byte bounds JSON string escaping as well as
    UTF-8. Depth limits also stop cycles without recursive traversal.
    """
    nodes = 0
    charged = 0

    def charge_string(value, maximum):
        nonlocal charged
        _need(len(value) <= maximum, 'input_string_too_large')
        try:
            length = len(value.encode('utf-8'))
        except UnicodeError:
            raise ImportConfirmationRejected('invalid_input_utf8') from None
        _need(length <= maximum, 'input_string_too_large')
        charged += length * 6 + 16

    def clone(value, depth):
        nonlocal nodes, charged
        nodes += 1
        _need(nodes <= MAX_INPUT_NODES and depth <= MAX_INPUT_DEPTH, 'input_structure_too_large')
        charged += 16
        kind = type(value)
        if kind is str:
            charge_string(value, MAX_STRING_BYTES)
            result = value
        elif kind is int:
            _need(-MAX_SAFE_INTEGER <= value <= MAX_SAFE_INTEGER, 'invalid_input_integer')
            charged += 24
            result = value
        elif kind in (type(None), bool):
            result = value
        elif kind is list:
            _need(len(value) <= MAX_CONTAINER_ITEMS, 'input_container_too_large')
            result = []
            for item in value:
                _need(len(result) < MAX_CONTAINER_ITEMS, 'input_container_too_large')
                result.append(clone(item, depth + 1))
        elif kind is dict:
            _need(len(value) <= MAX_MAP_ITEMS, 'input_container_too_large')
            result = {}
            try:
                for key, item in value.items():
                    _need(len(result) < MAX_MAP_ITEMS, 'input_container_too_large')
                    _need(type(key) is str, 'invalid_input_type')
                    charge_string(key, 128)
                    _need(charged <= MAX_INPUT_BYTES, 'input_bytes_too_large')
                    result[key] = clone(item, depth + 1)
            except RuntimeError:
                raise ImportConfirmationRejected('input_changed_during_snapshot') from None
        else:
            raise ImportConfirmationRejected('invalid_input_type')
        _need(charged <= MAX_INPUT_BYTES, 'input_bytes_too_large')
        return result

    return tuple(clone(value, 0) for value in values)


def verify_import_confirmation(confirmation, *, trusted_import_keys,
                               trusted_copy_keys, expected, result_manifest,
                               accepted_envelopes, prior_confirmation_sha256=None):
    """Validate exact signed bindings; never performs an import or deletion.

    ``expected`` includes Viewer identities and archive evidence from a trusted
    pre-established mapping, not values taken from this confirmation. Accepted
    envelopes are authenticated durable accepted-copy records. On replay the
    caller must supply the first atomically persisted confirmation digest as
    ``prior_confirmation_sha256``; omitting it is valid only for initial receipt.

    At a trust boundary, invoke this function with the decoded confirmation and
    independent trusted bindings. Never accept a caller-supplied
    ``VerifiedImportConfirmation`` instance as proof: that type is publicly
    constructible and its constructor does not verify a signature. A future
    request handler must enforce a raw byte limit before JSON parsing; this
    function's bounded object snapshot only applies after parsing.
    """
    (confirmation, trusted_import_keys, trusted_copy_keys, expected, result_manifest,
     accepted_envelopes, prior_confirmation_sha256) = _snapshot_inputs(
        confirmation, trusted_import_keys, trusted_copy_keys, expected,
        result_manifest, accepted_envelopes, prior_confirmation_sha256)
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
    canonical = protocol.canonical_json(confirmation)
    digest = hashlib.sha256(canonical).hexdigest()
    if prior_confirmation_sha256 is not None:
        _need(_hash(prior_confirmation_sha256) and prior_confirmation_sha256 == digest,
              'confirmation_replay_changed')
    return VerifiedImportConfirmation(canonical, digest)
