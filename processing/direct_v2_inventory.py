"""Validated V2 worker inventory; this alone grants no storage or execution.

Unlike direct worker V1, V2 separately binds the full Viewer source and derived
manifests. Raw retained inputs absent from the engine manifest remain in the
inventory and must receive durable custody before any future acknowledgement.
"""
from copy import deepcopy
import json
from types import MappingProxyType

from . import reference_protocol as protocol
from .reference_source_intake import validate_source_intake


def prepare(payload, *, authenticated_principal, environment, capability_fingerprint,
            options_fingerprint, allowed_odm_options, trusted_source_keys, authority_id,
            available_engines=('odm', 'treedar')):
    """Revalidate the signed V2 request and return a non-authorizing inventory."""
    protocol._slug(authenticated_principal, 'authenticated principal')
    checked = validate_source_intake(payload, environment=environment,
        capability_fingerprint=capability_fingerprint, options_fingerprint=options_fingerprint,
        allowed_odm_options=allowed_odm_options, trusted_source_keys=trusted_source_keys,
        authority_id=authority_id, available_engines=available_engines)
    proof = checked['sourceAttestation']['proof']
    protocol._require(proof['principalId'] == authenticated_principal,
                      'authenticated source principal mismatch')
    # Keep source and derived digests separate from the V1 pipeline-subset
    # digest. A raw GCP CSV is retained here even though it is not engine input.
    result = dict(protocol='ltds-direct-worker-inventory-v2', schemaVersion=2,
        authorityId=authority_id, principalId=authenticated_principal,
        environment=environment, submissionId=checked['submissionId'],
        viewerTaskId=checked['viewerTaskId'], providerTaskId=checked['providerTaskId'],
        sourceManifest=deepcopy(checked['sourceManifest']),
        derivedManifest=deepcopy(checked['derivedManifest']),
        pipelineManifest=deepcopy(checked['manifest']), pipeline=deepcopy(checked['pipeline']),
        capabilityFingerprint=checked['capabilityFingerprint'],
        sourceAttestation=deepcopy(checked['sourceAttestation']),
        sourceManifestSha256=checked['sourceManifestSha256'],
        derivedManifestSha256=checked['derivedManifestSha256'],
        pipelineManifestSha256=checked['manifestSha256'], pipelineDigest=checked['pipelineDigest'],
        attestationSha256=checked['sourceAttestation']['attestationSha256'],
        sourceSnapshotSha256=proof['sourceSnapshotSha256'],
        preparedEnvelopeSha256=proof['preparedEnvelopeSha256'],
        activationId=proof['activationId'], sourceCustodyAccepted=False,
        transferAuthorized=False, processingAuthorized=False)
    # Internal Python-only digest, not a Viewer wire/JCS contract.
    result['inventorySha256'] = protocol.digest(result)
    return result


def verify(value, *, expected_binding, **verification):
    """Reverify the original signed request and exact independently pinned binding.

    The snapshot/envelope digests remain commitments; this does not prove their
    preimages, current worker fencing, file presence, or durable custody.
    """
    binding_fields = ('authorityId', 'principalId', 'environment', 'submissionId',
        'viewerTaskId', 'providerTaskId', 'sourceManifestSha256', 'derivedManifestSha256',
        'pipelineManifestSha256', 'pipelineDigest', 'attestationSha256',
        'sourceSnapshotSha256', 'preparedEnvelopeSha256', 'activationId')
    protocol._require(type(value) is dict and type(expected_binding) is dict
        and set(expected_binding) == set(binding_fields), 'worker inventory binding required')
    protocol._closed(value, ('protocol', 'schemaVersion', 'authorityId', 'principalId', 'environment',
        'submissionId', 'viewerTaskId', 'providerTaskId', 'sourceManifest', 'derivedManifest',
        'pipelineManifest', 'pipeline', 'capabilityFingerprint', 'sourceAttestation',
        'sourceManifestSha256', 'derivedManifestSha256', 'pipelineManifestSha256', 'pipelineDigest',
        'attestationSha256', 'sourceSnapshotSha256', 'preparedEnvelopeSha256', 'activationId',
        'sourceCustodyAccepted', 'transferAuthorized', 'processingAuthorized', 'inventorySha256'),
        'worker inventory')
    for name in binding_fields:
        protocol._require(type(value.get(name)) is type(expected_binding[name])
            and value.get(name) == expected_binding[name], 'worker inventory binding changed')
    request = dict(schemaVersion=2, submissionId=value['submissionId'],
        providerTaskId=value['providerTaskId'], viewerTaskId=value['viewerTaskId'],
        sourceManifest=value['sourceManifest'], derivedManifest=value['derivedManifest'],
        manifest=value['pipelineManifest'], pipeline=value['pipeline'],
        capabilityFingerprint=value['capabilityFingerprint'], sourceAttestation=value['sourceAttestation'])
    checked = prepare(request, **verification)
    protocol._require(protocol.canonical_json(checked) == protocol.canonical_json(value),
                      'worker inventory changed')
    return checked


def worker_object(value, object_id, *, expected_binding, **verification):
    """Return an exact V2 transfer spec, after revalidating independent pins.

    V2 retained objects may be zero bytes. Their SHA-256 is still mandatory;
    this function neither receives bytes nor establishes storage custody.
    """
    checked = verify(value, expected_binding=expected_binding, **verification)
    protocol._slug(object_id, 'source object id')
    found = [(kind, obj) for kind, manifest in (
        ('source', checked['sourceManifest']), ('derived', checked['derivedManifest']))
        for obj in manifest['objects'] if obj['id'] == object_id]
    protocol._require(len(found) == 1, 'worker inventory object not found')
    kind, obj = found[0]
    return dict(protocol='ltds-direct-worker-object-v2', objectKind=kind,
        objectId=obj['id'], byteSize=obj['byteSize'], sha256=obj['sha256'],
        retentionClass=obj['retentionClass'], retentionHoldId=obj['retentionHoldId'],
        sourceCustodyAccepted=False, transferAuthorized=False, processingAuthorized=False)


_CONSTRUCTION_TOKEN = object()


class VerifiedWorkerInventory:
    """Immutable process-local index, never a worker authorization or receipt.

    The trusted caller must check the durable reservation/fence on each use.
    Reverify the signed original with independent current pins before closure.
    """
    __slots__ = ('_wire', '_binding_wire', '_reservation_wire', '_objects')

    def __init__(self, checked, binding, reservation, token=None):
        if token is not _CONSTRUCTION_TOKEN:
            raise ValueError('verified worker inventory factory required')
        object.__setattr__(self, '_wire', protocol.canonical_json(checked))
        object.__setattr__(self, '_binding_wire', protocol.canonical_json(binding))
        object.__setattr__(self, '_reservation_wire', protocol.canonical_json(reservation))
        index = {}
        for kind, manifest in (('source', checked['sourceManifest']), ('derived', checked['derivedManifest'])):
            for obj in manifest['objects']:
                if obj['id'] in index:
                    raise ValueError('worker inventory duplicate object')
                index[obj['id']] = (kind, obj['byteSize'], obj['sha256'],
                                    obj['retentionClass'], obj['retentionHoldId'])
        object.__setattr__(self, '_objects', MappingProxyType(index))

    def __setattr__(self, name, value):
        raise AttributeError('verified worker inventory is immutable')

    def object(self, object_id, *, reservation_binding):
        protocol._require(protocol.canonical_json(reservation_binding) == self._reservation_wire,
                          'worker inventory reservation changed')
        protocol._slug(object_id, 'source object id')
        value = self._objects.get(object_id)
        protocol._require(value is not None, 'worker inventory object not found')
        kind, size, sha256, retention, hold = value
        return dict(protocol='ltds-direct-worker-object-v2', objectKind=kind,
            objectId=object_id, byteSize=size, sha256=sha256, retentionClass=retention,
            retentionHoldId=hold, sourceCustodyAccepted=False, transferAuthorized=False,
            processingAuthorized=False)

    def revalidate_for_closure(self, *, expected_binding, reservation_binding, **verification):
        protocol._require(protocol.canonical_json(expected_binding) == self._binding_wire,
                          'worker inventory reservation changed')
        protocol._require(protocol.canonical_json(reservation_binding) == self._reservation_wire,
                          'worker inventory reservation changed')
        return verify(json.loads(self._wire), expected_binding=expected_binding, **verification)


def prepare_worker_index(value, *, expected_binding, reservation_binding, **verification):
    """Verify once and index exact V2 objects against independently pinned state."""
    checked = verify(value, expected_binding=expected_binding, **verification)
    fields = ('reservationId', 'jobId', 'attemptId', 'generation', 'authorityId',
              'environment', 'nodeId', 'workerId', 'workerInstanceId')
    protocol._closed(reservation_binding, fields, 'worker reservation binding')
    for field in ('reservationId', 'jobId', 'attemptId'):
        protocol._uuid(reservation_binding[field], field)
    protocol._integer(reservation_binding['generation'], 'generation')
    for field in ('nodeId', 'workerId', 'workerInstanceId'):
        protocol._slug(reservation_binding[field], field)
    protocol._require(reservation_binding['jobId'] == checked['providerTaskId']
        and reservation_binding['attemptId'] == checked['submissionId']
        and reservation_binding['authorityId'] == checked['authorityId']
        and reservation_binding['environment'] == checked['environment'],
        'worker reservation identity mismatch')
    return VerifiedWorkerInventory(checked, expected_binding, reservation_binding, _CONSTRUCTION_TOKEN)
