"""Validated V2 worker inventory; this alone grants no storage or execution.

Unlike direct worker V1, V2 separately binds the full Viewer source and derived
manifests. Raw retained inputs absent from the engine manifest remain in the
inventory and must receive durable custody before any future acknowledgement.
"""
from copy import deepcopy

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
