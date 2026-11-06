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
        sourceManifestSha256=checked['sourceManifestSha256'],
        derivedManifestSha256=checked['derivedManifestSha256'],
        pipelineManifestSha256=checked['manifestSha256'], pipelineDigest=checked['pipelineDigest'],
        attestationSha256=checked['sourceAttestation']['attestationSha256'],
        sourceSnapshotSha256=proof['sourceSnapshotSha256'],
        preparedEnvelopeSha256=proof['preparedEnvelopeSha256'],
        activationId=proof['activationId'], sourceCustodyAccepted=False,
        transferAuthorized=False, processingAuthorized=False)
    result['inventorySha256'] = protocol.digest(result)
    return result
