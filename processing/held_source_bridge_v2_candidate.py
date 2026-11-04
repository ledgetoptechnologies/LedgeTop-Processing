"""Held-only dual-signature inspection. This is not an ingestion authority."""
import base64
import hashlib
import io
import json
import re
from datetime import datetime, timezone

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives import serialization
from . import reference_protocol as p
from .held_viewer_snapshot_verifier_candidate import verify_snapshot
from .paged_snapshot_journal_candidate import FIELDS as V1_FIELDS
from .viewer_paged_frame_candidate import _pairs
from .viewer_selected_frames_candidate import verify as verify_selection, binding_digest

PROTOCOL = 'ltds-viewer-paged-source-snapshot-v2'
DOMAIN = b'LTDS-VIEWER-PAGED-SOURCE-SNAPSHOT-V2\0'
BRIDGE_FIELDS = ('protocol', 'snapshotId', 'issuerId', 'keyId', 'intentSha256', 'intentByteCount',
                 'signatureSha256', 'signatureByteCount', 'sourceFrameStreamSha256', 'sourceFrameStreamByteCount',
                 'selectedFrameStreamSha256', 'selectedFrameStreamByteCount', 'gcpEvidenceStreamSha256',
                 'gcpEvidenceStreamByteCount', 'gcpFileStreamSha256', 'gcpFileStreamByteCount')
CONTEXT_FIELDS = ('issuerId', 'keyId', 'principalId', 'environment', 'coordinatorAuthorityId', 'jobId',
                  'generation', 'submissionId', 'activationId', 'viewerTaskId', 'viewerAttemptId',
                  'preparedEnvelopeSha256', 'datasetId', 'datasetManifestSha256', 'pipelineDigest',
                  'processingManifestSha256', 'processingClosureSha256', 'revocationEpoch', 'localIssuerId', 'localKeyId')


def verify(*, statement_bytes, outer_key, local_key, context, now_ms, intent_bytes, local_signature,
           source_frames, selected_frames, evidence_frames, file_frames, processing_closure, processing_pages):
    """Inspect exact bytes against independently supplied keys/context; always held.

    Callers must provision keys, context and time from trusted server state. This
    candidate does not stage bytes, persist replay state or authorize custody.
    """
    try:
        p._closed(context, CONTEXT_FIELDS, 'bridge context')
        if (not isinstance(outer_key, Ed25519PublicKey) or not isinstance(local_key, Ed25519PublicKey)
                or type(now_ms) is not int or type(statement_bytes) is not bytes
                or not 0 < len(statement_bytes) <= 65536):
            raise ValueError()
        if outer_key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw) == local_key.public_bytes(
                serialization.Encoding.Raw, serialization.PublicFormat.Raw):
            raise ValueError()
        statement = json.loads(statement_bytes.decode('utf8'), object_pairs_hook=_pairs,
                               parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        p._closed(statement, (*V1_FIELDS, 'processingManifestSha256', 'viewerHeldSource'), 'bridge statement')
        if (p.canonical_json(statement) != statement_bytes or statement['protocol'] != PROTOCOL
                or type(statement['schemaVersion']) is not int or statement['schemaVersion'] != 2):
            raise ValueError()
        sig = statement['signature']
        if type(sig) is not str or len(sig) != 86 or not re.fullmatch(r'[A-Za-z0-9_-]{86}', sig):
            raise ValueError()
        decoded = base64.urlsafe_b64decode(sig + '==')
        if len(decoded) != 64 or base64.urlsafe_b64encode(decoded).decode().rstrip('=') != sig:
            raise ValueError()
        outer_key.verify(decoded, DOMAIN + p.canonical_json({k: v for k, v in statement.items() if k != 'signature'}))
        for key in CONTEXT_FIELDS:
            if key not in ('revocationEpoch', 'localIssuerId', 'localKeyId') and statement[key] != context[key]:
                raise ValueError()
        p._environment(context['environment']); p._uuid(context['coordinatorAuthorityId'], 'authority')
        for key in ('submissionId', 'activationId', 'viewerTaskId', 'viewerAttemptId'):
            p._uuid(context[key], key)
        for key in ('issuerId', 'keyId', 'principalId', 'jobId', 'datasetId', 'localIssuerId', 'localKeyId'):
            p._slug(context[key], key)
        for key in ('preparedEnvelopeSha256', 'datasetManifestSha256', 'pipelineDigest', 'processingManifestSha256', 'processingClosureSha256'):
            p._hash(context[key], key)
        p._integer(context['generation'], 'generation'); p._integer(context['revocationEpoch'], 'epoch', allow_zero=True)
        times = []
        for key in ('issuedAt', 'expiresAt'):
            value = statement[key]
            if type(value) is not str or not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z', value):
                raise ValueError()
            times.append(datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc))
        issued, expires = (int(t.timestamp() * 1000) for t in times)
        if not (0 < expires - issued <= 900000 and issued <= now_ms + 60000 and now_ms < expires):
            raise ValueError()
        bridge = statement['viewerHeldSource']; p._closed(bridge, BRIDGE_FIELDS, 'held bridge')
        if (bridge['protocol'] != 'ltds-viewer-held-source-snapshot-v1'
                or (bridge['issuerId'], bridge['keyId']) != (context['localIssuerId'], context['localKeyId'])):
            raise ValueError()
        parts = (('intent', intent_bytes), ('signature', local_signature), ('sourceFrameStream', source_frames),
                 ('selectedFrameStream', selected_frames), ('gcpEvidenceStream', evidence_frames), ('gcpFileStream', file_frames))
        for prefix, payload in parts:
            digest, size = bridge[prefix + 'Sha256'], bridge[prefix + 'ByteCount']
            if payload is None:
                if digest is not None or size is not None or not prefix.startswith('gcp'):
                    raise ValueError()
            elif (type(payload) is not bytes or type(size) is not int or size != len(payload) or size <= 0
                  or hashlib.sha256(payload).hexdigest() != digest):
                raise ValueError()
        if len(local_signature) != 64 or (evidence_frames is None) != (file_frames is None):
            raise ValueError()
        verify_snapshot(intent_bytes=intent_bytes, signature=local_signature, public_key=local_key,
            issuer_id=context['localIssuerId'], key_id=context['localKeyId'], dataset_id=context['datasetId'],
            task_id=context['viewerTaskId'], attempt_id=context['viewerAttemptId'], source_frames=source_frames,
            selected_frames=selected_frames, evidence_frames=evidence_frames, file_frames=file_frames)
        local = json.loads(intent_bytes.decode('utf8'), object_pairs_hook=_pairs)
        for outer, inner in (('snapshotId','snapshotId'), ('datasetId','datasetId'),
                             ('datasetManifestSha256','datasetManifestSha256'), ('viewerTaskId','taskId'),
                             ('viewerAttemptId','attemptId'), ('sourcePageHashes','sourcePageHashes'),
                             ('selectedPageHashes','selectedPageHashes'), ('sourceCount','sourceCount'),
                             ('selectedCount','selectedCount'), ('inputManifestSha256','inputManifestSha256')):
            if statement[outer] != local[inner]:
                raise ValueError()
        if bridge['snapshotId'] != local['snapshotId']:
            raise ValueError()
        for prefix, wire in (('source', source_frames), ('selected', selected_frames)):
            lines = wire.splitlines(keepends=True)
            if statement[prefix + 'PageBytes'] != [len(line) - 1 for line in lines]:
                raise ValueError()
        gcp = local['gcp']
        gcp_keys = ('gcpEvidenceVersion', 'gcpEvidencePageHashes', 'gcpEvidencePageBytes',
                    'gcpEvidencePageCount', 'gcpEvidenceByteCount', 'gcpEvidenceSha256')
        expected_gcp = ((gcp['evidenceVersion'], gcp['evidencePageHashes'], gcp['evidencePageBytes'],
                         gcp['evidencePageCount'], gcp['evidenceByteCount'], gcp['evidenceSha256'])
                        if gcp is not None else (None,) * len(gcp_keys))
        if tuple(statement[key] for key in gcp_keys) != expected_gcp:
            raise ValueError()
        binding = dict(datasetId=context['datasetId'], datasetManifestSha256=context['datasetManifestSha256'],
            principalId=context['principalId'], environment=context['environment'], authorityId=context['coordinatorAuthorityId'],
            viewerTaskId=context['viewerTaskId'], viewerAttemptId=context['viewerAttemptId'], pipelineDigest=context['pipelineDigest'],
            revocationEpoch=context['revocationEpoch'], sourcePageHashes=statement['sourcePageHashes'], sourceCount=statement['sourceCount'],
            selectedPageHashes=statement['selectedPageHashes'], selectedCount=statement['selectedCount'],
            inputManifestSha256=statement['inputManifestSha256'], processingClosureSha256=context['processingClosureSha256'])
        verify_selection(io.BytesIO(source_frames), io.BytesIO(selected_frames), source_hashes=statement['sourcePageHashes'],
            source_count=statement['sourceCount'], selected_hashes=statement['selectedPageHashes'],
            input_manifest_sha256=statement['inputManifestSha256'], processing_closure=processing_closure,
            processing_pages=processing_pages, closure_sha256=context['processingClosureSha256'],
            trusted_binding=binding, expected_binding_sha256=binding_digest(binding))
        return {'state': 'held_bridge_not_authorized'}
    except Exception:
        raise ValueError('held_bridge_invalid') from None
