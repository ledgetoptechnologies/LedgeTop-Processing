"""Verify Viewer-held snapshot evidence; never grants custody or execution."""
import hashlib
import io
import json

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from . import reference_protocol as p
from .viewer_paged_frame_candidate import verify as verify_source, _pairs, MAX_FRAME_BYTES, MAX_TOTAL_BYTES
from .viewer_selected_frames_candidate import DOMAIN as SELECTED_DOMAIN, VERSION as SELECTED_VERSION
from .gcp_evidence_stream_candidate import verify as verify_evidence
from .gcp_file_stream_candidate import verify as verify_file

DOMAIN = b'ltds-viewer-held-source-snapshot-v1\0'
FIELDS = ('protocol', 'snapshotId', 'issuerId', 'keyId', 'createdAt', 'datasetId',
          'datasetManifestSha256', 'taskId', 'attemptId', 'inputManifestSha256',
          'sourceCount', 'datasetFileCount', 'sourcePageHashes', 'sourceFrameBytes',
          'selectedCount', 'selectedPageHashes', 'selectedFrameBytes', 'gcp')


def verify_snapshot(*, intent_bytes, signature, public_key, issuer_id, key_id,
                    dataset_id, task_id, attempt_id, source_frames, selected_frames,
                    evidence_frames, file_frames):
    """Verify configured signer and independent task IDs; result remains held."""
    try:
        if (type(intent_bytes) is not bytes or not 0 < len(intent_bytes) <= 65536
                or type(signature) is not bytes or len(signature) != 64
                or not isinstance(public_key, Ed25519PublicKey)):
            raise ValueError()
        public_key.verify(signature, DOMAIN + intent_bytes)
        intent = json.loads(intent_bytes.decode('utf8'), object_pairs_hook=_pairs,
                            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        p._closed(intent, FIELDS, 'held snapshot')
        if (p.canonical_json(intent) != intent_bytes or intent['protocol'] != 'ltds-viewer-held-source-snapshot-v1'
                or (intent['issuerId'], intent['keyId'], intent['datasetId'], intent['taskId'], intent['attemptId'])
                != (issuer_id, key_id, dataset_id, task_id, attempt_id)):
            raise ValueError()
        p._uuid(intent['snapshotId'], 'snapshot'); p._uuid(task_id, 'task'); p._uuid(attempt_id, 'attempt')
        for value in (issuer_id, key_id, dataset_id): p._slug(value, 'snapshot binding')
        for key in ('datasetManifestSha256', 'inputManifestSha256'): p._hash(intent[key], key)
        for key in ('sourceCount', 'datasetFileCount', 'selectedCount', 'sourceFrameBytes', 'selectedFrameBytes'):
            p._integer(intent[key], key)
        if (type(source_frames) is not bytes or type(selected_frames) is not bytes
                or len(source_frames) != intent['sourceFrameBytes'] or len(selected_frames) != intent['selectedFrameBytes']
                or len(selected_frames) > MAX_TOTAL_BYTES):
            raise ValueError()
        checked, selected = verify_source(io.BytesIO(source_frames), expected_page_hashes=intent['sourcePageHashes'],
                                          expected_items=intent['sourceCount'], _capture_selection=True)
        generated = checked['generatedGcp']
        if intent['sourceCount'] != intent['datasetFileCount'] + generated:
            raise ValueError()
        lines = selected_frames.splitlines(keepends=True)
        hashes = intent['selectedPageHashes']
        if type(hashes) is not list or not 0 < len(hashes) <= 64 or len(lines) != len(hashes): raise ValueError()
        count = 0
        seen = set()
        input_digest = hashlib.sha256(b'[')
        for index, (line, pin) in enumerate(zip(lines, hashes)):
            if not line.endswith(b'\n') or len(line) > MAX_FRAME_BYTES + 1: raise ValueError()
            raw = line[:-1]; frame = json.loads(raw.decode('utf8'), object_pairs_hook=_pairs)
            p._closed(frame, ('version', 'index', 'records'), 'selected frame')
            if (frame['version'] != SELECTED_VERSION or frame['index'] != index or type(frame['index']) is not int
                    or type(frame['records']) is not list or not 0 < len(frame['records']) <= 512
                    or p.canonical_json(frame) != raw or hashlib.sha256(SELECTED_DOMAIN + raw).hexdigest() != pin):
                raise ValueError()
            for record in frame['records']:
                if count >= len(selected): raise ValueError()
                source = selected[count]
                if record.get('type') == 'dataset_file':
                    p._closed(record, ('type', 'ordinal', 'pageObjectId', 'datasetFileId'), 'selected dataset')
                    if source['type'] != 'dataset_file' or record['datasetFileId'] != source['datasetFileId']:
                        raise ValueError()
                elif record.get('type') == 'attempt_generated_gcp':
                    p._closed(record, ('type', 'ordinal', 'pageObjectId', 'attemptId', 'mediaType'), 'selected GCP')
                    if (source['type'] != 'attempt_generated_gcp' or record['attemptId'] != attempt_id
                            or record['attemptId'] != source['attemptId']): raise ValueError()
                    p._string(record['mediaType'], 'generated media type', 127)
                else:
                    raise ValueError()
                p._slug(record['pageObjectId'], 'page object ID')
                if type(record['ordinal']) is not int or record['ordinal'] != count or record['pageObjectId'] in seen:
                    raise ValueError()
                seen.add(record['pageObjectId'])
                entry = {'relativePath': source['relativePath'], 'byteSize': source['byteSize'], 'sha256': source['sha256']}
                if count: input_digest.update(b',')
                input_digest.update(json.dumps(entry, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf8'))
                count += 1
        input_digest.update(b']')
        if (count != intent['selectedCount'] or count != len(selected)
                or input_digest.hexdigest() != intent['inputManifestSha256']): raise ValueError()
        gcp = intent['gcp']
        if generated:
            if type(gcp) is not dict or type(evidence_frames) is not bytes or type(file_frames) is not bytes: raise ValueError()
            p._closed(gcp, ('evidenceVersion', 'evidencePageHashes', 'evidencePageBytes', 'evidencePageCount',
                'evidenceByteCount', 'evidenceSha256', 'fileVersion', 'filePageHashes', 'filePageBytes',
                'filePageCount', 'fileByteCount', 'fileSha256', 'generatedGcp'), 'GCP snapshot')
            if gcp.get('evidenceVersion') != 'ltds-viewer-gcp-evidence-chunks-v1' or gcp.get('fileVersion') != 'ltds-viewer-generated-gcp-file-chunks-v1': raise ValueError()
            if (gcp['evidencePageCount'] != len(gcp['evidencePageHashes'])
                    or gcp['filePageCount'] != len(gcp['filePageHashes'])): raise ValueError()
            records = [record for line in source_frames.splitlines()
                       for record in json.loads(line, object_pairs_hook=_pairs)['records']
                       if record['type'] == 'attempt_generated_gcp']
            if len(records) != 1 or {k:v for k,v in records[0].items() if k != 'type'} != gcp['generatedGcp']:
                raise ValueError()
            if (gcp['generatedGcp']['attemptId'] != attempt_id
                    or gcp['generatedGcp']['sha256'] != gcp['fileSha256']
                    or gcp['generatedGcp']['byteSize'] != gcp['fileByteCount']): raise ValueError()
            verify_evidence(io.BytesIO(evidence_frames), attempt_id=attempt_id, evidence_sha256=gcp['evidenceSha256'],
                page_hashes=gcp['evidencePageHashes'], page_bytes=gcp['evidencePageBytes'], decoded_bytes=gcp['evidenceByteCount'])
            verify_file(io.BytesIO(file_frames), attempt_id=attempt_id, evidence_sha256=gcp['evidenceSha256'],
                file_sha256=gcp['fileSha256'], page_hashes=gcp['filePageHashes'], page_bytes=gcp['filePageBytes'],
                decoded_bytes=gcp['fileByteCount'])
        elif gcp is not None or evidence_frames is not None or file_frames is not None:
            raise ValueError()
        return {'state': 'held_snapshot_not_authorized', 'sourceCount': intent['sourceCount'],
                'datasetFileCount': intent['datasetFileCount'], 'generatedGcp': generated}
    except Exception:
        raise ValueError('held_snapshot_invalid') from None
