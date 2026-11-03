"""Fail-closed inspection of verification-only v3 copy records; no acceptance."""
import hashlib
import json
from . import reference_protocol as p

DOMAIN=b'ltds-viewer-prepared-envelope-jcs-v3\0'


def held_copy_ids(db, *, exclude_job=None):
    """Validate every retained record before trusting any namespace identity."""
    count,total = db.execute("SELECT COUNT(*),COALESCE(SUM(length(CAST(payload_json AS BLOB))),0) FROM reference_viewer_v3_copy_hold").fetchone()
    if count > 10000 or total > 64*1024*1024:
        raise ValueError('held copy scan limit exceeded')
    rows=db.execute('''SELECT h.job_id,h.envelope_sha256,h.payload_sha256,
        CASE WHEN typeof(h.payload_json)='text' AND length(CAST(h.payload_json AS BLOB))<=? THEN h.payload_json END AS payload_json,
        r.submission_id,r.generation,r.environment,r.manifest_sha,r.pipeline_digest,
        o.protocol,o.principal_id,o.environment AS owner_environment,
        e.activation_id,e.viewer_task_id,e.viewer_attempt_id,e.envelope_sha256 AS prepared_sha,
        CASE WHEN typeof(e.envelope_json)='text' AND length(CAST(e.envelope_json AS BLOB))<=? THEN e.envelope_json END AS envelope_json,
        a.binding_sha256,a.binding_json
        FROM reference_viewer_v3_copy_hold h
        LEFT JOIN reference_jobs r ON r.job_id=h.job_id
        LEFT JOIN reference_viewer_submission_owners o ON o.job_id=h.job_id
        LEFT JOIN reference_viewer_envelopes_v2 e ON e.job_id=h.job_id
        LEFT JOIN reference_viewer_activation a ON a.job_id=h.job_id''',(p.MAX_BODY_BYTES,p.MAX_BODY_BYTES)).fetchall()
    result=set()
    for row in rows:
        try:
            import rfc8785
            wire=row['payload_json'];ewire=row['envelope_json'];awire=row['binding_json']
            if not all(type(v) is str for v in (wire,ewire,awire)): raise ValueError('missing evidence')
            proof=json.loads(wire);envelope=json.loads(ewire);activation=json.loads(awire)
            p._closed(proof,('acceptance','viewerReceipt'),'held proof')
            acceptance,signed=proof['acceptance'],proof['viewerReceipt']
            ids=[part[key] for part in (acceptance,signed) for key in ('receiptId','storageReceiptId')]
            for value in ids:p._slug(value,'held receipt ID')
            if len(set(ids))!=4: raise ValueError('duplicate IDs')
            if p.canonical_json(proof).decode()!=wire or hashlib.sha256(wire.encode()).hexdigest()!=row['payload_sha256']:
                raise ValueError('held proof changed')
            if (rfc8785.dumps(envelope).decode()!=ewire or hashlib.sha256(DOMAIN+ewire.encode()).hexdigest()!=row['prepared_sha']
                or row['envelope_sha256']!=row['prepared_sha'] or row['protocol']!='ltds-viewer-prepared-envelope-jcs-v3'
                or row['owner_environment']!=row['environment'] or envelope['submission']['submissionId']!=row['submission_id']
                or (envelope['activationId'],envelope['viewerTaskId'],envelope['viewerAttemptId']) != (row['activation_id'],row['viewer_task_id'],row['viewer_attempt_id'])):
                raise ValueError('prepared evidence changed')
            if (p.canonical_json(activation).decode()!=awire or p.digest(activation)!=row['binding_sha256']
                or (activation['jobId'],activation['generation'],activation['environment'],activation['principalId'],activation['submissionId'],activation['activationId'],activation['viewerTaskId'],activation['viewerAttemptId']) !=
                   (row['job_id'],row['generation'],row['environment'],row['principal_id'],row['submission_id'],row['activation_id'],row['viewer_task_id'],row['viewer_attempt_id'])):
                raise ValueError('activation evidence changed')
            for part in (acceptance,signed):
                if (part['jobId'],part['generation'],part['environment'],part['inputManifestSha256'],part['pipelineDigest']) != (row['job_id'],row['generation'],row['environment'],row['manifest_sha'],row['pipeline_digest']):
                    raise ValueError('copy binding changed')
            if row['job_id']!=exclude_job:result.update(ids)
        except (ImportError,ValueError,TypeError,KeyError,json.JSONDecodeError):
            raise ValueError('held copy evidence invalid') from None
    return result
