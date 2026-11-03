"""Fail-closed inspection of verification-only v3 copy records; no acceptance."""
import hashlib
import json
from . import reference_protocol as p

DOMAIN=b'ltds-viewer-prepared-envelope-jcs-v3\0'
MAX_SCAN_ROWS=10000
MAX_SCAN_BYTES=64*1024*1024


def held_copy_ids(db, *, exclude_job=None):
    """Validate every retained record before trusting any namespace identity."""
    joins = """ FROM reference_viewer_v3_copy_hold h
        LEFT JOIN reference_jobs r ON r.job_id=h.job_id
        LEFT JOIN reference_viewer_submission_owners o ON o.job_id=h.job_id
        LEFT JOIN reference_viewer_envelopes_v2 e ON e.job_id=h.job_id
        LEFT JOIN reference_viewer_activation a ON a.job_id=h.job_id
        LEFT JOIN reference_viewer_v3_policy v ON v.job_id=h.job_id"""
    # SQLite computes only scalar lengths/types before Python materializes any
    # proof, envelope, activation, input or policy bytes.
    fields=('h.payload_json','e.envelope_json','a.binding_json','r.input_json','v.policy_json',
        'h.job_id','h.envelope_sha256','h.payload_sha256','r.submission_id','r.environment','r.manifest_sha',
        'r.pipeline_digest','r.request_digest','o.protocol','o.principal_id','o.environment','o.request_digest',
        'e.activation_id','e.viewer_task_id','e.viewer_attempt_id','e.envelope_sha256','a.binding_sha256','v.policy_sha256')
    invalid=' OR '.join("typeof(%s)!='text' OR length(CAST(%s AS BLOB)) NOT BETWEEN 1 AND %d"%(f,f,p.MAX_BODY_BYTES) for f in fields) + " OR typeof(r.generation)!='integer'"
    sizes='+'.join('COALESCE(length(CAST(%s AS BLOB)),0)'%f for f in fields)
    count,total,bad=db.execute('SELECT COUNT(*),COALESCE(SUM('+sizes+'),0),COALESCE(SUM(CASE WHEN '+invalid+' THEN 1 ELSE 0 END),0)'+joins).fetchone()
    if count>MAX_SCAN_ROWS or total>MAX_SCAN_BYTES or bad:
        raise ValueError('held copy scan limit exceeded')
    rows=db.execute("""SELECT h.job_id,h.envelope_sha256,h.payload_sha256,h.payload_json,
        r.submission_id,r.generation,r.environment,r.manifest_sha,r.pipeline_digest,r.request_digest,r.input_json,
        o.protocol,o.principal_id,o.environment AS owner_environment,o.request_digest AS owner_digest,
        e.activation_id,e.viewer_task_id,e.viewer_attempt_id,e.envelope_sha256 AS prepared_sha,e.envelope_json,
        a.binding_sha256,a.binding_json,v.policy_json,v.policy_sha256"""+joins)
    result=set()
    for row in rows:
        try:
            import rfc8785
            wire=row['payload_json'];ewire=row['envelope_json'];awire=row['binding_json'];iwire=row['input_json'];pwire=row['policy_json']
            if not all(type(v) is str for v in (wire,ewire,awire,iwire,pwire)): raise ValueError('missing evidence')
            proof=json.loads(wire);envelope=json.loads(ewire);activation=json.loads(awire);normalized=json.loads(iwire);policy=json.loads(pwire)
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
            if (p.canonical_json(normalized).decode()!=iwire or p.canonical_json(policy).decode()!=pwire
                or hashlib.sha256(pwire.encode()).hexdigest()!=row['policy_sha256']
                or normalized['requestDigest']!=row['request_digest'] or row['owner_digest']!=row['request_digest']
                or normalized['pipelineDigest']!=row['pipeline_digest'] or normalized['manifestSha256']!=row['manifest_sha']
                or normalized['environment']!=row['environment'] or p.digest(envelope['submission'])!=row['request_digest']
                or p.digest(envelope['submission']['pipeline'])!=row['pipeline_digest']
                or envelope['submission']!={k:v for k,v in normalized.items() if k not in ('environment','requestDigest','manifestSha256','pipelineDigest')}):
                raise ValueError('stored input or policy changed')
            if (p.canonical_json(activation).decode()!=awire or p.digest(activation)!=row['binding_sha256']
                or (activation['jobId'],activation['generation'],activation['environment'],activation['principalId'],activation['submissionId'],activation['activationId'],activation['viewerTaskId'],activation['viewerAttemptId']) !=
                   (row['job_id'],row['generation'],row['environment'],row['principal_id'],row['submission_id'],row['activation_id'],row['viewer_task_id'],row['viewer_attempt_id'])
                or (activation['manifestSha256'],activation['pipelineDigest'],activation['requestDigest']) != (row['manifest_sha'],row['pipeline_digest'],row['request_digest'])):
                raise ValueError('activation evidence changed')
            for part in (acceptance,signed):
                if (part['jobId'],part['generation'],part['environment'],part['inputManifestSha256'],part['pipelineDigest']) != (row['job_id'],row['generation'],row['environment'],row['manifest_sha'],row['pipeline_digest']):
                    raise ValueError('copy binding changed')
            if row['job_id']!=exclude_job:result.update(ids)
        except (ImportError,ValueError,TypeError,KeyError,json.JSONDecodeError):
            raise ValueError('held copy evidence invalid') from None
    return result
