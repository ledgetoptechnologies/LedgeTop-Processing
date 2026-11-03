"""Fail-closed inspection of verification-only v3 copy records; no acceptance."""
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from . import reference_protocol as p

DOMAIN=b'ltds-viewer-prepared-envelope-jcs-v3\0'
MAX_SCAN_ROWS=10000
MAX_SCAN_BYTES=64*1024*1024
MAX_SQLITE_LENGTH=64*1024*1024


@contextmanager
def bounded_sqlite(db):
    if not db.in_transaction: raise ValueError('held copy scan requires transaction')
    if not hasattr(db, 'setlimit') or not hasattr(db, 'getlimit'):
        raise ValueError('SQLite length limit unavailable')
    old=db.getlimit(sqlite3.SQLITE_LIMIT_LENGTH)
    db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,min(old,MAX_SQLITE_LENGTH))
    try: yield
    except sqlite3.DataError: raise ValueError('SQLite evidence length exceeded') from None
    finally: db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,old)


def bounded_json(text):
    from .viewer_import_confirmation_store import _preflight_stored_json
    if type(text) is not str: raise ValueError('invalid JSON text')
    data=text.encode('utf-8')
    _preflight_stored_json(data)
    try: return json.loads(text)
    except (ValueError,TypeError,RecursionError): raise ValueError('invalid bounded JSON') from None


def held_copy_ids(db, *, exclude_job=None):
    with bounded_sqlite(db):
        return _held_copy_ids_bounded(db,exclude_job=exclude_job)


def _held_copy_ids_bounded(db, *, exclude_job=None):
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
    # First bound row count and character lengths. Only after this succeeds
    # may SQLite cast any TEXT to BLOB to measure UTF-8 bytes.
    count=db.execute('SELECT COUNT(*) FROM (SELECT 1 '+joins+' LIMIT ?)',(MAX_SCAN_ROWS+1,)).fetchone()[0]
    if count>MAX_SCAN_ROWS: raise ValueError('held copy scan limit exceeded')
    invalid=' OR '.join("typeof(%s)!='text' OR length(%s) NOT BETWEEN 1 AND %d"%(f,f,p.MAX_BODY_BYTES) for f in fields) + " OR typeof(r.generation)!='integer'"
    if db.execute('SELECT 1 '+joins+' WHERE '+invalid+' LIMIT 1').fetchone():
        raise ValueError('held copy scan field too large')
    lengths=','.join('length(CAST(%s AS BLOB))'%f for f in fields)
    total=0
    for sizes in db.execute('SELECT '+lengths+joins):
        if any(n is None or n>p.MAX_BODY_BYTES for n in sizes): raise ValueError('held copy scan field too large')
        total+=sum(sizes)
        if total>MAX_SCAN_BYTES: raise ValueError('held copy scan limit exceeded')
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
            proof=bounded_json(wire);envelope=bounded_json(ewire);activation=bounded_json(awire);normalized=bounded_json(iwire);policy=bounded_json(pwire)
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


def legacy_reserved_ids(db):
    """Bound and stream same-database legacy ID namespaces under one transaction."""
    specs=(('reference_acceptances',('receipt_id','receipt_json')),
        ('reference_viewer_receipts',('receipt_id','storage_receipt_id')),
        ('reference_terminal_acceptances',('receipt_id','storage_receipt_id')),
        ('viewer_import_confirmations_candidate_v1',('confirmation_id',)),
        ('viewer_odm_import_confirmations_candidate_v2',('confirmation_id',)))
    result=set();remaining_rows=MAX_SCAN_ROWS;remaining_bytes=MAX_SCAN_BYTES
    with bounded_sqlite(db):
        for table,fields in specs:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone(): continue
            count=db.execute('SELECT COUNT(*) FROM (SELECT 1 FROM '+table+' LIMIT ?)',(remaining_rows+1,)).fetchone()[0]
            if count>remaining_rows: raise ValueError('legacy namespace row limit')
            remaining_rows-=count
            invalid=' OR '.join("typeof(%s)!='text' OR length(%s) NOT BETWEEN 1 AND %d"%(f,f,p.MAX_BODY_BYTES) for f in fields)
            if db.execute('SELECT 1 FROM '+table+' WHERE '+invalid+' LIMIT 1').fetchone(): raise ValueError('legacy namespace field too large')
            lengths=','.join('length(CAST(%s AS BLOB))'%f for f in fields)
            for sizes in db.execute('SELECT '+lengths+' FROM '+table):
                if any(n is None or n>p.MAX_BODY_BYTES for n in sizes): raise ValueError('legacy namespace field too large')
                remaining_bytes-=sum(sizes)
                if remaining_bytes<0: raise ValueError('legacy namespace byte limit')
            for row in db.execute('SELECT '+','.join(fields)+' FROM '+table):
                for index,value in enumerate(row):
                    if fields[index]=='receipt_json':
                        obj=bounded_json(value)
                        for key in ('receiptId','storageReceiptId'):
                            p._slug(obj[key],'legacy receipt ID');result.add(obj[key])
                    else:
                        p._slug(value,'legacy receipt ID');result.add(value)
    return result
