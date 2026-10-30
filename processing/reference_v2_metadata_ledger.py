"""Disabled, metadata-only v2 coordinator ledger; no execution or transport.

Authenticated caller identity is supplied by a future transport. It does not
share a scheduler execution slot and must never be used to start a worker.
"""
from contextlib import nullcontext
from datetime import datetime
import hashlib
import hmac
import json
import math
import secrets
import uuid

from . import reference_protocol as p
from .admission_drain import AdmissionClosed
from .maintenance import Conflict
from .reference_manifest_v2 import validate_manifest_v2
from .reference_input_budget_v2 import decode_canonical_input_v2, plan_input_budget


_DDL = '''CREATE TABLE reference_v2_staged_jobs (
                job_id TEXT PRIMARY KEY, submission_id TEXT UNIQUE NOT NULL, principal TEXT NOT NULL,
                environment TEXT NOT NULL, request_json TEXT NOT NULL, request_sha TEXT NOT NULL,
                claim_json TEXT, claim_sha TEXT, reservation_json TEXT, reservation_sha TEXT, plan_json TEXT, plan_sha TEXT)'''


class V2MetadataLedger:
    def __init__(self, queue, *, enabled=False, max_metadata_bytes=64*1024*1024):
        if enabled is not True:
            raise Conflict('v2 metadata ledger disabled')
        if type(max_metadata_bytes) is not int or not 1 <= max_metadata_bytes <= 1024*1024*1024:
            raise ValueError('bounded metadata quota required')
        self.queue, self.max_metadata_bytes = queue, max_metadata_bytes
        with queue._db() as db:
            found = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_v2_staged_jobs'").fetchone()
            if found is None: db.execute(_DDL)
            elif found[0] != _DDL: raise Conflict('v2 schema unavailable')
            self._check_schema(db)

    @staticmethod
    def _check_schema(db):
        found=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_v2_staged_jobs'").fetchone()
        extras=db.execute("SELECT 1 FROM sqlite_master WHERE tbl_name='reference_v2_staged_jobs' AND (type='trigger' OR (type='index' AND sql IS NOT NULL)) LIMIT 1").fetchone()
        if found is None or found[0]!=_DDL or extras is not None: raise Conflict('v2 schema unavailable')

    @staticmethod
    def _fetch(db, field, value, principal=None):
        if field not in ('job_id','submission_id'): raise ValueError('invalid lookup')
        where = field+'=?' + (' AND principal=?' if principal is not None else '')
        args = (value, principal) if principal is not None else (value,)
        sizes=db.execute("SELECT length(CAST(request_json AS BLOB)), length(CAST(claim_json AS BLOB)), length(CAST(reservation_json AS BLOB)), length(CAST(plan_json AS BLOB)), length(CAST(job_id AS BLOB)), length(CAST(submission_id AS BLOB)), length(CAST(principal AS BLOB)), length(CAST(environment AS BLOB)), length(CAST(request_sha AS BLOB)), length(CAST(claim_sha AS BLOB)), length(CAST(reservation_sha AS BLOB)), length(CAST(plan_sha AS BLOB)) FROM reference_v2_staged_jobs WHERE "+where,args).fetchone()
        if sizes is None: return None
        limits=(16*1024*1024+1024,4096,4096,16*1024*1024,36,36,128,10,64,64,64,64)
        if any(value is not None and (type(value) is not int or value<0 or value>limit) for value,limit in zip(sizes,limits)): raise Conflict('v2 persisted metadata invalid')
        return db.execute('SELECT * FROM reference_v2_staged_jobs WHERE '+where,args).fetchone()

    def _run(self, operation):
        gate = getattr(self.queue, 'admission_drain', None)
        try:
            with gate.admit() if gate is not None else nullcontext():
                with self.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE')
                    self._check_schema(db)
                    if gate is not None and gate._closed(db): raise Conflict('admission closed')
                    return operation(db)
        except AdmissionClosed:
            raise Conflict('admission closed') from None

    def _quota(self, db, extra):
        used = db.execute('''SELECT COALESCE(SUM(length(CAST(request_json AS BLOB))+
            COALESCE(length(CAST(claim_json AS BLOB)),0)+COALESCE(length(CAST(reservation_json AS BLOB)),0)+COALESCE(length(CAST(plan_json AS BLOB)),0)),0)
            FROM reference_v2_staged_jobs''').fetchone()[0]
        if used + extra > self.max_metadata_bytes: raise Conflict('v2 metadata quota reached')

    def find_exact_submission_replay(self, body, *, principal, environment):
        """Recover an identical committed receipt without consulting current policy."""
        p._slug(principal,'trusted principal');p._environment(environment)
        p._closed(body,('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint'),'v2 submission')
        p._uuid(body['submissionId'],'submissionId')
        encoded=p.canonical_json({'principal':principal,'environment':environment,'body':body}).decode()
        def read(db):
            row=self._fetch(db,'submission_id',body['submissionId'],principal=principal)
            if row is None: return None
            self._verify_row(row)
            if row['environment']!=environment or row['request_json']!=encoded:
                raise Conflict('v2 submission unavailable')
            return {'jobId':row['job_id'],'generation':1,'created':False}
        return self._run(read)

    def submit(self, body, *, principal, environment, capability_fingerprint, allowed_odm_options, options_fingerprint=None):
        p._slug(principal, 'trusted principal'); p._environment(environment);p._hash(capability_fingerprint,'capability')
        body=decode_canonical_input_v2(p.canonical_json(body))
        p._closed(body, ('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint'), 'v2 submission')
        p._require(type(body['schemaVersion']) is int and body['schemaVersion']==2,'unsupported version')
        p._uuid(body['submissionId'],'submissionId');p._require(body['capabilityFingerprint']==capability_fingerprint,'stale capability')
        validate_manifest_v2(body['manifest'])
        # Reuse v1's ODM-only option/pipeline checks on a synthetic one-image
        # manifest; actual v2 inventory is validated above, not by this surrogate.
        one=next((o for o in body['manifest']['objects'] if o['kind']=='image' and o['byteSize']>0),None)
        p._require(one is not None,'positive-size image required')
        surrogate={'id':'validation','revision':1,'totalBytes':one['byteSize'],'objectCount':1,
                   'inputCounts':{'images':1,'lidarObjects':0,'points':0},'objects':[one]}
        surrogate['sha256']=p.manifest_sha256(surrogate)
        p.validate_submission({'schemaVersion':1,'submissionId':body['submissionId'],'manifest':surrogate,
            'pipeline':body['pipeline'],'capabilityFingerprint':capability_fingerprint},environment=environment,
            capability_fingerprint=capability_fingerprint,available_engines=('odm',),allowed_odm_options=allowed_odm_options,options_fingerprint=options_fingerprint)
        request={'principal':principal,'environment':environment,'body':body}
        encoded=p.canonical_json(request).decode(); digest=p.digest(request)
        def write(db):
            old=self._fetch(db,'submission_id',body['submissionId'])
            if old:
                self._verify_row(old)
                if old['request_sha']!=digest or old['request_json']!=encoded: raise Conflict('v2 submission unavailable')
                return {'jobId':old['job_id'],'generation':1,'created':False}
            self._quota(db,len(encoded.encode()))
            job=str(uuid.uuid4())
            db.execute('INSERT INTO reference_v2_staged_jobs(job_id,submission_id,principal,environment,request_json,request_sha) VALUES(?,?,?,?,?,?)',(job,body['submissionId'],principal,environment,encoded,digest))
            return {'jobId':job,'generation':1,'created':True}
        return self._run(write)

    @staticmethod
    def _verify_row(row):
        try:
            request = json.loads(row['request_json'])
            p._closed(request, ('principal','environment','body'), 'persisted request')
            p._closed(request['body'], ('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint'), 'persisted body')
            p._uuid(row['job_id'], 'jobId'); p._uuid(row['submission_id'], 'submissionId')
            p._slug(row['principal'], 'principal'); p._environment(row['environment'])
            if (type(request['body']['schemaVersion']) is not int or request['body']['schemaVersion'] != 2): raise ValueError()
            p._hash(request['body']['capabilityFingerprint'], 'capability')
            if (p.canonical_json(request).decode() != row['request_json'] or p.digest(request) != row['request_sha'] or request['principal'] != row['principal']
                    or request['environment'] != row['environment']
                    or request['body']['submissionId'] != row['submission_id']):
                raise ValueError()
            validate_manifest_v2(request['body']['manifest'])
            claim = json.loads(row['claim_json']) if row['claim_json'] is not None else None
            if (claim is None) != (row['claim_sha'] is None) or (claim is not None and (p.canonical_json(claim).decode() != row['claim_json'] or p.digest(claim) != row['claim_sha'])):
                raise ValueError()
            if claim is not None:
                p._closed(claim, ('startupId','nodeId','workerId','instanceId','requestId','token'), 'persisted claim')
                p._uuid(claim['startupId'], 'startupId')
                for key in ('nodeId','workerId','instanceId','requestId'): p._slug(claim[key],key)
                if type(claim['token']) is not str or not claim['token'].isascii() or not 1 <= len(claim['token']) <= 128: raise ValueError()
            stored_plan = json.loads(row['plan_json']) if row['plan_json'] is not None else None
            if (stored_plan is None) != (row['plan_sha'] is None) or (stored_plan is not None and (p.canonical_json(stored_plan).decode() != row['plan_json'] or p.digest(stored_plan) != row['plan_sha'])): raise ValueError()
            reservation = json.loads(row['reservation_json']) if row['reservation_json'] is not None else None
            if (reservation is None) != (stored_plan is None): raise ValueError()
            if (reservation is None) != (row['reservation_sha'] is None) or (reservation is not None and (p.canonical_json(reservation).decode() != row['reservation_json'] or p.digest(reservation) != row['reservation_sha'])): raise ValueError()
            if reservation is not None:
                p._closed(reservation, ('protocolVersion','jobId','generation','reservationId','manifestSha256','environment','capabilityFingerprint','expiresAt','rangeBytes','requestBudget','byteBudget','nonce'), 'persisted reservation')
                if (reservation['jobId'] != row['job_id'] or reservation['environment'] != row['environment']
                        or reservation['manifestSha256'] != request['body']['manifest']['sha256']
                        or reservation['capabilityFingerprint'] != request['body']['capabilityFingerprint']
                        or type(reservation['protocolVersion']) is not int or reservation['protocolVersion'] != 2
                        or type(reservation['generation']) is not int or reservation['generation'] != 1): raise ValueError()
                p._slug(reservation['nonce'], 'nonce')
                manifest = request['body']['manifest']
                bare = {key:reservation[key] for key in ('protocolVersion','jobId','generation','reservationId','manifestSha256','environment','capabilityFingerprint','expiresAt','rangeBytes')}
                bare['objects'] = [{key:item[key] for key in ('id','byteSize','sha256')} for item in manifest['objects']]
                bare.update(retryRequests=0,retryBytes=0)
                minimum = plan_input_budget(bare)
                p._integer(reservation['requestBudget'], 'request budget')
                p._integer(reservation['byteBudget'], 'byte budget', allow_zero=True)
                bare['retryRequests'] = reservation['requestBudget'] - minimum['requestBudget']
                bare['retryBytes'] = reservation['byteBudget'] - minimum['byteBudget']
                calculated = plan_input_budget(bare)
                if (calculated['requestBudget'],calculated['byteBudget']) != (reservation['requestBudget'],reservation['byteBudget']): raise ValueError()
                verified_plan = plan_input_budget(stored_plan)
                for key in ('protocolVersion','jobId','generation','reservationId','manifestSha256','environment','capabilityFingerprint','expiresAt','rangeBytes','requestBudget','byteBudget'):
                    if type(verified_plan[key]) is not type(reservation[key]) or verified_plan[key] != reservation[key]: raise ValueError()
                if stored_plan['objects'] != bare['objects']: raise ValueError()
            return request, claim
        except (ValueError, KeyError, TypeError, RecursionError, UnicodeError):
            raise Conflict('v2 persisted metadata invalid') from None

    def claim_metadata(self, job_id, *, principal, node_id, worker_id, instance_id, request_id):
        p._uuid(job_id,'jobId')
        for value in (principal,node_id,worker_id,instance_id,request_id): p._slug(value,'identity')
        self.queue._known_node(node_id)
        def write(db):
            row=self._fetch(db,'job_id',job_id,principal)
            if row is None: raise Conflict('v2 metadata claim unavailable')
            _request, prior = self._verify_row(row)
            if prior is not None:
                if (prior['startupId'],prior['nodeId'],prior['workerId'],prior['instanceId'],prior['requestId']) != (self.queue.startup_id,node_id,worker_id,instance_id,request_id): raise Conflict('v2 metadata claim held or differs')
                return {'claimToken':prior['token'],'jobId':job_id}
            claim={'startupId':self.queue.startup_id,'nodeId':node_id,'workerId':worker_id,'instanceId':instance_id,'requestId':request_id,'token':secrets.token_urlsafe(32)}
            encoded=p.canonical_json(claim).decode();self._quota(db,len(encoded.encode()));db.execute('UPDATE reference_v2_staged_jobs SET claim_json=?,claim_sha=? WHERE job_id=?',(encoded,p.digest(claim),job_id))
            return {'claimToken':claim['token'],'jobId':job_id}
        return self._run(write)

    def bind_reservation(self, job_id, envelope, plan, *, principal, claim_token, clock):
        p._uuid(job_id,'jobId');p._slug(principal,'trusted principal')
        if type(claim_token) is not str or not claim_token.isascii() or len(claim_token)>128: raise Conflict('v2 metadata claim unavailable')
        envelope,plan=(decode_canonical_input_v2(p.canonical_json(x)) for x in (envelope,plan))
        p._closed(envelope,('protocolVersion','jobId','generation','reservationId','manifestSha256','environment','capabilityFingerprint','expiresAt','rangeBytes','requestBudget','byteBudget','nonce'),'v2 reservation')
        p._slug(envelope['nonce'],'nonce'); calculated=plan_input_budget(plan)
        for key in calculated:
            if key in envelope and (type(envelope[key]) is not type(calculated[key]) or envelope[key]!=calculated[key]): raise Conflict('reservation plan differs')
        if envelope['jobId']!=job_id: raise Conflict('reservation job differs')
        expiry=datetime.fromisoformat(envelope['expiresAt'].replace('Z','+00:00')).timestamp()
        def write(db):
            row=self._fetch(db,'job_id',job_id,principal)
            if row is None or not row['claim_json']: raise Conflict('v2 metadata claim unavailable')
            request, claim=self._verify_row(row);body=request['body'];manifest=body['manifest']
            if (claim['startupId']!=self.queue.startup_id or not hmac.compare_digest(claim['token'],claim_token)
                or envelope['generation']!=1 or envelope['environment']!=row['environment']
                or envelope['capabilityFingerprint']!=body['capabilityFingerprint'] or envelope['manifestSha256']!=manifest['sha256']
                or plan['objects']!=[{k:o[k] for k in ('id','byteSize','sha256')} for o in manifest['objects']]): raise Conflict('reservation binding differs')
            now=clock()
            if type(now) not in (int,float) or not math.isfinite(now) or now>=expiry: raise Conflict('reservation expired')
            encoded=p.canonical_json(envelope).decode();digest=p.digest(envelope)
            plan_encoded=p.canonical_json(plan).decode();plan_digest=p.digest(plan)
            if row['reservation_json']:
                if row['reservation_json']!=encoded or row['reservation_sha']!=digest or row['plan_json']!=plan_encoded or row['plan_sha']!=plan_digest: raise Conflict('reservation already bound')
                return {'reservationSha256':digest,'accepted':True}
            self._quota(db,len(encoded.encode())+len(plan_encoded.encode()))
            now=clock()
            if type(now) not in (int,float) or not math.isfinite(now) or now>=expiry: raise Conflict('reservation expired')
            db.execute('UPDATE reference_v2_staged_jobs SET reservation_json=?,reservation_sha=?,plan_json=?,plan_sha=? WHERE job_id=?',(encoded,digest,plan_encoded,plan_digest,job_id))
            return {'reservationSha256':digest,'accepted':True}
        return self._run(write)

    def snapshot_bytes(self, job_id, *, principal, claim_token, clock):
        """Internal current-epoch observation; not broker authorization."""
        p._uuid(job_id, 'jobId'); p._slug(principal, 'trusted principal')
        if type(claim_token) is not str or not claim_token.isascii() or len(claim_token)>128:
            raise Conflict('v2 metadata claim unavailable')
        def read(db):
            row=self._fetch(db,'job_id',job_id,principal)
            if row is None: raise Conflict('v2 metadata claim unavailable')
            request, claim=self._verify_row(row)
            if claim is None or claim['startupId'] != self.queue.startup_id or not hmac.compare_digest(claim['token'],claim_token) or row['reservation_json'] is None:
                raise Conflict('v2 metadata claim unavailable')
            reservation=json.loads(row['reservation_json'])
            expiry=datetime.fromisoformat(reservation['expiresAt'].replace('Z','+00:00')).timestamp()
            now=clock()
            if type(now) not in (int,float) or not math.isfinite(now) or now>=expiry:
                raise Conflict('reservation expired')
            return {'manifestBytes':p.canonical_json(request['body']['manifest']),
                    'manifestSha256':request['body']['manifest']['sha256'],
                    'planBytes':row['plan_json'].encode('utf-8'),'planSha256':row['plan_sha'],
                    'reservationBytes':row['reservation_json'].encode('utf-8'),
                    'reservationSha256':row['reservation_sha']}
        return self._run(read)
