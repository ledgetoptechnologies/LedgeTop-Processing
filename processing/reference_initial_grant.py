"""Initial-only authenticated delivery profile. No issuer key or renewal rights.

The Viewer owns durable bearer recovery and source custody. Processing validates
its authenticated delivery against local job/claim/reservation and pinned issuer
configuration. Epoch successors remain unavailable. Nothing autostarts here.
"""
from dataclasses import dataclass
import base64
import copy
import json
import re

from . import reference_protocol as p
from . import reference_grant_renewal as renewal
from .reference_grant_control import expected_seed,seed_bytes,INITIAL_FIELDS
from .reference_grant_history import validate_historical_grant

PROFILE_FIELDS=('schemaVersion','issuerRealm','keyId','maxEpochs','maxLifetimeMs','brokerCapabilities')


@dataclass(frozen=True,repr=False)
class InitialGrantPolicy:
    canonical: str

    @classmethod
    def from_payload(cls,value):
        p._closed(value,PROFILE_FIELDS,'initial grant policy')
        p._require(type(value['schemaVersion']) is int and value['schemaVersion']==1,'initial policy version')
        p._require(type(value['maxEpochs']) is int and value['maxEpochs']==0,'initial-only epoch zero required')
        p._integer(value['maxLifetimeMs'],'initial lifetime')
        for key in ('issuerRealm','keyId'):p._slug(value[key],key)
        caps=value['brokerCapabilities'];p._closed(caps,renewal.CAPABILITY_FIELDS,'broker capabilities')
        p._require(type(caps['schemaVersion']) is int and caps['schemaVersion']==2 and caps['profile']==renewal.PROFILE
            and caps['wholeObject200'] is True,'whole-object broker profile required')
        for key in renewal.CAPABILITY_FIELDS[3:]:
            if key=='maxEpochs':renewal.integer(caps[key],0,renewal.MAX_EPOCHS)
            else:p._integer(caps[key],key)
        p._require(value['maxLifetimeMs']<=caps['maxLifetimeMs'],'lifetime capability differs')
        raw=p.canonical_json(value);p._require(len(raw)<=16384,'initial policy bound')
        return cls(raw.decode())

    def payload(self):return json.loads(self.canonical)

    def transfer_policy(self,manifest,*,byte_budget=None):
        """Job-specific policy; ALL physical GET attempts must fit epoch ZERO."""
        value=self.payload();objects=manifest['objects']
        p._require(type(objects) is list and 0<len(objects)<=p.MAX_OBJECTS,'initial object count')
        total=manifest['totalBytes'];p._integer(total,'input bytes')
        budget=min(p.MAX_SAFE_INTEGER,3*total) if byte_budget is None else byte_budget
        p._integer(budget,'initial byte budget');p._require(total<=budget<=min(p.MAX_SAFE_INTEGER,3*total),'initial byte ceiling')
        attempts=min(3*len(objects),len(objects)+10);caps=value['brokerCapabilities']
        largest=max(o['byteSize'] for o in objects)
        p._require(largest<=caps['maxResponseBytes'] and largest<=caps['maxConcurrentBytes'], 'whole object incompatible')
        p._require(attempts<=caps['maxRequestsPerEpoch'] and attempts<=caps['maxTotalRequests']
            and budget<=caps['maxTotalBytes'],'epoch-zero capacity incompatible')
        return dict(profile=renewal.PROFILE,maxEpochs=0,maxLifetimeMs=value['maxLifetimeMs'],byteBudget=budget,attemptBudget=attempts)


POLICY_SQL="CREATE TABLE reference_initial_policy (singleton INTEGER PRIMARY KEY CHECK(singleton=1), record TEXT NOT NULL CHECK(typeof(record)='text' AND length(CAST(record AS BLOB))<=16384), sha TEXT NOT NULL CHECK(typeof(sha)='text' AND length(sha)=64))"
JOBS_SQL="CREATE TABLE reference_initial_jobs (job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), record TEXT NOT NULL CHECK(typeof(record)='text' AND length(CAST(record AS BLOB))<=4096), sha TEXT NOT NULL CHECK(typeof(sha)='text' AND length(sha)=64))"
RECEIPTS_SQL="CREATE TABLE reference_initial_receipts (job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), body_sha TEXT NOT NULL CHECK(typeof(body_sha)='text' AND length(body_sha)=64), record TEXT NOT NULL CHECK(typeof(record)='text' AND length(CAST(record AS BLOB))<=16384), receipt TEXT NOT NULL CHECK(typeof(receipt)='text' AND length(CAST(receipt AS BLOB))<=4096))"
TABLES=(POLICY_SQL,JOBS_SQL,RECEIPTS_SQL)
TRIGGERS=tuple('CREATE TRIGGER '+table+'_'+action.lower()+' BEFORE '+action+' ON '+table+
    " BEGIN SELECT RAISE(ABORT,'immutable initial grant history'); END"
    for table in ('reference_initial_policy','reference_initial_jobs','reference_initial_receipts') for action in ('UPDATE','DELETE'))+tuple(
    'CREATE TRIGGER '+table+'_insert BEFORE INSERT ON '+table+' WHEN EXISTS(SELECT 1 FROM '+table+' WHERE '+key+'=NEW.'+key+
    ") BEGIN SELECT RAISE(ABORT,'immutable initial grant collision'); END"
    for table,key in (('reference_initial_policy','singleton'),('reference_initial_jobs','job_id'),('reference_initial_receipts','job_id')))


def check_policy_binding(db,policy,*,create=False):
    """Compatible-source latch, before Queue schema/clock writers.

    Existing pre-feature binaries cannot enforce this ABI; deployment must fence
    them. This guard does not migrate their histories or manufacture authority.
    """
    p._require(db.execute('SELECT 1 FROM temp.sqlite_master LIMIT 1').fetchone() is None,'initial TEMP context forbidden')
    p._require([row[1] for row in db.execute('PRAGMA database_list')]==['main','temp'],'initial attached context forbidden')
    if policy is not None:
        p._require(type(policy) is InitialGrantPolicy and InitialGrantPolicy.from_payload(policy.payload())==policy,'concrete initial policy required')
    exists=db.execute("SELECT 1 FROM sqlite_master WHERE name GLOB 'reference_initial_*' LIMIT 1").fetchone()
    if not exists:
        if not create or policy is None:return
        for sql in TABLES+TRIGGERS:db.execute(sql)
        db.execute('INSERT INTO reference_initial_policy VALUES(1,?,?)',(policy.canonical,p.digest(policy.payload())))
        return
    p._require(policy is not None,'latched initial policy cannot be omitted')
    limit=len(TABLES)+len(TRIGGERS)
    p._require(db.execute("SELECT 1 FROM sqlite_master WHERE name GLOB 'reference_initial_*' AND sql IS NOT NULL LIMIT 1 OFFSET ?",(limit,)).fetchone() is None,'initial ABI count')
    p._require(db.execute("SELECT 1 FROM sqlite_master WHERE name GLOB 'reference_initial_*' AND sql IS NOT NULL AND (typeof(sql)!='text' OR length(CAST(sql AS BLOB))>8192) LIMIT 1").fetchone() is None,'initial ABI bounds')
    observed=tuple(row[0] for row in db.execute("SELECT sql FROM sqlite_master WHERE name GLOB 'reference_initial_*' AND sql IS NOT NULL LIMIT ?",(limit+1,)))
    p._require(sorted(observed)==sorted(TABLES+TRIGGERS),'initial ABI changed')
    p._require(db.execute('SELECT 1 FROM reference_initial_policy LIMIT 1 OFFSET 1').fetchone() is None,'initial policy count')
    p._require(db.execute("SELECT 1 FROM reference_initial_policy WHERE typeof(record)!='text' OR length(CAST(record AS BLOB))>16384 OR typeof(sha)!='text' OR length(sha)!=64 LIMIT 1").fetchone() is None,'initial policy bound')
    row=db.execute('SELECT record,sha FROM reference_initial_policy WHERE singleton=1').fetchone()
    p._require(row is not None and tuple(row)==(policy.canonical,p.digest(policy.payload())),'initial policy changed')


def validate_initial_envelope(payload,*,job,node_id,worker_id,policy,now_ms=None):
    """No HMAC verification: Viewer authentication is supplied by real API.

    now_ms=None validates historical exact replay without inventing a past clock.
    Fresh delivery MUST pass an actual finite clock and original reservation TTL.
    """
    p._require(type(policy) is InitialGrantPolicy,'configured issuer policy required')
    p._closed(payload,INITIAL_FIELDS,'initial envelope')
    p._require(type(payload['schemaVersion']) is int and payload['schemaVersion']==2 and payload['profile']==renewal.PROFILE,'initial envelope version')
    request=payload['request'];p._closed(request,renewal.REQUEST_FIELDS,'initial request')
    p._require(len(p.canonical_json(request))<=renewal.MAX_REQUEST_BYTES,'initial request bound')
    p._require(type(request['schemaVersion']) is int and request['schemaVersion']==2 and request['profile']==renewal.PROFILE
        and type(request['epoch']) is int and request['epoch']==0 and request['predecessorSha256'] is None,'initial epoch zero required')
    reservation=job['transferRequest']
    for key,value in dict(jobId=job['jobId'],generation=job['generation'],environment=job['environment'],
        reservationId=reservation['reservationId'],manifestSha256=job['manifestSha256'],nodeId=node_id,workerId=worker_id,
        expiresAt=reservation['expiresAt'],nonce=reservation['nonce']).items():
        p._require(type(request[key]) is type(value) and request[key]==value,'initial request binding differs')
    validate_historical_grant(payload['grant'],job=job)
    token=payload['grant']['token']
    p._require(re.fullmatch(r'[A-Za-z0-9_-]{43}',token) is not None,'initial HMAC bearer shape required')
    decoded=base64.urlsafe_b64decode(token+'=')
    p._require(len(decoded)==32 and base64.urlsafe_b64encode(decoded).rstrip(b'=').decode()==token,'canonical bearer encoding required')
    settings=policy.payload();transfer=policy.transfer_policy(job['submission']['manifest'],byte_budget=reservation['byteBudget'])
    expected=expected_seed(request,dict(job=job,policy=transfer,capabilities=settings['brokerCapabilities'],keyId=settings['keyId'],issuerRealm=settings['issuerRealm']))
    p._require(seed_bytes(payload['issuanceSeed'])==seed_bytes(expected),'initial issuer seed binding differs')
    if now_ms is not None:
        renewal.integer(now_ms)
        remaining=renewal.milliseconds(reservation['expiresAt'])-now_ms
        p._require(0<remaining<=settings['brokerCapabilities']['maxEpochTtlMs'] and remaining<=settings['maxLifetimeMs'],'initial expiry incompatible')
    receipt=dict(schemaVersion=1,receiptType='initial_grant_delivery',jobId=job['jobId'],generation=job['generation'],
        environment=job['environment'],reservationId=reservation['reservationId'],manifestSha256=job['manifestSha256'],
        nonce=reservation['nonce'],requestId=request['requestId'],requestSha256=p.digest(request),
        grantSha256=p.digest(payload['grant']),envelopeSha256=p.digest(payload),accepted=True,mayDownload=False)
    record=dict(schemaVersion=1,request=copy.deepcopy(request),issuanceSeed=copy.deepcopy(expected),
        immutableJobSha256=p.digest({k:job[k] for k in ('jobId','generation','environment','manifestSha256','pipelineDigest','submission','transferRequest')}))
    p._require(len(p.canonical_json(record))<=16384 and len(p.canonical_json(receipt))<=4096,'initial receipt bound')
    return record,receipt
