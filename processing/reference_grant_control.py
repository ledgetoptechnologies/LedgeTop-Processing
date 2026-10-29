"""Hard-off candidate renewable receiver route, with a synthetic-only factory.

Not mounted in any server. No key, issuer, real authentication, custody or lease
authority. Synthetic callbacks supply explicitly test-owned SQLite connections
and job metadata. The receiver never learns the Viewer's HMAC secret.
"""
import base64
import copy
import hashlib
import json
import re
import sqlite3
import threading

from . import reference_grant_renewal as renewal
from . import reference_protocol as p
from .reference_api import Response
from .reference_transfer import validate_grant

PREFIX = '/ltds/renewable/v1'
ISSUANCE_PROFILE = 'ltds-renewable-hmac-issuance-v1'
SEED_FIELDS = ('profile','schemaVersion','keyId','issuerRealm','jobId','generation',
    'environment','reservationId','nodeId','workerId','manifestSha256',
    'objectClosureSha256','policySha256','capabilitiesSha256','requestSha256',
    'requestId','epoch','predecessorSha256','expiresAt','nonce')
INITIAL_FIELDS = ('schemaVersion','profile','request','issuanceSeed','grant')
CONTEXT_FIELDS = ('journalDb','controlDb','job','nodeId','workerId','policy',
    'capabilities','startedMs','stage','inflightDrained','keyId','issuerRealm')
CONTROL_SCHEMA = (
    "CREATE TABLE initial_intent (id INTEGER PRIMARY KEY CHECK(id=1), record TEXT NOT NULL CHECK(typeof(record)='text' AND length(CAST(record AS BLOB))<=16384), sha TEXT NOT NULL CHECK(typeof(sha)='text' AND length(sha)=64))",
    "CREATE TRIGGER initial_insert BEFORE INSERT ON initial_intent WHEN EXISTS(SELECT 1 FROM initial_intent) BEGIN SELECT RAISE(ABORT,'immutable initial intent'); END",
    "CREATE TRIGGER initial_update BEFORE UPDATE ON initial_intent BEGIN SELECT RAISE(ABORT,'immutable initial intent'); END",
    "CREATE TRIGGER initial_delete BEFORE DELETE ON initial_intent BEGIN SELECT RAISE(ABORT,'immutable initial intent'); END",
)


def seed_bytes(seed):
    """Structural byte contract only; never accepts a seed as issuer authority."""
    renewal.closed(seed, SEED_FIELDS)
    renewal.require(seed['profile']==ISSUANCE_PROFILE and type(seed['schemaVersion']) is int and seed['schemaVersion']==1)
    for key in ('keyId','issuerRealm','jobId','reservationId','nodeId','workerId','nonce'):
        try:p._slug(seed[key],key)
        except ValueError:raise renewal.RenewalHeld('issuance_identifier_held') from None
    renewal.integer(seed['generation']);renewal.integer(seed['epoch'],0,renewal.MAX_EPOCHS)
    renewal.require(seed['environment'] in ('production','staging'))
    for key in ('manifestSha256','objectClosureSha256','policySha256','capabilitiesSha256','requestSha256'):
        try:p._hash(seed[key],key)
        except ValueError:raise renewal.RenewalHeld('issuance_digest_held') from None
    try:p._uuid(seed['requestId'],'request')
    except ValueError:raise renewal.RenewalHeld('issuance_request_held') from None
    if seed['epoch']==0:renewal.require(seed['predecessorSha256'] is None)
    else:
        try:p._hash(seed['predecessorSha256'],'predecessor')
        except ValueError:raise renewal.RenewalHeld('issuance_predecessor_held') from None
    renewal.milliseconds(seed['expiresAt'])
    return renewal.encoded(seed)


def expected_seed(request, context):
    """Reconstruct from caller-owned records, not supplied digest assertions."""
    result={key:copy.deepcopy(request[key]) for key in
        ('jobId','generation','environment','reservationId','nodeId','workerId',
         'manifestSha256','requestId','epoch','predecessorSha256','expiresAt','nonce')}
    result.update(profile=ISSUANCE_PROFILE,schemaVersion=1,keyId=context['keyId'],
        issuerRealm=context['issuerRealm'],objectClosureSha256=p.digest(context['job']['submission']['manifest']['objects']),
        policySha256=p.digest(context['policy']),capabilitiesSha256=p.digest(context['capabilities']),
        requestSha256=p.digest(request))
    seed_bytes(result)
    return result


class RenewableControlAPI:
    """Production entry remains hard-off regardless of caller flags."""
    def handle(self, method, path, headers, body=b'', *, protected_transport=False):
        return Response(404,{'error':'not_found'})


def create_synthetic_control(*, scope, authorize, resolve, clock_ms):
    if scope!='synthetic-fixture-only' or not all(callable(v) for v in (authorize,resolve,clock_ms)):
        raise renewal.RenewalHeld('synthetic_control_only')
    return _SyntheticControl(authorize,resolve,clock_ms)


class _SyntheticControl(RenewableControlAPI):
    def __init__(self,authorize,resolve,clock_ms):
        self.authorize,self.resolve,self.clock_ms=authorize,resolve,clock_ms
        self.lock=threading.RLock()

    def _context(self,job_id,identity):
        context=self.resolve(job_id)
        renewal.closed(context,CONTEXT_FIELDS)
        renewal.closed(context['job'],('jobId','generation','environment','manifestSha256','submission','transferRequest'))
        renewal.require(context['job']['jobId']==job_id and context['job']['environment']==identity['environment'])
        objects=context['job']['submission']['manifest']['objects']
        renewal.require(type(objects) is list and 0<len(objects)<=p.MAX_OBJECTS
                        and len(p.canonical_json(context['job']))<=p.MAX_BODY_BYTES)
        renewal.integer(context['startedMs'])
        renewal.integer(context['policy']['maxLifetimeMs'])
        renewal.integer(context['startedMs']+context['policy']['maxLifetimeMs'])
        for key in ('journalDb','controlDb'):
            db=context[key]
            renewal.require(type(db) is sqlite3.Connection and not db.in_transaction)
            renewal.require(db.execute('PRAGMA journal_mode').fetchone()[0]=='delete'
                            and db.execute('PRAGMA synchronous').fetchone()[0]==2)
            renewal.require(db.execute('SELECT count(*) FROM temp.sqlite_master').fetchone()[0]==0
                            and [r[1] for r in db.execute('PRAGMA database_list')]==['main','temp'])
        renewal.require(context['journalDb'] is not context['controlDb'])
        return context

    def _intent(self,context,*,put=None):
        db=context['controlDb'];db.execute('BEGIN IMMEDIATE')
        try:
            renewal.require(db.execute('SELECT 1 FROM main.sqlite_master WHERE sql IS NOT NULL LIMIT 1 OFFSET ?',
                (len(CONTROL_SCHEMA),)).fetchone() is None,'control_catalog_bound')
            renewal.require(db.execute("SELECT 1 FROM main.sqlite_master WHERE sql IS NOT NULL AND (typeof(sql)!='text' OR length(CAST(sql AS BLOB))>8192) LIMIT 1").fetchone() is None,'control_catalog_bound')
            catalog=db.execute("SELECT sql FROM main.sqlite_master WHERE sql IS NOT NULL LIMIT ?",(len(CONTROL_SCHEMA)+1,)).fetchall()
            if not catalog and put is not None:
                for sql in CONTROL_SCHEMA:db.execute(sql)
            else:renewal.require(sorted(r[0] for r in catalog)==sorted(CONTROL_SCHEMA),'control_schema_held')
            renewal.require(db.execute('SELECT 1 FROM initial_intent LIMIT 1 OFFSET 1').fetchone() is None)
            renewal.require(db.execute("SELECT 1 FROM initial_intent WHERE typeof(record)!='text' OR length(CAST(record AS BLOB))>16384 OR typeof(sha)!='text' OR length(sha)!=64 LIMIT 1").fetchone() is None)
            row=db.execute('SELECT record,sha FROM initial_intent WHERE id=1').fetchone()
            if row:
                renewal.require(hashlib.sha256(row[0].encode()).hexdigest()==row[1])
                if put is not None:renewal.require(row[0]==renewal.encoded(put).decode(),'initial_intent_conflict')
                result=renewal.decode(row[0].encode())
            elif put is not None:
                raw=renewal.encoded(put);renewal.require(len(raw)<=16384)
                db.execute('INSERT INTO initial_intent VALUES(1,?,?)',(raw.decode(),hashlib.sha256(raw).hexdigest()));result=put
            else:raise renewal.RenewalHeld('initial_intent_missing')
            db.commit();return result
        except BaseException:db.rollback();raise

    def _journal(self,context,original=None):
        if original is None:
            db=context['journalDb']
            renewal.require(db.execute("SELECT 1 FROM sqlite_master WHERE name='renewal_meta' AND type='table'").fetchone() is not None)
            renewal.require(db.execute("SELECT 1 FROM renewal_meta WHERE typeof(original_grant)!='text' OR length(CAST(original_grant AS BLOB))>? LIMIT 1",(renewal.MAX_GRANT_BYTES,)).fetchone() is None)
            row=db.execute('SELECT original_grant FROM renewal_meta WHERE id=1').fetchone()
            renewal.require(row is not None);original=renewal.decode(row[0].encode())
        intent=self._intent(context)
        renewal.require(p.digest(context['job'])==intent['originalJobSha256'],'initial_job_binding_held')
        renewal.require(expected_seed(intent['request'],context)==intent['seed'],'initial_seed_binding_held')
        renewal.require(p.digest(original)==intent['originalGrantSha256'])
        return renewal.RenewalJournal(context['journalDb'],enabled=True,job=context['job'],original_grant=original,
            node_id=context['nodeId'],worker_id=context['workerId'],policy=context['policy'],capabilities=context['capabilities'],started_ms=context['startedMs'])

    def _initial(self,payload,context,now):
        renewal.closed(payload,INITIAL_FIELDS)
        renewal.require(type(payload['schemaVersion']) is int and payload['schemaVersion']==2 and payload['profile']==renewal.PROFILE)
        request=payload['request'];renewal.closed(request,renewal.REQUEST_FIELDS)
        renewal.require(len(renewal.encoded(request))<=renewal.MAX_REQUEST_BYTES)
        renewal.require(type(request['epoch']) is int and request['epoch']==0 and request['predecessorSha256'] is None)
        renewal.require(type(request['schemaVersion']) is int and request['schemaVersion']==2 and request['profile']==renewal.PROFILE)
        job=context['job'];reservation=job['transferRequest']
        for key,value in dict(jobId=job['jobId'],generation=job['generation'],environment=job['environment'],
            reservationId=reservation['reservationId'],manifestSha256=job['manifestSha256'],nodeId=context['nodeId'],
            workerId=context['workerId'],expiresAt=reservation['expiresAt'],nonce=reservation['nonce']).items():
            renewal.require(type(request[key]) is type(value) and request[key]==value)
        wanted=expected_seed(request,context)
        renewal.require(seed_bytes(payload['issuanceSeed'])==seed_bytes(wanted),'initial_seed_binding_held')
        token=payload['grant'].get('token')
        renewal.require(type(token) is str and re.fullmatch('[A-Za-z0-9_-]{43}',token) is not None)
        renewal.require(len(base64.urlsafe_b64decode(token+'='))==32)
        renewal.require(base64.urlsafe_b64encode(base64.urlsafe_b64decode(token+'=')).rstrip(b'=').decode()==token)
        intent=dict(schemaVersion=1,request=request,seed=wanted,initialBodySha256=p.digest(payload),originalJobSha256=p.digest(job),
                    originalGrantSha256=p.digest(payload['grant']))
        # Matching durable intent permits exact historical receipt reconciliation;
        # it never authorizes a new GET after the initial grant expired.
        existing=context['controlDb'].execute("SELECT 1 FROM sqlite_master WHERE name='initial_intent'").fetchone()
        if existing:
            old=self._intent(context)
            renewal.require(old==intent,'initial_intent_conflict')
            journal=self._journal(context,original=payload['grant'])
        else:
            renewal.require(context['stage']=='awaiting_transfer' and context['inflightDrained'] is True)
            validate_grant(payload['grant'],job=job,clock=lambda:now/1000)
            # Compatibility validated BEFORE any control-journal effects.
            manifest=job['submission']['manifest']
            policy=renewal.validate_policy(context['policy'],total_bytes=manifest['totalBytes'],object_count=len(manifest['objects']))
            renewal.validate_compatibility(context['capabilities'],policy=policy,manifest=manifest,
                started_ms=context['startedMs'],original_expiry_ms=renewal.milliseconds(reservation['expiresAt']))
            expiry=renewal.milliseconds(reservation['expiresAt'])
            renewal.require(context['startedMs']<expiry<=context['startedMs']+policy['maxLifetimeMs']
                            and policy['byteBudget']==payload['grant']['byteBudget'])
            self._intent(context,put=intent)
            journal=self._journal(context,original=payload['grant'])
        return dict(requestId=request['requestId'],epoch=0,grantSha256=p.digest(payload['grant']),accepted=True)

    def handle(self,method,path,headers,body=b'',*,protected_transport=False):
        if protected_transport is not True:return Response(503,{'error':'protected_transport_required'})
        with self.lock:
            try:
                identity=self.authorize(headers)
                renewal.closed(identity,('role','jobId','environment'))
                renewal.require(identity['role'] in ('worker','issuer'))
            except Exception:return Response(401,{'error':'unauthorized'})
            match=re.fullmatch(re.escape(PREFIX)+r'/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})/(initial-grant|renewal-requests|renewal-grants|private-grant|reconcile/([0-9a-f-]{36}))',path)
            if match is None or identity['jobId']!=match[1]:return Response(404,{'error':'not_found'})
            try:
                context=self._context(match[1],identity);now=renewal.integer(self.clock_ms())
                suffix=match[2];role=identity['role']
                if method=='POST' and suffix=='initial-grant' and role=='issuer':
                    return Response(200,self._initial(renewal.decode(body),context,now))
                journal=self._journal(context)
                if method=='POST' and suffix=='renewal-requests' and role=='worker':
                    renewal.require(context['stage']=='fetching')
                    return Response(200,journal.request(renewal.decode(body),now_ms=now,inflight_drained=context['inflightDrained']))
                if method=='GET' and suffix=='renewal-requests' and role=='issuer':
                    with journal.transaction():
                        journal.audit();row=journal.db.execute('SELECT record FROM renewal_requests ORDER BY epoch DESC LIMIT 1').fetchone()
                    return Response(200,{'request':renewal.decode(row[0].encode()) if row else None})
                if method=='POST' and suffix=='renewal-grants' and role=='issuer':
                    renewal.require(context['stage']=='fetching')
                    return Response(200,journal.accept(renewal.decode(body),now_ms=now,inflight_drained=context['inflightDrained']))
                if method=='GET' and suffix.startswith('reconcile/'):
                    return Response(200,{'receipt':journal.reconcile(match[3])})
                if method=='GET' and suffix=='private-grant' and role=='worker':
                    with journal.transaction():
                        journal.audit();epoch,raw=journal.db.execute('SELECT epoch,record FROM renewal_epochs ORDER BY epoch DESC LIMIT 1').fetchone()
                    return Response(200,{'epoch':epoch,'privateGrant':renewal.decode(raw.encode())})
                return Response(405,{'error':'method_not_allowed'})
            except Exception:
                return Response(409,{'error':'renewable_control_held'})
