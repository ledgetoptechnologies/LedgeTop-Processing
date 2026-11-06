"""Unwired, bounded coordinator profile selection and replay primitive.

The caller MUST authenticate the Viewer principal and worker capability over
trusted transports and provide a private durable SQLite database. This module
does not implement network authentication, worker admission, reservation,
transfer authorization, custody or output acceptance.
"""
import hashlib
import json
import sqlite3
import uuid
from . import direct_limits as limits, reference_protocol as p

MAX_REQUEST_BYTES = 4096
MAX_CAPABILITY_BYTES = 4096
MAX_RESPONSE_BYTES = 4096
MAX_JSON_DEPTH = 4
MAX_SELECTIONS = 64
MAX_STORAGE_BYTES = 1024 * 1024
REQUEST_FIELDS = frozenset(('operationId','principalId','authorityId','nodeId','workerId',
    'workerInstanceId','jobId','attemptId','reservationId','generation','environment','manifestSha256',
    'derivedManifestSha256','profile'))
CAP_FIELDS = frozenset(('authorityId','nodeId','workerId','workerInstanceId','profile',
    'maxSource','maxDerived','maxManifestBytes'))
IDENTITY = ('authorityId','nodeId','workerId','workerInstanceId')

class NegotiationHeld(ValueError): pass

def verify_schema(db):
    rows=db.execute('PRAGMA table_info(direct_negotiation_candidate)').fetchall()
    expected=[('principal','TEXT',1,1),('operation','TEXT',1,2),('request','BLOB',1,0),
              ('capability','BLOB',1,0),('response','BLOB',1,0)]
    if [(r[1],r[2].upper(),r[3],r[5]) for r in rows]!=expected:
        raise NegotiationHeld('direct negotiation schema requires inspection')

def _decode(raw,maximum):
    if type(raw) is not bytes or not 0 < len(raw) <= maximum:
        raise NegotiationHeld('bounded bytes required')
    # Depth is checked on the raw stream before JSON parsing, ignoring strings.
    depth=0; quoted=False; escaped=False
    for byte in raw:
        if quoted:
            if escaped: escaped=False
            elif byte==92: escaped=True
            elif byte==34: quoted=False
        elif byte==34: quoted=True
        elif byte in (91,123):
            depth+=1
            if depth>MAX_JSON_DEPTH: raise NegotiationHeld('JSON depth exceeded')
        elif byte in (93,125):
            depth-=1
            if depth<0: raise NegotiationHeld('invalid JSON nesting')
    if quoted or depth: raise NegotiationHeld('invalid JSON nesting')
    def pairs(items):
        out={}
        for key,value in items:
            if key in out: raise ValueError('duplicate key')
            out[key]=value
        return out
    def invalid(_): raise ValueError('noninteger number')
    try:
        value=json.loads(raw.decode('utf-8','strict'),object_pairs_hook=pairs,
                         parse_float=invalid,parse_constant=invalid)
        if type(value) is not dict or p.canonical_json(value)!=raw: raise ValueError()
        return value
    except (ValueError,TypeError,UnicodeError,RecursionError):
        raise NegotiationHeld('invalid canonical JSON') from None

def negotiate(db,raw_request,raw_capabilities,*,authenticated_principal,authenticated_worker,admission_guard,_store=None,_inside_transaction=False):
    """Persist selection for exact replay, never claims reservation or custody.

    Identity arguments are trusted adapter outputs, not request values or
    caller-provided HTTP headers. The trusted guard must check freeze and fresh
    worker capability *within this same transaction* on every call, including
    replay; a caller-supplied boolean is insufficient. This experimental hook
    is not the real coordinator freeze/census integration. Never expose this
    primitive directly.
    """
    req=_decode(raw_request,MAX_REQUEST_BYTES)
    cap=_decode(raw_capabilities,MAX_CAPABILITY_BYTES)
    try:
        if set(req)!=REQUEST_FIELDS or set(cap)!=CAP_FIELDS: raise ValueError()
        for key in ('operationId','authorityId','jobId','attemptId','reservationId'):p._uuid(req[key],key)
        for key in ('principalId','nodeId','workerId','workerInstanceId'):p._slug(req[key],key)
        p._integer(req['generation'],'generation');p._environment(req['environment'])
        for key in ('manifestSha256','derivedManifestSha256'):p._hash(req[key],key)
        if (type(authenticated_principal) is not str or req['principalId']!=authenticated_principal
                or type(authenticated_worker) is not dict or set(authenticated_worker)!=set(IDENTITY)
                or any(req[k]!=authenticated_worker[k] or cap[k]!=authenticated_worker[k] for k in IDENTITY)
                or req['profile']!=limits.PROFILE or cap['profile']!=limits.PROFILE):raise ValueError()
        for field,minimum in (('maxSource',limits.MAX_SOURCE),('maxDerived',limits.MAX_DERIVED),
                              ('maxManifestBytes',limits.MAX_MANIFEST_BYTES)):
            if type(cap[field]) is not int or cap[field]<minimum or cap[field]>9007199254740991:raise ValueError()
    except (ValueError,TypeError,KeyError):
        raise NegotiationHeld('identity, capability or profile rejected') from None
    if not callable(admission_guard):raise NegotiationHeld('trusted admission guard required')
    if (db.in_transaction != _inside_transaction or (_inside_transaction and _store is None)
            or db.execute('PRAGMA synchronous').fetchone()[0] not in (2,3)):
        raise NegotiationHeld('exclusive FULL transaction required')
    cap_hash=hashlib.sha256(raw_capabilities).hexdigest()
    response=p.canonical_json(dict(protocol='ltds-direct-profile-selection-candidate-v1',
        operationId=req['operationId'],profile=req['profile'],requestSha256=hashlib.sha256(raw_request).hexdigest(),
        capabilitySha256=cap_hash,state='selected_not_reserved',sourceCustodyAccepted=False,transferAuthorized=False))
    if len(response)>MAX_RESPONSE_BYTES:raise NegotiationHeld('response limit exceeded')
    if not _inside_transaction:db.execute('BEGIN IMMEDIATE')
    try:
        checkpoint='direct_guard_'+uuid.uuid4().hex
        db.execute('SAVEPOINT '+checkpoint)
        # Do not pass a writable SQLite connection to the trust hook. It is
        # still an in-process hook, not authenticated freshness evidence.
        allowed=admission_guard(dict(req),dict(cap)) is True
        try:db.execute('RELEASE SAVEPOINT '+checkpoint)
        except sqlite3.DatabaseError:raise NegotiationHeld('guard changed transaction') from None
        if not allowed or not db.in_transaction:
            raise NegotiationHeld('freeze or capability freshness unverified')
        db.execute('''CREATE TABLE IF NOT EXISTS direct_negotiation_candidate (
            principal TEXT NOT NULL, operation TEXT NOT NULL, request BLOB NOT NULL,
            capability BLOB NOT NULL, response BLOB NOT NULL, PRIMARY KEY(principal,operation))''')
        verify_schema(db)
        old=db.execute('SELECT request,capability,response FROM direct_negotiation_candidate WHERE principal=? AND operation=?',
                       (req['principalId'],req['operationId'])).fetchone()
        if old is None:
            count,used=db.execute('''SELECT count(*),coalesce(sum(length(CAST(principal AS BLOB))+
                length(CAST(operation AS BLOB))+length(request)+length(capability)+length(response)),0)
                FROM direct_negotiation_candidate''').fetchone()
            charge=len(req['principalId'].encode())+len(req['operationId'].encode())+len(raw_request)+len(raw_capabilities)+len(response)
            if count>=MAX_SELECTIONS or used+charge>MAX_STORAGE_BYTES:
                raise NegotiationHeld('negotiation journal capacity reached')
            if _store is not None:
                _store._quota(db,charge)
            db.execute('INSERT INTO direct_negotiation_candidate VALUES(?,?,?,?,?)',
                       (req['principalId'],req['operationId'],raw_request,raw_capabilities,response))
        elif bytes(old[0])!=raw_request or bytes(old[1])!=raw_capabilities or bytes(old[2])!=response:
            raise NegotiationHeld('negotiation conflict; reconciliation required')
        if _store is None:db.commit()
    except BaseException:
        if not _inside_transaction:db.rollback()
        raise
    return response

def negotiate_in_coordinator(store,raw_request,raw_capabilities,*,authenticated_principal,authenticated_worker,admission_guard):
    """Prototype using actual queue freeze operation and shared logical census.

    The guard is still a trust hook, not authenticated network proof. Selection
    is not atomic with a reservation; never expose or deploy this function.
    """
    from .reference_store import ReferenceStore
    if not isinstance(store,ReferenceStore):raise NegotiationHeld('reference store required')
    with store.queue._coordinator_freeze.operation('source-io-admission'), store.queue._db() as db:
        return negotiate(db,raw_request,raw_capabilities,authenticated_principal=authenticated_principal,
            authenticated_worker=authenticated_worker,admission_guard=admission_guard,_store=store)
