"""Unwired, bounded coordinator profile selection and replay primitive.

The caller MUST authenticate the Viewer principal and worker capability over
trusted transports and provide a private durable SQLite database. This module
does not implement network authentication, worker admission, reservation,
transfer authorization, custody or output acceptance.
"""
import hashlib
import json
from . import direct_limits as limits, reference_protocol as p

MAX_REQUEST_BYTES = 4096
MAX_CAPABILITY_BYTES = 4096
MAX_RESPONSE_BYTES = 4096
MAX_JSON_DEPTH = 4
REQUEST_FIELDS = frozenset(('operationId','principalId','authorityId','nodeId','workerId',
    'workerInstanceId','jobId','attemptId','generation','environment','manifestSha256',
    'derivedManifestSha256','profile'))
CAP_FIELDS = frozenset(('authorityId','nodeId','workerId','workerInstanceId','profile',
    'maxSource','maxDerived','maxManifestBytes'))
IDENTITY = ('authorityId','nodeId','workerId','workerInstanceId')

class NegotiationHeld(ValueError): pass

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

def negotiate(db,raw_request,raw_capabilities,*,authenticated_principal,authenticated_worker):
    """Persist selection for exact replay, never claims reservation or custody.

    Identity arguments are trusted adapter outputs, not request values or
    caller-provided HTTP headers. Never expose this primitive directly.
    """
    req=_decode(raw_request,MAX_REQUEST_BYTES)
    cap=_decode(raw_capabilities,MAX_CAPABILITY_BYTES)
    try:
        if set(req)!=REQUEST_FIELDS or set(cap)!=CAP_FIELDS: raise ValueError()
        for key in ('operationId','authorityId','jobId','attemptId'):p._uuid(req[key],key)
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
    if db.in_transaction or db.execute('PRAGMA synchronous').fetchone()[0] not in (2,3):
        raise NegotiationHeld('exclusive FULL transaction required')
    cap_hash=hashlib.sha256(raw_capabilities).hexdigest()
    response=p.canonical_json(dict(protocol='ltds-direct-profile-selection-candidate-v1',
        operationId=req['operationId'],profile=req['profile'],requestSha256=hashlib.sha256(raw_request).hexdigest(),
        capabilitySha256=cap_hash,state='selected_not_reserved',sourceCustodyAccepted=False,transferAuthorized=False))
    if len(response)>MAX_RESPONSE_BYTES:raise NegotiationHeld('response limit exceeded')
    db.execute('BEGIN IMMEDIATE')
    try:
        db.execute('''CREATE TABLE IF NOT EXISTS direct_negotiation_candidate (
            principal TEXT NOT NULL, operation TEXT NOT NULL, request BLOB NOT NULL,
            capability BLOB NOT NULL, response BLOB NOT NULL, PRIMARY KEY(principal,operation))''')
        old=db.execute('SELECT request,capability,response FROM direct_negotiation_candidate WHERE principal=? AND operation=?',
                       (req['principalId'],req['operationId'])).fetchone()
        if old is None:
            db.execute('INSERT INTO direct_negotiation_candidate VALUES(?,?,?,?,?)',
                       (req['principalId'],req['operationId'],raw_request,raw_capabilities,response))
        elif bytes(old[0])!=raw_request or bytes(old[1])!=raw_capabilities or bytes(old[2])!=response:
            raise NegotiationHeld('negotiation conflict; reconciliation required')
        db.commit()
    except BaseException:
        db.rollback();raise
    return response
