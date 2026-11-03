"""Unmounted Viewer v2 source-range reconciliation; never authorizes IO.

Caller must obtain ``binding`` from an authenticated prepared attempt, not
from the grant or client JSON. Broker authorization and epoch fencing remain
external and unimplemented. This is a synthetic accounting model, not a ledger
migration or permission to fetch source data.
"""
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from . import reference_protocol as p

CHUNK = 8 * 1024 * 1024
IDENTITY = ('environment', 'generation', 'jobId', 'manifestSha256', 'reservationId', 'sourceVersion', 'viewerAttemptId')
TRANSFER = ('protocol','viewerAttemptId','reservationId','generation','expiresAt','manifestSha256','sourceVersion','objectIds','chunkBytes','requestBudget','byteBudget','nonce','attemptBudget')
GRANT = (*TRANSFER, 'jobId', 'environment', 'token')
POLICY = ('chunkBytes','requestBudget','byteBudget','attemptBudget')


def _identity(value):
    p._closed(value, IDENTITY, 'authenticated attempt binding')
    p._environment(value['environment'])
    p._integer(value['generation'], 'generation')
    for key in ('jobId','reservationId'):
        p._slug(value[key], key)
    p._uuid(value['viewerAttemptId'], 'Viewer attempt')
    if not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', value['viewerAttemptId']):
        raise ValueError('invalid Viewer attempt')
    p._hash(value['manifestSha256'], 'manifest hash')
    if type(value['sourceVersion']) is not str or not re.fullmatch(r'[\x21-\x7e]{1,256}', value['sourceVersion']):
        raise ValueError('invalid source version')
    return value


def _timestamp(value):
    if type(value) is not str or not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z', value):
        raise ValueError('invalid timestamp')
    try:
        parsed = datetime.fromisoformat(value[:-1] + '+00:00')
        if parsed.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z') != value:
            raise ValueError()
        return parsed.timestamp()
    except (ValueError, OverflowError):
        raise ValueError('invalid timestamp') from None


def reconcile(*, binding, transfer, grant, objects, now):
    """Check a v2 grant against independent binding; returns non-secret metadata.

    No signatures, bearer possession, current epoch, or source object bytes are
    verified. Success is never permission to issue HTTP or mutate a worker.
    """
    _identity(binding)
    p._closed(transfer, TRANSFER, 'Viewer source transfer')
    p._closed(grant, GRANT, 'Viewer source grant')
    if transfer['protocol'] != 'source-range-v2' or grant['protocol'] != 'source-range-v2':
        raise ValueError('unsupported source protocol')
    if any(type(transfer[k]) is not type(grant[k]) or transfer[k] != grant[k]
           for k in TRANSFER if k != 'expiresAt'):
        raise ValueError('transfer binding mismatch')
    if any(type(grant[k]) is not type(binding[k]) or grant[k] != binding[k] for k in IDENTITY):
        raise ValueError('authenticated attempt binding mismatch')
    p._slug(transfer['nonce'], 'nonce')
    if type(grant['token']) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{43}', grant['token']):
        raise ValueError('invalid bearer')
    if type(now) not in (int, float) or isinstance(now, bool) or not 0 <= now <= p.MAX_SAFE_INTEGER:
        raise ValueError('invalid clock')
    expiry, deadline = _timestamp(grant['expiresAt']), _timestamp(transfer['expiresAt'])
    if not now < expiry <= deadline:
        raise ValueError('grant expiry mismatch')
    for key in POLICY:
        p._integer(transfer[key], key)
    if transfer['chunkBytes'] != CHUNK:
        raise ValueError('noncanonical chunk size')
    if type(objects) is not list or not objects or len(objects) > p.MAX_OBJECTS:
        raise ValueError('invalid object closure')
    for obj in objects:
        p._closed(obj, ('id','byteSize','sha256'), 'object')
        p._slug(obj['id'], 'object')
        p._integer(obj['byteSize'], 'object size', allow_zero=True)
        p._hash(obj['sha256'], 'object hash')
    ids = [x['id'] for x in objects]
    if len(set(ids)) != len(ids) or transfer['objectIds'] != ids:
        raise ValueError('object closure mismatch')
    total = sum(x['byteSize'] for x in objects)
    minimum = sum((x['byteSize'] + CHUNK - 1) // CHUNK for x in objects)
    if (not 1 <= total <= p.MAX_SAFE_INTEGER or not minimum
            or transfer['requestBudget'] < minimum or transfer['attemptBudget'] < minimum
            or transfer['byteBudget'] < total):
        raise ValueError('invalid transfer budget')
    return {'identity': dict(binding), 'policy': {k: transfer[k] for k in POLICY},
            'objects': [dict(x) for x in objects]}


def canonical_identity(identity):
    _identity(identity)
    # All fields are ASCII; sorted compact JSON matches Viewer canonicalIdentity.
    return json.dumps(identity, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def _contract(value):
    p._closed(value, ('identity','policy','objects'), 'reconciled contract')
    _identity(value['identity'])
    policy, objects = value['policy'], value['objects']
    p._closed(policy, POLICY, 'policy')
    for key in POLICY: p._integer(policy[key], key)
    if policy['chunkBytes'] != CHUNK or type(objects) is not list or not objects or len(objects) > p.MAX_OBJECTS:
        raise ValueError('invalid closure')
    for obj in objects:
        p._closed(obj, ('id','byteSize','sha256'), 'object')
        p._slug(obj['id'], 'object'); p._integer(obj['byteSize'], 'size', allow_zero=True); p._hash(obj['sha256'], 'hash')
    if len({obj['id'] for obj in objects}) != len(objects): raise ValueError('duplicate object')
    total=sum(obj['byteSize'] for obj in objects)
    minimum=sum((obj['byteSize']+CHUNK-1)//CHUNK for obj in objects)
    if (not 1 <= total <= p.MAX_SAFE_INTEGER or not minimum or policy['byteBudget'] < total
            or policy['requestBudget'] < minimum or policy['attemptBudget'] < minimum):
        raise ValueError('invalid budget')


def request_id(identity, obj, start, length):
    encoded = canonical_identity(identity)
    p._closed(obj, ('id','byteSize','sha256'), 'object')
    p._slug(obj['id'], 'object'); p._integer(obj['byteSize'], 'size', allow_zero=True); p._hash(obj['sha256'], 'hash')
    p._integer(start, 'offset', allow_zero=True); p._integer(length, 'length')
    if start % CHUNK or start >= obj['byteSize'] or length != min(CHUNK, obj['byteSize'] - start):
        raise ValueError('noncanonical chunk')
    wire = json.dumps([encoded, obj['id'], obj['byteSize'], obj['sha256'], start, length],
                      separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')
    return hashlib.sha256(wire).hexdigest()


class SyntheticViewerV2Ledger:
    """Synthetic v2 retry accounting; intentionally incompatible with v3."""
    SCHEMA = """
      CREATE TABLE identities (parent TEXT PRIMARY KEY, identity_json TEXT NOT NULL, policy_json TEXT NOT NULL, objects_json TEXT NOT NULL, requests INTEGER NOT NULL, bytes INTEGER NOT NULL, attempts INTEGER NOT NULL);
      CREATE TABLE ranges (parent TEXT NOT NULL REFERENCES identities(parent), request_id TEXT NOT NULL, object_id TEXT NOT NULL, start INTEGER NOT NULL, length INTEGER NOT NULL, PRIMARY KEY(parent,request_id));
      CREATE TABLE http_attempts (parent TEXT NOT NULL, sequence INTEGER NOT NULL, request_id TEXT NOT NULL, PRIMARY KEY(parent,sequence), FOREIGN KEY(parent,request_id) REFERENCES ranges(parent,request_id));
    """
    def __init__(self, path):
        self.db = sqlite3.connect(path, isolation_level=None, timeout=5)
        self.db.execute('PRAGMA foreign_keys=ON')
        try:
            self.db.execute('BEGIN IMMEDIATE')
            present = {r[0] for r in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if present and not {'identities','ranges','http_attempts'} <= present:
                raise ValueError('unrecognized ledger')
            if not present:
                for stmt in self.SCHEMA.split(';'):
                    if stmt.strip(): self.db.execute(stmt)
            expected=sqlite3.connect(':memory:')
            try:
                expected.executescript(self.SCHEMA)
                query="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN ('table','index','trigger') AND tbl_name IN ('identities','ranges','http_attempts') ORDER BY type,name"
                norm=lambda rows:[(t,n,b,' '.join(s.split()) if s else None) for t,n,b,s in rows]
                if norm(self.db.execute(query)) != norm(expected.execute(query)):
                    raise ValueError('incompatible ledger schema')
            finally: expected.close()
            self._check(); self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK'); self.db.close(); raise

    def _check(self):
        if self.db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok' or self.db.execute('PRAGMA foreign_key_check').fetchone():
            raise ValueError('ledger corrupt')
        for parent,ij,pj,oj,requests,byte_count,attempts in self.db.execute('SELECT * FROM identities'):
            identity=json.loads(ij);policy=json.loads(pj);objects=json.loads(oj)
            _contract({'identity':identity,'policy':policy,'objects':objects})
            if (canonical_identity(identity)!=ij or parent != p.canonical_json([identity['environment'],identity['jobId'],identity['generation']]).decode()
                or p.canonical_json(policy).decode()!=pj or p.canonical_json(objects).decode()!=oj):
                raise ValueError('ledger binding corrupt')
            for key in POLICY: p._integer(policy[key], key)
            ranges=self.db.execute('SELECT request_id,object_id,start,length FROM ranges WHERE parent=?',(parent,)).fetchall()
            attempt_rows=self.db.execute('SELECT sequence,request_id FROM http_attempts WHERE parent=? ORDER BY sequence',(parent,)).fetchall()
            if requests!=len(ranges) or byte_count!=sum(x[3] for x in ranges) or not requests<=attempts<=policy['attemptBudget'] or requests>policy['requestBudget'] or byte_count>policy['byteBudget']:
                raise ValueError('ledger totals corrupt')
            if (len(attempt_rows)!=attempts or any(seq!=index for index,(seq,_) in enumerate(attempt_rows,1))
                    or {x[0] for x in ranges}!={x[1] for x in attempt_rows}):
                raise ValueError('ledger attempt journal corrupt')
            for rid,oid,start,length in ranges:
                matches=[o for o in objects if o.get('id')==oid]
                if len(matches)!=1 or request_id(identity,matches[0],start,length)!=rid:
                    raise ValueError('ledger range corrupt')

    def close(self): self.db.close()

    def record_attempt(self, contract, *, object_id, start, length, claimed_request_id):
        _contract(contract)
        identity=contract['identity']; ij=canonical_identity(identity)
        matches=[o for o in contract['objects'] if o.get('id')==object_id]
        if len(matches)!=1: raise ValueError('unknown object')
        rid=request_id(identity,matches[0],start,length)
        if claimed_request_id!=rid: raise ValueError('request identity mismatch')
        parent=p.canonical_json([identity['environment'],identity['jobId'],identity['generation']]).decode()
        pj=p.canonical_json(contract['policy']).decode();oj=p.canonical_json(contract['objects']).decode()
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self._check()
            row=self.db.execute('SELECT identity_json,policy_json,objects_json,requests,bytes,attempts FROM identities WHERE parent=?',(parent,)).fetchone()
            if row is None:
                self.db.execute('INSERT INTO identities VALUES(?,?,?,?,0,0,0)',(parent,ij,pj,oj)); row=(ij,pj,oj,0,0,0)
            if row[:3]!=(ij,pj,oj): raise ValueError('successor identity requires reconciliation')
            old=self.db.execute('SELECT object_id,start,length FROM ranges WHERE parent=? AND request_id=?',(parent,rid)).fetchone()
            if old and old!=(object_id,start,length): raise ValueError('range identity changed')
            policy=contract['policy'];created=old is None
            if row[5]>=policy['attemptBudget'] or (created and (row[3]>=policy['requestBudget'] or length>policy['byteBudget']-row[4])):
                raise ValueError('source budget exhausted')
            if created:self.db.execute('INSERT INTO ranges VALUES(?,?,?,?,?)',(parent,rid,object_id,start,length))
            self.db.execute('INSERT INTO http_attempts VALUES(?,?,?)',(parent,row[5]+1,rid))
            self.db.execute('UPDATE identities SET requests=requests+?,bytes=bytes+?,attempts=attempts+1 WHERE parent=?',(int(created),length if created else 0,parent))
            self.db.execute('COMMIT')
            return {'created':created,'requests':row[3]+int(created),'bytes':row[4]+(length if created else 0),'attempts':row[5]+1}
        except BaseException:self.db.execute('ROLLBACK');raise
