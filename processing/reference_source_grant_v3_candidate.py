"""Inactive source-grant v3 and accounting model. Not a broker or runtime route.

The caller must authenticate the grant and trusted state independently. This
model neither serves bytes nor authorizes an actual worker or source transfer.
"""
import hashlib
import json
import re
import sqlite3

from . import reference_protocol as p

DOMAIN = b'ltds-source-accounting-v3\x00'
PROTOCOL = 'ltds-source-grant-v3'
ACCOUNT = ('environment','jobId','generation','viewerAttemptId','reservationId','manifestSha256','sourceVersion')
AUTHORITY = ('activationDigest','coordinatorAuthorityId','executionEpoch')
POLICY = ('chunkBytes','requestBudget','byteBudget','attemptBudget')


def _id(value, name):
    if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}', value):
        raise ValueError('invalid ' + name)


def _account(account):
    p._closed(account, ACCOUNT, 'accounting identity')
    p._environment(account['environment'])
    for field in ('jobId','viewerAttemptId','reservationId','sourceVersion'):
        _id(account[field], field)
    p._integer(account['generation'], 'generation')
    p._hash(account['manifestSha256'], 'manifest digest')


def _authority(authority):
    p._closed(authority, AUTHORITY, 'grant authority')
    p._hash(authority['activationDigest'], 'activation digest')
    for field in ('coordinatorAuthorityId','executionEpoch'):
        _id(authority[field], field)


def validate(grant, *, trusted_state, operational_limits, now):
    """Compare claims to independent state; no authentication is performed."""
    p._closed(grant, ('schemaVersion','protocol','accounting','authority','policy','expiresAt','bearer','nonce','objects'), 'source grant')
    if type(grant['schemaVersion']) is not int or grant['schemaVersion'] != 3 or grant['protocol'] != PROTOCOL:
        raise ValueError('unsupported source grant')
    p._closed(trusted_state, ('accounting','authority','policy','objects'), 'trusted worker state')
    if type(now) is not int or type(grant['expiresAt']) is not int or grant['expiresAt'] <= now or grant['expiresAt'] > p.MAX_SAFE_INTEGER:
        raise ValueError('grant expired or invalid')
    for field in ('bearer','nonce'):
        _id(grant[field], field)
    for objects in (grant['objects'], trusted_state['objects']):
        if type(objects) is not list or not objects or len(objects) > p.MAX_OBJECTS:
            raise ValueError('invalid object closure')
        for obj in objects:
            p._closed(obj, ('id','byteSize','sha256'), 'source object')
            _id(obj['id'], 'object ID')
            p._integer(obj['byteSize'], 'object size')
            p._hash(obj['sha256'], 'object digest')
        if len({o['id'] for o in objects}) != len(objects):
            raise ValueError('duplicate source object')
    total_bytes = sum(o['byteSize'] for o in grant['objects'])
    p._integer(total_bytes, 'total source bytes')
    p._closed(operational_limits, (*POLICY, 'totalSourceBytes'), 'operational limits')
    for field in (*POLICY, 'totalSourceBytes'):
        p._integer(operational_limits[field], field)
    for value in (grant, trusted_state):
        _account(value['accounting'])
        _authority(value['authority'])
        p._closed(value['policy'], POLICY, 'source policy')
        for field in POLICY:
            p._integer(value['policy'][field], field)
    if total_bytes > operational_limits['totalSourceBytes']:
        raise ValueError('source exceeds operational limit')
    if any(grant['policy'][field] > operational_limits[field] for field in POLICY):
        raise ValueError('policy exceeds operational limit')
    if not total_bytes <= grant['policy']['byteBudget'] <= total_bytes * grant['policy']['attemptBudget']:
        raise ValueError('byte budget violates source attempt relationship')
    if any(p.canonical_json(grant[key]) != p.canonical_json(trusted_state[key]) for key in ('accounting','authority','policy','objects')):
        raise ValueError('source grant does not match independent trusted state')
    return hashlib.sha256(DOMAIN + p.canonical_json(grant['accounting'])).hexdigest()


class SyntheticRangeLedger:
    """SQLite test model; exact replay is metadata only and serves no bytes."""
    def __init__(self, path, *, operational_limits):
        self.operational_limits = operational_limits
        self.db = sqlite3.connect(path, isolation_level=None, timeout=5)
        self.db.execute('PRAGMA foreign_keys=ON')
        schema = """
          CREATE TABLE lineage(environment TEXT NOT NULL, job_id TEXT NOT NULL,
            generation INTEGER NOT NULL, identity TEXT NOT NULL, PRIMARY KEY(environment,job_id,generation));
          CREATE TABLE accounting(identity TEXT PRIMARY KEY, policy TEXT NOT NULL, objects TEXT NOT NULL,
            requests INTEGER NOT NULL, bytes INTEGER NOT NULL);
          CREATE TABLE attempts(identity TEXT NOT NULL REFERENCES accounting(identity), attempt_id TEXT NOT NULL, PRIMARY KEY(identity,attempt_id));
          CREATE TABLE charges(identity TEXT NOT NULL REFERENCES accounting(identity),
            request_id TEXT NOT NULL, attempt_id TEXT NOT NULL, object_id TEXT NOT NULL, offset INTEGER NOT NULL, length INTEGER NOT NULL,
            PRIMARY KEY(identity,request_id));
        """
        names = {'lineage', 'accounting', 'attempts', 'charges'}
        try:
            self.db.execute('BEGIN IMMEDIATE')
            actual_names = {r[0] for r in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            present = names & actual_names
            if present and present != names:
                raise ValueError('partial ledger schema requires reconciliation')
            if not present:
                for statement in schema.split(';'):
                    if statement.strip(): self.db.execute(statement)
            expected = sqlite3.connect(':memory:')
            try:
                expected.executescript(schema)
                query = "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN ('table','index','trigger') AND tbl_name IN ('lineage','accounting','attempts','charges') ORDER BY type,name"
                normalize = lambda rows: [(t,n,table,' '.join(sql.split()) if sql else None) for t,n,table,sql in rows]
                if normalize(self.db.execute(query)) != normalize(expected.execute(query)):
                    raise ValueError('incompatible ledger schema')
            finally:
                expected.close()
            self._check_integrity()
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            self.db.close()
            raise

    def _check_integrity(self):
        if self.db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok' or self.db.execute('PRAGMA foreign_key_check').fetchone():
            raise ValueError('ledger integrity check failed')
        if self.db.execute("""SELECT 1 FROM accounting a WHERE
            (SELECT COUNT(*) FROM lineage l WHERE l.identity=a.identity)!=1 OR
            a.requests != (SELECT COUNT(*) FROM charges c WHERE c.identity=a.identity) OR
            a.bytes != COALESCE((SELECT SUM(c.length) FROM charges c WHERE c.identity=a.identity),0)
            LIMIT 1""").fetchone():
            raise ValueError('ledger lineage or accounting mismatch')
        if self.db.execute("""SELECT 1 FROM lineage l LEFT JOIN accounting a ON a.identity=l.identity
            WHERE a.identity IS NULL LIMIT 1""").fetchone():
            raise ValueError('orphan ledger lineage')
        if self.db.execute("""SELECT 1 FROM charges c LEFT JOIN attempts t ON
            t.identity=c.identity AND t.attempt_id=c.attempt_id WHERE t.identity IS NULL LIMIT 1""").fetchone():
            raise ValueError('orphan transfer attempt')
        if self.db.execute("""SELECT 1 FROM attempts t LEFT JOIN charges c ON
            t.identity=c.identity AND t.attempt_id=c.attempt_id WHERE c.identity IS NULL LIMIT 1""").fetchone():
            raise ValueError('attempt without a charge')

    def close(self):
        self.db.close()

    def charge(self, grant, *, trusted_state, now, request_id, attempt_id, object_id, offset, length):
        identity = validate(grant, trusted_state=trusted_state, operational_limits=self.operational_limits, now=now)
        for value, name in ((request_id,'request ID'),(attempt_id,'attempt ID'),(object_id,'object ID')):
            _id(value, name)
        p._integer(offset, 'offset', allow_zero=True)
        p._integer(length, 'length')
        if offset + length > p.MAX_SAFE_INTEGER:
            raise ValueError('range exceeds safe integer')
        obj = next((o for o in grant['objects'] if o['id'] == object_id), None)
        if obj is None or length > grant['policy']['chunkBytes'] or offset + length > obj['byteSize']:
            raise ValueError('range outside object closure or chunk budget')
        policy = p.canonical_json(grant['policy']).decode('utf-8')
        objects = p.canonical_json(grant['objects']).decode('utf-8')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self._check_integrity()
            account = grant['accounting']
            parent = (account['environment'],account['jobId'],account['generation'])
            lineage = self.db.execute('SELECT identity FROM lineage WHERE environment=? AND job_id=? AND generation=?', parent).fetchone()
            if lineage is not None and lineage[0] != identity:
                raise ValueError('successor identity requires lineage migration')
            if lineage is None:
                self.db.execute('INSERT INTO lineage VALUES(?,?,?,?)', (*parent,identity))
            row = self.db.execute('SELECT policy,objects,requests,bytes FROM accounting WHERE identity=?', (identity,)).fetchone()
            if row is None:
                self.db.execute('INSERT INTO accounting VALUES(?,?,?,0,0)', (identity,policy,objects))
                row = (policy,objects,0,0)
            if row[0] != policy or row[1] != objects:
                raise ValueError('accounting policy changed')
            old = self.db.execute('SELECT attempt_id,object_id,offset,length FROM charges WHERE identity=? AND request_id=?', (identity,request_id)).fetchone()
            if old:
                if old != (attempt_id,object_id,offset,length):
                    raise ValueError('request identity changed')
                self.db.execute('COMMIT')
                return {'identity': identity, 'created': False, 'requests': row[2], 'bytes': row[3]}
            attempt = self.db.execute('SELECT 1 FROM attempts WHERE identity=? AND attempt_id=?', (identity,attempt_id)).fetchone()
            count = self.db.execute('SELECT COUNT(*) FROM attempts WHERE identity=?', (identity,)).fetchone()[0]
            if attempt is None and count >= grant['policy']['attemptBudget']:
                raise ValueError('source attempt budget exhausted')
            if row[2] >= grant['policy']['requestBudget'] or length > grant['policy']['byteBudget'] - row[3]:
                raise ValueError('source budget exhausted')
            if attempt is None:
                self.db.execute('INSERT INTO attempts VALUES(?,?)', (identity,attempt_id))
            self.db.execute('INSERT INTO charges VALUES(?,?,?,?,?,?)', (identity,request_id,attempt_id,object_id,offset,length))
            self.db.execute('UPDATE accounting SET requests=requests+1,bytes=bytes+? WHERE identity=?', (length,identity))
            self.db.execute('COMMIT')
            return {'identity':identity,'created':True,'requests':row[2]+1,'bytes':row[3]+length}
        except BaseException:
            self.db.execute('ROLLBACK')
            raise
