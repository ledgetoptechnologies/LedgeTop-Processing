"""Experimental input progress journal. Not wired into the worker runtime.

The caller owns the SQLite connection and must validate the manifest, verify
file bytes, and durably record the absolute scratch target before completion.
This primitive does not implement grants, filesystem custody, migration, or a
fence against older worker binaries. Data-version checks conservatively detect
commits through other SQLite connections while this connection stays open;
they do not detect changes through the caller-owned connection, file replacement,
database restore, or mutations outside SQLite. The caller must exclusively own
the connection and establish a separate worker/storage fence.
"""
import json
import sqlite3

from . import reference_protocol as p
from .reference_input_budget_v2 import MAX_OBJECTS


class InputJournalError(ValueError):
    pass


def _json(value):
    return p.canonical_json(value).decode('utf-8')


class InputJournal:
    def __init__(self, db):
        if not isinstance(db, sqlite3.Connection) or db.in_transaction:
            raise InputJournalError('idle SQLite connection required')
        self.db = db
        self._verified = {}
        with db:
            db.execute('''CREATE TABLE IF NOT EXISTS input_journal_jobs (
                job TEXT PRIMARY KEY, binding TEXT NOT NULL, next_ordinal INTEGER NOT NULL,
                allocated INTEGER NOT NULL, object_count INTEGER NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS input_journal_objects (
                job TEXT NOT NULL, ordinal INTEGER NOT NULL, object_id TEXT NOT NULL,
                size INTEGER NOT NULL, sha256 TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                done INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(job, ordinal), UNIQUE(job, object_id))''')

    def seed(self, binding, objects):
        """Seed once from a validated manifest; exact binding replay is idempotent."""
        if self.db.in_transaction:
            raise InputJournalError('idle SQLite connection required')
        try:
            p._closed(binding, ('jobId', 'environment', 'generation', 'manifestSha256', 'objectsSha256', 'maxAttempts'), 'input binding')
            p._uuid(binding['jobId'], 'jobId'); p._environment(binding['environment'])
            p._integer(binding['generation'], 'generation')
            p._require(type(binding['maxAttempts']) is int and 1 <= binding['maxAttempts'] <= 100, 'invalid attempt limit')
            p._hash(binding['manifestSha256'], 'manifestSha256'); p._hash(binding['objectsSha256'], 'objectsSha256')
            p._require(type(objects) is list and 0 < len(objects) <= MAX_OBJECTS, 'invalid input count')
            total = 0
            for obj in objects:
                p._closed(obj, ('id', 'byteSize', 'sha256'), 'input object')
                p._slug(obj['id'], 'object id'); p._integer(obj['byteSize'], 'object size', allow_zero=True)
                p._hash(obj['sha256'], 'object hash'); total += obj['byteSize']
            p._integer(total, 'total bytes', allow_zero=True)
            p._require(len({o['id'] for o in objects}) == len(objects), 'duplicate object')
            p._require(p.digest(objects) == binding['objectsSha256'], 'object digest mismatch')
        except (ValueError, TypeError, KeyError):
            raise InputJournalError('invalid input binding') from None
        job = binding['jobId']; encoded = _json(binding)
        self._verified.pop(job, None)
        try:
            self.db.execute('BEGIN IMMEDIATE')
            version = self.db.execute('PRAGMA data_version').fetchone()[0]
            row = self.db.execute('SELECT binding,next_ordinal,allocated,object_count FROM input_journal_jobs WHERE job=?', (job,)).fetchone()
            if row is not None:
                if row[0] != encoded:
                    raise InputJournalError('input binding conflict')
                self._verify_existing(job, row, objects)
                self.db.commit()
                self._verified[job] = (encoded, version)
                return False
            self.db.execute('INSERT INTO input_journal_jobs VALUES(?,?,0,0,?)', (job, encoded, len(objects)))
            self.db.executemany('INSERT INTO input_journal_objects(job,ordinal,object_id,size,sha256) VALUES(?,?,?,?,?)',
                ((job, n, obj['id'], obj['byteSize'], obj['sha256']) for n, obj in enumerate(objects)))
            self.db.commit()
            self._verified[job] = (encoded, version)
            return True
        except Exception:
            self.db.rollback()
            raise

    def _verify_existing(self, job, state, objects):
        if (type(state[1]) is not int or not 0 <= state[1] <= len(objects)
                or state[3] != len(objects) or type(state[2]) is not int or state[2] < 0):
            raise InputJournalError('input journal inconsistent')
        maximum = json.loads(state[0])['maxAttempts']
        rows = self.db.execute('SELECT ordinal,object_id,size,sha256,attempts,done FROM input_journal_objects WHERE job=? ORDER BY ordinal', (job,))
        count = 0; allocated = 0
        for count, row in enumerate(rows, 1):
            index = count - 1
            if index >= len(objects):
                raise InputJournalError('input journal inconsistent')
            obj = objects[index]
            if (tuple(row[:4]) != (index, obj['id'], obj['byteSize'], obj['sha256'])
                    or type(row[4]) is not int or not 0 <= row[4] <= maximum
                    or row[5] != int(index < state[1])):
                raise InputJournalError('input journal inconsistent')
            if row[5]: allocated += row[2]
        if count != len(objects) or allocated != state[2]:
            raise InputJournalError('input journal inconsistent')

    def current(self, binding):
        if self.db.in_transaction:
            raise InputJournalError('idle SQLite connection required')
        try:
            self.db.execute('BEGIN IMMEDIATE')
            result = self._current(binding)
            self.db.commit()
            return result
        except Exception:
            self.db.rollback()
            raise

    def _current(self, binding):
        self._require_verified(binding)
        row = self.db.execute('SELECT binding,next_ordinal,allocated,object_count FROM input_journal_jobs WHERE job=?', (binding['jobId'],)).fetchone()
        if row is None or row[0] != _json(binding):
            raise InputJournalError('input binding conflict')
        if (any(type(value) is not int for value in row[1:]) or not 0 <= row[1] <= row[3]
                or not 0 <= row[2] <= p.MAX_SAFE_INTEGER or not 1 <= row[3] <= MAX_OBJECTS):
            raise InputJournalError('input journal inconsistent')
        if row[1] == row[3]:
            return None
        obj = self.db.execute('SELECT object_id,size,sha256,attempts,done FROM input_journal_objects WHERE job=? AND ordinal=?', (binding['jobId'],row[1])).fetchone()
        if (obj is None or obj[4] != 0 or type(obj[3]) is not int
                or not 0 <= obj[3] <= binding['maxAttempts'] or type(obj[1]) is not int
                or not 0 <= obj[1] <= p.MAX_SAFE_INTEGER - row[2]):
            raise InputJournalError('input journal inconsistent')
        return dict(ordinal=row[1], objectId=obj[0], byteSize=obj[1], sha256=obj[2], attempts=obj[3], allocatedBefore=row[2], target=row[2]+obj[1])

    def _require_verified(self, binding):
        expected = self._verified.get(binding['jobId'])
        version = self.db.execute('PRAGMA data_version').fetchone()[0]
        if expected != (_json(binding), version):
            raise InputJournalError('input journal requires validated open')

    def attempt(self, binding, ordinal):
        if self.db.in_transaction:
            raise InputJournalError('idle SQLite connection required')
        try:
            self.db.execute('BEGIN IMMEDIATE')
            item = self._current(binding)
            if item is None or item['ordinal'] != ordinal or item['attempts'] >= binding['maxAttempts']:
                raise InputJournalError('input attempt unavailable')
            self.db.execute('UPDATE input_journal_objects SET attempts=attempts+1 WHERE job=? AND ordinal=?', (binding['jobId'],ordinal))
            self.db.commit()
            return item['attempts'] + 1
        except Exception:
            self.db.rollback()
            raise

    def complete(self, binding, ordinal, object_id, size, sha256, target):
        """Record a caller-verified object and already-applied absolute target.

        Exact completion replay is idempotent; this does not itself verify bytes
        or inspect the separate scratch ledger.
        """
        if self.db.in_transaction:
            raise InputJournalError('idle SQLite connection required')
        try:
            self.db.execute('BEGIN IMMEDIATE')
            self._require_verified(binding)
            job = self.db.execute('SELECT binding,next_ordinal,allocated FROM input_journal_jobs WHERE job=?', (binding['jobId'],)).fetchone()
            row = self.db.execute('SELECT object_id,size,sha256,done,attempts FROM input_journal_objects WHERE job=? AND ordinal=?', (binding['jobId'],ordinal)).fetchone()
            if job is None or job[0] != _json(binding) or row is None or tuple(row[:3]) != (object_id,size,sha256):
                raise InputJournalError('input identity conflict')
            if row[3] == 1:
                if ordinal != job[1] - 1 or target != job[2]:
                    raise InputJournalError('input replay conflict')
                self.db.commit(); return False
            if row[3] != 0 or type(row[4]) is not int or not 1 <= row[4] <= binding['maxAttempts'] or ordinal != job[1] or target != job[2] + size or target > p.MAX_SAFE_INTEGER:
                raise InputJournalError('input target conflict')
            self.db.execute('UPDATE input_journal_objects SET done=1 WHERE job=? AND ordinal=?', (binding['jobId'],ordinal))
            self.db.execute('UPDATE input_journal_jobs SET next_ordinal=next_ordinal+1, allocated=? WHERE job=?', (target,binding['jobId']))
            self.db.commit(); return True
        except Exception:
            self.db.rollback()
            raise
