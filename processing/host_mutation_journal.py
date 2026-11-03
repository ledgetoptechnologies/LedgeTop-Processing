"""Disabled building block for a cooperative host-side NodeODM mutation guard.

The caller must hold the provisioned NodeGate lock and bind its reservation to
the actual job before opening this journal. This module does not authenticate a
caller, proxy HTTP, attest a Docker identity, or prevent access around NodeGate.
Pending mutations are uncertain after any interruption and are never replayed.
"""
import os
from contextlib import contextmanager
from pathlib import Path
import re
import sqlite3
import uuid

from .node_gate import GateError

_HASH = re.compile(r'[0-9a-f]{64}\Z')


class MutationHeld(GateError):
    """An already-issued mutation requires independent reconciliation."""


class HostMutationJournal:
    """Journal exact operation identities under a bound, locked NodeGate."""

    def __init__(self, gate, receipt_id):
        self.gate, self.receipt_id = gate, receipt_id
        state, reservation = gate._reservation(receipt_id)
        if reservation['job'] is None or reservation['completionDigest'] is not None:
            raise GateError('guard requires an active bound reservation')
        self.generation = state['generation']
        self.job = dict(reservation['job'])
        self._identity = (receipt_id, str(self.generation), self.job['id'], gate.engine_id)
        self.db = None

    def _current(self):
        state, reservation = self.gate._reservation(self.receipt_id)
        if (state['generation'] != self.generation or reservation['job'] != self.job
                or reservation['completionDigest'] is not None):
            raise GateError('node reservation changed')
        return state

    def create(self):
        """Initialize once; a crash after the durable marker requires inspection."""
        state = self._current()
        if 'hostMutationJournal' in state:
            raise GateError('guard journal already exists or needs inspection')
        name = 'mutations-' + uuid.uuid4().hex + '.sqlite'
        state['hostMutationJournal'] = {'receiptId': self.receipt_id, 'file': name}
        self.gate._save(state)
        path = self.gate.root / name
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        if os.name != 'nt':
            fd = os.open(self.gate.root, os.O_RDONLY)
            try: os.fsync(fd)
            finally: os.close(fd)
        self._open(path)
        try:
            self.db.execute('CREATE TABLE identity(receipt TEXT NOT NULL, generation TEXT NOT NULL, job TEXT NOT NULL, engine TEXT NOT NULL)')
            self.db.execute('CREATE TABLE operations(seq INTEGER PRIMARY KEY, op_id TEXT NOT NULL UNIQUE, kind TEXT NOT NULL, request_digest TEXT NOT NULL, ack_digest TEXT, state TEXT NOT NULL)')
            self.db.execute('INSERT INTO identity VALUES(?,?,?,?)', self._identity)
            self.db.commit()
        except Exception:
            self.close()
            raise
        return self

    def open(self):
        state = self._current()
        marker = state.get('hostMutationJournal')
        if (not isinstance(marker, dict) or set(marker) != {'receiptId', 'file'}
                or marker['receiptId'] != self.receipt_id
                or not isinstance(marker['file'], str)
                or not re.fullmatch(r'mutations-[0-9a-f]{32}\.sqlite', marker['file'])):
            raise GateError('guard journal marker missing or invalid')
        path = self.gate.root / marker['file']
        if path.is_symlink() or not path.is_file():
            raise GateError('guard journal missing; inspection required')
        self._open(path)
        try:
            rows = self.db.execute('SELECT receipt,generation,job,engine FROM identity').fetchall()
            if rows != [self._identity]: raise GateError('guard journal identity mismatch')
            self.db.execute('SELECT seq,op_id,kind,request_digest,ack_digest,state FROM operations LIMIT 1').fetchall()
        except Exception as error:
            self.close()
            raise GateError('guard journal invalid; inspection required') from error
        return self

    def _open(self, path):
        if self.db is not None: raise GateError('guard journal already open')
        self.db = sqlite3.connect(f'{Path(path).as_uri()}?mode=rw', uri=True)
        self.db.execute('PRAGMA synchronous=FULL')

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None

    def _ready(self):
        self._current()
        if self.db is None: raise GateError('guard journal not open')

    @contextmanager
    def _transaction(self):
        # Serialize reads as well as writes across separate journal connections.
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _validate_operations(self):
        # Invalid acknowledgments must never look like permission to send again.
        for state, digest in self.db.execute('SELECT state,ack_digest FROM operations'):
            if state == 'pending' and digest is None:
                continue
            if state == 'done' and isinstance(digest, str) and _HASH.fullmatch(digest):
                continue
            raise GateError('guard journal malformed; inspection required')

    def begin(self, op_id, kind, request_digest):
        """Record intent before mutation. A done replay returns its ack hash.

        A pending replay raises MutationHeld; it is never an instruction to
        resend. The caller must independently resolve uncertain outcomes.
        """
        self._ready()
        try: valid_id = str(uuid.UUID(op_id)) == op_id
        except (ValueError, TypeError, AttributeError): valid_id = False
        if not valid_id or kind not in ('init', 'upload', 'commit') or not isinstance(request_digest, str) or not _HASH.fullmatch(request_digest):
            raise GateError('invalid operation identity')
        with self._transaction():
            self._validate_operations()
            prior = self.db.execute('SELECT seq,op_id,kind,request_digest,ack_digest,state FROM operations WHERE op_id=?', (op_id,)).fetchone()
            if prior:
                if (prior[2], prior[3]) != (kind, request_digest): raise GateError('operation identity conflict')
                if prior[5] != 'done': raise MutationHeld('mutation outcome uncertain')
                return prior[4]
            if self.db.execute("SELECT 1 FROM operations WHERE state<>'done' LIMIT 1").fetchone():
                raise MutationHeld('earlier mutation outcome uncertain')
            last = self.db.execute('SELECT seq,kind FROM operations ORDER BY seq DESC LIMIT 1').fetchone()
            if last and last[0] >= 50000: raise GateError('guard journal operation limit reached')
            if (last is None and kind != 'init') or (last and (kind == 'init' or last[1] == 'commit')):
                raise GateError('invalid mutation ordering')
            self.db.execute('INSERT INTO operations(op_id,kind,request_digest,state) VALUES(?,?,?,?)', (op_id, kind, request_digest, 'pending'))
        return None

    def acknowledge(self, op_id, ack_digest):
        self._ready()
        if not isinstance(ack_digest, str) or not _HASH.fullmatch(ack_digest):
            raise GateError('invalid acknowledgment digest')
        with self._transaction():
            self._validate_operations()
            row = self.db.execute('SELECT ack_digest,state FROM operations WHERE op_id=?', (op_id,)).fetchone()
            if row is None: raise GateError('unknown mutation')
            if row[1] == 'done':
                if row[0] != ack_digest: raise GateError('acknowledgment conflict')
                return
            if row[1] != 'pending': raise GateError('guard journal invalid')
            self.db.execute('UPDATE operations SET state=?,ack_digest=? WHERE op_id=?', ('done', ack_digest, op_id))
