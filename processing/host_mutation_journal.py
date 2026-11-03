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

from .node_gate import GateError, _digest

_HASH = re.compile(r'[0-9a-f]{64}\Z')


class MutationHeld(GateError):
    """An already-issued mutation requires independent reconciliation."""


class HostMutationJournal:
    """Journal exact operation identities under a bound, locked NodeGate."""

    def __init__(self, gate, receipt_id):
        self.gate, self.receipt_id = gate, receipt_id
        state, reservation = gate._reservation(receipt_id)
        if reservation['job'] is None:
            raise GateError('guard requires an active bound reservation')
        self.generation = state['generation']
        self.job = dict(reservation['job'])
        self._identity = (receipt_id, str(self.generation), self.job['id'], gate.engine_id)
        self.db = None

    def _current(self, *, allow_completion=False):
        state, reservation = self.gate._reservation(self.receipt_id)
        if (state['generation'] != self.generation or reservation['job'] != self.job
                or (reservation['completionDigest'] is not None and not allow_completion)):
            raise GateError('node reservation changed')
        return state

    def create(self):
        """Initialize once; a crash after the durable marker requires inspection."""
        state = self._current()
        if 'hostMutationJournal' in state:
            raise GateError('guard journal already exists or needs inspection')
        name = 'mutations-' + uuid.uuid4().hex + '.sqlite'
        # An older loader accepts unknown keys on schema 1. Atomically upgrade
        # with the marker so it cannot release or reuse an uncertain node.
        # Keep schema 2 after completion: downgrade needs external control.
        state['schema'] = 2
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
        state = self._current(allow_completion=True)
        marker = state.get('hostMutationJournal')
        if (not isinstance(marker, dict) or set(marker) not in ({'receiptId', 'file'}, {'receiptId', 'file', 'completion'})
                or marker['receiptId'] != self.receipt_id
                or not isinstance(marker['file'], str)
                or not re.fullmatch(r'mutations-[0-9a-f]{32}\.sqlite', marker['file'])):
            raise GateError('guard journal marker missing or invalid')
        completion = marker.get('completion')
        if ((completion is None) != (state['reservation']['completionDigest'] is None)
                or (completion is not None and
                    (not isinstance(completion, dict) or set(completion) != {'engineTask', 'outboxId'}
                     or completion['engineTask'] != self.job['id']
                     or not isinstance(completion['outboxId'], str)
                     or not _HASH.fullmatch(completion['outboxId'])))):
            raise GateError('guard completion marker invalid')
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

    def bind_viewer_job(self, claim):
        """Durably bind a verified claim before issuing the first mutation.

        The gate lock protects this separate journal transaction. Existing
        journals with operations may not be retroactively bound.
        """
        self._ready()
        from . import reference_protocol as protocol
        if type(claim) is not dict or claim.get('engineTaskId') != self.job['id']:
            raise GateError('invalid viewer job binding')
        value = protocol.canonical_json(claim).decode('utf-8')
        digest = protocol.digest(claim)
        with self._transaction():
            if self.db.execute('SELECT 1 FROM operations LIMIT 1').fetchone() is not None:
                raise MutationHeld('cannot bind an issued mutation')
            self.db.execute('CREATE TABLE IF NOT EXISTS viewer_job_binding '
                            '(id INTEGER PRIMARY KEY CHECK(id=1), claim_json TEXT NOT NULL, claim_sha256 TEXT NOT NULL)')
            prior = self.db.execute('SELECT claim_json, claim_sha256 FROM viewer_job_binding WHERE id=1').fetchone()
            if prior is None:
                self.db.execute('INSERT INTO viewer_job_binding VALUES(1,?,?)', (value,digest))
            elif prior != (value,digest):
                raise GateError('viewer job binding differs')

    def verify_viewer_job(self, claim):
        """Verify exact persisted identity; never authorize resume."""
        self._current(allow_completion=True)
        from . import reference_protocol as protocol
        if self.db is None: raise GateError('guard journal not open')
        try:
            row = self.db.execute('SELECT claim_json, claim_sha256 FROM viewer_job_binding WHERE id=1').fetchone()
            expected = (protocol.canonical_json(claim).decode('utf-8'), protocol.digest(claim))
            if row != expected: raise GateError('viewer job binding differs')
        except sqlite3.Error as exc:
            raise GateError('viewer job binding unavailable') from exc

    def has_viewer_job(self):
        """A bound job may never fall back to the unbound mutation route."""
        self._current(allow_completion=True)
        if self.db is None: raise GateError('guard journal not open')
        try:
            table = self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_job_binding'").fetchone()
            if table is None: return False
            rows = self.db.execute('SELECT id FROM viewer_job_binding').fetchall()
            if rows != [(1,)]: raise GateError('viewer job binding invalid')
            return True
        except sqlite3.Error as exc:
            raise GateError('viewer job binding unavailable') from exc

    def checkpoint_completion(self, *, engine_task, outbox_id, payload):
        """Atomically bind durable completion to this journal without releasing it.

        Caller must first independently verify terminal engine identity and persist
        its validated result to the outbox. This checkpoint does not authenticate
        engine or receipt transports and does not exclude non-cooperating writers.
        """
        state = self._current(allow_completion=True)
        if self.db is None: raise GateError('guard journal not open')
        if engine_task != self.job['id'] or not isinstance(outbox_id, str) or not _HASH.fullmatch(outbox_id):
            raise GateError('invalid terminal identity')
        with self._transaction():
            last = self._validate_operations()
            if last is None or last[1:] != ('commit', 'done'):
                raise MutationHeld('completed commit evidence required')
        marker = state.get('hostMutationJournal')
        if not isinstance(marker, dict) or marker.get('receiptId') != self.receipt_id:
            raise GateError('guard journal identity changed')
        completion = {'engineTask': engine_task, 'outboxId': outbox_id}
        digest = _digest(payload)
        if marker.get('completion', completion) != completion:
            raise GateError('guarded completion identity changed')
        if state['reservation']['completionDigest'] not in (None, digest):
            raise GateError('guarded completion differs')
        marker['completion'] = completion
        state['reservation']['completionDigest'] = digest
        self.gate._save(state)

    def acknowledge_completion(self, *, outbox_id, job_ack):
        """Release only after caller verifies an authenticated durable receipt.

        The journal file is retained; only the gate marker and reservation are
        atomically retired. This is not permission to clean result files.
        """
        self.check_reconciliation_ready()
        state = self._current(allow_completion=True)
        marker = state.get('hostMutationJournal')
        expected = {'engineTask': self.job['id'], 'outboxId': outbox_id}
        if not isinstance(marker, dict) or marker.get('completion') != expected:
            raise GateError('guarded completion checkpoint mismatch')
        identity, digest = self.gate._job(state, job_ack), _digest(job_ack)
        if identity != self.job or digest != state['reservation']['completionDigest']:
            raise GateError('completion acknowledgment does not match durable result')
        state['reservation'] = None
        state['lastAcknowledgment'] = {'receiptId': self.receipt_id, 'job': identity,
            'completionDigest': digest, 'generation': state['generation'], 'kind': 'completion'}
        del state['hostMutationJournal']
        self.gate._save(state)

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
        # Invalid rows must never look like permission to send or release.
        previous = None
        for seq, op_id, kind, request, digest, state in self.db.execute(
                'SELECT seq,op_id,kind,request_digest,ack_digest,state FROM operations ORDER BY seq'):
            try:
                valid_id = str(uuid.UUID(op_id)) == op_id
            except (ValueError, TypeError, AttributeError):
                valid_id = False
            valid_order = ((previous is None and seq == 1 and kind == 'init') or
                           (previous is not None and seq == previous[0] + 1
                            and previous[1] != 'commit' and kind in ('upload', 'commit')
                            and previous[2] == 'done'))
            if (not valid_id or not valid_order or not isinstance(request, str)
                    or not _HASH.fullmatch(request)
                    or not ((state == 'pending' and digest is None) or
                            (state == 'done' and isinstance(digest, str) and _HASH.fullmatch(digest)))):
                raise GateError('guard journal malformed; inspection required')
            previous = (seq, kind, state)
        return previous

    def check_reconciliation_ready(self):
        """Check local evidence only; this does not authorize or release a gate."""
        self._current(allow_completion=True)
        if self.db is None: raise GateError('guard journal not open')
        with self._transaction():
            last = self._validate_operations()
            if last is None or last[1:] != ('commit', 'done'):
                raise MutationHeld('completed commit evidence required')

    def contains_operation(self, op_id):
        """Check durable intent under the same gate lock used for mutation."""
        self._ready()
        try:
            if str(uuid.UUID(op_id)) != op_id: raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise GateError('invalid operation identity') from None
        with self._transaction():
            self._validate_operations()
            return self.db.execute('SELECT 1 FROM operations WHERE op_id=?', (op_id,)).fetchone() is not None

    def inspect_operation(self, op_id, kind, request_digest):
        """Read exact durable evidence; absence or disagreement is not proof."""
        self._ready()
        try:
            if str(uuid.UUID(op_id)) != op_id: raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise GateError('invalid operation identity') from None
        if kind not in ('init', 'upload', 'commit') or not isinstance(request_digest, str) or not _HASH.fullmatch(request_digest):
            raise GateError('invalid operation identity')
        with self._transaction():
            self._validate_operations()
            row = self.db.execute('SELECT kind,request_digest,ack_digest,state FROM operations WHERE op_id=?', (op_id,)).fetchone()
            if row is None or row[:2] != (kind, request_digest):
                raise MutationHeld('operation evidence unavailable')
            return {'state': row[3], 'ackDigest': row[2]}

    def require_done(self, op_id, request_digest, ack_digest):
        """Confirm the exact durable operation before releasing its spool copy."""
        self._ready()
        with self._transaction():
            self._validate_operations()
            row = self.db.execute('SELECT kind,request_digest,ack_digest,state FROM operations WHERE op_id=?',
                                  (op_id,)).fetchone()
            if row != ('upload', request_digest, ack_digest, 'done'):
                raise MutationHeld('upload acknowledgment requires inspection')

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
