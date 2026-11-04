"""Coordinator-local admission barrier; does not fence direct engine access."""
from contextlib import contextmanager
import threading
import uuid
from .maintenance import Conflict


class AdmissionClosed(Exception):
    pass


class AdmissionDrain:
    _DDL = 'CREATE TABLE coordinator_admission (singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority_id TEXT NOT NULL, closed INTEGER NOT NULL CHECK(closed IN (0,1)), generation INTEGER NOT NULL CHECK(generation>=0))'
    _OPS_DDL = 'CREATE TABLE coordinator_admission_operations (request_id TEXT PRIMARY KEY, authority_id TEXT NOT NULL, expected_generation INTEGER NOT NULL, expected_closed INTEGER NOT NULL, closed INTEGER NOT NULL, generation INTEGER NOT NULL)'

    def __init__(self, queue):
        self.queue = queue
        self._lock = threading.RLock()
        self._transition_lock = threading.Lock()
        self._closing = threading.Event()
        self._background = 0
        self._background_idle = threading.Condition()
        with queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='coordinator_admission'").fetchone()
            operations = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='coordinator_admission_operations'").fetchone()
            if existing is None:
                if operations is not None: raise AdmissionClosed()
                db.execute(self._DDL)
                db.execute(self._OPS_DDL)
                db.execute('INSERT INTO coordinator_admission VALUES(1,?,0,0)', (queue.authority_id,))
            else:
                if existing[0] != self._DDL or operations is None or operations[0] != self._OPS_DDL or db.execute('SELECT COUNT(*) FROM coordinator_admission').fetchone()[0] != 1:
                    raise AdmissionClosed()
                self._closed(db)

    def _closed(self, db):
        row = db.execute('SELECT authority_id,closed,generation FROM coordinator_admission WHERE singleton=1').fetchone()
        if row is None or row[0] != self.queue.authority_id or row[1] not in (0, 1) or type(row[2]) is not int or not 0 <= row[2] <= 2**53-1:
            raise AdmissionClosed()
        return bool(row[1])

    @contextmanager
    def admit(self):
        # Hold the barrier through all admission side effects, including file
        # upload; closing waits until prior admitted requests have returned.
        if self._closing.is_set(): raise AdmissionClosed()
        with self._lock:
            if self._closing.is_set(): raise AdmissionClosed()
            with self.queue._db() as db:
                if self._closed(db): raise AdmissionClosed()
            yield

    @contextmanager
    def background(self):
        """Writes nothing admitted must wait for, and a close waits out.

        ``admit`` serializes its holders, which a transfer of many minutes must
        not do to uploads. This holder is only counted: closing announces itself
        on the yielded event, the holder stops at its next safe point, and the
        durable close, and so any census after it, follows once the count is zero.
        Never taken with ``_lock``, which a long admitted request may hold.
        """
        with self._background_idle:
            if self._closing.is_set(): raise AdmissionClosed()
            with self.queue._db() as db:
                if self._closed(db): raise AdmissionClosed()
            self._background += 1
        try:
            yield self._closing
        finally:
            with self._background_idle:
                self._background -= 1
                self._background_idle.notify_all()

    def set_closed(self, value, *, request_id, expected_generation, expected_closed, expected_authority):
        if expected_authority != self.queue.authority_id:
            raise Conflict('admission authority changed')
        if type(value) is not bool or type(expected_closed) is not bool or type(expected_generation) is not int or not 0 <= expected_generation < 2**53-1:
            raise ValueError('exact expected admission state required')
        try:
            if type(request_id) is not str or str(uuid.UUID(request_id)) != request_id: raise ValueError()
        except (ValueError, AttributeError): raise ValueError('canonical request UUID required') from None
        # Serialize state transitions while announcing a close before waiting
        # on a long-lived upload/download. New admissions fail immediately;
        # the durable close receipt is issued only after active work exits.
        with self._transition_lock:
            if value:self._closing.set()
            try:
                if value:
                    with self._background_idle:
                        while self._background: self._background_idle.wait()
                return self._persist_transition(value,request_id,expected_generation,expected_closed)
            finally:
                if value:self._closing.clear()

    def _persist_transition(self, value, request_id, expected_generation, expected_closed):
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            current = self._closed(db)
            generation = db.execute('SELECT generation FROM coordinator_admission WHERE singleton=1').fetchone()[0]
            previous = db.execute('SELECT authority_id,expected_generation,expected_closed,closed,generation FROM coordinator_admission_operations WHERE request_id=?', (request_id,)).fetchone()
            if previous is not None:
                if tuple(previous[:4]) != (self.queue.authority_id, expected_generation, int(expected_closed), int(value)):
                    raise Conflict('admission request identity changed')
                return {'authorityId':self.queue.authority_id,'requestId':request_id,'closed':value,'generation':previous[4]}
            if generation != expected_generation or current != expected_closed:
                raise Conflict('admission expected state changed')
            count = db.execute('SELECT COUNT(*) FROM coordinator_admission_operations').fetchone()[0]
            # Reserve one durable receipt for reopening after the last close.
            if count >= (9999 if value else 10000):
                raise Conflict('admission history limit reached')
            db.execute('UPDATE coordinator_admission SET closed=?,generation=? WHERE singleton=1', (int(value), generation+1))
            db.execute('INSERT INTO coordinator_admission_operations VALUES(?,?,?,?,?,?)', (request_id,self.queue.authority_id,expected_generation,int(expected_closed),int(value),generation+1))
        return {'authorityId':self.queue.authority_id,'requestId':request_id,'closed':value,'generation':generation+1}

    def status(self):
        with self._lock, self.queue._db() as db:
            closed = self._closed(db)
            generation = db.execute('SELECT generation FROM coordinator_admission WHERE singleton=1').fetchone()[0]
            return {'authorityId':self.queue.authority_id,'closed':closed,'generation':generation}
