"""Coordinator-local admission barrier; does not fence direct engine access."""
from contextlib import contextmanager
import threading
import uuid
from .maintenance import Conflict


class AdmissionClosed(Exception):
    pass


class AdmissionDrain:
    _DDL = 'CREATE TABLE coordinator_admission (singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority_id TEXT NOT NULL, closed INTEGER NOT NULL CHECK(closed IN (0,1)), generation INTEGER NOT NULL CHECK(generation>=0))'
    _HOLD_DDL = 'CREATE TABLE coordinator_recovery_hold (singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority_id TEXT NOT NULL)'
    _OPS_DDL = 'CREATE TABLE coordinator_admission_operations (request_id TEXT PRIMARY KEY, authority_id TEXT NOT NULL, expected_generation INTEGER NOT NULL, expected_closed INTEGER NOT NULL, closed INTEGER NOT NULL, generation INTEGER NOT NULL)'

    def __init__(self, queue, *, initial_closed=False):
        if type(initial_closed) is not bool:raise ValueError('explicit initial state required')
        self.queue = queue
        self._lock = threading.RLock()
        self._transition_lock = threading.Lock()
        self._closing = threading.Event()
        with queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='coordinator_admission'").fetchone()
            operations = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='coordinator_admission_operations'").fetchone()
            if existing is None:
                if operations is not None: raise AdmissionClosed()
                db.execute(self._DDL)
                db.execute(self._OPS_DDL)
                db.execute('INSERT INTO coordinator_admission VALUES(1,?,?,0)', (queue.authority_id,int(initial_closed)))
            else:
                if existing[0] != self._DDL or operations is None or operations[0] != self._OPS_DDL or db.execute('SELECT COUNT(*) FROM coordinator_admission').fetchone()[0] != 1:
                    raise AdmissionClosed()
                closed=self._closed(db)
                if initial_closed and not closed:raise AdmissionClosed()

    def _recovery_held(self, db):
        table=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='coordinator_recovery_hold'").fetchone()
        if table is None:return False
        if table[0]!=self._HOLD_DDL:raise AdmissionClosed()
        rows=db.execute('SELECT singleton,authority_id FROM coordinator_recovery_hold').fetchall()
        if len(rows)!=1 or rows[0][0]!=1 or rows[0][1]!=self.queue.authority_id:raise AdmissionClosed()
        return True

    def arm_recovery_hold(self):
        """Irreversibly block cooperative admissions pending future recovery.

        This only covers the existing drain boundary. It is not a node lease,
        an authenticated census, or a fence on active/direct engine work. No
        release method exists until all-node recovery has been qualified.
        """
        with self._transition_lock:
            self._closing.set()
            try:
                with self._lock, self.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE')
                    closed=self._closed(db)
                    if self._recovery_held(db):return
                    if not closed:
                        generation=db.execute('SELECT generation FROM coordinator_admission WHERE singleton=1').fetchone()[0]
                        if generation>=2**53-1:raise AdmissionClosed()
                        db.execute('UPDATE coordinator_admission SET closed=1,generation=? WHERE singleton=1',(generation+1,))
                    db.execute(self._HOLD_DDL)
                    db.execute('INSERT INTO coordinator_recovery_hold VALUES(1,?)',(self.queue.authority_id,))
            finally:self._closing.clear()

    def _closed(self, db):
        row = db.execute('SELECT authority_id,closed,generation FROM coordinator_admission WHERE singleton=1').fetchone()
        if row is None or row[0] != self.queue.authority_id or row[1] not in (0, 1) or type(row[2]) is not int or not 0 <= row[2] <= 2**53-1:
            raise AdmissionClosed()
        return bool(row[1]) or self._recovery_held(db)

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
                return self._persist_transition(value,request_id,expected_generation,expected_closed)
            finally:
                if value:self._closing.clear()

    def _persist_transition(self, value, request_id, expected_generation, expected_closed):
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            current = self._closed(db)
            if not value and self._recovery_held(db):
                raise Conflict('node recovery hold has no qualified release')
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
