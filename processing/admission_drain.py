"""Coordinator-local admission barrier; does not fence direct engine access."""
from contextlib import contextmanager
import threading


class AdmissionClosed(Exception):
    pass


class AdmissionDrain:
    _DDL = 'CREATE TABLE coordinator_admission (singleton INTEGER PRIMARY KEY CHECK(singleton=1), closed INTEGER NOT NULL CHECK(closed IN (0,1)))'

    def __init__(self, queue):
        self.queue = queue
        self._lock = threading.RLock()
        with queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='coordinator_admission'").fetchone()
            if existing is None:
                db.execute(self._DDL)
                db.execute('INSERT INTO coordinator_admission VALUES(1,0)')
            else:
                if existing[0] != self._DDL or db.execute('SELECT COUNT(*) FROM coordinator_admission').fetchone()[0] != 1:
                    raise AdmissionClosed()
                self._closed(db)

    def _closed(self, db):
        row = db.execute('SELECT closed FROM coordinator_admission WHERE singleton=1').fetchone()
        if row is None or row[0] not in (0, 1):
            raise AdmissionClosed()
        return bool(row[0])

    @contextmanager
    def admit(self):
        # Hold the barrier through all admission side effects, including file
        # upload; closing waits until prior admitted requests have returned.
        with self._lock:
            with self.queue._db() as db:
                if self._closed(db): raise AdmissionClosed()
            yield

    def set_closed(self, value):
        if type(value) is not bool: raise ValueError('boolean closed required')
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._closed(db)
            db.execute('UPDATE coordinator_admission SET closed=? WHERE singleton=1', (int(value),))
        return {'closed': value}

    def status(self):
        with self._lock, self.queue._db() as db:
            return {'closed': self._closed(db)}
