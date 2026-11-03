"""Opt-in cooperative upload gate; synthetic only, not a disk quota."""
import hashlib
import json
import os
import threading
from contextlib import contextmanager
from .scratch_policy import PolicyConflict, ScratchLedger
from .viewer_limits import MAX_UPLOAD_BYTES


class ViewerUploadCapacity:
    def __init__(self, ledger, store, spool):
        if not isinstance(ledger, ScratchLedger):
            raise ValueError('ScratchLedger required')
        self.ledger, self.store = ledger, store
        self.spool = os.path.realpath(spool)
        if any(os.stat(path).st_dev != os.stat(ledger.root).st_dev for path in (self.spool, store.root)):
            raise ValueError('spool and tasks must share reserved filesystem')
        self._lock = threading.RLock()

    def _task(self, identifier, environment):
        with self.store.queue._db() as db:
            row = self.store._row(db, identifier)
            if row['environment'] != environment or row['stage'] != 'uploading' or row['job_id']:
                raise PolicyConflict('task unavailable')
            total = db.execute('SELECT COALESCE(SUM(bytes),0) FROM viewer_files WHERE uuid=?', (identifier,)).fetchone()[0]
            return row['generation'], total

    def _identity(self, identifier, generation):
        return 'viewer-' + identifier + '-' + str(generation)

    def _check(self, identifier, environment, declared, body_bytes):
        if type(declared) is not int or not 1 <= declared <= self.store.max_input_bytes:
            raise PolicyConflict('declared total required')
        if type(body_bytes) is not int or not 1 <= body_bytes <= MAX_UPLOAD_BYTES:
            raise PolicyConflict('invalid request size')
        generation, actual = self._task(identifier, environment)
        if actual > declared:
            raise PolicyConflict('input exceeds declaration')
        reservation = self._identity(identifier, generation)
        manifest = hashlib.sha256(json.dumps([identifier,generation,environment,declared],separators=(',',':')).encode()).hexdigest()
        self.ledger.reserve(job_id=identifier, generation=generation, reservation_id=reservation,
            manifest_sha256=manifest, input_bytes=declared, output_budget=MAX_UPLOAD_BYTES,
            environment=environment, ttl_seconds=86400, worker_ready=True)
        # Never allow expiry of a published upload reservation, including when
        # the first request fails before writing a byte.
        self.ledger.set_engine_state(reservation, 'uncertain')
        self.ledger.record_allocated(reservation, actual)
        with self.ledger._transaction() as db:
            row = self.ledger._reservation(db, reservation)
            outstanding = db.execute('SELECT COALESCE(SUM(remaining),0) FROM reservations').fetchone()[0]
            outstanding += db.execute('SELECT COALESCE(SUM(bytes),0) FROM control_charges').fetchone()[0]
            if row['state'] != 'uncertain' or self.ledger.probe(self.ledger.root) < self.ledger.floor + outstanding:
                raise PolicyConflict('insufficient unreserved scratch')
        return actual

    @contextmanager
    def upload(self, identifier, environment, declared, body_bytes):
        with self._lock:
            self._check(identifier, environment, declared, body_bytes)
            yield

    def file(self, identifier, environment, declared, filename, size):
        self._check(identifier, environment, declared, 1)
        key = self.store._filename(filename)
        with self.store.queue._db() as db:
            exists = db.execute('SELECT bytes FROM viewer_files WHERE uuid=? AND name_key=?', (identifier,key)).fetchone()
            total = db.execute('SELECT COALESCE(SUM(bytes),0) FROM viewer_files WHERE uuid=?', (identifier,)).fetchone()[0]
        if (exists and exists[0] != size) or (not exists and total + size > declared):
            raise PolicyConflict('input exceeds declaration')

    @contextmanager
    def commit(self, identifier, environment, declared):
        with self._lock:
            if self._check(identifier, environment, declared, 1) != declared:
                raise PolicyConflict('declared total not yet uploaded')
            yield
