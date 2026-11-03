"""Experimental persistent, conservative remote-upload disk reservation.

No automatic orphan deletion. This protects only cooperating new writers; an
offline census and external exclusion of old/direct writers remain necessary.
"""
import os
from contextlib import closing
from pathlib import Path
import re
import sqlite3
import stat


class SpoolBudgetError(Exception):
    pass


class GuardedSpoolBudget:
    def __init__(self, root, *, cap, floor=1024 ** 3):
        if os.name == 'nt' or type(cap) is not int or cap < 1 or type(floor) is not int or floor < 0:
            raise SpoolBudgetError('invalid spool configuration')
        self.root, self.cap, self.floor = Path(root), cap, floor
        self.path = self.root / '.guarded-spool.sqlite3'
        flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW
        fd = os.open(self.path, flags, 0o600)
        try:
            info = os.fstat(fd)
            current = os.stat(self.path, follow_symlinks=False)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077 or (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino)):
                raise SpoolBudgetError('invalid budget ledger')
        finally:
            os.close(fd)
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('CREATE TABLE IF NOT EXISTS spool_reservations ('
                       'name TEXT PRIMARY KEY, bytes INTEGER NOT NULL, stage TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS spool_policy ('
                       'id INTEGER PRIMARY KEY CHECK(id=1), cap INTEGER NOT NULL, floor INTEGER NOT NULL)')
            db.execute('INSERT OR IGNORE INTO spool_policy VALUES(1,?,?)', (cap, floor))
            if db.execute('SELECT cap,floor FROM spool_policy WHERE id=1').fetchone() != (cap, floor):
                raise SpoolBudgetError('spool policy differs; explicit reconciliation required')
            db.commit()
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try: os.fsync(directory)
        finally: os.close(directory)

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.execute('PRAGMA synchronous=FULL')
        db.execute('PRAGMA journal_mode=DELETE')
        return db

    @staticmethod
    def _valid_name(name):
        return type(name) is str and re.fullmatch(r'incoming-[0-9a-f]{48}', name) is not None

    def _rows(self, db):
        rows = db.execute('SELECT name,bytes,stage FROM spool_reservations').fetchall()
        for name, size, stage in rows:
            if (not self._valid_name(name) or type(size) is not int or size < 1
                    or stage not in ('receiving', 'spooled', 'mutation_started', 'discardable')):
                raise SpoolBudgetError('invalid spool reservation')
        return rows

    def reserve(self, name, size):
        if not self._valid_name(name) or type(size) is not int or not 0 < size <= self.cap:
            raise SpoolBudgetError('invalid spool reservation')
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            rows = self._rows(db)
            known = {row[0] for row in rows}
            if any(entry.startswith('incoming-') and entry not in known for entry in os.listdir(self.root)):
                raise SpoolBudgetError('untracked spool requires reconciliation')
            used = sum(row[1] for row in rows)
            fs = os.statvfs(self.root)
            # Double-counting already-written bytes is deliberately conservative.
            if used + size > self.cap or fs.f_bavail * fs.f_frsize - used - size < self.floor:
                raise SpoolBudgetError('spool capacity unavailable')
            db.execute('INSERT INTO spool_reservations VALUES(?,?,?)', (name, size, 'receiving'))
            db.commit()

    def transition(self, name, expected, target):
        allowed = {('receiving', 'spooled'), ('spooled', 'mutation_started'),
                   ('receiving', 'discardable'), ('spooled', 'discardable'),
                   ('mutation_started', 'discardable')}
        if not self._valid_name(name) or (expected, target) not in allowed:
            raise SpoolBudgetError('invalid spool transition')
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            self._rows(db)
            result = db.execute('UPDATE spool_reservations SET stage=? WHERE name=? AND stage=?',
                                (target, name, expected))
            if result.rowcount != 1: raise SpoolBudgetError('spool reservation differs')
            db.commit()

    def release_discarded(self, name, root_fd):
        if not self._valid_name(name): raise SpoolBudgetError('invalid spool name')
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            self._rows(db)
            try:
                os.stat(name, dir_fd=root_fd, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise SpoolBudgetError('spool file still exists')
            result = db.execute("DELETE FROM spool_reservations WHERE name=? AND stage='discardable'", (name,))
            if result.rowcount != 1: raise SpoolBudgetError('spool reservation is not discardable')
            db.commit()

    def inspect(self):
        """Read-only crash census; report without deleting or freeing bytes."""
        with closing(self._connect()) as db:
            db.execute('BEGIN')
            rows = self._rows(db)
            known = {row[0] for row in rows}
            files = {entry for entry in os.listdir(self.root) if entry.startswith('incoming-')}
            result = {'reservations': [{'name': n, 'bytes': s, 'stage': stage, 'present': n in files}
                                       for n, s, stage in rows],
                      'untracked': sorted(files - known)}
            db.commit()
            return result
