"""Persistent replay fence for held-only signed job admission.

All methods require the canonical physical NodeGate lock. A consumed nonce is
never deleted; a crash between ledger write and gate binding requires operator
reconciliation. This is cooperative and does not fence older/direct writers.
"""
import os
import sqlite3
import stat
from pathlib import Path

from . import reference_protocol as protocol
from .host_mutation_journal import MutationHeld
from .node_gate import GateError


class JobClaimLedger:
    def __init__(self, gate):
        self.gate = gate
        self.path = gate.root / 'job-claim-ledger.sqlite'

    def _open(self):
        self.gate._check_lock()
        state = self.gate.snapshot()
        marker = state.get('jobClaimLedger')
        if marker is None:
            # Never infer that an already-existing unmarked ledger is empty.
            if self.path.exists() or self.path.is_symlink():
                raise GateError('unmarked claim ledger requires inspection')
            state['schema'] = 2
            state['jobClaimLedger'] = {'version': 1, 'file': self.path.name}
            self.gate._save(state)
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, 'O_NOFOLLOW', 0), 0o600)
                os.close(fd)
                if os.name != 'nt':
                    fd = os.open(self.gate.root, os.O_RDONLY | os.O_DIRECTORY)
                    try: os.fsync(fd)
                    finally: os.close(fd)
                db = sqlite3.connect(f'{self.path.as_uri()}?mode=rw', uri=True)
                try:
                    db.execute('PRAGMA synchronous=FULL')
                    db.execute('CREATE TABLE claims(nonce TEXT PRIMARY KEY, claim_digest TEXT NOT NULL, receipt TEXT NOT NULL)')
                    db.commit()
                finally: db.close()
            except BaseException:
                # The marker stays durable: an incomplete ledger is never rebuilt.
                raise
        elif marker != {'version': 1, 'file': self.path.name}:
            raise GateError('invalid claim ledger marker')
        try:
            info = self.path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or (os.name != 'nt' and (info.st_uid != os.geteuid() or info.st_mode & 0o077))):
                raise ValueError()
            db = sqlite3.connect(f'{self.path.as_uri()}?mode=rw', uri=True)
            db.execute('PRAGMA synchronous=FULL')
            if db.execute('PRAGMA quick_check').fetchone() != ('ok',):
                db.close(); raise ValueError()
            db.execute('SELECT nonce,claim_digest,receipt FROM claims LIMIT 1').fetchall()
            return db
        except (OSError, sqlite3.Error, ValueError):
            raise GateError('claim ledger requires inspection') from None

    def consume(self, claim, receipt):
        """Durably consume one nonce; replay is always held without mutation."""
        digest = protocol.digest(claim)
        db = self._open()
        try:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT claim_digest,receipt FROM claims WHERE nonce=?', (claim['nonce'],)).fetchone()
            if row is not None:
                db.rollback()
                state = self.gate.snapshot()
                reservation = state['reservation']
                if row[0] == digest and reservation is not None and reservation['receiptId'] == row[1]:
                    raise MutationHeld('claim already bound; inspect existing reservation')
                raise GateError('claim replay or collision requires inspection')
            db.execute('INSERT INTO claims(nonce,claim_digest,receipt) VALUES(?,?,?)',
                       (claim['nonce'], digest, receipt))
            db.commit()
        except BaseException:
            if db.in_transaction: db.rollback()
            raise
        finally:
            db.close()
