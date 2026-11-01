"""Disabled local witness for one owned live init; not Docker authority.

The trusted supervisor opens and retains the pidfd before the process exits.
It cannot reconstruct a reaped PID identity after supervisor loss; reopen holds.
No process is signalled by this module.
"""
import os
from pathlib import Path
import select
import sqlite3
import threading

class Held(RuntimeError): pass


def process_identity(pid):
    value=Path('/proc',str(pid),'stat').read_text()
    fields=value[value.rfind(')')+2:].split()
    return int(fields[1]),int(fields[19])


class InitWitness:
    def __init__(self,journal,*,fixture=False):
        if fixture is not True: raise Held('witness not qualified for runtime')
        self.lock=threading.Lock(); self.fd=None; self.owner_pid=os.getpid()
        self.db=sqlite3.connect(journal,isolation_level=None)
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS witness (id INTEGER PRIMARY KEY CHECK(id=1), phase TEXT NOT NULL, pid INTEGER, parent INTEGER, birth INTEGER)')
        self.db.execute("INSERT OR IGNORE INTO witness VALUES (1,'new',NULL,NULL,NULL)")

    def bind_live(self,pid,*,expected_parent,expected_birth):
        with self.lock:
            if os.getpid()!=self.owner_pid or self.fd is not None: raise Held('fresh owning process required')
            if self.db.execute('SELECT phase FROM witness WHERE id=1').fetchone()[0]!='new':
                raise Held('prior witness requires inspection')
            if type(pid) is not int or pid<=0: raise Held('exact PID required')
            before=process_identity(pid)
            if before!=(expected_parent,expected_birth): raise Held('init identity mismatch')
            fd=os.pidfd_open(pid,0)
            try:
                if process_identity(pid)!=before or select.select([fd],[],[],0)[0]:
                    raise Held('init changed or already exited')
                self.db.execute('BEGIN IMMEDIATE')
                try:
                    self.db.execute("UPDATE witness SET phase='bound',pid=?,parent=?,birth=? WHERE id=1",(pid,*before))
                    self.db.execute('COMMIT')
                except BaseException:
                    self.db.execute('ROLLBACK'); raise
                self.fd=fd
            except BaseException:
                os.close(fd); raise

    def binding(self):
        with self.lock:
            if os.getpid()!=self.owner_pid or self.fd is None: raise Held('original witness unavailable')
            row=self.db.execute('SELECT phase,pid,parent,birth FROM witness WHERE id=1').fetchone()
            if row is None or row[0]!='bound': raise Held('witness binding unavailable')
            return tuple(row[1:])

    def duplicate_after_exit(self):
        with self.lock:
            if os.getpid()!=self.owner_pid or self.fd is None: raise Held('original witness unavailable')
            if not select.select([self.fd],[],[],0)[0]: raise Held('init still live')
            # Duplicate this witness's retained FD only; no caller-supplied FD.
            return os.dup(self.fd)

    def close(self):
        with self.lock:
            try:
                if self.fd is not None:
                    fd,self.fd=self.fd,None
                    os.close(fd)
            finally:
                self.db.close()
