"""Disabled, cooperative, same-host multi-filesystem admission candidate.

Not a block reservation, a quota, a remote/NAS protocol, or a production gate.
The caller must own all ledger roots in a single trusted mount namespace and
must supply every copy/overhead budget. External writers can exhaust disk after
admission. Acquired reservations are never rolled back automatically.
"""
import hashlib
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
import stat

from .scratch_policy import ScratchLedger


CROSSFS_QUALIFIED = False
# Process-global cooperating profile only; actual host ownership/provisioning
# and mount identity are not yet qualified.
CROSSFS_CANONICAL_JOURNAL = Path.home() / ".local/share/ledgetop-processing/crossfs/admission.sqlite"


class CrossFsHeld(ValueError):
    pass


def _canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',', ':'),allow_nan=False)


class CrossFsAdmission:
    """Durable idempotent acquisition across existing ScratchLedger instances."""
    def __init__(self, journal, ledgers):
        if CROSSFS_QUALIFIED is not True:
            raise CrossFsHeld('cross-filesystem authority not qualified')
        self.path=Path(journal).absolute()
        if self.path != CROSSFS_CANONICAL_JOURNAL.absolute():
            raise CrossFsHeld('canonical cross-filesystem journal required')
        if not self.path.parent.is_dir(): raise CrossFsHeld('journal parent unavailable')
        if type(ledgers) is not dict or not ledgers: raise CrossFsHeld('ledgers required')
        fd=os.open(self.path,os.O_CREAT | os.O_RDWR | getattr(os,'O_NOFOLLOW',0),0o600)
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode & 0o077)):
                raise CrossFsHeld('private journal identity required')
            self._journal_identity=(info.st_dev,info.st_ino)
        finally:
            os.close(fd)
        self.ledgers=ledgers
        for label,ledger in ledgers.items():
            if type(label) is not str or not label or type(ledger) is not ScratchLedger:
                raise CrossFsHeld('named scratch ledgers required')
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS attempts (id TEXT PRIMARY KEY, plan TEXT NOT NULL, state TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS parts (attempt TEXT NOT NULL, device TEXT NOT NULL, ledger TEXT NOT NULL, bytes INTEGER NOT NULL, reservation TEXT NOT NULL, state TEXT NOT NULL, PRIMARY KEY(attempt,device))')
            db.execute('CREATE TABLE IF NOT EXISTS device_authorities (device TEXT PRIMARY KEY, identity TEXT NOT NULL)')

    @contextmanager
    def _db(self):
        before=self.path.lstat()
        if stat.S_ISLNK(before.st_mode) or (before.st_dev,before.st_ino)!=self._journal_identity:
            raise CrossFsHeld('canonical journal identity changed')
        db=sqlite3.connect(self.path,timeout=10)
        try:
            after=self.path.lstat()
            if stat.S_ISLNK(after.st_mode) or (after.st_dev,after.st_ino)!=self._journal_identity:
                raise CrossFsHeld('canonical journal identity changed')
            db.execute('PRAGMA synchronous=FULL')
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def acquire(self, *, attempt_id, job_id, generation, manifest_sha256, environment, ttl_seconds, demands):
        if (type(attempt_id) is not str or not attempt_id or len(attempt_id)>128
                or type(demands) is not list or not demands):
            raise CrossFsHeld('bounded attempt and demands required')
        by_device={}
        for demand in demands:
            if (type(demand) is not dict or demand.get('ledger') not in self.ledgers
                    or set(demand) not in ({'ledger','bytes'},{'ledger','inputBytes','outputBudget'})):
                raise CrossFsHeld('invalid capacity demand')
            input_bytes=demand.get('inputBytes',demand.get('bytes'))
            output_budget=demand.get('outputBudget',0)
            if (type(input_bytes) is not int or type(output_budget) is not int
                    or input_bytes<0 or output_budget<0 or not 0<input_bytes+output_budget<=2**53-1):
                raise CrossFsHeld('invalid capacity demand')
            ledger=self.ledgers[demand['ledger']]
            st=os.stat(ledger.root,follow_symlinks=False)
            device=str(st.st_dev)
            identity=[str(ledger.root),st.st_dev,st.st_ino,str(ledger.path.absolute())]
            existing=by_device.get(device)
            if existing and existing['identity']!=identity:
                raise CrossFsHeld('same filesystem requires one canonical ledger')
            if not existing:
                existing=by_device[device]={'identity':identity,'ledger':demand['ledger'],'bytes':0,'inputBytes':0,'outputBudget':0}
            existing['inputBytes']+=input_bytes;existing['outputBudget']+=output_budget
            existing['bytes']+=input_bytes+output_budget
            if existing['bytes']>2**53-1:raise CrossFsHeld('capacity overflow')
        plan={'job':job_id,'generation':generation,'manifest':manifest_sha256,'environment':environment,
              'ttl':ttl_seconds,'devices':by_device}
        encoded=_canonical(plan)
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            for device,entry in by_device.items():
                encoded_identity=_canonical(entry['identity'])
                existing=db.execute('SELECT identity FROM device_authorities WHERE device=?',(device,)).fetchone()
                if existing is not None and existing[0]!=encoded_identity:
                    raise CrossFsHeld('filesystem already bound to another ledger')
                if existing is None:
                    db.execute('INSERT INTO device_authorities VALUES(?,?)',(device,encoded_identity))
            row=db.execute('SELECT plan,state FROM attempts WHERE id=?',(attempt_id,)).fetchone()
            if row and row[0]!=encoded:raise CrossFsHeld('attempt identity changed')
            if not row:
                db.execute("INSERT INTO attempts VALUES(?,?,'acquiring')",(attempt_id,encoded))
                for device,entry in by_device.items():
                    rid='crossfs-'+hashlib.sha256((attempt_id+'\0'+device).encode()).hexdigest()[:32]
                    db.execute("INSERT INTO parts VALUES(?,?,?,?,?,'pending')",(attempt_id,device,entry['ledger'],entry['bytes'],rid))
        # Each deterministic reservation is persisted before its cross-journal
        # acknowledgement. A lost response replays the same reservation ID.
        for device,entry in by_device.items():
            ledger=self.ledgers[entry['ledger']]
            st=os.stat(ledger.root,follow_symlinks=False)
            if [str(ledger.root),st.st_dev,st.st_ino,str(ledger.path.absolute())]!=entry['identity']:
                raise CrossFsHeld('filesystem identity changed')
            with self._db() as db:
                part=db.execute('SELECT reservation FROM parts WHERE attempt=? AND device=?',(attempt_id,device)).fetchone()
            if part is None:raise CrossFsHeld('reservation journal incomplete')
            try:
                ledger.reserve(job_id=job_id,generation=generation,reservation_id=part[0],
                    manifest_sha256=manifest_sha256,input_bytes=entry['inputBytes'],output_budget=entry['outputBudget'],
                    environment=environment,ttl_seconds=ttl_seconds,worker_ready=True)
                # Conservative pin: never expire a recorded component while
                # another filesystem is still being acquired. This is not an
                # assertion that an engine has started; release is not wired.
                ledger.set_engine_state(part[0], 'uncertain')
            except Exception as error:
                # No acquired reservation is released; recovery needs same plan.
                raise CrossFsHeld('partial admission requires reconciliation') from error
            with self._db() as db:
                db.execute('BEGIN IMMEDIATE')
                if db.execute("UPDATE parts SET state='acquired' WHERE attempt=? AND device=? AND reservation=?",(attempt_id,device,part[0])).rowcount!=1:
                    raise CrossFsHeld('reservation journal changed')
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute("SELECT COUNT(*) FROM parts WHERE attempt=? AND state!='acquired'",(attempt_id,)).fetchone()[0]:
                raise CrossFsHeld('partial admission remains held')
            db.execute("UPDATE attempts SET state='admitted' WHERE id=?",(attempt_id,))
        return {'attemptId':attempt_id,'state':'admitted'}
