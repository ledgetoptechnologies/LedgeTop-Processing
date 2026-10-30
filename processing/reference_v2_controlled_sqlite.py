"""Disabled Linux SQLite connection-origin investigation, not an authority gate.

Trusted supervisor must exclude all directory and file replacement, database
restore, noncooperating writers, native threads opening descriptors, and other
SQLite connections for the entire operation. The inode checks are conditional on that exclusion, not a SQLite-supported
connection-to-inode proof. Restores and hardlinks require separate fencing.
"""
import os
from pathlib import Path
import sqlite3
import stat
import threading
from .reference_worker_fence_v2 import WorkerFence
from . import reference_protocol as p

EPOCH_DDL="CREATE TABLE trusted_epoch (id INTEGER PRIMARY KEY CHECK(id=1), epoch TEXT NOT NULL)"
INTENT_DDL="CREATE TABLE trusted_intent (job TEXT PRIMARY KEY, identity TEXT NOT NULL)"

def _schema_template():
 db=sqlite3.connect(':memory:')
 try:
  db.execute(EPOCH_DDL);db.execute(INTENT_DDL)
  return tuple(db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'))
 finally:db.close()

EXPECTED_SCHEMA=_schema_template()

class ControlledOpenHeld(ValueError): pass

class ControlledSQLite:
 def __init__(self,*,fence,path,expected_inode,expected_epoch):
  if os.name!='posix' or type(fence) is not WorkerFence:
   raise ControlledOpenHeld('POSIX and worker fence required')
  if threading.active_count()!=1:raise ControlledOpenHeld('single Python thread required')
  self.fence=fence;self.path=Path(path).absolute();self._db=None;self.anchor=None
  self.expected_inode=expected_inode;self.expected_epoch=expected_epoch;self.pid=os.getpid()
  try:
   fence.check()
   if self.path.resolve(strict=True)!=self.path or any(p.is_symlink() for p in (self.path,*self.path.parents)):
    raise ControlledOpenHeld('canonical database path required')
   parent=self.path.parent.stat()
   if parent.st_uid!=os.geteuid() or stat.S_IMODE(parent.st_mode)!=0o700:raise ControlledOpenHeld('private directory required')
   self.anchor=os.open(self.path,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
   st=os.fstat(self.anchor)
   if not stat.S_ISREG(st.st_mode) or st.st_uid!=os.geteuid() or stat.S_IMODE(st.st_mode)!=0o600 or st.st_nlink!=1 or (st.st_dev,st.st_ino)!=expected_inode:
    raise ControlledOpenHeld('expected database inode required')
   # The supervisor guarantees this pathname cannot be replaced between the
   # inode check, SQLite open and all subsequent operations. SQLite does not
   # expose the actual connection descriptor to Python.
   self._db=sqlite3.connect(self.path.as_uri()+'?mode=rw',uri=True,isolation_level=None)
   self._db.execute('PRAGMA synchronous=FULL');self.check()
   self._validate_schema()
  except Exception:
   self.close();raise

 def check(self):
  if self.pid!=os.getpid() or self._db is None:raise ControlledOpenHeld('connection owner changed')
  self.fence.check()
  try:
   a=os.fstat(self.anchor);b=os.stat(self.path,follow_symlinks=False)
   if any((x.st_dev,x.st_ino)!=self.expected_inode for x in (a,b)) or a.st_nlink!=1 or b.st_nlink!=1:
    raise ControlledOpenHeld('database file identity changed')
  except OSError:raise ControlledOpenHeld('database identity unavailable') from None

 def _validate_schema(self):
  schema=tuple(tuple(r) for r in self._db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'))
  if schema!=EXPECTED_SCHEMA or self._db.execute('SELECT 1 FROM sqlite_temp_master LIMIT 1').fetchone():
   raise ControlledOpenHeld('exact preprovisioned schema required')
  if self._db.execute('SELECT id,epoch FROM trusted_epoch').fetchall()!=[(1,self.expected_epoch)]:
   raise ControlledOpenHeld('preprovisioned epoch differs')

 def record_once(self,job,identity):
  """Write a single immutable pending identity. No arbitrary callback executes.

  The caller owns the identity and decides how to reconcile any uncertain
  commit; duplicate rows are never authorization to retry an external call.
  """
  p._uuid(job,'job')
  if type(identity) is not str or not 1<=len(identity.encode('utf8'))<=16384:raise ControlledOpenHeld('bounded identity required')
  self.check()
  if self._db.in_transaction:raise ControlledOpenHeld('idle connection required')
  self._db.execute('BEGIN IMMEDIATE')
  try:
   self._validate_schema();self.check()
   if self._db.execute('SELECT 1 FROM trusted_intent WHERE job=?',(job,)).fetchone():raise ControlledOpenHeld('intent already recorded')
   self._db.execute('INSERT INTO trusted_intent VALUES(?,?)',(job,identity))
   self._validate_schema();self.check();self._db.execute('COMMIT')
  except BaseException:
   if self._db.in_transaction:self._db.execute('ROLLBACK')
   raise
  self.check()

 def lookup(self,job):
  p._uuid(job,'job');self.check();self._validate_schema()
  result=self._db.execute('SELECT identity FROM trusted_intent WHERE job=?',(job,)).fetchone()
  self.check();return None if result is None else result[0]

 def close(self):
  if self._db is not None:self._db.close();self._db=None
  if self.anchor is not None:os.close(self.anchor);self.anchor=None
