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
from contextlib import contextmanager
from .reference_worker_fence_v2 import WorkerFence

class ControlledOpenHeld(ValueError): pass

class ControlledSQLite:
 def __init__(self,*,fence,path,expected_inode,expected_epoch):
  if os.name!='posix' or type(fence) is not WorkerFence:
   raise ControlledOpenHeld('POSIX and worker fence required')
  if threading.active_count()!=1:raise ControlledOpenHeld('single Python thread required')
  self.fence=fence;self.path=Path(path).absolute();self.db=None;self.anchor=None
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
   self.db=sqlite3.connect(self.path.as_uri()+'?mode=rw',uri=True,isolation_level=None)
   self.db.execute('PRAGMA synchronous=FULL');self.check()
   if self.db.execute('SELECT epoch FROM trusted_epoch WHERE id=1').fetchall()!=[(expected_epoch,)]:
    raise ControlledOpenHeld('preprovisioned epoch differs')
  except Exception:
   self.close();raise

 def check(self):
  if self.pid!=os.getpid() or self.db is None:raise ControlledOpenHeld('connection owner changed')
  self.fence.check()
  try:
   a=os.fstat(self.anchor);b=os.stat(self.path,follow_symlinks=False)
   if any((x.st_dev,x.st_ino)!=self.expected_inode for x in (a,b)) or a.st_nlink!=1 or b.st_nlink!=1:
    raise ControlledOpenHeld('database file identity changed')
  except OSError:raise ControlledOpenHeld('database identity unavailable') from None

 @contextmanager
 def transaction(self):
  """Check before and after commit; post-commit errors remain uncertain."""
  self.check()
  if self.db.in_transaction:raise ControlledOpenHeld('idle connection required')
  self.db.execute('BEGIN IMMEDIATE')
  try:
   if self.db.execute('SELECT epoch FROM trusted_epoch WHERE id=1').fetchall()!=[(self.expected_epoch,)]:
    raise ControlledOpenHeld('epoch changed')
   self.check();yield self.db;self.check();self.db.execute('COMMIT')
  except BaseException:
   if self.db.in_transaction:self.db.execute('ROLLBACK')
   raise
  self.check()

 def close(self):
  if self.db is not None:self.db.close();self.db=None
  if self.anchor is not None:os.close(self.anchor);self.anchor=None
