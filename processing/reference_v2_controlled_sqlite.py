"""Disabled Linux SQLite connection-origin investigation, not an authority gate.

Trusted supervisor must exclude all directory and file replacement, database
restore, noncooperating writers, native threads opening descriptors, and other
SQLite connections for the entire operation. The /proc fd census is evidence
under those assumptions, not a SQLite-supported connection-to-inode API.
"""
import os
from pathlib import Path
import sqlite3
import stat
import threading
from .reference_worker_fence_v2 import WorkerFence

class ControlledOpenHeld(ValueError): pass

def _fds():
 result={}
 for name in os.listdir('/proc/self/fd'):
  try:
   st=os.stat('/proc/self/fd/'+name);result[int(name)]=(st.st_dev,st.st_ino)
  except FileNotFoundError:pass
 return result

class ControlledSQLite:
 def __init__(self,*,fence,path,expected_inode):
  if os.name!='posix' or not os.path.isdir('/proc/self/fd') or type(fence) is not WorkerFence:
   raise ControlledOpenHeld('Linux procfs and worker fence required')
  if threading.active_count()!=1:raise ControlledOpenHeld('single Python thread required')
  self.fence=fence;self.path=Path(path).absolute();self.db=None;self.anchor=None
  self.sqlite_fd=None;self.expected_inode=expected_inode;self.pid=os.getpid()
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
   before=_fds()
   self.db=sqlite3.connect('file:/proc/self/fd/'+str(self.anchor)+'?mode=rw',uri=True,isolation_level=None)
   self.db.execute('PRAGMA schema_version').fetchone()
   after=_fds();new=[fd for fd,identity in after.items() if fd not in before and identity==expected_inode]
   if len(new)!=1:raise ControlledOpenHeld('SQLite descriptor observation ambiguous')
   self.sqlite_fd=new[0];self.check()
  except Exception:
   self.close();raise

 def check(self):
  if self.pid!=os.getpid() or self.db is None:raise ControlledOpenHeld('connection owner changed')
  self.fence.check()
  try:
   a=os.fstat(self.anchor);b=os.stat(self.path,follow_symlinks=False);c=os.stat('/proc/self/fd/'+str(self.sqlite_fd))
   if any((x.st_dev,x.st_ino)!=self.expected_inode for x in (a,b,c)) or a.st_nlink!=1 or b.st_nlink!=1:
    raise ControlledOpenHeld('database file identity changed')
  except OSError:raise ControlledOpenHeld('database identity unavailable') from None

 def close(self):
  if self.db is not None:self.db.close();self.db=None
  if self.anchor is not None:os.close(self.anchor);self.anchor=None
