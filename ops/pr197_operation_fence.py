"""Cooperating predecessor locks for one stopped-state backup operation.

This is not a physical host writer fence or authority to start a candidate.
Both existing lock inodes are held until the caller exits this context.
"""
import os
from pathlib import Path
import re
import stat

from ops.state_fence import held_flocks
from ops.stopped_viewer_backup import _lock


class OperationFenceHeld(ValueError):pass


class OperationFence:
    def __init__(self,root,lease_directory,authority_id,*,enabled=False):
        if enabled is not True or not re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',authority_id):
            raise OperationFenceHeld('explicit_bound_operation_required')
        self.root=Path(root);self.leases=Path(lease_directory);self.authority=authority_id
        self.pid=os.getpid();self.fds=[];self.facts=[]

    def __enter__(self):
        from ops.pr197_stopped_census import _mount
        if self.pid!=os.getpid() or self.fds:raise OperationFenceHeld('fence_owner_changed')
        _mount(self.root)
        if not self.leases.is_absolute() or self.leases.resolve()!=self.leases:
            raise OperationFenceHeld('canonical_lease_directory_required')
        try:
            for path in (self.leases/(self.authority+'.lease'),self.root/'coordinator.lock'):
                fd=_lock(path);self.fds.append(fd)
                item=os.fstat(fd);self.facts.append((path,item.st_dev,item.st_ino))
            self.check();return self
        except BaseException:
            self.close();raise

    def check(self):
        if os.getpid()!=self.pid or len(self.fds)!=2:raise OperationFenceHeld('fence_owner_changed')
        try:
            owners=held_flocks(Path('/proc/locks').read_text())
            for fd,(path,dev,ino) in zip(self.fds,self.facts):
                current=os.fstat(fd);named=path.lstat()
                if (not stat.S_ISREG(current.st_mode) or current.st_nlink!=1 or
                    (current.st_dev,current.st_ino)!=(dev,ino) or (named.st_dev,named.st_ino)!=(dev,ino) or
                    self.pid not in owners.get((os.major(dev),os.minor(dev),ino),set())):
                    raise OperationFenceHeld('operation_lock_lost_or_replaced')
        except OperationFenceHeld:raise
        except OSError:raise OperationFenceHeld('operation_lock_unavailable') from None

    def close(self):
        for fd in reversed(self.fds):os.close(fd)
        self.fds=[];self.facts=[]

    def __exit__(self,*_):self.close()
