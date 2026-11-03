"""Synthetic cooperative POSIX spool exclusion; not production fencing."""
import os
import stat
from pathlib import Path
from contextlib import contextmanager

class SpoolHeld(ValueError): pass

class SpoolLock:
    def __init__(self, root):
        self.root=Path(root).absolute()
        if os.name != 'posix' or self.root.is_symlink() or self.root.resolve()!=self.root:
            raise SpoolHeld('private local root required')
        info=self.root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode & 0o077:
            raise SpoolHeld('private local root required')
        self.identity=(info.st_dev,info.st_ino)
        self.path=self.root/'viewer-spool.lock';self.marker=self.root/'viewer-spool.dirty'

    def _root(self):
        info=self.root.lstat()
        if self.root.resolve()!=self.root or (info.st_dev,info.st_ino)!=self.identity or info.st_uid!=os.geteuid() or info.st_mode & 0o077:
            raise SpoolHeld('spool root changed')

    @contextmanager
    def acquire(self):
        import fcntl
        self._root()
        fd=os.open(self.path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
        acquired=False; clean=False
        try:
            info=os.fstat(fd);named=self.path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_nlink!=1
                or info.st_mode & 0o077 or (info.st_dev,info.st_ino)!=(named.st_dev,named.st_ino)):
                raise SpoolHeld('lock identity changed')
            try: fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError: raise SpoolHeld('spool busy') from None
            acquired=True;self._root()
            marker=os.open(self.marker,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
            try: os.fsync(marker)
            finally: os.close(marker)
            rootfd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY)
            try: os.fsync(rootfd)
            finally: os.close(rootfd)
            yield
            clean=True
        except FileExistsError:
            raise SpoolHeld('prior spool operation requires reconciliation') from None
        finally:
            try:
                if acquired and clean:
                    self._root();os.unlink(self.marker)
                    rootfd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY)
                    try: os.fsync(rootfd)
                    finally: os.close(rootfd)
            finally: os.close(fd)
