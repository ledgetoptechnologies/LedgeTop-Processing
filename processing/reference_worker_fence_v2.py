"""Disabled local cooperative worker fence prototype; not runtime authority.

Only cooperating processes opening the same stable lock inode are excluded.
A trusted supervisor must fence old/uncooperative workers, prevent inode or
mount replacement, and prohibit inherited lock descriptors in child processes.
This does not fence another host, engine, open file descriptor, or root user.
"""
import os
from pathlib import Path
import stat


class WorkerFenceHeld(ValueError):
    pass


class WorkerFence:
    def __init__(self, directory, name='worker.lock'):
        if os.name != 'posix': raise WorkerFenceHeld('POSIX flock required')
        if name != 'worker.lock': raise WorkerFenceHeld('fixed lock name required')
        path=Path(directory).absolute()
        if path.resolve(strict=True)!=path or any(p.is_symlink() for p in (path,*path.parents)):
            raise WorkerFenceHeld('canonical trusted directory required')
        flags=os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        self.dirfd=os.open(path,flags)
        self.fd=None;self.pid=os.getpid();self.path=path
        try:
            info=os.fstat(self.dirfd)
            if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o700:
                raise WorkerFenceHeld('private worker directory required')
            self.dir_id=(info.st_dev,info.st_ino)
            self.fd=os.open(name,os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,dir_fd=self.dirfd)
            info=os.fstat(self.fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o600 or info.st_nlink!=1:
                raise WorkerFenceHeld('private single-link lock required')
            self.lock_id=(info.st_dev,info.st_ino)
            import fcntl
            try: fcntl.flock(self.fd,fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError: raise WorkerFenceHeld('worker fence already held') from None
            self.check()
        except Exception:
            self.close();raise

    def check(self):
        if self.fd is None or self.pid != os.getpid(): raise WorkerFenceHeld('worker fence owner changed')
        try:
            directory=os.stat(self.path,follow_symlinks=False)
            lock=os.stat('worker.lock',dir_fd=self.dirfd,follow_symlinks=False)
            fd=os.fstat(self.fd); dfd=os.fstat(self.dirfd)
            if ((directory.st_dev,directory.st_ino)!=self.dir_id or (dfd.st_dev,dfd.st_ino)!=self.dir_id
                or (lock.st_dev,lock.st_ino)!=self.lock_id or (fd.st_dev,fd.st_ino)!=self.lock_id
                or not stat.S_ISDIR(directory.st_mode) or not stat.S_ISREG(lock.st_mode)
                or fd.st_nlink!=1 or fd.st_uid!=os.geteuid() or stat.S_IMODE(fd.st_mode)!=0o600
                or directory.st_uid!=os.geteuid() or stat.S_IMODE(directory.st_mode)!=0o700):
                raise WorkerFenceHeld('worker fence identity changed')
        except OSError: raise WorkerFenceHeld('worker fence identity unavailable') from None

    def close(self):
        if self.fd is not None: os.close(self.fd); self.fd=None
        if self.dirfd is not None: os.close(self.dirfd); self.dirfd=None

    def __enter__(self): self.check(); return self
    def __exit__(self,*args): self.close()
