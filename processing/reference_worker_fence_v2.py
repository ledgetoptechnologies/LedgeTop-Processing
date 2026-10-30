"""Disabled local cooperative worker fence prototype; not runtime authority.

Only cooperating processes opening the same stable lock inode are excluded.
A trusted supervisor must fence old/uncooperative workers, prevent inode or
mount replacement, and prevent callers from directly unlocking or duplicating the descriptor.
This does not fence another host, engine, open file descriptor, or root user.
The caller must treat the Python object and its private descriptors as trusted;
Python cannot prevent deliberate dup, direct close, or LOCK_UN by that caller.
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
        self._dirfd=os.open(path,flags)
        self._fd=None;self.pid=os.getpid();self.path=path
        try:
            info=os.fstat(self._dirfd)
            if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o700:
                raise WorkerFenceHeld('private worker directory required')
            self.dir_id=(info.st_dev,info.st_ino)
            self._fd=os.open(name,os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,dir_fd=self._dirfd)
            info=os.fstat(self._fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o600 or info.st_nlink!=1:
                raise WorkerFenceHeld('private single-link lock required')
            self.lock_id=(info.st_dev,info.st_ino)
            import fcntl
            try: fcntl.flock(self._fd,fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError: raise WorkerFenceHeld('worker fence already held') from None
            self.check()
            os.register_at_fork(after_in_child=self._after_fork)
        except Exception:
            self.close();raise

    def check(self):
        if self._fd is None or self.pid != os.getpid(): raise WorkerFenceHeld('worker fence owner changed')
        try:
            directory=os.stat(self.path,follow_symlinks=False)
            lock=os.stat('worker.lock',dir_fd=self._dirfd,follow_symlinks=False)
            fd=os.fstat(self._fd); dfd=os.fstat(self._dirfd)
            if ((directory.st_dev,directory.st_ino)!=self.dir_id or (dfd.st_dev,dfd.st_ino)!=self.dir_id
                or (lock.st_dev,lock.st_ino)!=self.lock_id or (fd.st_dev,fd.st_ino)!=self.lock_id
                or not stat.S_ISDIR(directory.st_mode) or not stat.S_ISREG(lock.st_mode)
                or fd.st_nlink!=1 or fd.st_uid!=os.geteuid() or stat.S_IMODE(fd.st_mode)!=0o600
                or directory.st_uid!=os.geteuid() or stat.S_IMODE(directory.st_mode)!=0o700):
                raise WorkerFenceHeld('worker fence identity changed')
        except OSError: raise WorkerFenceHeld('worker fence identity unavailable') from None

    def _after_fork(self):
        # Close inherited descriptors only: LOCK_UN would unlock the shared
        # open-file description still owned by the parent process.
        self.close()

    def close(self):
        if self._fd is not None: os.close(self._fd); self._fd=None
        if self._dirfd is not None: os.close(self._dirfd); self._dirfd=None

    def __enter__(self): self.check(); return self
    def __exit__(self,*args): self.close()
