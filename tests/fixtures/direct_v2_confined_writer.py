"""Synthetic one-job writer, trusted bootstrap for isolated recovery tests only."""
import ctypes
import ctypes.util
import errno
import hashlib
import os
import socket
import sys

DENIED=('clone','clone3','fork','vfork','socket','socketpair','sendmsg','sendmmsg',
        'ptrace','process_vm_readv','process_vm_writev','pidfd_getfd','io_uring_setup',
        'io_uring_enter','io_uring_register','unshare','setns','bpf','execve','execveat',
        'open_by_handle_at','userfaultfd')
lib=ctypes.CDLL(ctypes.util.find_library('seccomp'),use_errno=True)
lib.seccomp_init.argtypes=[ctypes.c_uint32];lib.seccomp_init.restype=ctypes.c_void_p
lib.seccomp_syscall_resolve_name.argtypes=[ctypes.c_char_p];lib.seccomp_syscall_resolve_name.restype=ctypes.c_int
lib.seccomp_rule_add.argtypes=[ctypes.c_void_p,ctypes.c_uint32,ctypes.c_int,ctypes.c_uint];lib.seccomp_rule_add.restype=ctypes.c_int
lib.seccomp_load.argtypes=[ctypes.c_void_p];lib.seccomp_load.restype=ctypes.c_int
context=lib.seccomp_init(0x7fff0000)
if not context:raise RuntimeError('seccomp context unavailable')
for name in DENIED:
    number=lib.seccomp_syscall_resolve_name(name.encode())
    if number>=0 and lib.seccomp_rule_add(context,0x00050000|errno.EPERM,number,0)!=0:
        raise RuntimeError('seccomp rule failed')
if lib.seccomp_load(context)!=0:raise RuntimeError('seccomp load failed')
for operation in (lambda:os.fork(),lambda:socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)):
    try:operation()
    except OSError as error:
        if error.errno!=errno.EPERM:raise
    else:raise RuntimeError('delegation restriction failed')
libc=ctypes.CDLL(None,use_errno=True)
libc.syscall.restype=ctypes.c_long
for name in ('sendmsg','ptrace','pidfd_getfd','io_uring_setup'):
    number=lib.seccomp_syscall_resolve_name(name.encode())
    if number>=0:
        ctypes.set_errno(0)
        result=libc.syscall(number,-1,0,0,0,0,0)
        if result!=-1 or ctypes.get_errno()!=errno.EPERM:raise RuntimeError('syscall isolation failed')
path=sys.argv[1];mode=sys.argv[2]
fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
try:
    os.write(1,b'READY\n')
    if mode=='partial':
        chunk=os.read(0,1)
        if chunk!=b'a':raise RuntimeError('synthetic input mismatch')
        os.write(fd,chunk);os.fsync(fd);os.write(1,b'PARTIAL\n')
        os.read(0,1)
    else:
        data=b''
        while len(data)<3:
            part=os.read(0,3-len(data))
            if not part:raise RuntimeError('synthetic input truncated')
            data+=part
        if hashlib.sha256(data).hexdigest()!=hashlib.sha256(b'abc').hexdigest():raise RuntimeError('synthetic digest mismatch')
        os.write(fd,data);os.fsync(fd);os.write(1,b'COMPLETE\n')
finally:os.close(fd)
