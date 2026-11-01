"""Isolated UID mapping / socket access fixture; no account or host config writes."""
import ctypes, errno, fcntl, json, os, select, signal, socket, subprocess, sys, tempfile, time
from pathlib import Path

# The private directory/socket and fork are owned solely by this process.
with tempfile.TemporaryDirectory(prefix='codex-uid-owner-') as root:
    os.chmod(root,0o700)
    path=str(Path(root)/'control.sock')
    owner_program=Path(root)/'owner-only'
    owner_program.write_text('#!/bin/sh\nexit 0\n'); os.chmod(owner_program,0o700)
    control=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
    control.bind(path); os.chmod(path,0o600); control.listen(1)
    ready_r,ready_w=os.pipe(); go_r,go_w=os.pipe(); report_r,report_w=os.pipe()
    child=None; childfd=None
    try:
        child=os.fork()
        if child==0:
            try:
                os.close(ready_r); os.close(go_w); os.close(report_r); control.close()
                libc=ctypes.CDLL(None,use_errno=True)
                if libc.unshare(0x10000000)!=0: raise OSError(ctypes.get_errno(),'unshare user failed')
                os.write(ready_w,b'R'); os.close(ready_w)
                if os.read(go_r,1)!=b'G': os._exit(70)
                os.close(go_r)
                os.setgid(65534); os.setuid(65534)
                sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); sock.settimeout(2)
                try:
                    try: sock.connect(path); result=0
                    except OSError as e: result=e.errno
                finally: sock.close()
                try:
                    subprocess.run([str(owner_program)],check=True,timeout=2,capture_output=True)
                    launch_result=0
                except OSError as e: launch_result=e.errno
                os.write(report_w,json.dumps({'uid':os.getuid(),'connectErrno':result,'launchErrno':launch_result}).encode())
                os.close(report_w); os._exit(0)
            except BaseException:
                os._exit(71)
        os.close(ready_w); ready_w=None; os.close(go_r); go_r=None; os.close(report_w); report_w=None
        childfd=os.pidfd_open(child,0)
        if not select.select([ready_r],[],[],3)[0] or os.read(ready_r,1)!=b'R': raise RuntimeError('child user namespace unavailable')
        # Before each map write, verify this unreaped direct child remains
        # alive in a distinct user namespace with the same parent/birth.
        def identity():
            raw=Path('/proc',str(child),'stat').read_text()
            fields=raw[raw.rfind(')')+2:].split()
            return int(fields[1]),int(fields[19])
        expected=identity()
        if expected[0]!=os.getpid(): raise RuntimeError('child parent mismatch')
        hostns=os.open('/proc/self/ns/user',os.O_RDONLY|os.O_CLOEXEC)
        try:
            childns=os.open('/proc/'+str(child)+'/ns/user',os.O_RDONLY|os.O_CLOEXEC)
            try:
                if fcntl.ioctl(childns,0xb703)!=0x10000000: raise RuntimeError('not a user namespace')
                host,other=os.fstat(hostns),os.fstat(childns)
                if (host.st_dev,host.st_ino)==(other.st_dev,other.st_ino): raise RuntimeError('child not isolated')
                def write_map(name,value):
                    if identity()!=expected or select.select([childfd],[],[],0)[0]: raise RuntimeError('child identity changed')
                    current=os.stat('/proc/'+str(child)+'/ns/user')
                    if (current.st_dev,current.st_ino)!=(other.st_dev,other.st_ino): raise RuntimeError('child namespace changed')
                    Path('/proc',str(child),name).write_text(value)
                write_map('setgroups','deny')
                write_map('uid_map','0 0 65536\n')
                write_map('gid_map','0 0 65536\n')
            finally: os.close(childns)
        finally: os.close(hostns)
        os.write(go_w,b'G'); os.close(go_w); go_w=None
        if not select.select([report_r],[],[],4)[0]: raise TimeoutError('child report')
        value=json.loads(os.read(report_r,1024))
        if value!={'uid':65534,'connectErrno':errno.EACCES,'launchErrno':errno.EACCES}: raise RuntimeError('unexpected access result')
        deadline=time.monotonic()+3
        while not select.select([childfd],[],[],0)[0]:
            if time.monotonic()>=deadline: raise TimeoutError('child exit')
            time.sleep(.01)
        _,status=os.waitpid(child,0); child=None
        if not os.WIFEXITED(status) or os.WEXITSTATUS(status)!=0: raise RuntimeError('child failure')
        print(json.dumps({'separateUid':True,'ownerSocketDenied':True,'ownerLaunchDenied':True}))
    finally:
        for fd in (ready_r,ready_w,go_r,go_w,report_r,report_w):
            if fd is not None:
                try: os.close(fd)
                except OSError: pass
        if child is not None:
            if childfd is not None:
                try: signal.pidfd_send_signal(childfd,signal.SIGKILL)
                except ProcessLookupError: pass
            else:
                # Owned direct unreaped fork, PID cannot be recycled before wait.
                try: os.kill(child,signal.SIGKILL)
                except ProcessLookupError: pass
            try: os.waitpid(child,0)
            except ChildProcessError: pass
        if childfd is not None: os.close(childfd)
        control.close()
