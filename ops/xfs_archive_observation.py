"""Disabled read-only XFS quota research; never a write authority.

The experimental implementation supports only Linux x86-64's observed ABI.
It cannot prove exclusive writers or physical free capacity. Shared host
administrators and the calling mount namespace remain trusted.
"""
import ctypes
import fcntl
import os
from pathlib import Path
import platform
import re
import struct
import sys

class ObservationUnavailable(ValueError):pass

def _require(value):
    if not value:raise ObservationUnavailable('XFS quota observation unavailable')

def observe(mount_path,project_directory,project_id,hard_bytes):
    """Deliberately disabled until real positive tests and review pass."""
    raise ObservationUnavailable('XFS quota observation unavailable')

def _open_directory(path):
    flags=os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC
    fd=os.open('/',flags)
    try:
        for part in Path(path).parts[1:]:
            child=os.open(part,flags,dir_fd=fd);os.close(fd);fd=child
        return fd
    except BaseException:os.close(fd);raise

def _mount_id(fd):
    with open('/proc/self/fdinfo/'+str(fd),encoding='ascii') as stream:
        values=[line.split(':',1)[1].strip() for line in stream if line.startswith('mnt_id:')]
    _require(len(values)==1 and values[0].isdecimal());return int(values[0])

def _mount_record(mount_id):
    with open('/proc/self/mountinfo','rb') as stream:data=stream.read(1024*1024+1)
    _require(len(data)<=1024*1024)
    matches=[]
    for line in data.decode('ascii').splitlines():
        left,sep,right=line.partition(' - ');_require(bool(sep))
        fields=left.split();tail=right.split();_require(len(fields)>=6 and len(tail)>=3)
        if fields[0]==str(mount_id):
            def decode(value):
                _require(re.search(r'\\(?!040|011|012|134)',value) is None)
                return re.sub(r'\\(040|011|012|134)',lambda m:chr(int(m.group(1),8)),value)
            matches.append((fields[0],fields[2],decode(fields[3]),decode(fields[4]),fields[5],tail[0],decode(tail[1]),tail[2]))
    _require(len(matches)==1);return matches[0]

def _project(fd):
    data=bytearray(28);fcntl.ioctl(fd,0x801c581f,data,True)
    flags,_,_,project,_=struct.unpack('=IIIII',data[:20]);return project,flags

def _kernel_quota(fd,project):
    """quotactl_fd x86-64; exact structures checked against host headers."""
    _require(sys.platform.startswith('linux') and platform.machine()=='x86_64'
             and ctypes.sizeof(ctypes.c_void_p)==8 and sys.byteorder=='little')
    libc=ctypes.CDLL(None,use_errno=True);libc.syscall.restype=ctypes.c_long
    # QCMD(('X'<<8)+operation, PRJQUOTA=2); syscall 443 quotactl_fd.
    def call(operation,identifier,buffer):
        result=libc.syscall(ctypes.c_long(443),ctypes.c_int(fd),ctypes.c_uint(((ord('X')<<8)+operation)<<8|2),
                            ctypes.c_uint(identifier),ctypes.byref(buffer))
        if result!=0:raise OSError(ctypes.get_errno(),'quota query failed')
    state=(ctypes.c_ubyte*160)();state[0]=1;call(8,0,state)
    _require(state[0]==1 and struct.unpack_from('=H',state,2)[0]&0x30==0x30)
    quota=(ctypes.c_ubyte*112)();call(3,project,quota)
    raw=bytes(quota);version,flags,_,returned=struct.unpack_from('=bbHI',raw,0)
    hard=struct.unpack_from('=Q',raw,8)[0]*512;used=struct.unpack_from('=Q',raw,40)[0]*512
    _require(version==1 and flags&2 and returned==project and hard>0 and used<=hard)
    return hard,used

def _observe_unqualified(mount_path,project_directory,project_id,hard_bytes):
    _require(sys.platform.startswith('linux') and os.geteuid()==0 and type(project_id) is int and 1<=project_id<=4294967295
             and type(hard_bytes) is int and hard_bytes>=512 and hard_bytes%512==0)
    mount=Path(mount_path);directory=Path(project_directory)
    _require(mount.is_absolute() and str(mount)==os.path.normpath(str(mount))
             and directory.parent==mount and directory.name not in ('','.','..'))
    root_fd=project_fd=None
    try:
        root_fd=_open_directory(mount)
        project_fd=os.open(directory.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=root_fd)
        before=os.fstat(root_fd);project_before=os.fstat(project_fd)
        mid=_mount_id(root_fd);record=_mount_record(mid)
        _require(_mount_id(project_fd)==mid and record[1]=='%d:%d'%(os.major(before.st_dev),os.minor(before.st_dev))
                 and record[2]=='/' and record[3]==str(mount) and record[5]=='xfs' and 'rw' in record[4].split(',')
                 and project_before.st_dev==before.st_dev)
        project,flags=_project(project_fd);_require(project==project_id and flags&0x200)
        hard,used=_kernel_quota(root_fd,project_id);_require(hard==hard_bytes)
        after=os.fstat(root_fd);project_after=os.fstat(project_fd)
        named=os.stat(directory.name,dir_fd=root_fd,follow_symlinks=False);named_root=os.stat(mount,follow_symlinks=False)
        _require((before.st_dev,before.st_ino)==(after.st_dev,after.st_ino)==(named_root.st_dev,named_root.st_ino)
                 and (project_before.st_dev,project_before.st_ino)==(project_after.st_dev,project_after.st_ino)==(named.st_dev,named.st_ino)
                 and _mount_id(root_fd)==_mount_id(project_fd)==mid and _mount_record(mid)==record
                 and _project(project_fd)==(project,flags))
        return {'projectId':project,'hardBytes':hard,'usedBytes':used,'mountId':mid}
    except (OSError,ValueError,UnicodeError,OverflowError):raise ObservationUnavailable('XFS quota observation unavailable') from None
    finally:
        if project_fd is not None:os.close(project_fd)
        if root_fd is not None:os.close(root_fd)
