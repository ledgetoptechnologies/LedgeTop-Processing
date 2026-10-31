"""Opt-in read-only host observation, not an archive-write authority.

Requires trusted host administrators, tools and mount namespace. It does not
establish exclusive writers, physical free space or a safe retry reservation.
Nothing invokes this module automatically.
"""
import fcntl
import json
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import struct
import subprocess
import sys
import time

class ObservationUnavailable(ValueError): pass


def _require(value):
    if not value: raise ObservationUnavailable('XFS quota observation unavailable')


def _run(argv):
    """Bound elapsed time and combined output, including failing children."""
    process=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                             env={'PATH':'/usr/sbin:/usr/bin:/sbin:/bin','LC_ALL':'C'},start_new_session=True)
    selector=selectors.DefaultSelector();out=bytearray();err=bytearray();deadline=time.monotonic()+5
    try:
        selector.register(process.stdout,selectors.EVENT_READ,out);selector.register(process.stderr,selectors.EVENT_READ,err)
        while selector.get_map():
            timeout=deadline-time.monotonic();_require(timeout>0)
            events=selector.select(timeout);_require(bool(events))
            for key,_ in events:
                data=os.read(key.fd,4096)
                if not data:selector.unregister(key.fileobj);continue
                key.data.extend(data);_require(len(out)+len(err)<=16384)
        code=process.wait(timeout=max(.01,deadline-time.monotonic()))
        _require(code==0 and not err)
        return out.decode('ascii')
    except BaseException:
        if process.poll() is None:
            try:os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            process.wait(timeout=5)
        raise
    finally:
        selector.close();process.stdout.close();process.stderr.close()


def _mount(path):
    value=json.loads(_run(['/usr/bin/findmnt','--json','--target',path,'--output','ID,TARGET,SOURCE,FSTYPE,OPTIONS,MAJ:MIN']))
    mounts=value.get('filesystems');_require(type(mounts) is list and len(mounts)==1)
    mount=mounts[0];_require(type(mount) is dict and set(mount)=={'id','target','source','fstype','options','maj:min'}
                                  and type(mount['id']) is int and mount['id']>0)
    _require(mount['target']==path and mount['fstype']=='xfs' and type(mount['source']) is str
             and re.fullmatch(r'[0-9]+:[0-9]+',mount['maj:min']) is not None
             and type(mount['options']) is str and 'rw' in mount['options'].split(','))
    return mount


def _project(fd):
    data=bytearray(28);fcntl.ioctl(fd,0x801c581f,data,True)
    flags,_,_,project,_=struct.unpack('=IIIII',data[:20]);return project,flags


def observe(mount_path,project_directory,project_id,hard_bytes):
    """Snapshot only; callers must not treat success as permission to write."""
    _require(sys.platform.startswith('linux') and os.geteuid()==0 and type(project_id) is int and 1<=project_id<=4294967295
             and type(hard_bytes) is int and hard_bytes>=1024 and hard_bytes%1024==0)
    mount=Path(mount_path);directory=Path(project_directory)
    _require(mount.is_absolute() and str(mount)==os.path.normpath(str(mount))
             and directory.parent==mount and directory.name not in ('','.','..'))
    root_fd=project_fd=None
    try:
        flags=os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC
        root_fd=os.open(mount,flags);project_fd=os.open(directory.name,flags,dir_fd=root_fd)
        before=os.fstat(root_fd);project_before=os.fstat(project_fd)
        snapshot=_mount(str(mount));_require(snapshot['maj:min']=='%d:%d'%(os.major(before.st_dev),os.minor(before.st_dev)))
        _require(project_before.st_dev==before.st_dev)
        project,project_flags=_project(project_fd);_require(project==project_id and project_flags&0x200)
        state=_run(['/usr/sbin/xfs_quota','-x','-c','state -p',str(mount)])
        _require(re.findall(r'^Project quota state on (.+)$',state,re.M)==[str(mount)+' ('+snapshot['source']+')']
                 and re.findall(r'^\s*Accounting:\s*(\S+)\s*$',state,re.M)==['ON']
                 and re.findall(r'^\s*Enforcement:\s*(\S+)\s*$',state,re.M)==['ON'])
        report=_run(['/usr/sbin/xfs_quota','-x','-c','report -p -n -N',str(mount)])
        rows=[re.fullmatch(r'#([0-9]+)\s+([0-9]+)\s+([0-9]+)\s+([0-9]+)\s+\S+\s+\[[^\]]+\]',line.strip()) for line in report.splitlines() if line.strip()]
        _require(all(rows));selected=[m for m in rows if int(m.group(1))==project_id];_require(len(selected)==1)
        used,hard=int(selected[0].group(2))*1024,int(selected[0].group(4))*1024
        _require(hard==hard_bytes and used<=hard)
        after=os.fstat(root_fd);project_after=os.fstat(project_fd);named=os.stat(directory.name,dir_fd=root_fd,follow_symlinks=False)
        named_root=os.stat(mount,follow_symlinks=False)
        _require((before.st_dev,before.st_ino)==(after.st_dev,after.st_ino)==(named_root.st_dev,named_root.st_ino)
                 and (project_before.st_dev,project_before.st_ino)==(project_after.st_dev,project_after.st_ino)==(named.st_dev,named.st_ino)
                 and _mount(str(mount))==snapshot and _project(project_fd)==(project,project_flags))
        return {'projectId':project,'hardBytes':hard,'usedBytes':used,'device':snapshot['maj:min']}
    except (OSError,ValueError,KeyError,UnicodeError,subprocess.SubprocessError):
        raise ObservationUnavailable('XFS quota observation unavailable') from None
    finally:
        if project_fd is not None:os.close(project_fd)
        if root_fd is not None:os.close(root_fd)
