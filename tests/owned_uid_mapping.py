"""Only map a blocked owned direct child after live identity checks."""
import fcntl,os,select
from pathlib import Path

def map_owned_child(pid,pidfd):
    def identity():
        raw=Path('/proc',str(pid),'stat').read_text(); fields=raw[raw.rfind(')')+2:].split()
        return int(fields[1]),int(fields[19])
    expected=identity()
    if expected[0]!=os.getpid(): raise RuntimeError('child parent mismatch')
    hostfd=os.open('/proc/self/ns/user',os.O_RDONLY|os.O_CLOEXEC)
    try:
        childfd=os.open('/proc/'+str(pid)+'/ns/user',os.O_RDONLY|os.O_CLOEXEC)
        try:
            if fcntl.ioctl(childfd,0xb703)!=0x10000000: raise RuntimeError('not a user namespace')
            a,b=os.fstat(hostfd),os.fstat(childfd)
            if (a.st_dev,a.st_ino)==(b.st_dev,b.st_ino): raise RuntimeError('child not isolated')
            for name,value in (('setgroups','deny'),('uid_map','0 0 65536\n'),('gid_map','0 0 65536\n')):
                if identity()!=expected or select.select([pidfd],[],[],0)[0]: raise RuntimeError('child changed')
                now=os.stat('/proc/'+str(pid)+'/ns/user')
                if (now.st_dev,now.st_ino)!=(b.st_dev,b.st_ino): raise RuntimeError('namespace changed')
                Path('/proc',str(pid),name).write_text(value)
        finally: os.close(childfd)
    finally: os.close(hostfd)
