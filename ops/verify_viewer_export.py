"""Strict, bounded extractor/verifier for the two pinned Viewer subtrees."""
import hashlib,os,stat,sys,tarfile
from pathlib import Path,PurePosixPath

LIMIT=2*1024*1024*1024
MAX_ENTRIES=50000

class InvalidExport(ValueError):pass

def _inventory(archives):
    expected={};total=0
    for filename,prefix in archives:
        with tarfile.open(filename,'r:') as tar:
            for member in tar:
                name=member.name.rstrip('/')
                parts=name.split('/')
                if (not name or name.startswith('/') or '\\' in name or any(x in ('','.','..') for x in parts)
                    or not (name==prefix or name.startswith(prefix+'/'))):raise InvalidExport('unsafe archive path')
                if name in expected:raise InvalidExport('duplicate archive path')
                if not (member.isdir() or member.isfile()):raise InvalidExport('unsupported archive type')
                mode=member.mode & 0o7777
                if mode not in ((0o755,) if member.isdir() else (0o644,0o755)):raise InvalidExport('unsafe archive mode')
                if member.size<0 or member.size>LIMIT:raise InvalidExport('archive size invalid')
                total+=member.size
                if total>LIMIT or len(expected)>=MAX_ENTRIES:raise InvalidExport('archive limit exceeded')
                digest=None
                if member.isfile():
                    h=hashlib.sha256();stream=tar.extractfile(member)
                    if stream is None:raise InvalidExport('archive file unavailable')
                    remaining=member.size
                    while remaining:
                        chunk=stream.read(min(1024*1024,remaining))
                        if not chunk:raise InvalidExport('truncated archive')
                        remaining-=len(chunk);h.update(chunk)
                    digest=h.hexdigest()
                expected[name]=(mode,member.size,digest)
        if prefix not in expected:raise InvalidExport('archive prefix missing')
    for path in list(expected):
        for parent in PurePosixPath(path).parents:
            if str(parent)=='.':break
            entry=expected.setdefault(str(parent),(0o755,0,None))
            if entry[2] is not None:raise InvalidExport('file used as directory')
    return expected

def check(root,archives,*,extract=False):
    root=Path(root);info=root.lstat()
    if not stat.S_ISDIR(info.st_mode) or root.is_symlink() or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o700:raise InvalidExport('export root invalid')
    expected=_inventory(archives)
    if extract:
        if any(root.iterdir()):raise InvalidExport('export root not empty')
        for name,(mode,size,digest) in sorted(expected.items(),key=lambda item:(item[0].count('/'),item[0])):
            if digest is None:
                path=root/name;path.mkdir(mode=0o755);os.chmod(path,0o755)
        for filename,prefix in archives:
            with tarfile.open(filename,'r:') as tar:
                for member in tar:
                    if not member.isfile():continue
                    name=member.name
                    if name not in expected or expected[name]!=(member.mode & 0o7777,member.size,expected[name][2]) or expected[name][2] is None:
                        raise InvalidExport('archive changed during extraction')
                    path=root/name
                    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
                    try:
                        os.fchmod(fd,member.mode);source=tar.extractfile(member)
                        with os.fdopen(fd,'wb',closefd=False) as target:
                            while True:
                                chunk=source.read(1024*1024)
                                if not chunk:break
                                target.write(chunk)
                    finally:os.close(fd)
    found=set()
    for current,dirs,files in os.walk(root,followlinks=False):
        for item in dirs+files:
            path=Path(current)/item;name=path.relative_to(root).as_posix();found.add(name)
            if name not in expected:raise InvalidExport('unexpected export path')
            mode,size,digest=expected[name];entry=path.lstat()
            if entry.st_uid!=os.geteuid() or entry.st_gid!=os.getegid() or stat.S_IMODE(entry.st_mode)!=mode:
                raise InvalidExport('export ownership or mode changed')
            if digest is None:
                if not stat.S_ISDIR(entry.st_mode):raise InvalidExport('export type changed')
            else:
                if not stat.S_ISREG(entry.st_mode) or entry.st_nlink!=1 or entry.st_size!=size:raise InvalidExport('export type or size changed')
                fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
                try:
                    h=hashlib.sha256()
                    while True:
                        data=os.read(fd,1024*1024)
                        if not data:break
                        h.update(data)
                finally:os.close(fd)
                if h.hexdigest()!=digest:raise InvalidExport('export bytes changed')
    if found!=set(expected):raise InvalidExport('export path missing')

def main():
    if len(sys.argv)!=5 or sys.argv[1] not in ('extract','verify'):raise SystemExit('invalid arguments')
    _,mode,root,app,shared=sys.argv
    try:check(root,[(app,'apps/viewer'),(shared,'packages/shared')],extract=mode=='extract')
    except (InvalidExport,OSError,tarfile.TarError) as error:raise SystemExit('Viewer export rejected: '+str(error)) from None
if __name__=='__main__':main()
