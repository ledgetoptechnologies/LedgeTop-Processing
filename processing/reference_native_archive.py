"""Inert native archive ownership/cache helpers; no job completion or cleanup."""
import hashlib
import os
import re
import stat
import time
import zipfile
from urllib.error import HTTPError


class ArchiveHeld(ValueError):pass


def owned_file(ledger,reservation,relative,expected_size=None,expected_sha=None):
    ledger._owned_path(reservation,relative)
    path=ledger.root/relative
    descriptor=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0))
    try:
        before=os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1:raise ArchiveHeld('owned regular input required')
        digest=hashlib.sha256()
        with os.fdopen(os.dup(descriptor),'rb') as stream:
            while chunk:=stream.read(1024*1024):digest.update(chunk)
        after,current=os.fstat(descriptor),path.lstat()
        if ((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=
                (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)
                or (after.st_dev,after.st_ino)!=(current.st_dev,current.st_ino)):
            raise ArchiveHeld('owned file changed during verification')
        if expected_size is not None and before.st_size!=expected_size or expected_sha is not None and digest.hexdigest()!=expected_sha:
            raise ArchiveHeld('input integrity changed')
        return path,before.st_size,digest.hexdigest()
    finally:os.close(descriptor)


def part_fd(row,part,*,create=False):
    flags=os.O_RDWR|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0)
    if create:flags|=os.O_CREAT|os.O_EXCL
    descriptor=os.open(part,flags,0o600)
    try:
        observed,current=os.fstat(descriptor),part.lstat()
        if (not stat.S_ISREG(observed.st_mode) or observed.st_nlink!=1
                or (observed.st_dev,observed.st_ino)!=(current.st_dev,current.st_ino)
                or os.name!='nt' and observed.st_uid!=os.geteuid()
                or not create and (str(observed.st_dev),str(observed.st_ino))!=(row['part_dev'],row['part_ino'])):
            raise ArchiveHeld('archive cache ownership changed')
        return descriptor
    except BaseException:os.close(descriptor);raise


def download_archive(engine,row,reservation,part,budget,*,relative,file,phase,checkpoint,read_row,allocated=None,deadline_seconds=None):
    """One bounded GET into an exact inode-bound cache; caller owns journaling.

    Checkpoint commits cache identity before its first byte. A new successful GET
    must open before retained partial bytes can be truncated. No engine mutation,
    result registration, reservation settlement or parent completion occurs here.
    """
    if row['phase']=='archive_pending':
        descriptor=part_fd(row,part)
        try:size=os.fstat(descriptor).st_size
        finally:os.close(descriptor)
        expected=row['archive_expected_size']
        if expected is not None and size==int(expected) and 0<size<=budget and zipfile.is_zipfile(part):
            if allocated is not None:allocated(size)
            return file(reservation,relative)[1:]
    elif part.exists():raise ArchiveHeld('unbound archive cache requires reconciliation')
    deadline=time.monotonic()+deadline_seconds if deadline_seconds is not None else None
    try:response=engine.download(row['job'])
    except HTTPError as error:error.close();raise
    with response:
        if deadline is not None and time.monotonic()>=deadline:raise ArchiveHeld('archive response deadline expired')
        declared=getattr(response,'headers',{}).get('Content-Length')
        if declared is not None:
            if not isinstance(declared,str) or not re.fullmatch(r'[0-9]{1,16}',declared) or not 0<int(declared)<=budget:
                raise ArchiveHeld('archive declared length exceeds output budget')
            declared=int(declared)
        create=row['phase']!='archive_pending'
        if create:phase(row['job'],'archive_open_pending')
        descriptor=part_fd(row,part,create=create)
        try:
            observed=os.fstat(descriptor);os.fsync(descriptor)
            if create and os.name!='nt':
                directory_fd=os.open(part.parent,os.O_RDONLY|os.O_DIRECTORY)
                try:os.fsync(directory_fd)
                finally:os.close(directory_fd)
            checkpoint(row['job'],str(observed.st_dev),str(observed.st_ino),str(declared) if declared is not None else None)
            with os.fdopen(os.dup(descriptor),'wb') as output:
                output.truncate(0);output.seek(0);count=0
                try:
                    read=getattr(response,'read1',response.read) if deadline is not None else response.read
                    while True:
                        if deadline is not None:
                            remaining=deadline-time.monotonic()
                            if remaining<=0:raise ArchiveHeld('archive response deadline expired')
                            socket=getattr(getattr(getattr(response,'fp',None),'raw',None),'_sock',None)
                            if socket is not None:socket.settimeout(remaining)
                            elif not getattr(response,'isclosed',lambda:False)():raise ArchiveHeld('bounded archive stream required')
                        chunk=read(min(1024*1024,budget-count+1))
                        if not chunk:break
                        count+=len(chunk)
                        if count>budget:raise ArchiveHeld('archive exceeds scratch output budget')
                        output.write(chunk)
                        if deadline is not None and declared is not None and count==declared:break
                    output.flush();os.fsync(output.fileno())
                    if declared is not None and count!=declared:raise ArchiveHeld('archive response was truncated')
                finally:
                    # Persist/account bytes retained after a failed GET as well.
                    # This callback is optional so standalone behavior is intact.
                    if allocated is not None:
                        output.flush();os.fsync(output.fileno());allocated(os.fstat(descriptor).st_size)
        finally:os.close(descriptor)
    _,size,sha=file(reservation,relative)
    check=part_fd(read_row(row['job']),part);os.close(check)
    if not size or size>budget or not zipfile.is_zipfile(part):raise ArchiveHeld('invalid or oversized engine archive')
    return size,sha
