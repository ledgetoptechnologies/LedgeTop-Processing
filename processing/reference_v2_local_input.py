"""Disabled multi-object local input publication prototype; no network authority.

Requires the caller to fence all other writers, old binaries, engines and file
cleanup; cooperative WorkerFence alone cannot prove this. No engine start or hold release is provided. Crashes with a leftover partial file hold
for manual reconciliation; no file is automatically deleted on uncertainty.
"""
import hashlib
import os
import stat

from .reference_input_journal import InputJournal
from .reference_manifest_v2 import decode_manifest_v2
from .reference_worker_fence_v2 import WorkerFence
from .scratch_policy import ScratchLedger


class InputHeld(ValueError): pass


def publish_next_input(*, fence, journal, ledger, reservation_id, binding, manifest_bytes, source):
    """Publish the next object from a trusted byte stream or reconcile a published file.

    The stream, manifest origin and exclusive worker ownership are caller-owned;
    the digest is not authentication. source=None permits reconciliation only.
    The scratch hold is retained forever until an independently reviewed release.
    """
    if os.name != 'posix' or type(fence) is not WorkerFence or type(journal) is not InputJournal or type(ledger) is not ScratchLedger:
        raise InputHeld('qualified local components required')
    fence.check()
    journal.seed_manifest(binding,manifest_bytes)
    manifest=decode_manifest_v2(manifest_bytes)
    item=journal.current(binding)
    if item is None:
        raise InputHeld('all inputs journaled; hold remains')
    scope=dict(job_id=binding['jobId'],generation=binding['generation'],environment=binding['environment'],
               manifest_sha256=binding['manifestSha256'],input_bytes=manifest['totalBytes'])
    if item['ordinal']==0:
        ledger.begin_input_write_hold(reservation_id,**scope,object_id=item['objectId'],ordinal=0,target=item['target'])
    target_check=dict(scope,allocated_before=item['allocatedBefore'],target=item['target'])
    ledger.check_input_write_target(reservation_id,**target_check)
    fence.check()
    directory=ledger.ensure_job_directory(reservation_id)
    dfd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    target=item['objectId']+'.input'; partial=item['objectId']+'.part'
    try:
        di=os.fstat(dfd); pathinfo=os.stat(directory,follow_symlinks=False)
        if (di.st_dev,di.st_ino)!=(pathinfo.st_dev,pathinfo.st_ino) or di.st_uid!=os.geteuid() or stat.S_IMODE(di.st_mode)!=0o700:
            raise InputHeld('private directory changed')
        try: os.stat(partial,dir_fd=dfd,follow_symlinks=False)
        except FileNotFoundError: pass
        else: raise InputHeld('partial input requires reconciliation')
        try: fd=os.open(target,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC,dir_fd=dfd)
        except FileNotFoundError:
            if source is None:raise InputHeld('input not published') from None
            journal.attempt(binding,item['ordinal'])
            fd=os.open(partial,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=dfd)
            try:
                digest=hashlib.sha256();remaining=item['byteSize']
                while remaining:
                    chunk=source.read(min(1024*1024,remaining))
                    if type(chunk) is not bytes or not chunk or len(chunk)>remaining:raise InputHeld('input length mismatch')
                    digest.update(chunk);remaining-=len(chunk)
                    view=memoryview(chunk)
                    while view:
                        written=os.write(fd,view)
                        if written<=0:raise InputHeld('input write failed')
                        view=view[written:]
                if source.read(1)!=b'' or digest.hexdigest()!=item['sha256']:raise InputHeld('input integrity mismatch')
                os.fsync(fd)
            finally: os.close(fd)
            fence.check()
            os.link(partial,target,src_dir_fd=dfd,dst_dir_fd=dfd,follow_symlinks=False)
            os.unlink(partial,dir_fd=dfd);os.fsync(dfd)
            fd=os.open(target,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC,dir_fd=dfd)
        try:
            before=os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_uid!=os.geteuid() or stat.S_IMODE(before.st_mode)!=0o600 or before.st_nlink!=1 or before.st_size!=item['byteSize']:
                raise InputHeld('published input identity invalid')
            digest=hashlib.sha256();count=0
            while True:
                chunk=os.read(fd,1024*1024)
                if not chunk:break
                count+=len(chunk)
                if count>item['byteSize']:raise InputHeld('input length mismatch')
                digest.update(chunk)
            after=os.fstat(fd);entry=os.stat(target,dir_fd=dfd,follow_symlinks=False)
            if ((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)
                    or (entry.st_dev,entry.st_ino)!=(after.st_dev,after.st_ino) or count!=item['byteSize'] or digest.hexdigest()!=item['sha256']):
                raise InputHeld('published input changed')
        finally:os.close(fd)
        current=journal.current(binding)
        if current is None or current['ordinal']!=item['ordinal'] or current['attempts']<1 or current['target']!=item['target']:
            raise InputHeld('persisted input attempt required')
        fence.check()
        ledger.check_input_write_target(reservation_id,**target_check)
        ledger.record_allocated(reservation_id,item['target'])
        fence.check()
        journal.complete(binding,item['ordinal'],item['objectId'],item['byteSize'],item['sha256'],item['target'])
        return {'state':'input_verified_hold_retained','objectId':item['objectId']}
    finally:os.close(dfd)
