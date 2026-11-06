"""Unwired local whole-source verification; no Viewer ACK or execution grant."""
import os
from pathlib import Path
import stat
import uuid

from . import reference_protocol as p
from .direct_worker_receiver import _object, check_scope, derived_digest
from .direct_worker_scratch import require_fenced, scratch_settings
from .node_gate import NodeGate
from .scratch_policy import PolicyConflict


def _persist_receipt(parent, expected, *, name='direct-source-closure.json'):
    """Publish/revalidate exact local receipt; uncertain outcomes retain pin."""
    directory=os.open(parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    if name not in ('direct-source-closure.json','direct-worker-signed-closure.json',
                    'direct-v2-source-closure.json'):
        raise ValueError('closed receipt name required')
    raw=p.canonical_json(expected)
    try:
        info=os.fstat(directory)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o077:
            raise PolicyConflict('direct closure directory changed')
        try:
            fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
        except FileNotFoundError:
            temp='.direct-closure-'+uuid.uuid4().hex+'.part'
            out=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=directory)
            with os.fdopen(out,'wb') as stream:
                stream.write(raw);stream.flush();os.fsync(stream.fileno())
            os.link(temp,name,src_dir_fd=directory,dst_dir_fd=directory,follow_symlinks=False)
            os.unlink(temp,dir_fd=directory);os.fsync(directory)
            fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
        try:
            before=os.fstat(fd)
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_uid!=os.geteuid()
                    or before.st_mode&0o077 or before.st_size!=len(raw) or os.read(fd,len(raw)+1)!=raw):
                raise PolicyConflict('direct closure receipt changed')
            os.fsync(fd);after=os.fstat(fd);current=os.stat(name,dir_fd=directory,follow_symlinks=False)
            fields=('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns')
            if any(getattr(before,k)!=getattr(after,k) or getattr(after,k)!=getattr(current,k) for k in fields):
                raise PolicyConflict('direct closure receipt changed')
        finally:os.close(fd)
        os.fsync(directory)
    finally:os.close(directory)


def verify_closure(ledger,spool,manifest,item,*,derived_ids,expected_worker,engine_id,
                   coordinator_origin,gate_generation,worker_signer=None):
    """Persist a local observation only after every selected byte is verified."""
    item,sha,scratch_id=require_fenced(ledger,item,expected_worker=expected_worker)
    if type(manifest) is not dict or type(manifest.get('objects')) is not list or not manifest['objects']:
        raise PolicyConflict('direct manifest invalid')
    _object(manifest,item,manifest['objects'][0]['id'],derived_ids)
    if (derived_digest(manifest,derived_ids)!=item['derivedManifestSha256']
            or len(derived_ids)>=len(manifest['objects'])):
        raise PolicyConflict('direct derived inventory changed')
    result=dict(protocol='ltds-direct-local-closure-v1',reservationId=item['reservationId'],
        scratchBindingSha256=sha,authorityId=item['authorityId'],nodeId=item['nodeId'],
        workerId=item['workerId'],workerInstanceId=item['workerInstanceId'],jobId=item['jobId'],
        attemptId=item['attemptId'],generation=item['generation'],environment=item['environment'],
        manifestSha256=item['manifestSha256'],derivedManifestSha256=item['derivedManifestSha256'],
        sourceObjectCount=len(manifest['objects'])-len(derived_ids),derivedObjectCount=len(derived_ids),
        localClosureComplete=True,sourceCustodyAccepted=False,processingAuthorized=False)
    if 'limitProfile' in item:
        result['limitProfile']=item['limitProfile']
    with NodeGate(engine_id) as gate:
        check_scope(ledger,spool,item,sha,scratch_id,gate,coordinator_origin=coordinator_origin,gate_generation=gate_generation)
        receipt_parent=spool.root.parent
        if (receipt_parent!=spool.mount_path and spool.mount_path not in receipt_parent.parents):
            raise PolicyConflict('direct closure receipt outside verified mount')
        spool._assert_mount()
        if receipt_parent.stat().st_dev!=spool._mount_identity[0]:
            raise PolicyConflict('direct closure receipt mount changed')
        # A premature completion request with obviously absent objects must not
        # strand the transfer. This metadata-only probe is advisory; the later
        # pinned hash checks are authoritative and fail closed on any race.
        for obj in manifest['objects']:
            try:(spool.root/spool.storage_key(item['jobId'],item['generation'],obj['id'])).lstat()
            except FileNotFoundError:raise PolicyConflict('direct closure incomplete') from None
        pin=ledger.begin_direct_writer(scratch_id,scratch_settings(item,scratch_id))
        # Never use audit_job here: it removes partials. Unknown entries or a
        # missing/changed object hold the operation and preserve the pin.
        for obj in manifest['objects']:
            spool.verify_object(job_id=item['jobId'],generation=item['generation'],object_id=obj['id'],
                                byte_size=obj['byteSize'],sha256=obj['sha256'])
        directory=spool.root/item['jobId']/str(item['generation'])
        names={Path(spool.storage_key(item['jobId'],item['generation'],o['id'])).name for o in manifest['objects']}
        with os.scandir(directory) as entries:
            found=set()
            for entry in entries:
                if len(found)>=len(names):raise PolicyConflict('direct closure unexpected file')
                found.add(entry.name)
        if found!=names:raise PolicyConflict('direct closure incomplete')
        spool._fsync_directory(directory)
        spool._assert_mount()
        _persist_receipt(receipt_parent,result)
        spool._assert_mount()
        signed=None
        if worker_signer is not None:
            from .direct_worker_receipt import WorkerClosureSigner, verify
            if not isinstance(worker_signer,WorkerClosureSigner):
                raise PolicyConflict('pinned worker signer required')
            signed=worker_signer.issue(result)
            verify(signed,expected_closure=result,issuer_id=worker_signer.issuer_id,
                   key_id=worker_signer.key_id,public_key=worker_signer._key.public_key())
            _persist_receipt(receipt_parent,signed,name='direct-worker-signed-closure.json')
            spool._assert_mount()
        require_fenced(ledger,item,expected_worker=expected_worker)
        spool._assert_mount()
        ledger._retire_direct_writer(scratch_id,pin)
    return signed if worker_signer is not None else result
