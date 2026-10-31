"""Unqualified outer launcher lock; no cutover or candidate start is enabled.

Only launchers required by an external host policy to acquire this exact lock
are excluded. Old binaries and privileged Docker administrators can bypass it.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import select
import stat

from ops.authority_handoff import DockerProcessAttestor,_read_private,_write_once
from ops.pr197_operation_fence import OperationFence
from ops.state_fence import held_flocks
from ops.stopped_viewer_backup import _lock


class OuterHeld(ValueError):pass


class OuterAuthorityGuard:
    def __init__(self,directory,authority,*,enabled=False):
        if enabled is not True or not re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',authority):
            raise OuterHeld('explicit_outer_authority_required')
        self.directory=Path(directory);self.authority=authority;self.pid=os.getpid()
        self.outer=None;self.pending=self.directory/(authority+'.outer-handoff');self.binding=None

    def _absent(self):
        try:self.pending.lstat()
        except FileNotFoundError:return
        except OSError:raise OuterHeld('outer_marker_unavailable') from None
        raise OuterHeld('outer_handoff_requires_reconciliation')

    def __enter__(self):
        directory=self.directory
        if (self.pid!=os.getpid() or self.outer is not None or not directory.is_absolute() or directory.resolve()!=directory):
            raise OuterHeld('outer_owner_or_directory_changed')
        info=directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o077:
            raise OuterHeld('private_outer_directory_required')
        self._absent()
        try:
            self.outer=_lock(directory/(self.authority+'.outer-lock'))
            held=os.fstat(self.outer);self.identity=(held.st_dev,held.st_ino)
            self._absent();self.check();return self
        except BaseException:
            self.close();raise

    def check(self):
        if self.pid!=os.getpid() or self.outer is None:raise OuterHeld('outer_owner_changed')
        try:
            held=os.fstat(self.outer);named=(self.directory/(self.authority+'.outer-lock')).lstat()
            owners=held_flocks(Path('/proc/locks').read_text())
            if ((held.st_dev,held.st_ino)!=self.identity or (named.st_dev,named.st_ino)!=self.identity
                or self.pid not in owners.get((os.major(held.st_dev),os.minor(held.st_dev),held.st_ino),set())):
                raise OuterHeld('outer_lock_lost')
        except OuterHeld:raise
        except OSError:raise OuterHeld('outer_lock_unavailable') from None

    def prepare_candidate(self,fence,candidate_state,image_id,*,operation_id):
        """Record one attempt then free only the inner lease; never start code."""
        if (type(fence) is not OperationFence or fence.authority!=self.authority or
            type(image_id) is not str or not re.fullmatch(r'sha256:[0-9a-f]{64}',image_id) or
            type(operation_id) is not str or not re.fullmatch(r'[0-9a-f]{32}',operation_id)):
            raise OuterHeld('outer_handoff_binding_required')
        self.check();fence.check();self._absent()
        state=Path(candidate_state)
        if not state.is_absolute() or state.resolve()!=state or state==fence.root:
            raise OuterHeld('separate_candidate_state_required')
        info=state.lstat();candidate_lock=(state/'coordinator.lock').lstat()
        if not stat.S_ISDIR(info.st_mode) or not stat.S_ISREG(candidate_lock.st_mode) or candidate_lock.st_nlink!=1:
            raise OuterHeld('candidate_state_identity_required')
        if (candidate_lock.st_dev,candidate_lock.st_ino)==fence.facts[1][1:]:
            raise OuterHeld('separate_candidate_lock_required')
        record={'schemaVersion':1,'operationId':operation_id,'authorityId':self.authority,'imageId':image_id,
                'candidateDevice':info.st_dev,'candidateInode':info.st_ino,
                'candidateLockDevice':candidate_lock.st_dev,'candidateLockInode':candidate_lock.st_ino}
        payload=json.dumps(record,sort_keys=True,separators=(',',':')).encode()
        _write_once(self.pending,payload)
        self.check();fence.check()
        # No descriptor is sent to the candidate. It must independently lock
        # the same authority inode. Failure at any later step leaves marker.
        os.close(fence.fds[0]);fence.fds[0]=None
        self.binding=(record,hashlib.sha256(payload).hexdigest(),fence)
        return {'state':'candidate_start_not_authorized','pending':True}

    def observe_candidate(self,pid,attestor,candidate_state):
        """Check exact process-owned independent locks; observation only."""
        if (self.binding is None or type(pid) is not int or pid<=0 or
            type(attestor) is not DockerProcessAttestor or not hasattr(os,'pidfd_open')):
            raise OuterHeld('candidate_observation_unavailable')
        record,digest,fence=self.binding
        if attestor.image!=record['imageId']:raise OuterHeld('candidate_image_changed')
        self.check()
        try:
            payload=_read_private(self.pending,4096)
            if hashlib.sha256(payload).hexdigest()!=digest:raise OuterHeld('outer_marker_changed')
            info=Path(candidate_state).lstat();lock=(Path(candidate_state)/'coordinator.lock').lstat()
            if (info.st_dev,info.st_ino,lock.st_dev,lock.st_ino)!=(record['candidateDevice'],record['candidateInode'],
                    record['candidateLockDevice'],record['candidateLockInode']):raise OuterHeld('candidate_state_changed')
            old=os.fstat(fence.fds[1]);lease_path,lease_dev,lease_ino=fence.facts[0]
            lease=lease_path.lstat()
            if (lease.st_dev,lease.st_ino)!=(lease_dev,lease_ino):raise OuterHeld('inner_lease_changed')
            pidfd=os.pidfd_open(pid,0)
            try:
                start=attestor.inspect(pid)
                owners=held_flocks(Path('/proc/locks').read_text())
                def held_by(st,owner):return owner in owners.get((os.major(st.st_dev),os.minor(st.st_dev),st.st_ino),set())
                if (select.select([pidfd],[],[],0)[0] or not held_by(lease,pid) or not held_by(lock,pid)
                        or not held_by(old,self.pid) or attestor.inspect(pid)!=start):
                    raise OuterHeld('candidate_lock_ownership_unverified')
            finally:os.close(pidfd)
            self.check()
            return {'candidateLocksObserved':True,'admissionAuthorized':False,'rollbackAuthorized':False}
        except OuterHeld:raise
        except Exception:raise OuterHeld('candidate_observation_unavailable') from None

    def close(self):
        if self.outer is not None:os.close(self.outer);self.outer=None

    def __exit__(self,*_):self.close()
