"""Host-wide exclusive lease on the coordinator authority.

``coordinator.lock`` lives inside the state volume, so it cannot stop two
coordinators that run on *copies* of that volume with the same authority. This
lease lives in a separate host directory that every coordinator container
bind-mounts. It is an OS ``flock`` on ``<authority>.lease``: held for the life
of the process, released by the kernel when the process exits, and never
granted to a second holder. Acquisition happens before the state database is
opened for writing.

It fences only coordinators that run this code with the same lease directory.
It does not fence the d738 image, which has no lease, or a coordinator started
with a different lease directory.
"""

import json
import os
from pathlib import Path
import re
import sqlite3
import time

AUTHORITY = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
MARKER_DDL = ('CREATE TABLE coordinator_authority_lease (singleton INTEGER PRIMARY KEY CHECK(singleton=1), '
              'required INTEGER NOT NULL CHECK(required=1))')


class LeaseUnavailable(RuntimeError):
    pass


def _require_no_handoff_markers(*paths):
    for path in paths:
        try:path.lstat()
        except FileNotFoundError:continue
        except OSError:raise LeaseUnavailable('authority_handoff_marker_unavailable') from None
        raise LeaseUnavailable('authority_handoff_requires_reconciliation')


def existing_authority(state_dir):
    """Read the stored authority without creating or modifying any state."""
    path = Path(state_dir) / 'jobs.sqlite3'
    if not path.is_file():
        return None
    db = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)
    try:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_identity'").fetchone():
            return None
        row = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
        return row[0] if row else None
    finally:
        db.close()


class AuthorityLease:
    def __init__(self, directory, authority):
        if type(authority) is not str or not AUTHORITY.fullmatch(authority):
            raise LeaseUnavailable('authority_lease_invalid_authority')
        directory = Path(directory)
        if directory.is_symlink() or not directory.is_dir():
            raise LeaseUnavailable('authority_lease_directory_unavailable')
        self.authority = authority
        self.path = directory / (authority + '.lease')
        intent = directory / (authority + '.handoff-intent')
        receipt = directory / (authority + '.handoff-intent.ack')
        # A durable transfer intent means GO may have reached another
        # coordinator, even when the prior lock owner is gone. Ordinary startup
        # never resolves or clears it. Legacy binaries do not enforce this.
        _require_no_handoff_markers(intent,receipt)
        import fcntl
        try:
            descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        except OSError:
            raise LeaseUnavailable('authority_lease_unavailable') from None
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            _require_no_handoff_markers(intent,receipt)
        except LeaseUnavailable:
            os.close(descriptor)
            raise
        except OSError:
            os.close(descriptor)
            raise LeaseUnavailable('authority_lease_held') from None
        self._descriptor = descriptor
        # Diagnostic only; the flock, not this content, is the lease.
        record = json.dumps({'authorityId': authority, 'pid': os.getpid(), 'host': os.uname().nodename,
                             'acquiredAt': int(time.time())}).encode() + b'\n'
        os.ftruncate(descriptor, 0)
        os.pwrite(descriptor, record, 0)
        os.fsync(descriptor)

    def close(self):
        descriptor, self._descriptor = getattr(self, '_descriptor', None), None
        if descriptor is not None:
            os.close(descriptor)


def acquire(directory, state_dir):
    """Lease the authority already stored in ``state_dir``; refuse fresh state."""
    authority = existing_authority(state_dir)
    if authority is None:
        raise LeaseUnavailable('authority_lease_requires_existing_authority')
    return AuthorityLease(directory, authority)


def acquire_transferred(sock, directory, state_dir, *, supervisor_uid, operation_id,
                        container_id, image_id, experimental=False):
    """Receive one held lease FD from a trusted supervisor before DB writes.

    Not wired to application startup: host writer exclusion, exact container
    attestation, supervisor lifetime and lost-ACK recovery are unqualified.
    """
    import array, fcntl, hashlib, socket, stat, struct
    if experimental is not True or sock.family!=socket.AF_UNIX or not hasattr(socket,'SO_PEERCRED'):
        raise LeaseUnavailable('experimental_handoff_required')
    fd=None;fds=array.array('i')
    try:
        _pid,uid,_gid=struct.unpack('3i',sock.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,struct.calcsize('3i')))
        if uid!=supervisor_uid:raise ValueError()
        authority=existing_authority(state_dir)
        if authority is None or not AUTHORITY.fullmatch(authority):raise ValueError()
        sock.settimeout(5)
        data,ancillary,flags,_=sock.recvmsg(4100,socket.CMSG_SPACE(array.array('i').itemsize))
        unexpected=False
        for level,kind,value in ancillary:
            if level==socket.SOL_SOCKET and kind==socket.SCM_RIGHTS:
                fds.frombytes(value[:len(value)-(len(value)%fds.itemsize)])
            else:unexpected=True
        if flags & (socket.MSG_CTRUNC|socket.MSG_TRUNC) or unexpected or len(fds)!=1:raise ValueError()
        fd=fds.pop()
        os.set_inheritable(fd,False)
        while len(data)<4:
            part=sock.recv(4-len(data))
            if not part:raise ValueError()
            data+=part
        size=int.from_bytes(data[:4],'big')
        if size<1 or size>4096 or len(data)>size+4:raise ValueError()
        while len(data)<size+4:
            part=sock.recv(size+4-len(data))
            if not part:raise ValueError()
            data+=part
        def unique(pairs):
            result={}
            for key,item in pairs:
                if key in result:raise ValueError()
                result[key]=item
            return result
        payload=data[4:];value=json.loads(payload,object_pairs_hook=unique,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        keys={'version','operationId','authorityId','pid','startTime','containerId','imageId','leaseDevice','leaseInode'}
        if (type(value) is not dict or set(value)!=keys or value['version']!=1 or value['operationId']!=operation_id
                or value['authorityId']!=authority or value['containerId']!=container_id or value['imageId']!=image_id):
            raise ValueError()
        held=os.fstat(fd);named=(Path(directory)/(authority+'.lease')).lstat()
        if (not stat.S_ISREG(held.st_mode) or held.st_nlink!=1 or not stat.S_ISREG(named.st_mode)
                or (held.st_dev,held.st_ino)!=(named.st_dev,named.st_ino)
                or (held.st_dev,held.st_ino)!=(value['leaseDevice'],value['leaseInode'])):raise ValueError()
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        # A different open description must see the same inode locked.
        probe=os.open(Path(directory)/(authority+'.lease'),os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:
            info=os.fstat(probe)
            if (info.st_dev,info.st_ino)!=(held.st_dev,held.st_ino):raise ValueError()
            try:fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:pass
            else:raise ValueError()
        finally:os.close(probe)
        sock.sendall(hashlib.sha256(payload).hexdigest().encode()+b'\n')
        response=b''
        while len(response)<3:
            part=sock.recv(3-len(response))
            if not part:raise ValueError()
            response+=part
        if response!=b'GO\n':raise ValueError()
        lease=object.__new__(AuthorityLease);lease.authority=authority
        lease.path=Path(directory)/(authority+'.lease');lease._descriptor=fd;fd=None
        return lease
    except Exception:raise LeaseUnavailable('authority_handoff_uncertain') from None
    finally:
        if fd is not None:os.close(fd)
        for extra in fds:os.close(extra)


def record_required(queue, lease):
    """Persist that this state needs a lease, after checking the queue's authority."""
    if queue.authority_id != lease.authority:
        raise LeaseUnavailable('authority_lease_authority_changed')
    with queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        existing = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='coordinator_authority_lease'").fetchone()
        if existing is None:
            db.execute(MARKER_DDL)
            db.execute('INSERT INTO coordinator_authority_lease VALUES(1,1)')
        elif existing[0] != MARKER_DDL or db.execute('SELECT COUNT(*) FROM coordinator_authority_lease').fetchone()[0] != 1:
            raise LeaseUnavailable('authority_lease_marker_changed')


def required(state_dir):
    """Read-only check, before any state write, that this state demands a lease."""
    path = Path(state_dir) / 'jobs.sqlite3'
    if not path.is_file():
        return False
    db = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)
    try:
        return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_authority_lease'").fetchone() is not None
    finally:
        db.close()
