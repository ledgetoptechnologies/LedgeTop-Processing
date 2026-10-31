"""Narrow synthetic predecessor census; not a cutover or writer fence.

Only exact schemas observed from the installed predecessor image are admitted.
An externally read-only mount does not exclude writers in other namespaces.
"""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat

from ops.state_census import census
from ops.stopped_viewer_backup import _mount_path

PREDECESSOR_REVISION='693cb7eca32295da959a4f42399f317577ef8a8a'
PREDECESSOR_IMAGE='sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004'
SCHEMAS=frozenset(('e0f9dbbc692a81273d3ad105600a420a246e4e682efb0b9bb4724c6536441799',
                   '249bfba270623180a562922e921a7b95f28c836b833837cc546d7322c38a16d6'))
MAX_LIMITS={'entries':25000,'file_bytes':1024**3,'total_bytes':2*1024**3,
            'database_bytes':64*1024**2,'rows':50000,'row_bytes':1024**2,'row_total_bytes':32*1024**2}
DEFAULT_LIMITS={'entries':5000,'file_bytes':64*1024**2,'total_bytes':256*1024**2,
                'database_bytes':8*1024**2,'rows':4096,'row_bytes':256*1024,'row_total_bytes':8*1024**2}

class CensusHeld(ValueError):pass


def _identity(fd):
    info=os.fstat(fd)
    try:
        with open('/proc/self/fdinfo/'+str(fd),encoding='ascii') as stream:
            values=[line.split(':',1)[1].strip() for line in stream if line.startswith('mnt_id:')]
        if len(values)!=1 or not values[0].isdecimal():raise ValueError()
        return info.st_dev,info.st_ino,int(values[0])
    except (OSError,ValueError):raise CensusHeld('mount_identity_unavailable') from None


def _mount(root):
    if os.name!='posix' or not hasattr(os,'O_NOFOLLOW') or not root.is_absolute() or root.resolve()!=root:
        raise CensusHeld('canonical_linux_mount_required')
    try:
        entries=[]
        with open('/proc/self/mountinfo',encoding='utf-8') as stream:
            for line in stream:
                fields=line.split()
                if len(fields)<10:raise ValueError()
                entries.append((Path(_mount_path(fields[4])),fields[5].split(',')))
        exact=[options for path,options in entries if path==root]
        if len(exact)!=1 or 'ro' not in exact[0] or any(root in path.parents for path,_ in entries):
            raise CensusHeld('readonly_unshared_mount_root_required')
    except CensusHeld:raise
    except (OSError,ValueError):raise CensusHeld('mountinfo_unavailable') from None


def stopped_census(root,key,*,limits=None):
    """Read-only keyed census. Never grants writer, cutover, or rollback authority."""
    root=Path(root)
    if type(key) is not bytes or len(key)!=32:raise CensusHeld('census_key_required')
    if limits is None:limits=DEFAULT_LIMITS.copy()
    if (type(limits) is not dict or set(limits)!=set(MAX_LIMITS) or
        any(type(limits[name]) is not int or not 1<=limits[name]<=maximum for name,maximum in MAX_LIMITS.items())):
        raise CensusHeld('bounded_census_limits_required')
    limits=limits.copy()
    _mount(root)
    directory=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        identity=_identity(directory)
        db_path=root/'jobs.sqlite3';info=db_path.lstat()
        snapshot=lambda item:(item.st_dev,item.st_ino,item.st_mode,item.st_uid,item.st_gid,item.st_nlink,item.st_size,item.st_mtime_ns,item.st_ctime_ns)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size>limits['database_bytes']:
            raise CensusHeld('bounded_regular_database_required')
        if any(os.path.lexists(str(db_path)+suffix) for suffix in ('-wal','-shm','-journal')):
            raise CensusHeld('sqlite_sidecar_requires_checkpoint')
        with db_path.open('rb') as stream:header=stream.read(20)
        if len(header)!=20 or header[:16]!=b'SQLite format 3\x00' or header[18:20]!=b'\x01\x01':
            raise CensusHeld('checkpointed_sqlite_required')
        db=sqlite3.connect(db_path.as_uri()+'?mode=ro&immutable=1',uri=True)
        try:
            objects=db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name').fetchall()
            schema=hashlib.sha256(json.dumps(objects,separators=(',',':'),ensure_ascii=True).encode()).hexdigest()
        finally:db.close()
        if schema not in SCHEMAS:raise CensusHeld('unqualified_predecessor_schema')
        document=census(root,key,limits=limits)
        if (document['database']['quickCheck']!='ok' or document['database']['sidecars'] or
            any(document['tree']['rejected'].values()) or snapshot(db_path.lstat())!=snapshot(info)):
            raise CensusHeld('unstable_or_unsupported_state')
        named=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            if _identity(named)!=identity or _identity(directory)!=identity:raise CensusHeld('mount_identity_changed')
        finally:os.close(named)
        # Schema equality cannot establish which executable wrote the state.
        return {'schemaProfileRevision':PREDECESSOR_REVISION,'schemaProfileImage':PREDECESSOR_IMAGE,'schemaSha256':schema,
                'limits':limits,'writerFenceQualified':False,'cutoverAuthorized':False,'rollbackAuthorized':False,'census':document}
    except CensusHeld:raise
    except (OSError,sqlite3.Error,KeyError,ValueError):raise CensusHeld('census_unavailable') from None
    finally:os.close(directory)
