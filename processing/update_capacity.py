"""Shared per-Engine staging accounting. No pulls, pruning, or time-based expiry."""
from contextlib import contextmanager
from dataclasses import asdict,dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading

from .update_registry import DockerRegistry,IMAGE_MEDIA,NAME,TAG,_repository,_run

GiB=1024**3
MAX_BYTES=2**53-1
SCHEMA=(
    'CREATE TABLE IF NOT EXISTS capacity_binding(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)',
    '''CREATE TABLE IF NOT EXISTS reservations (
        pull_id TEXT PRIMARY KEY,binding TEXT NOT NULL,bytes INTEGER NOT NULL,
        first_candidate_id TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('active','released')),release_proof TEXT)''',
    'CREATE TABLE IF NOT EXISTS legacy_closures(path TEXT PRIMARY KEY,inventory TEXT NOT NULL,proof TEXT NOT NULL)')


class CapacityError(ValueError):pass
class CapacityBusy(CapacityError):pass


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def integer(value,minimum=0):
    if type(value) is not int or not minimum<=value<=MAX_BYTES:raise CapacityError('invalid_capacity_bound')
    return value


def sha(value,prefixed=False):
    if type(value) is not str or not re.fullmatch(('sha256:' if prefixed else '')+r'[a-f0-9]{64}',value):raise CapacityError('invalid_artifact_digest')


def absolute(value):
    path=Path(value)
    if (not path.is_absolute() or path!=Path(os.path.normpath(path)) or '..' in path.parts
            or type(value) is str and str(path)!=value):
        raise CapacityError('fixed_absolute_path_required')
    return path


@dataclass(frozen=True)
class CapacityPolicy:
    docker_executable:str
    docker_endpoint:str
    engine_id:str
    docker_root_dir:str
    min_free_bytes:int=100*GiB
    reservation_bytes:int=64*GiB
    probe_timeout_seconds:int=10
    legacy_capacity_journals:tuple[str,...]=()

    def __post_init__(self):
        absolute(self.docker_executable);absolute(self.docker_root_dir)
        if (type(self.engine_id) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}',self.engine_id)
                or type(self.docker_endpoint) is not str or not re.fullmatch(r'unix:///[A-Za-z0-9_./-]+',self.docker_endpoint)):
            raise CapacityError('fixed_local_engine_required')
        absolute(self.docker_endpoint[7:])
        if integer(self.min_free_bytes)<100*GiB or integer(self.reservation_bytes)!=64*GiB:
            raise CapacityError('reviewed_floor_and_reservation_required')
        if not 1<=integer(self.probe_timeout_seconds)<=60:raise CapacityError('bounded_probe_required')
        if (type(self.legacy_capacity_journals) is not tuple or len(self.legacy_capacity_journals)>16
                or any(type(path) is not str for path in self.legacy_capacity_journals)
                or tuple(sorted(set(self.legacy_capacity_journals)))!=self.legacy_capacity_journals):
            raise CapacityError('closed_legacy_inventory_required')
        for path in self.legacy_capacity_journals:absolute(path)


def canonical_capacity_path(engine_id):
    """OS-user home, not a supervisor/channel/configured state directory."""
    if os.name!='posix':raise CapacityError('linux_capacity_journal_required')
    import pwd
    if type(engine_id) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}',engine_id):raise CapacityError('fixed_engine_required')
    home=Path(pwd.getpwuid(os.geteuid()).pw_dir)
    return home/'.local'/'state'/'LedgeTop-Processing'/'image-staging'/hashlib.sha256(engine_id.encode()).hexdigest()


def disk_snapshot(root):
    """Current f_bavail bytes; existing allocated image bytes are already used."""
    path=absolute(root)
    if path.resolve()!=path or any(item.is_symlink() for item in (path,*path.parents)):
        raise CapacityError('docker_root_real_directory_required')
    before=path.lstat()
    if not stat.S_ISDIR(before.st_mode):raise CapacityError('docker_root_real_directory_required')
    fs=os.statvfs(path);after=path.lstat()
    if (before.st_dev,before.st_ino)!=(after.st_dev,after.st_ino):raise CapacityError('docker_root_changed_during_probe')
    return dict(path=str(path),device=before.st_dev,inode=before.st_ino,fsId=fs.f_fsid,blockSize=fs.f_frsize,
        totalBytes=fs.f_blocks*fs.f_frsize,freeBytes=fs.f_bavail*fs.f_frsize)


class ImageStagingCapacity:
    def __init__(self,policy,*,docker_probe=None,disk_probe=None):
        if os.name!='posix' or type(policy) is not CapacityPolicy:raise CapacityError('fixed_linux_capacity_policy_required')
        self.policy=policy;self._pid=os.getpid();self._mutex=threading.RLock()
        self._policy_value=canonical(asdict(policy))
        self.docker_probe=docker_probe or self._docker_snapshot;self.disk_probe=disk_probe or disk_snapshot
        if not callable(self.docker_probe) or not callable(self.disk_probe):raise CapacityError('fixed_readonly_probes_required')
        self.root=absolute(canonical_capacity_path(policy.engine_id));self._make_root()
        self.path=self.root/'capacity.sqlite3';self.lock_path=self.root/'capacity.lock'
        self._root_identity=self._directory_identity()
        self._lock_identity=self._create_file(self.lock_path)
        self._db_identity=self._create_file(self.path)
        with self._locked(),self._db() as db:
            names={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if names-{'capacity_binding','reservations','legacy_closures'}:raise CapacityError('dedicated_capacity_journal_required')
            for sql in SCHEMA:db.execute(sql)
            observed=self._snapshot()
            binding=dict(schemaVersion=1,policy=asdict(policy),rootIdentity=self._root_identity,
                lockIdentity=self._lock_identity,databaseIdentity=self._db_identity,disk=self._disk_identity(observed),schemaSha256=self._schema(db))
            old=db.execute('SELECT value FROM capacity_binding WHERE id=1').fetchone()
            if old and old[0]!=canonical(binding):raise CapacityError('capacity_policy_or_storage_changed')
            if not old and (db.execute('SELECT 1 FROM reservations').fetchone() or db.execute('SELECT 1 FROM legacy_closures').fetchone()):
                raise CapacityError('unbound_capacity_state')
            db.execute('INSERT OR IGNORE INTO capacity_binding VALUES(1,?)',(canonical(binding),))
            self._binding=canonical(binding)
        self._sync_root()

    def _make_root(self):
        missing=[];cursor=self.root
        while not cursor.exists():missing.append(cursor);cursor=cursor.parent
        if any(item.is_symlink() for item in (cursor,*cursor.parents)):raise CapacityError('private_capacity_root_required')
        for path in reversed(missing):path.mkdir(mode=0o700)
        self._directory_identity()

    def _directory_identity(self):
        if self.root.resolve()!=self.root or any(item.is_symlink() for item in (self.root,*self.root.parents)):
            raise CapacityError('private_capacity_root_required')
        info=self.root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o700:
            raise CapacityError('private_capacity_root_required')
        for item in self.root.iterdir():
            if item.name not in ('capacity.sqlite3','capacity.lock','capacity.sqlite3-journal'):
                raise CapacityError('foreign_capacity_state_refused')
        return [info.st_dev,info.st_ino]

    def _create_file(self,path):
        fd=os.open(path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
        try:return self._file_identity(path,fd)
        finally:os.close(fd)

    def _file_identity(self,path,fd=None):
        info=path.lstat();observed=os.fstat(fd) if fd is not None else info
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or observed.st_nlink!=1 or info.st_uid!=os.geteuid()
                or stat.S_IMODE(info.st_mode)!=0o600 or (info.st_dev,info.st_ino)!=(observed.st_dev,observed.st_ino)):
            raise CapacityError('private_regular_capacity_file_required')
        return [info.st_dev,info.st_ino]

    def _check(self):
        if self._pid!=os.getpid():raise CapacityError('capacity_owner_process_changed')
        if type(self.policy) is not CapacityPolicy or canonical(asdict(self.policy))!=self._policy_value:
            raise CapacityError('capacity_policy_changed')
        if (self._directory_identity()!=self._root_identity or self._file_identity(self.path)!=self._db_identity
                or self._file_identity(self.lock_path)!=self._lock_identity):raise CapacityError('capacity_storage_identity_changed')
        journal=self.path.with_name(self.path.name+'-journal')
        if journal.exists() or journal.is_symlink():self._file_identity(journal)

    @contextmanager
    def _locked(self):
        self._check()  # Before a potentially inherited mutex after fork.
        with self._mutex:
            self._check();fd=os.open(self.lock_path,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK)
            try:
                import fcntl
                if self._file_identity(self.lock_path,fd)!=self._lock_identity or os.get_inheritable(fd):raise CapacityError('capacity_lock_identity_changed')
                try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:raise CapacityBusy('capacity_transaction_active') from None
                self._check();yield;self._check()
            finally:os.close(fd)  # No explicit unlock from inherited children.

    @contextmanager
    def _db(self):
        self._check();db=sqlite3.connect(self.path,timeout=5);db.row_factory=sqlite3.Row
        try:
            if db.execute('PRAGMA journal_mode').fetchone()[0]!='delete':raise CapacityError('capacity_journal_mode_changed')
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            if hasattr(self,'_binding') and self._schema(db)!=json.loads(self._binding)['schemaSha256']:
                raise CapacityError('capacity_schema_changed')
            yield db;self._check();self._commit(db)
        except BaseException:db.rollback();raise
        finally:db.close()

    def _commit(self,db):db.commit()

    def _schema(self,db):
        def inventory(connection):
            return [[*row[:3],re.sub(r'\s+','',row[3]).lower() if row[3] is not None else None]
                for row in connection.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        memory=sqlite3.connect(':memory:')
        try:
            for sql in SCHEMA:memory.execute(sql)
            expected=inventory(memory)
        finally:memory.close()
        actual=inventory(db)
        if actual!=expected:raise CapacityError('dedicated_capacity_schema_required')
        return digest(actual)

    def _sync_root(self):
        fd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(fd)
        finally:os.close(fd)

    def _docker_snapshot(self):
        template='{"engineId":{{json .ID}},"dockerRootDir":{{json .DockerRootDir}}}'
        result=_run([self.policy.docker_executable,'--host',self.policy.docker_endpoint,'info','--format',template],
            timeout=self.policy.probe_timeout_seconds,max_bytes=8192)
        if result.returncode:raise CapacityError('docker_storage_probe_unavailable')
        def unique(pairs):
            value={}
            for key,item in pairs:
                if key in value:raise CapacityError('ambiguous_docker_storage_probe')
                value[key]=item
            return value
        return json.loads(result.stdout,object_pairs_hook=unique)

    def _snapshot(self):
        try:
            engine=self.docker_probe()
            if type(engine) is not dict or engine!={'engineId':self.policy.engine_id,'dockerRootDir':self.policy.docker_root_dir}:
                raise CapacityError('physical_docker_storage_changed')
            value=self.disk_probe(self.policy.docker_root_dir)
            fields={'path','device','inode','fsId','blockSize','totalBytes','freeBytes'}
            if type(value) is not dict or set(value)!=fields or value['path']!=self.policy.docker_root_dir:
                raise CapacityError('actual_storage_probe_required')
            for key in ('device','inode','fsId'):
                if type(value[key]) is not int or not 0<=value[key]<=2**128-1:raise CapacityError('invalid_filesystem_identity')
            integer(value['blockSize'],1);integer(value['totalBytes'],1);integer(value['freeBytes'])
            if value['freeBytes']>value['totalBytes']:raise CapacityError('invalid_available_space')
            return dict(value)
        except CapacityError:raise
        except Exception:raise CapacityError('storage_evidence_unavailable') from None

    @staticmethod
    def _disk_identity(snapshot):return {key:value for key,value in snapshot.items() if key not in ('totalBytes','freeBytes')}

    def _bound_snapshot(self,db,*,verify_legacy=True):
        row=db.execute('SELECT value FROM capacity_binding WHERE id=1').fetchone()
        value=self._snapshot()
        if not row or row[0]!=self._binding or json.loads(row[0])['disk']!=self._disk_identity(value):
            raise CapacityError('capacity_disk_binding_changed')
        for reservation in db.execute('SELECT * FROM reservations'):
            sha(reservation['pull_id']);sha(reservation['first_candidate_id'])
            binding=json.loads(reservation['binding'])
            if (type(binding) is not dict or set(binding)!={'repository','manifestDigest','configDigest','platform','engineId'}
                    or digest(binding)!=reservation['pull_id'] or binding['engineId']!=self.policy.engine_id
                    or binding['platform']!='linux/amd64' or reservation['bytes']!=self.policy.reservation_bytes
                    or reservation['state'] not in ('active','released')):raise CapacityError('reservation_evidence_changed')
            if reservation['state']=='active' and reservation['release_proof'] is not None:raise CapacityError('reservation_evidence_changed')
            if reservation['state']=='released':
                proof=json.loads(reservation['release_proof'])
                if (type(proof) is not dict or set(proof)!={'schemaVersion','pullId','binding','metadata','proofSha256'}
                        or type(proof['schemaVersion']) is not int or proof['schemaVersion']!=1
                        or proof['pullId']!=reservation['pull_id'] or proof['binding']!=binding
                        or proof['proofSha256']!=digest({key:item for key,item in proof.items() if key!='proofSha256'})):
                    raise CapacityError('released_proof_changed')
        if verify_legacy:self._verify_legacy(db)
        return value

    def _candidate(self,candidate):
        fields={'schemaVersion','channel','repository','tag','platform','manifestDigest','configDigest','compressedBytes','descriptorSetSha256','candidateId'}
        if type(candidate) is not dict or type(candidate.get('schemaVersion')) is not int:raise CapacityError('closed_registry_candidate_required')
        version=candidate['schemaVersion'];fields|=({'localImageId'} if version==1 else {'manifestDescriptor'} if version==2 else set())
        if version not in (1,2) or set(candidate)!=fields or len(canonical(candidate))>16384:raise CapacityError('closed_registry_candidate_required')
        for key in ('candidateId','descriptorSetSha256'):sha(candidate[key])
        for key in ('manifestDigest','configDigest'):sha(candidate[key],True)
        if (candidate['candidateId']!=digest({key:value for key,value in candidate.items() if key!='candidateId'})
                or type(candidate['channel']) is not str or not NAME.fullmatch(candidate['channel'])
                or type(candidate['tag']) is not str or not TAG.fullmatch(candidate['tag']) or candidate['platform']!='linux/amd64'):
            raise CapacityError('candidate_binding_changed')
        _repository(candidate['repository'],canonical=True);integer(candidate['compressedBytes'])
        if version==1 and candidate['localImageId']!=candidate['configDigest']:raise CapacityError('legacy_candidate_binding_changed')
        if version==2:
            item=candidate['manifestDescriptor']
            if (type(item) is not dict or set(item)!={'mediaType','digest','size'} or item['mediaType'] not in IMAGE_MEDIA
                    or item['digest']!=candidate['manifestDigest'] or not 1<=integer(item['size'])<=16*1024**2):raise CapacityError('candidate_descriptor_changed')
        binding=dict(repository=candidate['repository'],manifestDigest=candidate['manifestDigest'],configDigest=candidate['configDigest'],
            platform=candidate['platform'],engineId=self.policy.engine_id)
        return digest(binding),canonical(binding)

    def __call__(self,candidate,bytes):
        identifier,binding=self._candidate(candidate)
        if type(bytes) is not int or bytes!=self.policy.reservation_bytes or candidate['compressedBytes']>bytes:
            raise CapacityError('exact_staging_reservation_required')
        with self._locked(),self._db() as db:
            disk=self._bound_snapshot(db)
            old=db.execute('SELECT * FROM reservations WHERE pull_id=?',(identifier,)).fetchone()
            if old and (old['binding']!=binding or old['bytes']!=bytes):raise CapacityError('immutable_reservation_changed')
            if old and old['state']=='released':raise CapacityError('released_artifact_requires_explicit_recovery')
            active=db.execute("SELECT COALESCE(SUM(bytes),0) FROM reservations WHERE state='active'").fetchone()[0]
            needed=self.policy.min_free_bytes+active+(0 if old else bytes)
            if disk['freeBytes']<needed:return False
            if not old:
                db.execute("INSERT INTO reservations VALUES(?,?,?,?,'active',NULL)",(identifier,binding,bytes,candidate['candidateId']))
            return True

    def _registry(self,registry):
        if (not isinstance(registry,DockerRegistry) or registry.policy.engine_id!=self.policy.engine_id
                or registry.policy.docker_executable!=self.policy.docker_executable or registry.policy.docker_endpoint!=self.policy.docker_endpoint
                or registry.policy.staging_reservation_bytes!=self.policy.reservation_bytes):
            raise CapacityError('exact_actual_registry_required')

    def _metadata(self,candidate,metadata):
        fields={'candidateId','repository','manifestDigest','configDigest','imageId','imageIdKind','platform','localSize','engineId'}
        if (type(metadata) is not dict or set(metadata)!=fields
                or any(metadata[key]!=candidate[key] for key in ('candidateId','repository','manifestDigest','configDigest','platform'))
                or metadata['engineId']!=self.policy.engine_id or metadata['imageIdKind'] not in ('config','manifest')
                or metadata['imageId']!=candidate[{'config':'configDigest','manifest':'manifestDigest'}[metadata['imageIdKind']]]
                or integer(metadata['localSize'])>self.policy.reservation_bytes):raise CapacityError('exact_cached_metadata_required')
        return json.loads(canonical(metadata))

    def release_staged(self,candidate,registry):
        self._registry(registry);identifier,binding=self._candidate(candidate)
        with self._locked(),self._db() as db:
            self._bound_snapshot(db)
            try:metadata=self._metadata(candidate,registry.inspect_staged_artifact(candidate))
            except Exception:raise CapacityError('definite_cached_artifact_required') from None
            self._bound_snapshot(db)
            proof=dict(schemaVersion=1,pullId=identifier,binding=json.loads(binding),
                metadata={key:value for key,value in metadata.items() if key!='candidateId'})
            proof['proofSha256']=digest(proof)
            row=db.execute('SELECT * FROM reservations WHERE pull_id=?',(identifier,)).fetchone()
            if row is None:return False  # Does not import/release a manual book.
            if row['binding']!=binding or row['bytes']!=self.policy.reservation_bytes:raise CapacityError('reservation_binding_changed')
            if row['state']=='released':
                if row['release_proof']!=canonical(proof):raise CapacityError('released_proof_changed')
                return True
            db.execute("UPDATE reservations SET state='released',release_proof=? WHERE pull_id=? AND state='active'",(canonical(proof),identifier))
            return True

    def _verify_legacy(self,db):
        for path in self.policy.legacy_capacity_journals:
            row=db.execute('SELECT inventory,proof FROM legacy_closures WHERE path=?',(path,)).fetchone()
            if row is None or row[0]!=canonical(self._legacy_inventory(path)):
                raise CapacityError('explicit_legacy_capacity_reconciliation_required')
            self._closure(json.loads(row[0]),json.loads(row[1]))

    def _closure(self,inventory,proof):
        if (type(proof) is not dict or set(proof)!={'schemaVersion','inventorySha256','aliases','proofSha256'}
                or type(proof['schemaVersion']) is not int or proof['schemaVersion']!=1
                or proof['inventorySha256']!=digest(inventory) or type(proof['aliases']) is not list
                or proof['proofSha256']!=digest({key:value for key,value in proof.items() if key!='proofSha256'})
                or len(proof['aliases'])!=len(inventory['rows'])):raise CapacityError('legacy_closure_proof_changed')
        for row,alias in zip(inventory['rows'],proof['aliases']):
            if (type(alias) is not dict or set(alias)!={'legacyCandidateId','canonicalCandidateId','pullId','binding',
                    'legacyCandidateSha256','canonicalCandidateSha256','metadata'} or alias['legacyCandidateId']!=row['candidate']):
                raise CapacityError('legacy_closure_alias_changed')
            for key in ('legacyCandidateId','canonicalCandidateId','pullId','legacyCandidateSha256','canonicalCandidateSha256'):sha(alias[key])
            binding=alias['binding']
            if (type(binding) is not dict or set(binding)!={'repository','manifestDigest','configDigest','platform','engineId'}
                    or binding['engineId']!=self.policy.engine_id or binding['platform']!='linux/amd64'
                    or alias['pullId']!=digest(binding)):raise CapacityError('legacy_closure_alias_changed')
            _repository(binding['repository'],canonical=True);sha(binding['manifestDigest'],True);sha(binding['configDigest'],True)
            self._metadata(dict(binding,candidateId=alias['canonicalCandidateId']),alias['metadata'])

    def verify_legacy(self):
        """Startup/tick check, including fresh physical storage identity."""
        with self._locked(),self._db() as db:self._bound_snapshot(db)
        return None

    def _legacy_inventory(self,path):
        source=absolute(path)
        if source.resolve()!=source or any(item.is_symlink() for item in (source,*source.parents)):
            raise CapacityError('private_legacy_book_required')
        parent=source.parent.stat()
        if parent.st_uid!=os.geteuid() or not stat.S_ISDIR(parent.st_mode) or stat.S_IMODE(parent.st_mode)!=0o700:
            raise CapacityError('private_legacy_book_required')
        identity=self._file_identity(source);before=source.lstat()
        if before.st_size>16*1024**2:raise CapacityError('bounded_legacy_book_required')
        sidecars=[source.with_name(source.name+suffix) for suffix in ('-wal','-shm','-journal')]
        if any(item.exists() or item.is_symlink() for item in sidecars):raise CapacityError('inactive_legacy_book_required')
        db=sqlite3.connect(source.as_uri()+'?mode=ro&immutable=1',uri=True,timeout=5)
        try:
            db.execute('BEGIN')
            if db.execute('PRAGMA quick_check').fetchall()!=[('ok',)]:raise CapacityError('legacy_integrity_unproven')
            objects=[list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
            expected=[['index','sqlite_autoindex_reservations_1','reservations',None],
                ['table','reservations','reservations','CREATE TABLE reservations(candidate TEXT PRIMARY KEY,bytes INTEGER NOT NULL)']]
            compact=lambda rows:[[ *row[:3],re.sub(r'\s+','',row[3]).lower() if row[3] is not None else None] for row in rows]
            if compact(objects)!=compact(expected):raise CapacityError('unsupported_legacy_capacity_schema')
            if [tuple(row) for row in db.execute('PRAGMA table_info(reservations)')]!=[(0,'candidate','TEXT',0,None,1),(1,'bytes','INTEGER',1,None,0)]:
                raise CapacityError('unsupported_legacy_capacity_schema')
            rows=[dict(candidate=row[0],bytes=row[1]) for row in db.execute('SELECT candidate,bytes FROM reservations ORDER BY candidate')]
            if len(rows)>10000:raise CapacityError('bounded_legacy_rows_required')
            for row in rows:
                sha(row['candidate'])
                if type(row['bytes']) is not int or row['bytes']!=self.policy.reservation_bytes:raise CapacityError('legacy_reservation_bound_changed')
        finally:db.close()
        fd=os.open(source,os.O_RDONLY|os.O_NOFOLLOW);h=hashlib.sha256();size=0
        try:
            if self._file_identity(source,fd)!=identity:raise CapacityError('legacy_identity_changed')
            while True:
                chunk=os.read(fd,1048576)
                if not chunk:break
                size+=len(chunk)
                if size>before.st_size:raise CapacityError('legacy_content_changed')
                h.update(chunk)
        finally:os.close(fd)
        after=source.lstat()
        if (self._file_identity(source)!=identity or size!=before.st_size
                or (after.st_size,after.st_mtime_ns)!=(before.st_size,before.st_mtime_ns)
                or any(item.exists() or item.is_symlink() for item in sidecars)):
            raise CapacityError('legacy_content_changed')
        return dict(schemaVersion=1,path=str(source),identity=dict(device=before.st_dev,inode=before.st_ino,
            uid=before.st_uid,mode=stat.S_IMODE(before.st_mode),bytes=before.st_size,mtimeNs=before.st_mtime_ns),
            schemaSha256=digest(objects),rowsSha256=digest(rows),fileSha256=h.hexdigest(),rows=rows)

    def reconcile_legacy(self,path,registry):
        self._registry(registry);path=str(absolute(path))
        if path not in self.policy.legacy_capacity_journals:raise CapacityError('explicit_legacy_inventory_required')
        with self._locked(),self._db() as db:
            self._bound_snapshot(db,verify_legacy=False);inventory=self._legacy_inventory(path);aliases=[]
            for row in inventory['rows']:
                try:
                    with registry._db() as source:
                        raw=source.execute('SELECT value FROM registry_candidates WHERE id=?',(row['candidate'],)).fetchone()
                    if raw is None:raise CapacityError('orphan_legacy_reservation')
                    old=json.loads(raw[0]);registry._candidate(old)
                    if old['schemaVersion']!=1:raise CapacityError('closed_v1_legacy_candidate_required')
                    identifier=registry._pull_id(old)
                    with registry._db() as source:
                        staged=source.execute('SELECT state,metadata FROM registry_staging WHERE id=?',(identifier,)).fetchone()
                    if staged is None or staged['state']!='staged':raise CapacityError('legacy_pull_outcome_not_definite')
                    metadata=json.loads(staged['metadata']);current_id=metadata['candidateId'];sha(current_id)
                    with registry._db() as source:
                        current=source.execute('SELECT value FROM registry_candidates WHERE id=?',(current_id,)).fetchone()
                    if current is None:raise CapacityError('canonical_staged_candidate_missing')
                    candidate=json.loads(current[0]);registry._candidate(candidate)
                    if candidate['schemaVersion']!=2 or registry._pull_id(candidate)!=identifier:
                        raise CapacityError('legacy_alias_artifact_changed')
                    old_key,old_binding=self._candidate(old);new_key,new_binding=self._candidate(candidate)
                    if (old_key,old_binding)!=(new_key,new_binding):raise CapacityError('legacy_alias_artifact_changed')
                    checked=self._metadata(candidate,registry.inspect_staged(candidate))
                    if canonical(checked)!=staged['metadata']:raise CapacityError('staged_legacy_alias_proof_changed')
                    aliases.append(dict(legacyCandidateId=row['candidate'],canonicalCandidateId=current_id,pullId=identifier,
                        binding=json.loads(old_binding),legacyCandidateSha256=digest(old),canonicalCandidateSha256=digest(candidate),metadata=checked))
                except CapacityError:raise
                except Exception:raise CapacityError('legacy_cached_proof_unavailable') from None
            if self._legacy_inventory(path)!=inventory:raise CapacityError('legacy_book_changed_during_reconciliation')
            self._bound_snapshot(db,verify_legacy=False)
            proof=dict(schemaVersion=1,inventorySha256=digest(inventory),aliases=aliases)
            proof['proofSha256']=digest(proof)
            self._closure(inventory,proof)
            old=db.execute('SELECT inventory,proof FROM legacy_closures WHERE path=?',(path,)).fetchone()
            if old:
                if tuple(old)!=(canonical(inventory),canonical(proof)):raise CapacityError('immutable_legacy_closure_changed')
                return True
            db.execute('INSERT INTO legacy_closures VALUES(?,?,?)',(path,canonical(inventory),canonical(proof)))
            return True
