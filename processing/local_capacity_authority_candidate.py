"""Inactive local capacity owner, not a remotely usable or host-global authority.

An operator must pre-provision an owner-private binding file and journal. This
wrapper holds the existing process lock and refuses changed provisioned identity.
Other users, old writers, mount races, and container administrators can bypass
it. No external service, cleanup, engine attestation, or NAS protocol is wired.
"""
import json
import hashlib
import os
from pathlib import Path
import re
import stat
import sys
import threading

from ops.reference_worker_daemon import DaemonStateLock
from .crossfs_admission_candidate import CrossFsAdmission, CrossFsHeld
from .scratch_policy import ScratchLedger

LOCAL_CAPACITY_AUTHORITY_QUALIFIED = False


class LocalCapacityAuthority:
    def __init__(self, state_root, binding_path, journal, ledgers):
        if LOCAL_CAPACITY_AUTHORITY_QUALIFIED is not True:
            raise CrossFsHeld('local capacity authority not qualified')
        self.root=Path(state_root).absolute()
        self.binding=Path(binding_path).absolute()
        self.journal=Path(journal).absolute()
        self.ledgers=ledgers
        self.pid=os.getpid()
        self._mutex=threading.RLock()
        self._owner=None
        self._gate=None
        self._binding_identity=None
        self._journal_identity=None
        self._binding_value=None
        self._ledger_bindings=None
        self._mount_pins={}

    def _read_binding(self):
        if self.binding.parent!=self.root or self.binding.resolve()!=self.binding:
            raise CrossFsHeld('private provisioned binding required')
        fd=os.open(self.binding,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size>16384
                    or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode & 0o077))):
                raise CrossFsHeld('private provisioned binding required')
            chunks=[];remaining=16385
            while remaining:
                part=os.read(fd,remaining)
                if not part:break
                chunks.append(part);remaining-=len(part)
            data=b''.join(chunks)
            if len(data)>16384:raise CrossFsHeld('binding exceeds limit')
            current=self.binding.lstat()
            if (current.st_dev,current.st_ino)!=(info.st_dev,info.st_ino):
                raise CrossFsHeld('binding identity changed')
            return (info.st_dev,info.st_ino),data
        finally:os.close(fd)

    @staticmethod
    def _mount_identity(fd):
        info=os.fstat(fd)
        fields=Path(f'/proc/self/fdinfo/{fd}').read_text().splitlines()
        values=[line.split(':',1)[1].strip() for line in fields if line.startswith('mnt_id:')]
        if len(values)!=1 or not values[0].isdigit() or not stat.S_ISDIR(info.st_mode):
            raise CrossFsHeld('Linux mount identity unavailable')
        return (info.st_dev,info.st_ino,int(values[0]))

    def _pin_mounts(self):
        if not sys.platform.startswith('linux') or not hasattr(os,'O_PATH'):
            raise CrossFsHeld('Linux descriptor mount identity required')
        try:
            for name,ledger in self.ledgers.items():
                fd=os.open(ledger.root,os.O_PATH|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
                try:identity=self._mount_identity(fd)
                except BaseException:
                    os.close(fd);raise
                self._mount_pins[name]=(fd,identity)
        except BaseException:
            self._close_mounts();raise

    def _close_mounts(self):
        pins,self._mount_pins=self._mount_pins,{}
        failure=None
        for fd,_ in pins.values():
            try:os.close(fd)
            except OSError as error:
                if failure is None:failure=error
        if failure is not None:raise CrossFsHeld('mount descriptor cleanup failed') from failure

    def _check_mounts(self):
        if set(self._mount_pins)!=set(self.ledgers):raise CrossFsHeld('mount pins unavailable')
        for name,ledger in self.ledgers.items():
            pinned,identity=self._mount_pins[name]
            if self._mount_identity(pinned)!=identity:raise CrossFsHeld('pinned mount identity changed')
            current=os.open(ledger.root,os.O_PATH|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
            try:
                if self._mount_identity(current)!=identity:raise CrossFsHeld('mounted filesystem changed')
            finally:os.close(current)

    def _check(self):
        if self.pid!=os.getpid() or self._owner is None:
            raise CrossFsHeld('local authority owner unavailable')
        self._owner.check(self.root)
        self._check_mounts()
        binding_identity,binding_value=self._read_binding()
        if binding_identity!=self._binding_identity or binding_value!=self._binding_value:
            raise CrossFsHeld('capacity authority binding changed')
        for name,ledger in self.ledgers.items():
            root=ledger.root.lstat(); db=ledger.path.lstat()
            if (stat.S_ISLNK(root.st_mode) or not stat.S_ISDIR(root.st_mode) or stat.S_ISLNK(db.st_mode)
                    or not stat.S_ISREG(db.st_mode) or db.st_nlink!=1
                    or [root.st_dev,root.st_ino,db.st_dev,db.st_ino]!=self._ledger_bindings[name]):
                raise CrossFsHeld('capacity ledger identity changed')
        info=self.journal.lstat()
        if (stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                or (info.st_dev,info.st_ino)!=self._journal_identity):
            raise CrossFsHeld('capacity journal identity changed')

    def __enter__(self):
        with self._mutex:
            if self.pid!=os.getpid() or self._owner is not None:
                raise CrossFsHeld('local authority owner unavailable')
            owner=DaemonStateLock(self.root)
            try:
                owner.__enter__()
                self._owner=owner
                identity,raw=self._read_binding()
                def unique_pairs(items):
                    result={}
                    for key,item in items:
                        if key in result:raise CrossFsHeld('duplicate binding key')
                        result[key]=item
                    return result
                value=json.loads(raw,object_pairs_hook=unique_pairs)
                if (type(value) is not dict or set(value)!={'schemaVersion','authorityId','journal','journalIdentity','ledgers'}
                        or type(value['schemaVersion']) is not int or value['schemaVersion']!=1
                        or type(value['authorityId']) is not str or not re.fullmatch('[a-f0-9]{32}',value['authorityId'])
                        or value['journal']!=str(self.journal) or type(value['ledgers']) is not dict
                        or type(self.ledgers) is not dict or set(value['ledgers'])!=set(self.ledgers)):
                    raise CrossFsHeld('invalid provisioned capacity authority')
                info=self.journal.lstat()
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                        or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode & 0o077))
                        or value['journalIdentity']!=[info.st_dev,info.st_ino]):
                    raise CrossFsHeld('provisioned journal differs')
                self._ledger_bindings={}
                for name,ledger in self.ledgers.items():
                    if type(ledger) is not ScratchLedger:
                        raise CrossFsHeld('provisioned ledger differs')
                    root=ledger.root.lstat(); db=ledger.path.lstat()
                    fingerprint=[root.st_dev,root.st_ino,db.st_dev,db.st_ino]
                    if value['ledgers'][name]!={'root':str(ledger.root),'journal':str(ledger.path.absolute()),'floor':ledger.floor,'identity':fingerprint}:
                        raise CrossFsHeld('provisioned ledger differs')
                    self._ledger_bindings[name]=fingerprint
                self._binding_identity,self._binding_value=identity,raw
                self._journal_identity=(info.st_dev,info.st_ino)
                self._pin_mounts()
                self._check()
                self._gate=CrossFsAdmission(self.journal,self.ledgers)
                self._check()
                return self
            except BaseException:
                try:self._close_mounts()
                finally:
                    if self._owner is not None:
                        try:owner.__exit__(None,None,None)
                        finally:self._owner=None
                raise

    def acquire(self, **kwargs):
        with self._mutex:
            self._check()
            result=self._gate.acquire(**kwargs)
            self._check()
            return result

    def hold_evidence(self, attempt_id):
        """Digest exact current cooperative holds; never a transfer grant."""
        with self._mutex:
            self._check()
            with self._gate._db() as db:
                attempt=db.execute('SELECT plan,state FROM attempts WHERE id=?',(attempt_id,)).fetchone()
                parts=db.execute('SELECT device,ledger,bytes,reservation,state FROM parts WHERE attempt=? ORDER BY device',(attempt_id,)).fetchall()
            if attempt is None or attempt[1]!='admitted':raise CrossFsHeld('cooperative hold unavailable')
            plan=json.loads(attempt[0]);devices=plan.get('devices')
            if type(devices) is not dict or len(parts)!=len(devices):raise CrossFsHeld('hold inventory changed')
            evidence=[]
            for device,label,amount,reservation,state in parts:
                entry=devices.get(device)
                if (type(entry) is not dict or state!='acquired' or entry.get('ledger')!=label
                        or entry.get('bytes')!=amount or label not in self.ledgers):
                    raise CrossFsHeld('hold component changed')
                ledger=self.ledgers[label]
                with ledger._transaction() as db:
                    row=db.execute('SELECT settings,state,remaining,allocated FROM reservations WHERE id=?',(reservation,)).fetchone()
                    if row is None or row['state'] not in ('uncertain','engine_started'):raise CrossFsHeld('cooperative hold unavailable')
                    settings=json.loads(row['settings'])
                    expected={'job_id':plan['job'],'generation':plan['generation'],'reservation_id':reservation,
                        'manifest_sha256':plan['manifest'],'input_bytes':entry['inputBytes'],'output_budget':entry['outputBudget'],
                        'environment':plan['environment'],'ttl_seconds':plan['ttl']}
                    if settings!=expected or row['remaining']+row['allocated']!=amount:
                        raise CrossFsHeld('cooperative hold identity changed')
                    evidence.append({'device':device,'ledger':label,'reservation':reservation,
                        'settingsSha256':hashlib.sha256(json.dumps(settings,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
                        'reservedBytes':amount,'state':'active',
                        'mountIdentity':list(self._mount_pins[label][1]),'ledgerIdentity':self._ledger_bindings[label]})
            self._check()
            value={'attemptId':attempt_id,'authorityId':json.loads(self._binding_value)['authorityId'],
                   'planSha256':hashlib.sha256(attempt[0].encode()).hexdigest(),'holds':evidence}
            return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()

    def __exit__(self,*args):
        with self._mutex:
            owner=self._owner
            try:
                self._check()
            finally:
                try:
                    if owner is not None:owner.__exit__(*args)
                finally:
                    try:self._close_mounts()
                    finally:self._owner=None;self._gate=None
