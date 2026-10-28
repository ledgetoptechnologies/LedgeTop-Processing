"""One bounded update-service tick; no timer, process launcher or job dispatcher.

The caller must hold the actual supervisor's process lease. Its immutable
seals/intent machinery remain authoritative. Discovery is the only retried
remote read, after a durable cooldown; issued pipeline mutations are reconciled
read-only, never retried. Capacity-release metadata is explicitly idempotent.
"""
from contextlib import contextmanager
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time

from .update_registry import _json,_sha
from .update_supervisor import UpdateSupervisor


class ServiceError(ValueError):pass
class ServiceHeld(ServiceError):pass


class UpdateService:
    def __init__(self,path,*,supervisor,config_id,code_binding_sha256,capacity_admitter,capacity_releaser,
                 idle_probe,can_schedule,discovery_interval_seconds=21600,error_cooldown_seconds=300,
                 max_transitions=8,clock=time.time):
        if not isinstance(supervisor,UpdateSupervisor):raise ServiceError('actual_supervisor_required')
        supervisor.check()
        if type(config_id) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}',config_id):
            raise ServiceError('fixed_service_config_id_required')
        if type(code_binding_sha256) is not str or not re.fullmatch(r'[a-f0-9]{64}',code_binding_sha256):
            raise ServiceError('fixed_service_code_binding_required')
        for value in (discovery_interval_seconds,error_cooldown_seconds):
            if type(value) is not int or not 1<=value<=2**53-1:raise ServiceError('positive_service_interval_required')
        if type(max_transitions) is not int or not 1<=max_transitions<=8:raise ServiceError('bounded_tick_required')
        if any(not callable(value) for value in (capacity_admitter,capacity_releaser,idle_probe,can_schedule,clock)):
            raise ServiceError('explicit_service_dependencies_required')
        self.path=Path(path).absolute();self.supervisor=supervisor
        if self.path.parent!=supervisor.path.parent or self.path in (supervisor.path,supervisor.lock_path):
            raise ServiceError('same_private_supervisor_root_required')
        self.interval,self.cooldown,self.maximum=discovery_interval_seconds,error_cooldown_seconds,max_transitions
        self.admit,self.release,self.idle_probe,self.can_schedule,self.clock=capacity_admitter,capacity_releaser,idle_probe,can_schedule,clock
        self._pid=os.getpid();self._mutex=threading.RLock();self._busy=False
        root=self.path.parent.stat()
        self._binding=_json(dict(schemaVersion=1,configId=config_id,codeBindingSha256=code_binding_sha256,
            supervisorPath=str(supervisor.path),supervisorPolicySha256=supervisor.policy_sha256,
            registryPath=str(supervisor.registry.path),rootIdentity=[root.st_dev,root.st_ino],
            discoveryInterval=discovery_interval_seconds,errorCooldown=error_cooldown_seconds,maxTransitions=max_transitions))
        with supervisor._mutex:
            if self.path.is_symlink():raise ServiceError('private_service_journal_required')
            fd=os.open(self.path,os.O_CREAT|os.O_RDWR|getattr(os,'O_NOFOLLOW',0),0o600);os.close(fd)
            with self._db() as db:
                db.execute('CREATE TABLE IF NOT EXISTS service_binding(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)')
                db.execute('''CREATE TABLE IF NOT EXISTS service_discovery (
                    id INTEGER PRIMARY KEY CHECK(id=1),highwater REAL NOT NULL,next_due REAL NOT NULL,
                    state TEXT NOT NULL,last_started REAL,last_finished REAL,attempt INTEGER NOT NULL)''')
                db.execute('''CREATE TABLE IF NOT EXISTS service_capacity_releases (
                    operation TEXT PRIMARY KEY,candidate TEXT NOT NULL,state TEXT NOT NULL,released INTEGER)''')
                old=db.execute('SELECT value FROM service_binding WHERE id=1').fetchone()
                if old and old[0]!=self._binding:raise ServiceHeld('service_binding_changed')
                if not old and db.execute('SELECT 1 FROM service_discovery').fetchone():raise ServiceHeld('unbound_service_history')
                db.execute('INSERT OR IGNORE INTO service_binding VALUES(1,?)',(self._binding,))
                db.execute("INSERT OR IGNORE INTO service_discovery VALUES(1,0,0,'idle',NULL,NULL,0)")
            supervisor._sync_directory(self.path.parent)

    def _check(self):
        if self._pid!=os.getpid():raise ServiceHeld('service_process_changed')
        self.supervisor.check()

    @contextmanager
    def _db(self):
        self._check();info=self.path.lstat()
        if (self.path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode & 0o077))):
            raise ServiceHeld('private_service_journal_required')
        db=sqlite3.connect(self.path,timeout=10);db.row_factory=sqlite3.Row;db.execute('PRAGMA synchronous=FULL')
        try:
            db.execute('BEGIN IMMEDIATE');yield db;self._check();self._commit(db)
        except BaseException:db.rollback();raise
        finally:db.close()

    def _commit(self,db):db.commit()

    def _observe(self):
        value=self.clock()
        if type(value) not in (int,float) or not math.isfinite(value) or not 0<=value<=2**53-1:
            raise ServiceHeld('valid_service_clock_required')
        # Commit highwater independently before callbacks/business operations.
        with self._db() as db:
            old=db.execute('SELECT highwater FROM service_discovery WHERE id=1').fetchone()[0]
            if type(old) not in (int,float) or not math.isfinite(old) or old<0:raise ServiceHeld('service_clock_state_changed')
            now=max(value,old)
            db.execute('UPDATE service_discovery SET highwater=? WHERE id=1',(now,))
        return now

    def _discovery(self):
        with self._db() as db:
            if db.execute('SELECT value FROM service_binding WHERE id=1').fetchone()[0]!=self._binding:
                raise ServiceHeld('service_binding_changed')
            row=dict(db.execute('SELECT * FROM service_discovery WHERE id=1').fetchone())
        if (row['state'] not in ('idle','intent','cooldown','pending_recovered')
                or any(type(row[key]) not in (int,float) or not math.isfinite(row[key]) or row[key]<0 for key in ('highwater','next_due'))
                or type(row['attempt']) is not int or row['attempt']<0):raise ServiceHeld('service_discovery_state_changed')
        return row

    def _reserve_discovery(self,now):
        with self._db() as db:
            db.execute("UPDATE service_discovery SET state='intent',last_started=?,next_due=?,attempt=attempt+1 WHERE id=1",
                (now,now+self.cooldown))

    def _finish_discovery(self,success):
        now=self._observe()
        with self._db() as db:
            db.execute('UPDATE service_discovery SET state=?,last_finished=?,next_due=? WHERE id=1',
                ('idle' if success else 'cooldown',now,now+(self.interval if success else self.cooldown)))

    def _pending_discovery_recovery(self,now):
        # A committed pending supervisor row survives a lost discovery-service
        # checkpoint. Delay another scan; no external call is inferred/replayed.
        with self._db() as db:
            db.execute("UPDATE service_discovery SET state='pending_recovered',last_finished=?,next_due=MAX(next_due,?) WHERE id=1 AND state='intent'",
                (now,now+self.interval))

    def _result(self,status,pending,transitions):
        self._check()
        return dict(status=status,operationId=pending['operationId'] if pending else None,
            state=pending['state'] if pending else None,transitions=transitions,nextDiscoveryAt=self._discovery()['next_due'])

    def _release_capacity(self,pending):
        if pending['state'] not in ('staged','qualified','scheduled','completed'):return
        operation=pending['operationId'];candidate=pending['candidate'];digest=_sha(candidate)
        with self._db() as db:
            row=db.execute('SELECT * FROM service_capacity_releases WHERE operation=?',(operation,)).fetchone()
            if row:
                if row['candidate']!=digest or row['state'] not in ('intent','done'):raise ServiceHeld('capacity_release_binding_changed')
                if row['state']=='done':
                    if row['released'] not in (0,1):raise ServiceHeld('capacity_release_binding_changed')
                    return
            else:db.execute("INSERT INTO service_capacity_releases VALUES(?,?,'intent',NULL)",(operation,digest))
        released=self.release(candidate,self.supervisor.registry)
        if type(released) is not bool:raise ServiceHeld('explicit_capacity_release_result_required')
        self._check()
        with self._db() as db:
            db.execute("UPDATE service_capacity_releases SET state='done',released=? WHERE operation=? AND candidate=?",
                (int(released),operation,digest))

    def _permission(self,callback,pending):
        try:value=callback(pending)
        except Exception:raise ServiceHeld('service_readiness_unavailable') from None
        self._check()
        if type(value) is not bool:raise ServiceHeld('explicit_service_readiness_required')
        return value

    def tick(self):
        """One candidate cycle at most; caller owns the actual Supervisor lease."""
        self._check()
        with self.supervisor._mutex,self._mutex:
            self._check()
            if self._busy:raise ServiceHeld('service_tick_in_progress')
            self._busy=True
            try:return self._tick()
            except ServiceError:raise
            except Exception:raise ServiceHeld('service_state_or_owner_unavailable') from None
            finally:self._busy=False

    def _tick(self):
        now=self._observe();discovery=self._discovery();pending=self.supervisor.pending();transitions=0
        if pending is None:
            if now<discovery['next_due']:
                return self._result('discovery_cooldown' if discovery['state'] in ('intent','cooldown') else 'not_due',None,0)
            self._reserve_discovery(now)
            try:pending=self.supervisor.discover()
            except Exception:
                self._finish_discovery(False)
                return self._result('discovery_cooldown',None,1)
            self._finish_discovery(True);transitions=1
            if pending is None:return self._result('no_change',None,transitions)
        else:self._pending_discovery_recovery(now)

        if pending['state'].endswith('_intent'):
            try:pending=self.supervisor.reconcile(pending['operationId'])
            except Exception:return self._result('held',pending,transitions+1)
            try:self._release_capacity(pending)
            except Exception:return self._result('capacity_release_held',pending,transitions+1)
            # Reconciliation never chains into another external mutation.
            return self._result('completed' if pending['state']=='completed' else 'reconciled',pending,transitions+1)

        while transitions<self.maximum:
            if pending['state']=='completed':return self._result('completed',pending,transitions)
            if pending['state'] not in ('discovered','staged','qualified','scheduled'):
                return self._result('held',pending,transitions)
            try:self._release_capacity(pending)
            except Exception:return self._result('capacity_release_held',pending,transitions)
            if pending['state'] in ('discovered','staged'):
                if not self._permission(self.idle_probe,pending):return self._result('waiting_idle',pending,transitions)
                if pending['state']=='discovered':
                    try:admitted=self.admit(pending['candidate'],self.supervisor.registry)
                    except Exception:raise ServiceHeld('capacity_admission_unavailable') from None
                    self._check()
                    if type(admitted) is not bool:raise ServiceHeld('explicit_capacity_admission_required')
                    if not admitted:return self._result('waiting_capacity',pending,transitions)
            elif pending['state']=='qualified':
                if not self._permission(self.can_schedule,pending):return self._result('waiting_schedule',pending,transitions)
            previous=pending['state']
            try:pending=self.supervisor.advance(pending['operationId'])
            except Exception:
                pending=self.supervisor.pending()
                return self._result('held',pending,transitions+1)
            transitions+=1
            if pending['state']=='staged':
                try:self._release_capacity(pending)
                except Exception:return self._result('capacity_release_held',pending,transitions)
            if pending['state']==previous:return self._result('waiting_run' if previous=='scheduled' else 'waiting',pending,transitions)
        return self._result('completed' if pending['state']=='completed' else 'transition_budget',pending,transitions)
