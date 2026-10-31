"""Isolated node enrollment prototype; never starts or authorizes an engine.

The caller provisions the state owner, TLS CA, physical NodeGate and private
output root. No listener, daemon, engine dispatch, result ACK or cleanup is
installed here. Noncooperating Docker/host writers remain outside the fence.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import ssl
import stat
import threading
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener

from . import reference_protocol as p
from .direct_outbox_contract import validate_binding
from .direct_output_slot import SlotHeld, open_slot, _identity
from .node_gate import NodeGate, GateError, SLUG, ENGINE, _binding
from .reference_transfer import _TransferWatchdog, _DeadlineHTTPSHandler
from ops.reference_worker_daemon import DaemonStateLock


class EnrollmentHeld(RuntimeError):pass

CONFIG_DDL='CREATE TABLE config (singleton INTEGER PRIMARY KEY CHECK(singleton=1), value TEXT NOT NULL)'
INTENT_DDL='CREATE TABLE intent (singleton INTEGER PRIMARY KEY CHECK(singleton=1), request TEXT NOT NULL, receipt TEXT NOT NULL, claim TEXT, binding TEXT, slot_phase TEXT NOT NULL, slot_identity TEXT)'


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*_args):raise EnrollmentHeld('redirect_forbidden')


class ViewerNodeEnrollment:
    """One owner and one durable intent; no automatic release or dispatch."""
    def __init__(self,owner,*,origin,ca_file,ca_sha256,bearer,node_id,worker_id,instance_id,
                 engine_id,authority_id,gate_origin,gate_generation,gate_lock_identity,
                 output_root,output_root_identity,output_budget,max_budget,allowed_environments):
        if type(owner) is not DaemonStateLock:raise EnrollmentHeld('worker_owner_required')
        owner.check();self.owner=owner;self._pid=os.getpid();self._operation_lock=threading.RLock()
        parsed=urlsplit(origin)
        if (type(origin) is not str or not origin.isascii() or len(origin)>2048 or '\\' in origin
                or any(ord(char)<=32 or ord(char)==127 for char in origin)
                or parsed.scheme!='https' or not parsed.hostname or parsed.username is not None or parsed.password is not None
                or parsed.path not in ('','/') or parsed.query or parsed.fragment or parsed.port==0 or origin.rstrip('/')!=origin):
            raise EnrollmentHeld('coordinator_origin_invalid')
        p._hash(ca_sha256,'CA digest')
        ca=Path(ca_file)
        if not ca.is_absolute() or ca.resolve()!=ca:raise EnrollmentHeld('CA_path_invalid')
        fd=os.open(ca,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
        try:
            info=os.fstat(fd);named=ca.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or (info.st_dev,info.st_ino)!=(named.st_dev,named.st_ino) or info.st_size>65536:
                raise EnrollmentHeld('CA_identity_invalid')
            data=os.read(fd,65537)
            if hashlib.sha256(data).hexdigest()!=ca_sha256:raise EnrollmentHeld('CA_digest_mismatch')
        finally:os.close(fd)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);context.minimum_version=ssl.TLSVersion.TLSv1_2
        context.load_verify_locations(cadata=data.decode('ascii'))
        if (not context.check_hostname or context.verify_mode!=ssl.CERT_REQUIRED or type(bearer) is not str
                or not 32<=len(bearer)<=4096 or not bearer.isascii() or any(ord(c)<33 or ord(c)>126 for c in bearer)):
            raise EnrollmentHeld('worker_auth_invalid')
        if _binding(node_id,gate_origin,authority_id)['coordinatorOrigin']!=gate_origin:
            raise EnrollmentHeld('gate_origin_invalid')
        if not isinstance(worker_id,str) or not SLUG.fullmatch(worker_id) or not isinstance(engine_id,str) or not ENGINE.fullmatch(engine_id):
            raise EnrollmentHeld('worker_or_engine_invalid')
        p._slug(instance_id,'worker instance identity')
        p._integer(gate_generation,'gate generation')
        if (type(gate_lock_identity) is not list or len(gate_lock_identity)!=3
                or any(type(v) is not int or v<0 for v in gate_lock_identity)):
            raise EnrollmentHeld('gate_identity_invalid')
        if (type(allowed_environments) not in (tuple,list) or not allowed_environments
                or len(set(allowed_environments))!=len(allowed_environments)
                or not set(allowed_environments)<={'production','staging'}):
            raise EnrollmentHeld('environment_policy_invalid')
        if type(output_budget) is not int or type(max_budget) is not int or not 1<=output_budget<=max_budget<=p.MAX_SAFE_INTEGER:
            raise EnrollmentHeld('output_budget_invalid')
        self.config=json.loads(p.canonical_json(dict(origin=origin,caSha256=ca_sha256,nodeId=node_id,workerId=worker_id,instanceId=instance_id,
            engineId=engine_id,authorityId=authority_id,gateOrigin=gate_origin,gateGeneration=gate_generation,
            gateLock=gate_lock_identity,outputRoot=str(output_root),outputRootIdentity=output_root_identity,
            outputBudget=output_budget,maxBudget=max_budget,allowedEnvironments=sorted(allowed_environments))))
        self._bearer=bearer;self._ssl_context=context
        self.path=owner.root/'viewer-enrollment.sqlite'
        descriptor=os.open(self.path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
        try:
            info=os.fstat(descriptor);named=self.path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid()
                    or stat.S_IMODE(info.st_mode)!=0o600 or (info.st_dev,info.st_ino)!=(named.st_dev,named.st_ino)):
                raise EnrollmentHeld('journal_identity_invalid')
            self._journal_identity=(info.st_dev,info.st_ino);os.fsync(descriptor)
        finally:os.close(descriptor)
        rootfd=os.open(owner.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(rootfd)
        finally:os.close(rootfd)
        with self._db(bootstrap=True) as db:
            objects=db.execute("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").fetchall()
            if not objects:db.execute(CONFIG_DDL);db.execute(INTENT_DDL)
            self._schema(db)
            value=p.canonical_json(self.config).decode();existing=db.execute('SELECT value FROM config WHERE singleton=1').fetchone()
            if existing and existing[0]!=value:raise EnrollmentHeld('enrollment_policy_changed')
            if not existing:
                if db.execute('SELECT 1 FROM intent LIMIT 1').fetchone():raise EnrollmentHeld('enrollment_policy_missing')
                db.execute('INSERT INTO config VALUES(1,?)',(value,))

    @contextmanager
    def _db(self,*,bootstrap=False):
        if os.getpid()!=self._pid:raise EnrollmentHeld('worker_process_changed')
        self.owner.check();info=self.path.lstat()
        if (self.path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid()
                or stat.S_IMODE(info.st_mode)!=0o600 or info.st_size>4*1024*1024
                or (info.st_dev,info.st_ino)!=self._journal_identity):
            raise EnrollmentHeld('journal_identity_changed')
        self._sidecars()
        db=sqlite3.connect(self.path,timeout=5,isolation_level=None)
        try:
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            if db.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise EnrollmentHeld('journal_invalid')
            if not bootstrap:
                self._schema(db)
                rows=db.execute('SELECT singleton,value FROM config').fetchall()
                if rows!=[(1,p.canonical_json(self.config).decode())]:raise EnrollmentHeld('enrollment_policy_changed')
            yield db
            self.owner.check();after=self.path.lstat()
            if (after.st_dev,after.st_ino)!=self._journal_identity:raise EnrollmentHeld('journal_identity_changed')
            self._sidecars()
            db.commit()
        except BaseException:db.rollback();raise
        finally:db.close()

    def _sidecars(self):
        for suffix in ('-journal','-wal','-shm'):
            path=Path(str(self.path)+suffix)
            try:info=path.lstat()
            except FileNotFoundError:continue
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid()
                    or stat.S_IMODE(info.st_mode)!=0o600 or info.st_size>16*1024*1024):
                raise EnrollmentHeld('journal_sidecar_invalid')

    @staticmethod
    def _schema(db):
        query="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        memory=sqlite3.connect(':memory:')
        try:
            memory.execute(CONFIG_DDL);memory.execute(INTENT_DDL)
            if db.execute(query).fetchall()!=memory.execute(query).fetchall():raise EnrollmentHeld('journal_schema_changed')
        finally:memory.close()

    def _request(self,request):
        url=self.config['origin']+'/ltds/worker/v1/viewer-claims'
        query=Request(url,data=p.canonical_json(request),method='POST',headers={'Authorization':'Bearer '+self._bearer,
            'Accept':'application/json','Content-Type':'application/json','Accept-Encoding':'identity'})
        watchdog=_TransferWatchdog(30)
        try:
            opener=build_opener(ProxyHandler({}),_NoRedirect(),_DeadlineHTTPSHandler(watchdog,context=self._ssl_context)).open
            response=opener(query,timeout=min(10,watchdog.remaining()))
        except HTTPError as error:
            watchdog.close();error.close();raise EnrollmentHeld('claim_rejected_or_unavailable') from None
        except Exception:
            watchdog.close();raise EnrollmentHeld('claim_unavailable') from None
        try:
            if (any(len(response.headers.get_all(name,[]))>1 for name in ('Content-Length','Content-Type','Content-Encoding','Transfer-Encoding'))
                    or (response.headers.get('Transfer-Encoding') and
                        (response.headers.get('Transfer-Encoding').lower()!='chunked' or response.headers.get('Content-Length')))
                    or response.status!=200 or response.geturl()!=url or response.headers.get('Content-Type','').split(';')[0]!='application/json'
                    or response.headers.get('Content-Encoding','identity')!='identity'):
                raise EnrollmentHeld('claim_response_invalid')
            parts=[];count=0
            while True:
                watchdog.remaining();data=response.read1(min(65537-count,65536))
                if not data:break
                parts.append(data);count+=len(data)
                if count>65536:raise EnrollmentHeld('claim_response_invalid')
            raw=b''.join(parts)
            if len(raw)>65536:raise EnrollmentHeld('claim_response_invalid')
            length=response.headers.get('Content-Length')
            if length is not None and (not length.isascii() or not length.isdecimal() or int(length)!=len(raw)):
                raise EnrollmentHeld('claim_response_invalid')
            return p.decode_submission(raw)
        except EnrollmentHeld:raise
        except Exception:raise EnrollmentHeld('claim_response_unavailable') from None
        finally:response.close();watchdog.close()

    def _validate_claim(self,claim,request):
        fields=('schemaVersion','authorityId','jobId','viewerTaskId','viewerGeneration','environment','nodeId',
            'workerId','workerInstanceId','engineTaskId','inputInventorySha256','pipelineSha256','imageCount',
            'inputBytes','state','claimRequestId','claimToken')
        p._closed(claim,fields,'Viewer claim');c=self.config
        if (type(claim['schemaVersion']) is not int or claim['schemaVersion']!=1 or claim['state']!='claimed_unenrolled'
            or (claim['authorityId'],claim['nodeId'],claim['workerId'],claim['workerInstanceId'])!=(c['authorityId'],c['nodeId'],c['workerId'],c['instanceId'])
            or (claim['jobId'],claim['environment'],claim['claimRequestId'])!=(request['jobId'],request['environment'],request['claimRequestId'])
            or type(claim['claimToken']) is not str or not 1<=len(claim['claimToken'])<=128
            or any(ord(v)<33 or ord(v)>126 for v in claim['claimToken'])):
            raise EnrollmentHeld('claim_binding_mismatch')
        for field in ('jobId','viewerTaskId','engineTaskId'):p._uuid(claim[field],field)
        for field in ('viewerGeneration','imageCount','inputBytes'):p._integer(claim[field],field)
        for field in ('inputInventorySha256','pipelineSha256'):p._hash(claim[field],field)
        return claim

    def _check_gate(self,gate):
        state=gate.snapshot();info=os.fstat(gate._stream.fileno());path=gate.root/'gate.lock';named=path.lstat()
        named_fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK)
        try:named_identity=_identity(named_fd)
        finally:os.close(named_fd)
        if (gate.root.resolve()!=gate.root or path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o600
                or _identity(gate._stream.fileno())!=self.config['gateLock'] or named_identity!=self.config['gateLock']
                or (named.st_dev,named.st_ino)!=(info.st_dev,info.st_ino)
                or (state['engineId'],state['nodeId'],state['authorityId'],state['coordinatorOrigin'],state['generation'])!=
                   (self.config['engineId'],self.config['nodeId'],self.config['authorityId'],self.config['gateOrigin'],self.config['gateGeneration'])):
            raise EnrollmentHeld('gate_binding_changed')
        return state

    def enroll(self,job_id,request_id,environment):
        if os.getpid()!=self._pid:raise EnrollmentHeld('worker_process_changed')
        with self._operation_lock:return self._enroll(job_id,request_id,environment)

    def _enroll(self,job_id,request_id,environment):
        p._uuid(job_id,'job ID');p._slug(request_id,'claim request ID');p._environment(environment)
        if environment not in self.config['allowedEnvironments']:raise EnrollmentHeld('environment_forbidden')
        request=dict(jobId=job_id,claimRequestId=request_id,environment=environment)
        with self._db() as db:
            row=db.execute('SELECT request,receipt,claim,binding FROM intent WHERE singleton=1').fetchone()
            if row is None:
                receipt=secrets.token_hex(16);db.execute('INSERT INTO intent VALUES(1,?,?,NULL,NULL,?,NULL)',(p.canonical_json(request).decode(),receipt,'not_started'))
            else:
                if row[0]!=p.canonical_json(request).decode():raise EnrollmentHeld('enrollment_intent_changed')
                receipt=row[1]
        claim=self._validate_claim(self._request(request),request);claim_text=p.canonical_json(claim).decode()
        with self._db() as db:
            row=db.execute('SELECT claim,binding FROM intent WHERE singleton=1').fetchone()
            if row[0] is not None and row[0]!=claim_text:raise EnrollmentHeld('claim_replay_changed')
            previously_bound=row[1] is not None
            db.execute('UPDATE intent SET claim=? WHERE singleton=1',(claim_text,))
        c=self.config
        with NodeGate(c['engineId']) as gate:
            state=self._check_gate(gate);reservation=state['reservation']
            if reservation is None:
                if previously_bound:raise EnrollmentHeld('gate_reservation_disappeared')
                gate.reserve(c['nodeId'],c['gateOrigin'],c['authorityId'],c['gateGeneration'],receipt)
            elif reservation['receiptId']!=receipt or reservation['completionDigest'] is not None:
                raise EnrollmentHeld('gate_reservation_conflict')
            elif previously_bound and reservation['job'] is None:
                raise EnrollmentHeld('gate_binding_disappeared')
            expected_job=dict(id=job_id,workerId=c['workerId'],nodeId=c['nodeId'],authorityId=c['authorityId'])
            gate.bind(receipt,expected_job)
            state=self._check_gate(gate)
            if state['reservation']['job']!=expected_job:raise EnrollmentHeld('gate_binding_changed')
            binding=dict(schemaVersion=1,authorityId=c['authorityId'],coordinatorOrigin=c['gateOrigin'],
                gateGeneration=c['gateGeneration'],gateReservationId=receipt,nodeId=c['nodeId'],workerId=c['workerId'],
                engineId=c['engineId'],environment=environment,viewerTaskId=claim['viewerTaskId'],
                viewerGeneration=claim['viewerGeneration'],jobId=job_id,engineTaskId=claim['engineTaskId'],
                inputSetSha256=claim['inputInventorySha256'],pipelineSha256=claim['pipelineSha256'],outputBudgetBytes=c['outputBudget'])
            digest=validate_binding(binding);text=p.canonical_json(binding).decode()
            with self._db() as db:
                saved=db.execute('SELECT binding FROM intent WHERE singleton=1').fetchone()[0]
                if saved is not None and saved!=text:raise EnrollmentHeld('enrollment_binding_changed')
                db.execute('UPDATE intent SET binding=? WHERE singleton=1',(text,))
            # Commit intent before touching the allocation. After an
            # interrupted attempt, a missing file is ambiguous and never
            # authorizes creation of a replacement.
            with self._db() as db:
                slot_row=db.execute('SELECT slot_phase,slot_identity FROM intent WHERE singleton=1').fetchone()
                phase,recorded=slot_row
                if phase=='not_started':
                    if recorded is not None:raise EnrollmentHeld('slot_journal_invalid')
                    db.execute("UPDATE intent SET slot_phase='creating' WHERE singleton=1")
                    create=True
                elif phase=='creating':
                    if recorded is not None:raise EnrollmentHeld('slot_journal_invalid')
                    create=False
                elif phase=='committed':
                    if not isinstance(recorded,str):raise EnrollmentHeld('slot_journal_invalid')
                    create=False
                else:raise EnrollmentHeld('slot_journal_invalid')
            slot=open_slot(c['outputRoot'],c['outputRootIdentity'],digest,c['outputBudget'],
                           max_budget=c['maxBudget'],create=create)
            try:
                state=self._check_gate(gate)
                if state['reservation']['receiptId']!=receipt or state['reservation']['job']!=expected_job:
                    raise EnrollmentHeld('gate_binding_changed')
                slot._check()
                actual=p.canonical_json(dict(root=_identity(slot._root),allocation=_identity(slot._fd),
                                             metadata=_identity(slot._meta))).decode()
                with self._db() as db:
                    current=db.execute('SELECT slot_phase,slot_identity FROM intent WHERE singleton=1').fetchone()
                    if current==('creating',None):
                        db.execute("UPDATE intent SET slot_phase='committed',slot_identity=? WHERE singleton=1",(actual,))
                    elif current!=('committed',actual):raise EnrollmentHeld('slot_identity_changed')
                slot._check();self._check_gate(gate)
                return dict(state='enrolled_no_dispatch',bindingSha256=digest)
            finally:slot.close()
