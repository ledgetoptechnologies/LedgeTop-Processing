"""Owned worker stop/config-pin/start handover inside managed maintenance only.

No queue, timer, credential provisioning, engine mutation or forced termination.
All unknown service outcomes retain the journal and require reconciliation.
"""
from contextlib import contextmanager
import base64
import hashlib
import json
import os
import re
from pathlib import Path
import shlex
import stat
import subprocess
import sys
import time
import uuid
from urllib.request import Request, build_opener, ProxyHandler, HTTPSHandler, HTTPRedirectHandler
from urllib.error import HTTPError,URLError

from processing import reference_protocol as p
from processing.reference_tls import verified_client_context
from processing.reference_worker_readiness import PATH, PROTOCOL, source_digest, validate_settings


class WorkerHandoverHeld(ValueError):pass
class WorkerHandoverUncertain(WorkerHandoverHeld):pass


def digest(raw):return hashlib.sha256(raw).hexdigest()
def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def private_bytes(path, maximum=65536):
    path=Path(path).absolute()
    info=path.lstat()
    if (path.resolve()!=path or any(part.is_symlink() for part in (path,*path.parents))
            or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size>maximum
            or info.st_uid!=os.geteuid() or info.st_mode & 0o077):
        raise WorkerHandoverHeld('private_worker_config_required')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    try:
        observed=os.fstat(fd)
        if (observed.st_dev,observed.st_ino)!=(info.st_dev,info.st_ino):raise WorkerHandoverHeld('worker_config_changed')
        with os.fdopen(fd,'rb',closefd=False) as stream:raw=stream.read(maximum+1)
        if len(raw)>maximum:raise WorkerHandoverHeld('worker_config_too_large')
        return raw
    finally:os.close(fd)


def durable_json(path,value):
    from ops.node_ingress import save
    save(path,value)


class SystemdWorkerControl:
    """One reviewed user unit, exact source/config command and process identity."""
    def __init__(self,settings,*,runner=subprocess.run):
        self.settings=settings;self.runner=runner
        self.unit=settings['unitName'];self.config=Path(settings['configPath'])
        self.source=Path(settings['sourceRoot']);self.python=Path(settings['pythonExecutable'])
        if (type(self.unit) is not str or not self.unit.endswith('.service')
                or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-' for c in self.unit)):
            raise WorkerHandoverHeld('fixed_worker_unit_required')
        self._unit()

    def _unit(self):
        path=Path(self.settings['unitFile']).absolute();info=path.lstat()
        if (path.resolve()!=path or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                or info.st_uid!=os.geteuid() or info.st_mode & 0o022 or info.st_size>16384):
            raise WorkerHandoverHeld('reviewed_worker_unit_required')
        raw=path.read_bytes()
        if digest(raw)!=self.settings['unitSha256']:raise WorkerHandoverHeld('worker_unit_changed')
        entries={}
        for line in raw.decode('utf-8').splitlines():
            if not line or line.startswith(('#',';','[')):continue
            if '=' not in line:raise WorkerHandoverHeld('worker_unit_changed')
            key,value=line.split('=',1)
            if key in entries:raise WorkerHandoverHeld('ambiguous_worker_unit')
            entries[key]=value.replace('%h',str(Path.home()))
        expected=[str(self.python),'-B','-m','ops.reference_worker_daemon','--config',str(self.config)]
        if (shlex.split(entries.get('ExecStart',''))!=expected
                or Path(entries.get('WorkingDirectory','')).resolve()!=self.source
                or entries.get('Type')!='simple' or entries.get('SendSIGKILL')!='no'
                or entries.get('KillMode')!='mixed' or entries.get('KillSignal') not in (None,'SIGTERM','15')
                or entries.get('UMask')!='0077' or entries.get('NoNewPrivileges')!='yes'
                or entries.get('FailureAction') not in (None,'none') or entries.get('SuccessAction') not in (None,'none')
                or any(key.startswith('Exec') and key!='ExecStart' for key in entries)):
            raise WorkerHandoverHeld('worker_unit_command_changed')

    def _run(self,arguments):
        self._unit()
        try:result=self.runner(['/usr/bin/systemctl','--user',*arguments],capture_output=True,text=True,timeout=15)
        except (OSError,subprocess.SubprocessError):raise WorkerHandoverUncertain('worker_service_outcome_unknown') from None
        if result.returncode:raise WorkerHandoverHeld('worker_service_command_failed')
        return result.stdout

    def _loaded_unit(self):
        # Fragment bytes do not prove what systemd loaded: drop-ins and a stale
        # manager cache can change termination or launch semantics independently.
        names=('FragmentPath','DropInPaths','NeedDaemonReload','Type','KillMode','KillSignal',
            'SendSIGKILL','UMask','NoNewPrivileges','ExecStart','ExecStartPre','ExecStartPost',
            'ExecStop','ExecStopPost','ExecReload','FailureAction','SuccessAction')
        raw=self._run(['show','--no-pager','--property='+','.join(names),self.unit])
        if len(raw)>32768:raise WorkerHandoverHeld('loaded_worker_unit_changed')
        lines=raw.splitlines()
        if len(lines)!=len(names) or any('=' not in line for line in lines):
            raise WorkerHandoverHeld('loaded_worker_unit_unavailable')
        fields=dict(line.split('=',1) for line in lines)
        expected=dict(FragmentPath=self.settings['unitFile'],DropInPaths='',NeedDaemonReload='no',Type='simple',
            KillMode='mixed',KillSignal='15',SendSIGKILL='no',UMask='0077',NoNewPrivileges='yes',
            ExecStartPre='',ExecStartPost='',ExecStop='',ExecStopPost='',ExecReload='',FailureAction='none',SuccessAction='none')
        if set(fields)!=set(names) or any(fields.get(key)!=value for key,value in expected.items()):
            raise WorkerHandoverHeld('loaded_worker_unit_changed')
        # systemctl show's ExecStart value is a structured command record. Refuse
        # multiple commands or unsupported encodings instead of guessing argv.
        command=fields['ExecStart']
        match=re.fullmatch(r'\{ path=([^;{}]+) ; argv\[\]=([^;{}]+) ; ignore_errors=no ; start_time=[^;{}]* ; stop_time=[^;{}]* ; pid=[0-9]+ ; code=[^;{}]* ; status=[^;{}]* \}',command)
        expected_argv=[str(self.python),'-B','-m','ops.reference_worker_daemon','--config',str(self.config)]
        if match is None or match.group(1)!=str(self.python) or shlex.split(match.group(2))!=expected_argv:
            raise WorkerHandoverHeld('loaded_worker_command_changed')

    def probe(self):
        self._loaded_unit()
        raw=self._run(['show','--no-pager','--property=MainPID,ActiveState,SubState,ControlGroup,InvocationID,FragmentPath',self.unit])
        fields=dict(line.split('=',1) for line in raw.splitlines() if '=' in line)
        if (set(fields)!={'MainPID','ActiveState','SubState','ControlGroup','InvocationID','FragmentPath'}
                or fields['FragmentPath']!=self.settings['unitFile'] or not fields['MainPID'].isdigit()):
            raise WorkerHandoverHeld('worker_service_identity_unavailable')
        value=dict(pid=int(fields['MainPID']),active=fields['ActiveState'],substate=fields['SubState'],
            cgroup=fields['ControlGroup'],invocationId=fields['InvocationID'])
        if value['pid']:
            proc=Path('/proc')/str(value['pid'])
            if (proc.stat().st_uid!=os.geteuid() or (proc/'cwd').resolve()!=self.source
                    or (proc/'exe').resolve()!=self.python.resolve()
                    or (proc/'cmdline').read_bytes().split(b'\0')[:-1] != [os.fsencode(x) for x in
                        (str(self.python),'-B','-m','ops.reference_worker_daemon','--config',str(self.config))]):
                raise WorkerHandoverHeld('worker_process_identity_changed')
            value['startTime']=(proc/'stat').read_text().rsplit(')',1)[1].split()[19]
        return value

    def stopped(self,previous):
        value=self.probe()
        if value['pid']!=0 or value['active'] not in ('inactive','failed'):return False
        if previous.get('pid'):
            try:
                start=(Path('/proc')/str(previous['pid'])/'stat').read_text().rsplit(')',1)[1].split()[19]
            except FileNotFoundError:pass
            else:
                if start==previous.get('startTime'):return False
        group=previous.get('cgroup')
        if not group or not group.startswith('/') or '..' in Path(group).parts:
            raise WorkerHandoverHeld('worker_cgroup_identity_unavailable')
        root=Path('/sys/fs/cgroup')/group.lstrip('/')
        if root.exists():
            if root.resolve()!=root:raise WorkerHandoverHeld('worker_cgroup_changed')
            count=0
            for path in root.rglob('cgroup.procs'):
                count+=1
                if count>1024 or path.is_symlink() or path.read_text().strip():return False
        return True

    def command(self,action):
        if action not in ('stop','start'):raise WorkerHandoverHeld('invalid_worker_service_action')
        self._loaded_unit()
        self._run(['--no-block',action,self.unit])


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None


class ReferenceWorkerHandover:
    """One existing managed-update receipt owns all worker side effects."""
    FIELDS=('schemaVersion','unitName','unitFile','unitSha256','pythonExecutable','configPath',
            'sourceRoot','sourceSha256','readinessOrigin','readinessCAFile')

    def __init__(self,settings,receipt,*,guard,control=None,readiness_request=None,clock=time.monotonic):
        if not sys.platform.startswith('linux'):raise WorkerHandoverHeld('linux_worker_handover_required')
        p._closed(settings,self.FIELDS,'reviewed reference worker')
        if type(settings['schemaVersion']) is not int or settings['schemaVersion']!=1:raise WorkerHandoverHeld('worker_manifest_version')
        for name in ('unitSha256','sourceSha256'):p._hash(settings[name],name)
        for name in ('unitFile','pythonExecutable','configPath','sourceRoot','readinessCAFile'):
            if type(settings[name]) is not str or not Path(settings[name]).is_absolute():raise WorkerHandoverHeld('fixed_worker_paths_required')
        from urllib.parse import urlsplit
        endpoint=urlsplit(settings['readinessOrigin'])
        if (endpoint.scheme!='https' or not endpoint.hostname or endpoint.username is not None or endpoint.password is not None
                or endpoint.path not in ('','/') or endpoint.query or endpoint.fragment):raise WorkerHandoverHeld('fixed_worker_tls_origin_required')
        self.settings=dict(settings);self.receipt=Path(receipt);self.path=self.receipt/'worker-handover.json'
        self.config_path=Path(settings['configPath']);self.guard=guard;self.clock=clock
        self.control=control or SystemdWorkerControl(settings)
        self.readiness_request=readiness_request or self._https_readiness
        self._lease=None;self.state=None
        self._pid=os.getpid();self._gate_owner=None
        self._manifest_sha=p.digest(self.settings)

    def __enter__(self):
        import fcntl
        lock=self.config_path.parent/'.reference-worker-maintenance.lock'
        fd=os.open(lock,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid() or info.st_mode & 0o077:
                raise WorkerHandoverHeld('worker_config_lease_changed')
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BaseException:os.close(fd);raise
        self._lease=fd;self._lease_path=lock;self._lease_identity=(info.st_dev,info.st_ino)
        try:
            if self.path.exists():
                self.state=json.loads(private_bytes(self.path,256*1024))
                if self.state.get('manifestSha256')!=self._manifest_sha:raise WorkerHandoverHeld('worker_handover_binding_changed')
        except BaseException:os.close(fd);self._lease=None;raise
        return self

    def __exit__(self,*_):
        if self._pid!=os.getpid():raise WorkerHandoverHeld('worker_handover_process_changed')
        if self._lease is not None:os.close(self._lease);self._lease=None

    def _check_lease(self):
        if self._lease is None or self._pid!=os.getpid():raise WorkerHandoverHeld('worker_handover_owner_required')
        info=self._lease_path.lstat();opened=os.fstat(self._lease)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid() or info.st_mode & 0o077
                or (info.st_dev,info.st_ino)!=self._lease_identity or (opened.st_dev,opened.st_ino)!=self._lease_identity):
            raise WorkerHandoverHeld('worker_config_lease_changed')

    def _save(self,**values):
        self._check_lease()
        self.state.update(values);durable_json(self.path,self.state)

    def _config(self):
        raw=private_bytes(self.config_path);config=json.loads(raw)
        readiness=validate_settings(config.get('maintenanceReadiness'))
        if (readiness['sourceRoot']!=self.settings['sourceRoot'] or readiness['sourceSha256']!=self.settings['sourceSha256']
                or source_digest(self.settings['sourceRoot'])!=self.settings['sourceSha256']):
            raise WorkerHandoverHeld('worker_source_binding_changed')
        return raw,config

    def _https_readiness(self,request,config):
        context=verified_client_context(self.settings['readinessCAFile'])
        opener=build_opener(ProxyHandler({}),HTTPSHandler(context=context),_NoRedirect())
        url=self.settings['readinessOrigin'].rstrip('/')+PATH
        message=Request(url,data=encoded(request),headers={'Authorization':'Bearer '+config['maintenanceReadiness']['bearer'],
            'Content-Type':'application/json'},method='POST')
        with opener.open(message,timeout=10) as response:
            raw=response.read(65537)
            if (response.status!=200 or response.geturl()!=url or len(raw)>65536
                    or not response.headers.get('Content-Type','').startswith('application/json')
                    or response.headers.get('Content-Encoding') not in (None,'identity')):
                raise WorkerHandoverHeld('worker_readiness_unavailable')
        return json.loads(raw)

    def ready(self,*,expected_sha=None,activation=None,require_idle=True):
        raw,config=self._config();nonce=str(uuid.uuid4())
        if expected_sha is not None and digest(raw)!=expected_sha:raise WorkerHandoverHeld('worker_config_changed')
        value=self.readiness_request(dict(schemaVersion=1,nonce=nonce,requireIdle=require_idle),config)
        process=self.control.probe()
        p._closed(value,('schemaVersion','protocol','nonce','bootId','pid','nodeId','workerId','workerInstanceId','authorityId',
            'engineId','gateGeneration','engineVersion','configSha256','sourceSha256','driverBindingSha256','readerEpoch',
            'activation','ready','idle','optionsSha256','safeOptionsSha256','loadedSourceSha256'),'worker readiness proof')
        expected=dict(schemaVersion=1,protocol=PROTOCOL,nonce=nonce,nodeId=config['nodeId'],workerId=config['workerId'],
            workerInstanceId=config['workerInstanceId'],authorityId=config['authorityId'],engineId=config['engineId'],
            gateGeneration=config['gateGeneration'],engineVersion=config['engineVersion'],configSha256=digest(raw),
            sourceSha256=self.settings['sourceSha256'],activation=activation,ready=True,pid=process['pid'])
        if (type(value) is not dict or any(value.get(key)!=item for key,item in expected.items())
                or process['active']!='active' or process['pid']<=0
                or type(value['schemaVersion']) is not int or type(value['pid']) is not int
                or type(value['gateGeneration']) is not int or type(value['ready']) is not bool or type(value['idle']) is not bool
                or (require_idle and value.get('idle') is not True)):
            raise WorkerHandoverHeld('worker_readiness_binding_changed')
        for name in ('bootId','readerEpoch'):p._slug(value.get(name),name)
        p._uuid(value['bootId'],'worker boot')
        p._hash(value.get('driverBindingSha256'),'worker driver pin')
        for key in ('optionsSha256','safeOptionsSha256','loadedSourceSha256'):p._hash(value[key],key)
        return value,process

    def authorize_plan(self,node_id,node,adapter):
        self._check_lease()
        raw,config=self._config()
        if (config['nodeId']!=node_id or config['engineId']!=adapter.args.docker_engine_id
                or config['engineOrigin'].rstrip('/')!=adapter.node.origin.rstrip('/')):
            raise WorkerHandoverHeld('worker_engine_binding_changed')
        if self.state is not None and (self.state['planId'],self.state['targetImage'],self.state['targetEngineVersion'])!=(
                node['planId'],node['targetImage'],node['targetEngineVersion']):
            raise WorkerHandoverHeld('worker_handover_plan_changed')

    def _gate(self,gate):
        self._check_lease()
        _,config=self._config();state=gate.snapshot()
        if (state['engineId'],state['nodeId'],state['authorityId'],state['coordinatorOrigin'],state['generation'],state['reservation'])!=(
                config['engineId'],config['nodeId'],config['authorityId'],config['gateCoordinatorOrigin'].rstrip('/'),config['gateGeneration'],None):
            raise WorkerHandoverHeld('worker_physical_gate_changed')
        self.guard()

    def stop_old(self,gate,node,deadline):
        self._gate(gate)
        self._gate_owner=gate
        if self.state is None:
            raw,config=self._config()
            while True:
                try:
                    proof,process=self.ready(expected_sha=digest(raw),activation=config['maintenanceReadiness']['activation'])
                    break
                except HTTPError as error:
                    if error.code!=503 or self.clock()>=deadline:raise
                except (URLError,TimeoutError):
                    if self.clock()>=deadline:raise
                time.sleep(min(0.05,max(0,deadline-self.clock())))
            self.state=dict(schemaVersion=1,manifestSha256=self._manifest_sha,planId=node['planId'],targetImage=node['targetImage'],
                targetEngineVersion=node['targetEngineVersion'],oldConfig=base64.b64encode(raw).decode(),oldConfigSha256=digest(raw),
                oldProof=proof,oldProcess=process,activationId=str(uuid.uuid4()),stage='stop_pending')
            self._save()
            self.control.command('stop')
            self._save(stage='stop_issued')
        while not self.control.stopped(self.state['oldProcess']):
            if self.clock()>=deadline:raise WorkerHandoverUncertain('worker_stop_outcome_unknown')
            time.sleep(min(0.2,max(0,deadline-self.clock())))
        if digest(private_bytes(self.config_path))!=self.state['oldConfigSha256']:raise WorkerHandoverHeld('worker_config_changed')
        self._save(stage='worker_stopped')

    def before_replace(self,gate):
        self._gate(gate)
        if self.state is None or self.state['stage']!='worker_stopped' or not self.control.stopped(self.state['oldProcess']):
            raise WorkerHandoverHeld('worker_stop_not_proven')
        _,config=self._config()
        closed_worker_census(config)

    def _replace_config(self,raw,expected_sha):
        self._check_lease()
        if digest(private_bytes(self.config_path))!=expected_sha:raise WorkerHandoverHeld('worker_config_compare_and_swap_failed')
        temporary=self.config_path.with_name(self.config_path.name+'.handover-'+self.state['activationId'])
        fd=os.open(temporary,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,'wb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
        if digest(private_bytes(self.config_path))!=expected_sha:raise WorkerHandoverHeld('worker_config_compare_and_swap_failed')
        os.replace(temporary,self.config_path)
        fd=os.open(self.config_path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(fd)
        finally:os.close(fd)

    def pin_new(self,gate):
        self.before_replace(gate)
        old=json.loads(base64.b64decode(self.state['oldConfig']))
        activation=dict(schemaVersion=1,activationId=self.state['activationId'],planId=self.state['planId'],
            imageId=self.state['targetImage'],engineVersion=self.state['targetEngineVersion'])
        old['engineVersion']=self.state['targetEngineVersion'];old['maintenanceReadiness']['activation']=activation
        raw=encoded(old)
        self._save(stage='config_swap_pending',newConfigSha256=digest(raw),activation=activation)
        self._replace_config(raw,self.state['oldConfigSha256'])
        self._save(stage='config_committed')

    def start_new(self,deadline):
        self._check_lease();self.guard()
        ambiguous=self.state['stage']=='worker_start_pending'
        if self.state['stage']=='config_committed':
            self._save(stage='worker_start_pending');self.control.command('start');self._save(stage='worker_start_issued')
        elif self.state['stage'] not in ('worker_start_pending','worker_start_issued','worker_ready'):
            raise WorkerHandoverHeld('worker_start_not_authorized')
        while True:
            try:
                proof,process=self.ready(expected_sha=self.state['newConfigSha256'],activation=self.state['activation'])
                if proof['bootId']==self.state['oldProof']['bootId']:raise WorkerHandoverHeld('old_worker_boot_still_serving')
                public=dict(schemaVersion=1,planId=self.state['planId'],manifestSha256=self._manifest_sha,
                    targetImage=self.state['targetImage'],targetEngineVersion=self.state['targetEngineVersion'],
                    activationId=self.state['activationId'],configSha256=self.state['newConfigSha256'],
                    sourceSha256=self.settings['sourceSha256'],readinessSha256=p.digest(proof))
                self._save(stage='worker_ready',newProof=proof,newProcess=process,publicReceipt=public);return proof
            except Exception:
                if self.clock()>=deadline:
                    if ambiguous:raise WorkerHandoverUncertain('worker_start_outcome_unknown') from None
                    raise WorkerHandoverHeld('new_worker_readiness_unavailable') from None
                time.sleep(min(0.2,max(0,deadline-self.clock())))

    def verify_ready(self,*,require_idle=True):
        if self.state is None or self.state['stage']!='worker_ready':raise WorkerHandoverHeld('worker_handover_not_ready')
        deadline=self.clock()+2
        while True:
            try:
                proof,process=self.ready(expected_sha=self.state['newConfigSha256'],activation=self.state['activation'],require_idle=require_idle)
                break
            except HTTPError as error:
                if error.code!=503 or self.clock()>=deadline:raise
            except (URLError,TimeoutError):
                if self.clock()>=deadline:raise
            time.sleep(0.05)
        if require_idle and (proof['bootId']!=self.state['newProof']['bootId'] or process!=self.state['newProcess']):
            raise WorkerHandoverHeld('worker_ready_process_changed')
        return proof

    def public_receipt_sha256(self):
        if self.state is None or self.state.get('stage')!='worker_ready':raise WorkerHandoverHeld('worker_handover_not_ready')
        expected=dict(schemaVersion=1,planId=self.state['planId'],manifestSha256=self._manifest_sha,
            targetImage=self.state['targetImage'],targetEngineVersion=self.state['targetEngineVersion'],
            activationId=self.state['activationId'],configSha256=self.state['newConfigSha256'],
            sourceSha256=self.settings['sourceSha256'],readinessSha256=p.digest(self.state['newProof']))
        if self.state.get('publicReceipt')!=expected:raise WorkerHandoverHeld('worker_handover_receipt_changed')
        return p.digest(expected)

    def reconcile_pin(self):
        if self.state['stage']!='config_swap_pending':return
        if digest(private_bytes(self.config_path))!=self.state['newConfigSha256']:
            raise WorkerHandoverUncertain('worker_config_swap_outcome_unknown')
        fd=os.open(self.config_path,os.O_RDONLY|os.O_NOFOLLOW)
        try:os.fsync(fd)
        finally:os.close(fd)
        fd=os.open(self.config_path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(fd)
        finally:os.close(fd)
        self._save(stage='config_committed')

    def stop_new(self,deadline):
        self._check_lease();self.guard()
        if self.state['stage'] not in ('worker_start_issued','worker_ready'):
            raise WorkerHandoverUncertain('worker_start_outcome_unknown')
        if digest(private_bytes(self.config_path))!=self.state['newConfigSha256']:raise WorkerHandoverHeld('worker_config_changed')
        process=self.control.probe()
        if process['pid']:
            self._save(stage='rollback_stop_pending',rollbackProcess=process)
            self.control.command('stop');self._save(stage='rollback_stop_issued')
            while not self.control.stopped(process):
                if self.clock()>=deadline:raise WorkerHandoverUncertain('rollback_worker_stop_unknown')
                time.sleep(min(0.2,max(0,deadline-self.clock())))
        elif not self.control.stopped(self.state.get('newProcess',self.state['oldProcess'])):
            raise WorkerHandoverUncertain('rollback_worker_stop_unknown')
        self._save(stage='rollback_worker_stopped')

    def restore_old_config(self,gate):
        self._gate(gate)
        if self.state['stage'] not in ('rollback_worker_stopped','worker_stopped'):
            raise WorkerHandoverHeld('rollback_worker_not_stopped')
        raw=base64.b64decode(self.state['oldConfig'])
        if digest(raw)!=self.state['oldConfigSha256']:raise WorkerHandoverHeld('old_worker_config_changed')
        observed=digest(private_bytes(self.config_path))
        if observed!=self.state['oldConfigSha256']:
            if observed!=self.state.get('newConfigSha256'):raise WorkerHandoverHeld('worker_config_compare_and_swap_failed')
            self._save(stage='rollback_config_pending')
            self._replace_config(raw,observed)
        self._save(stage='rollback_config_committed')

    def start_old(self,deadline):
        self._check_lease();self.guard()
        ambiguous=self.state['stage']=='rollback_start_pending'
        if self.state['stage']=='rollback_config_committed':
            self._save(stage='rollback_start_pending');self.control.command('start');self._save(stage='rollback_start_issued')
        elif self.state['stage'] not in ('rollback_start_pending','rollback_start_issued','rolled_back'):
            raise WorkerHandoverHeld('rollback_config_not_committed')
        old=json.loads(base64.b64decode(self.state['oldConfig']))
        while True:
            try:
                proof,process=self.ready(expected_sha=self.state['oldConfigSha256'],activation=old['maintenanceReadiness']['activation'])
                self._save(stage='rolled_back',rollbackProof=proof,rollbackReadyProcess=process)
                return proof
            except Exception:
                if self.clock()>=deadline:
                    if ambiguous:raise WorkerHandoverUncertain('rollback_worker_start_unknown') from None
                    raise WorkerHandoverHeld('rollback_worker_readiness_unavailable') from None
                time.sleep(min(0.2,max(0,deadline-self.clock())))


def closed_worker_census(config):
    """Compose existing immutable history validators; never edit worker stores."""
    from processing.reference_driver import ReferenceDriver
    from processing.reference_execution import ExecutionRouter,_migration_rows
    from types import SimpleNamespace
    import sqlite3
    root=Path(config['stateRoot'])
    physical=dict(nodeId=config['nodeId'],workerId=config['workerId'],engineId=config['engineId'],authorityId=config['authorityId'],
        origin=config['gateCoordinatorOrigin'].rstrip('/'),gateGeneration=config['gateGeneration'])
    drivers=ReferenceDriver.migration_census(root/'driver.sqlite',physical_binding=physical)
    worker=SimpleNamespace(path=root/'worker.sqlite',ledger=SimpleNamespace(path=root/'scratch.sqlite'),
        outbox=SimpleNamespace(path=root/'outbox.sqlite'))
    if (root/'execution-routes.sqlite').exists():
        tree=root/'treedar-delivery.sqlite'
        if tree.exists():
            # The existing census verifies all route/result/outbox bindings.
            _migration_rows(tree,'SELECT job FROM executions')
            db=sqlite3.connect(tree.as_uri()+'?mode=ro&immutable=1',uri=True)
            db.row_factory=sqlite3.Row
        else:
            db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row
            db.execute('CREATE TABLE executions(job TEXT)')
        try:
            return ExecutionRouter.migration_census(root/'execution-routes.sqlite',driver_path=root/'driver.sqlite',
                physical_binding=physical,odm_worker=worker,failure_outbox=SimpleNamespace(path=root/'failure-outbox.sqlite'),
                delivery_db=db,current_envelope=None)
        finally:db.close()
    local={row['job']:row for row in _migration_rows(worker.path,'SELECT * FROM executions')}
    if set(local)!=set(drivers['jobs']) or any(item['kind']!='odm' for item in drivers['jobs'].values()):
        raise WorkerHandoverHeld('worker_history_not_closed')
    storage=dict(reservations=_migration_rows(root/'scratch.sqlite','SELECT * FROM reservations'),
        success=_migration_rows(root/'outbox.sqlite','SELECT * FROM completions'),
        failure=_migration_rows(root/'failure-outbox.sqlite','SELECT * FROM completions'),
        successReceipts=_migration_rows(root/'outbox.sqlite','SELECT * FROM durable_receipts'),
        failureReceipts=_migration_rows(root/'failure-outbox.sqlite','SELECT * FROM durable_receipts'))
    for key,field in (('success','outboxId'),('failure','failureOutboxId')):
        expected={item[field] for item in drivers['jobs'].values() if item[field] is not None}
        if {item['id'] for item in storage[key]}!=expected or {item['outbox'] for item in storage[key+'Receipts']}!=expected:
            raise WorkerHandoverHeld('worker_outbox_history_not_closed')
    for item in storage['reservations']:
        if item['state']=='expired':continue
        job=drivers['jobs'].get(item['job'])
        if job is None or item['state']!='terminal' or (item['generation'],item['environment'])!=(job['binding']['generation'],job['binding']['environment']):
            raise WorkerHandoverHeld('worker_scratch_history_not_closed')
        if json.loads(item['settings'])['manifest_sha256']!=job['binding']['manifestSha256']:
            raise WorkerHandoverHeld('worker_scratch_history_not_closed')
    for job_id,item in drivers['jobs'].items():
        row=local[job_id]
        if row['phase']=='delivered':
            result=p.validate_result_manifest(json.loads(row['result']),job_id=job_id,generation=item['binding']['generation'],
                environment=item['binding']['environment'],pipeline_digest=item['binding']['pipelineDigest'])
            # Version is from the immutable accepted result, never the new pin.
            runtime=dict(engineVersion=result['engineVersions']['odm'])
        else:runtime={}
        ExecutionRouter._migration_odm_closed(dict(binding=json.dumps(item['binding']),runtime=json.dumps(runtime)),item,row,worker,storage)
    return drivers


def readonly_handover_sha256(settings,receipt):
    """Authenticate current serving identity without changing past completion."""
    handover=ReferenceWorkerHandover(settings,receipt,guard=lambda:None)
    handover.state=json.loads(private_bytes(handover.path,256*1024))
    if handover.state.get('manifestSha256')!=handover._manifest_sha:
        raise WorkerHandoverHeld('worker_handover_binding_changed')
    handover.verify_ready(require_idle=False)
    return handover.public_receipt_sha256()
