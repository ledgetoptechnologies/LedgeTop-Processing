"""Host-side canonical ODM worker phases within an owned backend update.

No scheduler, Docker socket in a worker, credential issuance or import action.
The caller retains the existing maintenance/ingress locks and exact node plan.
"""
from dataclasses import dataclass,asdict
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
import uuid

from ops.node_ingress import save
from ops.reference_worker_daemon import DaemonStateLock
from processing.node_gate import NodeGate,GateBusy
from processing import reference_protocol as p
from processing.update_registry import _run
from processing.container_update import ComposeNode,PreflightError,UpdateError,Uncertain


class WorkerMaintenanceHeld(ValueError):pass

class WorkerComposeNode(ComposeNode):
    """Existing Compose recipe, with the owned worker fence at Docker mutation."""
    def __init__(self,args,root,worker):
        super().__init__(args,root);self.worker_maintenance=worker

    def replace(self,image):
        try:command=self.prepare(image)
        except Uncertain:raise
        except UpdateError:raise PreflightError('replacement validation failed before container mutation') from None
        worker=self.worker_maintenance
        state,gate,adapter=worker._replacement_context
        require(adapter is self,'worker_replacement_adapter_changed')
        require(image in (state['oldImage'],state['targetImage']),'worker_replacement_image_changed')
        worker.fence(state,gate)
        worker._stopped_owner(state)
        current=worker.config()[1]
        require(current==state['worker']['beforeConfig'],'worker_configuration_changed_before_backend_mutation')
        require(worker.publication_read(current)==state['worker']['beforePublication'],'worker_publication_changed_before_backend_mutation')
        worker.fence(state,gate)
        self.docker(command,timeout=120)

def require(value,reason):
    if not value:raise WorkerMaintenanceHeld(reason)

def digest(value):return p.digest(value)

def private(path,*,directory=False):
    path=Path(path).absolute();info=path.lstat()
    require(path.resolve()==path and not any(x.is_symlink() for x in (path,*path.parents))
        and info.st_uid==os.geteuid() and not info.st_mode&0o077
        and (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode) and info.st_nlink==1),'owned_worker_path_required')
    return path,info

def identity(info):return [info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns,info.st_uid,info.st_gid,stat.S_IMODE(info.st_mode),info.st_nlink]

def read_file(path,maximum=65536):
    path,before=private(path);require(before.st_size<=maximum,'worker_file_bound')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    try:
        require(identity(os.fstat(fd))==identity(before),'worker_file_changed')
        with os.fdopen(fd,'rb',closefd=False) as stream:raw=stream.read(maximum+1)
        require(len(raw)==before.st_size and identity(path.lstat())==identity(before),'worker_file_changed')
    finally:os.close(fd)
    return raw,identity(before)

def read_json(path):return p.decode_submission(read_file(path)[0])

@dataclass(frozen=True)
class WorkerPolicy:
    node_id:str
    authority_id:str
    engine_id:str
    gate_generation:int
    container_id:str
    container_name:str
    image_id:str
    recipe_sha256:str
    config_directory:str
    config_base_sha256:str
    initial_config_sha256:str
    state_root:str
    scratch_root:str
    gate_directory:str
    result_origin:str
    tls_ca_sha256:str
    tls_certificate_sha256:str
    legacy_maintenance_paths:tuple
    container_config_directory:str='/run/ledgetop-worker'
    docker_executable:str='/usr/bin/docker'
    docker_endpoint:str='unix:///run/docker.sock'
    timeout_seconds:int=30

    def __post_init__(self):
        require(self.node_id in ('cluster','main'),'fixed_worker_node_required');p._uuid(self.authority_id,'worker authority')
        p._integer(self.gate_generation,'worker gate generation')
        require(type(self.engine_id) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9:._-]{0,255}',self.engine_id),'fixed_worker_engine_required')
        require(type(self.container_id) is str and re.fullmatch('[a-f0-9]{64}',self.container_id),'fixed_worker_container_required')
        require(type(self.image_id) is str and re.fullmatch('sha256:[a-f0-9]{64}',self.image_id),'fixed_worker_image_required')
        p._slug(self.container_name,'worker container')
        for sha in (self.recipe_sha256,self.config_base_sha256,self.initial_config_sha256,self.tls_ca_sha256,self.tls_certificate_sha256):p._hash(sha,'worker policy digest')
        for path in (self.config_directory,self.state_root,self.scratch_root,self.gate_directory,self.container_config_directory):
            require(type(path) is str and Path(path).is_absolute(),'absolute_worker_policy_path_required')
        require(type(self.legacy_maintenance_paths) is tuple and 1<=len(self.legacy_maintenance_paths)<=8,'legacy_updater_fence_required')
        for path in self.legacy_maintenance_paths:require(type(path) is str and Path(path).is_absolute() and Path(path).name=='maintenance.json','legacy_updater_path_required')
        require(self.docker_executable=='/usr/bin/docker' and self.docker_endpoint=='unix:///run/docker.sock','fixed_host_docker_required')
        require(type(self.timeout_seconds) is int and 1<=self.timeout_seconds<=60,'bounded_worker_operation_required')
        from urllib.parse import urlsplit
        import ipaddress
        parsed=urlsplit(self.result_origin)
        require(parsed.scheme=='https' and parsed.path=='' and not parsed.query and not parsed.fragment and parsed.username is None and parsed.password is None and parsed.port,'fixed_private_worker_tls_required')
        require('%' not in parsed.hostname,'literal_worker_ip_required')
        ipaddress.ip_address(parsed.hostname)

    @property
    def sha256(self):return digest(asdict(self))

def load_profile(root):
    root=Path(root);require(not os.path.lexists(root/'maintenance.json'),'legacy_maintenance_path_must_be_unavailable')
    raw=read_json(root/'maintenance-worker-v2.json')
    p._closed(raw,('schemaVersion','maintenance','worker'),'worker maintenance profile')
    require(type(raw['schemaVersion']) is int and raw['schemaVersion']==2,'worker_maintenance_v2_required')
    settings=raw['maintenance'];required={'nodeId','coordinator','project','service'}
    require(type(settings) is dict and required<=set(settings) and set(settings)<=required|{'expectedEngine','nodePort','uploadLayout','processLayout'},'closed_worker_maintenance_settings_required')
    require(type(raw['worker']) is dict and type(raw['worker'].get('legacy_maintenance_paths')) is list,'closed_worker_maintenance_policy_required')
    worker=dict(raw['worker']);worker['legacy_maintenance_paths']=tuple(worker['legacy_maintenance_paths'])
    p._closed(worker,tuple(WorkerPolicy.__dataclass_fields__),'worker maintenance policy')
    policy=WorkerPolicy(**worker)
    require(settings['nodeId']==policy.node_id,'worker_settings_node_changed')
    for path in policy.legacy_maintenance_paths:require(not os.path.lexists(path),'legacy_updater_still_enabled')
    return settings,policy

def offline_census(root):
    """Under actual DaemonStateLock; bounded immutable SQL, no repairs/expiry."""
    root,_=private(root,directory=True);result={};paths=[]
    with os.scandir(root) as stream:
        for entry in stream:
            require(len(paths)<32,'worker_state_inventory_bound');paths.append(Path(entry.path))
    required={'driver.sqlite','worker.sqlite','scratch.sqlite','outbox.sqlite','failure-outbox.sqlite'}
    require(required<={x.name for x in paths},'complete_worker_state_required')
    for path in paths:
        require(not path.name.endswith(('-wal','-shm','-journal')),'worker_sql_sidecar_unresolved')
        require(path.name in required|{'engine-cleanup.sqlite','worker-daemon.lock','scratch.sqlite.readers.lock'},'unknown_worker_state_file')
        private(path)
        if not path.name.endswith('.sqlite'):continue
        raw,fingerprint=read_file(path,64*1024**2)
        require(raw[:20]==b'SQLite format 3\0\x10\x00\x01\x01','worker_delete_sqlite_required')
        db=sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True,timeout=1);db.row_factory=sqlite3.Row
        try:
            budget=[0]
            def progress():budget[0]+=1;return int(budget[0]>10000)
            db.set_progress_handler(progress,1000);db.execute('BEGIN')
            require(db.execute('PRAGMA quick_check').fetchone()[0]=='ok','worker_sql_integrity_failed')
            names=list(db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 65"))
            require(len(names)<=64,'worker_schema_bound');tables={r[0] for r in names}
            if path.name=='driver.sqlite':
                require('driver_jobs' in tables and not db.execute("SELECT 1 FROM driver_jobs WHERE stage!='completed' LIMIT 1").fetchone(),'worker_pending_driver_or_acceptance')
            if path.name in ('outbox.sqlite','failure-outbox.sqlite'):
                require('completions' in tables and not db.execute("SELECT 1 FROM completions WHERE state!='acknowledged' LIMIT 1").fetchone(),'worker_pending_outbox')
            if path.name=='worker.sqlite':
                require('executions' in tables and not db.execute("SELECT 1 FROM executions WHERE phase NOT IN ('delivered','failure_delivered') LIMIT 1").fetchone(),'worker_execution_intent_unresolved')
            if path.name=='scratch.sqlite':
                require({'pins','reservations','reader_session_epochs','reader_session_binding','reader_owned_pins'}<=tables,'canonical_worker_reader_schema_required')
                require(not db.execute("SELECT 1 FROM pins WHERE kind='read' AND state!='released' LIMIT 1").fetchone(),'worker_reader_pin_unresolved')
                require(not db.execute("SELECT 1 FROM reader_owned_pins o WHERE o.state NOT IN ('released','superseded') OR NOT EXISTS(SELECT 1 FROM pins p WHERE p.reservation=o.reservation AND p.object=o.object AND p.reference=o.reference AND p.kind='read' AND p.state='released') LIMIT 1").fetchone(),'worker_reader_owner_unresolved')
                require(db.execute('SELECT COUNT(*) FROM reader_session_epochs').fetchone()[0]<=10000,'worker_reader_epoch_bound')
                epochs={r['epoch']:dict(r) for r in db.execute('SELECT * FROM reader_session_epochs')}
                binding=db.execute('SELECT current_epoch FROM reader_session_binding WHERE id=1').fetchone()
                require(binding is not None and binding[0] in epochs and epochs[binding[0]]['state']=='closed','worker_reader_epoch_unclosed')
                require(epochs[binding[0]]['successor'] is None,'worker_reader_epoch_unclosed')
                qualified={binding[0]}
                for epoch in epochs:
                    seen=set();current=epoch
                    while current not in qualified:
                        require(current in epochs and current not in seen and epochs[current]['state']=='superseded','worker_reader_epoch_unclosed')
                        seen.add(current);current=epochs[current]['successor']
                    qualified.update(seen)
                require(not db.execute("SELECT 1 FROM reservations WHERE state NOT IN ('terminal','expired') LIMIT 1").fetchone(),'worker_scratch_execution_unresolved')
                if 'control_charges' in tables:require(not db.execute('SELECT 1 FROM control_charges LIMIT 1').fetchone(),'noncanonical_worker_control_history_requires_review')
            if path.name=='engine-cleanup.sqlite':require(not db.execute("SELECT 1 FROM removals WHERE phase!='removed' LIMIT 1").fetchone(),'worker_engine_cleanup_unresolved')
            hashes={}
            for name in tables:
                require(re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,127}',name),'worker_sql_schema_changed')
                count=db.execute('SELECT COUNT(*) FROM "'+name+'"').fetchone()[0];require(count<=10000,'worker_row_bound')
                columns=[r[1] for r in db.execute('PRAGMA table_info("'+name+'")')];require(0<len(columns)<=64 and all(re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,127}',c) for c in columns),'worker_column_bound')
                total=' + '.join('COALESCE(length(CAST("'+column+'" AS BLOB)),0)' for column in columns)
                require(db.execute('SELECT COALESCE(SUM('+total+'),0) FROM "'+name+'"').fetchone()[0]<=64*1024**2,'worker_row_byte_bound')
                rows=[dict(r) for r in db.execute('SELECT * FROM "'+name+'"')]
                if name=='driver_jobs':
                    for row in rows:row.pop('background_checked',None)
                hashes[name]=digest(sorted(rows,key=lambda value:p.canonical_json(value)))
            result[path.name]=hashes
        finally:db.close()
        require(identity(path.lstat())==fingerprint,'worker_state_changed_during_census')
    return result

_HISTORY={'driver.sqlite':'driver_jobs','worker.sqlite':'executions','outbox.sqlite':'completions','failure-outbox.sqlite':'completions'}

def immutable_history(root):
    """Read exact retained claims/manifests/outbox rows, not live reader metadata.

    Only used before reopening the owned updating barrier, when no new claims
    may appear. It writes nothing and tolerates no unstable SQLite observation.
    """
    result={}
    for filename,table in _HISTORY.items():
        path=Path(root)/filename;_path,before=private(path)
        require(before.st_size<=64*1024**2 and not any(os.path.lexists(str(path)+suffix) for suffix in ('-wal','-shm','-journal')),'worker_history_snapshot_unavailable')
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        try:require(os.read(fd,20)==b'SQLite format 3\0\x10\x00\x01\x01' and identity(os.fstat(fd))==identity(before),'worker_history_sqlite_format_changed')
        finally:os.close(fd)
        db=sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True,timeout=1);db.row_factory=sqlite3.Row
        try:
            db.execute('BEGIN');count=db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]
            require(count<=10000,'worker_history_row_bound')
            columns=[r[1] for r in db.execute('PRAGMA table_info('+table+')')]
            require(0<len(columns)<=64 and all(re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,127}',c) for c in columns),'worker_history_schema_changed')
            total=' + '.join('COALESCE(length(CAST("'+c+'" AS BLOB)),0)' for c in columns)
            require(db.execute('SELECT COALESCE(SUM('+total+'),0) FROM '+table).fetchone()[0]<=64*1024**2,'worker_history_byte_bound')
            rows=[dict(r) for r in db.execute('SELECT * FROM '+table)]
            if table=='driver_jobs':
                for row in rows:row.pop('background_checked',None)
            result[filename]=digest(sorted(rows,key=p.canonical_json))
        finally:db.close()
        require(identity(path.lstat())==identity(before),'worker_history_changed_during_read')
    return result


# One-shot child bounds connect/TLS/header/body together and is killed/joined on
# timeout. Its only secret source is the existing private config, never argv or
# environment; stdout is retained privately, closed-validated, never forwarded.
_HEALTH_READ='''import json,ssl,sys,urllib.request,urllib.error
from pathlib import Path
directory=Path(sys.argv[1]);config=json.loads((directory/'worker.json').read_bytes())
ca=Path(config['caFile']);base=Path(sys.argv[3]);ca=directory/ca.relative_to(base)
context=ssl.create_default_context();context.load_verify_locations(cafile=str(ca))
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):return None
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect(),urllib.request.HTTPSHandler(context=context))
request=urllib.request.Request(sys.argv[2]+'/ltds/worker/v1/health',headers={'Authorization':'Bearer '+config['resultReadBearer'],'Accept':'application/json','Accept-Encoding':'identity'})
with opener.open(request,timeout=30) as response:
 assert response.status==200 and response.geturl()==request.full_url
 assert response.headers.get('Content-Type','').split(';')[0].lower()=='application/json'
 assert response.headers.get('Content-Encoding','identity')=='identity'
 raw=response.read(16385);assert len(raw)<=16384
 sys.stdout.buffer.write(raw)
'''

class WorkerMaintenance:
    def __init__(self,root,policy,api,*,runner=None,operator=None,owned_schedule_request_id=None):
        require(type(policy) is WorkerPolicy,'concrete_worker_policy_required')
        self.root,_=private(root,directory=True);self.policy=policy;self.api=api;self.runner=runner or _run
        self._pid=os.getpid();self._policy_sha=policy.sha256;self._owner=None
        self.operator=operator;self.owned_schedule_request_id=owned_schedule_request_id
        self.publication=self.root/'worker-current.json';self.marker=self.root/'worker-publication-marker.json'
        self._check()

    def _check(self):
        require(os.getpid()==self._pid and self.policy.sha256==self._policy_sha,'worker_maintenance_binding_changed')
        for path in self.policy.legacy_maintenance_paths:require(not os.path.lexists(path),'legacy_updater_still_enabled')
        for path in (self.policy.config_directory,self.policy.state_root,self.policy.scratch_root,self.policy.gate_directory):private(path,directory=True)
        require(NodeGate(self.policy.engine_id).root==Path(self.policy.gate_directory),'actual_canonical_gate_namespace_required')
        private(Path(self.policy.gate_directory)/'gate.lock');private(Path(self.policy.gate_directory)/'gate.json')

    def docker(self,args):
        self._check()
        result=self.runner([self.policy.docker_executable,'--host',self.policy.docker_endpoint,*args],timeout=self.policy.timeout_seconds,max_bytes=32768)
        require(result.returncode==0,'worker_docker_outcome_unknown')
        require(len(result.stdout)<=32768 and len(result.stderr)<=32768,'worker_docker_output_bound')
        return result.stdout.decode() if isinstance(result.stdout,bytes) else result.stdout

    def inspect(self):
        # Full immutable recipe, excluding only dynamic process/health state.
        template='{"id":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"config":{{json .Config}},"host":{{json .HostConfig}},"mounts":{{json .Mounts}},"running":{{json .State.Running}},"pid":{{json .State.Pid}},"exitCode":{{json .State.ExitCode}}}'
        value=p.decode_submission(self.docker(['inspect','--format',template,self.policy.container_id]))
        p._closed(value,('id','name','image','config','host','mounts','running','pid','exitCode'),'worker container inspection')
        require((value['id'],value['name'],value['image'])==(self.policy.container_id,'/'+self.policy.container_name,self.policy.image_id),'owned_worker_container_changed')
        require(type(value['running']) is bool and type(value['pid']) is int and type(value['exitCode']) is int,'worker_process_state_unknown')
        recipe={k:value[k] for k in ('image','config','host','mounts')}
        require(digest(recipe)==self.policy.recipe_sha256,'worker_recipe_changed')
        host=value['host'];config=value['config']
        require(config['User']=='1000:1000' and config['Entrypoint']==['python','-m','ops.reference_worker_daemon']
            and config['Cmd']==['--config',str(Path(self.policy.container_config_directory)/'worker.json')] and host['NetworkMode']=='host'
            and host['ReadonlyRootfs'] is True and host['Privileged'] is False
            and host.get('UsernsMode','')=='' and host.get('AutoRemove') is False
            and host.get('StopTimeout',config.get('StopTimeout')) in (None,-1),'canonical_worker_recipe_required')
        require(len(value['mounts'])==4,'canonical_worker_mount_inventory_required')
        mounts={m['Destination']:m for m in value['mounts']}
        worker_config,_view=self.config()
        expected={self.policy.container_config_directory:(self.policy.config_directory,False),worker_config['stateRoot']:(self.policy.state_root,True),
            worker_config['scratchRoot']:(self.policy.scratch_root,True),'/var/lib/ledgetop-gates/LedgeTop-Processing/node-gates/'+hashlib.sha256(self.policy.engine_id.encode()).hexdigest():(self.policy.gate_directory,True)}
        require(set(mounts)==set(expected),'canonical_worker_mount_inventory_required')
        for destination,(source,writable) in expected.items():
            mount=mounts[destination];require(mount['Type']=='bind' and mount['Source']==source and mount['RW'] is writable,'canonical_worker_mount_changed')
        return value

    def mounts(self):
        """Actual socket-free worker namespace stat, issued by trusted HOST CLI."""
        config,_view=self.config();gate_target='/var/lib/ledgetop-gates/LedgeTop-Processing/node-gates/'+hashlib.sha256(self.policy.engine_id.encode()).hexdigest()
        pairs=[(self.policy.config_directory,self.policy.container_config_directory),(self.policy.state_root,config['stateRoot']),
            (self.policy.scratch_root,config['scratchRoot']),(self.policy.gate_directory,gate_target),
            (str(Path(self.policy.config_directory)/'worker.json'),str(Path(self.policy.container_config_directory)/'worker.json'))]
        for key in ('caFile','serverCertFile','serverKeyFile'):
            pairs.append((str(Path(self.policy.config_directory)/Path(config[key]).relative_to(self.policy.container_config_directory)),config[key]))
        code='import os,stat,json,sys; print(json.dumps([[s.st_dev,s.st_ino,s.st_uid,s.st_gid,stat.S_IMODE(s.st_mode)] for p in json.loads(sys.argv[1]) for s in [os.lstat(p)]]))'
        actual=p.decode_submission(self.docker(['exec',self.policy.container_id,'python','-c',code,json.dumps([target for _source,target in pairs])]))
        wanted=[]
        for source,_target in pairs:
            value=Path(source).lstat();wanted.append([value.st_dev,value.st_ino,value.st_uid,value.st_gid,stat.S_IMODE(value.st_mode)])
        require(actual==wanted,'worker_mount_inode_or_owner_changed')
        return digest(wanted)

    def config(self):
        path=Path(self.policy.config_directory)/'worker.json';raw,fingerprint=read_file(path);value=p.decode_submission(raw)
        require(digest({k:v for k,v in value.items() if k!='engineVersion'})==self.policy.config_base_sha256,'immutable_worker_configuration_changed')
        require(value.get('serviceProfile')=={'schemaVersion':1,'mode':'odm_only'} and 'executionRouting' not in value,'canonical_odm_worker_profile_required')
        require((value['nodeId'],value['authorityId'],value['engineId'],value['gateGeneration'])==
            (self.policy.node_id,self.policy.authority_id,self.policy.engine_id,self.policy.gate_generation),'worker_physical_configuration_changed')
        require(all(type(value[k]) is str and Path(value[k]).is_absolute() for k in ('stateRoot','scratchRoot'))
            and value['stateRoot']!=value['scratchRoot'],'worker_persistent_mount_configuration_changed')
        require(type(value['engineVersion']) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}',value['engineVersion']),'worker_engine_version_required')
        import ssl
        for field,expected in (('caFile',self.policy.tls_ca_sha256),('serverCertFile',self.policy.tls_certificate_sha256)):
            relative=Path(value[field]).relative_to(self.policy.container_config_directory);certificate=Path(self.policy.config_directory)/relative
            body,_=read_file(certificate,16384)
            require(hashlib.sha256(ssl.PEM_cert_to_DER_cert(body.decode())).hexdigest()==expected,'worker_tls_configuration_changed')
        key=Path(self.policy.config_directory)/Path(value['serverKeyFile']).relative_to(self.policy.container_config_directory);private(key)
        return value,dict(sha256=hashlib.sha256(raw).hexdigest(),identity=fingerprint,engineVersion=value['engineVersion'])

    def health(self,*,idle):
        config,before=self.config()
        result=self.runner(['/usr/bin/python3','-I','-B','-c',_HEALTH_READ,self.policy.config_directory,self.policy.result_origin,self.policy.container_config_directory],timeout=self.policy.timeout_seconds,max_bytes=16384)
        require(result.returncode==0,'worker_private_health_unavailable');value=p.decode_submission(result.stdout)
        fields=('schemaVersion','profile','scope','identityScope','nodeId','authorityId','workerId','workerInstanceId','engineId','gateGeneration','engineVersion','state','reason','driverAlive','listenerAlive','workState','retainedJobs','pendingJobs','awaitingAcceptance')
        p._closed(value,fields,'canonical worker health')
        require(type(value['schemaVersion']) is int and value['schemaVersion']==1 and value['profile']=='odm_only'
            and value['scope']=='service_liveness' and value['identityScope']=='configured_service','worker_health_profile_changed')
        expected={key:config[key] for key in ('nodeId','authorityId','workerId','workerInstanceId','engineId','gateGeneration','engineVersion')}
        require(all(type(value[k]) is type(v) and value[k]==v for k,v in expected.items())
            and value['state']=='ready' and value['reason'] is None and value['driverAlive'] is True and value['listenerAlive'] is True,'worker_health_identity_changed')
        counts=[value[k] for k in ('retainedJobs','pendingJobs','awaitingAcceptance')]
        if value['workState']=='busy':require(counts==[None,None,None],'worker_health_counts_changed')
        else:
            require(all(type(n) is int and 0<=n<=2**53-1 for n in counts) and counts[0]>=counts[1]+counts[2],'worker_health_counts_changed')
            retained,pending,awaiting=counts
            wanted='empty' if not retained else 'pending_work' if pending else 'awaiting_acceptance' if awaiting else 'idle'
            require(value['workState']==wanted,'worker_health_counts_changed')
        if idle:require(value['workState'] in ('empty','idle') and value['pendingJobs']==0 and value['awaitingAcceptance']==0,'worker_not_quiescent')
        require(self.config()[1]==before,'worker_configuration_changed_during_health')
        return {k:value[k] for k in fields}

    def wait_health(self,*,idle):
        deadline=time.monotonic()+self.policy.timeout_seconds
        while True:
            try:return self.health(idle=idle)
            except WorkerMaintenanceHeld:
                if time.monotonic()>=deadline:raise
                time.sleep(min(.05,max(0,deadline-time.monotonic())))

    def fence(self,state,gate):
        self._check();snapshot=gate.snapshot()
        require(self.docker(['info','--format','{{.ID}}']).strip()==self.policy.engine_id,'actual_worker_engine_changed')
        require((snapshot['engineId'],snapshot['nodeId'],snapshot['authorityId'],snapshot['coordinatorOrigin'],snapshot['generation'],snapshot['reservation'])==
            (self.policy.engine_id,self.policy.node_id,self.policy.authority_id,self.api.origin,self.policy.gate_generation,None),'worker_maintenance_gate_changed')
        self.barrier(state)

    def barrier(self,state):
        p._uuid(state['planId'],'owned worker maintenance plan');p._integer(state['worker']['reservedRevision'],'owned worker maintenance revision')
        require(type(state['targetImage']) is str and re.fullmatch('sha256:[a-f0-9]{64}',state['targetImage']),'immutable_worker_backend_target_required')
        require(self.api.json('GET','/v1/prototype/authority')=={'authorityId':self.policy.authority_id},'worker_maintenance_authority_changed')
        node=self.api.json('GET','/v1/prototype/nodes/'+self.policy.node_id)['node']
        require(node['state']=='updating' and (node['planId'],node['targetImage'],node['revision'])==
            (state['planId'],state['targetImage'],state['worker']['reservedRevision']),'owned_worker_maintenance_barrier_changed')
        jobs=self.api.json('GET','/v1/prototype/jobs')['jobs']
        require(not any(j.get('nodeId')==self.policy.node_id and j['status'] in ('running','held') for j in jobs),'worker_claim_not_drained')

    def phase(self,state,receipt,phase,**fields):
        private(receipt,directory=True)
        state.setdefault('worker',{}).update(phase=phase,**fields);save(Path(receipt)/'managed-update.json',state)

    def publication_value(self,view,operation):return dict(schemaVersion=1,profileSha256=self.policy.sha256,config=view,operation=operation)

    def publication_read(self,view):
        require(self.marker.exists() and self.publication.exists(),'worker_publication_requires_explicit_initialization')
        marker=read_json(self.marker);p._closed(marker,('schemaVersion','profileSha256','initial'),'worker publication marker')
        require(type(marker['schemaVersion']) is int and marker['schemaVersion']==1 and marker['profileSha256']==self.policy.sha256,'worker_publication_marker_changed');p._hash(marker['initial'],'initial worker publication')
        value=read_json(self.publication);p._closed(value,('schemaVersion','profileSha256','config','operation'),'worker publication')
        require(value['schemaVersion']==1 and value['profileSha256']==self.policy.sha256 and value['config']==view,'worker_publication_changed')
        return value

    def initialize(self,view,operation):
        if self.publication.exists():return self.publication_read(view)
        require(not self.marker.exists() and view['sha256']==self.policy.initial_config_sha256,'worker_publication_missing_or_not_initial')
        value=self.publication_value(view,operation)
        save(self.marker,dict(schemaVersion=1,profileSha256=self.policy.sha256,initial=digest(value)))
        # A lost publication leaves the durable marker; missing is never reset.
        save(self.publication,value)
        return value

    def ready(self,adapter):
        require(self.inspect()['running'],'worker_service_not_running')
        self.mounts()
        self.health(idle=True);config,view=self.config()
        if self.publication.exists():self.publication_read(view)
        else:require(not self.marker.exists() and view['sha256']==self.policy.initial_config_sha256,'worker_publication_unresolved')
        native=self.backend({},adapter,target=adapter.inspect()['Image'])
        require(native['engineVersion']==config['engineVersion'],'worker_native_version_changed')
        current=self.runtime_current()
        require(current['engineVersion']==config['engineVersion'] and current['targetImage'] in (None,native['imageId']),'runtime_previous_binding_changed')
        return True

    def before_backend(self,state,receipt,gate,reserved,adapter):
        self._check();require('worker' not in state,'worker_stop_intent_must_not_replay')
        p._uuid(self.owned_schedule_request_id,'owned schedule request')
        require(self.operator is not None and self.operator.origin==self.api.origin,'owned_schedule_operator_required')
        response=self.operator.json('GET','/v1/prototype/nodes/'+self.policy.node_id+'/schedule-owned/'+self.owned_schedule_request_id)
        require(type(response) is dict and set(response)=={'receipt'},'owned_schedule_receipt_required')
        owned=response['receipt']
        require(type(owned) is dict and all(owned.get(key)==value for key,value in dict(requestId=self.owned_schedule_request_id,
            authorityId=self.policy.authority_id,nodeId=self.policy.node_id,planId=state['planId'],targetImage=state['targetImage']).items()),'owned_schedule_receipt_changed')
        require(state['binding'].get('ownedScheduleRequestId')==self.owned_schedule_request_id,'owned_schedule_binding_changed')
        state['worker']=dict(reservedRevision=reserved['reservedRevision'],profileSha256=self.policy.sha256)
        self.fence(state,gate);item=self.inspect();require(item['running'],'initial_worker_must_be_running')
        mount_sha=self.mounts()
        self.wait_health(idle=True);config,view=self.config();publication=self.initialize(view,state['planId'])
        runtime=self.runtime_current()
        require(runtime['engineVersion']==view['engineVersion'] and runtime['targetImage'] in (None,state['oldImage']),'runtime_previous_binding_changed')
        before=self.backend(state,adapter,target=state['oldImage'])
        require(before['engineVersion']==view['engineVersion'],'previous_worker_native_version_changed')
        self.phase(state,receipt,'stop_pending',beforeConfig=view,beforePublication=publication,beforeBackend=before,mountSha256=mount_sha,beforeRuntime=runtime)
        self.fence(state,gate)
        self.docker(['stop','--time','-1',self.policy.container_id])
        item=self.inspect();require(not item['running'] and item['pid']==0 and item['exitCode']==0,'graceful_worker_stop_unproven')
        self.phase(state,receipt,'stopped')
        self._owner=DaemonStateLock(self.policy.state_root).__enter__()
        census=offline_census(self.policy.state_root)
        self.phase(state,receipt,'quiesced',census=census)
        self._replacement_context=(state,gate,adapter)

    def lookup_stop(self,state):
        require(state['worker']['profileSha256']==self.policy.sha256,'worker_operation_policy_changed')
        item=self.inspect()
        return dict(committed=not item['running'] and item['pid']==0 and item['exitCode']==0,containerId=item['id'])

    def backend(self,state,adapter,*,target):
        require(self.docker(['info','--format','{{.ID}}']).strip()==self.policy.engine_id,'actual_worker_engine_changed')
        item=adapter.inspect();require(item['Image']==target and re.fullmatch('[a-f0-9]{64}',item['Id']),'worker_native_image_changed')
        # ComposeNode performs its existing exact Compose service/loopback
        # checks; independently inspect the same CID through the fixed socket.
        raw=self.docker(['inspect','--format','{"id":{{json .Id}},"image":{{json .Image}},"running":{{json .State.Running}},"ports":{{json .NetworkSettings.Ports}},"labels":{{json .Config.Labels}}}',item['Id']])
        observed=p.decode_submission(raw);require((observed['id'],observed['image'],observed['running'])==(item['Id'],target,True),'worker_native_container_changed')
        config,_=self.config()
        require(config['engineOrigin']==adapter.node.origin and config['gateCoordinatorOrigin']==self.api.origin,'worker_native_loopback_changed')
        from urllib.parse import urlsplit
        require(observed['ports'].get(str(adapter.args.node_port)+'/tcp')==[{'HostIp':'127.0.0.1','HostPort':str(urlsplit(adapter.node.origin).port)}],'worker_native_port_changed')
        labels=observed['labels'];require(labels.get('com.docker.compose.project')==adapter.args.project
            and labels.get('com.docker.compose.service')==adapter.args.service
            and labels.get('com.docker.compose.project.config_files')==adapter.args.compose_file,'worker_native_compose_changed')
        info=adapter.node.json('GET','/info');version=info.get('engineVersion')
        require(type(version) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}',version),'worker_native_version_unavailable')
        return dict(containerId=item['Id'],imageId=target,engineVersion=version)

    def _stopped_owner(self,state):
        require(self.lookup_stop(state)['committed'],'worker_stop_not_proven')
        if self._owner is None:self._owner=DaemonStateLock(self.policy.state_root).__enter__()
        self._owner.check(self.policy.state_root)
        require(offline_census(self.policy.state_root)==state['worker']['census'],'quiesced_worker_history_changed')

    def rebind(self,state,receipt,version,gate):
        worker=state['worker'];self._stopped_owner(state)
        path=Path(self.policy.config_directory)/'worker.json';config,view=self.config()
        if worker['phase']=='rebind_pending':
            # Local syscall reply/checkpoint may be lost. Only exact published
            # temp inode+bytes proves success; old config never implies replay.
            expected=worker['afterConfig']
            require(view['sha256']==expected['sha256'] and view['engineVersion']==expected['engineVersion']
                and view['identity'][:4]==expected['identity'][:4] and view['identity'][5:]==expected['identity'][5:],'worker_rebind_outcome_unresolved')
            worker['afterConfig']=view;save(Path(receipt)/'managed-update.json',state)
        elif worker['phase']=='quiesced':
            require(view==worker['beforeConfig'],'worker_previous_configuration_changed')
            config['engineVersion']=version;raw=p.canonical_json(config)
            temporary=path.parent/('worker-'+state['planId']+'.tmp')
            require(not os.path.lexists(temporary),'worker_rebind_temp_requires_inspection')
            self.fence(state,gate)
            fd=os.open(temporary,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
            try:
                with os.fdopen(fd,'wb',closefd=False) as stream:stream.write(raw);stream.flush();os.fsync(fd)
            finally:os.close(fd)
            after=dict(sha256=hashlib.sha256(raw).hexdigest(),identity=identity(temporary.lstat()),engineVersion=version)
            self.phase(state,receipt,'rebind_pending',afterConfig=after)
            require(self.config()[1]==worker['beforeConfig'],'worker_configuration_cas_failed')
            self.fence(state,gate)
            os.replace(temporary,path)
            fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(fd)
            finally:os.close(fd)
            # rename changes ctime. Identity binds stable inode/bytes/mtime;
            # record the actual published identity before advancing publication.
            current=self.config()[1]
            require(current['sha256']==after['sha256'] and current['identity'][:4]==after['identity'][:4]
                and current['identity'][5:]==after['identity'][5:],'worker_configuration_publication_changed')
            worker['afterConfig']=current;save(Path(receipt)/'managed-update.json',state)
        else:require(worker['phase'] in ('publication_pending','rebound','start_pending','verified','finish_pending'),'worker_rebind_phase_changed')
        config,current=self.config()
        require(current==worker['afterConfig'] and current['engineVersion']==version,'worker_rebound_configuration_changed')
        before=worker['beforePublication'];after=self.publication_value(current,state['planId'])
        publication=read_json(self.publication)
        if worker['phase']=='publication_pending':require(publication==after,'worker_publication_outcome_unresolved')
        else:
            require(publication==before,'worker_current_publication_cas_failed')
            self.phase(state,receipt,'publication_pending',afterPublication=after)
            self.fence(state,gate)
            save(self.publication,after)
        self.phase(state,receipt,'rebound',afterPublication=after)

    def after_backend(self,state,receipt,gate,adapter):
        self.fence(state,gate);worker=state['worker']
        require(worker['profileSha256']==self.policy.sha256,'worker_operation_policy_changed')
        backend=self.backend(state,adapter,target=state['targetImage'])
        if 'backend' in worker:require(backend==worker['backend'],'worker_native_checkpoint_changed')
        else:self.phase(state,receipt,worker['phase'],backend=backend)
        if worker['phase']=='verified':
            self.verify(state,adapter,idle=True);return
        if worker['phase']=='start_pending':
            # A discovered running original container may prove committed start.
            # Never repeat start when stopped, even if the original call failed.
            require(self.inspect()['running'],'worker_start_outcome_unresolved')
        else:
            require(worker['phase'] in ('quiesced','rebind_pending','publication_pending','rebound'),'worker_before_restart_phase_changed')
            if worker['phase']!='rebound':self.rebind(state,receipt,backend['engineVersion'],gate)
            self.close();self.fence(state,gate)
            self.phase(state,receipt,'start_pending',backend=backend)
        self.close()
        # Factory/Driver pin admission must enter the same NodeGate themselves.
        # The owned updating barrier + ingress/operation locks remain in force.
        gate.__exit__(None,None,None)
        try:
            self.barrier(state)
            if not self.inspect()['running']:
                # Only a new, FULL-journaled start intent reaches this branch;
                # callers resuming an unknown start were rejected above.
                require(worker.get('startIssued') is not True,'worker_start_must_not_replay')
                worker['startIssued']=True;save(Path(receipt)/'managed-update.json',state)
                self.docker(['start',self.policy.container_id])
            deadline=time.monotonic()+self.policy.timeout_seconds
            while True:
                self.barrier(state);item=self.inspect()
                require(item['running'],'worker_restart_failed')
                try:self.health(idle=True);break
                except WorkerMaintenanceHeld:
                    if time.monotonic()>=deadline:raise
                    time.sleep(min(.05,max(0,deadline-time.monotonic())))
        finally:
            # Failure to reacquire is a held operation; no synthetic gate reset.
            deadline=time.monotonic()+self.policy.timeout_seconds
            while True:
                try:gate.__enter__();break
                except GateBusy:
                    if time.monotonic()>=deadline:raise
                    time.sleep(.05)
        self.fence(state,gate);self.verify(state,adapter,idle=True)
        self.phase(state,receipt,'verified')

    def verify(self,state,adapter,*,idle):
        require(state['worker']['profileSha256']==self.policy.sha256,'worker_operation_policy_changed')
        require(self.inspect()['running'],'worker_service_not_running')
        self.mounts()
        config,view=self.config();self.publication_read(view)
        require(view==state['worker']['afterConfig'],'worker_configuration_not_operation_publication')
        native=self.backend(state,adapter,target=state['targetImage'])
        require(native==state['worker']['backend'] and native['engineVersion']==config['engineVersion'],'worker_native_restart_binding_changed')
        if idle:self.wait_health(idle=True)
        else:self.health(idle=False)
        if idle:
            wanted={filename:state['worker']['census'][filename][table] for filename,table in _HISTORY.items()}
            require(immutable_history(self.policy.state_root)==wanted,'retained_worker_history_changed_on_restart')

    def finish_intent(self,state,receipt):
        require(state['worker']['phase']=='verified','worker_finish_intent_must_not_replay')
        require(state['worker'].get('runtimePublication',{}).get('phase')=='committed','runtime_publication_required_before_finish')
        self.verify_runtime(state)
        self.phase(state,receipt,'finish_pending')

    def runtime_current(self):
        from processing.reference_runtime_versions import CURRENT_FIELDS
        require(self.operator is not None and self.operator.origin==self.api.origin,'runtime_operator_required')
        response=self.operator.json('GET','/v1/prototype/nodes/'+self.policy.node_id+'/runtime-version')
        require(type(response) is dict and set(response)=={'runtime'},'runtime_current_envelope_changed')
        current=response['runtime'];p._closed(current,CURRENT_FIELDS,'current runtime')
        require(type(current['schemaVersion']) is int and type(current['gateGeneration']) is int,'runtime_current_format_changed')
        require(all(current.get(key)==value for key,value in dict(schemaVersion=1,authorityId=self.policy.authority_id,nodeId=self.policy.node_id,
            engineId=self.policy.engine_id,gateGeneration=self.policy.gate_generation,scope='configured_runtime').items()),'runtime_current_identity_changed')
        p._integer(current['epoch'],'runtime epoch',allow_zero=True);p._integer(current['nodeEpoch'],'node runtime epoch',allow_zero=True)
        p._hash(current['bindingsSha256'],'runtime bindings')
        require(current['nodeEpoch']<=current['epoch'] and type(current['engineVersion']) is str
            and re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}',current['engineVersion']),'runtime_current_format_changed')
        if current['nodeEpoch']==0:require(current['targetImage'] is None and current['publicationRequestId'] is None,'runtime_initial_binding_changed')
        else:
            p._uuid(current['publicationRequestId'],'runtime publication request')
            require(type(current['targetImage']) is str and re.fullmatch('sha256:[a-f0-9]{64}',current['targetImage']),'runtime_current_image_changed')
        return current

    def verify_runtime(self,state):
        from processing.reference_runtime_versions import validate_version_receipt,validate_current_version
        journal=state['worker']['runtimePublication']
        p._closed(journal,('phase','request','receipt'),'runtime publication receipt')
        require(journal['phase']=='committed','runtime_publication_not_committed')
        worker=state['worker'];expected=dict(authorityId=self.policy.authority_id,planId=state['planId'],expectedRevision=worker['reservedRevision'],
            ownedScheduleRequestId=state['binding']['ownedScheduleRequestId'],expectedVersion=worker['beforeConfig']['engineVersion'],
            newVersion=worker['backend']['engineVersion'],targetImage=state['targetImage'],engineId=self.policy.engine_id,gateGeneration=self.policy.gate_generation)
        require(all(journal['request'].get(key)==value for key,value in expected.items()),'runtime_publication_request_changed')
        validate_version_receipt(journal['receipt'],journal['request'],self.policy.node_id)
        return validate_current_version(self.runtime_current(),journal['request'],self.policy.node_id,journal['receipt'])

    def publish_runtime(self,state,receipt,gate,adapter):
        """One owned POST; a lost response is reconciled by receipt lookup only."""
        from processing.reference_runtime_versions import validate_version_request,validate_version_receipt,validate_current_version
        self.fence(state,gate);self.verify(state,adapter,idle=True)
        worker=state['worker'];journal=worker.get('runtimePublication')
        if journal is None:
            old=self.runtime_current()
            require(old['engineVersion']==worker['beforeConfig']['engineVersion']
                and old['targetImage'] in (None,state['oldImage']),'runtime_previous_binding_changed')
            request=dict(schemaVersion=1,requestId=str(uuid.uuid4()),authorityId=self.policy.authority_id,planId=state['planId'],
                expectedRevision=worker['reservedRevision'],ownedScheduleRequestId=self.owned_schedule_request_id,
                expectedVersion=worker['beforeConfig']['engineVersion'],newVersion=worker['backend']['engineVersion'],
                targetImage=state['targetImage'],engineId=self.policy.engine_id,gateGeneration=self.policy.gate_generation)
            validate_version_request(request)
            journal=dict(phase='pending',request=request)
            self.phase(state,receipt,worker['phase'],runtimePublication=journal)
            self.fence(state,gate)
            private_receipt=read_json(Path(receipt)/'update.json')
            require(private_receipt['planId']==state['planId'] and private_receipt['reservedRevision']==worker['reservedRevision'],
                'runtime_original_update_token_binding_changed')
            response=self.operator.json('POST','/v1/prototype/nodes/'+self.policy.node_id+'/runtime-version',request,
                headers={'X-LTDS-Update-Token':private_receipt['updateToken']})
        else:
            if journal.get('phase')=='pending':p._closed(journal,('phase','request'), 'runtime publication intent')
            else:p._closed(journal,('phase','request','receipt'),'runtime publication receipt')
            require(journal['phase'] in ('pending','committed'),'runtime_publication_phase_changed')
            request=journal['request'];validate_version_request(request)
            expected=dict(authorityId=self.policy.authority_id,planId=state['planId'],expectedRevision=worker['reservedRevision'],
                ownedScheduleRequestId=self.owned_schedule_request_id,expectedVersion=worker['beforeConfig']['engineVersion'],
                newVersion=worker['backend']['engineVersion'],targetImage=state['targetImage'],engineId=self.policy.engine_id,gateGeneration=self.policy.gate_generation)
            require(all(request.get(key)==value for key,value in expected.items()),'runtime_publication_request_changed')
            response=self.operator.json('GET','/v1/prototype/nodes/'+self.policy.node_id+'/runtime-version/receipts/'+request['requestId'])
        require(type(response) is dict and set(response)=={'receipt'},'runtime_receipt_envelope_changed')
        published=validate_version_receipt(response['receipt'],request,self.policy.node_id)
        if journal['phase']=='committed':require(journal['receipt']==published,'runtime_historical_receipt_changed')
        else:
            journal=dict(phase='committed',request=request,receipt=published)
            self.phase(state,receipt,worker['phase'],runtimePublication=journal)
        validate_current_version(self.runtime_current(),request,self.policy.node_id,published)
        self.fence(state,gate)

    def close(self):
        if self._owner is not None:self._owner.__exit__(None,None,None);self._owner=None

