"""Linux startup/API/options-only qualification of a definite staged image.

No engine tasks are submitted: the fixed probe issues only two internal GETs.
This does not qualify reconstruction, GPU initialization/kernels, real imagery,
throughput, or resource use. It introduces no CPU/RAM/PID/thread limits. Every
Docker mutation is journaled before one attempt; lookup never mutates Docker.
Only a verified removed, previously stopped own container can yield success.
"""
from contextlib import contextmanager
from dataclasses import asdict,dataclass
import hashlib
import os
from pathlib import Path,PurePosixPath
import re
import sqlite3
import stat
import sys
import threading
import uuid

from .update_registry import DockerRegistry,_json,_sha,_load,_run,_digest,_integer
from .update_supervisor import seal_receipt
from .options import _validate_info,_validate_options


class QualifierError(ValueError):pass
class QualifierHeld(QualifierError):pass


PROBE = r'''"use strict";
const http=require("http");
setTimeout(()=>process.exit(2),65000).unref();
const deadline=Date.now()+60000;
function get(path,limit){return new Promise((resolve,reject)=>{
 const req=http.get({hostname:"127.0.0.1",port:3000,path,method:"GET",timeout:3000},res=>{
  let n=0,parts=[];
  if(res.statusCode!==200||!String(res.headers["content-type"]||"").includes("application/json")){res.resume();reject(new Error("response"));return;}
  res.on("data",chunk=>{n+=chunk.length;if(n>limit){req.destroy();reject(new Error("limit"));}else parts.push(chunk);});
  res.on("end",()=>resolve(Buffer.concat(parts).toString("utf8")));res.on("error",reject);
 });req.on("timeout",()=>req.destroy(new Error("timeout")));req.on("error",reject);
});}
(async()=>{while(Date.now()<deadline){try{
 const info=await get("/info",65536),options=await get("/options",262144);
 process.stdout.write(JSON.stringify({schemaVersion:1,info,options}));return;
 }catch(e){await new Promise(r=>setTimeout(r,250));}}
 process.exitCode=2;
})().catch(()=>{process.exitCode=2;});'''
PROBE_SHA=hashlib.sha256(PROBE.encode()).hexdigest()
WRITE_PATHS=('/var/www/data','/var/www/tmp','/var/www/logs','/tmp')
REQUEST_FIELDS={'schemaVersion','operationId','nodeId','engineId','authorityId','candidateId','repository',
    'manifestDigest','configDigest','localImageId','platform','previousImageId','hostGeneration','supervisorPolicySha256',
    'qualifierId','qualificationPolicySha256','receiptDirectory','requestSha256'}
_NUM=r'(?:0|[1-9][0-9]*)'
_PRE=r'(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)'
_SEMVER=re.compile(_NUM+r'\.'+_NUM+r'\.'+_NUM+r'(?:-'+_PRE+r'(?:\.'+_PRE+r')*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?\Z')
_BANNER=re.compile(r'\ANow using node v(?P<node>[0-9A-Za-z.+-]{1,32}) \(npm v(?P<npm>[0-9A-Za-z.+-]{1,32})\)\r?\n')


def parse_launcher_stdout(value):
    """Parse JSON or exactly one approved, anchored and <=96-byte NVM banner."""
    if type(value) is not str or len(value.encode())>1024*1024:raise QualifierHeld('invalid_launcher_stdout')
    prefix='';node=npm=None;kind='raw-json'
    match=_BANNER.match(value)
    if match:
        prefix=match.group(0);node,npm=match.group('node','npm')
        if len(prefix.encode())>96 or not _SEMVER.fullmatch(node) or not _SEMVER.fullmatch(npm):
            raise QualifierHeld('invalid_launcher_stdout')
        value=value[len(prefix):]
        # No extra line, whitespace prefix, second banner or diagnostic text.
        if not value.startswith('{'):raise QualifierHeld('invalid_launcher_stdout')
        kind='nvm-use-banner'
    try:wire=_load(value)
    except Exception:raise QualifierHeld('invalid_launcher_stdout') from None
    return wire,dict(launcherPrefixKind=kind,launcherNodeVersion=node,launcherNpmVersion=npm,
        launcherPrefixSha256=hashlib.sha256(prefix.encode()).hexdigest())


@dataclass(frozen=True)
class StartupPolicy:
    qualifier_id:str
    engine_id:str
    node_id:str
    authority_id:str
    expected_engine:str
    startup_spec_sha256:str
    supervisor_state_root:str
    run_user:str
    filesystem_mode:str='ephemeral-node-defaults-v1'
    launcher_stdout_mode:str='node-or-nvm-use-banner-v1'
    write_paths:tuple[str,...]=WRITE_PATHS
    working_directory:str='/var/www'
    node_executable:str='/usr/bin/node'
    command_timeout_seconds:int=30
    stop_timeout_seconds:int=15

    def __post_init__(self):
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}',self.qualifier_id):raise QualifierError('invalid_qualifier_identity')
        if self.node_id not in ('main','cluster') or self.expected_engine not in ('odm','odx'):raise QualifierError('explicit_node_and_engine_required')
        if self.filesystem_mode!='ephemeral-node-defaults-v1':raise QualifierError('reviewed_filesystem_mode_required')
        if self.launcher_stdout_mode!='node-or-nvm-use-banner-v1':raise QualifierError('reviewed_launcher_stdout_mode_required')
        try:
            if str(uuid.UUID(self.authority_id))!=self.authority_id:raise ValueError()
        except Exception:raise QualifierError('exact_authority_required') from None
        if not re.fullmatch(r'[a-f0-9]{64}',self.startup_spec_sha256):raise QualifierError('reviewed_startup_spec_required')
        if not re.fullmatch(r'(?:[0-9]+(?::[0-9]+)?|odm)',self.run_user):raise QualifierError('reviewed_container_user_required')
        root=Path(self.supervisor_state_root)
        if not root.is_absolute() or any(c in str(root) for c in '\r\n,'):raise QualifierError('fixed_state_root_required')
        if type(self.write_paths) is not tuple or set(self.write_paths)!=set(WRITE_PATHS) or len(self.write_paths)!=4:
            raise QualifierError('reviewed_private_write_paths_required')
        if self.working_directory!='/var/www' or self.node_executable!='/usr/bin/node':raise QualifierError('reviewed_node_layout_required')
        for value in (self.command_timeout_seconds,self.stop_timeout_seconds):
            if type(value) is not int or not 1<=value<=300:raise QualifierError('bounded_timeout_required')


def startup_spec(image):
    """Hash only reviewed default startup fields; never print raw arguments."""
    keys={'User','WorkingDir','Entrypoint','Cmd','Volumes'}
    if type(image) is not dict or set(image)!=keys:raise QualifierHeld('invalid_image_startup_metadata')
    if (type(image['User']) is not str or image['WorkingDir']!='/var/www' or image['Volumes'] not in (None,{})
            or not isinstance(image['Entrypoint'],list) or not image['Entrypoint']
            or (image['Cmd'] is not None and not isinstance(image['Cmd'],list))):
        raise QualifierHeld('unsupported_default_startup')
    args=image['Entrypoint']+(image['Cmd'] or [])
    if (args[0]!='/usr/bin/node' or any(type(arg) is not str or len(arg)>4096 or '\n' in arg or '\r' in arg for arg in args)
            or any(re.search(r'(?i)token|password|credential|secret|authorization|://',arg) for arg in args)):
        raise QualifierHeld('unsafe_default_startup_arguments')
    return _sha(image)


class StartupQualifier:
    def __init__(self,path,policy,*,registry,runner=None):
        if not sys.platform.startswith('linux'):raise QualifierError('linux_host_required')
        if type(policy) is not StartupPolicy or not isinstance(registry,DockerRegistry):raise QualifierError('fixed_policy_and_actual_registry_required')
        if registry.policy.engine_id!=policy.engine_id:raise QualifierError('registry_physical_engine_changed')
        self.path=Path(path).absolute();self.policy=policy;self.registry=registry;self.runner=runner or _run
        self.policy_sha256=_sha(dict(policy=asdict(policy),registryPolicy=asdict(registry.policy),probeSha256=PROBE_SHA,
                                    scope='startup-api-options-only-v1'))
        self.binding=dict(qualifierId=policy.qualifier_id,policySha256=self.policy_sha256)
        self._pid=os.getpid();self._mutex=threading.RLock();self._busy=False;self._fd=None
        self.path.parent.mkdir(parents=True,mode=0o700,exist_ok=True)
        if any(item.is_symlink() for item in (self.path,*self.path.parents)):raise QualifierError('private_qualifier_journal_required')
        info=self.path.parent.stat()
        if info.st_uid!=os.geteuid() or info.st_mode & 0o077:raise QualifierError('private_qualifier_journal_required')
        root=Path(policy.supervisor_state_root)
        if any(item.is_symlink() for item in (root,*root.parents)) or not root.is_dir():raise QualifierError('private_supervisor_root_required')
        info=root.stat()
        if info.st_uid!=os.geteuid() or info.st_mode & 0o077:raise QualifierError('private_supervisor_root_required')
        with self._exclusive():
            fd=os.open(self.path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600);os.close(fd)
            with self._db() as db:
                db.execute('CREATE TABLE IF NOT EXISTS qualifier_binding(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)')
                db.execute('''CREATE TABLE IF NOT EXISTS qualifications (
                    id TEXT PRIMARY KEY,request TEXT NOT NULL,state TEXT NOT NULL,name TEXT NOT NULL UNIQUE,
                    container TEXT,defaults TEXT,evidence TEXT,stopped TEXT,receipt TEXT)''')
                value=_json(dict(policySha256=self.policy_sha256,registryPath=str(registry.path),leaseIdentity=self._identity))
                old=db.execute('SELECT value FROM qualifier_binding WHERE id=1').fetchone()
                if old and old[0]!=value:raise QualifierHeld('qualifier_policy_binding_changed')
                if not old and db.execute('SELECT 1 FROM qualifications').fetchone():raise QualifierHeld('unbound_qualification_history')
                db.execute('INSERT OR IGNORE INTO qualifier_binding VALUES(1,?)',(value,))
            self._sync(self.path.parent)

    @staticmethod
    def _sync(path):
        fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(fd)
        finally:os.close(fd)

    @contextmanager
    def _exclusive(self):
        if self._pid!=os.getpid():raise QualifierHeld('qualifier_process_changed')
        with self._mutex:
            if self._busy:raise QualifierHeld('qualifier_operation_in_progress')
            self._busy=True
            fd=None
            try:
                import fcntl
                path=self.path.with_name(self.path.name+'.lock')
                fd=os.open(path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
                info,current=os.fstat(fd),path.lstat()
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid() or info.st_mode & 0o077
                        or (info.st_dev,info.st_ino)!=(current.st_dev,current.st_ino)):raise QualifierHeld('private_qualifier_lease_required')
                fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                self._fd=fd;self._identity=(info.st_dev,info.st_ino);self._sync(self.path.parent)
                yield
            except QualifierError:raise
            except Exception:raise QualifierHeld('qualifier_outcome_requires_inspection') from None
            finally:
                self._fd=None
                if fd is not None:os.close(fd)
                self._busy=False

    def _check(self):
        if self._pid!=os.getpid() or self._fd is None:raise QualifierHeld('qualifier_owner_unavailable')
        info,current=os.fstat(self._fd),self.path.with_name(self.path.name+'.lock').lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid() or info.st_mode & 0o077
                or (info.st_dev,info.st_ino)!=self._identity or (current.st_dev,current.st_ino)!=self._identity):
            raise QualifierHeld('qualifier_lease_identity_changed')

    @contextmanager
    def _db(self):
        self._check();info=self.path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid() or info.st_mode & 0o077:
            raise QualifierHeld('private_qualifier_journal_required')
        db=sqlite3.connect(self.path,timeout=10);db.row_factory=sqlite3.Row;db.execute('PRAGMA synchronous=FULL')
        try:
            db.execute('BEGIN IMMEDIATE');yield db;self._check();self._commit(db)
        except BaseException:db.rollback();raise
        finally:db.close()

    def _commit(self,db):db.commit()

    def _command(self,args,*,timeout=None):
        self._check()
        command=[self.registry.policy.docker_executable,'--host',self.registry.policy.docker_endpoint,*args]
        try:
            result=self.runner(command,timeout=timeout or self.policy.command_timeout_seconds,max_bytes=1024*1024)
            if (result.returncode!=0 or any(not isinstance(value,(str,bytes)) or len(value.encode() if isinstance(value,str) else value)>1024*1024
                    for value in (result.stdout,result.stderr))):raise ValueError()
            return result.stdout.decode() if isinstance(result.stdout,bytes) else result.stdout
        except Exception:raise QualifierHeld('docker_qualification_outcome_unknown') from None

    def _engine(self):
        if self._command(['info','--format','{{.ID}}']).strip()!=self.policy.engine_id:raise QualifierHeld('physical_engine_changed')

    def _request(self,request):
        try:
            if type(request) is not dict or set(request)!=REQUEST_FIELDS:raise ValueError()
            request=_load(_json(request))
            for key in ('operationId','candidateId','supervisorPolicySha256','requestSha256'):
                if not re.fullmatch(r'[a-f0-9]{64}',request[key]):raise ValueError()
            _digest(request['manifestDigest']);_digest(request['configDigest']);_digest(request['localImageId'])
            _digest(request['previousImageId']);_integer(request['hostGeneration'])
            directory=Path(self.policy.supervisor_state_root)/'receipts'/request['operationId']/'qualification'
            if (type(request['schemaVersion']) is not int or request['schemaVersion']!=1 or request['nodeId']!=self.policy.node_id
                    or request['engineId']!=self.policy.engine_id or request['authorityId']!=self.policy.authority_id
                    or request['qualifierId']!=self.policy.qualifier_id or request['qualificationPolicySha256']!=self.policy_sha256
                    or request['platform']!='linux/amd64' or request['receiptDirectory']!=str(directory)
                    or request['requestSha256']!=_sha({key:value for key,value in request.items() if key!='requestSha256'})):
                raise ValueError()
            if any(path.is_symlink() for path in (directory,*directory.parents)):raise ValueError()
            info=directory.stat()
            if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode & 0o077:raise ValueError()
            with self.registry._db() as db:
                row=db.execute('SELECT value FROM registry_candidates WHERE id=?',(request['candidateId'],)).fetchone()
            if row is None:raise ValueError()
            candidate=_load(row[0])
            if any(request[key]!=candidate[key] for key in ('repository','manifestDigest','configDigest','platform')):raise ValueError()
            return request,candidate
        except Exception:raise QualifierHeld('exact_qualification_request_required') from None

    def _row(self,identifier):
        with self._db() as db:row=db.execute('SELECT * FROM qualifications WHERE id=?',(identifier,)).fetchone()
        return dict(row) if row else None

    def _phase(self,identifier,state,**values):
        if set(values)-{'container','defaults','evidence','stopped','receipt'}:raise QualifierError('invalid_journal_update')
        with self._db() as db:
            db.execute('UPDATE qualifications SET '+','.join(['state=?']+[key+'=?' for key in values])+' WHERE id=?',
                (state,*(_json(value) if key!='container' else value for key,value in values.items()),identifier))

    def _defaults(self,image):
        template='{"User":{{json .Config.User}},"WorkingDir":{{json .Config.WorkingDir}},"Entrypoint":{{json .Config.Entrypoint}},"Cmd":{{json .Config.Cmd}},"Volumes":{{json .Config.Volumes}}}'
        value=_load(self._command(['image','inspect','--format',template,image]))
        if startup_spec(value)!=self.policy.startup_spec_sha256:raise QualifierHeld('reviewed_startup_spec_changed')
        return dict(startupSpecSha256=self.policy.startup_spec_sha256,
            argsSha256=_sha({key:value[key] for key in ('Entrypoint','Cmd')}))

    def _inspect(self,row,request,defaults):
        if not row['container']:raise QualifierHeld('exact_container_identity_required')
        config='{'+','.join('"'+key+'":{{json .Config.'+key+'}}' for key in ('User','WorkingDir','Entrypoint','Cmd'))+'}'
        host_keys=('NetworkMode','ReadonlyRootfs','Privileged','Tmpfs','PortBindings','RestartPolicy','AutoRemove','CapDrop',
            'SecurityOpt','Binds','Devices','DeviceRequests','PidMode','IpcMode','NanoCpus','Memory','PidsLimit','LogConfig')
        host='{'+','.join('"'+key+'":{{json .HostConfig.'+key+'}}' for key in host_keys)+'}'
        template=('{'+'"id":{{json .Id}},"image":{{json .Image}},"running":{{json .State.Running}},"status":{{json .State.Status}},'
            '"pid":{{json .State.Pid}},"exitCode":{{json .State.ExitCode}},"binding":{{json (index .Config.Labels "ltds.qualifier.binding")}},'
            '"config":'+config+',"host":'+host+',"mounts":{{json .Mounts}}}')
        value=_load(self._command(['container','inspect','--format',template,row['container']]))
        c,h=value.get('config',{}),value.get('host',{})
        expected_tmp={path:'rw,nosuid,nodev,mode=1777' for path in self.policy.write_paths}
        mounts=value.get('mounts')
        mounts_ok=mounts in (None,[])
        if isinstance(mounts,list) and len(mounts)==len(self.policy.write_paths):
            mounts_ok=({item.get('Destination') for item in mounts if isinstance(item,dict)}==set(self.policy.write_paths)
                and all(isinstance(item,dict) and item.get('Type')=='tmpfs' and item.get('Source') in ('',None)
                    and item.get('RW') is True for item in mounts))
        if (set(value)!={'id','image','running','status','pid','exitCode','binding','config','host','mounts'}
                or value['id']!=row['container'] or value['image']!=request['localImageId'] or value['binding']!=request['requestSha256']
                or type(value['running']) is not bool or type(value['pid']) is not int or type(value['exitCode']) is not int
                or value['status'] not in ('created','running','exited','dead')
                or c.get('User')!=self.policy.run_user or c.get('WorkingDir')!=self.policy.working_directory
                or _sha({key:c.get(key) for key in ('Entrypoint','Cmd')})!=defaults['argsSha256']
                or h.get('NetworkMode')!='none' or h.get('ReadonlyRootfs') is not False or h.get('Privileged') is not False
                or h.get('Tmpfs')!=expected_tmp or h.get('PortBindings') not in (None,{}) or not mounts_ok
                or h.get('RestartPolicy',{}).get('Name') not in ('no','') or h.get('AutoRemove') is not False
                or h.get('CapDrop')!=['ALL'] or h.get('SecurityOpt') not in (['no-new-privileges'],['no-new-privileges:true'])
                or h.get('Binds') not in (None,[]) or h.get('Devices') not in (None,[]) or h.get('DeviceRequests') not in (None,[])
                or h.get('LogConfig',{}).get('Type')!='none'
                or h.get('PidMode') not in ('',None) or h.get('IpcMode') not in ('private','')
                or h.get('NanoCpus')!=0 or h.get('Memory')!=0 or h.get('PidsLimit') not in (None,0,-1)):
            raise QualifierHeld('owned_container_spec_changed')
        return dict(id=value['id'],image=value['image'],running=value['running'],status=value['status'],pid=value['pid'],exitCode=value['exitCode'],
                    binding=value['binding'],specSha256=_sha(dict(user=c['User'],workingDir=c['WorkingDir'],entrypoint=c['Entrypoint'],cmd=c['Cmd'],host=h)))

    def _probe(self,row,request):
        raw,prefix=parse_launcher_stdout(self._command(['exec',row['container'],self.policy.node_executable,'-e',PROBE],timeout=70))
        if type(raw) is not dict or set(raw)!={'schemaVersion','info','options'} or type(raw['schemaVersion']) is not int or raw['schemaVersion']!=1:
            raise QualifierHeld('invalid_startup_probe_wire')
        if type(raw['info']) is not str or len(raw['info'].encode())>65536 or type(raw['options']) is not str or len(raw['options'].encode())>262144:
            raise QualifierHeld('startup_probe_limit')
        info=_validate_info(_load(raw['info']));options=_validate_options(_load(raw['options']))
        if info['engine']!=self.policy.expected_engine or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}',info['engineVersion']):
            raise QualifierHeld('actual_engine_dialect_or_version_changed')
        return dict(schemaVersion=1,scope='startup-api-options-only-v1',requestSha256=request['requestSha256'],
            probeSha256=PROBE_SHA,engine=info['engine'],apiVersion=info['version'],engineVersion=info['engineVersion'],
            filesystemMode=self.policy.filesystem_mode,
            launcherStdoutMode=self.policy.launcher_stdout_mode,**prefix,
            configDigest=request['configDigest'],manifestDigest=request['manifestDigest'],localImageId=request['localImageId'],
            imageIdKind=_load(row['defaults'])['imageIdKind'],
            optionsSha256=_sha(sorted(options,key=lambda value:value['name'])),optionsCount=len(options),
            reconstructionQualified=False,gpuInitializationQualified=False,gpuKernelsQualified=False,
            noTasksSubmitted=True,noProductionMounts=True,noPublishedPorts=True)

    def _absent(self,container):
        self._engine()
        ids=self._command(['container','ls','--all','--no-trunc','--filter','id='+container,'--format','{{.ID}}']).split()
        if ids not in ([],[container]):raise QualifierHeld('container_absence_unproven')
        self._engine();return ids==[]

    def _finish(self,row,request):
        stopped=_load(row['stopped']) if row['stopped'] else None
        evidence=_load(row['evidence']) if row['evidence'] else None
        self._evidence(evidence,request,row['state']=='completed')
        if (row['state'] not in ('remove_intent','completed') or not re.fullmatch(r'[a-f0-9]{64}',row['container'] or '')
                or not stopped or stopped.get('id')!=row['container'] or stopped.get('image')!=request['localImageId']
                or stopped.get('binding')!=request['requestSha256'] or stopped.get('running') is not False or stopped.get('pid')!=0
                or type(stopped.get('pid')) is not int or stopped.get('status') not in ('exited','dead')
                or not re.fullmatch(r'[a-f0-9]{64}',stopped.get('specSha256',''))
                or not evidence or evidence.get('requestSha256')!=request['requestSha256']
                or evidence.get('scope')!='startup-api-options-only-v1' or not self._absent(row['container'])):
            raise QualifierHeld('verified_removed_owned_container_required')
        if row['state']=='completed' and (evidence['containerId']!=row['container']
                or evidence['containerSpecSha256']!=stopped['specSha256']):
            raise QualifierHeld('sealed_qualification_evidence_changed')
        evidence=dict(evidence,containerId=row['container'],containerSpecSha256=stopped['specSha256'],
                      defaultStartupSha256=self.policy.startup_spec_sha256,stopped=True,removed=True,tmpfsOutputsDisposed=True,
                      containerWritableLayerDisposed=True)
        common=('operationId','nodeId','engineId','authorityId','candidateId','manifestDigest','configDigest','localImageId','supervisorPolicySha256','hostGeneration')
        receipt=seal_receipt(dict(schemaVersion=1,requestSha256=request['requestSha256'],**{key:request[key] for key in common},
            qualifierId=self.policy.qualifier_id,qualificationPolicySha256=self.policy_sha256,passed=True,
            engineVersion=evidence['engineVersion'],evidenceSha256=_sha(evidence)),'qualificationSha256')
        if row['receipt'] and _load(row['receipt'])!=receipt:raise QualifierHeld('sealed_qualification_receipt_changed')
        if row['state']!='completed':self._phase(row['id'],'completed',evidence=evidence,receipt=receipt)
        return receipt

    def _evidence(self,evidence,request,completed):
        keys={'schemaVersion','scope','requestSha256','probeSha256','engine','apiVersion','engineVersion','optionsSha256',
            'optionsCount','reconstructionQualified','gpuInitializationQualified','gpuKernelsQualified','noTasksSubmitted',
            'noProductionMounts','noPublishedPorts','postProbeRunningConfirmed','configDigest','manifestDigest','localImageId','imageIdKind','filesystemMode',
            'launcherStdoutMode','launcherPrefixKind','launcherNodeVersion','launcherNpmVersion','launcherPrefixSha256'}
        if completed:keys|={'containerId','containerSpecSha256','defaultStartupSha256','stopped','removed','tmpfsOutputsDisposed','containerWritableLayerDisposed'}
        if (type(evidence) is not dict or set(evidence)!=keys or type(evidence['schemaVersion']) is not int or evidence['schemaVersion']!=1
                or evidence['scope']!='startup-api-options-only-v1' or evidence['requestSha256']!=request['requestSha256']
                or evidence['filesystemMode']!=self.policy.filesystem_mode
                or evidence['launcherStdoutMode']!=self.policy.launcher_stdout_mode
                or evidence['probeSha256']!=PROBE_SHA or evidence['engine']!=self.policy.expected_engine
                or any(evidence[key]!=request[key] for key in ('configDigest','manifestDigest','localImageId'))
                or evidence['imageIdKind'] not in ('config','manifest')
                or (evidence['imageIdKind']=='config' and evidence['localImageId']!=evidence['configDigest'])
                or (evidence['imageIdKind']=='manifest' and evidence['localImageId']!=evidence['manifestDigest'])
                or any(evidence[key] is not False for key in ('reconstructionQualified','gpuInitializationQualified','gpuKernelsQualified'))
                or any(evidence[key] is not True for key in ('noTasksSubmitted','noProductionMounts','noPublishedPorts','postProbeRunningConfirmed'))
                or not 1<=_integer(evidence['optionsCount'])<=2048
                or not re.fullmatch(r'[a-f0-9]{64}',evidence['optionsSha256'])
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}',evidence['engineVersion'])):
            raise QualifierHeld('startup_evidence_changed')
        if evidence['launcherPrefixKind']=='raw-json':
            if (evidence['launcherNodeVersion'] is not None or evidence['launcherNpmVersion'] is not None
                    or evidence['launcherPrefixSha256']!=hashlib.sha256(b'').hexdigest()):raise QualifierHeld('launcher_evidence_changed')
        elif evidence['launcherPrefixKind']=='nvm-use-banner':
            node,npm=evidence['launcherNodeVersion'],evidence['launcherNpmVersion']
            if (type(node) is not str or type(npm) is not str or not 1<=len(node)<=32 or not 1<=len(npm)<=32
                    or not _SEMVER.fullmatch(node) or not _SEMVER.fullmatch(npm)):
                raise QualifierHeld('launcher_evidence_changed')
            prefix='Now using node v'+node+' (npm v'+npm+')'
            prefixes=[(prefix+ending).encode() for ending in ('\n','\r\n')]
            if not any(len(value)<=96 and hashlib.sha256(value).hexdigest()==evidence['launcherPrefixSha256'] for value in prefixes):
                raise QualifierHeld('launcher_evidence_changed')
        else:raise QualifierHeld('launcher_evidence_changed')
        if completed and (evidence['defaultStartupSha256']!=self.policy.startup_spec_sha256
                or any(evidence[key] is not True for key in ('stopped','removed','tmpfsOutputsDisposed','containerWritableLayerDisposed'))):
            raise QualifierHeld('startup_evidence_changed')
        _validate_info(dict(version=evidence['apiVersion'],engine=evidence['engine'],engineVersion=evidence['engineVersion']))

    def qualify(self,request):
        with self._exclusive():
            request,candidate=self._request(request);identifier=request['requestSha256'];row=self._row(identifier)
            if row:
                if row['request']!=_json(request):raise QualifierHeld('qualification_request_rebound')
                if row['state']=='completed':return self._finish(row,request)
                raise QualifierHeld('prior_qualification_requires_inspection')
            staged=self.registry.inspect_staged(candidate)
            if staged['imageId']!=request['localImageId'] or staged['configDigest']!=request['configDigest']:
                raise QualifierHeld('staged_runtime_image_binding_changed')
            self._engine();defaults=dict(self._defaults(request['localImageId']),imageIdKind=staged['imageIdKind'])
            with self._db() as db:
                if db.execute("SELECT 1 FROM qualifications WHERE state!='completed'").fetchone():raise QualifierHeld('prior_qualification_requires_inspection')
                name='ltds-startup-'+identifier[:32]
                db.execute("INSERT INTO qualifications VALUES(?,?,'create_intent',?,NULL,?,NULL,NULL,NULL)",
                    (identifier,_json(request),name,_json(defaults)))
            args=['create','--pull','never','--name',name,'--label','ltds.qualifier.binding='+identifier,
                '--network','none','--cap-drop','ALL','--security-opt','no-new-privileges',
                '--user',self.policy.run_user,'--workdir',self.policy.working_directory,'--restart','no',
                '--log-driver','none','--env','HOME=/tmp']
            for path in self.policy.write_paths:args+=['--tmpfs',path+':rw,nosuid,nodev,mode=1777']
            args+=[request['localImageId']]
            container=self._command(args).strip()
            if not re.fullmatch(r'[a-f0-9]{64}',container):raise QualifierHeld('container_create_identity_unknown')
            self._phase(identifier,'created',container=container);row=self._row(identifier)
            created=self._inspect(row,request,defaults)
            if created['running'] or created['pid']!=0 or created['status']!='created':raise QualifierHeld('fresh_created_container_required')
            self._engine()
            self._phase(identifier,'start_intent');self._command(['start',container])
            row=self._row(identifier);observed=self._inspect(row,request,defaults)
            if not observed['running'] or observed['pid']<=0 or observed['status']!='running':raise QualifierHeld('candidate_startup_not_running')
            self._phase(identifier,'exec_intent');evidence=self._probe(row,request)
            self._phase(identifier,'probe_durable',evidence=evidence);self._engine()
            live=self._inspect(self._row(identifier),request,defaults)
            if not live['running'] or live['pid']<=0 or live['status']!='running':
                raise QualifierHeld('candidate_exited_after_probe')
            evidence=dict(evidence,postProbeRunningConfirmed=True)
            self._phase(identifier,'probe_live',evidence=evidence)
            self._phase(identifier,'stop_intent');self._command(['stop','--time',str(self.policy.stop_timeout_seconds),container],
                timeout=self.policy.stop_timeout_seconds+15)
            row=self._row(identifier);stopped=self._inspect(row,request,defaults)
            if stopped['running'] or stopped['pid']!=0 or stopped['status'] not in ('exited','dead'):raise QualifierHeld('candidate_terminal_state_unproven')
            self._phase(identifier,'stopped',stopped=stopped);self._engine()
            self._phase(identifier,'remove_intent');self._command(['rm',container])
            return self._finish(self._row(identifier),request)

    def lookup(self,request):
        """Read-only Docker proof; never create/start/exec/stop/remove on restart."""
        with self._exclusive():
            request,candidate=self._request(request);row=self._row(request['requestSha256'])
            if row is None:return None
            if row['request']!=_json(request):raise QualifierHeld('qualification_request_rebound')
            if row['state'] not in ('remove_intent','completed'):raise QualifierHeld('qualification_outcome_requires_inspection')
            return self._finish(row,request)
