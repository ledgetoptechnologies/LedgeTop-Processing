"""First worker installation only; never start/enable a unit or process a job.

New root-owned, service-readable version directories are separate from retained
preparations. Unknown partial effects stay held. Existing worker state, leases,
configuration, units and permissions are never adopted, repaired or overwritten.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tarfile
import time
import uuid

from ops.enrolled_coordinator_transport import BoundedSubprocess, TransportHeld, canonical, decode, digest, require

PROTOCOL = 'ltds-first-worker-enrollment-v1'
UNIT = 'ledgetop-processing-reference-worker.service'
MAX_ARCHIVE = 256 * 1024**2
MAX_EXPANDED = 512 * 1024**2
MAX_FILES = 8192
INSTALL_BASE = Path('/opt/ledgetop-processing-reference')
UNSET_STARTUP_NAMES = ('LD_PRELOAD','LD_LIBRARY_PATH','LD_AUDIT','PYTHONPATH','PYTHONHOME',
                       'PYTHONUSERBASE','PYTHONSTARTUP','PYTHONINSPECT','PYTHONPYCACHEPREFIX')


def closed(value, fields):
    require(type(value) is dict and set(value) == set(fields), 'closed_enrollment_manifest_required')


def absolute(value):
    require(type(value) is str and len(value) <= 4096 and value.startswith('/') and
            str(PurePosixPath(value)) == value and '..' not in PurePosixPath(value).parts and
            not any(c in value for c in ('\x00','\n','\r','%')), 'literal_enrollment_path_required')
    return Path(value)


def sha(value):
    require(type(value) is str and re.fullmatch('[a-f0-9]{64}',value), 'reviewed_enrollment_hash_required')


def integer(value, minimum=0):
    require(type(value) is int and minimum <= value <= 2**53-1, 'bounded_enrollment_integer_required')


def validate_manifest(value):
    closed(value, ('schemaVersion','protocol','operationId','host','service','installationRoot',
                   'source','dependencies','config','unitSha256','gate','engine'))
    require(type(value['schemaVersion']) is int and value['schemaVersion'] == 1 and
            value['protocol'] == PROTOCOL, 'enrollment_profile_required')
    require(type(value['operationId']) is str and str(uuid.UUID(value['operationId'])) == value['operationId'],
            'enrollment_operation_required')
    closed(value['host'], ('hostname','bootId','dockerEngineId'))
    require(all(type(item) is str and 1 <= len(item) <= 256 and '\n' not in item
                for item in value['host'].values()), 'original_worker_host_required')
    require(re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,252}',value['host']['hostname']) and
            re.fullmatch('[A-Za-z0-9][A-Za-z0-9:._-]{0,255}',value['host']['dockerEngineId']) and
            str(uuid.UUID(value['host']['bootId']))==value['host']['bootId'],'original_worker_host_required')
    service=value['service'];closed(service, ('name','uid','gid','home'))
    require(type(service['name']) is str and re.fullmatch('[a-z_][a-z0-9_-]{0,31}',service['name']),
            'fixed_service_account_required')
    integer(service['uid'],1);integer(service['gid'],1);home=absolute(service['home'])
    root=absolute(value['installationRoot'])
    require(root.parent == INSTALL_BASE and root.name == value['operationId'] and root != home and home not in root.parents,
            'separate_versioned_installation_required')
    for name in ('source','dependencies','config'):
        item=value[name]
        fields=('path','sha256','bytes','ownerUid')
        if name=='source': fields+=('commit','footprintSha256','requirementsSha256')
        if name=='dependencies':fields+=('pythonSha256','pythonVersion','versions')
        closed(item,fields);absolute(item['path']);sha(item['sha256']);integer(item['ownerUid'])
        require(item['ownerUid'] in (0,service['uid']),'existing_artifact_owner_required')
        integer(item['bytes'],1)
        require(item['bytes'] <= (65536 if name=='config' else MAX_ARCHIVE), 'bounded_enrollment_artifact_required')
    source=value['source']
    require(type(source['commit']) is str and re.fullmatch('[a-f0-9]{40}',source['commit']), 'source_commit_required')
    sha(source['footprintSha256']);sha(source['requirementsSha256']);sha(value['unitSha256'])
    deps=value['dependencies'];sha(deps['pythonSha256'])
    require(type(deps['pythonVersion']) is str and re.fullmatch(r'3\.(10|11|12|13|14)\.[0-9]{1,3}',deps['pythonVersion']),
            'reviewed_python_profile_required')
    closed(deps['versions'], ('cryptography','cffi','pycparser','rfc8785'))
    expected=dict(cryptography='50.0.2',cffi='2.1.1',pycparser='2.23' if deps['pythonVersion'].startswith('3.10.') else '3.12',rfc8785='0.1.4')
    require(deps['versions']==expected, 'reviewed_dependency_profile_changed')
    gate=value['gate'];closed(gate, ('path','sha256','nodeId','authorityId','generation','coordinatorOrigin'))
    sha(gate['sha256']);integer(gate['generation'],1)
    require(type(gate['nodeId']) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,63}',gate['nodeId']),
            'original_node_required')
    require(type(gate['authorityId']) is str and str(uuid.UUID(gate['authorityId']))==gate['authorityId'],
            'original_authority_required')
    from processing.node_gate import _binding
    require(_binding(gate['nodeId'],gate['coordinatorOrigin'],gate['authorityId'])['coordinatorOrigin']==gate['coordinatorOrigin'],
            'original_gate_origin_required')
    expected_gate=home/'.local/state/LedgeTop-Processing/node-gates'/hashlib.sha256(value['host']['dockerEngineId'].encode()).hexdigest()/'gate.json'
    require(absolute(gate['path']) == expected_gate, 'original_canonical_gate_required')
    engine=value['engine'];closed(engine, ('containerId','imageId','version','loopbackPort','catalogSha256'))
    sha(engine['catalogSha256'])
    require(type(engine['containerId']) is str and re.fullmatch('[a-f0-9]{64}',engine['containerId']) and
            type(engine['imageId']) is str and re.fullmatch('sha256:[a-f0-9]{64}',engine['imageId']), 'original_engine_pin_required')
    require(type(engine['version']) is str and 1<=len(engine['version'])<=128, 'engine_version_required')
    integer(engine['loopbackPort'],1024);require(engine['loopbackPort']<=65535,'engine_port_required')
    require(hashlib.sha256(render_unit(value).encode()).hexdigest() == value['unitSha256'], 'derived_worker_unit_changed')
    return decode(canonical(value))


def render_unit(manifest):
    root=absolute(manifest['installationRoot']);home=absolute(manifest['service']['home'])
    # Same termination/UMask policy as ops/systemd/user's reviewed template,
    # using exact versioned paths instead of a mutable current symlink.
    for path in (root,home):
        require(not any(c.isspace() or c in '\\"\'' for c in str(path)), 'literal_systemd_path_required')
    text=('[Unit]\nDescription=LedgeTop Processing reference worker\nStartLimitIntervalSec=10min\nStartLimitBurst=20\n\n'
          '[Service]\nType=simple\nWorkingDirectory='+str(root/'source')+'\nExecStart='+str(root/'venv/bin/python')+
          ' -B -m ops.reference_worker_daemon --config '+str(home/'.config/ledgetop-processing/reference-worker.json')+
          '\nEnvironment=PYTHONNOUSERSITE=1 HOME='+str(home)+' XDG_STATE_HOME='+str(home/'.local/state')+
          '\nUnsetEnvironment='+' '.join(UNSET_STARTUP_NAMES)+
          '\nRestart=on-failure\nRestartSec=15s\nTimeoutStartSec=30s\nTimeoutStopSec=10min\nKillMode=mixed\n'
          'SendSIGKILL=no\nUMask=0077\nNoNewPrivileges=yes\n\n[Install]\nWantedBy=default.target\n')
    return text


def identity(info):
    return [info.st_dev,info.st_ino,info.st_mode,info.st_uid,info.st_gid,info.st_nlink,info.st_size,info.st_mtime_ns,info.st_ctime_ns]


def original(path):
    require(path.is_absolute() and path.resolve()==path and not any(p.is_symlink() for p in (path,*path.parents)),
            'original_installation_paths_required')


def read_file(path, *, owner, maximum, mode=0o600):
    original(path);before=path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink==1 and before.st_uid==owner and
            stat.S_IMODE(before.st_mode)==mode and before.st_size<=maximum, 'private_enrollment_input_required')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
    try:
        require(identity(before)==identity(os.fstat(fd)), 'enrollment_input_changed')
        with os.fdopen(fd,'rb',closefd=False) as stream:raw=stream.read(maximum+1)
        require(len(raw)==before.st_size and identity(before)==identity(os.fstat(fd))==identity(path.lstat()),
                'enrollment_input_changed')
        return raw
    finally:os.close(fd)


def sync_directory(path):
    original(path);fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:os.fsync(fd)
    finally:os.close(fd)


def directory_identity(path):
    original(path);info=path.lstat();require(stat.S_ISDIR(info.st_mode),'original_worker_parent_required')
    return [info.st_dev,info.st_ino,info.st_mode,info.st_uid,info.st_gid]


def user_directory_script(home,path,uid):
    parts=path.relative_to(home).parts
    require(parts and len(parts)<=16,'bounded_worker_parent_required')
    # Execute as the service user, never root: no attacker-swapped directory
    # can receive a privileged chown, and existing modes remain unchanged.
    return (('import json,os,pathlib,stat\nhome=pathlib.Path('+repr(str(home))+');parts='+repr(tuple(parts))+';uid='+str(uid)+'\n')+'''
assert os.geteuid()==uid
assert home.resolve()==home
opened=[]
try:
 fd=os.open(home,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC);opened.append(fd)
 for name in parts:
  try:child=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd)
  except FileNotFoundError:
   os.mkdir(name,0o700,dir_fd=fd)
   child=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd)
   os.fsync(fd)
  opened.append(child);fd=child
  info=os.fstat(fd)
  assert info.st_uid==uid and not info.st_mode&0o022 and info.st_mode&0o300==0o300
 info=os.fstat(fd);os.fsync(fd)
 print(json.dumps([info.st_dev,info.st_ino,info.st_mode,info.st_uid,info.st_gid]))
finally:
 for fd in reversed(opened):os.close(fd)
''').encode()


def source_footprint(root):
    files={};entries=0;total=0
    for package in ('processing','ops'):
        pending=[root/package]
        while pending:
            path=pending.pop();original(path)
            with os.scandir(path) as iterator:
                for item in iterator:
                    entries+=1;require(entries<=4096 and not item.is_symlink(),'bounded_source_footprint_required')
                    if item.is_dir(follow_symlinks=False):pending.append(Path(item.path))
                    elif item.name.endswith('.py'):
                        raw=read_file(Path(item.path),owner=0,maximum=4*1024**2,mode=0o440)
                        files[Path(item.path).relative_to(root).as_posix()]=hashlib.sha256(raw).hexdigest();total+=len(raw)
                        require(len(files)<=512 and total<=32*1024**2,'bounded_source_footprint_required')
    require('ops/reference_worker_daemon.py' in files and 'processing/reference_driver.py' in files,'complete_worker_source_required')
    return hashlib.sha256(json.dumps(files,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()


class ActualBoundary:
    def __init__(self):self.runner=BoundedSubprocess()
    def run(self,args,**kwargs):return self.runner.run(args,**kwargs)
    def check_host(self,m):
        require(os.name=='posix' and os.geteuid()==0,'root_owned_installation_privilege_required')
        import pwd
        account=pwd.getpwnam(m['service']['name'])
        require((account.pw_uid,account.pw_gid,account.pw_dir)==(m['service']['uid'],m['service']['gid'],m['service']['home']),
                'existing_service_identity_changed')
        require(Path('/proc/sys/kernel/random/boot_id').read_text().strip()==m['host']['bootId'] and
                self.run(['/usr/bin/hostname'],output_limit=4096).decode().strip()==m['host']['hostname'] and
                self.run(['/usr/bin/docker','--host','unix:///run/docker.sock','info','--format','{{.ID}}'],output_limit=4096).decode().strip()==m['host']['dockerEngineId'],
                'original_installation_host_changed')
    def local_filesystem(self,path):
        value=self.run(['/usr/bin/stat','-f','-c','%T',str(path)],output_limit=4096).decode().strip()
        require(value in ('ext2/ext3','ext4','xfs','btrfs','tmpfs','overlayfs'), 'local_enrollment_filesystem_required')
    def engine(self,m,config):
        from processing.viewer_service import NativeEngine
        from processing.options import _sha,_validate_options
        from processing.reference_worker_catalog import reflects_secret
        fmt='{"id":{{json .Id}},"image":{{json .Image}},"running":{{json .State.Running}},"ports":{{json .NetworkSettings.Ports}}}'
        raw=self.run(['/usr/bin/docker','--host','unix:///run/docker.sock','inspect','--format',fmt,m['engine']['containerId']],output_limit=65536)
        value=decode(raw,65536);port=m['engine']['loopbackPort']
        require(value.get('id')==m['engine']['containerId'] and value.get('image')==m['engine']['imageId'] and
                value.get('running') is True and value.get('ports',{}).get('3000/tcp')==
                [dict(HostIp='127.0.0.1',HostPort=str(port))], 'original_private_engine_changed')
        client=NativeEngine('http://127.0.0.1:'+str(port),config['engineToken'])
        info=client.read('/info',limit=65536);options=_validate_options(client.read('/options',limit=262144))
        require(not reflects_secret([info,options],config['engineToken']),'native_credential_reflection_refused')
        require(type(info) is dict and info.get('engineVersion')==m['engine']['version'] and
                type(info.get('taskQueueCount')) is int and info['taskQueueCount']==0 and
                _sha(sorted(options,key=lambda item:item['name']))==m['engine']['catalogSha256'],
                'original_engine_metadata_changed')
        byname={item['name']:item for item in options}
        require(all(byname.get(name,{}).get('type')=='bool' for name in ('pc-ept','gltf','3d-tiles')),
                'required_viewer_outputs_unavailable')
    def as_user(self,m,args,**kwargs):
        uid=m['service']['uid']
        return self.run(['/usr/sbin/runuser','--user',m['service']['name'],'--','/usr/bin/env',
            'XDG_RUNTIME_DIR=/run/user/'+str(uid),'DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/'+str(uid)+'/bus',*args],**kwargs)
    def ensure_user_directory(self,m,path):
        raw=self.as_user(m,['/usr/bin/python3','-I','-B','-'],
            input_bytes=user_directory_script(absolute(m['service']['home']),path,m['service']['uid']),
            output_limit=4096,error_limit=4096,timeout=15)
        return decode(raw,4096)
    def assert_clean_manager_environment(self,m):
        # For the separately guarded activation path, not an authorization to
        # start anything. Never return manager environment contents or secrets.
        raw=self.as_user(m,['/usr/bin/systemctl','--user','show-environment'],output_limit=65536).decode()
        names=[]
        for line in raw.splitlines():
            require('=' in line,'worker_manager_environment_unavailable')
            name=line.split('=',1)[0]
            require(re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',name) and name not in names,
                    'worker_manager_environment_unavailable')
            names.append(name)
        require(not any(name.startswith(('LD_','PYTHON')) and name not in UNSET_STARTUP_NAMES and
                        name!='PYTHONNOUSERSITE' for name in names),
                'unexpected_worker_manager_startup_override')
        return dict(managerEnvironmentSha256=hashlib.sha256(raw.encode()).hexdigest(),unexpectedSourceOverridesAbsent=True)
    def no_unit(self,m,*,owned=False):
        raw=self.as_user(m,['/usr/bin/systemctl','--user','show','--no-pager',
            '--property=LoadState,ActiveState,MainPID,UnitFileState',UNIT],output_limit=4096).decode()
        fields=dict(line.split('=',1) for line in raw.splitlines() if '=' in line)
        require(fields==dict(LoadState='not-found',ActiveState='inactive',MainPID='0',UnitFileState='') or
                owned and fields==dict(LoadState='loaded',ActiveState='inactive',MainPID='0',UnitFileState='disabled'),
                'existing_worker_unit_requires_handover')
    def validate_prepared(self,m,stage):
        source=str(absolute(m['installationRoot'])/'source')
        script=('import sys,json,hashlib,importlib.metadata as md\nsys.dont_write_bytecode=True\nsys.path.insert(0,'+repr(source)+')\n'
            'from ops.reference_worker_daemon import load_config\nfrom processing.reference_worker_readiness import immutable_source_paths,source_digest\n'
            'immutable_source_paths('+repr(source)+')\nc=load_config('+repr(str(stage))+')\n'
            'print(json.dumps(dict(pythonVersion=sys.version.split()[0],versions={k:md.version(k) for k in '+repr(tuple(m['dependencies']['versions']))+'},'
            'sourceSha256=source_digest('+repr(source)+'),configSha256=hashlib.sha256(open('+repr(str(stage))+',"rb").read()).hexdigest())))\n').encode()
        raw=self.as_user(m,[str(absolute(m['installationRoot'])/'venv/bin/python'),'-I','-B','-'],
                         input_bytes=script,output_limit=16384,error_limit=4096,timeout=60)
        expected=dict(pythonVersion=m['dependencies']['pythonVersion'],versions=m['dependencies']['versions'],
                      sourceSha256=m['source']['footprintSha256'],configSha256=m['config']['sha256'])
        require(decode(raw)==expected,'prepared_worker_dependency_or_source_changed')


class Installer:
    def __init__(self,manifest,*,boundary=None):
        self.m=validate_manifest(manifest);self.boundary=boundary or ActualBoundary()
        self.root=absolute(self.m['installationRoot']);self.journal=self.root/'enrollment.json'
        home=absolute(self.m['service']['home'])
        self.config=home/'.config/ledgetop-processing/reference-worker.json'
        self.unit=home/'.config/systemd/user'/UNIT

    def _gate(self):
        g=self.m['gate'];raw=read_file(absolute(g['path']),owner=self.m['service']['uid'],maximum=16384)
        require(hashlib.sha256(raw).hexdigest()==g['sha256'],'original_node_gate_changed')
        value=decode(raw,16384)
        require(value.get('schema')==1 and value.get('engineId')==self.m['host']['dockerEngineId'] and
                all(value.get(k)==g[k] for k in ('nodeId','authorityId','generation','coordinatorOrigin')) and
                value.get('reservation','missing') is None,'original_idle_gate_required')

    def _check(self):
        self.boundary.check_host(self.m);self._gate()

    def _install_parent(self):
        pending=[];current=self.root.parent
        while not current.exists():pending.append(current);current=current.parent
        original(current);info=current.stat()
        require(info.st_uid==0 and not info.st_mode&0o022,'root_owned_installation_parent_required')
        self.boundary.local_filesystem(current)
        for path in reversed(pending):
            path.mkdir(mode=0o750);os.chown(path,0,self.m['service']['gid']);sync_directory(path.parent)

    def _state(self):
        raw=read_file(self.journal,owner=0,maximum=4*1024**2);value=decode(raw,4*1024**2)
        closed(value,('schemaVersion','manifestSha256','rootIdentity','phase','steps','receipt'))
        info=self.root.lstat()
        require(value['schemaVersion']==1 and value['manifestSha256']==digest(self.m) and
                value['rootIdentity']==[info.st_dev,info.st_ino] and info.st_uid==0 and info.st_gid==self.m['service']['gid'] and
                stat.S_IMODE(info.st_mode)==0o750 and value['phase'] in ('preparing','prepared','enrolling','enrolled') and
                type(value['steps']) is dict,'owned_enrollment_journal_required')
        return value

    def _save(self,state,*,initial=False):
        raw=canonical(state);require(len(raw)<=4*1024**2,'bounded_enrollment_journal_required')
        temp=self.root/('journal-'+uuid.uuid4().hex+'.tmp')
        fd=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
        try:
            with os.fdopen(fd,'wb',closefd=False) as stream:stream.write(raw);stream.flush();os.fsync(fd)
        finally:os.close(fd)
        if initial:
            os.link(temp,self.journal,follow_symlinks=False);os.unlink(temp)
        else:
            self._state();os.replace(temp,self.journal)
        sync_directory(self.root)

    @contextmanager
    def _owned(self,create=False):
        self._check()
        if create:self._install_parent()
        original(self.root.parent)
        parent=self.root.parent.stat()
        require(parent.st_uid==0 and not parent.st_mode&0o022,'root_owned_installation_parent_required')
        if not self.root.exists():
            require(create,'prepared_enrollment_required')
            self.root.mkdir(mode=0o750);os.chown(self.root,0,self.m['service']['gid']);sync_directory(self.root.parent)
            info=self.root.stat()
            self._save(dict(schemaVersion=1,manifestSha256=digest(self.m),rootIdentity=[info.st_dev,info.st_ino],
                phase='preparing',steps={},receipt=None),initial=True)
        state=self._state()
        import fcntl
        lock=self.root/'install.lock'
        fd=os.open(lock,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
        try:
            info=os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and info.st_uid==0 and info.st_nlink==1 and
                    stat.S_IMODE(info.st_mode)==0o600,'owned_enrollment_lock_required')
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            state=self._state();yield state
        finally:os.close(fd)

    def _artifact(self,name):
        a=self.m[name];self.boundary.local_filesystem(absolute(a['path']))
        raw=read_file(absolute(a['path']),owner=a['ownerUid'],maximum=a['bytes'])
        require(len(raw)==a['bytes'] and hashlib.sha256(raw).hexdigest()==a['sha256'],'reviewed_enrollment_artifact_changed')
        return raw

    @contextmanager
    def _archive(self,name):
        a=self.m[name];path=absolute(a['path']);self.boundary.local_filesystem(path);original(path);before=path.lstat()
        require(stat.S_ISREG(before.st_mode) and before.st_nlink==1 and before.st_uid==a['ownerUid'] and
                stat.S_IMODE(before.st_mode)==0o600 and before.st_size==a['bytes'],'private_enrollment_input_required')
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
        try:
            with os.fdopen(fd,'rb',closefd=False) as stream:
                require(identity(before)==identity(os.fstat(fd)),'enrollment_input_changed')
                h=hashlib.sha256();size=0
                while True:
                    block=stream.read(65536)
                    if not block:break
                    size+=len(block);require(size<=a['bytes'],'bounded_enrollment_artifact_required');h.update(block)
                require(size==a['bytes'] and h.hexdigest()==a['sha256'] and
                        identity(before)==identity(os.fstat(fd))==identity(path.lstat()),'reviewed_enrollment_artifact_changed')
                stream.seek(0);yield stream
                require(identity(before)==identity(os.fstat(fd))==identity(path.lstat()),'enrollment_input_changed')
        finally:os.close(fd)

    def _extract(self,name,destination):
        # Plain bounded TAR only, no compressed bombs, links, special nodes,
        # duplicate namespace or permission inheritance from old preparations.
        entries=set();expanded=0
        with self._archive(name) as stream,tarfile.open(fileobj=stream,mode='r|') as archive:
            destination.mkdir(mode=0o750);os.chown(destination,0,self.m['service']['gid'])
            for member in archive:
                path=PurePosixPath(member.name)
                require(member.name==path.as_posix() and not path.is_absolute() and '..' not in path.parts and
                        path.parts and len(member.name.encode())<=512 and member.name not in entries and len(entries)<MAX_FILES and (member.isdir() or member.isfile()) and
                        not member.issym() and not member.islnk(),'closed_worker_archive_required')
                entries.add(member.name);target=destination.joinpath(*path.parts)
                pending=[];parent=target.parent
                while parent!=destination and not parent.exists():pending.append(parent);parent=parent.parent
                for parent in reversed(pending):parent.mkdir(mode=0o750);os.chown(parent,0,self.m['service']['gid'])
                if member.isdir():
                    if not target.exists():target.mkdir(mode=0o750);os.chown(target,0,self.m['service']['gid'])
                    else:require(target.is_dir() and not target.is_symlink(),'worker_archive_namespace_conflict')
                else:
                    require(0<=member.size<=64*1024**2,'bounded_worker_archive_member_required')
                    expanded+=member.size;require(expanded<=MAX_EXPANDED,'bounded_worker_archive_required')
                    stream=archive.extractfile(member);require(stream is not None,'worker_archive_member_required')
                    fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
                    try:
                        with stream,os.fdopen(fd,'wb',closefd=False) as output:
                            remaining=member.size
                            while remaining:
                                block=stream.read(min(65536,remaining));require(bool(block),'complete_worker_archive_required')
                                output.write(block);remaining-=len(block)
                            output.flush();os.fchown(fd,0,self.m['service']['gid'])
                            os.fchmod(fd,0o550 if name=='dependencies' and member.mode&0o111 else 0o440);os.fsync(fd)
                    finally:os.close(fd)
        for directory,subdirs,_ in os.walk(destination,topdown=False):
            os.chmod(directory,0o550);sync_directory(Path(directory))
        sync_directory(destination.parent)

    def _tree(self,path,*,budget=None):
        entries={};count=0
        for directory,subdirs,files in os.walk(path):
            for name in sorted(subdirs+files):
                if budget is not None:budget.check()
                current=Path(directory)/name;original(current);info=current.lstat();count+=1
                require(count<=MAX_FILES and info.st_uid==0 and info.st_gid==self.m['service']['gid'] and
                        stat.S_IMODE(info.st_mode) in (0o440,0o550),'immutable_worker_tree_required')
                record=dict(identity=identity(info))
                if stat.S_ISREG(info.st_mode):
                    if budget is not None:budget.charge(info.st_size)
                    data=read_file(current,owner=0,maximum=64*1024**2,mode=stat.S_IMODE(info.st_mode))
                    record['sha256']=hashlib.sha256(data).hexdigest()
                else:require(stat.S_ISDIR(info.st_mode),'immutable_worker_tree_required')
                entries[current.relative_to(path).as_posix()]=record
        return dict(rootIdentity=identity(path.lstat()),entries=entries)

    def installed_tree_sha256(self,steps):
        """Opaque version1 install proof; readable paths only, no journal access.

        Root-directory mtime/ctime are excluded because publishing enrollment
        metadata legitimately changes them. Every source/dependency leaf and
        private config/unit descriptor remains covered by its existing proof.
        """
        original(self.root);info=self.root.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid==0 and info.st_gid==self.m['service']['gid']
                and stat.S_IMODE(info.st_mode)==0o750,'original_installation_root_required')
        parent=self.root.parent.lstat()
        require(parent.st_uid==0 and not parent.st_mode&0o022,'root_owned_installation_ancestor_required')
        fields=('source','dependencies','stageConfig','config','unit')
        require(all(name in steps for name in fields),'complete_install_only_tree_required')
        return digest(dict(schemaVersion=1,rootIdentity=[info.st_dev,info.st_ino,info.st_mode,info.st_uid,info.st_gid],
                           parentIdentity=[parent.st_dev,parent.st_ino,parent.st_mode,parent.st_uid,parent.st_gid],
                           steps={name:steps[name] for name in fields},tls=self._tls_proofs()))

    def _tls_proofs(self):
        config=decode(read_file(self.root/'worker-config.json',owner=self.m['service']['uid'],maximum=65536),65536)
        proofs={}
        for name in ('caFile','serverCertFile','serverKeyFile'):
            path=absolute(config.get(name));original(path);info=path.lstat();mode=stat.S_IMODE(info.st_mode)
            require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_uid in (0,self.m['service']['uid'])
                    and mode in (0o400,0o440,0o600,0o644)
                    and (name!='serverKeyFile' or info.st_uid==self.m['service']['uid'] and mode==0o600),
                    'installed_tls_file_changed')
            data=read_file(path,owner=info.st_uid,maximum=65536,mode=mode)
            proofs[name]=dict(identity=identity(info),parentIdentity=directory_identity(path.parent),
                              sha256=hashlib.sha256(data).hexdigest())
        return proofs

    def observe_installed_tree(self):
        """Fresh service-readable whole-tree proof with aggregate/time budgets."""
        class Budget:
            remaining=2*MAX_EXPANDED
            deadline=time.monotonic()+60
            def check(self):require(time.monotonic()<self.deadline,'installed_tree_duration_exceeded')
            def charge(self,amount):
                self.check();require(type(amount) is int and 0<=amount<=self.remaining,'installed_tree_bytes_exceeded')
                self.remaining-=amount
        budget=Budget();steps={name:self._tree(self.root/relative,budget=budget)
                              for name,relative in (('source','source'),('dependencies','venv'))}
        for name,path in (('stageConfig',self.root/'worker-config.json'),('config',self.config),('unit',self.unit)):
            budget.check();steps[name]=self._private_proof(path,self.m['service']['uid'])
        pyvenv=read_file(self.root/'venv/pyvenv.cfg',owner=0,maximum=65536,mode=0o440).decode()
        entries={}
        for line in pyvenv.splitlines():
            if not line.strip():continue
            require('=' in line,'closed_python_environment_required');key,value=line.split('=',1)
            require(key.strip() not in entries,'closed_python_environment_required');entries[key.strip()]=value.strip()
        require(entries.get('include-system-site-packages')=='false','isolated_python_environment_required')
        return self.installed_tree_sha256(steps)

    def _new_private(self,path,raw,owner):
        return self._new_at_parent(path,raw,owner,directory_identity(path.parent))

    def _private_proof(self,path,owner):
        parent=directory_identity(path.parent)
        raw=read_file(path,owner=owner,maximum=65536)
        require(parent==directory_identity(path.parent),'worker_parent_changed')
        return dict(identity=identity(path.lstat()),sha256=hashlib.sha256(raw).hexdigest(),parentIdentity=parent)

    def _new_at_parent(self,path,raw,owner,parent):
        directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
        fd=None
        try:
            info=os.fstat(directory)
            observed=[info.st_dev,info.st_ino,info.st_mode,info.st_uid,info.st_gid]
            require(observed==parent==directory_identity(path.parent),'worker_parent_changed')
            fd=os.open(path.name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=directory)
            with os.fdopen(fd,'wb',closefd=False) as stream:stream.write(raw);stream.flush();os.fchown(fd,owner,self.m['service']['gid']);os.fsync(fd)
            created=os.fstat(fd)
            require(stat.S_ISREG(created.st_mode) and created.st_nlink==1 and created.st_uid==owner and
                    created.st_gid==self.m['service']['gid'] and stat.S_IMODE(created.st_mode)==0o600,
                    'new_private_worker_file_required')
            require(identity(os.fstat(fd))==identity(os.stat(path.name,dir_fd=directory,follow_symlinks=False)),
                    'new_worker_file_changed')
            os.fsync(directory)
            require(observed==directory_identity(path.parent) and identity(os.fstat(fd))==identity(path.lstat()),
                    'worker_parent_changed')
            return dict(identity=identity(os.fstat(fd)),sha256=hashlib.sha256(raw).hexdigest(),parentIdentity=parent)
        finally:
            if fd is not None:os.close(fd)
            os.close(directory)

    def _verify(self,state):
        for name in ('source','dependencies'):
            path=self.root/('venv' if name=='dependencies' else name)
            require(state['steps'].get(name)==self._tree(path),'prepared_worker_tree_changed')
        stage=self.root/'worker-config.json'
        require(state['steps'].get('stageConfig')==self._private_proof(stage,self.m['service']['uid']),
                'prepared_worker_config_changed')
        for name,path in (('config',self.config),('unit',self.unit)):
            if name in state['steps']:
                require(state['steps'][name]==self._private_proof(path,self.m['service']['uid']),
                        'enrolled_worker_file_changed')
        self.boundary.validate_prepared(self.m,stage)
        receipt=state.get('receipt')
        if receipt is not None and ('installedTreeSchemaVersion' in receipt or 'installedTreeSha256' in receipt):
            require(type(receipt.get('installedTreeSchemaVersion')) is int and receipt['installedTreeSchemaVersion']==1
                    and receipt.get('installedTreeSha256')==self.installed_tree_sha256(state['steps']),
                    'installed_tree_receipt_changed')

    def _config_bytes(self,*,fresh):
        raw=self._artifact('config');config=decode(raw,65536)
        gate=self.m['gate'];expected=dict(nodeId=gate['nodeId'],authorityId=gate['authorityId'],
            engineId=self.m['host']['dockerEngineId'],gateGeneration=gate['generation'],
            gateCoordinatorOrigin=gate['coordinatorOrigin'],engineVersion=self.m['engine']['version'])
        require(all(config.get(key)==value for key,value in expected.items()),'original_worker_configuration_binding_changed')
        origin=config.get('engineOrigin')
        from urllib.parse import urlsplit
        parsed=urlsplit(origin)
        require(parsed.scheme=='http' and parsed.hostname=='127.0.0.1' and parsed.port==self.m['engine']['loopbackPort'] and
                parsed.path in ('','/') and parsed.username is None and parsed.password is None and not parsed.query and not parsed.fragment,
                'original_loopback_engine_origin_required')
        readiness=config.get('maintenanceReadiness')
        require(type(readiness) is dict and readiness.get('sourceRoot')==str(self.root/'source') and
                readiness.get('sourceSha256')==self.m['source']['footprintSha256'] and
                readiness.get('activation','missing') is None,'exact_readonly_worker_source_required')
        home=absolute(self.m['service']['home'])
        paths=dict(stateRoot=home/'.local/share/ledgetop-processing-reference/worker-state'/self.m['operationId'],
                   scratchRoot=home/'.local/share/ledgetop-processing-reference/worker-scratch'/self.m['operationId'])
        for name,path in paths.items():
            require(config.get(name)==str(path),'new_operation_worker_state_required')
            if fresh:require(not os.path.lexists(path),'existing_worker_custody_refused')
        return raw,config

    def prepare(self):
        with self._owned(create=True) as state:
            self.boundary.no_unit(self.m)
            require(not os.path.lexists(self.config) and not os.path.lexists(self.unit),'existing_worker_files_require_handover')
            raw,config=self._config_bytes(fresh=True);self.boundary.engine(self.m,config)
            for name in ('source','dependencies'):
                path=self.root/('venv' if name=='dependencies' else name)
                if name in state['steps']:require(state['steps'][name]==self._tree(path),'prepared_worker_tree_changed');continue
                require(not os.path.lexists(path),'partial_preparation_requires_inspection')
                self._extract(name,path);state['steps'][name]=self._tree(path);self._save(state)
            require(source_footprint(self.root/'source')==self.m['source']['footprintSha256'],'source_footprint_changed')
            requirements=read_file(self.root/'source/requirements.txt',owner=0,maximum=65536,mode=0o440)
            require(hashlib.sha256(requirements).hexdigest()==self.m['source']['requirementsSha256'],'requirements_pin_changed')
            python=read_file(self.root/'venv/bin/python',owner=0,maximum=64*1024**2,mode=0o550)
            require(hashlib.sha256(python).hexdigest()==self.m['dependencies']['pythonSha256'],'interpreter_pin_changed')
            pyvenv=read_file(self.root/'venv/pyvenv.cfg',owner=0,maximum=65536,mode=0o440).decode()
            entries={}
            for line in pyvenv.splitlines():
                if not line.strip():continue
                require('=' in line,'closed_python_environment_required');key,value=line.split('=',1)
                require(key.strip() not in entries,'closed_python_environment_required');entries[key.strip()]=value.strip()
            require(entries.get('include-system-site-packages')=='false','isolated_python_environment_required')
            if 'stageConfig' not in state['steps']:
                state['steps']['stageConfig']=self._new_private(self.root/'worker-config.json',raw,self.m['service']['uid']);self._save(state)
            self._verify(state);state['phase']='prepared';self._save(state)
            return self._receipt(state)

    def _parent(self,path,state):
        proof=self.boundary.ensure_user_directory(self.m,path)
        require(proof==directory_identity(path),'worker_parent_changed')
        info=path.stat()
        require(info.st_uid==self.m['service']['uid'] and not info.st_mode&0o022 and info.st_mode&0o300==0o300,
                'owned_worker_parent_required')
        return proof

    def enroll(self):
        with self._owned() as state:
            require(state['phase'] in ('prepared','enrolling','enrolled'),'prepared_worker_required')
            self._verify(state);self.boundary.no_unit(self.m,owned='unit' in state['steps'])
            raw,config=self._config_bytes(fresh=state['phase']!='enrolled');self.boundary.engine(self.m,config)
            state['phase']='enrolling';self._save(state)
            for name,path,raw in (('config',self.config,raw),('unit',self.unit,render_unit(self.m).encode())):
                if name in state['steps']:
                    require(state['steps'][name]==self._private_proof(path,self.m['service']['uid']),
                            'enrolled_worker_file_changed');continue
                require(not os.path.lexists(path),'foreign_worker_file_refused')
                parent=self._parent(path.parent,state)
                state['steps'][name]=self._new_at_parent(path,raw,self.m['service']['uid'],parent);self._save(state)
            self._verify(state);self._gate();state['phase']='enrolled'
            if state['receipt'] is None:state['receipt']=dict(schemaVersion=1,protocol=PROTOCOL,operationId=self.m['operationId'],
                manifestSha256=digest(self.m),sourceSha256=self.m['source']['footprintSha256'],
                configSha256=self.m['config']['sha256'],unitSha256=self.m['unitSha256'],unitInstalled=True,
                serviceStartIssued=False,serviceEnableIssued=False,engineMutationIssued=False,
                installedTreeSchemaVersion=1,installedTreeSha256=self.installed_tree_sha256(state['steps']))
            self._save(state);return state['receipt']

    def _receipt(self,state):
        return state['receipt'] or dict(schemaVersion=1,protocol=PROTOCOL,operationId=self.m['operationId'],
            manifestSha256=digest(self.m),phase=state['phase'],serviceStartIssued=False,serviceEnableIssued=False)

    def inspect(self):
        self._check()
        if not self.root.exists():return dict(schemaVersion=1,protocol=PROTOCOL,state='absent',operationId=self.m['operationId'])
        state=self._state();self._verify(state)
        if state['phase']=='enrolled':
            for name,path in (('config',self.config),('unit',self.unit)):
                require(state['steps'][name]==self._private_proof(path,self.m['service']['uid']),
                        'enrolled_worker_file_changed')
        return self._receipt(state)


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=('prepare','inspect','enroll'))
    parser.add_argument('--manifest',required=True);args=parser.parse_args()
    try:
        path=absolute(args.manifest);manifest=decode(read_file(path,owner=0,maximum=262144),262144)
        value=getattr(Installer(manifest),args.action)();print(json.dumps(value,sort_keys=True));return 0
    except Exception as error:
        reason=str(error) if isinstance(error,TransportHeld) and re.fullmatch('[a-z_]{1,80}',str(error)) else 'inspection_required'
        print(json.dumps(dict(error='first_worker_enrollment_held',reason=reason,serviceStartIssued=False,serviceEnableIssued=False)));return 1


if __name__=='__main__':raise SystemExit(main())
