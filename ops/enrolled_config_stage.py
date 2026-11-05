"""Exclusive service-UID configuration preparation, not runtime activation.

Only new operation-owned config files are written. Existing permissions, old
configs, processing state, leases, engines and NAS are never changed.
"""
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import uuid
from .coordinator_startup_policy import SOURCES


def require(condition,reason='config_stage_unavailable'):
    if not condition:raise RuntimeError(reason)


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def identity(info):return [info.st_dev,info.st_ino,info.st_mode,info.st_uid,info.st_gid]


def close_verified(fd):
    os.close(fd)
    try:os.fstat(fd)
    except OSError as error:require(error.errno==errno.EBADF)
    else:require(False)


def write_new(settings,payloads,*,root='/stage'):
    """Run inside the bounded UID10001 staging container. No import effects."""
    require(os.geteuid()==10001 and type(settings) is dict and
            set(settings)=={'schemaVersion','operationId','planSha256','imageId','rootIdentity','hashes','sourceHashes'})
    require(type(settings['schemaVersion']) is int and settings['schemaVersion']==1)
    require(str(uuid.UUID(settings['operationId']))==settings['operationId'])
    require(re.fullmatch('[a-f0-9]{64}',settings['planSha256']) and re.fullmatch('sha256:[a-f0-9]{64}',settings['imageId']))
    require(type(settings['sourceHashes']) is dict and set(settings['sourceHashes'])==set(SOURCES)
            and all(type(pin) is str and re.fullmatch('[a-f0-9]{64}',pin) for pin in settings['sourceHashes'].values()))
    require(type(payloads) is dict and set(payloads)==set(settings['hashes'])=={'bootstrap.json','final.json'})
    require(all(type(raw) is bytes and len(raw)<=262144 and hashlib.sha256(raw).hexdigest()==settings['hashes'][name]
                for name,raw in payloads.items()))
    path=Path(root);before=path.lstat()
    require(path.resolve()==path and stat.S_ISDIR(before.st_mode) and before.st_uid==10001
            and not before.st_mode&0o022 and identity(before)==settings['rootIdentity'])
    directory=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    # All mutable names are constants or a canonical operation UUID. No payload
    # names or configuration values are interpreted as paths or instructions.
    name='first-cutover-'+settings['operationId'];owned=None
    try:
        require(identity(os.fstat(directory))==settings['rootIdentity'])
        os.mkdir(name,mode=0o700,dir_fd=directory)  # Never exist_ok or overwrite.
        owned=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=directory)
        info=os.fstat(owned)
        require(info.st_uid==info.st_gid==10001 and stat.S_IMODE(info.st_mode)==0o700)
        for filename,raw in payloads.items():
            fd=os.open(filename,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=owned)
            try:
                opened=os.fstat(fd)
                require(stat.S_ISREG(opened.st_mode) and opened.st_uid==opened.st_gid==10001
                        and stat.S_IMODE(opened.st_mode)==0o600 and opened.st_nlink==1)
                offset=0
                while offset<len(raw):
                    count=os.write(fd,raw[offset:]);require(count>0);offset+=count
                os.fsync(fd);os.lseek(fd,0,os.SEEK_SET)
                copied=bytearray()
                while len(copied)<len(raw):
                    part=os.read(fd,min(65536,len(raw)-len(copied)));require(bool(part));copied.extend(part)
                current=os.fstat(fd);named=os.stat(filename,dir_fd=owned,follow_symlinks=False)
                require((current.st_dev,current.st_ino)==(opened.st_dev,opened.st_ino)==(named.st_dev,named.st_ino)
                        and current.st_size==len(raw) and bytes(copied)==raw)
            finally:close_verified(fd)
        os.fsync(owned);os.fsync(directory)
        require(identity(path.lstat())==identity(os.fstat(directory))==settings['rootIdentity'])
        result=dict(schemaVersion=1,status='staged',operationId=settings['operationId'],planSha256=settings['planSha256'],
                    imageId=settings['imageId'],rootIdentity=settings['rootIdentity'],hashes=settings['hashes'])
        closing=owned;owned=None;close_verified(closing)
        closing=directory;directory=None;close_verified(closing)
        return result
    finally:
        # Failure exits nonzero; the controller will retain partial new files
        # and refuse promotion. Nothing retries a possibly reused descriptor.
        if owned is not None:
            try:os.close(owned)
            except OSError:pass
        if directory is not None:
            try:os.close(directory)
            except OSError:pass


def serialized_writer(settings,payloads):
    module=Path(__file__).read_text().split('\nclass ConfigPreparation:',1)[0].replace('from .coordinator_startup_policy import SOURCES','')
    tail='''\ntry:
 result=write_new('''+repr(settings)+','+repr(payloads)+''')
 print(json.dumps(result,sort_keys=True,separators=(',',':')))
except BaseException:
 print('{"schemaVersion":1,"status":"failed"}')
 raise SystemExit(2)
'''
    bootstrap='''import sys,importlib.util,hashlib,os
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/app')
SOURCES='''+repr(SOURCES)+'''
spec=importlib.util.find_spec('processing.app')
if spec is None or spec.origin!='/app/processing/app.py':raise SystemExit(2)
for relative,pin in '''+repr(settings['sourceHashes'])+'''.items():
 path=Path('/app')/relative
 if path.resolve()!=path or os.access(path,os.W_OK) or path.stat().st_size>1048576:raise SystemExit(2)
 if hashlib.sha256(path.read_bytes()).hexdigest()!=pin:raise SystemExit(2)
'''
    source=(bootstrap+module+tail).encode()
    require(len(source)<=1048576,'bounded_config_writer_input_required')
    return source


class ConfigPreparation:
    def __init__(self,plan,bundle,operation_directory,*,docker=None):
        from .enrolled_coordinator_cutover import validate_plan,CutoverJournal
        from .enrolled_coordinator_deployment import validate_bundle
        from .enrolled_coordinator_transport import HostOperation,DockerCLI,TrackedExec
        self.plan=validate_plan(plan);self.bundle=validate_bundle(bundle,plan)
        self.root=Path(operation_directory);self.lock=HostOperation(self.root)
        self.receipt=CutoverJournal(self.root);self.receipt.path=self.root/'config-preparation.json'
        self.docker=docker or DockerCLI(binary=bundle['host']['dockerBinary'])
        self.tracked=TrackedExec(self.docker,plan['operationId'],self.root)
        from .enrolled_coordinator_boundary import BoundaryProbe
        self.boundary=BoundaryProbe(plan,bundle,self.docker,self.tracked)
        self.name='ltds-config-stage-'+plan['operationId'].replace('-','')[:16]

    def _original(self):
        from .enrolled_coordinator_transport import digest,immutable_container_recipe,container_environment,_absolute_path
        self.boundary.host()
        item=self.docker.inspect(self.plan['predecessor']['containerId'])
        require(item['Image']==self.plan['predecessor']['imageId']
                and digest(immutable_container_recipe(item))==self.plan['bindings']['predecessorRecipeSha256']
                and digest(container_environment(item))==self.plan['bindings']['predecessorEnvironmentSha256'],
                'original_config_mount_identity_changed')
        mounts=[m for m in item['Mounts'] if m['Destination']=='/run/processing-reference']
        require(len(mounts)==1 and mounts[0]['Type']=='bind' and mounts[0]['RW'] is False,'existing_readonly_config_mount_required')
        _absolute_path(mounts[0]['Source'])
        return item,mounts[0]['Source']

    def _inputs(self,bootstrap,final):
        from .enrolled_coordinator_transport import private_bytes,digest
        from .coordinator_startup_policy import build_startup_policy
        payloads={name+'.json':private_bytes(path,maximum=262144) for name,path in (('bootstrap',bootstrap),('final',final))}
        for role in ('bootstrap','final'):
            require(hashlib.sha256(payloads[role+'.json']).hexdigest()==self.bundle['artifacts'][role+'Config']['sha256'],
                    'reviewed_private_config_payload_changed')
        generated=build_startup_policy(self.bundle['reviewedSourceRoot'],expected_source_hashes=self.bundle['sourceHashes'],
            candidate_image_id=self.plan['candidate']['imageId'],candidate_source_commit=self.plan['candidate']['sourceCommit'],
            bootstrap_config_bytes=payloads['bootstrap.json'],final_config_bytes=payloads['final.json'],
            node_endpoints=self.bundle['nodeEndpoints'],authority_id=self.plan['authorityId'],
            reviewed_schema_bytes=private_bytes(self.bundle['artifacts']['predecessorSchema']['path'],maximum=65536))
        require(hashlib.sha256(generated).hexdigest()==self.plan['candidate']['declaredTransformSha256'],
                'reviewed_config_policy_changed')
        return payloads

    def _root_info(self):
        item,source=self._original()
        require(item['State']['Running'] is True,'running_original_for_initial_config_root_observation_required')
        script=b'''import os,stat,json
from pathlib import Path
p=Path('/run/processing-reference');s=p.lstat()
if p.resolve()!=p or not stat.S_ISDIR(s.st_mode) or s.st_uid!=10001 or s.st_mode&0o022:raise RuntimeError('private_config_root_required')
rows=[line.split() for line in Path('/proc/self/mountinfo').read_text().splitlines()]
matches=[row for row in rows if len(row)>6 and row[4]=='/run/processing-reference' and '-' in row]
if len(matches)!=1 or matches[0][matches[0].index('-')+1] not in ('ext4','xfs','btrfs','zfs','tmpfs','overlay'):raise RuntimeError('local_config_filesystem_required')
print(json.dumps(dict(rootIdentity=[s.st_dev,s.st_ino,s.st_mode,s.st_uid,s.st_gid])))
'''
        packet=self.tracked(item['Id'],script,request_id=str(uuid.uuid4()),timeout=10,output_limit=4096)
        require(packet['bodySucceeded'] is True and packet['process']['retired'] is True,'retired_original_config_observer_required')
        return source,packet['result']['rootIdentity']

    def _args(self,record):
        from .enrolled_coordinator_transport import OWNER_LABEL,ROLE_LABEL,INTENT_LABEL
        return ['container','create','--name',self.name,'--pull','never','--interactive','--init=false',
            '--read-only','--user','10001:10001','--network','none','--restart','no','--stop-signal','SIGTERM',
            '--stop-timeout','-1','--cap-drop','ALL','--security-opt','no-new-privileges','--pids-limit','64',
            '--memory','134217728','--memory-swap','134217728','--cpus','1','--log-driver','none','--no-healthcheck',
            '--label',OWNER_LABEL+'='+self.plan['operationId'],'--label',ROLE_LABEL+'=config-stage',
            '--label',INTENT_LABEL+'='+record['createIntent'],'--mount','type=bind,src='+record['configRoot']+',dst=/stage',
            '--entrypoint','python',self.plan['candidate']['imageId'],'-I','-B','-']

    def _owned(self,record):
        from .enrolled_coordinator_transport import OWNER_LABEL,ROLE_LABEL,INTENT_LABEL
        ids=self.docker.matching(['label='+OWNER_LABEL+'='+self.plan['operationId'],'label='+ROLE_LABEL+'=config-stage'])
        require(len(ids)==1,'exact_config_stage_helper_required')
        item=self.docker.inspect(ids[0]);c=item['Config'];h=item['HostConfig']
        require(item['Image']==self.plan['candidate']['imageId'] and item['Name']=='/'+self.name
                and c['Labels'][INTENT_LABEL]==record['createIntent'] and c.get('User')=='10001:10001'
                and c.get('Entrypoint')==['python'] and c.get('Cmd')==['-I','-B','-'] and c.get('OpenStdin') is True
                and c.get('StopSignal')=='SIGTERM' and c.get('StopTimeout')==-1 and c.get('Healthcheck',{}).get('Test')==['NONE']
                and h.get('ReadonlyRootfs') is True and h.get('NetworkMode')=='none' and not h.get('Init')
                and h.get('RestartPolicy')=={'Name':'no','MaximumRetryCount':0} and h.get('Privileged') is False
                and h.get('CapDrop')==['ALL'] and not h.get('CapAdd') and not h.get('Devices') and not h.get('DeviceRequests')
                and h.get('SecurityOpt') in (['no-new-privileges'],['no-new-privileges=true'])
                and h.get('PidsLimit')==64 and h.get('Memory')==h.get('MemorySwap')==134217728 and h.get('NanoCpus')==1000000000
                and not h.get('PidMode') and h.get('IpcMode','') in ('','private') and not h.get('UsernsMode')
                and not h.get('VolumesFrom') and not h.get('ExtraHosts') and not h.get('Dns') and not h.get('AutoRemove')
                and not h.get('PortBindings') and h.get('LogConfig',{}).get('Type')=='none',
                'closed_config_stage_recipe_changed')
        require(len(item['Mounts'])==1 and item['Mounts'][0]['Type']=='bind' and item['Mounts'][0]['Source']==record['configRoot']
                and item['Mounts'][0]['Destination']=='/stage' and item['Mounts'][0]['RW'] is True,
                'config_stage_must_not_mount_state_or_leases')
        if record['containerId'] is not None:require(item['Id']==record['containerId'],'config_helper_identity_changed')
        return item

    def create(self,bootstrap_input,final_input):
        from .enrolled_coordinator_transport import digest
        payloads=self._inputs(bootstrap_input,final_input)
        with self.lock.locked(self.plan),self.receipt.locked():
            require(not self.receipt.path.exists(),'existing_config_preparation_is_inspection_only')
            source,root_identity=self._root_info();directory='first-cutover-'+self.plan['operationId']
            for role in ('bootstrap','final'):
                require(self.bundle['artifacts'][role+'Config']['path']==source+'/'+directory+'/'+role+'.json',
                        'exclusive_operation_config_paths_required')
            settings=dict(schemaVersion=1,operationId=self.plan['operationId'],planSha256=digest(self.plan),
                          imageId=self.plan['candidate']['imageId'],rootIdentity=root_identity,
                          sourceHashes=self.bundle['sourceHashes'],
                          hashes={name:hashlib.sha256(raw).hexdigest() for name,raw in payloads.items()})
            serialized_writer(settings,payloads)  # Check stdin budget before create.
            record=dict(schemaVersion=1,operationId=self.plan['operationId'],planSha256=digest(self.plan),settings=settings,
                configRoot=source,phase='create-intent',containerId=None,
                writerSha256=hashlib.sha256(serialized_writer(settings,payloads)).hexdigest(),
                createIntent=str(uuid.uuid5(uuid.UUID(self.plan['operationId']),'config-create')),
                startIntent=str(uuid.uuid5(uuid.UUID(self.plan['operationId']),'config-start')))
            self.receipt.write(record)
            # One durable intent precedes this sole Docker create mutation.
            raw=self.docker.command(*self._args(record),mutation=True,timeout=30,output_limit=4096)
            identity=raw.decode('ascii').strip();require(re.fullmatch('[a-f0-9]{64}',identity),'created_config_helper_identity_required')
            record.update(containerId=identity,phase='created');self._owned(record);self.receipt.write(record)
            return dict(phase=record['phase'],containerId=identity)

    def _load(self):
        from .enrolled_coordinator_transport import digest
        record=self.receipt.read()
        require(set(record)=={'schemaVersion','operationId','planSha256','settings','configRoot','phase','containerId','createIntent','startIntent','writerSha256'}
                and record['schemaVersion']==1 and record['operationId']==self.plan['operationId']
                and record['planSha256']==digest(self.plan) and record['phase'] in ('create-intent','created','start-intent','verified','cleanup-intent','cleaned'),
                'exact_config_preparation_receipt_required')
        settings=record['settings']
        require(type(settings) is dict and set(settings)=={'schemaVersion','operationId','planSha256','imageId','rootIdentity','hashes','sourceHashes'}
                and settings['schemaVersion']==1 and settings['operationId']==self.plan['operationId']
                and settings['planSha256']==digest(self.plan) and settings['imageId']==self.plan['candidate']['imageId']
                and settings['sourceHashes']==self.bundle['sourceHashes']
                and type(settings['rootIdentity']) is list and len(settings['rootIdentity'])==5
                and all(type(n) is int for n in settings['rootIdentity']) and settings['rootIdentity'][3]==10001
                and settings['hashes']=={role+'.json':self.bundle['artifacts'][role+'Config']['sha256'] for role in ('bootstrap','final')}
                and record['createIntent']==str(uuid.uuid5(uuid.UUID(self.plan['operationId']),'config-create'))
                and record['startIntent']==str(uuid.uuid5(uuid.UUID(self.plan['operationId']),'config-start'))
                and type(record['writerSha256']) is str and re.fullmatch('[a-f0-9]{64}',record['writerSha256']),
                'config_preparation_binding_changed')
        return record

    def start(self,bootstrap_input,final_input):
        payloads=self._inputs(bootstrap_input,final_input)
        with self.lock.locked(self.plan),self.receipt.locked():
            record=self._load();require(record['phase']=='created','config_start_unknown_is_inspection_only')
            item=self._owned(record);require(item['State']['Running'] is False and item['State']['StartedAt'].startswith('0001-'),
                                             'config_helper_already_started')
            original,source=self._original()
            require(source==record['configRoot'],'original_config_root_changed')
            if original['State']['Running']:
                _,root_identity=self._root_info();require(root_identity==record['settings']['rootIdentity'],'original_config_root_changed')
            body=serialized_writer(record['settings'],payloads)
            require(hashlib.sha256(body).hexdigest()==record['writerSha256'],'reviewed_config_writer_changed')
            record['phase']='start-intent';self.receipt.write(record)
            self.docker.command('container','start','--attach','--interactive',item['Id'],input_bytes=body,
                                mutation=True,timeout=60,output_limit=8192)
            return self._inspect(record)

    def _inspect(self,record):
        if record['phase'] in ('cleanup-intent','cleaned'):
            from .enrolled_coordinator_transport import OWNER_LABEL,ROLE_LABEL
            ids=self.docker.matching(['label='+OWNER_LABEL+'='+self.plan['operationId'],'label='+ROLE_LABEL+'=config-stage'])
            require(not ids,'config_cleanup_outcome_pending')
            record['phase']='cleaned';self.receipt.write(record)
            return dict(phase='cleaned',containerId=record['containerId'])
        item=self._owned(record)
        if record['phase']=='create-intent':
            require(item['State']['Running'] is False and item['State']['StartedAt'].startswith('0001-'),
                    'unknown_config_helper_start')
            record.update(phase='created',containerId=item['Id']);self.receipt.write(record)
            return dict(phase='created',containerId=item['Id'])
        if record['phase']=='created':return dict(phase='created',containerId=item['Id'])
        require(item['State']['Running'] is False and item['State']['Pid']==0 and item['State']['ExitCode']==0
                and type(item['State'].get('StartedAt')) is str and not item['State']['StartedAt'].startswith('0001-')
                and type(item['State'].get('FinishedAt')) is str and not item['State']['FinishedAt'].startswith('0001-')
                and item['State'].get('OOMKilled') is False and not item['State'].get('Error'),
                'config_writer_not_verified_terminal')
        original,source=self._original();require(source==record['configRoot'],'original_config_root_changed')
        if original['State']['Running']:
            _,root_identity=self._root_info();require(root_identity==record['settings']['rootIdentity'],'original_config_root_changed')
        # A stopped original cannot exec a fresh stat helper. The exact writer
        # contract verified the retained root identity before/after its writes;
        # immutable mount inspection + non-following set readback work stopped.
        self.docker.read_private_config_set(self.plan['predecessor']['containerId'],
            '/run/processing-reference/first-cutover-'+self.plan['operationId'],record['settings']['hashes'])
        record['phase']='verified';self.receipt.write(record)
        return dict(phase='verified',containerId=item['Id'],hashes=record['settings']['hashes'])

    def inspect(self):
        with self.lock.locked(self.plan),self.receipt.locked():return self._inspect(self._load())

    def cleanup(self):
        """Remove only the verified stopped helper; staged files are retained."""
        with self.lock.locked(self.plan),self.receipt.locked():
            record=self._load();require(record['phase']=='verified','verified_config_before_helper_cleanup_required')
            item=self._owned(record)
            require(item['State']['Running'] is False and item['State']['Pid']==0 and item['State']['ExitCode']==0,
                    'known_terminal_config_helper_required')
            record['phase']='cleanup-intent';self.receipt.write(record)
            self.docker.command('container','rm',item['Id'],mutation=True,timeout=30,output_limit=4096)
            return self._inspect(record)
