"""Concrete custody/immutable TAR evidence, never restore or lease a copy.

The existing phase driver supplies original boundary and tracked-exec proofs.
This module creates no helper/container, changes no live permissions, and never
extracts a backup into a live database or treats retained data as authority.
"""
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

from . import coordinator_predecessor_census as census


class CustodyHeld(RuntimeError):
    pass


def require(value,reason):
    if not value:raise CustodyHeld(reason)


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def digest(value):return hashlib.sha256(canonical(value)).hexdigest()


class ArchiveReader:
    """Bound bytes and cooperative duration before retaining any content."""
    def __init__(self,stream,maximum,seconds):
        require(type(maximum) is int and maximum>0 and type(seconds) in (int,float) and 0<seconds<=900,
                'bounded_archive_read_required')
        self.stream,self.maximum,self.deadline=stream,maximum,time.monotonic()+seconds
        self.used=0;self.sha=hashlib.sha256()

    def read(self,size,*,eof=False):
        require(type(size) is int and 0<size<=65536,'bounded_archive_chunk_required')
        require(time.monotonic()<self.deadline,'archive_deadline_exceeded')
        result=bytearray()
        while len(result)<size:
            chunk=self.stream.read(min(size-len(result),self.maximum-self.used+1))
            require(type(chunk) is bytes and len(chunk)<=size-len(result),'archive_reader_contract_changed')
            if not chunk:
                require(eof or len(result)==size,'truncated_archive')
                break
            self.used+=len(chunk);require(self.used<=self.maximum,'archive_byte_budget_exceeded')
            self.sha.update(chunk);result.extend(chunk)
            require(time.monotonic()<self.deadline,'archive_deadline_exceeded')
        return bytes(result)


def _number(raw):
    if raw[0]&0x80:
        require(not raw[0]&0x40,'negative_archive_number')
        value=int.from_bytes(bytes([raw[0]&0x7f])+raw[1:],'big')
    else:
        text=raw.strip(b' \0')
        require(not text or re.fullmatch(b'[0-7]+',text),'invalid_archive_number')
        value=int(text,8) if text else 0
    require(value<2**53,'archive_number_budget_exceeded')
    return value


def _text(raw):
    raw=raw.split(b'\0',1)[0]
    try:return raw.decode('utf-8')
    except UnicodeError:raise CustodyHeld('archive_path_encoding_invalid') from None


def _path(value):
    require(type(value) is str and 0<len(value.encode('utf-8'))<=4096 and '\0' not in value and '\\' not in value,
            'bounded_relative_archive_path_required')
    if value.startswith('./'):value=value[2:]
    if value in ('','.'):return ''
    value=value.rstrip('/')
    require(not value.startswith('/') and all(part not in ('','.','..') for part in value.split('/')),
            'original_relative_archive_path_required')
    return value


def _pax(raw):
    values={};offset=0
    while offset<len(raw):
        space=raw.find(b' ',offset,offset+12)
        require(space>offset and raw[offset:space].isdigit(),'bounded_pax_record_required')
        length=int(raw[offset:space]);require(0<length<=65536 and offset+length<=len(raw),'bounded_pax_record_required')
        item=raw[space+1:offset+length]
        require(item.endswith(b'\n') and b'=' in item,'bounded_pax_record_required')
        name,value=item[:-1].split(b'=',1)
        require(name in (b'path',b'mtime',b'atime',b'ctime') and name not in values and len(value)<=4096 and b'\0' not in value,
                'unsupported_archive_extension')
        values[name]=value;offset+=length
    return values


def validate_tar(stream, *, baseline,key,max_bytes,max_seconds=300,uid=10001,gid=10001):
    """Stream every byte and compare original UID/mode/path/content to full census.

    This proves data retention, not RPC/process closure or original ownership.
    The caller must separately require tracked clean completion before sealing a
    receipt. TAR remains opaque evidence and is never automatically extracted.
    """
    require(type(baseline) is dict and baseline.get('profile')==census.PROFILE and
            baseline.get('retainedFilesValidated') is True and baseline.get('validationScope')=='full-retained-custody' and
            type(baseline.get('files')) is dict and type(baseline.get('directories')) is dict,
            'full_original_custody_required')
    require(type(key) is bytes and len(key)==32,'private_census_key_required')
    reader=ArchiveReader(stream,max_bytes,max_seconds);seen=set();files={};directories={}
    extensions={};long_name=None;count=0;total=0;root_seen=False
    while True:
        header=reader.read(512)
        if header==bytes(512):
            require(reader.read(512)==bytes(512),'two_archive_end_blocks_required')
            require(not extensions and long_name is None,'dangling_archive_extension')
            while True:
                padding=reader.read(65536,eof=True)
                if not padding:break
                require(not any(padding),'nonzero_archive_trailer')
            break
        count+=1;require(count<=100000,'archive_entry_budget_exceeded')
        expected=_number(header[148:156]);actual=sum(header[:148])+8*32+sum(header[156:])
        require(expected==actual,'archive_header_checksum_invalid')
        kind=header[156:157];size=_number(header[124:136])
        require(size<=max_bytes,'archive_member_budget_exceeded')
        if kind in (b'x',b'L'):
            require(size<=65536,'archive_extension_budget_exceeded')
            raw=reader.read(size) if size else b''
            if size%512:require(not any(reader.read(512-size%512)),'nonzero_archive_padding')
            if kind==b'x':require(not extensions,'duplicate_archive_extension');extensions=_pax(raw)
            else:
                require(long_name is None and len(raw)<=4097 and raw.endswith(b'\0'),'bounded_long_archive_name_required')
                long_name=_text(raw)
            continue
        require(kind in (b'0',b'\0',b'5'),'nonregular_archive_member')
        require(not _text(header[157:257]),'archive_links_refused')
        name=_text(header[:100]);prefix=_text(header[345:500])
        if prefix:name=prefix+'/'+name
        if long_name is not None:name=long_name
        if b'path' in extensions:
            require(long_name is None,'ambiguous_archive_path')
            name=extensions[b'path'].decode('utf-8')
        relative=_path(name);long_name=None;extensions={}
        require(relative not in seen,'duplicate_archive_path');seen.add(relative)
        mode=_number(header[100:108]);owner=_number(header[108:116]);group=_number(header[116:124])
        require(mode<=0o7777 and owner==uid and group==gid,'original_archive_owner_mode_required')
        path_id=hmac.new(key,b'path\0'+relative.encode(),hashlib.sha256).hexdigest()
        if kind==b'5':
            require(size==0,'archive_directory_payload_refused')
            if relative=='':
                require(not root_seen and mode==baseline['rootMode'],'original_archive_root_mode_required');root_seen=True
            else:
                require(baseline['directories'].get(path_id)==dict(mode=mode),'unknown_or_changed_archive_directory')
                directories[path_id]=dict(mode=mode)
            continue
        require(relative!='' and path_id in baseline['files'],'unknown_archive_file')
        expected=baseline['files'][path_id]
        require(type(expected) is dict and expected.get('bytes')==size and expected.get('mode')==mode,
                'original_archive_file_size_mode_changed')
        content=hmac.new(key,b'file\0',hashlib.sha256);remaining=size
        while remaining:
            chunk=reader.read(min(65536,remaining));content.update(chunk);remaining-=len(chunk)
        if size%512:require(not any(reader.read(512-size%512)),'nonzero_archive_padding')
        result=dict(bytes=size,content=content.hexdigest(),mode=mode)
        require(result==expected,'original_archive_file_content_changed')
        files[path_id]=result;total+=size
    require(set(files)==set(baseline['files']) and set(directories)==set(baseline['directories']),
            'incomplete_original_archive_namespace')
    return dict(schemaVersion=1,kind='full-original-tar-validation',archiveBytes=reader.used,
        archiveSha256=reader.sha.hexdigest(),fileCount=len(files),directoryCount=len(directories),contentBytes=total,
        manifestSha256=digest(dict(files=files,directories=directories,rootMode=baseline['rootMode'],uid=uid,gid=gid)),
        originalUid=uid,originalGid=gid,writerExclusionProven=False,originalAuthorityProven=False,
        restoreAuthorized=False)


class PrivateArtifacts:
    """Original controller-owned700 directory, immutable600 evidence only."""
    def __init__(self,root,operation_id,plan_sha256):
        self.root=Path(root).absolute();self.operation_id=operation_id;self.plan_sha256=plan_sha256
        require(str(uuid.UUID(operation_id))==operation_id and re.fullmatch('[a-f0-9]{64}',plan_sha256),
                'exact_operation_artifact_binding_required')
        require(self.root.resolve()==self.root and not any(p.is_symlink() for p in self.root.parents),
                'original_artifact_directory_required')
        info=self.root.lstat();self.identity=(info.st_dev,info.st_ino)
        require(stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o700,
                'private_artifact_directory_required')

    def check(self):
        info=self.root.lstat()
        require((info.st_dev,info.st_ino)==self.identity and info.st_uid==os.geteuid() and
                stat.S_ISDIR(info.st_mode) and stat.S_IMODE(info.st_mode)==0o700 and not self.root.is_symlink(),
                'artifact_directory_changed')

    def directory(self):
        self.check();fd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
        info=os.fstat(fd)
        if (info.st_dev,info.st_ino)!=self.identity:
            os.close(fd);raise CustodyHeld('artifact_directory_changed')
        return fd

    def open_new(self,name):
        directory=self.directory()
        try:return os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=directory)
        finally:os.close(directory)

    def name(self,kind,intent,dispatch,extension='json'):
        require(kind in ('baseline','backup','projection') and extension in ('json','tar') and
                type(intent) is dict and intent.get('operationId')==self.operation_id and
                intent.get('planSha256')==self.plan_sha256,'original_intent_artifact_required')
        identity=intent.get('intentId');require(type(identity) is str and str(uuid.UUID(identity))==identity,'canonical_intent_required')
        suffix='read'
        if dispatch is not None:
            require(type(dispatch) is dict and set(dispatch)=={'intentSha256','attempt','dispatchId'} and
                    dispatch['intentSha256']==digest(intent) and type(dispatch['attempt']) is int and 1<=dispatch['attempt']<=128,
                    'exact_dispatch_artifact_required')
            value=dispatch['dispatchId'];require(type(value) is str and str(uuid.UUID(value))==value,'canonical_dispatch_required')
            suffix=str(dispatch['attempt'])+'-'+value
        return 'custody-'+kind+'-'+identity+'-'+suffix+'.'+extension

    def read(self,name,maximum):
        self.check();require(type(name) is str and name.startswith('custody-') and '/' not in name and '\\' not in name and
                            name.endswith(('.json','.tar')),'owned_artifact_name_required')
        directory=self.directory()
        try:
            before=os.stat(name,dir_fd=directory,follow_symlinks=False)
            require(stat.S_ISREG(before.st_mode) and before.st_uid==os.geteuid() and before.st_nlink==1 and
                    stat.S_IMODE(before.st_mode)==0o600 and before.st_size<=maximum,'private_bounded_artifact_required')
            fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC,dir_fd=directory)
            try:
                require(census._same(os.fstat(fd))==census._same(before),'artifact_identity_changed')
                data=bytearray()
                while len(data)<before.st_size:
                    chunk=os.read(fd,min(65536,before.st_size-len(data)));require(chunk,'truncated_private_artifact');data.extend(chunk)
                require(census._same(os.fstat(fd))==census._same(os.stat(name,dir_fd=directory,follow_symlinks=False))==
                        census._same(before),'artifact_changed')
                self.check();return bytes(data)
            finally:os.close(fd)
        finally:os.close(directory)

    def barrier(self,name,raw):
        """Exact bytes are not a durability receipt; retry both original barriers."""
        directory=self.directory()
        try:
            before=os.stat(name,dir_fd=directory,follow_symlinks=False)
            require(stat.S_ISREG(before.st_mode) and before.st_uid==os.geteuid() and before.st_nlink==1 and
                    stat.S_IMODE(before.st_mode)==0o600 and before.st_size==len(raw),'private_barrier_artifact_required')
            fd=os.open(name,os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=directory)
            try:
                require(census._same(os.fstat(fd))==census._same(before),'barrier_artifact_identity_changed')
                cursor=0
                while cursor<len(raw):
                    chunk=os.pread(fd,min(65536,len(raw)-cursor),cursor)
                    require(chunk==raw[cursor:cursor+len(chunk)] and chunk,'barrier_artifact_bytes_changed');cursor+=len(chunk)
                os.fsync(fd);os.fsync(directory)
                require(census._same(os.fstat(fd))==census._same(os.stat(name,dir_fd=directory,follow_symlinks=False))==
                        census._same(before),'barrier_artifact_changed')
                self.check()
            finally:os.close(fd)
        finally:os.close(directory)

    def retain(self,kind,intent,dispatch,body):
        self.check();name=self.name(kind,intent,dispatch)
        document=dict(schemaVersion=1,kind=kind,operationId=self.operation_id,planSha256=self.plan_sha256,
                      intentSha256=digest(intent),dispatch=dispatch,body=body)
        raw=canonical(document);require(len(raw)<=16*1048576,'custody_manifest_budget_exceeded')
        try:fd=self.open_new(name)
        except FileExistsError:require(self.read(name,16*1048576)==raw,'immutable_custody_artifact_changed')
        else:
            try:
                cursor=0
                while cursor<len(raw):
                    count=os.write(fd,raw[cursor:cursor+65536]);require(count>0,'artifact_write_outcome_unknown');cursor+=count
            finally:os.close(fd)
        self.barrier(name,raw)
        require(self.read(name,16*1048576)==raw,'retained_custody_artifact_unverified')
        return dict(name=name,sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw),kind=kind)

    def load(self,reference,kind):
        require(type(reference) is dict and set(reference)=={'name','sha256','bytes','kind'} and reference['kind']==kind and
                type(reference['bytes']) is int and 0<reference['bytes']<=16*1048576,'exact_custody_reference_required')
        raw=self.read(reference['name'],reference['bytes'])
        require(len(raw)==reference['bytes'] and hashlib.sha256(raw).hexdigest()==reference['sha256'],'custody_reference_changed')
        value=census._json(raw.decode('utf-8'))
        require(value.get('operationId')==self.operation_id and value.get('planSha256')==self.plan_sha256 and
                value.get('kind')==kind,'custody_operation_changed')
        return value

    def existing(self,kind,intent,dispatch):
        name=self.name(kind,intent,dispatch);raw=self.read(name,16*1048576)
        value=census._json(raw.decode('utf-8'))
        require(type(value) is dict and set(value)=={'schemaVersion','kind','operationId','planSha256','intentSha256','dispatch','body'} and
                value['schemaVersion']==1 and value['kind']==kind and value['operationId']==self.operation_id and
                value['planSha256']==self.plan_sha256 and value['intentSha256']==digest(intent) and value['dispatch']==dispatch,
                'existing_custody_intent_binding_changed')
        return dict(name=name,sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw),kind=kind),value


MODULE_NAMES=('coordinator_predecessor_census','coordinator_candidate_projection','coordinator_startup_policy')


class CustodyOps:
    """One phase driver's concrete IO helper; never an independent controller.

    tracked_exec must return a verified completed/retired remote-child receipt.
    boundary_check must independently inspect the original owner and all writer
    containers; no boolean is treated as original lease authority.
    """
    def __init__(self,docker,*,holder_id,holder_settings,operation_directory,key,key_sha256,
                 module_sources,module_sha256,reviewed_schema_bytes,policy_bytes,policy_sha256,
                 tracked_exec,boundary_check,max_artifact_bytes,max_backup_bytes,
                 max_metadata_bytes=64*1048576,max_seconds=300,safety_floor_bytes=64*1048576):
        require(type(holder_id) is str and re.fullmatch('[a-f0-9]{64}',holder_id),'exact_holder_id_required')
        require(type(key) is bytes and len(key)==32 and hashlib.sha256(key).hexdigest()==key_sha256,'pinned_census_key_required')
        require(set(module_sources)==set(MODULE_NAMES)==set(module_sha256) and
                all(type(module_sources[name]) is bytes and len(module_sources[name])<=1048576 and
                    hashlib.sha256(module_sources[name]).hexdigest()==module_sha256[name] for name in MODULE_NAMES),
                'reviewed_custody_code_required')
        require(type(reviewed_schema_bytes) is bytes and len(reviewed_schema_bytes)<=65536 and
                hashlib.sha256(canonical(census._json(reviewed_schema_bytes.decode()))).hexdigest()==census.SCHEMA_SHA256,
                'pinned_predecessor_schema_required')
        require(type(policy_bytes) is bytes and len(policy_bytes)<=1048576 and hashlib.sha256(policy_bytes).hexdigest()==policy_sha256,
                'pinned_startup_policy_required')
        require(callable(tracked_exec) and callable(boundary_check),'tracked_original_io_boundary_required')
        for value in (max_artifact_bytes,max_backup_bytes,max_metadata_bytes,safety_floor_bytes):
            require(type(value) is int and value>0,'positive_custody_budget_required')
        require(type(max_seconds) in (int,float) and 0<max_seconds<=600,'bounded_custody_duration_required')
        self.docker,self.holder_id,self.settings=docker,holder_id,dict(holder_settings)
        self.artifacts=PrivateArtifacts(operation_directory,holder_settings['operationId'],holder_settings['planSha256'])
        self.key,self.modules,self.schema,self.policy=key,dict(module_sources),reviewed_schema_bytes,policy_bytes
        self.policy_sha,self.tracked_exec,self.boundary_check=policy_sha256,tracked_exec,boundary_check
        self.max_artifact,self.max_backup,self.max_metadata=max_artifact_bytes,max_backup_bytes,max_metadata_bytes
        self.seconds,self.floor=max_seconds,safety_floor_bytes

    def _boundary(self,role,context,owner_id):
        require(type(owner_id) is str and re.fullmatch('[a-f0-9]{64}',owner_id),'exact_boundary_container_required')
        value=self.boundary_check(role,context)
        require(type(value) is dict and set(value)=={'schemaVersion','role','container','holder','physical','custody','writers','gate'} and
                value['schemaVersion']==1 and value['role']==role,'closed_original_boundary_required')
        container=value['container'];physical=value['physical'];operation=physical['operationBinding']
        require(container['id']==owner_id and re.fullmatch(r'sha256:[a-f0-9]{64}',container['imageId']) and
                type(container['pid']) is int and container['pid']>0 and type(container['startedAt']) is str and
                operation['authorityId']==self.settings['authorityId'] and operation['root']==self.settings['stateRoot'] and
                operation['database']==self.settings['stateRoot']+'/jobs.sqlite3' and
                operation['authorityLease']['directory']==self.settings['leaseDirectory'],'original_physical_authority_required')
        require(type(value['writers']) is list and value['writers'] and
                all(row['running'] is False and row['pid']==0 and type(row['finishedAt']) is str for row in value['writers']),
                'all_other_writers_must_be_stopped')
        if role=='holder':
            require(value['gate'] is None,'holder_is_not_a_gate_authority')
            held=value['holder']
            require(type(held) is dict and held.get('settings')==self.settings and held.get('pid')==1 and
                    type(held.get('startTime')) is str and held['startTime'].isdigit() and
                    held.get('mainIdentity')==physical['state']['main'] and
                    held.get('leaseIdentity')==operation['authorityLease']['fileIdentity'] and
                    type(held.get('mainDescriptor')) is int and held['mainDescriptor']>=3 and
                    type(held.get('leaseDescriptor')) is int and held['leaseDescriptor']>=3,
                    'actual_original_holder_descriptors_required')
        else:
            gate=value['gate']
            require(type(gate) is dict and set(gate)=={'snapshot','ownerSha256'} and
                    type(gate['ownerSha256']) is str and re.fullmatch('[a-f0-9]{64}',gate['ownerSha256']),
                    'independent_frozen_gate_owner_required')
            snapshot=gate['snapshot']
            require(type(snapshot) is dict and set(snapshot)=={'authorityId','startupId','revision','planId','phase','operations','reservedControlBytes'} and
                    snapshot['authorityId']==self.settings['authorityId'] and snapshot['phase']=='frozen' and
                    type(snapshot['revision']) is int and snapshot['revision']==3 and
                    type(snapshot['operations']) is int and snapshot['operations']==0 and
                    type(snapshot['reservedControlBytes']) is int and 2048<=snapshot['reservedControlBytes']<=4096 and
                    type(snapshot['planId']) is str and str(uuid.UUID(snapshot['planId']))==snapshot['planId'] and
                    type(snapshot['startupId']) is str and str(uuid.UUID(snapshot['startupId']))==snapshot['startupId'],
                    'authenticated_current_frozen_gate_required')
            observed=value['custody']
            require(value['holder'] is None and type(observed) is dict and observed.get('status')=='verified' and
                    observed.get('protocol')=='ltds-runtime-source-custody-v1' and
                    observed.get('authorityId')==snapshot['authorityId'] and observed.get('startupId')==snapshot['startupId'] and
                    all(observed.get(name) is True for name in ('sourceReadersPresent','leaseVerified','sessionVerified','journalMatches')) and
                    observed.get('bindingSha256')==physical.get('sourceBindingSha256'),
                    'actual_frozen_final_source_owner_required')
        return value

    def _source(self,request):
        # All three reviewed modules execute in memory; no package/tool files
        # are written into the holder or original state/NAS directories.
        header='import sys,types,json,base64\np=types.ModuleType("ops");p.__path__=[];sys.modules["ops"]=p\n'
        for name in MODULE_NAMES:
            source=self.modules[name].decode('utf-8')
            header+='m=types.ModuleType('+repr('ops.'+name)+');m.__package__="ops";m.__file__="<custody-capsule>";sys.modules[m.__name__]=m;setattr(p,'+repr(name)+',m);exec(compile('+repr(source)+',m.__file__,"exec"),m.__dict__)\n'
        packet=canonical(request)
        tail='r=json.loads('+repr(packet)+')\nc=p.coordinator_predecessor_census\n'
        if request['kind']=='census':
            tail+='result=c.census(r["root"],key=bytes.fromhex(r["keyHex"]),authority_id=r["authorityId"],node_ids=("cluster","main"),uid=10001,max_artifact_bytes=r["maxArtifactBytes"],max_metadata_bytes=r["maxMetadataBytes"],logical_root=r["root"],reviewed_schema_bytes=base64.b64decode(r["schema"]),max_duration_seconds=r["seconds"],expected_counts={"jobs":22,"viewer_tasks":7,"viewer_reservations":0,"viewer_option_evidence":7,"viewer_files":155,"reference_jobs":0,"owned_maintenance_schedules":16})\n'
        else:
            tail+='result=p.coordinator_candidate_projection.verify_projection(r["root"],baseline=r["baseline"],policy_bytes=base64.b64decode(r["policy"]),expected_policy_sha256=r["policySha256"],expected_baseline_sha256=r["baselineSha256"],key=bytes.fromhex(r["keyHex"]),phase=r["phase"],gate_binding=r["binding"],uid=10001,max_artifact_bytes=r["maxArtifactBytes"],max_metadata_bytes=r["maxMetadataBytes"],max_duration_seconds=r["seconds"],reviewed_schema_bytes=base64.b64decode(r["schema"]))\n'
        value=(header+tail+'print(json.dumps(result,sort_keys=True,separators=(",",":")))\n').encode()
        require(len(value)<=1048576,'serialized_custody_request_budget_exceeded');return value

    def _exec(self,owner_id,source,intent,dispatch,purpose,*,inspection=False):
        suffix=dispatch['dispatchId'] if dispatch is not None else 'read'
        request_id=str(uuid.uuid5(uuid.UUID(intent['intentId']),purpose+':'+suffix))
        if inspection:
            inspect=getattr(self.tracked_exec,'inspect',None);require(callable(inspect),'tracked_readonly_retirement_inspection_required')
            packet=inspect(owner_id,request_id,source,maximum=16*1048576)
        else:packet=self.tracked_exec(owner_id,source,request_id=request_id,timeout=self.seconds+30,output_limit=16*1048576)
        require(type(packet) is dict and set(packet)=={'schemaVersion','protocol','operationId','requestId','containerId','sourceSha256',
                    'process','bodySucceeded','result'} and type(packet.get('schemaVersion')) is int and packet['schemaVersion']==1 and packet.get('protocol')=='ltds-cutover-exec-v1' and
                packet.get('operationId')==self.settings['operationId'] and packet.get('requestId')==request_id and
                packet.get('containerId')==owner_id and packet.get('sourceSha256')==hashlib.sha256(source).hexdigest() and
                packet.get('bodySucceeded') is True and type(packet.get('process')) is dict and set(packet['process'])=={'pid','startTicks','retired'} and
                type(packet['process'].get('pid')) is int and packet['process']['pid']>0 and
                type(packet['process'].get('startTicks')) is str and packet['process']['startTicks'].isdigit() and
                packet['process'].get('retired') is True and type(packet.get('result')) is dict,
                'actual_remote_body_completion_and_retirement_required')
        return packet

    def _census(self,intent,dispatch,purpose,*,inspection=False):
        import base64
        source=self._source(dict(kind='census',root=self.settings['stateRoot'],keyHex=self.key.hex(),
            schema=base64.b64encode(self.schema).decode(),authorityId=self.settings['authorityId'],
            maxArtifactBytes=self.max_artifact,maxMetadataBytes=self.max_metadata,seconds=self.seconds))
        packet=self._exec(self.holder_id,source,intent,dispatch,purpose,inspection=inspection)
        result=packet['result']
        require(result.get('profile')==census.PROFILE and result.get('retainedFilesValidated') is True and
                result.get('validationScope')=='full-retained-custody' and
                result.get('authorityId')==self.settings['authorityId'],'actual_full_original_census_required')
        return packet

    @staticmethod
    def _receipt(packet):
        return dict({key:value for key,value in packet.items() if key!='result'},resultSha256=digest(packet['result']))

    def capture(self,intent,*,dispatch=None,context=None):
        before=self._boundary('holder',context,self.holder_id)
        packet=self._census(intent,dispatch,'capture-full-custody')
        after=self._boundary('holder',context,self.holder_id)
        require(digest(before)==digest(after),'original_holder_boundary_changed_during_capture')
        return self.artifacts.retain('baseline',intent,dispatch,
            dict(baseline=packet['result'],tracked=self._receipt(packet),boundaryBefore=before,boundaryAfter=after))

    def backup(self,baseline_reference,intent,*,dispatch,context=None):
        require(dispatch is not None,'backup_dispatch_required')
        original=self.artifacts.load(baseline_reference,'baseline');baseline=original['body']['baseline']
        before=self._boundary('holder',context,self.holder_id)
        require(digest(before)==digest(original['body']['boundaryAfter']),'original_backup_owner_changed')
        pre=self._census(intent,dispatch,'backup-pre-census');require(digest(pre['result'])==digest(baseline),'original_changed_before_copy')
        name=self.artifacts.name('backup',intent,dispatch,'tar');path=self.artifacts.root/name
        # Reserve against every retained partial/complete artifact. A rearm never
        # silently deletes an old partial or spends unbounded additional disk.
        used=0
        for entry in self.artifacts.root.iterdir():
            if entry.name.startswith('custody-'):
                info=entry.lstat();require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_uid==os.geteuid(),
                                          'unknown_custody_artifact')
                used+=info.st_size
        required=sum(item['bytes'] for item in baseline['files'].values())
        archive_limit=required+max(1048576,131072*(len(baseline['files'])+len(baseline['directories'])+1))
        require(used+archive_limit<=self.max_backup,'retained_backup_quota_reached')
        space=os.statvfs(self.artifacts.root);require(space.f_bavail*space.f_frsize>=archive_limit+self.floor,'backup_disk_floor_reached')
        self.artifacts.check()
        fd=self.artifacts.open_new(name)
        try:
            def sink(chunk):
                require(type(chunk) is bytes and 0<len(chunk)<=65536,'bounded_archive_sink_required')
                cursor=0
                while cursor<len(chunk):
                    count=os.write(fd,chunk[cursor:]);require(count>0,'backup_write_outcome_unknown');cursor+=count
            closure=self.docker.archive_to(self.holder_id,self.settings['stateRoot'],sink,max_bytes=archive_limit,timeout=self.seconds)
            require(type(closure) is dict and type(closure.get('clientExitCode')) is int and closure['clientExitCode']==0 and closure.get('streamClosed') is True,
                    'archive_rpc_closure_unknown')
            os.fsync(fd)
        finally:os.close(fd)
        directory=self.artifacts.directory()
        try:os.fsync(directory)
        finally:os.close(directory)
        self.artifacts.check();before_file=path.lstat()
        require(stat.S_ISREG(before_file.st_mode) and stat.S_IMODE(before_file.st_mode)==0o600 and
                before_file.st_nlink==1 and before_file.st_uid==os.geteuid(),'private_backup_tar_required')
        reader=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
        try:
            with os.fdopen(reader,'rb',closefd=False) as stream:
                proof=validate_tar(stream,baseline=baseline,key=self.key,max_bytes=archive_limit,max_seconds=self.seconds)
            require(census._same(os.fstat(reader))==census._same(path.lstat())==census._same(before_file),
                    'backup_tar_changed_during_verification')
        finally:os.close(reader)
        require(closure.get('bytes')==proof['archiveBytes'] and closure.get('sha256')==proof['archiveSha256'],
                'archive_stream_file_identity_changed')
        post=self._census(intent,dispatch,'backup-post-census');after=self._boundary('holder',context,self.holder_id)
        require(digest(post['result'])==digest(baseline) and digest(before)==digest(after),'original_changed_during_backup')
        return self.artifacts.retain('backup',intent,dispatch,dict(baselineReference=baseline_reference,
            tarName=name,tarValidation=proof,streamClosure=closure,preTracked=self._receipt(pre),postTracked=self._receipt(post),
            boundaryBefore=before,boundaryAfter=after))

    def projection(self,baseline_reference,intent,*,phase,binding,dispatch=None,context=None,final_id=None):
        import base64
        original=self.artifacts.load(baseline_reference,'baseline');baseline=original['body']['baseline']
        role='final' if phase=='final-frozen' else 'holder';owner_id=final_id if role=='final' else self.holder_id
        before=self._boundary(role,context,owner_id)
        require(before['physical']['operationBinding']==binding['operationBinding'],
                'independent_projection_physical_binding_changed')
        require(binding['operationBinding']==original['body']['boundaryAfter']['physical']['operationBinding'],
                'original_projection_authority_changed')
        if role=='final':
            require(before['custody']==binding['sourceCustody'] and
                    before['gate']['ownerSha256']==binding['ownerSha256'] and
                    before['gate']['snapshot']['planId']==binding['planId'] and
                    before['gate']['snapshot']['startupId']==binding['currentStartupId'],'actual_final_custody_or_gate_changed')
        request=dict(kind='projection',root=self.settings['stateRoot'],keyHex=self.key.hex(),
            schema=base64.b64encode(self.schema).decode(),policy=base64.b64encode(self.policy).decode(),
            policySha256=self.policy_sha,baseline=baseline,baselineSha256=digest(baseline),phase=phase,binding=binding,
            maxArtifactBytes=self.max_artifact,maxMetadataBytes=self.max_metadata,seconds=self.seconds)
        packet=self._exec(owner_id,self._source(request),intent,dispatch,'projection:'+phase)
        result=packet['result'];require(result.get('phase')==phase and result.get('retainedFilesValidated') is True and
            result.get('policySha256')==self.policy_sha and result.get('baselineSha256')==digest(baseline) and
            result.get('gateBindingSha256')==digest(binding),'actual_projection_proof_changed')
        after=self._boundary(role,context,owner_id);require(digest(before)==digest(after),'projection_owner_changed_during_io')
        return self.artifacts.retain('projection',intent,dispatch,dict(baselineReference=baseline_reference,
            projection=result,tracked=self._receipt(packet),boundaryBefore=before,boundaryAfter=after))


    def inspect_backup(self,baseline_reference,intent,*,dispatch,context=None):
        """Read completed evidence only; a bare/partial TAR never becomes ACK."""
        reference,document=self.artifacts.existing('backup',intent,dispatch);body=document['body']
        require(body['baselineReference']==baseline_reference and body['tarName']==self.artifacts.name('backup',intent,dispatch,'tar'),
                'original_backup_reference_changed')
        original=self.artifacts.load(baseline_reference,'baseline');baseline=original['body']['baseline']
        before=self._boundary('holder',context,self.holder_id)
        require(digest(before)==digest(body['boundaryAfter']),'current_original_backup_boundary_changed')
        for purpose,stored in (('backup-pre-census',body['preTracked']),('backup-post-census',body['postTracked'])):
            packet=self._census(intent,dispatch,purpose,inspection=True)
            require(self._receipt(packet)==stored and digest(packet['result'])==digest(baseline),
                    'original_backup_observer_retirement_changed')
        path=self.artifacts.root/body['tarName'];info=path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_uid==os.geteuid() and info.st_nlink==1 and
                stat.S_IMODE(info.st_mode)==0o600,'private_backup_tar_required')
        directory=self.artifacts.directory()
        try:
            fd=os.open(body['tarName'],os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=directory)
            try:
                require(census._same(os.fstat(fd))==census._same(info),'backup_identity_changed')
                with os.fdopen(fd,'rb',closefd=False) as stream:
                    proof=validate_tar(stream,baseline=baseline,key=self.key,max_bytes=body['tarValidation']['archiveBytes'],
                                       max_seconds=self.seconds)
                require(census._same(os.fstat(fd))==census._same(path.lstat())==census._same(info),'backup_changed')
            finally:os.close(fd)
        finally:os.close(directory)
        closure=body['streamClosure']
        require(proof==body['tarValidation'] and closure.get('streamClosed') is True and
                type(closure.get('clientExitCode')) is int and closure['clientExitCode']==0 and
                closure['bytes']==proof['archiveBytes'] and closure['sha256']==proof['archiveSha256'],
                'retained_archive_closure_changed')
        after=self._boundary('holder',context,self.holder_id)
        require(digest(before)==digest(after),'current_original_backup_boundary_changed')
        return reference

    def inspect_projection(self,baseline_reference,intent,*,phase,binding,dispatch=None,context=None,final_id=None):
        reference,document=self.artifacts.existing('projection',intent,dispatch);body=document['body']
        require(body['baselineReference']==baseline_reference,'original_projection_reference_changed')
        role='final' if phase=='final-frozen' else 'holder';owner=final_id if role=='final' else self.holder_id
        before=self._boundary(role,context,owner)
        require(digest(before)==digest(body['boundaryAfter']),'current_projection_boundary_changed')
        if role=='final':require(before['gate']['ownerSha256']==binding['ownerSha256'] and
            before['gate']['snapshot']['planId']==binding['planId'] and before['gate']['snapshot']['startupId']==binding['currentStartupId'],
            'current_final_gate_binding_changed')
        original=self.artifacts.load(baseline_reference,'baseline');baseline=original['body']['baseline']
        import base64
        request=dict(kind='projection',root=self.settings['stateRoot'],keyHex=self.key.hex(),
            schema=base64.b64encode(self.schema).decode(),policy=base64.b64encode(self.policy).decode(),
            policySha256=self.policy_sha,baseline=baseline,baselineSha256=digest(baseline),phase=phase,binding=binding,
            maxArtifactBytes=self.max_artifact,maxMetadataBytes=self.max_metadata,seconds=self.seconds)
        packet=self._exec(owner,self._source(request),intent,dispatch,'projection:'+phase,inspection=True)
        require(self._receipt(packet)==body['tracked'] and packet['result']==body['projection'],
                'retained_projection_or_retirement_changed')
        require(digest(before)==digest(self._boundary(role,context,owner)),'current_projection_boundary_changed')
        return reference


    def validate_backup_artifact(self,baseline_reference,backup_reference):
        """Revalidate accepted historical DATA after its old owner retired.

        No remote IO, tracking replay, lease acquisition or ownership assertion.
        The driver must separately validate the accepted journal chain and NEW
        current authority before any later control mutation.
        """
        original=self.artifacts.load(baseline_reference,'baseline');baseline=original['body']['baseline']
        document=self.artifacts.load(backup_reference,'backup');body=document['body']
        require(body['baselineReference']==baseline_reference and backup_reference['name'].endswith('.json') and
                body['tarName']==backup_reference['name'][:-5]+'.tar','historical_backup_binding_changed')
        path=self.artifacts.root/body['tarName'];directory=self.artifacts.directory()
        try:
            info=os.stat(body['tarName'],dir_fd=directory,follow_symlinks=False)
            require(stat.S_ISREG(info.st_mode) and info.st_uid==os.geteuid() and info.st_nlink==1 and
                    stat.S_IMODE(info.st_mode)==0o600 and info.st_size<=self.max_backup,'private_retained_backup_required')
            fd=os.open(body['tarName'],os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=directory)
            try:
                require(census._same(os.fstat(fd))==census._same(info),'historical_backup_identity_changed')
                with os.fdopen(fd,'rb',closefd=False) as stream:
                    proof=validate_tar(stream,baseline=baseline,key=self.key,max_bytes=body['tarValidation']['archiveBytes'],
                                       max_seconds=self.seconds)
                require(census._same(os.fstat(fd))==census._same(path.lstat())==census._same(info),'historical_backup_changed')
            finally:os.close(fd)
        finally:os.close(directory)
        closure=body['streamClosure']
        require(proof==body['tarValidation'] and closure.get('streamClosed') is True and
                type(closure.get('clientExitCode')) is int and closure['clientExitCode']==0 and
                closure['bytes']==proof['archiveBytes'] and closure['sha256']==proof['archiveSha256'],
                'historical_backup_receipt_changed')
        self.artifacts.check()
        return dict(schemaVersion=1,kind='retained-backup-data-validation',baselineReference=baseline_reference,
            backupReference=backup_reference,tarValidation=proof,originalAuthorityProven=False,restoreAuthorized=False)
