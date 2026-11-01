"""Draft TreeDar bundle metadata and an owner-supplied partial ZIP writer.

This is deliberately not a publication journal or a result/acceptance adapter.
The caller provides the durable one-attempt intent, state-owner lease, scratch
reservation and exclusive empty partial descriptor. Unknown publication recovery
belongs to that caller; this module never creates, renames or removes files.
"""
from dataclasses import asdict,dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import threading
from types import MappingProxyType
from typing import Protocol
import zipfile

from . import reference_protocol as p

FORMAT='ltds-treedar-bundle-v1'
MANIFEST_NAME='treedar-bundle.json'
ENGINE_VERSION='0.0.1'
NATIVE_FILES=MappingProxyType({
    'area_types.csv':('area_types_template','csv'),'areas.png':('area_map_preview','png'),
    'areas.tif':('area_labels','cog'),'chm.tif':('canopy_height','cog'),
    'chm_raw.tif':('canopy_height_unfiltered','cog'),
    'classify/classify_report.json':('classifier_report','json'),
    'classify/final_test_predictions.csv':('final_test_predictions','csv'),
    'classify/predictions.csv':('tree_type_predictions','csv'),
    'crowns.tif':('crown_labels','cog'),'dtm.tif':('ground_model','cog'),
    'labels.csv':('labels','csv'),'manifest.json':('engine_manifest','json'),
    'ortho_rgb.tif':('orthophoto_point_colour','cog'),'qc.json':('quality_report','json'),
    'report.html':('report','html'),'summary.json':('run_summary','json'),
    'tree_areas.csv':('tree_areas','csv'),'tree_features.csv':('tree_features','csv'),
    'tree_ids.csv':('lasting_tree_ids','csv'),'tree_map.geojson':('crowns','geojson'),
    'tree_map.png':('preview','png'),'trees.csv':('tree_table','csv')})
REQUIRED_FILES=frozenset(('areas.png','chm.tif','chm_raw.tif','crowns.tif','dtm.tif',
    'manifest.json','report.html','summary.json','tree_features.csv','tree_map.geojson','tree_map.png','trees.csv'))
LIMIT_KEYS=('maxArchiveBytes','maxRawBytes','maxFiles','maxManifestBytes','maxControlProofBytes','maxValidationDiskBytes')


class BundleHeld(ValueError):pass


def _require(condition,reason):
    if not condition:raise BundleHeld(reason)


def _closed(value,keys,reason):
    _require(type(value) is dict and set(value)==set(keys),reason)


def _integer(value,reason,minimum=1):
    _require(type(value) is int and minimum<=value<=p.MAX_SAFE_INTEGER,reason)
    return value


def _detached(value):return json.loads(p.canonical_json(value))


@dataclass(frozen=True)
class BundleLimits:
    maxArchiveBytes:int
    maxRawBytes:int
    maxFiles:int
    maxManifestBytes:int
    maxControlProofBytes:int
    maxValidationDiskBytes:int

    def __post_init__(self):
        for value in asdict(self).values():_integer(value,'explicit_positive_bundle_limits_required')
        _require(self.maxFiles<=64 and self.maxManifestBytes<=1024**2,'bounded_bundle_limits_required')
        _integer(self.maxRawBytes+self.maxManifestBytes,'bounded_expansion_required')

    @classmethod
    def from_mapping(cls,value):
        _closed(value,LIMIT_KEYS,'closed_bundle_limits_required')
        return cls(**value)


def _binding(value):
    _closed(value,('jobId','generation','environment','inputManifestSha256','pipelineDigest','sourceLidar'),'closed_original_binding_required')
    p._uuid(value['jobId'],'jobId');_integer(value['generation'],'generation_required');p._environment(value['environment'])
    for key in ('inputManifestSha256','pipelineDigest'):p._hash(value[key],key)
    source=value['sourceLidar']
    _closed(source,('objectId','byteSize','sha256','provenanceReceiptId'),'closed_original_lidar_required')
    p._slug(source['objectId'],'original LiDAR ID');_integer(source['byteSize'],'original_lidar_bytes_required')
    p._hash(source['sha256'],'original LiDAR hash');p._slug(source['provenanceReceiptId'],'provenance receipt ID')
    return _detached(value)


def _expected(value):
    _closed(value,('binding','engineVersion'),'closed_expected_bundle_identity_required')
    _require(value['engineVersion']==ENGINE_VERSION,'reviewed_treedar_version_required')
    return dict(binding=_binding(value['binding']),engineVersion=ENGINE_VERSION)


def _decode(wire,maximum):
    if type(wire) is bytes:
        _require(len(wire)<=maximum,'bundle_metadata_limit')
        # Match TextDecoder's UTF-8 BOM handling for Buffer wire input.
        text=wire.decode('utf-8-sig',errors='strict')
    else:
        _require(type(wire) is str and len(wire.encode('utf-8'))<=maximum,'bundle_metadata_limit')
        text=wire
    def unique(pairs):
        value={}
        for key,item in pairs:
            _require(key not in value and key not in ('__proto__','prototype','constructor'),'duplicate_or_unsafe_bundle_key')
            value[key]=item
        return value
    def integer(token):
        _require(re.fullmatch(r'0|[1-9][0-9]*',token) is not None,'bundle_integer_grammar_required')
        return int(token)
    def reject(_):raise BundleHeld('bundle_integer_grammar_required')
    value=json.loads(text,object_pairs_hook=unique,parse_int=integer,parse_float=reject,parse_constant=reject)
    def depth(item,level=0):
        _require(level<=12,'bundle_metadata_depth')
        if type(item) is list:
            _require(len(item)<=64,'bundle_metadata_array_limit')
            for child in item:depth(child,level+1)
        elif type(item) is dict:
            for child in item.values():depth(child,level+1)
    depth(value)
    return value


def manifest_sha256(value):return p.digest({key:item for key,item in value.items() if key!='manifestSha256'})


def validate_manifest(payload,expected,limits):
    """Pure draft validation only; no source, acquisition or publication proof."""
    try:
        _require(type(limits) is BundleLimits,'fixed_bundle_limits_required');expected=_expected(expected)
        value=_decode(payload,limits.maxManifestBytes) if type(payload) in (str,bytes) else payload
        _closed(value,('schemaVersion','format','binding','engineVersion','files','rawBytes','manifestSha256'),'closed_bundle_manifest_required')
        _require(type(value['schemaVersion']) is int and value['schemaVersion']==1 and value['format']==FORMAT
            and value['engineVersion']==ENGINE_VERSION,'bundle_version_required')
        _require(p.canonical_json(_binding(value['binding']))==p.canonical_json(expected['binding']),'original_bundle_binding_changed')
        _require(type(value['files']) is list and len(REQUIRED_FILES)<=len(value['files'])<=limits.maxFiles,'bundle_file_count_required')
        names=set();previous='';total=0
        for item in value['files']:
            _closed(item,('nativePath','role','format','byteSize','sha256'),'closed_native_file_required')
            name=item['nativePath']
            _require(type(name) is str and name in NATIVE_FILES and name>previous and name.lower() not in names,'known_sorted_native_paths_required')
            _require((item['role'],item['format'])==NATIVE_FILES[name],'native_semantics_changed')
            total+=_integer(item['byteSize'],'positive_native_bytes_required');p._hash(item['sha256'],'native file hash')
            _require(total<=limits.maxRawBytes and total<=p.MAX_SAFE_INTEGER,'raw_bundle_budget_exceeded')
            names.add(name.lower());previous=name
        _require(REQUIRED_FILES<=names,'required_native_outputs_missing')
        _require(_integer(value['rawBytes'],'raw_bytes_required')==total,'raw_byte_sum_changed')
        p._hash(value['manifestSha256'],'bundle manifest hash')
        _require(value['manifestSha256']==manifest_sha256(value),'bundle_manifest_digest_changed')
        _require(len(p.canonical_json(value))<=limits.maxManifestBytes,'bundle_metadata_limit')
        return _detached(value)
    except BundleHeld:raise
    except Exception:raise BundleHeld('invalid_bundle_manifest') from None


@dataclass(frozen=True)
class BundlePolicy:
    raw_root:str
    output_root:str
    state_root:str
    publication_id:str
    reservation_id:str
    allocation_base:int
    remaining_reserved_bytes:int
    source_stage_proof_sha256:str
    limits:BundleLimits

    def __post_init__(self):
        _require(type(self.limits) is BundleLimits,'fixed_bundle_limits_required')
        for value in (self.publication_id,self.reservation_id):p._slug(value,'host publication/reservation identity')
        _require(len(self.publication_id)<=64,'bounded_publication_identity_required')
        p._hash(self.source_stage_proof_sha256,'source stage proof')
        _integer(self.allocation_base,'parent_allocation_required',0);_integer(self.remaining_reserved_bytes,'reserved_capacity_required')
        _integer(self.allocation_base+self.remaining_reserved_bytes,'safe_parent_reservation_required')
        roots=[Path(value) for value in (self.raw_root,self.output_root,self.state_root)]
        _require(all(type(value) is str and Path(value).is_absolute() and str(Path(value))==value and '..' not in Path(value).parts
            for value in (self.raw_root,self.output_root,self.state_root)),'literal_host_roots_required')
        _require(all(a!=b and a not in b.parents and b not in a.parents for index,a in enumerate(roots) for b in roots[index+1:]),'disjoint_host_roots_required')


class PublicationGuard(Protocol):
    binding:dict
    def check(self,context:dict,*,mutation:bool)->None:
        """Prove the caller's existing state-owner lease, exact reservation,
        source terminal proof and durable one-attempt publication authority.
        When manifestSha256 is present, derive its expected value from the exact
        retained terminal native inventory and original binding; checking only
        that the opaque source proof file exists is insufficient.
        """


def _snapshot(info):return (info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns,info.st_nlink,info.st_uid,stat.S_IMODE(info.st_mode))


def _directory(value):
    path=Path(value)
    try:
        _require(path.resolve()==path and not any(item.is_symlink() for item in (path,*path.parents)),'real_existing_bundle_root_required')
        info=path.stat()
    except OSError:raise BundleHeld('real_existing_bundle_root_required') from None
    _require(stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o700,'owned_private_bundle_root_required')
    return (info.st_dev,info.st_ino)


def _sync_directory(path):
    descriptor=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)


def _regular(path,*,private=False):
    try:info=path.lstat()
    except OSError:raise BundleHeld('owned_single_link_bundle_file_required') from None
    _require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_uid==os.geteuid()
        and not info.st_mode&0o022 and (not private or stat.S_IMODE(info.st_mode)==0o600),'owned_single_link_bundle_file_required')
    return info


class _BoundedZip:
    def __init__(self,fd,maximum):self.fd,self.maximum,self.highwater=fd,maximum,0
    def write(self,value):
        view=memoryview(value);end=self.tell()+len(view)
        _require(end<=self.maximum,'archive_byte_cap_exceeded')
        while view:
            written=os.write(self.fd,view)
            _require(written>0,'partial_archive_write_failed');view=view[written:]
        self.highwater=max(self.highwater,end);return len(value)
    def tell(self):return os.lseek(self.fd,0,os.SEEK_CUR)
    def seek(self,offset,whence=os.SEEK_SET):
        result=os.lseek(self.fd,offset,whence);_require(0<=result<=self.maximum,'archive_seek_outside_cap');return result
    def flush(self):return None
    def seekable(self):return True


class BundlePartialWriter:
    """One-shot writer. Caller owns all files, leases and durable publication.

    prepare() reads original RAW files. intent_for(fd) supplies the exact draft
    intent the caller must persist before write_to(fd). This writer never
    changes that intent from 'streaming', and never publishes the partial.
    """
    def __init__(self,policy,expected,*,guard):
        _require(sys.platform.startswith('linux') and hasattr(os,'pread') and type(policy) is BundlePolicy,'linux_owned_partial_writer_required')
        self.policy=policy;self.expected=_expected(expected);self.policy_sha256=p.digest(asdict(policy))
        self._expected_sha=p.digest(self.expected);self._pid=os.getpid();self._mutex=threading.Lock();self._attempted=False
        self._roots={value:_directory(value) for value in (policy.raw_root,policy.output_root,policy.state_root)}
        self.guard=guard;self._files=None;self._manifest=None
        self._check();self._guard(mutation=False)

    @property
    def partial_path(self):return Path(self.policy.output_root)/(self.policy.publication_id+'.zip.partial')
    @property
    def intent_path(self):return Path(self.policy.state_root)/(self.policy.publication_id+'.intent.json')

    def guard_binding(self):
        return dict(policySha256=self.policy_sha256,expectedSha256=self._expected_sha,publicationId=self.policy.publication_id,
            sourceStageProofSha256=self.policy.source_stage_proof_sha256)

    def _check(self):
        _require(os.getpid()==self._pid and p.digest(asdict(self.policy))==self.policy_sha256
            and p.digest(self.expected)==self._expected_sha,'bundle_owner_or_policy_changed')
        for root,identity in self._roots.items():_require(_directory(root)==identity,'bundle_root_identity_changed')

    def _guard(self,*,mutation):
        self._check()
        _require(getattr(self.guard,'binding',None)==self.guard_binding() and callable(getattr(self.guard,'check',None)),'trusted_publication_guard_required')
        context=dict(expected=self.expected,publicationId=self.policy.publication_id,reservationId=self.policy.reservation_id,
            allocationBase=self.policy.allocation_base,remainingReservedBytes=self.policy.remaining_reserved_bytes,
            sourceStageProofSha256=self.policy.source_stage_proof_sha256,manifestSha256=self._manifest['manifestSha256'] if self._manifest else None)
        try:_require(self.guard.check(_detached(context),mutation=mutation) is None,'publication_guard_must_raise_or_return_none')
        except Exception:raise BundleHeld('publication_authority_unavailable') from None
        self._check()

    def _inventory(self,*,hash_files):
        root=Path(self.policy.raw_root);result={};directories=set();census=set();raw_bytes=0
        def walk(folder,prefix=''):
            nonlocal raw_bytes
            # Census before sorting: even a hostile directory cannot materialize
            # an unbounded list. Only the single native directory is allowed.
            names=[]
            with os.scandir(folder) as entries:
                for entry in entries:
                    name=prefix+entry.name
                    _require(name not in census and (name in NATIVE_FILES or name=='classify'),'unknown_or_excess_native_file')
                    census.add(name)
                    _require(len(census-{'classify'})<=self.policy.limits.maxFiles,'unknown_or_excess_native_file')
                    names.append(entry.name)
            for basename in sorted(names):
                path=folder/basename;name=prefix+basename;info=path.lstat()
                _require(not stat.S_ISLNK(info.st_mode),'raw_bundle_symlink_refused')
                if stat.S_ISDIR(info.st_mode):
                    _require(name=='classify' and info.st_uid==os.geteuid() and not info.st_mode&0o022,'unknown_native_directory')
                    directories.add(name);walk(path,name+'/');continue
                _require(name in NATIVE_FILES and len(result)<self.policy.limits.maxFiles,'unknown_or_excess_native_file')
                info=_regular(path);_integer(info.st_size,'positive_native_bytes_required')
                _require(info.st_size<=self.policy.limits.maxRawBytes-raw_bytes,'raw_bundle_budget_exceeded')
                raw_bytes+=info.st_size
                digest=None
                if hash_files:
                    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
                    try:
                        _require(_snapshot(info)==_snapshot(os.fstat(fd)),'raw_bundle_file_changed');hasher=hashlib.sha256()
                        consumed=0
                        while True:
                            # One overflow byte detects growth without chasing
                            # a moving EOF or spending beyond declared RAW.
                            block=os.read(fd,min(1024**2,info.st_size-consumed+1))
                            if not block:break
                            consumed+=len(block)
                            _require(consumed<=info.st_size,'raw_bundle_file_changed')
                            hasher.update(block)
                        _require(consumed==info.st_size,'raw_bundle_file_changed')
                        digest=hasher.hexdigest()
                        _require(_snapshot(info)==_snapshot(os.fstat(fd))==_snapshot(path.lstat()),'raw_bundle_file_changed')
                    finally:os.close(fd)
                result[name]=dict(snapshot=_snapshot(info),sha256=digest)
        walk(root)
        _require(REQUIRED_FILES<=set(result) and (not directories or any(name.startswith('classify/') for name in result)),'native_output_closure_missing')
        _require(sum(item['snapshot'][2] for item in result.values())<=self.policy.limits.maxRawBytes,'raw_bundle_budget_exceeded')
        return result

    def prepare(self):
        _require(not self._attempted,'partial_writer_attempt_already_issued');self._guard(mutation=False)
        if self._manifest is None:
            self._files=self._inventory(hash_files=True)
            files=[dict(nativePath=name,role=NATIVE_FILES[name][0],format=NATIVE_FILES[name][1],byteSize=item['snapshot'][2],sha256=item['sha256']) for name,item in sorted(self._files.items())]
            value=dict(schemaVersion=1,format=FORMAT,**self.expected,files=files,rawBytes=sum(item['byteSize'] for item in files))
            value['manifestSha256']=manifest_sha256(value);self._manifest=validate_manifest(value,self.expected,self.policy.limits)
        self._check_source();self._guard(mutation=False)
        return _detached(self._manifest)

    def _check_source(self):
        current=self._inventory(hash_files=False)
        _require(self._files is not None and {name:item['snapshot'] for name,item in current.items()}=={name:item['snapshot'] for name,item in self._files.items()},'raw_bundle_inventory_changed')

    def _partial(self,fd,*,empty):
        import fcntl
        _require(type(fd) is int and fd>=0,'owned_partial_descriptor_required')
        info=_regular(self.partial_path,private=True);observed=os.fstat(fd);flags=fcntl.fcntl(fd,fcntl.F_GETFL)
        _require(_snapshot(info)==_snapshot(observed) and flags&os.O_ACCMODE==os.O_RDWR and not flags&os.O_APPEND,'exact_owned_partial_descriptor_required')
        if empty:_require(info.st_size==0 and os.lseek(fd,0,os.SEEK_CUR)==0,'empty_unpublished_partial_required')
        return info

    def intent_for(self,fd):
        """Metadata helper only. Caller must persist this with exclusive create."""
        manifest=self.prepare();partial=self._partial(fd,empty=True)
        value=dict(schemaVersion=1,intentType='ltds-treedar-partial-intent-v1',state='streaming',publicationId=self.policy.publication_id,
            policySha256=self.policy_sha256,sourceStageProofSha256=self.policy.source_stage_proof_sha256,
            reservation=dict(id=self.policy.reservation_id,allocationBase=self.policy.allocation_base,remainingBytes=self.policy.remaining_reserved_bytes),
            roots={name:dict(device=str(identity[0]),inode=str(identity[1])) for name,identity in self._roots.items()},
            partial=dict(name=self.partial_path.name,device=str(partial.st_dev),inode=str(partial.st_ino)),manifest=manifest)
        value['intentSha256']=p.digest(value)
        return _detached(value)

    def _intent(self,expected):
        before=_regular(self.intent_path,private=True)
        maximum=self.policy.limits.maxManifestBytes+self.policy.limits.maxControlProofBytes
        _require(before.st_size<=maximum,'publication_control_budget_exceeded')
        fd=os.open(self.intent_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:
            _require(_snapshot(before)==_snapshot(os.fstat(fd)),'publication_intent_changed')
            data=os.read(fd,maximum+1)
            _require(len(data)==before.st_size and _snapshot(before)==_snapshot(os.fstat(fd))==_snapshot(self.intent_path.lstat()),'publication_intent_changed')
            # Intent has more nested fields than the wire manifest, but the
            # same bounded integer/duplicate-key JSON grammar.
            value=_decode(data,maximum)
            _require(p.canonical_json(value)==p.canonical_json(expected),'exact_durable_publication_intent_required')
            os.fsync(fd)
        finally:os.close(fd)
        for root in (Path(self.policy.state_root),Path(self.policy.state_root).parent,Path(self.policy.output_root),Path(self.policy.output_root).parent):_sync_directory(root)
        return data,before

    @staticmethod
    def _info(name,size):
        info=zipfile.ZipInfo(name,date_time=(1980,1,1,0,0,0));info.create_system=3
        info.compress_type=zipfile.ZIP_STORED;info.external_attr=(stat.S_IFREG|0o600)<<16;info.file_size=size
        return info

    def write_to(self,fd):
        _require(self._mutex.acquire(blocking=False),'partial_writer_busy')
        try:
            expected_intent=self.intent_for(fd);self._attempted=True
            manifest_bytes=p.canonical_json(self._manifest);intent_bytes,intent_stat=self._intent(expected_intent)
            control_bytes=len(intent_bytes)-len(manifest_bytes)
            _require(0<=control_bytes<=self.policy.limits.maxControlProofBytes,'publication_control_budget_exceeded')
            maximum_new=self.policy.limits.maxArchiveBytes+len(manifest_bytes)+self.policy.limits.maxControlProofBytes
            _require(self.policy.allocation_base>=self._manifest['rawBytes'] and maximum_new<=self.policy.remaining_reserved_bytes,'parent_reserved_capacity_insufficient')
            _require(self._manifest['rawBytes']+maximum_new<=self.policy.limits.maxValidationDiskBytes,'coexisting_raw_zip_control_budget_exceeded')
            self._guard(mutation=True);self._check_source();self._partial(fd,empty=True)
            stream=_BoundedZip(fd,self.policy.limits.maxArchiveBytes)
            with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED,allowZip64=True) as archive:
                archive.writestr(self._info(MANIFEST_NAME,len(manifest_bytes)),manifest_bytes)
                for item in self._manifest['files']:
                    self._guard(mutation=True)
                    source=Path(self.policy.raw_root)/item['nativePath'];info=_regular(source)
                    _require(_snapshot(info)==self._files[item['nativePath']]['snapshot'],'raw_bundle_file_changed')
                    source_fd=os.open(source,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
                    try:
                        _require(_snapshot(os.fstat(source_fd))==_snapshot(info),'raw_bundle_file_changed')
                        digest=hashlib.sha256();copied=0
                        with archive.open(self._info('artifacts/'+item['nativePath'],item['byteSize']),'w',force_zip64=item['byteSize']>zipfile.ZIP64_LIMIT) as destination:
                            while block:=os.read(source_fd,1024**2):
                                copied+=len(block);_require(copied<=item['byteSize'],'raw_bundle_file_changed');digest.update(block);destination.write(block)
                        _require(copied==item['byteSize'] and digest.hexdigest()==item['sha256'] and _snapshot(info)==_snapshot(os.fstat(source_fd))==_snapshot(source.lstat()),'raw_bundle_file_changed')
                    finally:os.close(source_fd)
            os.fsync(fd);self._check_source();self._guard(mutation=True)
            _require(_snapshot(intent_stat)==_snapshot(self.intent_path.lstat()),'publication_intent_changed')
            observed=self._partial(fd,empty=False);archive_size=observed.st_size
            self._verify_archive(fd,manifest_bytes)
            digest=hashlib.sha256();offset=0
            while block:=os.pread(fd,1024**2,offset):digest.update(block);offset+=len(block)
            _require(offset==archive_size and _snapshot(observed)==_snapshot(os.fstat(fd))==_snapshot(self.partial_path.lstat()),'owned_partial_changed')
            _sync_directory(Path(self.policy.output_root));self._guard(mutation=True)
            new_bytes=archive_size+len(intent_bytes)
            _require(new_bytes<=self.policy.remaining_reserved_bytes,'parent_reserved_capacity_insufficient')
            return dict(state='partial_complete_needs_durable_publication',format=FORMAT,binding=_detached(self.expected['binding']),engineVersion=ENGINE_VERSION,
                manifestSha256=self._manifest['manifestSha256'],archive=dict(byteSize=archive_size,sha256=digest.hexdigest()),nativeFiles=_detached(self._manifest['files']),
                allocation=dict(base=self.policy.allocation_base,newBytes=new_bytes,total=self.policy.allocation_base+new_bytes,
                    retainedRawBytes=self._manifest['rawBytes'],zipBytes=archive_size,manifestBytes=len(manifest_bytes),controlBytes=control_bytes),
                validationScope='constructed-partial-bytes-and-metadata-only')
        except BundleHeld:raise
        except Exception:raise BundleHeld('partial_bundle_outcome_requires_owner_reconciliation') from None
        finally:self._mutex.release()

    def _verify_archive(self,fd,manifest_bytes):
        with os.fdopen(os.dup(fd),'rb') as reader,zipfile.ZipFile(reader,'r') as archive:
            expected=[MANIFEST_NAME]+['artifacts/'+item['nativePath'] for item in self._manifest['files']]
            _require(archive.namelist()==expected,'constructed_zip_inventory_changed')
            _require(archive.read(MANIFEST_NAME)==manifest_bytes,'constructed_zip_manifest_changed')
            for item in self._manifest['files']:
                digest=hashlib.sha256();size=0
                with archive.open('artifacts/'+item['nativePath']) as source:
                    while block:=source.read(1024**2):digest.update(block);size+=len(block)
                _require(size==item['byteSize'] and digest.hexdigest()==item['sha256'],'constructed_zip_object_changed')
