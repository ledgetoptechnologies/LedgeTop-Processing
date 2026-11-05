"""Authenticated, read-only live worker readiness; never an execution controller."""
import hashlib
import hmac
import json
import os
from pathlib import Path
import stat
import sys
import time
import uuid

from . import reference_protocol as p
from .reference_api import Response

PATH = '/ltds/worker/v1/maintenance/readiness'
PROTOCOL = 'ltds-worker-maintenance-readiness-v1'
ACTIVATION_FIELDS = ('schemaVersion','activationId','planId','imageId','engineVersion')


def source_digest(root):
    """Bounded installed Python footprint, distinct from TreeDar-only source pins."""
    root=Path(root).absolute()
    if root.resolve()!=root or any(part.is_symlink() for part in (root,*root.parents)):
        raise ValueError('worker source identity changed')
    files={};total=0;entries=0
    for package in ('processing','ops'):
        base=root/package
        pending=[base];paths=[]
        while pending:
            directory=pending.pop()
            with os.scandir(directory) as iterator:
                for entry in iterator:
                    entries+=1
                    if entries>4096 or entry.is_symlink():raise ValueError('worker source identity changed')
                    if entry.is_dir(follow_symlinks=False):pending.append(Path(entry.path))
                    elif entry.name.endswith('.py'):paths.append(Path(entry.path))
        for path in sorted(paths):
            info=path.lstat()
            if (path.resolve()!=path or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                    or info.st_size>4*1024*1024 or len(files)>=512):
                raise ValueError('worker source identity changed')
            total+=info.st_size
            if total>32*1024*1024:raise ValueError('worker source footprint exceeds limit')
            with path.open('rb') as stream:data=stream.read(4*1024*1024+1)
            if len(data)!=info.st_size:raise ValueError('worker source changed during read')
            files[path.relative_to(root).as_posix()]=hashlib.sha256(data).hexdigest()
    if 'ops/reference_worker_daemon.py' not in files or 'processing/reference_driver.py' not in files:
        raise ValueError('worker source footprint incomplete')
    return p.digest(files)


_ACTUAL_SOURCE_ROOT=Path(__file__).resolve().parents[1]
_IMPORTED_SOURCE_SHA256=source_digest(_ACTUAL_SOURCE_ROOT)


def immutable_source_paths(root):
    """Current service identity must not be able to write runtime source paths."""
    if not sys.platform.startswith('linux'):raise ValueError('Linux readonly installed source required')
    root=Path(root);pending=[root/'ops',root/'processing'];result={};count=0
    paths=[root]
    while pending:
        directory=pending.pop();paths.append(directory)
        with os.scandir(directory) as entries:
            for entry in entries:
                count+=1
                if count>4096 or entry.is_symlink():raise ValueError('immutable worker source required')
                if entry.is_dir(follow_symlinks=False):pending.append(Path(entry.path))
                else:paths.append(Path(entry.path))
    for path in paths:
        info=path.lstat();readonly_mount=bool(os.statvfs(path).f_flag & os.ST_RDONLY)
        if (path.resolve()!=path or not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode))
                or (info.st_mode & 0o022 and not readonly_mount) or os.access(path,os.W_OK)):
            raise ValueError('readonly worker source required')
        result[path.relative_to(root).as_posix()]=dict(device=info.st_dev,inode=info.st_ino,
            mode=stat.S_IMODE(info.st_mode),owner=info.st_uid,group=info.st_gid,readonlyMount=readonly_mount)
    return result


def loaded_source_snapshot(root):
    """Bind actual imported package origins to the reviewed installed footprint."""
    root=Path(root).absolute()
    if root!=_ACTUAL_SOURCE_ROOT or source_digest(root)!=_IMPORTED_SOURCE_SHA256:
        raise ValueError('loaded worker source differs from reviewed installation')
    paths=immutable_source_paths(root)
    result={}
    for key,module in tuple(sys.modules.items()):
        spec=getattr(module,'__spec__',None)
        name=getattr(spec,'name',None) if key=='__main__' else key
        if type(name) is not str or name.split('.')[0] not in ('ops','processing'):continue
        if module is None or spec is None or spec.name!=name:raise ValueError('loaded worker module origin unavailable')
        relative=Path(*name.split('.'))
        expected=root/(relative/'__init__.py' if spec.submodule_search_locations is not None else relative.with_suffix('.py'))
        actual=getattr(module,'__file__',None);origin=getattr(spec,'origin',None)
        namespace=spec.submodule_search_locations is not None and actual is None and origin is None and not expected.exists()
        if not namespace and (type(actual) is not str or type(origin) is not str or Path(actual)!=expected or Path(origin)!=expected
                or expected.resolve()!=expected):raise ValueError('loaded worker module origin changed')
        if spec.submodule_search_locations is not None and (list(dict.fromkeys(spec.submodule_search_locations))!=[str(expected.parent)]
                or list(dict.fromkeys(getattr(module,'__path__',())))!=[str(expected.parent)]):
            raise ValueError('loaded worker package search path changed')
        if namespace:
            directory=expected.parent;info=directory.stat()
            result[name]=dict(relativePath=directory.relative_to(root).as_posix(),device=info.st_dev,inode=info.st_ino,namespace=True)
            continue
        info=expected.stat()
        result[name]=dict(relativePath=expected.relative_to(root).as_posix(),device=info.st_dev,inode=info.st_ino,
            byteSize=info.st_size,modifiedNs=info.st_mtime_ns,changedNs=info.st_ctime_ns,
            sha256=hashlib.sha256(expected.read_bytes()).hexdigest())
    if 'processing.reference_worker_readiness' not in result or 'processing.reference_protocol' not in result:
        raise ValueError('loaded worker source footprint incomplete')
    return dict(modules=result,paths=paths)


def validate_activation(value):
    if value is None:return None
    p._closed(value,ACTIVATION_FIELDS,'worker maintenance activation')
    p._require(type(value['schemaVersion']) is int and value['schemaVersion']==1,'activation version')
    for key in ('activationId','planId'):p._uuid(value[key],key)
    p._require(type(value['imageId']) is str and value['imageId'].startswith('sha256:'),'image pin')
    p._hash(value['imageId'][7:],'image pin')
    p._require(type(value['engineVersion']) is str and 1<=len(value['engineVersion'])<=128
        and all(0x21<=ord(c)<=0x7e for c in value['engineVersion']),'engine version pin')
    return dict(value)


def validate_settings(value):
    p._closed(value,('schemaVersion','bearer','sourceRoot','sourceSha256','activation'),'worker maintenance readiness')
    p._require(type(value['schemaVersion']) is int and value['schemaVersion']==1,'readiness version')
    token=value['bearer']
    p._require(type(token) is str and 32<=len(token)<=4096 and all(0x21<=ord(c)<=0x7e for c in token),'readiness credential')
    p._hash(value['sourceSha256'],'worker source pin')
    p._require(type(value['sourceRoot']) is str and Path(value['sourceRoot']).is_absolute(),'worker source root')
    validate_activation(value['activation'])
    return dict(value)


class WorkerReadinessAPI:
    def __init__(self, inner, *, settings, config_sha256, driver, owner, reader_session):
        self.inner=inner;self.settings=validate_settings(settings)
        self.config_sha256=config_sha256;self.driver=driver;self.owner=owner;self.session=reader_session
        p._hash(config_sha256,'loaded worker config')
        self.loaded_source=loaded_source_snapshot(self.settings['sourceRoot'])
        if self.settings['sourceSha256']!=_IMPORTED_SOURCE_SHA256:raise ValueError('worker loaded source pin differs')
        self.boot_id=str(uuid.uuid4());self.service=None

    def _snapshot(self,require_idle):
        driver=self.driver;worker=driver.worker
        if self.service is None:raise ValueError('worker service not started')
        health=self.service.health()
        if health['state']!='ready' or health['driverAlive'] is not True or health['listenerAlive'] is not True:
            raise ValueError('worker service not ready')
        if not driver._lock.acquire(blocking=False):raise ValueError('worker driver busy')
        try:
            deadline=time.monotonic()+6
            self.owner.check(driver.path.parent);self.session.check()
            current_source=loaded_source_snapshot(self.settings['sourceRoot'])
            if (self.settings['sourceSha256']!=_IMPORTED_SOURCE_SHA256
                    or current_source['paths']!=self.loaded_source['paths']
                    or any(current_source['modules'].get(key)!=value for key,value in self.loaded_source['modules'].items())):
                raise ValueError('worker loaded source changed')
            from .reference_execution import _migration_rows
            pins=_migration_rows(driver.path,'SELECT pin FROM driver_engine_pin WHERE singleton=1')
            bindings=_migration_rows(driver.path,'SELECT binding FROM driver_binding WHERE singleton=1')
            if len(pins)!=1 or pins[0]['pin']!=worker.version or len(bindings)!=1:
                raise ValueError('worker driver pin not committed')
            binding=bindings[0]
            worker._verify_local_engine()
            info=worker.engine.read('/info');tasks=worker.engine.read('/task/list')
            if (type(info) is not dict or info.get('engineVersion')!=worker.version
                    or type(info.get('taskQueueCount')) is not int or info['taskQueueCount']<0
                    or type(tasks) is not list or len(tasks)>128):
                raise ValueError('worker engine not ready')
            from .options import _validate_options
            from .reference_option_policy import safe_option_definitions
            definitions=_validate_options(worker.engine.read('/options',limit=262144))
            if worker.engine.token and worker.engine.token in p.canonical_json(definitions).decode():
                raise ValueError('engine options contain credentials')
            selected=safe_option_definitions(definitions)
            if not selected:raise ValueError('worker engine options unavailable')
            idle=info['taskQueueCount']==0
            seen=set()
            for task in tasks:
                if time.monotonic()>deadline:raise ValueError('worker readiness census deadline')
                if type(task) is not dict:raise ValueError('engine task census unavailable')
                identifier=task.get('uuid');p._uuid(identifier,'engine task')
                if identifier in seen:raise ValueError('duplicate engine task')
                seen.add(identifier)
                current=worker.engine.read('/task/'+identifier+'/info')
                if (type(current) is not dict or current.get('uuid')!=identifier
                        or type(current.get('status')) is not dict
                        or type(current['status'].get('code')) is not int
                        or current['status']['code'] not in (10,20,30,40,50)):
                    raise ValueError('engine task state unavailable')
                idle=idle and current['status']['code'] in (30,40,50)
            if require_idle and not idle:raise ValueError('worker engine not idle')
            activation=validate_activation(self.settings['activation'])
            if activation is not None and activation['engineVersion']!=worker.version:
                raise ValueError('worker activation pin changed')
            return dict(schemaVersion=1,protocol=PROTOCOL,bootId=self.boot_id,pid=os.getpid(),
                nodeId=worker.node_id,workerId=worker.worker_id,workerInstanceId=driver.worker_instance_id,
                authorityId=worker.authority_id,engineId=worker.engine_id,gateGeneration=worker.gate_generation,
                engineVersion=worker.version,configSha256=self.config_sha256,
                sourceSha256=self.settings['sourceSha256'],loadedSourceSha256=p.digest(self.loaded_source),driverBindingSha256=p.digest(json.loads(binding['binding'])),
                optionsSha256=p.digest(definitions),safeOptionsSha256=p.digest(selected),
                readerEpoch=self.session.epoch,activation=activation,ready=True,idle=idle)
        finally:driver._lock.release()

    def handle(self, method, path, headers, body=b'', *, protected_transport=False):
        if path!=PATH:return self.inner.handle(method,path,headers,body,protected_transport=protected_transport)
        if protected_transport is not True:return Response(503,{'error':'protected_transport_required'})
        authorization=[v for k,v in headers.items() if type(k) is str and k.lower()=='authorization'] if hasattr(headers,'items') else []
        expected='Bearer '+self.settings['bearer']
        if (len(authorization)!=1 or type(authorization[0]) is not str or not authorization[0].isascii()
                or not hmac.compare_digest(authorization[0],expected)):
            return Response(401,{'error':'unauthorized'})
        try:
            if method!='POST' or type(body) is not bytes or len(body)>4096:raise ValueError()
            request=p.decode_submission(body,max_bytes=4096)
            p._closed(request,('schemaVersion','nonce','requireIdle'),'readiness request')
            if type(request['schemaVersion']) is not int or request['schemaVersion']!=1:raise ValueError()
            if type(request['requireIdle']) is not bool:raise ValueError()
            p._uuid(request['nonce'],'readiness nonce')
        except (ValueError,TypeError):return Response(400,{'error':'invalid_readiness_request'})
        try:return Response(200,dict(self._snapshot(request['requireIdle']),nonce=request['nonce']))
        except Exception:return Response(503,{'error':'worker_not_ready'})
