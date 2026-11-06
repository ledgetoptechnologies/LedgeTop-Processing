"""Opt-in local fixture preparation, not installed enrollment or activation.

The caller must supply a separately seeded, read-only versioned source/venv
layout. This module never creates Docker resources, patches installer policy,
installs a unit, adopts a host path, or sends private worker inputs to the Hub.
"""
import hashlib
import json
import os
from pathlib import Path,PurePosixPath
import signal
import socket
import subprocess
import sys
import time
import tarfile
import sysconfig
import importlib.metadata
from urllib.parse import urlsplit
import uuid

from .enrolled_coordinator_transport import canonical, decode, digest, private_bytes, require, BoundedSubprocess
from . import enroll_reference_worker as install
from . import prepare_reference_worker_config as prep

PROTOCOL='ltds-local-worker-layout-v1'
MAX_ARCHIVE=256*1024**2


def write_private(path,raw):
    path=Path(path);path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    require(path.parent.resolve()==path.parent,'original_fixture_parent_required')
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
    try:
        with os.fdopen(fd,'wb',closefd=False) as stream:stream.write(raw);stream.flush();os.fsync(fd)
    finally:os.close(fd)
    fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
    try:os.fsync(fd)
    finally:os.close(fd)
    return dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw),ownerUid=os.geteuid())


def reference(path):
    return {key:value for key,value in descriptor(path).items() if key in ('path','sha256')}


def descriptor(path):
    path=Path(path);raw=install.read_file(path,owner=os.geteuid(),maximum=MAX_ARCHIVE)
    return dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw),ownerUid=os.geteuid())


def installation_id(operation,node):return str(uuid.uuid5(uuid.UUID(operation),'fixture-worker-install:'+node))


def verify_source_archive(source,archive):
    """Compare every real archive leaf to the separately mounted frozen tree."""
    source=Path(source);seen=set();count=total=0
    with tarfile.open(archive,mode='r|*') as stream:
        for member in stream:
            count+=1;parts=Path(member.name).parts
            require(count<=8192 and member.name and not member.name.startswith('/') and '\\' not in member.name
                    and PurePosixPath(member.name).as_posix()==member.name.rstrip('/')
                    and all(part not in ('.','..') for part in parts),'bounded_original_source_namespace_required')
            if member.isdir():continue
            require(member.isfile() and member.name not in seen and 0<=member.size<=4*1024**2,
                    'original_source_archive_leaf_required')
            seen.add(member.name);total+=member.size;require(total<=MAX_ARCHIVE,'bounded_source_archive_required')
            path=source/member.name;require(path.resolve()==path and path.is_file(),'original_source_archive_path_required')
            raw=stream.extractfile(member).read(4*1024**2+1)
            require(len(raw)==member.size and raw==path.read_bytes(),'source_archive_tree_mismatch')
    require(seen=={path.relative_to(source).as_posix() for path in source.rglob('*') if path.is_file()},
            'complete_source_archive_tree_required')


def seed_layout(source_root,source_archive,operation,candidate_image,source_commit,output_root):
    """Future ONE task-owned seed helper; never an installer or root/chown path.

    Its new owned volume is subsequently mounted read-only. Source directories
    are separate read-only binds of this exact frozen source, never extracted
    into a running app. The source and dependency TARs are real retained inputs.
    """
    require(os.geteuid()==os.getegid()==10001,'fixture_seed_uid_required')
    root=Path(output_root);info=root.lstat()
    require(root.resolve()==root and info.st_uid==10001 and not any(root.iterdir()),'new_owned_seed_volume_required')
    root.chmod(0o700);inputs=root/'inputs';inputs.mkdir(mode=0o700)
    source=Path(source_root);archive=Path(source_archive)
    require(source.resolve()==source and archive.resolve()==archive and bool(os.statvfs(source).f_flag&os.ST_RDONLY)
            and archive.stat().st_size<=MAX_ARCHIVE,'frozen_seed_source_required')
    verify_source_archive(source,archive)
    write_private(inputs/'source.tar',archive.read_bytes())
    from processing.reference_worker_readiness import source_digest
    source_pin=descriptor(inputs/'source.tar');source_pin.update(path='/run/qualification-worker-layout/inputs/source.tar',
        commit=source_commit,footprintSha256=source_digest(source),
        requirementsSha256=hashlib.sha256((source/'requirements.txt').read_bytes()).hexdigest())
    venv=root/'venv';(venv/'bin').mkdir(mode=0o700,parents=True)
    python=Path(sys.executable).resolve();binary=python.read_bytes();require(len(binary)<=64*1024**2,'bounded_fixture_interpreter_required')
    write_private(venv/'bin/python',binary);(venv/'bin/python').chmod(0o550)
    cfg=('home = '+str(Path(sys.base_prefix)/'bin')+'\ninclude-system-site-packages = false\nversion = '+sys.version.split()[0]+'\n').encode()
    write_private(venv/'pyvenv.cfg',cfg)
    pure=Path(sysconfig.get_path('purelib')).resolve();site=venv/'lib'/('python'+str(sys.version_info.major)+'.'+str(sys.version_info.minor))/'site-packages'
    versions={name:importlib.metadata.version(name) for name in ('cryptography','cffi','pycparser','rfc8785')}
    expected=dict(cryptography='50.0.2',cffi='2.1.1',pycparser='2.23' if sys.version.startswith('3.10.') else '3.12',rfc8785='0.1.4')
    require(versions==expected,'actual_fixture_dependency_profile_changed')
    copied=set();total=len(binary);count=0
    for name in versions:
        for relative in importlib.metadata.distribution(name).files or ():
            path=pure/relative
            # Console scripts outside site-packages are not interpreter inputs.
            if '..' in relative.parts or '__pycache__' in relative.parts:continue
            require(path.resolve()==path and not path.is_symlink() and path.is_file(),'original_dependency_leaf_required')
            target=site/relative
            if target in copied:continue
            copied.add(target);data=path.read_bytes();total+=len(data);count+=1
            require(total<=128*1024**2 and count<=8192,'bounded_fixture_dependency_layout_required')
            write_private(target,data)
    for path in sorted(venv.rglob('*'),reverse=True):
        path.chmod(0o550 if path.is_dir() or path==venv/'bin/python' else 0o440)
    venv.chmod(0o550)
    archive_path=inputs/'dependencies.tar';fd=os.open(archive_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    try:
        with os.fdopen(fd,'wb',closefd=False) as stream,tarfile.open(fileobj=stream,mode='w',format=tarfile.PAX_FORMAT) as tar:
            for path in sorted(venv.rglob('*')):tar.add(path,arcname=path.relative_to(venv).as_posix(),recursive=False)
        os.fsync(fd)
    finally:os.close(fd)
    deps=descriptor(archive_path);deps.update(path='/run/qualification-worker-layout/inputs/dependencies.tar',
        pythonSha256=hashlib.sha256(binary).hexdigest(),pythonVersion=sys.version.split()[0],versions=versions)
    value=dict(schemaVersion=1,protocol=PROTOCOL,operationId=operation,candidateImageId=candidate_image,
        source=source_pin,dependencies=deps,installations={node:dict(operationId=installation_id(operation,node),
            root=str(install.INSTALL_BASE/installation_id(operation,node))) for node in ('cluster','main')})
    write_private(root/'layout.json',canonical(value))
    return dict(protocol=PROTOCOL,sha256=digest(value),dependencyFiles=count,dependencyBytes=total,
                installationQualified=False,activationIssued=False)


def load_layout(path,operation,*,source_commit,candidate_image,expected_sha256):
    raw=private_bytes(path,maximum=65536)
    require(hashlib.sha256(raw).hexdigest()==expected_sha256,'reviewed_fixture_layout_digest_changed')
    value=decode(raw)
    install.closed(value,('schemaVersion','protocol','operationId','candidateImageId','source','dependencies','installations'))
    require(value['schemaVersion']==1 and type(value['schemaVersion']) is int and value['protocol']==PROTOCOL
            and value['operationId']==operation and value['candidateImageId']==candidate_image,'reviewed_fixture_layout_required')
    require(value['source']['commit']==source_commit and set(value['installations'])=={'cluster','main'},'fixture_source_layout_changed')
    common=('path','sha256','bytes','ownerUid')
    install.closed(value['source'],(*common,'commit','footprintSha256','requirementsSha256'))
    install.closed(value['dependencies'],(*common,'pythonSha256','pythonVersion','versions'))
    for name in ('source','dependencies'):
        entry=value[name];install.absolute(entry['path']);install.sha(entry['sha256']);install.integer(entry['bytes'],1)
        require(entry['ownerUid']==os.geteuid() and entry['bytes']<=MAX_ARCHIVE,'owned_bounded_fixture_archive_required')
        prep.archive_pin(entry)
    for node,entry in value['installations'].items():
        install.closed(entry,('operationId','root'))
        expected=installation_id(operation,node)
        require(entry==dict(operationId=expected,root=str(install.INSTALL_BASE/expected)),
                'derived_versioned_fixture_layout_required')
        source=Path(entry['root'])/'source';python=Path(entry['root'])/'venv/bin/python'
        require(source.resolve()==source and python.resolve()==python and source.is_dir() and python.is_file()
                and bool(os.statvfs(source).f_flag&os.ST_RDONLY) and bool(os.statvfs(python).f_flag&os.ST_RDONLY),
                'readonly_original_fixture_layout_required')
        identity=invoke(entry,'identity',{},Path('/tmp'))
        require(identity==dict(pythonSha256=value['dependencies']['pythonSha256'],
            pythonVersion=value['dependencies']['pythonVersion'],versions=value['dependencies']['versions'],
            sourceSha256=value['source']['footprintSha256'],requirementsSha256=value['source']['requirementsSha256']),
            'actual_fixture_source_or_dependencies_changed')
    return value


def clean_environment(home):
    return dict(HOME=str(home),XDG_STATE_HOME=str(home/'.local/state'),
                PYTHONNOUSERSITE='1',PYTHONDONTWRITEBYTECODE='1',LANG='C.UTF-8',PATH='/usr/local/bin:/usr/bin:/bin')


def process_identity(pid):
    require(type(pid) is int and pid>0,'owned_fixture_pid_required')
    raw=Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()
    require(raw[0]!='Z','fixture_child_not_retired')
    return raw[19]


def transport_diagnostic(error):
    """Types and numeric OS errno only; never stringify exception payloads."""
    result={'exceptionType':type(error).__name__[:80]}
    nested=getattr(error,'reason',None)
    if isinstance(nested,BaseException):
        result['reasonType']=type(nested).__name__[:80]
        number=getattr(nested,'errno',None)
        if type(number) is int and -65535<=number<=65535:result['errno']=number
    elif nested is not None:
        result['reasonType']='non_exception_reason'
    return result


def invoke(entry,mode,payload,home,*,timeout=15):
    source=str(Path(entry['root'])/'source');python=str(Path(entry['root'])/'venv/bin/python')
    body='import sys;sys.path.insert(0,'+repr(source)+');from tests.fixtures.enrolled_worker_factory_process import main;main('+repr(mode)+')'
    diagnostic=None
    if mode=='probe':
        diagnostic=Path(home)/('probe-'+str(uuid.uuid4()))
        body=('import os,json,traceback;from pathlib import Path;'+body.split(';from tests.',1)[0]+
            ';from ops.qualify_enrolled_worker_preparation import write_private,process_identity,transport_diagnostic;'+
            'd=Path('+repr(str(diagnostic))+');'+
            'write_private(d.with_suffix(".start.json"),json.dumps(dict(pid=os.getpid(),startTime=process_identity(os.getpid()))).encode())\n'+
            'try:\n from tests.fixtures.enrolled_worker_factory_process import main;main("probe")\n'+
            'except BaseException as e:\n'+
            ' frames=[dict(file=Path(f.filename).name,line=f.lineno,function=f.name) for f in traceback.extract_tb(e.__traceback__)[-8:]]\n'+
            ' raw=json.dumps(dict(**transport_diagnostic(e),frames=frames)).encode()\n'+
            ' write_private(d.with_suffix(".stderr.json"),raw[:4096]);raise\n')
    try:
        result=BoundedSubprocess().run(['/usr/bin/env','-i',*[key+'='+value for key,value in clean_environment(home).items()],
        python,'-I','-B','-c',body],input_bytes=canonical(payload),
            timeout=timeout,output_limit=65536,error_limit=4096)
    finally:
        if diagnostic is not None and diagnostic.with_suffix('.start.json').exists():
            start=decode(private_bytes(diagnostic.with_suffix('.start.json'),maximum=1024))
            try:process_identity(start['pid']);retired=False
            except FileNotFoundError:retired=True
            except Exception:retired=False
            write_private(diagnostic.with_suffix('.retirement.json'),canonical(dict(**start,retired=retired)))
    return decode(result)


def prepare_node(root,material,node,authority,layout,*,port,backend,controller_id):
    """Return only real composer outputs and derived installer settings."""
    from processing.viewer_acceptance_receipt import validate_trusted_viewer_receipt_keys
    require(material['final'].get('schemaVersion')==6 and
            bool(validate_trusted_viewer_receipt_keys(material['final'].get('viewerReceiptKeys',{}))),
            'final_viewer_trust_required_for_worker_preparation')
    root=Path(root);root.mkdir(mode=0o700)
    home=root/'home';home.mkdir(mode=0o700)
    entry=layout['installations'][node];engine='fixture-engine-'+node
    host=material['workerHost'];worker=material['bootstrap']['workers'][node]
    worker['resultOrigin']='https://'+host+':'+str(port)
    gate=invoke(entry,'gate',dict(home=str(home),engineId=engine,nodeId=node,authorityId=authority,
        coordinatorOrigin=material['final']['recoveryBindings'][node]['coordinatorOrigin']),home)
    require(gate['generation']==material['final']['recoveryBindings'][node]['gateGeneration'],'fixture_gate_generation_changed')
    tls={}
    for label,name in (('ca','fixture-ca.pem'),('certificate','fixture-cert.pem'),('privateKey','fixture-key.pem')):
        tls[label]=write_private(root/name,material['configurationFiles'][name])
    runtime=write_private(root/'final-runtime.json',canonical(material['final']))
    # Local native fixture accepts the protocol token; the value is never
    # sent to the coordinator or embedded in the public review artifact.
    native=write_private(root/'native.env',b'FIXTURE_NATIVE_TOKEN=synthetic-fixture-native-only\n')
    output=root/'prepared'
    manifest=dict(schemaVersion=1,protocol=install.PROTOCOL,operationId=entry['operationId'],
        host=dict(hostname=socket.gethostname(),bootId=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                  dockerEngineId=engine),
        service=dict(name='fixture',uid=os.geteuid(),gid=os.getegid(),home=str(home)),installationRoot=entry['root'],
        source=layout['source'],dependencies=layout['dependencies'],
        config=dict(path=str(output/'worker-config.json'),sha256='0'*64,bytes=1,ownerUid=os.geteuid()),
        unitSha256='0'*64,gate={key:gate[key] for key in ('path','sha256','nodeId','authorityId','generation','coordinatorOrigin')},
        engine=dict(containerId=controller_id,imageId=layout['candidateImageId'],version='3.8.5',
                    loopbackPort=urlsplit(backend.origin).port,catalogSha256=digest(backend.fixture_options)))
    manifest['unitSha256']=hashlib.sha256(install.render_unit(manifest).encode()).hexdigest()
    spec=dict(schemaVersion=1,protocol=prep.PROTOCOL,manifest=manifest,runtime=runtime,
        nativeCredential=dict(artifact=native,key='FIXTURE_NATIVE_TOKEN'),tls=tls,
        coordinator=dict(origin='https://'+host+':8443',listenerPort=8443,certificate=tls['certificate']),
        listener=dict(host=host,port=port),policy=dict(safetyFloor=0,transferTtl=600,cleanupGrace=3600,
            pollSeconds=0.1,allowedEnvironments=['production','staging'],cleanupEnabled=False,
            engineCleanupEnabled=False,sourceRangeV2Enabled=True,minimumTlsValiditySeconds=3600),outputDirectory=str(output))
    receipt=prep.prepare(spec)
    completed=decode(private_bytes(output/'enrollment-manifest.json',maximum=65536));install.validate_manifest(completed)
    raw=private_bytes(output/'worker-config.json',maximum=65536);config=decode(raw)
    stable=home/'.config/ledgetop-processing/reference-worker.json';write_private(stable,raw)
    unit=home/'.config/systemd/user'/install.UNIT;write_private(unit,install.render_unit(completed).encode())
    receipt_path=root/'preparation-receipt.json';write_private(receipt_path,canonical(receipt))
    expected=dict(nodeId=node,workerId=config['workerId'],workerInstanceId=config['workerInstanceId'],authorityId=authority,
        engineId=engine,gateGeneration=config['gateGeneration'],engineVersion=config['engineVersion'],
        configSha256=hashlib.sha256(raw).hexdigest(),sourceSha256=completed['source']['footprintSha256'],activation=None)
    planned=dict(nodeId=node,sshHost='127.0.0.1',sshUid=os.geteuid(),settings=dict(schemaVersion=1,
        unitName=install.UNIT,unitFile=str(unit),unitSha256=completed['unitSha256'],
        pythonExecutable=str(Path(entry['root'])/'venv/bin/python'),configPath=str(stable),
        sourceRoot=str(Path(entry['root'])/'source'),sourceSha256=expected['sourceSha256'],
        readinessOrigin=worker['resultOrigin'],readinessCAFile=config['caFile']),expected=expected)
    review=dict(nodeId=node,workerReferenceSha256=digest(planned),manifest=reference(output/'enrollment-manifest.json'),
        preparationReceipt=reference(receipt_path),enrollment=dict(kind='install-intent',operationId=completed['operationId'],
                                                               manifestSha256=digest(completed)))
    return dict(home=home,entry=entry,path=stable,config=config,manifest=planned,review=review,root=root)


def spawn_worker(instance,authority):
    entry=instance['entry'];source=str(Path(entry['root'])/'source');python=str(Path(entry['root'])/'venv/bin/python')
    body='import sys;sys.path.insert(0,'+repr(source)+');from tests.fixtures.enrolled_worker_factory_process import main;main("worker")'
    log=instance['root']/'worker-private.log';fd=os.open(log,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    try:
        process=subprocess.Popen([python,'-I','-B','-c',body],stdin=subprocess.PIPE,stdout=fd,stderr=fd,
            env=clean_environment(instance['home']),cwd=source,start_new_session=True)
    finally:os.close(fd)
    instance['process']=process;instance['startTime']=process_identity(process.pid);instance['log']=log
    write_private(instance['root']/'factory-child-start.json',canonical(dict(schemaVersion=1,
        pid=process.pid,startTime=instance['startTime'],parentPid=os.getpid(),pythonExecutable=python,
        configSha256=instance['manifest']['expected']['configSha256'],
        sourceSha256=instance['manifest']['expected']['sourceSha256'],scope='local-fixture-process')))
    process.stdin.write(canonical(dict(configPath=str(instance['path']),authorityId=authority,
                                      engineId=instance['config']['engineId'])))
    process.stdin.close()


def readiness(instance,planned):
    require(planned==instance['manifest'],'planned_worker_fixture_changed')
    process=instance['process'];require(process.poll() is None and process_identity(process.pid)==instance['startTime'],
                                       'owned_factory_worker_changed')
    require(instance['log'].stat().st_size<=262144,'bounded_private_worker_log_required')
    deadline=time.monotonic()+10
    while True:
        value=invoke(instance['entry'],'probe',dict(worker=planned,challenge=str(uuid.uuid4()),
            pid=process.pid,startTime=instance['startTime']),instance['home'])
        if value.get('pending') is not True:return value
        require(time.monotonic()<deadline,'actual_prepared_worker_unready');time.sleep(.02)


def stop_worker(instance):
    process=instance.get('process')
    if process is None:return
    if process.poll() is None:
        require(process_identity(process.pid)==instance['startTime'],'owned_worker_process_identity_changed')
        process.send_signal(signal.SIGTERM)
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:raise RuntimeError('factory_worker_shutdown_uncertain') from None
    # Record the reaped child's exact result even when the success guard refuses it.
    write_private(instance['root']/'factory-child-exit.json',canonical(dict(schemaVersion=1,
        pid=process.pid,startTime=instance['startTime'],exitCode=process.returncode,retired=True,
        scope='local-fixture-process')))
    require(process.returncode==0,'factory_worker_failed_before_shutdown')
    require(instance['log'].stat().st_size<=262144,'bounded_private_worker_log_required')

