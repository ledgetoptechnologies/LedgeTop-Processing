"""UNREVIEWED synthetic two-container runner; DO NOT EXECUTE before review."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import time

source=Path(__file__).resolve().parents[1]
viewer=Path(r'C:\Projects\viewer-download-retention-20261031')
viewer_commit='f13c4e0465640516305aa3f9eeb1ad0735254bb9'
processing_commit_expected='4020f922990bca17598390868a8d378e80615154'
fixture_commit='a174b8ae1b00e9f3bb4ab71f517bc80c2fe539ba'
fixture_hashes={
    'worker_process_outage_fixture.py':'50b68420d6a20e9171f505274c27000fc7a0253c6793605f1d13275581700549',
    'worker_process_outage_engine.py':'f7f3fbc6f3ff0af14c1ef4fad1c6343e4500a09681636e627e028c5e0035903e',
    'joined_worker_viewer_coordinator.py':'8a79a07be92ef4eb637ecb131e6bad086e69832dd38608cd76c41c5999b4a432',
    'joined_worker_viewer_supervisor.py':'6ddda9b59f07019e0a13fa893f27c7cdd2055b5c50a830c16ca85492f5bfa19a',
    'joined_worker_viewer_watchdog.py':'8ae1289fdd548c873657beba4569d3623d8179498c84dcfcb083469f5cb7157a',
    'joined_worker_viewer_sender.js':'dc9c7e374e4664d3ffeaf6037d8d0dd134f0ec4715e10a8d7424c5cdb63cfa2c',
}
run_id=os.environ['LT_SYNTHETIC_RUN_ID'];root=Path(os.environ['LT_SYNTHETIC_ROOT']).resolve()
if len(run_id)!=32 or any(c not in '0123456789abcdef' for c in run_id) or not root.is_dir():raise ValueError('invalid run identity')
label='ledgetop.synthetic.rehearsal'
names={'python':'synthetic-joined-python-'+run_id,'viewer':'synthetic-joined-viewer-'+run_id}
owned={}

def cmd(*args,timeout=8,stdout=subprocess.PIPE):
    return subprocess.run(args,cwd=source,check=True,timeout=timeout,stdout=stdout,stderr=subprocess.PIPE).stdout
def docker(*args,timeout=8):return cmd('docker',*args,timeout=timeout).decode().strip()
def inspect(role,reference):
    value=json.loads(docker('inspect',reference))[0]
    if (value['Name'].lstrip('/')!=names[role] or value['Config']['Labels'].get(label)!=run_id
        or len(value['Id'])!=64 or (role in owned and value['Id']!=owned[role])):raise RuntimeError('identity mismatch')
    return value
def create(role,*args):
    try: ident=docker('create','--name',names[role],'--label',f'{label}={run_id}',*args)
    except (subprocess.TimeoutExpired,subprocess.CalledProcessError):
        try:
            ident=inspect(role,names[role])['Id'];owned[role]=ident;print('RECONCILED_CREATE',role,ident,flush=True)
        except Exception as error:print('UNCERTAIN_CREATE',role,type(error).__name__,flush=True)
        raise
    if len(ident)!=64:raise RuntimeError('invalid container id')
    owned[role]=ident;inspect(role,ident);print('OWNED_CONTAINER',role,ident,flush=True);return ident
def exited(role):
    return inspect(role,owned[role])['State']
def logs(role):
    result=subprocess.run(['docker','logs',owned[role]],cwd=source,check=True,timeout=8,
                          stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    return result.stdout.decode('utf-8','replace')
def stop(role):
    status=exited(role)
    if status['Running']:
        docker('kill','--signal','KILL',owned[role]);until=time.monotonic()+8
        while time.monotonic()<until:
            status=exited(role)
            if not status['Running']:break
            time.sleep(.2)
    if status['Running']:raise RuntimeError('container still running')
    print('OWNED_CONTAINER_EXIT',role,owned[role],status['ExitCode'],flush=True)

try:
    processing_commit=cmd('git','rev-parse',processing_commit_expected+'^{commit}').decode().strip()
    if processing_commit!=processing_commit_expected:raise RuntimeError('unexpected Processing commit')
    fixture_oid=cmd('git','rev-parse',fixture_commit+'^{commit}').decode().strip()
    if fixture_oid!=fixture_commit:raise RuntimeError('invalid fixture commit')
    print('COMMITS',processing_commit,viewer_commit,'FIXTURE_COMMIT',fixture_oid,'RUN_ID',run_id,'ROOT',root,flush=True)
    for repo,commit,name in ((source,processing_commit,'processing'),(viewer,viewer_commit,'viewer')):
        target=root/name;target.mkdir();archive=root/(name+'.tar')
        with archive.open('wb') as output:cmd('git','-C',str(repo),'archive','--format=tar',commit,timeout=30,stdout=output)
        with tarfile.open(archive) as tar:tar.extractall(target,filter='data')
    for name,expected in fixture_hashes.items():
        data=cmd('git','show',fixture_oid+':tests/'+name)
        # Git normalized these files at commit time. Reconstruct the exact
        # reviewed/passing Windows snapshot bytes, then verify their digest.
        if name in ('worker_process_outage_fixture.py','worker_process_outage_engine.py'):
            data=data.replace(b'\n',b'\r\n')
        elif name=='joined_worker_viewer_watchdog.py':
            if not data.endswith(b'\n'):raise RuntimeError('unexpected fixture line ending')
            data=data[:-1]+b'\r\n'
        if hashlib.sha256(data).hexdigest()!=expected:raise RuntimeError('fixture committed hash mismatch')
        target=root/'processing'/'tests'/name;target.write_bytes(data)
        digest=hashlib.sha256(data).hexdigest()
        if hashlib.sha256(target.read_bytes()).hexdigest()!=digest:raise RuntimeError('fixture copy mismatch')
        print('FIXTURE_SHA256',name,digest,flush=True)
    for name in ('nativeReferenceConsumerCandidate.js','nativeReferenceStorageCandidate.js','nativeReferenceObjectCandidate.js',
                 'nativeReferenceReceiptCandidate.js','nativeReferenceBoundSenderCandidate.js','nativeReferenceAcceptanceCandidate.js',
                 'nativeReferenceProviderAuthCandidate.js','nativeReferenceSenderRevisionCandidate.js','nativeReferenceProtocolCandidate.js'):
        print('VIEWER_MODULE_SHA256',name,hashlib.sha256((root/'viewer'/'apps'/'viewer'/'server'/name).read_bytes()).hexdigest(),flush=True)
    retained=root/'retained';retained.mkdir()
    images={}
    for tag in ('python:3.12-slim','node:24-bookworm-slim'):
        image=docker('image','inspect','--format','{{.Id}}',tag)
        if not image.startswith('sha256:') or len(image)!=71:raise RuntimeError('invalid image')
        images[tag]=image;print('IMAGE_ID',tag,image,flush=True)
    common=['--init','--pids-limit','48','--memory','512m','--cpus','1','--cap-drop','ALL','--security-opt','no-new-privileges']
    binds=['--mount',f'type=bind,src={root / "processing"},dst=/processing,readonly',
           '--mount',f'type=bind,src={root / "viewer"},dst=/viewer,readonly',
           '--mount',f'type=bind,src={retained},dst=/retained']
    p=create('python','--network','none',*common,*binds,'--workdir','/processing',
             '--env','SYNTHETIC_RETAINED_DIR=/retained',images['python:3.12-slim'],
             'python','-u','-m','tests.joined_worker_viewer_watchdog')
    docker('start',p)
    marker=retained/'joined-root.json';deadline=time.monotonic()+100
    while time.monotonic()<deadline:
        status=exited('python')
        if not status['Running']:raise RuntimeError('python container exited before Viewer readiness')
        if marker.is_file():break
        time.sleep(.2)
    else:raise RuntimeError('Viewer readiness timeout')
    ready=json.loads(marker.read_text());joined=ready['root']
    if not isinstance(joined,str) or not joined.startswith('/retained/coordinator-process-') or '/' in joined[len('/retained/'):]:
        raise RuntimeError('invalid synthetic marker root')
    v=create('viewer','--network','container:'+p,*common,*binds,'--workdir','/processing',
             '--env','JOINED_ROOT='+joined,images['node:24-bookworm-slim'],'node','tests/joined_worker_viewer_sender.js')
    docker('start',v);deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        vs=exited('viewer')
        if not vs['Running']:break
        if not exited('python')['Running']:raise RuntimeError('python exited during Viewer run')
        time.sleep(.2)
    else:raise RuntimeError('Viewer container timeout')
    vlog=logs('viewer');print('VIEWER_LOG_BEGIN\n'+vlog+'\nVIEWER_LOG_END',flush=True)
    if vs['ExitCode']!=0 or 'VIEWER_ACTUAL_PROXY_STAGED_AND_SENT' not in vlog:raise RuntimeError('Viewer did not stage/send')
    deadline=time.monotonic()+20
    while time.monotonic()<deadline:
        ps=exited('python')
        if not ps['Running']:break
        time.sleep(.2)
    else:raise RuntimeError('python cleanup timeout')
    plog=logs('python');print('PYTHON_LOG_BEGIN\n'+plog+'\nPYTHON_LOG_END',flush=True)
    if ps['ExitCode']!=0 or 'WORKER_VIEWER_JOINED_ASSERTIONS_COMPLETE' not in plog or 'INNER_PROCESS_GROUP_CHECK_PASSED' not in plog:
        raise RuntimeError('joined coordinator assertions incomplete')
    print('BOTH_CONTAINER_EXITS_VERIFIED',p,v,flush=True)
finally:
    failures=[]
    for role in ('viewer','python'):
        if role in owned:
            try:stop(role)
            except Exception as error:
                failures.append(role);print('OWNED_CONTAINER_UNCERTAIN',role,owned[role],type(error).__name__,flush=True)
    if failures:raise RuntimeError('owned container state uncertain')
