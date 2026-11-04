"""Supervised single existing-coordinator pilot; no cleanup or automatic retry.

Review candidate. The user-selected photos are copied; originals never change.
Each host-local registry is one-shot and retains its record even on failure.
An interrupted supervisor never stops or removes its container or node task.
"""
import argparse
import base64
import hashlib
import types
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

PINNED_PROCESSING='625eb26132fd0042de490b63b9afc240d284f8c8'
PINNED_VIEWER='ad0bf46611de44545825db9147e90b68195902ab'
IMAGE='sha256:e7add2a490bb17d97b485b8641bd88455fff7839ade0f5eba493c3508124d413'
FIXED_REGISTRY=(Path(r'C:\Users\fstor\.codex\ledgetop-processing-pilot-registry') if os.name=='nt'
                else Path('/home/bkoltz/.local/share/ledgetop-processing-pilot-registry'))
PINNED_FILES={
 'tests/real_viewer_pilot.js':'00d95a5b5ac8a07c133a0e63247a675e31b65b3ce86e015b97f33671360f6b11',
 'tests/advisory_live_pilot_preflight.py':'cf7a939c34a799d50cae90ea27c3bbcf910e5bf10e8586973c490d8b8b4df3fe',
 'tests/prepare_pilot_input.py':'aa91fd69b70dc92fa8fc67f64e8f90bb4da169dc1f6e3811d79b4ba867ab9f1a',
}


def load_pinned(processing):
    blobs={}
    for name,digest in PINNED_FILES.items():
        raw=subprocess.check_output(['git','show',PINNED_PROCESSING+':'+name],cwd=processing,timeout=15)
        if hashlib.sha256(raw).hexdigest()!=digest:raise RuntimeError('processing_source_mismatch')
        blobs[name]=raw
    result={}
    for name,key in (('tests/advisory_live_pilot_preflight.py','preflight'),('tests/prepare_pilot_input.py','input')):
        module=types.ModuleType('_pinned_'+key)
        exec(compile(blobs[name],name,'exec'),module.__dict__)
        result[key]=module
    result['worker']=blobs['tests/real_viewer_pilot.js']
    return result


def verify_registry_acl(registry):
    if registry!=FIXED_REGISTRY:raise RuntimeError('fixed_registry_required')
    if os.name=='posix':
        st=registry.lstat()
        if st.st_uid!=os.geteuid() or st.st_mode & 0o077:raise RuntimeError('registry_acl_unverified')
        return
    if os.name!='nt':raise RuntimeError('registry_acl_unverified')
    # No ACL changes are made. Only the current user, SYSTEM and local admins
    # may have ACEs; inherited permissions are not accepted for the registry.
    path=str(FIXED_REGISTRY).replace("'","''")
    script=("$ErrorActionPreference='Stop'; $a=Get-Acl -LiteralPath '"+path+"'; "
       "$me=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value; "
       "$owner=([Security.Principal.NTAccount]$a.Owner).Translate([Security.Principal.SecurityIdentifier]).Value; "
       "$ok=$a.AreAccessRulesProtected -and $owner -eq $me; $mine=$false; "
       "foreach($r in $a.Access){$sid=$r.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value; "
       "if($sid -notin @($me,'S-1-5-18','S-1-5-32-544')){$ok=$false}; "
       "if($sid -eq $me -and $r.AccessControlType -eq 'Allow' -and "
       "(($r.FileSystemRights -band [Security.AccessControl.FileSystemRights]::FullControl) -eq "
       "[Security.AccessControl.FileSystemRights]::FullControl)){$mine=$true}}; "
       "if($ok -and $mine){exit 0}else{exit 1}")
    if subprocess.run(['pwsh','-NoProfile','-NonInteractive','-Command',script],
                      capture_output=True,timeout=15).returncode:raise RuntimeError('registry_acl_unverified')

# Reads the already configured staging credential remotely and returns it only
# to the local launcher over authenticated SSH. Never put the token in argv,
# environment, receipts, or application logs. The remote process checks its binding.
TOKEN_READER = r'''import json,pathlib,subprocess,sys
try:
 c=json.loads(subprocess.check_output(['docker','inspect','ledgetop-processing-prototype-c3fcb3a'],text=True,timeout=8))[0]
 env=dict(x.split('=',1) for x in c['Config']['Env'] if '=' in x)
 p=pathlib.Path('/home/bkoltz/.local/share/ledgetop-processing-prototype/viewer-ba01f37')
 stage=(p/'viewer-staging.token').read_text().strip(); prod=(p/'viewer-production.token').read_text().strip()
 if not stage or stage!=env.get('PROCESSING_VIEWER_STAGING_TOKEN') or not prod or prod!=env.get('PROCESSING_VIEWER_TOKEN') or stage==prod: raise ValueError()
 if len(stage.splitlines())!=1 or len(stage)<16 or len(stage)>1024: raise ValueError()
 sys.stdout.write(stage)
except Exception:
 sys.exit(1)
'''

SSH=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','-o','StrictHostKeyChecking=yes','bkoltz@192.168.50.89']


def exclusive_json(path, value):
    flags=os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0)
    fd=os.open(path,flags,0o600)
    try:
        with os.fdopen(fd,'w',encoding='utf-8',closefd=False) as f:
            json.dump(value,f,separators=(',',':'));f.flush();os.fsync(f.fileno())
    finally: os.close(fd)
    # Windows does not permit opening directories for fsync; report that limit
    # rather than asserting power-loss durability there.
    if os.name=='posix':
        fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(fd)
        finally:os.close(fd)


def checked_dir(path):
    path=Path(path)
    if not path.is_absolute() or path.resolve()!=path or not path.is_dir():
        raise ValueError('existing real absolute directory required')
    return path


def remote_snapshot(input_bytes, remote_script):
    result=subprocess.run(SSH+['python3','-'],input=remote_script,text=True,capture_output=True,timeout=30,check=True)
    data=json.loads(result.stdout)
    bools=('remoteOk','coordinatorIdentity','nodeIdentity','stagingBinding','providerExpected','optionsAvailable','nodeExpected','engineOnlyProcess')
    if any(data.get(k) is not True for k in bools):raise RuntimeError('remote_identity_or_binding_unverified')
    if any(type(data.get(k)) is not int or data[k]!=0 for k in ('providerQueueCount','nodeQueueCount','activeJobCount')):
        raise RuntimeError('remote_not_idle')
    if type(data.get('inputLimitBytes')) is not int or input_bytes>data['inputLimitBytes']:
        raise RuntimeError('input_limit_exceeded')
    # A conservative lower bound only; processing peak cannot be predicted.
    floor=max(20*1024**3,input_bytes*4)
    if type(data.get('clusterFreeBytes')) is not int or data['clusterFreeBytes']<floor:
        raise RuntimeError('cluster_space_insufficient')
    return data


def write_viewer_archive(viewer, archive):
    with archive.open('xb') as out:
        subprocess.run(['git','archive','--format=tar',PINNED_VIEWER,'apps/viewer'],cwd=viewer,stdout=out,timeout=60,check=True)
        out.flush();os.fsync(out.fileno())


def read_staging_token():
    # stdout is held only in this subprocess result and is never printed.
    result=subprocess.run(SSH+['python3','-'],input=TOKEN_READER,capture_output=True,text=True,timeout=20,check=True)
    token=result.stdout
    if not 16<=len(token)<=1024 or any(x in token for x in '\r\n\0'):
        raise RuntimeError('staging_credential_unavailable')
    return token


def launch(a):
    processing=checked_dir(a.processing);viewer=checked_dir(a.viewer);cache=checked_dir(a.cache)
    converter=checked_dir(a.converter);photos=checked_dir(a.photos);registry=checked_dir(a.registry)
    verify_registry_acl(registry)
    pinned=load_pinned(processing);preflight=pinned['preflight'];prepare=pinned['input'].prepare
    if not preflight.source_status(viewer) or not preflight.converter_status(converter):raise RuntimeError('source_or_converter_mismatch')
    if subprocess.run(['docker','image','inspect',IMAGE],capture_output=True,timeout=15).returncode:
        raise RuntimeError('pinned_runtime_missing')
    if registry==photos or photos in registry.parents or registry in photos.parents:
        raise RuntimeError('test_storage_overlaps_source')
    size=preflight.sample_size(photos);remote_snapshot(size,preflight.REMOTE)
    # Launcher marker persists on success, failure, crash and uncertain create.
    # This guards only this exact host registry, not other submitters/hosts.
    owner=uuid.uuid4().hex;name='ltds-real-pilot-'+owner
    marker=registry/'launcher.json'
    exclusive_json(marker,{'schemaVersion':1,'owner':owner,'containerName':name,'state':'claimed'})
    data=registry/('pilot-'+owner)
    prepared=prepare(photos,data,25)
    exclusive_json(data/'launch-intent.json',{'schemaVersion':1,'owner':owner,
        'sampleOwner':prepared['owner'],'containerName':name,'runtimeImage':IMAGE})
    # Repeat immediately before creation; this is a snapshot, not a reservation.
    remote_snapshot(prepared['bytes'],preflight.REMOTE);token=read_staging_token()
    # Archive exact tracked Viewer commit. Ignored node_modules and local files
    # are excluded and cannot override the npm ci lockfile installation.
    archive=data/'viewer-source.tar'
    write_viewer_archive(viewer, archive)
    worker=base64.b64encode(pinned['worker']).decode('ascii')
    script=('mkdir /tmp/deps && tar -xf /viewer-source.tar -C /tmp/deps '
        '&& cp /tmp/deps/apps/viewer/package.json /tmp/deps/apps/viewer/package-lock.json /tmp/deps/ '
        '&& npm ci --prefix /tmp/deps --omit=dev --offline --ignore-scripts --no-audit --no-fund --cache /npm-cache --logs-max=0 '
        '&& printf %s '+worker+' | base64 -d > /tmp/pilot-worker.js '
        '&& exec node /tmp/pilot-worker.js')
    cmd=['docker','create','--interactive','--name',name,'--label','ltds.pilot.owner='+owner,
        '--network','bridge','--read-only','--memory','1536m','--cpus','1','--pids-limit','96',
        '--tmpfs','/tmp:rw,nosuid,nodev,size=512m,mode=1777','--user','10001:10001','--workdir','/tmp']
    for src,dst,ro in ((archive,'/viewer-source.tar',True),(cache,'/npm-cache',True),
        (converter,'/opt/obj2tiles',True),(data,'/pilot',False),(registry,'/pilot-registry',False)):
        cmd+=['--mount',f'type=bind,src={src},dst={dst}'+(',readonly' if ro else '')]
    for value in ('NODE_PATH=/tmp/deps/node_modules','LD_LIBRARY_PATH=/opt/obj2tiles','VIEWER_SOURCE=/tmp/deps',
                  'PILOT_MODE=live','PILOT_REVIEWED_LAUNCHER=1','PILOT_ROOT=/pilot',
                  'PILOT_REGISTRY=/pilot-registry','PILOT_ENDPOINT=http://192.168.50.89:3005',
                  'PILOT_DEADLINE_SECONDS=7200'):
        cmd+=['--env',value]
    cmd+=['--entrypoint','sh',IMAGE,'-c',script]
    try: created=subprocess.run(cmd,capture_output=True,text=True,timeout=30,check=True).stdout.strip()
    except (subprocess.TimeoutExpired,subprocess.CalledProcessError):
        raise RuntimeError('container_create_ambiguous_preserve_all') from None
    exclusive_json(data/'container.json',{'schemaVersion':1,'owner':owner,'name':name,'id':created})
    inspect=json.loads(subprocess.check_output(['docker','inspect',created],text=True,timeout=15))[0]
    if (inspect['Id']!=created or inspect['Name']!='/'+name or inspect['Image']!=IMAGE
       or (inspect['Config'].get('Labels') or {}).get('ltds.pilot.owner')!=owner):
        raise RuntimeError('container_identity_mismatch_preserve_all')
    # Docker attach may be interrupted independently of daemon/container.
    # Never auto-stop, remove, retry or delete state, even on success.
    try:
        result=subprocess.run(['docker','start','--attach','--interactive',created],input=token+'\n',
            text=True,capture_output=True,timeout=6*3600)
    except subprocess.TimeoutExpired:
        raise RuntimeError('monitor_interrupted_preserve_all') from None
    finally: token=None
    if result.returncode:raise RuntimeError('pilot_failed_preserve_all')
    # The container's exit status alone is not proof of a successful result.
    # Require the worker's exclusive pre-submit receipt and completion proof to
    # identify the same owned attempt. The worker verified content hashes.
    try:
        receipt_path=data/'pilot-receipt.json'; complete_path=data/'pilot-complete.json'
        if any(p.is_symlink() or not p.is_file() for p in (receipt_path,complete_path)): raise ValueError()
        receipt=json.loads(receipt_path.read_text(encoding='utf-8')); complete=json.loads(complete_path.read_text(encoding='utf-8'))
        if (receipt.get('owner')!=prepared['owner'] or complete.get('owner')!=prepared['owner']
            or not receipt.get('attemptId') or receipt['attemptId']!=complete.get('attemptId')
            or not receipt.get('providerTaskId') or receipt['providerTaskId']!=complete.get('providerTaskId')
            or type(complete.get('files')) is not int or complete['files']<1
            or any(not isinstance(complete.get(k),str) or len(complete[k])!=64
                   or any(c not in '0123456789abcdef' for c in complete[k])
                   for k in ('archiveManifestSha256','tilesSha256'))): raise ValueError()
    except (OSError,ValueError,TypeError,KeyError,json.JSONDecodeError):
        raise RuntimeError('receipt_missing_or_mismatch_preserve_all') from None
    print(json.dumps({'status':'ready_for_review','ownedPilot':str(data),'containerName':name}))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('processing','viewer','cache','converter','photos','registry'):
        p.add_argument('--'+name,type=Path,required=True)
    try:launch(p.parse_args())
    except Exception as exc:
        # Avoid propagating subprocess stderr, provider addresses, client paths or tokens.
        print(json.dumps({'status':'held','reason':str(exc) if str(exc).isidentifier() else 'inspect_owned_state'}),file=sys.stderr)
        raise SystemExit(1)


if __name__=='__main__':main()
