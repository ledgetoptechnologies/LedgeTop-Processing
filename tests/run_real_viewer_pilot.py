"""Supervised single existing-coordinator pilot; no cleanup or automatic retry.

Review candidate. The user-selected photos are copied; originals never change.
Each host-local registry is one-shot and retains its record even on failure.
An interrupted supervisor never stops or removes its container or node task.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

from advisory_live_pilot_preflight import REMOTE, converter_status, sample_size, source_status
from prepare_pilot_input import prepare
from run_real_viewer_pilot_fake import IMAGE

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


def remote_snapshot(input_bytes):
    result=subprocess.run(SSH+['python3','-'],input=REMOTE,text=True,capture_output=True,timeout=30,check=True)
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
    parent=checked_dir(a.parent)
    if not source_status(viewer) or not converter_status(converter):raise RuntimeError('source_or_converter_mismatch')
    if subprocess.run(['docker','image','inspect',IMAGE],capture_output=True,timeout=15).returncode:
        raise RuntimeError('pinned_runtime_missing')
    if any(p==photos or photos in p.parents or p in photos.parents for p in (registry,parent)):
        raise RuntimeError('test_storage_overlaps_source')
    size=sample_size(photos);remote_snapshot(size)
    # Launcher marker persists on success, failure, crash and uncertain create.
    # This guards only this exact host registry, not other submitters/hosts.
    owner=uuid.uuid4().hex;name='ltds-real-pilot-'+owner
    marker=registry/'launcher.json'
    exclusive_json(marker,{'schemaVersion':1,'owner':owner,'containerName':name,'state':'claimed'})
    data=parent/('pilot-'+owner)
    prepared=prepare(photos,data,25)
    exclusive_json(data/'launch-intent.json',{'schemaVersion':1,'owner':owner,
        'sampleOwner':prepared['owner'],'containerName':name,'runtimeImage':IMAGE})
    # Repeat immediately before creation; this is a snapshot, not a reservation.
    remote_snapshot(prepared['bytes']);token=read_staging_token()
    script=('mkdir /tmp/deps && cp /viewer/apps/viewer/package.json /viewer/apps/viewer/package-lock.json /tmp/deps/ '
        '&& npm ci --prefix /tmp/deps --omit=dev --offline --ignore-scripts --no-audit --no-fund --cache /npm-cache --logs-max=0 '
        '&& mkdir /tmp/deps/apps && cp -R /viewer/apps/viewer /tmp/deps/apps/viewer '
        '&& exec node tests/real_viewer_pilot.js')
    cmd=['docker','create','--interactive','--name',name,'--label','ltds.pilot.owner='+owner,
        '--network','bridge','--read-only','--memory','1536m','--cpus','1','--pids-limit','96',
        '--tmpfs','/tmp:rw,nosuid,nodev,size=512m,mode=1777','--user','10001:10001','--workdir','/processing']
    for src,dst,ro in ((processing,'/processing',True),(viewer,'/viewer',True),(cache,'/npm-cache',True),
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
    if not (data/'pilot-complete.json').is_file():raise RuntimeError('receipt_missing_preserve_all')
    print(json.dumps({'status':'ready_for_review','ownedPilot':str(data),'containerName':name}))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('processing','viewer','cache','converter','photos','registry','parent'):
        p.add_argument('--'+name,type=Path,required=True)
    try:launch(p.parse_args())
    except Exception as exc:
        # Avoid propagating subprocess stderr, provider addresses, client paths or tokens.
        print(json.dumps({'status':'held','reason':str(exc) if str(exc).isidentifier() else 'inspect_owned_state'}),file=sys.stderr)
        raise SystemExit(1)


if __name__=='__main__':main()
