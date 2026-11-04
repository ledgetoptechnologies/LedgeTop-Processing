"""Run the pilot against a fake coordinator in one network-none container."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
from pilot_container_guard import stopped_pilot

IMAGE = 'sha256:e7add2a490bb17d97b485b8641bd88455fff7839ade0f5eba493c3508124d413'
CONVERTER = {'Obj2Tiles': '3577688939806dabdf1f4620cc05debe6e178180a99bf0308f4b8483b8a36725',
 'libktx.so': '0792dd32ce7d2a101d614a3d1dc1f2b4297869d9cab5d7f0bb4fee304c4c9bcc',
 'build-info.json': '2c8e7e4184ccff6d5e08290ba70645c353a5d1f18b53ec7b14f1a52a8e4e8f2d'}


def main():
    parser=argparse.ArgumentParser()
    for name in ('processing','viewer','cache','converter','data'):
        parser.add_argument('--'+name,type=Path,required=True)
    a=parser.parse_args()
    if any(not p.is_absolute() or not p.is_dir() for p in (a.processing,a.viewer,a.cache,a.converter)):
        raise ValueError('existing absolute input directories required')
    if not a.data.is_absolute() or a.data.exists() or a.data.is_symlink() or not a.data.parent.is_dir():
        raise ValueError('new absolute data directory required')
    for name,digest in CONVERTER.items():
        if hashlib.sha256((a.converter/name).read_bytes()).hexdigest()!=digest:raise ValueError('converter digest mismatch')
    a.data.mkdir(mode=0o700)
    (a.data/'registry').mkdir(mode=0o700)
    owner=uuid.uuid4().hex; name='ltds-pilot-fake-'+owner
    script=('mkdir /tmp/deps && cp /viewer/apps/viewer/package.json /viewer/apps/viewer/package-lock.json /tmp/deps/ '
        '&& npm ci --prefix /tmp/deps --omit=dev --offline --ignore-scripts --no-audit --no-fund --cache /npm-cache --logs-max=0 '
        '&& mkdir /tmp/deps/apps && cp -R /viewer/apps/viewer /tmp/deps/apps/viewer '
        '&& exec node tests/real_viewer_pilot_fake_runner.js')
    cmd=['docker','create','--name',name,'--label','ltds.fixture.owner='+owner,'--network','none','--read-only',
        '--memory','1536m','--cpus','1','--pids-limit','96','--tmpfs','/tmp:rw,nosuid,nodev,size=512m,mode=1777',
        '--user','10001:10001','--workdir','/processing']
    for source,target,readonly in ((a.processing,'/processing',True),(a.viewer,'/viewer',True),(a.cache,'/npm-cache',True),
        (a.converter,'/opt/obj2tiles',True),(a.data,'/pilot',False),(a.data/'registry','/pilot-registry',False)):
        cmd+=['--mount',f'type=bind,src={source},dst={target}'+(',readonly' if readonly else '')]
    cmd+=['--env','NODE_PATH=/tmp/deps/node_modules','--env','LD_LIBRARY_PATH=/opt/obj2tiles','--entrypoint','sh',IMAGE,'-c',script]
    expected=None
    try:
        created=subprocess.run(cmd,capture_output=True,text=True,timeout=30,check=True).stdout.strip()
    except (subprocess.TimeoutExpired,subprocess.CalledProcessError):
        print('Ambiguous create: inspect exact owned name',name,file=sys.stderr);raise
    def inspect():
        item=json.loads(subprocess.check_output(['docker','inspect',created],text=True,timeout=15))[0]
        if (item['Id']!=created or item['Name']!='/'+name or item['Image']!=IMAGE
            or item['Config']['Labels'].get('ltds.fixture.owner')!=owner):raise RuntimeError('container identity changed')
        return item
    try:
        item=inspect()
        mounts=[mount for mount in item['Mounts'] if mount['Destination']=='/pilot']
        if len(mounts)!=1 or os.path.normcase(os.path.normpath(mounts[0]['Source']))!=os.path.normcase(os.path.normpath(str(a.data))):
            raise RuntimeError('pilot mount identity cannot be verified')
        expected={'id':created,'name':name,'image':IMAGE,'owner':owner,'mountSource':mounts[0]['Source']}
        # Record the exact supervised container before it can write the pilot.
        # This is a fake fixture only; it does not establish a host-wide fence.
        fd=os.open(a.data/'container-owner.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        try:
            raw=json.dumps(expected,sort_keys=True).encode('utf-8')
            if os.write(fd,raw)!=len(raw):raise RuntimeError('short pilot owner receipt write')
            os.fsync(fd)
        finally:os.close(fd)
        if os.name=='posix':
            fd=os.open(a.data,os.O_RDONLY|os.O_DIRECTORY)
            try:os.fsync(fd)
            finally:os.close(fd)
        result=subprocess.run(['docker','start','-a',created],capture_output=True,text=True,timeout=180)
        print(result.stdout[-4000:]);print(result.stderr[-4000:],file=sys.stderr)
        if result.returncode:raise RuntimeError('fake pilot failed')
    finally:
        item=inspect()
        if item['State']['Running']:
            subprocess.run(['docker','stop','--time','3',created],capture_output=True,timeout=15,check=True)
        # If the owner receipt was created, refuse to remove the fixture unless
        # the same container and bind mount are observed stopped. This does not
        # establish that another host process cannot write the same directory.
        if expected is not None:stopped_pilot(inspect(),expected)
        else:inspect()
        subprocess.run(['docker','rm',created],capture_output=True,timeout=15,check=True)


if __name__=='__main__': main()
