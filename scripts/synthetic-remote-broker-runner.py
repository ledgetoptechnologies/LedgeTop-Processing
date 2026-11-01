"""REVIEW ONLY: synthetic broker namespace Docker launcher; do not run yet.

Inputs must be a reviewed immutable source snapshot and separately staged
synthetic server/client TLS directories. Creates labelled resources, never
removes them. Exact-ID verification precedes every task-owned container stop.
"""
import argparse
import hashlib
import importlib.util
import os
from pathlib import Path
import re
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent
SPEC=importlib.util.spec_from_file_location('namespace_rehearsal',HERE/'node-namespace-rehearsal.py')
base=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(base)
LABEL=base.LABEL


def volume_info(name):
    fmt='{{json .Name}}|{{json (index .Labels "'+LABEL+'")}}'
    fields=base.docker('volume','inspect','--format',fmt,name).split('|')
    if len(fields)!=2:raise RuntimeError('volume inspection failed')
    import json
    return tuple(json.loads(item) for item in fields)


def snapshot_digest(root):
    digest=hashlib.sha256();total=0
    for item in sorted(root.rglob('*')):
        if item.is_symlink():raise ValueError('source symlink refused')
        if item.is_dir():continue
        if not item.is_file():raise ValueError('non-file source entry refused')
        relative=item.relative_to(root).as_posix().encode();size=item.stat().st_size;total+=size
        if size>10*1024*1024 or total>100*1024*1024:raise ValueError('source exceeds bound')
        digest.update(len(relative).to_bytes(4,'big'));digest.update(relative);digest.update(size.to_bytes(8,'big'))
        with item.open('rb') as stream:
            while True:
                data=stream.read(65536)
                if not data:break
                digest.update(data)
    return digest.hexdigest()


def bind_path(raw):
    value=Path(raw)
    if value.is_symlink() or not value.is_dir():raise ValueError('bind directory unavailable')
    value=value.resolve()
    if ',' in str(value) or any(ord(c)<32 for c in str(value)):raise ValueError('unsafe bind path')
    return str(value)


def main():
    parser=argparse.ArgumentParser()
    for name in ('run-id','image-id','snapshot','snapshot-sha256','server-certs','client-certs'):
        parser.add_argument('--'+name,required=True)
    a=parser.parse_args()
    if not re.fullmatch('[a-f0-9]{24}',a.run_id) or not base.IMAGE_ID.fullmatch(a.image_id) or not re.fullmatch('[a-f0-9]{64}',a.snapshot_sha256):
        raise ValueError('invalid immutable inputs')
    snapshot=bind_path(a.snapshot);server_certs=bind_path(a.server_certs);client_certs=bind_path(a.client_certs)
    source=Path(snapshot);server=Path(server_certs);client=Path(client_certs)
    if snapshot_digest(source)!=a.snapshot_sha256:raise ValueError('source snapshot differs')
    if not (source/'harness.py').is_file() or not (source/'processing'/'node_mutation_broker.py').is_file():raise ValueError('source snapshot incomplete')
    expected_server={'server.crt','server.key','client.crt','ca.crt','SYNTHETIC-ONLY'}
    expected_client={'server.crt','client.key','client.crt','ca.crt','SYNTHETIC-ONLY'}
    for folder,expected in ((server,expected_server),(client,expected_client)):
        if {p.name for p in folder.iterdir()}!=expected:raise ValueError('credential layout differs')
        if any(not p.is_file() or p.is_symlink() or p.stat().st_size>65536 for p in folder.iterdir()):raise ValueError('credential files unsafe')
        if (folder/'SYNTHETIC-ONLY').read_bytes()!=b'SYNTHETIC TEST ONLY\n':raise ValueError('synthetic credential attestation missing')
    for name in ('server.crt','client.crt','ca.crt'):
        if (server/name).read_bytes()!=(client/name).read_bytes():raise ValueError('public certificate mismatch')
    if base.docker('image','inspect','--format','{{.Id}}',a.image_id)!=a.image_id:raise RuntimeError('local image differs')
    prefix='lt-remote-'+a.run_id;containers=[];volumes=[];failed=[];success=False
    common=['--label',LABEL+'='+a.run_id,'--restart=no','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges',
            '--memory','128m','--cpus','0.35','--pids-limit','48','--entrypoint','python3']
    def volume(suffix):
        name=prefix+'-'+suffix
        try:returned=base.docker('volume','create','--label',LABEL+'='+a.run_id,name)
        except (RuntimeError,subprocess.TimeoutExpired):
            print('UNKNOWN volume create outcome:',name,flush=True);raise
        if returned!=name or volume_info(name)!=(name,a.run_id):raise RuntimeError('volume identity differs')
        volumes.append(name);return name
    def create(suffix,network,user,mounts,program,extra=()):
        name=prefix+'-'+suffix
        args=['create','--name',name,'--network',network,*common,'--user',user,*extra]
        for mount in mounts:args.extend(('--mount',mount))
        args.extend((a.image_id,'-B',*program))
        identifier=base.create_or_reconcile('container',name,a.run_id,args)
        containers.append((identifier,name))
        base.verified_container(identifier,name,a.run_id,network)
        base.docker('start',identifier)
        return identifier
    def wait_success(identifier):
        if base.docker('wait',identifier,timeout=25)!='0':raise RuntimeError('synthetic container failed')
    try:
        node_state=volume('node-state');coordinator_state=volume('coordinator-state');control=volume('control')
        mounts=['type=volume,src='+node_state+',dst=/node-state','type=volume,src='+coordinator_state+',dst=/coordinator-state','type=volume,src='+control+',dst=/control']
        init=create('initializer','none','0:0',mounts,['-c',"import os;[(os.chown(p,65534,65534),os.chmod(p,0o700)) for p in ('/node-state','/coordinator-state','/control')]"],['--cap-add','CHOWN','--cap-add','FOWNER'])
        wait_success(init)
        source_mount='type=bind,src='+snapshot+',dst=/source,readonly'
        broker=create('broker','none','65534:65534',[source_mount,'type=volume,src='+node_state+',dst=/node-state','type=volume,src='+control+',dst=/control','type=bind,src='+client_certs+',dst=/certs,readonly'],['/source/harness.py','broker'])
        engine=create('engine','container:'+broker,'65534:65534',[source_mount],['/source/harness.py','engine'])
        probe=create('probe','none','65534:65534',[],['-c','import time;time.sleep(120)'])
        namespaces=[base.docker('exec',c,'python3','-c',"import os;print(os.readlink('/proc/self/ns/net'))",timeout=5) for c in (broker,engine,probe)]
        if namespaces[0]!=namespaces[1] or namespaces[0]==namespaces[2]:raise RuntimeError('namespace identities differ')
        if base.docker('exec',probe,'python3','-c',"import os;print(os.path.exists('/control/authority.sock'))",timeout=5)!='False':raise RuntimeError('probe unexpectedly has control socket')
        isolated=base.docker('exec',probe,'python3','-c',"import socket;s=socket.socket();s.settimeout(1);print(s.connect_ex(('127.0.0.1',30123)));s.close()",timeout=5)
        if isolated=='0':raise RuntimeError('isolated probe connected to unexpected listener')
        deadline=time.monotonic()+5
        while True:
            tables=base.docker('exec',broker,'python3','-c',"import pathlib,json;print(json.dumps([pathlib.Path('/proc/net/tcp').read_text(),pathlib.Path('/proc/net/tcp6').read_text()]))",timeout=5)
            import json
            tcp,tcp6=json.loads(tables);listeners=[]
            for family,table in ((4,tcp),(6,tcp6)):
                for line in table.splitlines()[1:]:
                    fields=line.split();addr,port=fields[1].split(':')
                    if int(port,16)==30123 and fields[3]=='0A':listeners.append((family,addr))
            if listeners==[(4,'0100007F')]:break
            if listeners or time.monotonic()>=deadline:raise RuntimeError('engine listener unavailable or non-loopback')
            time.sleep(.1)
        coordinator=create('coordinator','none','65534:65534',[source_mount,'type=volume,src='+coordinator_state+',dst=/coordinator-state','type=volume,src='+control+',dst=/control','type=bind,src='+server_certs+',dst=/certs,readonly'],['/source/harness.py','coordinator'])
        wait_success(coordinator);wait_success(broker)
        broker_log=base.docker('logs',broker,timeout=5);client_log=base.docker('logs',coordinator,timeout=5)
        if 'REMOTE_BROKER_TEST_PASS' not in broker_log or 'REMOTE_COORDINATOR_TEST_PASS' not in client_log:raise RuntimeError('synthetic evidence missing')
        success=True
    finally:
        for identifier,name in reversed(containers):
            try:
                current=base.verified_container(identifier,name,a.run_id)
                if current=='running':base.docker('stop','--time','2',identifier,timeout=8)
                if base.verified_container(identifier,name,a.run_id) not in ('created','exited'):raise RuntimeError('stop unconfirmed')
                print('verified stopped and retained:',identifier,name,flush=True)
            except Exception as error:
                failed.append(name);print('uncertain container:',name,type(error).__name__,flush=True)
        for name in volumes:
            try:
                if volume_info(name)!=(name,a.run_id):raise RuntimeError('volume identity differs')
                print('retained volume:',name,flush=True)
            except Exception as error:
                failed.append(name);print('uncertain volume:',name,type(error).__name__,flush=True)
        if failed:raise RuntimeError('resource verification incomplete')
    if success:print('PASS: synthetic remote authority init/replay, isolated mTLS control; no physical fence',flush=True)


if __name__=='__main__':
    try:main()
    except Exception as error:
        print('INCONCLUSIVE:',type(error).__name__,str(error),file=sys.stderr,flush=True);raise SystemExit(1)
