"""Unreviewed synthetic launcher; run only under paired_partial_drop_outer."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

from tests.paired_owned_windows import run_owned
from tests.paired_partial_drop_outer import IMAGE,LABEL

BASE='3e4c5eacfd4df22169b9b08ae5e4b90c9e7da57a'
VIEWER_COMMIT='7c526b49d78423372c29eac98b95a16a270c2bd3'
GOLDEN='5c81c6973a1447efa64cd27dbac68393cea0395111fa087c5bd3c451b18dca41'
SCRIPTS=('paired_partial_drop_in_container.py','paired_partial_drop_client.cjs')
ROOT=Path(__file__).resolve().parents[1]
VIEWER=Path(r'C:\Projects\viewer-v2-delivery-ledger')

def git(cwd,*args):
    return subprocess.run(['git',*args],cwd=cwd,check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=10).stdout

def docker(*args,timeout=15):
    return run_owned(['docker',*args],timeout=timeout)

def main():
    run_id=os.environ.get('LT_PARTIAL_RUN_ID','');raw=os.environ.get('LT_PARTIAL_ROOT','')
    if os.name!='nt' or not re.fullmatch(r'[a-f0-9]{32}',run_id):raise RuntimeError('outer ownership required')
    root=Path(raw).resolve(strict=True)
    if not root.name.startswith('lt-partial-drop-outer-') or list(root.iterdir()):raise RuntimeError('empty outer root required')
    if git(VIEWER,'rev-parse','HEAD').decode().strip()!=VIEWER_COMMIT:raise RuntimeError('Viewer commit differs')
    if git(VIEWER,'status','--porcelain','--','apps/viewer/server','apps/viewer/test/fixtures/reference-v1-acceptance-sender.golden.json'):raise RuntimeError('Viewer sources modified')
    git(ROOT,'diff','--quiet',BASE,'--','processing','contracts')
    if git(ROOT,'status','--porcelain','--','processing','contracts'):raise RuntimeError('Processing sources modified')
    for path in (ROOT/'contracts/reference-v1-acceptance-sender.golden.json', VIEWER/'apps/viewer/test/fixtures/reference-v1-acceptance-sender.golden.json'):
        if hashlib.sha256(path.read_bytes()).hexdigest()!=GOLDEN:raise RuntimeError('golden differs')
    code,out=docker('image','inspect',IMAGE,'--format','{{.Id}}')
    if code or out.decode().strip()!=IMAGE:raise RuntimeError('pinned image unavailable')
    scripts=root/'scripts';scripts.mkdir()
    hashes={}
    for name in SCRIPTS:
        data=(ROOT/'tests'/name).read_bytes();hashes[name]=hashlib.sha256(data).hexdigest()
        with (scripts/name).open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
    print('SNAPSHOT_SHA256',json.dumps(hashes,sort_keys=True),flush=True)
    name='lt-partial-drop-'+run_id;volume='lt-partial-volume-'+run_id
    code,out=docker('volume','inspect',volume)
    if code==0 or b'no such volume' not in out.lower():raise RuntimeError('volume absence not established')
    code,out=docker('volume','create','--label',LABEL+'='+run_id,volume)
    if code or out.decode().strip()!=volume:raise RuntimeError('volume creation uncertain')
    code,out=docker('volume','inspect',volume)
    if code:raise RuntimeError('volume inspection failed')
    value=json.loads(out)[0]
    if value['Name']!=volume or value.get('Driver')!='local' or (value.get('Labels') or {}).get(LABEL)!=run_id:raise RuntimeError('volume identity differs')
    def mount(source,target):
        source=str(source.resolve(strict=True))
        if ',' in source:raise RuntimeError('unsupported source path')
        return 'type=bind,source='+source+',target='+target+',readonly'
    args=['docker','run','--name',name,'--label',LABEL+'='+run_id,'--cidfile',str(root/'container.cid'),
        '--network','none','--read-only','--tmpfs','/tmp:rw,nosuid,noexec,size=128m','--cap-drop','ALL',
        '--security-opt','no-new-privileges','--cpus','1','--memory','1g','--pids-limit','64',
        '--mount',mount(ROOT/'processing','/scope/processing'),'--mount',mount(ROOT/'contracts','/scope/contracts'),
        '--mount',mount(scripts,'/scope/tests'),'--mount',mount(VIEWER/'apps/viewer/server','/viewer'),
        '--mount','type=volume,source='+volume+',target=/owned','--workdir','/scope',
        '--env','PYTHONDONTWRITEBYTECODE=1','--env','PYTHONPATH=/scope','--env','LT_PARTIAL_DROP_SYNTHETIC=1',
        '--entrypoint','/usr/bin/python3',IMAGE,'-u','/scope/tests/paired_partial_drop_in_container.py']
    code,out=run_owned(args,timeout=120)
    print(out.decode('utf8','replace'),flush=True)
    if code:raise RuntimeError('synthetic container failed')
    for name in SCRIPTS:
        if hashlib.sha256((scripts/name).read_bytes()).hexdigest()!=hashes[name]:raise RuntimeError('script snapshot changed')
    if git(VIEWER,'rev-parse','HEAD').decode().strip()!=VIEWER_COMMIT or git(VIEWER,'status','--porcelain','--','apps/viewer/server'):raise RuntimeError('Viewer source changed')
    git(ROOT,'diff','--quiet',BASE,'--','processing','contracts')
    if git(ROOT,'status','--porcelain','--','processing','contracts'):raise RuntimeError('Processing sources changed')

if __name__=='__main__':main()
