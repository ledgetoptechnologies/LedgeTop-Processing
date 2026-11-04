"""Read-only, advisory pilot checks. Does not copy photos or launch work."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

VIEWER_REVISION='ad0bf46611de44545825db9147e90b68195902ab'
CONVERTER={'Obj2Tiles':'3577688939806dabdf1f4620cc05debe6e178180a99bf0308f4b8483b8a36725',
 'libktx.so':'0792dd32ce7d2a101d614a3d1dc1f2b4297869d9cab5d7f0bb4fee304c4c9bcc',
 'build-info.json':'2c8e7e4184ccff6d5e08290ba70645c353a5d1f18b53ec7b14f1a52a8e4e8f2d'}

# Executed on cluster via the existing pinned SSH host key. Secrets remain in
# that process; stdout is a deliberately tiny, allowlisted aggregate object.
REMOTE=r'''
import json,pathlib,shutil,subprocess,urllib.parse,urllib.request
try:
 c=json.loads(subprocess.check_output(['docker','inspect','ledgetop-processing-prototype-c3fcb3a'],text=True,timeout=8))[0]
 n=json.loads(subprocess.check_output(['docker','inspect','lt-migration-1120ae14a8988e84513a4a8a46b78741-candidate'],text=True,timeout=8))[0]
 env=dict(x.split('=',1) for x in c['Config']['Env'] if '=' in x)
 p=pathlib.Path('/home/bkoltz/.local/share/ledgetop-processing-prototype/viewer-ba01f37')
 stage=(p/'viewer-staging.token').read_text().strip();prod=(p/'viewer-production.token').read_text().strip()
 def get(port,path,token):
  url='http://127.0.0.1:'+str(port)+path+'?'+urllib.parse.urlencode({'token':token})
  with urllib.request.urlopen(url,timeout=5) as r:return json.load(r)
 info=get(18080,'/info',stage);options=get(18080,'/options',stage);node=get(13001,'/info',env['LOCAL_NODE_TOKEN'])
 names={x.get('name') for x in options if isinstance(x,dict) and x.get('type')=='bool'}
 top=subprocess.check_output(['docker','top',n['Id'],'-eo','pid,comm'],text=True,timeout=8).splitlines()[1:]
 processes=[line.split() for line in top]
 allowed=all(len(x)==2 and x[0].isdigit() and x[1] in ('node','docker-init') for x in processes) and sum(x[1]=='node' for x in processes)==1
 query="import json,sqlite3; d=sqlite3.connect('file:/prototype-state/jobs.sqlite3?mode=ro',uri=True,timeout=3); print(json.dumps(d.execute(\"select count(*) from jobs where status in ('queued','running','held')\").fetchone()[0])); d.close()"
 active=json.loads(subprocess.check_output(['docker','exec','--user','10001:10001',c['Id'],'python','-c',query],text=True,timeout=8))
 print(json.dumps({'remoteOk':True,'coordinatorIdentity':c['Image']=='sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004' and c['State']['Running'] is True and (c['Config'].get('Labels') or {}).get('org.opencontainers.image.revision')=='693cb7eca32295da959a4f42399f317577ef8a8a',
  'nodeIdentity':n['Image']=='sha256:5ce31be03711d280e7f97ee3820dd982b68227181165b3f430b037447c3d13b6' and n['State']['Running'] is True,
  'stagingBinding':bool(stage) and stage==env.get('PROCESSING_VIEWER_STAGING_TOKEN') and bool(prod) and prod==env.get('PROCESSING_VIEWER_TOKEN') and stage!=prod,
  'providerExpected':info.get('engine')=='ledgetop-processing' and info.get('version')=='2.4.0' and info.get('maxParallelTasks')==1,
  'optionsAvailable':{'gltf','pc-ept'}<=names,'providerQueueCount':info.get('taskQueueCount'),
  'nodeExpected':node.get('engine')=='odm' and node.get('maxImages')==1000 and node.get('maxParallelTasks')==1,
  'nodeQueueCount':node.get('taskQueueCount'),'engineOnlyProcess':allowed,'activeJobCount':active,
  'inputLimitBytes':int(env.get('PROCESSING_VIEWER_MAX_INPUT_BYTES','0')),'clusterFreeBytes':shutil.disk_usage('/var/lib/docker').free}))
except Exception:
 print(json.dumps({'remoteOk':False,'reason':'remote_check_unavailable'}))
'''


def run_checked(args, *, cwd=None):
    return subprocess.run(args,cwd=cwd,capture_output=True,text=True,timeout=15,check=True).stdout.strip()


def source_status(viewer):
    if not viewer.is_absolute() or viewer.resolve()!=viewer or not viewer.is_dir():raise ValueError('real viewer path required')
    rev=run_checked(['git','rev-parse','HEAD'],cwd=viewer)
    clean=subprocess.run(['git','diff','--quiet'],cwd=viewer,timeout=15).returncode==0
    staged=subprocess.run(['git','diff','--cached','--quiet'],cwd=viewer,timeout=15).returncode==0
    extra=run_checked(['git','ls-files','--others','--exclude-standard'],cwd=viewer)
    return rev==VIEWER_REVISION and clean and staged and not extra


def converter_status(root):
    if not root.is_absolute() or root.resolve()!=root or not root.is_dir():raise ValueError('real converter directory required')
    for name,expected in CONVERTER.items():
        p=root/name
        if p.is_symlink() or not p.is_file():return False
        h=hashlib.sha256()
        with p.open('rb') as f:
            for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
        if h.hexdigest()!=expected:return False
    return True


def sample_size(source):
    if not source.is_absolute() or source.resolve()!=source or not source.is_dir():raise ValueError('real source directory required')
    images=sorted((p for p in source.iterdir() if p.suffix.lower() in ('.jpg','.jpeg')),key=lambda p:p.name.casefold())
    if len(images)<25 or len({p.name.casefold() for p in images[:25]})!=25:raise ValueError('25 unique JPEGs required')
    if any(p.is_symlink() or not p.is_file() for p in images[:25]):raise ValueError('regular JPEGs required')
    return sum(p.stat().st_size for p in images[:25])


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for arg in ('viewer','converter','photos','registry'):p.add_argument('--'+arg,type=Path,required=True)
    a=p.parse_args()
    size=sample_size(a.photos)
    if not a.registry.is_absolute() or a.registry.resolve()!=a.registry or not a.registry.is_dir():raise ValueError('existing real registry required')
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','-o','StrictHostKeyChecking=yes',
                           'bkoltz@192.168.50.89','python3','-'],input=REMOTE,capture_output=True,text=True,timeout=30)
    try:remote=json.loads(result.stdout) if result.returncode==0 else {'remoteOk':False,'reason':'ssh_unavailable'}
    except (ValueError,TypeError):remote={'remoteOk':False,'reason':'invalid_remote_response'}
    free=shutil.disk_usage(a.registry).free
    print(json.dumps({'advisoryOnly':True,'liveLaunchEnabled':False,'viewerSourceExactAndClean':source_status(a.viewer),
      'converterDigestMatch':converter_status(a.converter),'inputCount':25,'inputBytes':size,'localFreeBytes':free,
      'registryMarkerPresent':(a.registry/'active.json').exists(),'registryOwnershipAndAclVerified':False,
      'remote':remote,'notVerified':['registry ownership and ACL','cross-host duplicate fencing','external writers',
       'actual processing disk requirement','durable result custody','recovery','live Viewer deployment']}))


if __name__=='__main__':main()
