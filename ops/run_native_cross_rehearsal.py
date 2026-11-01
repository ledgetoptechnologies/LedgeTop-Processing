"""Unqualified synthetic rehearsal launcher; Viewer pin intentionally unset."""
import argparse,hashlib,json,os,secrets,subprocess,sys,tempfile,threading,time
from pathlib import Path

PROCESSING_BASE='42da801be55b686e43a78e35b88d32de89f8c5b4'
FIXTURE_COMMIT='ae496a2e5487de00850892348fab97a86ffaab8b'
VIEWER_COMMIT_PIN=None  # Await corrected owner-confirmed and independently reviewed commit.
IMAGE='sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668'
LABEL='org.openai.codex.native-cross'
MAX_ARCHIVE_BYTES=536870912

BOOTSTRAP=r'''
import hashlib,os,stat,tarfile,tempfile
from pathlib import Path,PurePosixPath
root=Path(tempfile.mkdtemp(prefix='native-cross-'));os.chmod(root,0o700)
def unpack(archive,dest,expected,allowed):
 h=hashlib.sha256()
 with open(archive,'rb') as f:
  for block in iter(lambda:f.read(1048576),b''):h.update(block)
 if h.hexdigest()!=expected:raise RuntimeError('archive hash changed')
 dest.mkdir(mode=0o700);seen=set();total=0
 with tarfile.open(archive,'r:') as t:
  for m in t:
   n=m.name.rstrip('/');parts=n.split('/')
   if not n or n.startswith('/') or '\\' in n or any(x in ('','.','..') for x in parts):raise RuntimeError('unsafe path')
   if n in seen or not any(n==p or n.startswith(p+'/') or p.startswith(n+'/') for p in allowed):raise RuntimeError('unexpected path')
   if not (m.isdir() or m.isfile()) or m.mode & 0o7000 or (m.mode & 0o777) not in (0o644,0o755):raise RuntimeError('unsafe archive entry')
   total+=m.size;seen.add(n)
   if len(seen)>50000 or total>536870912:raise RuntimeError('archive limit')
   path=dest.joinpath(*parts);path.parent.mkdir(parents=True,exist_ok=True)
   if m.isdir():path.mkdir(exist_ok=True)
   else:
    src=t.extractfile(m)
    if src is None:raise RuntimeError('missing archive bytes')
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as out:
     remaining=m.size
     while remaining:
      b=src.read(min(1048576,remaining))
      if not b:raise RuntimeError('truncated archive')
      out.write(b);remaining-=len(b)
unpack('/input/processing.tar',root/'processing',os.environ['PROCESSING_SHA'],('processing','tests/native_viewer_cross_server.py','tests/native_viewer_cross_client.cjs'))
unpack('/input/viewer.tar',root/'viewer',os.environ['VIEWER_SHA'],('apps/viewer','packages/shared'))
# ESM package resolution does not use NODE_PATH. This private fixture-only
# link points to dependencies supplied by the pinned read-only test image.
modules=Path('/src/apps/viewer/node_modules')
if not modules.is_dir() or not (modules/'geotiff').is_dir():raise RuntimeError('fixture image dependencies unavailable')
os.symlink(str(modules),str(root/'viewer/apps/viewer/node_modules'),target_is_directory=True)
os.chdir(root/'processing');os.environ['VIEWER_SOURCE']=str(root/'viewer');os.environ['PYTHONPATH']=str(root/'processing')
os.execv('/usr/bin/python3',['python3','tests/native_viewer_cross_server.py'])
'''

def call(args,timeout=15,**kwargs):
    return subprocess.run(args,check=True,timeout=timeout,**kwargs)
def capture(args,timeout=15):
    return call(args,timeout=timeout,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True).stdout.strip()
def archive(repo,revision,paths,destination):
    errors=[];done=threading.Event()
    process=subprocess.Popen(['git','-C',str(repo),'-c','tar.umask=0022','archive','--format=tar',revision,'--',*paths],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
    def copy():
        try:
            with destination.open('xb') as out:
                total=0
                while True:
                    block=process.stdout.read(1048576)
                    if not block:break
                    total+=len(block)
                    if total>MAX_ARCHIVE_BYTES:raise RuntimeError('archive too large')
                    out.write(block)
                out.flush();os.fsync(out.fileno())
        except BaseException as error:errors.append(error)
        finally:done.set()
    worker=threading.Thread(target=copy,daemon=True);worker.start();deadline=time.monotonic()+60
    try:
        while not done.wait(.1):
            if time.monotonic()>=deadline:raise TimeoutError('archive deadline')
        if errors:raise errors[0]
        if process.wait(timeout=5)!=0:raise RuntimeError('git archive failed')
    finally:
        if process.poll() is None:process.kill()
        process.wait(timeout=5);worker.join(5)
        if worker.is_alive():raise RuntimeError('archive reader termination uncertain')
        process.stdout.close()
    h=hashlib.sha256()
    with destination.open('rb') as f:
        for chunk in iter(lambda:f.read(1048576),b''):h.update(chunk)
    return h.hexdigest()
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--viewer-repo',required=True);parser.add_argument('--processing-repo',required=True);args=parser.parse_args()
    if VIEWER_COMMIT_PIN is None:raise SystemExit('Viewer source has not been pinned and reviewed')
    viewer=Path(args.viewer_repo).resolve(strict=True);processing=Path(args.processing_repo).resolve(strict=True)
    for repo,commit in ((viewer,VIEWER_COMMIT_PIN),(processing,FIXTURE_COMMIT)):
        if capture(['git','-C',str(repo),'rev-parse','--verify',commit+'^{commit}'])!=commit:raise RuntimeError('commit unavailable')
    call(['git','-C',str(processing),'diff','--quiet',PROCESSING_BASE,FIXTURE_COMMIT,'--','processing'],timeout=15)
    daemon=capture(['docker','info','--format','{{.ID}}']);image=capture(['docker','image','inspect',IMAGE,'--format','{{.Id}}'])
    if image!=IMAGE or not daemon:raise RuntimeError('Docker identity mismatch')
    run=secrets.token_hex(16);directory=Path(tempfile.mkdtemp(prefix='codex-native-cross-'+run+'-')).resolve();name='codex-native-cross-'+run
    evidence={'run':run,'containerName':name,'daemonId':daemon,'imageId':image,'processingCommit':FIXTURE_COMMIT,'viewerCommit':VIEWER_COMMIT_PIN}
    marker=directory/'run.json'
    with marker.open('x',encoding='utf-8') as out:json.dump(evidence,out,sort_keys=True);out.flush();os.fsync(out.fileno())
    print('Run identity:',json.dumps(evidence,sort_keys=True),'evidence:',directory,flush=True)
    container=None;attempted=False
    try:
        psha=archive(processing,FIXTURE_COMMIT,['processing','tests/native_viewer_cross_server.py','tests/native_viewer_cross_client.cjs'],directory/'processing.tar')
        vsha=archive(viewer,VIEWER_COMMIT_PIN,['apps/viewer','packages/shared'],directory/'viewer.tar')
        print('Archive SHA256:',psha,vsha,flush=True)
        attempted=True
        container=capture(['docker','create','--pull=never','--name',name,'--label',LABEL+'='+run,'--network=none','--cpus=1.5','--memory=768m','--pids-limit=128','--read-only','--user=10001:10001','--log-driver','json-file','--log-opt','max-size=1m','--log-opt','max-file=1','--tmpfs','/tmp:rw,nosuid,nodev,size=16g,mode=1777','--mount',f'type=bind,src={directory},dst=/input,readonly','--env','PROCESSING_SHA='+psha,'--env','VIEWER_SHA='+vsha,'--entrypoint','/usr/bin/python3',IMAGE,'-c',BOOTSTRAP],timeout=30)
        if len(container)!=64 or any(c not in '0123456789abcdef' for c in container):raise RuntimeError('container identity uncertain')
        with (directory/'container.id').open('x') as out:out.write(container+'\n');out.flush();os.fsync(out.fileno())
        def owned():
            if capture(['docker','info','--format','{{.ID}}'])!=daemon:return False
            value=json.loads(capture(['docker','inspect',container]))[0]
            return (value['Id']==container and value['Image']==image and value['Config']['Labels'].get(LABEL)==run and value['HostConfig']['NetworkMode']=='none')
        if not owned():raise RuntimeError('container identity mismatch')
        call(['docker','start',container],timeout=15,stdout=subprocess.DEVNULL)
        result=capture(['docker','wait',container],timeout=180)
        with (directory/'container.log').open('xb') as out:call(['docker','logs',container],timeout=15,stdout=out,stderr=subprocess.STDOUT)
        print('Exit status:',result,'evidence retained:',directory,flush=True)
        if result!='0':raise RuntimeError('synthetic rehearsal failed')
    finally:
        if container:
            try:
                if not owned():raise RuntimeError('ownership unavailable')
                state=capture(['docker','inspect','--format','{{.State.Running}}',container])
                if state=='true':
                    if not owned():raise RuntimeError('ownership changed')
                    call(['docker','stop','--time','5',container],timeout=15,stdout=subprocess.DEVNULL)
                if not owned() or capture(['docker','inspect','--format','{{.State.Running}}',container])!='false':raise RuntimeError('container state uncertain')
                if not owned():raise RuntimeError('ownership changed')
                call(['docker','rm',container],timeout=15,stdout=subprocess.DEVNULL)
            except Exception as error:
                print('Owned container cleanup uncertain; retained:',container,type(error).__name__,file=sys.stderr)
                raise RuntimeError('owned container cleanup uncertain') from error
        elif attempted:print('Container create outcome uncertain; inspect recorded name/label; no cleanup attempted:',name,file=sys.stderr)
        print('Evidence retained:',directory,flush=True)
if __name__=='__main__':main()
