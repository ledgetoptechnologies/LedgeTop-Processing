"""REVIEW ONLY. Read-only host guard; no containers are started."""
import hashlib,json,os,pathlib,re,stat,subprocess,sys

def require(ok,message):
    if not ok: raise SystemExit('preflight failed: '+message)
require(len(sys.argv)==4,'expected absolute fixture root, immutable manifest path and fixture nonce')
nonce=sys.argv[3];require(re.fullmatch(r'[0-9a-f]{32}',nonce) is not None,'invalid fixture nonce')
root=pathlib.Path(sys.argv[1]); manifest=pathlib.Path(sys.argv[2])
require(root.is_absolute() and root.name.startswith('node-retention-fixture-'),'unexpected root')
require(not root.is_symlink() and str(root)==os.path.realpath(root),'root symlink or alias')
st=root.stat();require(st.st_uid==0 and stat.S_IMODE(st.st_mode)==0o700,'root must be root:root mode 0700');require(st.st_gid==0,'root group must be root')
require(manifest.is_absolute() and not manifest.is_symlink() and str(manifest)==os.path.realpath(manifest),'manifest path alias')
expected=json.loads(manifest.read_text());names=('on/data','on/tmp','off/data','off/tmp','overlay/config.js','overlay/TaskManager.js','runner/fixture.cjs','runner/parser.cjs')
require(set(expected)==set(names[4:]),'manifest must pin all and only input artifacts')
paths=[root/name for name in names]
for item in paths:
    require(item.exists() and str(item)==os.path.realpath(item),'missing or aliased input')
    require(os.path.commonpath([str(root),str(item)])==str(root) and item!=root,'path outside root')
for i,a in enumerate(paths):
    for b in paths[i+1:]:require(os.path.commonpath([str(a),str(b)]) not in (str(a),str(b)),'overlapping paths')
require(all(p.is_dir() for p in paths[:4]),'state paths not directories')
for p in [root,*paths[:4]]:
    marker=p/'.fixture-marker';require(marker.is_file() and not marker.is_symlink() and marker.stat().st_nlink==1 and marker.read_text()==f'node-retention-{nonce}\n','missing or incorrect task-owned marker')
for p in paths[4:]:
    require(p.is_file() and p.stat().st_nlink==1,'artifact missing or hard-linked')
    digest=hashlib.sha256(p.read_bytes()).hexdigest()
    require(digest==expected[str(p.relative_to(root))],'artifact hash mismatch')
ids=subprocess.run(['docker','ps','-q'],capture_output=True,text=True,check=True,timeout=10).stdout.split()
if ids:
    containers=json.loads(subprocess.run(['docker','inspect',*ids],capture_output=True,text=True,check=True,timeout=20).stdout)
    for container in containers:
        for mount in container.get('Mounts',[]):
            src=mount.get('Source','')
            if not mount.get('RW') or not src.startswith('/'):continue
            src=os.path.realpath(src)
            require(os.path.commonpath([src,str(root)]) not in (src,str(root)),'writable running-container alias')
print('fixture host checks passed; point-in-time only')
