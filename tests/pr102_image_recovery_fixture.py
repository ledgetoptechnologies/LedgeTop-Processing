"""Owned isolated image rehearsal; no build, live mounts or external network."""
import hashlib, io, json, subprocess, sys, tarfile, uuid

IMAGE = 'sha256:318040006e6b2c333cd1383f14ae7e38fd51921aa0ffebec0eca0efb26e4b29c'
SOURCE = '19111a58660773302e143253a1e34b1901e83cbb'
TEST_SOURCE = '4de0de1b5459a5c248d893b640a84262a807889a'

def command(args, **kw):
    return subprocess.run(args, timeout=30, check=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, **kw)

archive = command(['git', 'archive', '--format=tar', SOURCE, 'processing']).stdout
manifest = {}
with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
    for item in bundle:
        if item.isdir():
            continue
        if not item.isfile() or not item.name.startswith('processing/'):
            raise RuntimeError('unsupported source archive entry')
        manifest[item.name[len('processing/'):]] = hashlib.sha256(bundle.extractfile(item).read()).hexdigest()
if not manifest:
    raise RuntimeError('empty source manifest')
test = command(['git', 'show', TEST_SOURCE + ':tests/test_viewer_service.py']).stdout
prefix = ('import hashlib,json,pathlib\n'
          'root=pathlib.Path("/app/processing")\n'
          'files={p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() '
          'for p in root.rglob("*") if p.is_file() and not p.is_symlink()}\n'
          'assert not any(p.is_symlink() for p in root.rglob("*"))\n'
          'assert files==json.loads(' + repr(json.dumps(manifest, sort_keys=True)) + ')\n').encode()
image = json.loads(command(['docker','image','inspect', IMAGE]).stdout)[0]
if (image['Id'] != IMAGE or image['Config']['User'] != '10001:10001'
        or image['Config']['Labels'].get('org.opencontainers.image.revision') != TEST_SOURCE):
    raise RuntimeError('image identity differs')
owner = uuid.uuid4().hex
name = 'codex-pr102-owned-' + owner
cid = None
try:
    created = command(['docker','create','-i','--name',name,'--label','codex.fixture.owner='+owner,
        '--network','none','--read-only','--tmpfs','/tmp:rw,nosuid,nodev,size=64m,mode=1777',
        '--cpus','0.5','--memory','256m','--pids-limit','128','--entrypoint','python',
        IMAGE,'-','-k','missing_unrecorded_archive','-v'])
    cid = created.stdout.decode().strip()
    if len(cid) != 64 or any(ch not in '0123456789abcdef' for ch in cid):
        raise RuntimeError('invalid created container ID')
    detail = json.loads(command(['docker','inspect',cid]).stdout)[0]
    if detail['Id'] != cid or detail['Image'] != IMAGE or detail['Name'] != '/'+name or detail['Config']['Labels'].get('codex.fixture.owner') != owner:
        raise RuntimeError('container identity mismatch')
    result = command(['docker','start','--attach','--interactive',cid], input=prefix+test)
    if b'Ran 3 tests' not in result.stderr or b'OK' not in result.stderr:
        raise RuntimeError('expected three synthetic tests did not complete')
    detail = json.loads(command(['docker','inspect',cid]).stdout)[0]
    if detail['State']['Running'] or detail['State']['ExitCode'] != 0:
        raise RuntimeError('synthetic container did not exit cleanly')
    sys.stdout.buffer.write(result.stdout)
    sys.stdout.buffer.write(result.stderr)
    print('Image source manifest matched and owned synthetic tests exited successfully.')
finally:
    if cid:
        try:
            detail = json.loads(command(['docker','inspect',cid]).stdout)[0]
            if detail['Id'] != cid or detail['Image'] != IMAGE or detail['Name'] != '/'+name or detail['Config']['Labels'].get('codex.fixture.owner') != owner:
                raise RuntimeError('cleanup ownership uncertain')
            if detail['State']['Running']:
                command(['docker','stop','--time','5',cid])
            command(['docker','rm',cid])
        except Exception:
            print('Owned container cleanup uncertain; retain and inspect:', cid, file=sys.stderr)
            raise
    else:
        print('Create did not return an ID; inspect unique owned name if outcome is uncertain:', name, file=sys.stderr)
