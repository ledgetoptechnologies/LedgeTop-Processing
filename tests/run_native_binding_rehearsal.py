"""Isolated rehearsal; retains evidence readable to local users allowed through its parent.

The caller must supply a private evidence parent; its child is 0755 so the
unprivileged container can read the source exports on the bind mount.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tarfile
import threading
import time

COORD = '0c245a8d1927347dfdbce1f8a9b071f2f24ddeef'
VIEWER = 'b05b51ce659bea101b6ea4231c6bd6dcd749f5c5'  # Known stale-context race; fixture only.
IMAGE = 'sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668'
VIEWER_PATHS = ['apps/viewer/server', 'apps/viewer/lod-converter-policy.cjs',
                'apps/viewer/test/fixtures/native-input-manifest-v1-candidate.json']

def command(args, *, cwd=None, timeout=30):
    return subprocess.run(args, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=timeout, check=True)

def export(repo, revision, paths, destination):
    listing=command(['git','ls-tree','-r','-z',revision,'--',*paths],cwd=repo).stdout
    if not listing or len(listing)>4*1024*1024:raise RuntimeError('invalid source tree listing')
    expected={}
    for entry in listing.split(b'\0'):
        if not entry:continue
        metadata,name=entry.split(b'\t',1);mode,kind,oid=metadata.split()
        if mode not in (b'100644',b'100755') or kind!=b'blob' or len(oid)!=40:
            raise RuntimeError('unsupported Git source entry')
        expected[name.decode('utf-8')]=oid.decode('ascii')
    archive = destination.parent / (destination.name + '.tar')
    errors = []; done = threading.Event()
    process = subprocess.Popen(['git', '-c', 'tar.umask=0022', '-c', 'core.autocrlf=false',
                                '-c', 'core.eol=lf', 'archive', '--format=tar', revision, '--', *paths],
                               cwd=repo, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    def copy():
        try:
            with archive.open('xb') as out:
                total = 0
                while True:
                    block = process.stdout.read(1024 * 1024)
                    if not block: break
                    total += len(block)
                    if total > 64 * 1024 * 1024: raise RuntimeError('archive too large')
                    out.write(block)
                out.flush(); os.fsync(out.fileno())
        except BaseException as exc:
            errors.append(exc)
        finally:
            done.set()
    worker = threading.Thread(target=copy, daemon=True); worker.start()
    try:
        deadline = time.monotonic() + 60
        while not done.wait(.1):
            if time.monotonic() >= deadline: raise TimeoutError('archive deadline')
        if errors: raise errors[0]
        if process.wait(timeout=5): raise RuntimeError('git archive failed')
    finally:
        if process.poll() is None: process.kill()
        process.wait(timeout=5); worker.join(5)
        if worker.is_alive(): raise RuntimeError('archive reader termination uncertain')
        process.stdout.close()
    print(destination.name + '_archive_sha256=' + hashlib.sha256(archive.read_bytes()).hexdigest(), flush=True)
    destination.mkdir(mode=0o755)
    with tarfile.open(archive) as tar:
        seen=set()
        for member in tar:
            path = Path(member.name)
            if (path.is_absolute() or '\\' in member.name or ':' in member.name or '..' in path.parts
                    or not (member.isfile() or member.isdir()) or member.mode & 0o7000):
                raise RuntimeError('unsafe archive member')
            target = destination / path
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                if member.name in seen or member.name not in expected:raise RuntimeError('unexpected source file')
                seen.add(member.name)
                target.parent.mkdir(parents=True, exist_ok=True)
                blob=hashlib.sha1(('blob '+str(member.size)+'\0').encode('ascii'))
                with tar.extractfile(member) as source, target.open('xb') as output:
                    for chunk in iter(lambda:source.read(1024*1024),b''):
                        blob.update(chunk);output.write(chunk)
                if blob.hexdigest()!=expected[member.name]:raise RuntimeError('Git source blob mismatch')
        if seen != set(expected):raise RuntimeError('incomplete source export')
    print(destination.name + '_git_blobs_verified=' + str(len(seen)),flush=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--coordinator-repo', required=True)
    parser.add_argument('--viewer-repo', required=True)
    parser.add_argument('--evidence-parent', required=True)
    args = parser.parse_args()
    if VIEWER is None:raise RuntimeError('Viewer source not pinned and reviewed')
    parent = Path(args.evidence_parent).resolve(strict=True)
    run = 'native-binding-' + secrets.token_hex(16)
    evidence = parent / run
    evidence.mkdir(mode=0o755)
    print('evidence=' + str(evidence), flush=True)
    export(args.coordinator_repo, COORD, ['processing', 'tests/__init__.py',
        'tests/test_native_manifest_binding.py', 'tests/test_native_plan_binding.py',
        'tests/test_native_plan_http.py', 'tests/test_viewer_http.py',
        'tests/fixtures/native_input_manifest_vectors.json'], evidence / 'coordinator')
    export(args.viewer_repo, VIEWER, VIEWER_PATHS, evidence / 'viewer')
    here = Path(__file__).resolve().parent
    for rel in ['test_native_binding_cross_language.py', 'fixtures/native_binding_cross_language.cjs']:
        source = here / rel
        target = evidence / 'coordinator' / 'tests' / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        print(rel + '_sha256=' + hashlib.sha256(target.read_bytes()).hexdigest(), flush=True)
    daemon = command(['docker', 'info', '--format', '{{.ID}}']).stdout.strip()
    if command(['docker', 'image', 'inspect', IMAGE, '--format', '{{.Id}}']).stdout.decode().strip() != IMAGE:
        raise RuntimeError('local image identity mismatch')
    print('daemon_id=' + daemon.decode(), flush=True)
    print('run_id=' + run, flush=True)
    with (evidence / 'run.json').open('x', encoding='utf-8') as out:
        json.dump({'run':run,'containerName':run,'daemonId':daemon.decode(),
                   'imageId':IMAGE,'coordinatorCommit':COORD,'viewerCommit':VIEWER}, out, sort_keys=True)
        out.flush(); os.fsync(out.fileno())
    print('create_pending=true', flush=True)
    result = command(['docker', 'create', '--pull=never', '--name', run, '--label', 'codex.synthetic.run=' + run,
        '--network', 'none', '--user', '65534:65534', '--cpus', '1', '--memory', '768m', '--pids-limit', '128',
        '--read-only', '--tmpfs', '/tmp:rw,nosuid,nodev,size=128m,mode=1777',
        '--mount', 'type=bind,src=' + str(evidence) + ',dst=/work,readonly',
        '-w', '/work/coordinator', '-e', 'NATIVE_BINDING_CROSS_LANGUAGE=1',
        '-e', 'PINNED_VIEWER_EXPORT=/work/viewer', '-e', 'NODE_BINARY=/usr/local/bin/node',
        '--entrypoint', '/usr/bin/python3', IMAGE, '-m', 'unittest', '-v',
        'tests.test_native_binding_cross_language.CrossLanguageBinding.test_viewer_client_replays_after_discarded_reply_and_restart'], timeout=30)
    container = result.stdout.decode().strip()
    if len(container) != 64 or any(c not in '0123456789abcdef' for c in container):
        raise RuntimeError('uncertain container ID; retain evidence and reconcile manually')
    print('container_id=' + container, flush=True)
    (evidence / 'container-id.txt').write_text(container + '\n', encoding='ascii')
    if command(['docker', 'info', '--format', '{{.ID}}']).stdout.strip() != daemon:
        raise RuntimeError('daemon changed; retain container')
    before = json.loads(command(['docker', 'inspect', container]).stdout)[0]
    if (before['Id'] != container or before['Image'] != IMAGE
            or before['HostConfig']['NetworkMode'] != 'none'
            or before['Config']['User'] != '65534:65534'
            or before['Config']['Labels'].get('codex.synthetic.run') != run
            or before['State']['Running']):
        raise RuntimeError('container identity uncertain; retain container')
    try:
        result = subprocess.run(['docker', 'start', '-a', container], stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=90, check=False)
        (evidence / 'test.log').write_bytes(result.stdout)
        print('test_exit=' + str(result.returncode), flush=True)
        if result.returncode:
            raise RuntimeError('synthetic test failed; evidence retained')
    except BaseException:
        print('container_state_uncertain=true; retain container and evidence', flush=True)
        raise
    # Never remove by name, on a changed daemon, or if the exact owned container is running.
    if command(['docker', 'info', '--format', '{{.ID}}']).stdout.strip() != daemon:
        raise RuntimeError('daemon changed; retain container')
    info = json.loads(command(['docker', 'inspect', container]).stdout)[0]
    if (info['Id'] != container or info['Image'] != IMAGE or info['HostConfig']['NetworkMode'] != 'none'
            or info['Config']['User'] != '65534:65534'
            or info['Config']['Labels'].get('codex.synthetic.run') != run
            or info['State']['Running']):
        raise RuntimeError('container ownership or exit uncertain; retain container')
    if command(['docker', 'info', '--format', '{{.ID}}']).stdout.strip() != daemon:
        raise RuntimeError('daemon changed; retain container')
    command(['docker', 'rm', container], timeout=30)
    print('owned_container_removed=true', flush=True)

if __name__ == '__main__':
    try:
        main()
    except BaseException as exc:
        print('rehearsal_failed=' + type(exc).__name__, file=sys.stderr, flush=True)
        raise
