"""Bounded local-only runner. Retains the exact task-owned container and data."""
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT=Path(r'C:\Users\fstor\.codex\worktrees\treedar-outer-provenance')
IMAGE='sha256:5be5aaecb962a41567bc8041f1e9e3432cb085b037fe057f9a562630b2e4b370'
LABEL='org.ledgetop.synthetic-run'

def docker(*args,timeout=10):
    return subprocess.run(['docker',*args],stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,timeout=timeout,check=True,text=True)

def owned(container,run):
    result=json.loads(docker('inspect',container).stdout)
    if (type(result) is not list or len(result)!=1 or result[0]['Id']!=container
            or result[0]['Config']['Labels'].get(LABEL)!=run):
        raise RuntimeError('container ownership uncertain; no stop authorized')
    return result[0]['State']

def main():
    if os.name!='nt' or Path.cwd().resolve()!=ROOT.resolve():
        raise SystemExit('exact Windows reviewed worktree required')
    if docker('image','inspect',IMAGE,'--format','{{.Id}}').stdout.strip()!=IMAGE:
        raise SystemExit('pinned image unavailable')
    run=uuid.uuid4().hex;name='lt-treedar-outer-'+run
    try:
        created=docker('create','--name',name,'--label',LABEL+'='+run,'--network','none',
            '--init','--cpus','1','--memory','1g','--pids-limit','128','--user','65534:65534',
            '--mount','type=bind,source='+str(ROOT)+',target=/work,readonly','--workdir','/work',
            IMAGE,'python','-B','-m','tests.run_treedar_outer_candidate',timeout=15)
    except Exception:
        print('container creation outcome uncertain; inspect name/label before retry:',name,run,flush=True)
        raise
    container=created.stdout.strip()
    if len(container)!=64 or any(c not in '0123456789abcdef' for c in container):
        raise RuntimeError('container ID uncertain; no stop authorized')
    print('owned container:',container,'run:',run,flush=True)
    owned(container,run)
    error=None
    try:
        started=docker('start','--attach',container,timeout=55)
        print(started.stdout,flush=True)
        if started.stderr:print(started.stderr,file=sys.stderr,flush=True)
    except Exception as exc:
        error=exc
        print('attach failed or deadline elapsed:',type(exc).__name__,flush=True)
    finally:
        try:
            state=owned(container,run)
            if state['Running']:
                docker('stop','--time','3',container,timeout=10)
                state=owned(container,run)
            print('owned container final state:',state['Status'],'exit:',state['ExitCode'],flush=True)
            if state['Running'] or state['Status']!='exited' or state['ExitCode']!=0:
                error=error or RuntimeError('synthetic container failed or remains active')
            if error:
                logs=docker('logs',container,timeout=10)
                print(logs.stdout,flush=True)
                if logs.stderr:print(logs.stderr,file=sys.stderr,flush=True)
        except Exception as exc:
            print('container final state/stop uncertain:',type(exc).__name__,flush=True)
            error=error or exc
    if error:raise SystemExit(1)

if __name__=='__main__':main()
