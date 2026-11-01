"""REVIEW ONLY: stage an immutable synthetic source snapshot; do not run yet.

Extracts only regular `processing/` files from an explicit commit into a new
run-owned directory and adds the reviewed synthetic harness. No deletion.
"""
import argparse
import io
import importlib.util
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile

HERE=Path(__file__).resolve().parent
SPEC=importlib.util.spec_from_file_location('broker_launcher',HERE/'synthetic-broker-namespace-runner.py')
launcher=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(launcher)


def main():
    parser=argparse.ArgumentParser()
    for name in ('repo','commit','new-root'):parser.add_argument('--'+name,required=True)
    a=parser.parse_args();repo=Path(a.repo);root=Path(a.new_root)
    if not re.fullmatch('[a-f0-9]{40}',a.commit):raise ValueError('full exact commit required')
    if not repo.is_absolute() or repo.resolve()!=repo or not repo.is_dir():raise ValueError('exact repo directory required')
    if not root.is_absolute() or root.parent.resolve()!=root.parent or root.exists() or root.is_symlink():
        raise ValueError('new absolute directory with real existing parent required')
    result=subprocess.run(['git','-C',str(repo),'archive','--format=tar',a.commit,'processing'],capture_output=True,timeout=20,check=False)
    if result.returncode or len(result.stdout)>100*1024*1024:raise RuntimeError('source archive unavailable or oversized')
    archive=tarfile.open(fileobj=io.BytesIO(result.stdout),mode='r:')
    members=archive.getmembers();total=0
    for member in members:
        path=PurePosixPath(member.name)
        if ('\\' in member.name or ':' in member.name or not path.parts or path.parts[0]!='processing'
                or path.is_absolute() or any(p in ('.','..') for p in path.parts)):
            raise ValueError('unsafe archive name')
        if not member.isdir() and not member.isfile():raise ValueError('archive links or special files refused')
        total+=member.size
        if member.size>10*1024*1024 or total>100*1024*1024:raise ValueError('source archive exceeds bound')
    root.mkdir(mode=0o755)
    for member in members:
        path=root.joinpath(*PurePosixPath(member.name).parts)
        if member.isdir():path.mkdir(mode=0o755,parents=True,exist_ok=True);continue
        path.parent.mkdir(mode=0o755,parents=True,exist_ok=True)
        source=archive.extractfile(member)
        if source is None:raise ValueError('archive content unavailable')
        with source,path.open('xb') as out:
            while True:
                chunk=source.read(65536)
                if not chunk:break
                out.write(chunk)
    harness=HERE/'synthetic-remote-outage'/'harness.py'
    if harness.is_symlink() or not harness.is_file():raise ValueError('harness unavailable')
    (root/'harness.py').write_bytes(harness.read_bytes())
    print('retained source root:',root,flush=True)
    print('source commit:',a.commit,'snapshot sha256:',launcher.snapshot_digest(root),flush=True)


if __name__=='__main__':main()
