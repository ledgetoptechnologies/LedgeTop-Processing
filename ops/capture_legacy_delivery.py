"""Capture a point-in-time legacy observation from a pinned, isolated image.

Run this module only from the archive-verified controller source. The resulting
private receipt remains provisional until it matches frozen full custody.
"""
import argparse
import hashlib
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
import uuid

from .enrolled_coordinator_transport import DockerCLI, _absolute_path, canonical, decode, private_bytes, require
from .legacy_delivery_qualification import validate_artifact, seal_capture

OWNER = 'org.ledgetop.legacy-delivery-observation'
IMAGE = re.compile(r'sha256:[0-9a-f]{64}\Z')
VOLUME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z')
HEX = re.compile(r'[0-9a-f]{64}\Z')
FILES = ('ops/legacy_delivery_qualification.py', 'ops/coordinator_predecessor_census.py',
         'ops/enrolled_coordinator_transport.py', 'tests/fixtures/observe_legacy_qualification.py')


def helper_hashes():
    root=Path(__file__).resolve().parents[1]; result={}
    for relative in FILES+('ops/capture_legacy_delivery.py',):
        path=root/relative
        require(path.is_file() and not path.is_symlink() and path.resolve()==path.absolute()
                and path.stat().st_size<=1048576,'legacy_capture_helper_changed')
        result[relative]=hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def capture(*, image, source_commit, state_volume, authority, reader_sha256, key, docker=None):
    require(type(image) is str and IMAGE.fullmatch(image) and type(source_commit) is str
            and re.fullmatch('[0-9a-f]{40}',source_commit) and type(state_volume) is str
            and VOLUME.fullmatch(state_volume) and type(reader_sha256) is str and HEX.fullmatch(reader_sha256)
            and type(key) is bytes and len(key)==32, 'legacy_capture_inputs_invalid')
    docker=docker or DockerCLI()
    docker.image(image)
    volumes=decode(docker.command('volume','inspect','--',state_volume,output_limit=65536))
    require(type(volumes) is list and len(volumes)==1 and volumes[0].get('Name')==state_volume,
            'existing_legacy_state_volume_required')
    token=uuid.uuid4().hex; name='ltds-legacy-observe-'+token
    root=Path(__file__).resolve().parents[1]
    pinned_helpers=helper_hashes()
    identifier=None
    with tempfile.TemporaryDirectory(prefix='ltds-legacy-observe-') as tmp:
        folder=Path(tmp); folder.chmod(0o755)
        for relative in FILES:
            path=root/relative
            require(path.is_file() and not path.is_symlink() and path.resolve()==path.absolute()
                    and path.stat().st_size<=1048576,'legacy_capture_helper_changed')
            target=folder/('observer.py' if relative.startswith('tests/') else relative)
            target.parent.mkdir(mode=0o755,parents=True,exist_ok=True)
            raw=path.read_bytes(); target.write_bytes(raw); target.chmod(0o644)
            require(hashlib.sha256(target.read_bytes()).hexdigest()==pinned_helpers[relative],
                    'legacy_capture_helper_changed')
        try:
            try:
                raw=docker.command('container','create','--name',name,'--label',OWNER+'='+token,
                    '--network','none','--read-only','--cpus','0.5','--memory','256m','--user','10001:10001',
                    '--interactive','--env','PYTHONDONTWRITEBYTECODE=1',
                    '--mount','type=volume,src='+state_volume+',dst=/prototype-state,readonly',
                    '--mount','type=bind,src='+str(folder)+',dst=/qualification,readonly',
                    '--entrypoint','python',image,'/qualification/observer.py',image,source_commit,
                    mutation=True,timeout=60,output_limit=4096)
            except Exception:
                # The Docker RPC may have succeeded despite a lost client reply.
                # The exact name is reported for inspection, never a blind retry.
                raise RuntimeError('legacy_capture_create_outcome_unknown: '+name) from None
            identifier=raw.decode('ascii').strip()
            require(re.fullmatch('[a-f0-9]{64}',identifier) is not None,'legacy_capture_container_invalid')
            def checked_container(observed):
                require(observed['Id']==identifier and observed['Image']==image and observed['Name']=='/'+name
                    and observed['Config']['Labels'].get(OWNER)==token
                    and observed['Config']['User']=='10001:10001'
                    and observed['Config']['Entrypoint']==['python']
                    and observed['Config']['Cmd']==['/qualification/observer.py',image,source_commit]
                    and observed['HostConfig']['NetworkMode']=='none' and observed['HostConfig']['ReadonlyRootfs'] is True,
                    'legacy_capture_container_changed')
                mounts=observed['Mounts']
                require(type(mounts) is list and len(mounts)==2
                    and any(m['Type']=='volume' and m.get('Name')==state_volume and
                            m['Destination']=='/prototype-state' and m['RW'] is False for m in mounts)
                    and any(m['Type']=='bind' and m['Source']==str(folder) and
                            m['Destination']=='/qualification' and m['RW'] is False for m in mounts),
                    'legacy_capture_mount_changed')
            observed=docker.inspect(identifier);checked_container(observed)
            require(observed['State']['Running'] is False,'legacy_capture_already_running')
            result=docker.command('container','start','--attach','--interactive',identifier,
                                  input_bytes=key,timeout=600,output_limit=1024*1024)
            value=validate_artifact(result,dict(authorityId=authority,
                candidate=dict(imageId=image,sourceCommit=source_commit)),reader_sha256=reader_sha256)
            observed=docker.inspect(identifier);checked_container(observed)
            require(observed['State']['Running'] is False and observed['State']['ExitCode']==0
                    and helper_hashes()==pinned_helpers, 'legacy_capture_incomplete_or_changed')
            return seal_capture(value,key=key,container_id=identifier,state_volume=state_volume,helper_hashes=pinned_helpers)
        finally:
            if identifier is not None:
                item=docker.inspect(identifier)
                require(item['Id']==identifier and item['Name']=='/'+name and item['Image']==image
                        and item['Config']['Labels'].get(OWNER)==token,'legacy_capture_cleanup_identity_changed')
                docker.command('container','rm','--force',identifier,mutation=True,timeout=60,output_limit=4096)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    for option in ('image','source-commit','state-volume','authority','reader-sha256','key-file','output'):
        parser.add_argument('--'+option,required=True)
    args=parser.parse_args(argv)
    key=private_bytes(args.key_file,maximum=32)
    raw=capture(image=args.image,source_commit=args.source_commit,state_volume=args.state_volume,
                authority=args.authority,reader_sha256=args.reader_sha256,key=key)
    target=Path(_absolute_path(args.output))
    require(target.is_absolute() and not target.exists() and not target.is_symlink(), 'new_legacy_receipt_required')
    require(target.parent.resolve()==target.parent and not any(path.is_symlink() for path in target.parent.parents),
            'original_private_receipt_parent_required')
    parent=target.parent.stat()
    require(parent.st_uid==os.geteuid() and stat.S_IMODE(parent.st_mode)==0o700,'private_legacy_receipt_parent_required')
    fd=os.open(target,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
    try:
        written=os.write(fd,raw);require(written==len(raw),'legacy_receipt_write_failed');os.fsync(fd)
    finally: os.close(fd)
    parentfd=os.open(target.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(parentfd)
    finally:os.close(parentfd)
    print('legacy delivery observation captured; SHA256='+hashlib.sha256(raw).hexdigest())
    return 0


if __name__=='__main__':raise SystemExit(main())
