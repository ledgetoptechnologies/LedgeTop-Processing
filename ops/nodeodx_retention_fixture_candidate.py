"""Read-only preflight and disabled plan for an isolated NodeODX fixture.

This module does not create containers or volumes. Production tasks and
credentials must never be supplied to a future fixture.
"""
import argparse
import hashlib
import json
import os
import re
import subprocess

IMAGES = frozenset({
    'sha256:5ce31be03711d280e7f97ee3820dd982b68227181165b3f430b037447c3d13b6',
    'sha256:b9d9a1a4603db9f092b790a6e58586fcf944758b7ca1149daefb2bd55dc6bdde',
})
DOCKER = '/usr/bin/docker'
HOST = 'unix:///var/run/docker.sock'


def _read(args, runner=subprocess.run):
    # No image pull or object mutation. Do not print raw Docker metadata:
    # default command and user strings may contain private configuration.
    env = os.environ.copy()
    env.pop('DOCKER_HOST', None)
    env.pop('DOCKER_CONTEXT', None)
    result = runner([DOCKER, '--host', HOST, *args], env=env, text=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    timeout=8, check=True)
    if len(result.stdout.encode()) > 65536:
        raise ValueError('Docker metadata too large')
    return result.stdout.strip()


def startup_digest(user, working_dir, entrypoint, command, volumes):
    if (not isinstance(user, str) or not isinstance(working_dir, str) or
            not all(x is None or isinstance(x, list) and all(isinstance(y, str) for y in x)
                    for x in (entrypoint, command)) or volumes not in (None, {})):
        raise ValueError('unexpected image startup metadata')
    value = {'User': user, 'WorkingDir': working_dir, 'Entrypoint': entrypoint,
             'Cmd': command, 'Volumes': volumes}
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def preflight(image, expected_engine_id, expected_startup_sha256, *, runner=subprocess.run):
    if image not in IMAGES:
        raise ValueError('image is not a reviewed exact candidate')
    if not isinstance(expected_engine_id, str) or not re.fullmatch(r'[A-Za-z0-9:_-]{1,128}', expected_engine_id):
        raise ValueError('exact expected Engine ID required')
    if not isinstance(expected_startup_sha256, str) or not re.fullmatch(r'[a-f0-9]{64}', expected_startup_sha256):
        raise ValueError('independently approved startup digest required')
    engine = _read(['info', '--format', '{{.ID}}'], runner)
    if engine != expected_engine_id:
        raise ValueError('Docker Engine identity differs')
    template = '[{{json .Id}},{{json .Os}},{{json .Architecture}},{{json .Config.User}},{{json .Config.WorkingDir}},{{json .Config.Entrypoint}},{{json .Config.Cmd}},{{json .Config.Volumes}}]'
    metadata = json.loads(_read(['image', 'inspect', image, '--format', template], runner))
    if not isinstance(metadata, list) or len(metadata) != 8:
        raise ValueError('unexpected image inspection')
    image_id, system, arch, user, working_dir, entrypoint, command, volumes = metadata
    if image_id != image or system != 'linux' or arch not in ('amd64', 'x86_64'):
        raise ValueError('local image identity or platform not proven')
    if volumes not in (None, {}):
        raise ValueError('image declares unreviewed volumes')
    if working_dir != '/var/www':
        raise ValueError('unreviewed image working directory')
    if startup_digest(user, working_dir, entrypoint, command, volumes) != expected_startup_sha256:
        raise ValueError('image startup identity differs')
    return {'engineMatches': True, 'imageMatches': True, 'platform': 'linux-amd64',
            'workingDirectoryMatches': True, 'declaredVolumes': False,
            'runtimeQualified': False}


def fixture_names(image, owner):
    """Describe fixture-owned names only; this does not create resources."""
    if image not in IMAGES or not isinstance(owner, str) or not re.fullmatch(r'[a-f0-9]{32}', owner):
        raise ValueError('exact image and unique fixture owner required')
    return {'container': 'ltds-nodeodx-test-' + owner, 'volume': 'ltds-nodeodx-data-' + owner}


def verify_owned_container(container, volume, image, owner, expected_startup_sha256, expected_env):
    """Fail closed on a narrow synthetic inspect shape, not an OS fence proof."""
    names = fixture_names(image, owner)
    if not isinstance(container, dict) or not isinstance(volume, dict):
        raise ValueError('missing owned resource inspection')
    host, config = container.get('HostConfig'), container.get('Config')
    if not isinstance(host, dict) or not isinstance(config, dict):
        raise ValueError('missing container configuration')
    if (not isinstance(expected_env, (list, tuple)) or not expected_env or
            any(not isinstance(x, str) or '=' not in x for x in expected_env) or
            len({x.split('=', 1)[0] for x in expected_env}) != len(expected_env) or
            'NVIDIA_VISIBLE_DEVICES=void' not in expected_env or config.get('Env') != list(expected_env)):
        raise ValueError('container environment differs from approved private policy')
    if (not isinstance(expected_startup_sha256, str) or not re.fullmatch(r'[a-f0-9]{64}', expected_startup_sha256) or
            startup_digest(config.get('User'), config.get('WorkingDir'), config.get('Entrypoint'),
                           config.get('Cmd'), config.get('Volumes')) != expected_startup_sha256):
        raise ValueError('container startup identity differs')
    if (not re.fullmatch(r'[a-f0-9]{64}', str(container.get('Id', ''))) or
            container.get('Name') != '/' + names['container'] or container.get('Image') != image or
            (config.get('Labels') or {}).get('ltds.fixture.owner') != owner or
            volume.get('Name') != names['volume'] or volume.get('Driver') != 'local' or volume.get('Scope') != 'local' or
            not isinstance(volume.get('Mountpoint'), str) or not volume['Mountpoint'].startswith('/') or
            (volume.get('Labels') or {}).get('ltds.fixture.owner') != owner or volume.get('Options') not in (None, {})):
        raise ValueError('owned resource identity mismatch')
    if (host.get('NetworkMode') != 'none' or host.get('Runtime') != 'runc' or
            host.get('Privileged') is not False or (host.get('RestartPolicy') or {}).get('Name') != 'no' or
            host.get('Devices') not in (None, []) or host.get('DeviceRequests') not in (None, []) or
            host.get('DeviceCgroupRules') not in (None, []) or host.get('Binds') not in (None, []) or
            host.get('PortBindings') not in (None, {}) or host.get('PublishAllPorts') is not False or host.get('PidMode') not in (None, '') or
            host.get('IpcMode') == 'host' or host.get('UsernsMode') not in (None, '') or host.get('CgroupnsMode') != 'private' or
            host.get('Sysctls') not in (None, {}) or host.get('GroupAdd') not in (None, []) or host.get('CapAdd') not in (None, [])):
        raise ValueError('unreviewed network, runtime, device or privilege')
    if (host.get('Memory') != 1024 ** 3 or host.get('NanoCpus') != 10 ** 9 or host.get('PidsLimit') != 128 or
            'ALL' not in (host.get('CapDrop') or []) or
            host.get('SecurityOpt') not in (['no-new-privileges'], ['no-new-privileges:true']) or
            (host.get('LogConfig') or {}).get('Type') != 'none'):
        raise ValueError('unreviewed resource or capability limits')
    expected_tmpfs = {'/var/www/tmp': 'rw,nosuid,nodev,size=128m,mode=1777',
                      '/var/www/logs': 'rw,nosuid,nodev,size=16m,mode=1777',
                      '/tmp': 'rw,nosuid,nodev,size=128m,mode=1777'}
    if host.get('Tmpfs') != expected_tmpfs:
        raise ValueError('unreviewed temporary mounts')
    requested_mounts = host.get('Mounts')
    if not isinstance(requested_mounts, list) or len(requested_mounts) != 1:
        raise ValueError('unreviewed requested mounts')
    requested = requested_mounts[0]
    if (not isinstance(requested, dict) or requested.get('Type') != 'volume' or
            requested.get('Source') != names['volume'] or requested.get('Target') != '/var/www/data' or
            requested.get('ReadOnly') not in (None, False) or
            not isinstance(requested.get('VolumeOptions'), dict) or requested['VolumeOptions'].get('NoCopy') is not True):
        raise ValueError('unreviewed requested data volume')
    networks = container.get('NetworkSettings')
    if (not isinstance(networks, dict) or networks.get('Ports') not in (None, {}) or
            not isinstance(networks.get('Networks'), dict) or set(networks['Networks']) != {'none'}):
        raise ValueError('unreviewed network attachments')
    mounts = container.get('Mounts')
    if not isinstance(mounts, list) or len(mounts) != 1:
        raise ValueError('unreviewed mounts')
    mount = mounts[0]
    if (not isinstance(mount, dict) or mount.get('Type') != 'volume' or mount.get('Name') != names['volume'] or
            mount.get('Source') != volume['Mountpoint'] or mount.get('Driver') != 'local' or
            mount.get('Destination') != '/var/www/data' or mount.get('RW') is not True):
        raise ValueError('unreviewed data volume')
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--image', required=True, choices=sorted(IMAGES))
    parser.add_argument('--engine-id', required=True)
    parser.add_argument('--startup-sha256', required=True)
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    if args.run:
        raise SystemExit('fixture execution disabled; review and lifecycle implementation required')
    print(json.dumps(preflight(args.image, args.engine_id, args.startup_sha256), sort_keys=True))


if __name__ == '__main__':
    main()
