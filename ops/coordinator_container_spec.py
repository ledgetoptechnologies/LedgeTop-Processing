"""Derive a candidate coordinator ``docker create`` argv from the old container.

Reads saved ``docker inspect`` output and executes nothing. Every HostConfig
field is either carried exactly, replaced deliberately (image, state volume,
name, network, ports, restart policy) or proven to be a Docker default; any
other value refuses. Environment values are written only to a private env
file; the printed summary names keys, never values.
"""

import argparse
import json
import re
import os
from pathlib import Path
import sys

CARRIED = ('ReadonlyRootfs', 'Tmpfs', 'Memory', 'MemorySwap', 'MemoryReservation', 'NanoCpus',
           'CpuQuota', 'CpuPeriod', 'CpuShares', 'CpusetCpus', 'PidsLimit', 'CapAdd', 'CapDrop',
           'SecurityOpt', 'Init', 'Ulimits', 'OomKillDisable', 'ExtraHosts', 'Dns')
REPLACED = ('Binds', 'Mounts', 'NetworkMode', 'PortBindings', 'RestartPolicy', 'AutoRemove')
DEFAULTS = {'IpcMode': ('private', 'shareable'), 'CgroupnsMode': ('private', 'host'),
            'ShmSize': (67108864,), 'Runtime': ('runc',)}
IGNORED = ('MaskedPaths', 'ReadonlyPaths', 'ConsoleSize', 'LogConfig')


class SpecError(ValueError):
    pass


def _empty(value):
    return value in (None, False, 0, '', [], {})


def _args(name, value):
    flag = {'ReadonlyRootfs': '--read-only', 'Init': '--init', 'OomKillDisable': '--oom-kill-disable'}
    if name in flag:
        return [flag[name]] if value else []
    if name == 'Tmpfs':
        return [arg for path, options in sorted(value.items())
                for arg in ('--tmpfs', path + (':' + options if options else ''))]
    if name == 'Memory':
        return ['--memory', str(value)]
    if name == 'MemorySwap':
        return ['--memory-swap', str(value)]
    if name == 'MemoryReservation':
        return ['--memory-reservation', str(value)]
    if name == 'NanoCpus':
        return ['--cpus', '%.9g' % (value / 1e9)]
    if name == 'CpuQuota':
        return ['--cpu-quota', str(value)]
    if name == 'CpuPeriod':
        return ['--cpu-period', str(value)]
    if name == 'CpuShares':
        return ['--cpu-shares', str(value)]
    if name == 'CpusetCpus':
        return ['--cpuset-cpus', value]
    if name == 'PidsLimit':
        return ['--pids-limit', str(value)]
    if name in ('CapAdd', 'CapDrop', 'SecurityOpt', 'ExtraHosts', 'Dns'):
        option = {'CapAdd': '--cap-add', 'CapDrop': '--cap-drop', 'SecurityOpt': '--security-opt',
                  'ExtraHosts': '--add-host', 'Dns': '--dns'}[name]
        return [arg for item in value for arg in (option, item)]
    if name == 'Ulimits':
        return [arg for item in value
                for arg in ('--ulimit', '%s=%d:%d' % (item['Name'], item['Soft'], item['Hard']))]
    raise SpecError('unsupported carried field ' + name)


LEASE_DESTINATION = '/run/ledgetop-authority-lease'
ADDED_KEYS = ('PROCESSING_AUTHORITY_LEASE_DIR', 'PROCESSING_VIEWER_PRODUCTION_ADMISSION')


def derive(container, old_image, new_image, *, mode, volume, name, network=None, lease_dir=None, lease_volume=None,
           production_admission=None, accept_compose_source=False):
    if mode not in ('isolated', 'promoted'):
        raise SpecError('mode must be isolated or promoted')
    host, config = container['HostConfig'], container['Config']
    labels = config.get('Labels') or {}
    if any(key.startswith('com.docker.compose.') for key in labels) and not accept_compose_source:
        raise SpecError('compose-managed container: pass accept_compose_source to recreate it outside compose')
    refused = sorted(key for key, value in host.items()
                     if key not in CARRIED + REPLACED + IGNORED
                     and not (key in DEFAULTS and value in DEFAULTS[key]) and not _empty(value))
    if refused:
        raise SpecError('unreviewed non-default HostConfig: ' + ','.join(refused))
    log = host.get('LogConfig') or {}
    # Carried exactly: only local, option-free drivers (compose may record the
    # daemon default, e.g. "local"); anything that ships logs elsewhere refuses.
    if log.get('Type') not in (None, '', 'json-file', 'local') or log.get('Config'):
        raise SpecError('unreviewed logging configuration')
    mounts = container.get('Mounts') or []
    if len(mounts) != 1 or mounts[0].get('Type') != 'volume' or not mounts[0].get('RW'):
        raise SpecError('exactly one writable named volume mount required')
    # Compose expresses a named volume as a Binds entry "NAME:DEST[:rw]". Accept
    # only that exact form for the one state volume; any host path refuses.
    for entry in host.get('Binds') or []:
        parts = entry.split(':')
        if (len(parts) not in (2, 3) or parts[0] != mounts[0].get('Name') or '/' in parts[0]
                or parts[1] != mounts[0]['Destination'] or parts[2:] not in ([], ['rw'])):
            raise SpecError('bind mounts are not supported')
    for field in ('Entrypoint', 'Cmd', 'Healthcheck', 'WorkingDir', 'User'):
        if config.get(field) != old_image['Config'].get(field):
            raise SpecError('container overrides image ' + field)
        if field != 'User' and old_image['Config'].get(field) != new_image['Config'].get(field):
            raise SpecError('new image changes ' + field + '; review before reuse')
    if config.get('User') != new_image['Config'].get('User'):
        raise SpecError('new image changes User')
    argv = ['docker', 'create', '--name', name,
            '--mount', 'type=volume,source=%s,destination=%s' % (volume, mounts[0]['Destination'])]
    for field in CARRIED:
        if not _empty(host.get(field)):
            argv += _args(field, host[field])
    if lease_dir is not None and lease_volume is not None:
        raise SpecError('choose one lease location: a host directory or a named volume')
    if lease_volume is not None:
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', lease_volume) or lease_volume == volume:
            raise SpecError('lease volume must be a plain volume name distinct from the state volume')
        argv += ['--mount', 'type=volume,source=%s,destination=%s' % (lease_volume, LEASE_DESTINATION)]
    if lease_dir is not None:
        if not lease_dir.startswith('/') or ',' in lease_dir or '=' in lease_dir:
            raise SpecError('lease directory must be an absolute host path without commas or equals signs')
        argv += ['--mount', 'type=bind,source=%s,destination=%s' % (lease_dir, LEASE_DESTINATION)]
    if production_admission not in (None, 'open', 'held'):
        raise SpecError('production admission must be open or held')
    networks = sorted((container.get('NetworkSettings') or {}).get('Networks') or {})
    if mode == 'isolated':
        if network != 'none':
            raise SpecError('isolated mode requires --network none: a bridge is reachable from the host')
        argv += ['--network', 'none', '--restart', 'no']
    else:
        if host.get('NetworkMode') in ('host', 'none') or len(networks) != 1:
            raise SpecError('promotion supports exactly one bridge network')
        endpoint = container['NetworkSettings']['Networks'][networks[0]] or {}
        if endpoint.get('IPAMConfig') or endpoint.get('Links'):
            raise SpecError('static network address or links require review')
        argv += ['--network', networks[0]]
        for port, bindings in sorted((host.get('PortBindings') or {}).items()):
            for binding in bindings or []:
                ip = binding.get('HostIp') or ''
                argv += ['--publish', '%s%s:%s' % (ip + ':' if ip else '', binding['HostPort'], port)]
        policy = host.get('RestartPolicy') or {}
        argv += ['--restart', policy.get('Name') or 'no']
    if log.get('Type'):
        argv += ['--log-driver', log['Type']]
    argv += ['--env-file', '<private env file>', new_image['Id']]
    env = config.get('Env') or []
    if any('\n' in item or '\r' in item or '=' not in item for item in env):
        raise SpecError('environment cannot be represented in an env file')
    if any(item.split('=', 1)[0] in ADDED_KEYS for item in env):
        raise SpecError('old environment already sets a cutover key; review before reuse')
    image_env = set(new_image['Config'].get('Env') or [])
    added = []
    if lease_dir is not None or lease_volume is not None:
        added.append('PROCESSING_AUTHORITY_LEASE_DIR=' + LEASE_DESTINATION)
    if production_admission is not None:
        added.append('PROCESSING_VIEWER_PRODUCTION_ADMISSION=' + production_admission)
    keys = sorted({item.split('=', 1)[0] for item in env + added})
    return {'argv': argv, 'envKeys': keys, 'oldNetworks': networks,
            'envFile': [item for item in env if item not in image_env] + added}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for option in ('container', 'old-image', 'new-image'):
        parser.add_argument('--' + option, required=True, type=Path, help='saved docker inspect JSON')
    parser.add_argument('--mode', required=True, choices=('isolated', 'promoted'))
    parser.add_argument('--volume', required=True)
    parser.add_argument('--name', required=True)
    parser.add_argument('--network')
    parser.add_argument('--env-out', required=True, type=Path)
    parser.add_argument('--lease-dir', help='host directory holding the authority lease')
    parser.add_argument('--lease-volume', help='named volume holding the authority lease (same daemon only)')
    parser.add_argument('--production-admission', choices=('open', 'held'))
    parser.add_argument('--accept-compose-source', action='store_true',
                        help='recreate a compose-managed container outside compose (labels are not carried)')
    args = parser.parse_args(argv)
    load = lambda path: json.loads(path.read_text())[0]
    try:
        spec = derive(load(args.container), load(args.old_image), load(args.new_image), mode=args.mode,
                      volume=args.volume, name=args.name, network=args.network, lease_dir=args.lease_dir, lease_volume=args.lease_volume,
                      production_admission=args.production_admission,
                      accept_compose_source=args.accept_compose_source)
    except SpecError as error:
        print('refused: ' + str(error), file=sys.stderr)
        return 2
    os.umask(0o077)
    descriptor = os.open(args.env_out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        stream.write(''.join(item + '\n' for item in spec['envFile']))
        stream.flush()
        os.fsync(stream.fileno())
    spec['argv'][spec['argv'].index('<private env file>')] = str(args.env_out)
    json.dump({'argv': spec['argv'], 'envKeys': spec['envKeys'], 'oldNetworks': spec['oldNetworks']},
              sys.stdout, indent=1)
    sys.stdout.write('\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
