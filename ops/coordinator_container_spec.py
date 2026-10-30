"""Derive a candidate coordinator ``docker create`` argv from the old container.

Reads saved ``docker inspect`` output and executes nothing. Every HostConfig
field is either carried exactly, replaced deliberately (image, state volume,
name, network, ports, restart policy) or proven to be a Docker default; any
other value refuses. Environment values are written only to a private env
file; the printed summary names keys, never values.
"""

import argparse
import json
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


def derive(container, old_image, new_image, *, mode, volume, name, network=None):
    if mode not in ('isolated', 'promoted'):
        raise SpecError('mode must be isolated or promoted')
    host, config = container['HostConfig'], container['Config']
    labels = config.get('Labels') or {}
    if any(key.startswith('com.docker.compose.') for key in labels):
        raise SpecError('compose-managed container: recreate through compose instead')
    refused = sorted(key for key, value in host.items()
                     if key not in CARRIED + REPLACED + IGNORED
                     and not (key in DEFAULTS and value in DEFAULTS[key]) and not _empty(value))
    if refused:
        raise SpecError('unreviewed non-default HostConfig: ' + ','.join(refused))
    log = host.get('LogConfig') or {}
    if log.get('Type') not in (None, '', 'json-file') or log.get('Config'):
        raise SpecError('unreviewed logging configuration')
    if host.get('Binds'):
        raise SpecError('bind mounts are not supported')
    mounts = container.get('Mounts') or []
    if len(mounts) != 1 or mounts[0].get('Type') != 'volume' or not mounts[0].get('RW'):
        raise SpecError('exactly one writable named volume mount required')
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
    networks = sorted((container.get('NetworkSettings') or {}).get('Networks') or {})
    if mode == 'isolated':
        if not network:
            raise SpecError('isolated mode requires its own dedicated network')
        argv += ['--network', network, '--restart', 'no']
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
    argv += ['--env-file', '<private env file>', new_image['Id']]
    env = config.get('Env') or []
    if any('\n' in item or '\r' in item or '=' not in item for item in env):
        raise SpecError('environment cannot be represented in an env file')
    image_env = set(new_image['Config'].get('Env') or [])
    keys = sorted({item.split('=', 1)[0] for item in env})
    return {'argv': argv, 'envKeys': keys, 'oldNetworks': networks,
            'envFile': [item for item in env if item not in image_env]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for option in ('container', 'old-image', 'new-image'):
        parser.add_argument('--' + option, required=True, type=Path, help='saved docker inspect JSON')
    parser.add_argument('--mode', required=True, choices=('isolated', 'promoted'))
    parser.add_argument('--volume', required=True)
    parser.add_argument('--name', required=True)
    parser.add_argument('--network')
    parser.add_argument('--env-out', required=True, type=Path)
    args = parser.parse_args(argv)
    load = lambda path: json.loads(path.read_text())[0]
    try:
        spec = derive(load(args.container), load(args.old_image), load(args.new_image), mode=args.mode,
                      volume=args.volume, name=args.name, network=args.network)
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
