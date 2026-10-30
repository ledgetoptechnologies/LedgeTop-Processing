"""Prove a container is absent from one pinned local daemon.

A failed ``docker inspect`` alone is not proof of absence: the daemon may be
unreachable, the call may fail for another reason, or the CLI may be talking
to a different daemon through ``DOCKER_HOST``, ``DOCKER_CONTEXT`` or a saved
context. So every call here:

- names the daemon explicitly with ``--host`` (default: the local
  ``unix:///var/run/docker.sock``; only a local unix socket or, for the Windows
  test host, a local named pipe is accepted);
- runs with every ``DOCKER_*`` variable removed and ``DOCKER_CONFIG`` pointed at
  a fresh empty directory, so no context, config or plugin can redirect it.

It exits 0 only when:

1. ``docker info`` succeeds and reports a daemon ID, equal to
   ``--expect-daemon-id`` when one is given (recorded in step 0);
2. ``docker ps -a --no-trunc`` succeeds and does not list the ID; and
3. ``docker container inspect`` fails *specifically* with "No such container".

It exits 1 when the container is present, and 2 when absence cannot be
established. Read-only: it never changes Docker state.
"""

import argparse
import json
import os
import re
import subprocess
import tempfile

CONTAINER_ID = re.compile(r'[0-9a-f]{64}')
LOCAL_HOST = re.compile(r'unix:///[^\s]+|npipe:////\./pipe/[A-Za-z0-9_.-]+')
DEFAULT_HOST = 'unix:///var/run/docker.sock'


def _environment(config_dir):
    environment = {key: value for key, value in os.environ.items() if not key.upper().startswith('DOCKER_')}
    environment['DOCKER_CONFIG'] = config_dir
    return environment


def _run(docker, host, environment, *arguments):
    try:
        result = subprocess.run([*docker, '--host', host, *arguments], capture_output=True, text=True,
                                timeout=60, env=environment)
    except (OSError, subprocess.TimeoutExpired) as error:
        return None, '', type(error).__name__
    return result.returncode, result.stdout, result.stderr


def check(container_id, docker=('docker',), host=DEFAULT_HOST, expect_daemon_id=None):
    if not CONTAINER_ID.fullmatch(container_id or ''):
        return 2, {'absent': False, 'reason': 'full 64-hex container ID required'}
    if not LOCAL_HOST.fullmatch(host or ''):
        return 2, {'absent': False, 'reason': 'only a local unix socket or named pipe may be queried'}
    with tempfile.TemporaryDirectory() as config_dir:
        environment = _environment(config_dir)
        status, out, _ = _run(docker, host, environment, 'info', '--format', '{{.ID}}')
        daemon = out.strip()
        if status != 0 or not daemon:
            return 2, {'absent': False, 'reason': 'daemon unavailable'}
        if expect_daemon_id is not None and daemon != expect_daemon_id:
            return 2, {'absent': False, 'reason': 'unexpected daemon'}
        status, out, _ = _run(docker, host, environment, 'ps', '-a', '--no-trunc', '--format', '{{.ID}}')
        if status != 0:
            return 2, {'absent': False, 'reason': 'container census failed'}
        listed = [line.strip() for line in out.splitlines() if line.strip()]
        if any(not CONTAINER_ID.fullmatch(item) for item in listed):
            return 2, {'absent': False, 'reason': 'container census unparseable'}
        if container_id in listed:
            return 1, {'absent': False, 'reason': 'listed', 'containersListed': len(listed)}
        status, out, err = _run(docker, host, environment, 'container', 'inspect', '--format', '{{.Id}}',
                                container_id)
        if status == 0:
            return 1, {'absent': False, 'reason': 'inspect found it', 'containersListed': len(listed)}
        # An unrelated or mixed error is not evidence about this exact ID.
        expected = re.compile(r'(?:Error:|Error response from daemon:) No such (?:container|object): ' + re.escape(container_id), re.IGNORECASE)
        if status is None or out.strip() or not expected.fullmatch(err.strip()):
            return 2, {'absent': False, 'reason': 'inspect failed without a not-found answer'}
        return 0, {'absent': True, 'containersListed': len(listed), 'host': host}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('container_id')
    parser.add_argument('--host', default=DEFAULT_HOST, help='local daemon socket (default %(default)s)')
    parser.add_argument('--expect-daemon-id', help='daemon ID recorded before the cutover')
    parser.add_argument('--docker', default='docker', help='docker executable (tests use a stub)')
    args = parser.parse_args(argv)
    status, report = check(args.container_id, docker=args.docker.split(), host=args.host,
                           expect_daemon_id=args.expect_daemon_id)
    print(json.dumps(report))
    return status


if __name__ == '__main__':
    raise SystemExit(main())
