"""Prove a container is absent: a successful daemon census AND a confirmed not-found.

A failed ``docker inspect`` alone is not proof of absence: the daemon may be
unreachable, or the call may fail for another reason. This exits 0 only when:

1. ``docker info`` succeeds and reports a daemon ID;
2. ``docker ps -a --no-trunc`` succeeds and does not list the ID; and
3. ``docker container inspect`` fails *specifically* with "No such container".

It exits 1 when the container is present, and 2 when absence cannot be
established. Read-only: it never changes Docker state.
"""

import argparse
import json
import re
import subprocess
import sys

CONTAINER_ID = re.compile(r'[0-9a-f]{64}')
NOT_FOUND = re.compile(r'no such (container|object)', re.IGNORECASE)


def _run(docker, *arguments):
    try:
        result = subprocess.run([*docker, *arguments], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as error:
        return None, '', type(error).__name__
    return result.returncode, result.stdout, result.stderr


def check(container_id, docker=('docker',)):
    if not CONTAINER_ID.fullmatch(container_id or ''):
        return 2, {'absent': False, 'reason': 'full 64-hex container ID required'}
    status, out, _ = _run(docker, 'info', '--format', '{{.ID}}')
    if status != 0 or not out.strip():
        return 2, {'absent': False, 'reason': 'daemon unavailable'}
    status, out, _ = _run(docker, 'ps', '-a', '--no-trunc', '--format', '{{.ID}}')
    if status != 0:
        return 2, {'absent': False, 'reason': 'container census failed'}
    listed = [line.strip() for line in out.splitlines() if line.strip()]
    if any(not CONTAINER_ID.fullmatch(item) for item in listed):
        return 2, {'absent': False, 'reason': 'container census unparseable'}
    if container_id in listed:
        return 1, {'absent': False, 'reason': 'listed', 'containersListed': len(listed)}
    status, out, err = _run(docker, 'container', 'inspect', '--format', '{{.Id}}', container_id)
    if status == 0:
        return 1, {'absent': False, 'reason': 'inspect found it', 'containersListed': len(listed)}
    if status is None or not NOT_FOUND.search(err):
        return 2, {'absent': False, 'reason': 'inspect failed without a not-found answer'}
    return 0, {'absent': True, 'containersListed': len(listed)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('container_id')
    parser.add_argument('--docker', default='docker', help='docker executable (tests use a stub)')
    args = parser.parse_args(argv)
    status, report = check(args.container_id, docker=args.docker.split())
    print(json.dumps(report))
    return status


if __name__ == '__main__':
    raise SystemExit(main())
