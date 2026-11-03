"""Run one Docker CLI command against one pinned local daemon, with no overrides.

Every Docker operation in the cutover runbook goes through this, not only the
absence check:

- ``--host`` names the daemon explicitly; only a local unix socket (or, for the
  Windows test host, a local named pipe) is accepted;
- every ``DOCKER_*`` variable is removed and ``DOCKER_CONFIG`` points at a fresh
  empty directory, so no context, config, plugin or credential helper applies;
- before the command, ``docker info`` must report ``--expect-daemon-id`` (the ID
  recorded in step 0). Without that ID, only ``info`` may be run, to record it;
- global CLI options in the passed command are refused, so the command cannot
  re-point itself (``--context``, ``--config``, ``-H`` and so on).

stdin, stdout and stderr pass through, and the command's exit status is
returned. A refusal or a daemon mismatch exits 125 without running it.
"""

import argparse
import os
import subprocess
import sys
import tempfile

try:  # run as a script from the evidence folder, next to container_absent.py
    from container_absent import LOCAL_HOST
except ImportError:  # imported as ops.pinned_docker (tests)
    from ops.container_absent import LOCAL_HOST

REFUSED = 125


def environment(config_dir):
    cleaned = {key: value for key, value in os.environ.items() if not key.upper().startswith('DOCKER_')}
    cleaned['DOCKER_CONFIG'] = config_dir
    return cleaned


def run(arguments, *, host, expect_daemon_id=None, docker=('docker',)):
    if not LOCAL_HOST.fullmatch(host or ''):
        print('pinned docker refused: only a local unix socket or named pipe', file=sys.stderr)
        return REFUSED
    if not arguments or arguments[0].startswith('-'):
        print('pinned docker refused: global options or an empty command', file=sys.stderr)
        return REFUSED
    if expect_daemon_id is None and arguments[0] != 'info':
        print('pinned docker refused: an expected daemon ID is required', file=sys.stderr)
        return REFUSED
    with tempfile.TemporaryDirectory() as config_dir:
        env = environment(config_dir)
        if expect_daemon_id is not None:
            try:
                probe = subprocess.run([*docker, '--host', host, 'info', '--format', '{{.ID}}'],
                                       capture_output=True, text=True, timeout=60, env=env)
            except (OSError, subprocess.TimeoutExpired):
                print('pinned docker refused: daemon unavailable', file=sys.stderr)
                return REFUSED
            if probe.returncode != 0 or probe.stdout.strip() != expect_daemon_id:
                print('pinned docker refused: daemon is not the recorded one', file=sys.stderr)
                return REFUSED
        try:
            return subprocess.run([*docker, '--host', host, *arguments], env=env).returncode
        except OSError:
            print('pinned docker refused: docker executable unavailable', file=sys.stderr)
            return REFUSED


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if '--' not in argv:
        print('usage: pinned_docker.py --host HOST [--expect-daemon-id ID] -- COMMAND...', file=sys.stderr)
        return REFUSED
    split = argv.index('--')
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--host', required=True)
    parser.add_argument('--expect-daemon-id')
    parser.add_argument('--docker', default='docker', help='docker executable (tests use a stub)')
    args = parser.parse_args(argv[:split])
    return run(argv[split + 1:], host=args.host, expect_daemon_id=args.expect_daemon_id,
               docker=args.docker.split())


if __name__ == '__main__':
    raise SystemExit(main())
