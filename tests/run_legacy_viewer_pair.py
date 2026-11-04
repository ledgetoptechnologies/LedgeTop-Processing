"""Run an isolated, disposable paired fixture; never point this at live services."""
import argparse
import json
import subprocess
import sys
import uuid
from pathlib import Path

IMAGE = 'sha256:e7add2a490bb17d97b485b8641bd88455fff7839ade0f5eba493c3508124d413'
PROCESSING = '6912439f8c859bf60a9d3281014f27de0e5a78a7'
VIEWER = 'ad0bf46611de44545825db9147e90b68195902ab'
FIXTURE_FILES = {'tests/legacy_viewer_pair_server.py', 'tests/legacy_viewer_pair_client.js',
                 'tests/legacy_viewer_pair_runner.js', 'tests/run_legacy_viewer_pair.py',
                 'docs/legacy-viewer-paired-fixture.md'}


def run(argv, *, cwd=None, timeout=15):
    return subprocess.run(argv, cwd=cwd, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout, check=True).stdout.strip()


def check_source(path, expected, *, fixture=False):
    if not path.is_absolute() or not path.is_dir():
        raise ValueError('source must be an existing absolute directory')
    head = run(['git', 'rev-parse', 'HEAD'], cwd=path)
    if head != expected:
        if not fixture:
            raise ValueError('unexpected source revision')
        run(['git', 'merge-base', '--is-ancestor', expected, 'HEAD'], cwd=path)
        changed = set(run(['git', 'diff', '--name-only', expected, 'HEAD'], cwd=path).splitlines())
        if changed != FIXTURE_FILES:
            raise ValueError('unexpected fixture commit contents')
    run(['git', 'diff', '--quiet'], cwd=path)
    run(['git', 'diff', '--cached', '--quiet'], cwd=path)
    untracked = set(run(['git', 'ls-files', '--others', '--exclude-standard'], cwd=path).splitlines())
    if untracked != (FIXTURE_FILES if fixture and head == expected else set()):
        raise ValueError('unexpected untracked source files')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--processing', type=Path, required=True)
    parser.add_argument('--viewer', type=Path, required=True)
    parser.add_argument('--npm-cache', type=Path, required=True, help='existing npm cache, mounted read-only')
    args = parser.parse_args()
    check_source(args.processing, PROCESSING, fixture=True)
    check_source(args.viewer, VIEWER)
    if not args.npm_cache.is_absolute() or not args.npm_cache.is_dir():
        raise ValueError('npm cache must be an existing absolute directory')
    owner = uuid.uuid4().hex
    name = 'ltds-legacy-pair-' + owner
    command = ['docker', 'create', '--name', name, '--label', 'ltds.fixture.owner=' + owner,
               '--network', 'none', '--read-only', '--memory', '512m', '--cpus', '1',
               '--pids-limit', '96', '--tmpfs', '/tmp:rw,nosuid,nodev,size=512m,mode=1777',
               '--user', '10001:10001', '--workdir', '/processing',
               '--mount', f'type=bind,src={args.processing},dst=/processing,readonly',
               '--mount', f'type=bind,src={args.viewer},dst=/viewer,readonly',
               '--mount', f'type=bind,src={args.npm_cache},dst=/npm-cache,readonly',
               '--env', 'NODE_PATH=/tmp/deps/node_modules', '--entrypoint', 'sh', IMAGE, '-c',
               'mkdir /tmp/deps && cp /viewer/apps/viewer/package.json /viewer/apps/viewer/package-lock.json /tmp/deps/ '
               '&& npm ci --prefix /tmp/deps --omit=dev --offline --ignore-scripts --no-audit --no-fund --cache /npm-cache --logs-max=0 '
               '&& mkdir /tmp/deps/apps && cp -R /viewer/apps/viewer /tmp/deps/apps/viewer '
               '&& exec node tests/legacy_viewer_pair_runner.js']
    # If create times out, ownership is ambiguous; retain and report the name.
    try:
        identifier = run(command, timeout=20)
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        print('Create failed or was ambiguous; inspect retained name:', name)
        raise
    def verify():
        obj = json.loads(run(['docker', 'inspect', identifier]))[0]
        if (obj['Id'] != identifier or obj['Name'] != '/' + name or obj['Image'] != IMAGE
                or obj['Config']['Labels'].get('ltds.fixture.owner') != owner):
            raise RuntimeError('fixture ownership changed; retain container')
        return obj
    try:
        verify()
        try:
            print(run(['docker', 'start', '-a', identifier], timeout=60))
        except subprocess.CalledProcessError as error:
            print(error.stdout, error.stderr, file=sys.stderr)
            raise
        except subprocess.TimeoutExpired:
            verify()
            run(['docker', 'stop', '--time', '3', identifier], timeout=12)
            raise
        state = verify()['State']
        if state['Running'] or state['ExitCode'] != 0:
            raise RuntimeError('fixture did not exit successfully')
    finally:
        original = sys.exc_info()[1]
        try:
            obj = verify()
            if obj['State']['Running']:
                run(['docker', 'stop', '--time', '3', identifier], timeout=12)
            verify()
            run(['docker', 'rm', identifier], timeout=15)
        except Exception as cleanup_error:
            if original is None:
                raise
            print('Fixture cleanup failed; inspect exact owned container:', identifier, cleanup_error, file=sys.stderr)


if __name__ == '__main__':
    main()
