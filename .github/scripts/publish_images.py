"""Publish two private pull-test images from main, never deploy them.

Both existing Dockerfiles are built and smoke checked before any push. Unique
run/attempt tags avoid replacing live tags. A lost push response is recorded as
uncertain and never automatically retried. Receipts contain no credentials.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import urllib.error
import urllib.request

REPOSITORY = 'ledgetoptechnologies/LedgeTop-Processing'
PACKAGES = ('ledgetop-processing-orchestrator', 'ledgetop-processing-treedar')


def context(env):
    if (env.get('GITHUB_REPOSITORY') != REPOSITORY or env.get('GITHUB_REF') != 'refs/heads/main'
            or env.get('GITHUB_EVENT_NAME') not in ('push', 'workflow_dispatch')
            or not re.fullmatch('[0-9a-f]{40}', env.get('GITHUB_SHA', ''))
            or not re.fullmatch('[0-9]+', env.get('GITHUB_RUN_ID', ''))
            or not re.fullmatch('[0-9]+', env.get('GITHUB_RUN_ATTEMPT', ''))):
        raise ValueError('publication requires this repository on main')
    return 'build-' + env['GITHUB_SHA'][:12] + '-' + env['GITHUB_RUN_ID'] + '-' + env['GITHUB_RUN_ATTEMPT']


def command(args, *, capture=False, env=None):
    result = subprocess.run(args, check=True, text=True, env=env,
                            stdout=subprocess.PIPE if capture else None,
                            stderr=subprocess.PIPE if capture else None, timeout=1200)
    return result.stdout if capture else None


def github(path):
    request = urllib.request.Request('https://api.github.com' + path, headers={
        'Authorization': 'Bearer ' + os.environ['GITHUB_TOKEN'],
        'Accept': 'application/vnd.github+json', 'User-Agent': 'LedgeTop-Processing-Publisher'})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise ValueError('oversized metadata response')
            return json.loads(raw)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise ValueError('GitHub metadata permission/read check failed') from None


def private_package(value, *, required=False):
    if value is None and not required:
        return
    if not isinstance(value, dict) or value.get('visibility') != 'private':
        raise ValueError('private package visibility is required')
    linked = (value.get('repository') or {}).get('full_name')
    if (required or linked is not None) and linked != REPOSITORY:
        raise ValueError('package must be linked to LedgeTop-Processing')


def receipt(value):
    path = Path('publish-receipt.json')
    temporary = path.with_suffix('.tmp')
    with temporary.open('w', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def main():
    tag = context(os.environ)
    state = {'repository': REPOSITORY, 'sourceCommit': os.environ['GITHUB_SHA'],
             'tag': tag, 'deploysRuntime': False, 'stage': 'preflight', 'images': []}
    receipt(state)
    try:
        repository = github('/repos/' + REPOSITORY)
        if not repository or not repository.get('private'):
            raise ValueError('private source repository required')
        for package in PACKAGES:
            private_package(github('/orgs/ledgetoptechnologies/packages/container/' + package))
        for package, build_root in zip(PACKAGES, ('.', 'engines/treedar')):
            reference = 'ghcr.io/ledgetoptechnologies/' + package + ':' + tag
            state.update(stage='build', currentPackage=package)
            receipt(state)
            command(['docker', 'build', '--platform', 'linux/amd64', '--pull=false',
                '--label', 'org.opencontainers.image.source=https://github.com/' + REPOSITORY,
                '--label', 'org.opencontainers.image.revision=' + state['sourceCommit'],
                '--tag', reference, build_root])
            item = json.loads(command(['docker', 'image', 'inspect', reference], capture=True))[0]
            if item['Config']['User'] != '10001:10001' or item['Os'] != 'linux' or item['Architecture'] != 'amd64':
                raise ValueError('image identity/platform/non-root check failed')
            state['images'].append({'package': package, 'reference': reference, 'localImageId': item['Id'],
                'dockerfileSha256': hashlib.sha256((Path(build_root) / 'Dockerfile').read_bytes()).hexdigest(),
                'status': 'built'})
        state['stage'] = 'smoke_checks'
        receipt(state)
        common = ['docker', 'run', '--rm', '--network', 'none', '--read-only', '--cpus', '1',
                  '--memory', '512m', '--pids-limit', '64', '--tmpfs', '/tmp:rw,nosuid,nodev,size=64m']
        health = """import os,threading,json,urllib.request
from http.server import ThreadingHTTPServer
from processing.config import from_environment
from processing.app import make_handler
assert os.getuid()==10001
s=ThreadingHTTPServer(('127.0.0.1',0),make_handler(from_environment()))
threading.Thread(target=s.serve_forever,daemon=True).start()
with urllib.request.urlopen('http://127.0.0.1:'+str(s.server_port)+'/healthz',timeout=5) as r:
 assert json.load(r)['status']=='ok'
s.shutdown();s.server_close()
"""
        command([*common, '--env', 'PROCESSING_API_TOKEN', '--entrypoint', 'python',
                 state['images'][0]['reference'], '-c', health], capture=True,
                env={**os.environ, 'PROCESSING_API_TOKEN': secrets.token_urlsafe(40)})
        command([*common, state['images'][1]['reference'], '--help'], capture=True)
        for image in state['images']:
            image['smokeCheckPassed'] = True
        receipt(state)
        for image in state['images']:
            state.update(stage='push_intent', currentPackage=image['package'])
            image['status'] = 'push_response_unconfirmed'
            receipt(state)
            command(['docker', 'push', image['reference']])
            image['status'] = 'push_returned'
            receipt(state)
            manifest = json.loads(command(['docker', 'buildx', 'imagetools', 'inspect',
                image['reference'], '--format', '{{json .Manifest}}'], capture=True))
            digest = manifest.get('digest')
            if not isinstance(digest, str) or not re.fullmatch('sha256:[0-9a-f]{64}', digest):
                raise ValueError('remote manifest digest unverified')
            image['digest'] = digest
            private_package(github('/orgs/ledgetoptechnologies/packages/container/' + image['package']), required=True)
            image.update(status='verified', visibility='private', linkedRepository=REPOSITORY)
            receipt(state)
        state.update(stage='completed', status='succeeded')
        receipt(state)
    except Exception:
        state['status'] = 'failed_or_uncertain'
        receipt(state)
        print('Publication stopped. Inspect the retained receipt and original registry references before any new attempt.')
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
