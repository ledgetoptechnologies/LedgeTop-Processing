"""Publish two private images from main and mark the verified pair.

Both existing Dockerfiles are built and smoke checked before any push. Unique
run/attempt tags avoid replacing live tags. A lost push response is recorded as
uncertain and never automatically retried. Receipts contain no credentials.

After both images are pushed and their remote identity verified, a main push
whose commit is still the head of main moves each package's `verified` tag to
that exact digest. Nothing here touches a host: the node-side auto-deploy step
reads the tag, pulls by digest and updates only through its drain window.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from processing import release_packages  # noqa: E402

REPOSITORY = release_packages.REPOSITORY
# Fixed namespaces are provisioned private and verified before publication.
PACKAGES = tuple(item['name'] for item in release_packages.PACKAGES)
PACKAGE_KEYS = tuple(item['key'] for item in release_packages.PACKAGES)
BUILD_ROOTS = tuple(item['buildRoot'] for item in release_packages.PACKAGES)
VERIFIED_TAG = release_packages.VERIFIED_TAG


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


class PackageVerificationError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def private_package(value):
    if value is None:
        raise PackageVerificationError('missing_or_inaccessible', 'package metadata unavailable')
    if not isinstance(value, dict):
        raise PackageVerificationError('metadata_invalid', 'package metadata invalid')
    if value.get('visibility') != 'private':
        raise PackageVerificationError('not_private', 'private package visibility is required')
    repository = value.get('repository')
    if not isinstance(repository, dict) or repository.get('full_name') != REPOSITORY:
        raise PackageVerificationError('unlinked_or_wrong_repository', 'package must be linked to LedgeTop-Processing')


def source_repository(value):
    # Source and package visibility are distinct and independently verified.
    if not isinstance(value, dict) or value.get('full_name') != REPOSITORY:
        raise ValueError('exact source repository required')
    return value.get('visibility')


def receipt(value):
    path = Path('publish-receipt.json')
    temporary = path.with_suffix('.tmp')
    with temporary.open('w', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def verify_package(state, package, stage):
    state.update(stage=stage, currentPackage=package)
    receipt(state)
    private_package(github('/orgs/ledgetoptechnologies/packages/container/' + package))


def labels(key):
    env = os.environ
    return {release_packages.SOURCE_LABEL: release_packages.SOURCE_URL,
            release_packages.REVISION_LABEL: env['GITHUB_SHA'],
            release_packages.PACKAGE_LABEL: key,
            release_packages.RUN_LABEL: env['GITHUB_RUN_ID'],
            release_packages.REF_LABEL: env['GITHUB_REF'],
            release_packages.EVENT_LABEL: env['GITHUB_EVENT_NAME']}


def remote_digest(reference):
    manifest = json.loads(command(['docker', 'buildx', 'imagetools', 'inspect',
        reference, '--format', '{{json .Manifest}}'], capture=True))
    digest = manifest.get('digest')
    if not isinstance(digest, str) or not re.fullmatch('sha256:[0-9a-f]{64}', digest):
        raise ValueError('remote manifest digest unverified')
    return digest


def main_head():
    branch = github('/repos/' + REPOSITORY + '/branches/main')
    head = branch.get('commit', {}).get('sha') if isinstance(branch, dict) else None
    if not isinstance(head, str) or not re.fullmatch('[0-9a-f]{40}', head):
        raise ValueError('main head unavailable')
    return head


def promote(state):
    """Move each package's `verified` tag to its verified digest from main's head.

    Packages activate independently: each tag names one image that was itself
    built, smoke tested and identity-verified, and a host follows exactly one
    package. A stop between the two tags (superseded head or failed push) can
    leave only the first package promoted; the receipt says which. Hosts never
    move backwards, so the next main run promotes both again.
    """
    if os.environ.get('GITHUB_EVENT_NAME') != 'push':
        state['promotion'] = {'status': 'skipped_not_main_push'}
        receipt(state)
        return
    if [image.get('status') for image in state['images']] != ['verified'] * len(PACKAGES):
        raise ValueError('promotion requires the whole verified pair')
    promoted = []
    try:
        for image in state['images']:
            # Checked again per package: a newer main commit has its own run.
            state.update(stage='promotion_main_head', currentPackage=image['package'])
            receipt(state)
            head = main_head()
            if head != state['sourceCommit']:
                image['promotion'] = 'skipped_superseded'
                state['promotion'] = {'status': 'partial_superseded' if promoted else 'skipped_superseded',
                                      'mainHead': head, 'promoted': promoted}
                receipt(state)
                return
            verify_package(state, image['package'], 'promotion_package_metadata')
            verified = image['reference'].rsplit(':', 1)[0] + ':' + VERIFIED_TAG
            image.update(verifiedTag=verified, promotion='push_response_unconfirmed')
            state['stage'] = 'promotion_push'
            receipt(state)
            command(['docker', 'tag', image['reference'], verified])
            command(['docker', 'push', verified])
            state['stage'] = 'promotion_digest'
            receipt(state)
            if remote_digest(verified) != image['digest']:
                raise ValueError('verified tag does not name the verified digest')
            image['promotion'] = 'verified'
            promoted.append(image['package'])
            receipt(state)
    except Exception:
        state['promotion'] = {'status': 'partial' if promoted else 'failed', 'promoted': promoted}
        raise
    state['promotion'] = {'status': 'promoted', 'tag': VERIFIED_TAG, 'promoted': promoted}
    receipt(state)


def main():
    tag = context(os.environ)
    state = {'repository': REPOSITORY, 'sourceCommit': os.environ['GITHUB_SHA'],
             'tag': tag, 'deploysRuntime': False, 'verifiedTag': VERIFIED_TAG, 'stage': 'preflight', 'images': []}
    receipt(state)
    try:
        state.update(stage='preflight_repository_metadata')
        receipt(state)
        repository = github('/repos/' + REPOSITORY)
        state['sourceRepositoryVisibility'] = source_repository(repository)
        for package in PACKAGES:
            verify_package(state, package, 'preflight_package_metadata')
        for package, key, build_root in zip(PACKAGES, PACKAGE_KEYS, BUILD_ROOTS):
            reference = release_packages.REGISTRY + '/' + package + ':' + tag
            state.update(stage='build', currentPackage=package)
            receipt(state)
            command(['docker', 'build', '--platform', 'linux/amd64', '--pull=false',
                *[part for name, value in sorted(labels(key).items()) for part in ('--label', name + '=' + value)],
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
        # A change to either namespace during the build must stop the pair
        # before even the first registry write.
        for package in PACKAGES:
            verify_package(state, package, 'prepush_pair_metadata')
        for image in state['images']:
            verify_package(state, image['package'], 'prepush_package_metadata')
            state.update(stage='push_intent', currentPackage=image['package'])
            image['status'] = 'push_response_unconfirmed'
            receipt(state)
            command(['docker', 'push', image['reference']])
            image['status'] = 'push_returned'
            state['stage'] = 'push_returned'
            receipt(state)
            state['stage'] = 'postpush_digest'
            receipt(state)
            digest = remote_digest(image['reference'])
            image['digest'] = digest
            image['immutableReference'] = image['reference'].rsplit(':', 1)[0] + '@' + digest
            verify_package(state, image['package'], 'postpush_package_metadata')
            state['stage'] = 'postpush_image_identity'
            receipt(state)
            command(['docker', 'pull', '--platform', 'linux/amd64', image['immutableReference']])
            remote = json.loads(command(['docker', 'image', 'inspect', image['immutableReference']], capture=True))[0]
            found = remote['Config'].get('Labels') or {}
            expected = labels(PACKAGE_KEYS[PACKAGES.index(image['package'])])
            if (remote['Id'] != image['localImageId'] or remote['Os'] != 'linux'
                    or remote['Architecture'] != 'amd64' or remote['Config']['User'] != '10001:10001'
                    or any(found.get(name) != value for name, value in expected.items())):
                raise ValueError('remote image identity unverified')
            image.update(remoteImageId=remote['Id'], imageIdentityVerified=True,
                         status='verified', visibility='private', linkedRepository=REPOSITORY)
            receipt(state)
        promote(state)
        state.update(stage='completed', status='succeeded')
        receipt(state)
    except Exception as error:
        stage = state['stage']
        if isinstance(error, PackageVerificationError):
            state['failureCode'] = ({'postpush_package_metadata': 'postpush_package_',
                                     'prepush_pair_metadata': 'prepush_package_',
                                     'prepush_package_metadata': 'prepush_package_',
                                     'promotion_package_metadata': 'promotion_package_'}.get(stage, 'preflight_package_')) + error.code
        else:
            state['failureCode'] = {
                'preflight_repository_metadata': 'preflight_repository_metadata_unavailable',
                'preflight_package_metadata': 'preflight_package_metadata_unavailable',
                'prepush_pair_metadata': 'prepush_package_metadata_unavailable',
                'prepush_package_metadata': 'prepush_package_metadata_unavailable',
                'push_intent': 'push_response_unconfirmed',
                'push_returned': 'push_returned_receipt_unavailable',
                'postpush_digest': 'postpush_digest_unverified',
                'postpush_image_identity': 'postpush_image_identity_unverified',
                'postpush_package_metadata': 'postpush_package_metadata_unavailable',
                'build': 'build_or_image_check_failed',
                'smoke_checks': 'smoke_check_failed',
                'promotion_main_head': 'promotion_main_head_unavailable',
                'promotion_package_metadata': 'promotion_package_metadata_unavailable',
                'promotion_push': 'promotion_push_unconfirmed',
                'promotion_digest': 'promotion_digest_unverified',
            }.get(stage, 'publication_failed')
        state['status'] = 'failed_or_uncertain'
        receipt(state)
        print('Publication stopped. Inspect the retained receipt and original registry references before any new attempt.')
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
