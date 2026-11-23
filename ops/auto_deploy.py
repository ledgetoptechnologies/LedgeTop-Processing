"""Node-side auto-deploy of the newest verified main image.

Run by the host's own timer through `node_maintenance.py auto-deploy`. Each run:

1. stops at once if the host-local hold file exists;
2. reads the package's `verified` tag only to learn its digest, pulls that
   exact digest with the host's existing read-only registry login, and checks
   the pulled image ID, platform, user and identity labels (source, package,
   main push, commit, publish run);
3. does nothing unless that run is newer than both the running pin and every
   candidate this host already tried, so it never goes backwards or retries a
   failed candidate;
4. schedules a drain window and hands the update to the existing drain-window
   runner, which drains, replaces, saves the new pin or rolls back.

The hosts still run only sha256 image IDs; the tag is never started. A plan
the agent did not create, a failed/held node and an existing operation receipt
all stop it for a person to inspect. It writes `auto-deploy/state.json` and
`auto-deploy/last-run.json` under the operator root on every run.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess

from processing import release_packages
from processing.container_update import save

HOLD_FILE = 'auto-deploy.hold'
STATE_DIR = 'auto-deploy'
DIGEST = re.compile(r'sha256:[0-9a-f]{64}\Z')
COMMIT = re.compile(r'[0-9a-f]{40}\Z')
RUN = re.compile(r'[1-9][0-9]{0,19}\Z')


class DeployError(Exception):
    """A candidate or host state the agent refuses to act on."""


def docker(args, timeout=900):
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError):
        raise DeployError('Docker did not answer') from None
    if result.returncode:
        raise DeployError('Docker command failed: ' + ' '.join(args[:3]))
    return result.stdout


def inspect(reference):
    items = json.loads(docker(['docker', 'image', 'inspect', reference], timeout=60))
    if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
        raise DeployError('unexpected image inspection')
    return items[0]


def identity(item, key):
    """Return the publish identity of a local image, or None if it is not ours."""
    labels = (item.get('Config') or {}).get('Labels') or {}
    if (labels.get(release_packages.SOURCE_LABEL) != release_packages.SOURCE_URL
            or labels.get(release_packages.PACKAGE_LABEL) != key):
        return None
    commit, run = labels.get(release_packages.REVISION_LABEL), labels.get(release_packages.RUN_LABEL)
    if not isinstance(commit, str) or not COMMIT.fullmatch(commit) or not isinstance(run, str) or not RUN.fullmatch(run):
        return None
    return {'commit': commit, 'runId': int(run), 'ref': labels.get(release_packages.REF_LABEL),
            'event': labels.get(release_packages.EVENT_LABEL)}


def resolve(key):
    """Pull the verified candidate by digest and prove what it is."""
    repository = release_packages.repository(key)
    docker(['docker', 'pull', '--platform', 'linux/amd64', repository + ':' + release_packages.VERIFIED_TAG])
    tagged = inspect(repository + ':' + release_packages.VERIFIED_TAG)
    digests = [value for value in tagged.get('RepoDigests') or [] if value.startswith(repository + '@')]
    if len(digests) != 1 or not DIGEST.fullmatch(digests[0].split('@', 1)[1]):
        raise DeployError('verified tag does not resolve to exactly one digest')
    reference = digests[0]
    docker(['docker', 'pull', '--platform', 'linux/amd64', reference])
    item = inspect(reference)
    if (item.get('Id') != tagged.get('Id') or not DIGEST.fullmatch(item.get('Id') or '')
            or reference not in (item.get('RepoDigests') or [])):
        raise DeployError('pulled image does not match the verified digest')
    if (item.get('Os') != 'linux' or item.get('Architecture') != 'amd64'
            or (item.get('Config') or {}).get('User') != '10001:10001'):
        raise DeployError('pulled image platform or user is not the published one')
    found = identity(item, key)
    if found is None:
        raise DeployError('pulled image lacks this package source, commit or run labels')
    return {'package': key, 'reference': reference, 'digest': reference.split('@', 1)[1],
            'imageId': item['Id'], **found}


@contextmanager
def agent_lock(directory):
    import fcntl
    fd = os.open(directory / 'agent.lock', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        yield True
    finally:
        os.close(fd)


def state_directory(root):
    directory = root / STATE_DIR
    if directory.is_symlink():
        raise DeployError('real private auto-deploy directory required')
    directory.mkdir(mode=0o700, exist_ok=True)
    if not directory.is_dir():
        raise DeployError('real private auto-deploy directory required')
    return directory


def load(path):
    return json.loads(path.read_text()) if path.exists() else {}


def utc(moment):
    return moment.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def auto_deploy(root, settings, api, *, schedule, run, wait_seconds, drain_seconds, now=None):
    """One idempotent agent pass. `schedule` and `run` are the existing runner's."""
    directory = state_directory(root)
    if (root / HOLD_FILE).exists():
        return record(directory, {'status': 'paused'})
    binding = settings.get('autoDeploy')
    if not isinstance(binding, dict):
        return record(directory, {'status': 'not_enabled'})
    with agent_lock(directory) as acquired:
        if not acquired:
            return {'status': 'busy'}
        try:
            result = step(root, directory, settings, binding, api, schedule=schedule, run=run,
                          wait_seconds=wait_seconds, drain_seconds=drain_seconds,
                          now=now or datetime.now(timezone.utc))
        except DeployError as error:
            result = {'status': 'refused', 'reason': str(error)}
        except Exception as error:
            # Runner and coordinator stops keep their own receipts; never replay.
            record(directory, {'status': 'stopped', 'error': type(error).__name__})
            raise
        return record(directory, result)


def record(directory, result):
    save(directory / 'last-run.json', {**result, 'at': utc(datetime.now(timezone.utc))})
    return result


def step(root, directory, settings, binding, api, *, schedule, run, wait_seconds, drain_seconds, now):
    key = binding.get('package')
    try:
        release_packages.package(key)
    except ValueError:
        raise DeployError('autoDeploy.package must name a published package') from None
    delay = binding.get('drainDelaySeconds', 0)
    if type(delay) is not int or not 0 <= delay <= 86400:
        raise DeployError('autoDeploy.drainDelaySeconds must be 0..86400')
    state_path = directory / 'state.json'
    state = load(state_path)
    node = api.json('GET', '/v1/prototype/nodes/' + settings['nodeId'])['node']
    pending = state.get('inProgress')

    if pending is None:
        if node['state'] not in ('active', 'scheduled', 'draining'):
            return {'status': 'not_eligible', 'nodeState': node['state']}
        candidate = resolve(key)
        if candidate['ref'] != 'refs/heads/main' or candidate['event'] != 'push':
            return {'status': 'ignored', 'reason': 'not_a_main_push', 'candidate': candidate}
        pin = pinned(root, settings)
        if candidate['imageId'] == pin:
            return {'status': 'up_to_date', 'image': pin, 'runId': candidate['runId']}
        current = identity(inspect(pin), key)
        if current is None:
            # First adoption of a package on a service is a reviewed manual
            # schedule; the agent only upgrades a service already running it.
            raise DeployError('managed service is not running this package; adopt it manually first')
        floor = max(current['runId'], state.get('highestRunId', 0))
        if candidate['runId'] <= floor:
            return {'status': 'not_newer', 'runId': candidate['runId'], 'floor': floor}
        if node['state'] == 'active':
            drain = utc(now + timedelta(seconds=delay))
            schedule(candidate['imageId'], drain, drain)
            node = api.json('GET', '/v1/prototype/nodes/' + settings['nodeId'])['node']
        elif node.get('targetImage') != candidate['imageId']:
            # Somebody else's plan: never adopt or replace it.
            return {'status': 'not_eligible', 'nodeState': node['state'], 'reason': 'other_plan'}
        # Recorded after scheduling; an interrupted pass adopts its own plan above.
        state.update(inProgress=candidate, highestRunId=candidate['runId'])
        save(state_path, state)
        pending = candidate

    if node['state'] not in ('scheduled', 'draining') or node.get('targetImage') != pending['imageId']:
        if node['state'] != 'active':
            return {'status': 'not_eligible', 'nodeState': node['state'], 'runId': pending['runId']}
        if pinned(root, settings) == pending['imageId']:
            state.update(applied=pending, inProgress=None)
            save(state_path, state)
            return {'status': 'succeeded', 'image': pending['imageId'], 'runId': pending['runId']}
        # An operator cancelled the window: keep the run floor, never retry it.
        state.update(cancelled=pending, inProgress=None)
        save(state_path, state)
        return {'status': 'cancelled', 'runId': pending['runId']}

    receipts = directory / 'receipts'
    receipts.mkdir(mode=0o700, exist_ok=True)
    receipt = receipts / (str(pending['runId']) + '-' + pending['imageId'][7:19])
    receipt.mkdir(mode=0o700, exist_ok=True)
    result = run(receipt, wait_seconds, drain_seconds)
    if result.get('status') == 'succeeded':
        state.update(applied=pending, inProgress=None)
    elif result.get('status') == 'failed':
        # The runner already rolled back and left the node failed for review.
        state.update(failed=pending, inProgress=None)
    save(state_path, state)
    return {**result, 'runId': pending['runId'], 'commit': pending['commit'], 'receipt': receipt.name}


def pinned(root, settings):
    return json.loads((root / 'compose.json').read_text())['services'][settings['service']]['image']
