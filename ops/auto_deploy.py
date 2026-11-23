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
4. creates a plan with the coordinator's idempotent owned-schedule request
   (compare-and-swap on the node revision), records that plan ID and target,
   and hands exactly that plan to the existing drain-window runner, which
   re-checks it on every poll, then drains, replaces, saves the pin or rolls back.

The hosts still run only sha256 image IDs; the tag is never started. A plan
the agent did not create (even for the same image), a replaced plan, a
failed/held node and an existing operation receipt all stop it for a person to
inspect. It writes `auto-deploy/state.json` and
`auto-deploy/last-run.json` under the operator root on every run.

`maintenance.json` must declare the service `kind` whose package this is
(`coordinator` or `treedar`; see processing/service_adapters.py). The agent
upgrades only a service already running that package, or one whose exact
current pin a person adopted with `node_maintenance.py adopt` (`adopt` below),
which records `auto-deploy/adoption.json`. It never adopts a service itself.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import uuid

from processing import release_packages, service_adapters
from processing.container_update import save, UpdateError
from processing.odx_smoke_worker import HTTPRefused, SmokeError

HOLD_FILE = 'auto-deploy.hold'
STATE_DIR = 'auto-deploy'
# Written on a definite coordinator refusal or an unprovable plan. While it
# exists no schedule request is sent; a person inspects and deletes it.
REFUSAL_FILE = 'refused.json'
# The owned schedule request in flight. Written before the request is sent and
# removed only when its receipt is bound or it was definitely refused. Absence
# is never proof (a timed-out request can still commit later), so an unresolved
# intent stays until its receipt appears or a person deletes this file.
INTENT_FILE = 'schedule-intent.json'
# Written once by the operator-run `adopt` command: the reviewed, unlabelled
# pin this service may be upgraded from. Never written by the agent.
ADOPTION_FILE = 'adoption.json'
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


def definite(error):
    """A 3xx/4xx answer: the coordinator handled and refused the request.

    5xx, transport failures and unreadable 2xx bodies are uncertain: the plan
    may exist, so only the read-only receipt lookup may follow.
    """
    return isinstance(error, HTTPRefused) and 300 <= error.status < 500


def latch(directory, state_path, state, stage, detail):
    # `save` is an atomic, fsynced replace: the latch is durable once it returns.
    save(directory / REFUSAL_FILE, {'stage': stage, **detail, 'at': utc(datetime.now(timezone.utc))})
    save(state_path, state)
    return {'status': 'refused', 'reason': stage + '_refused', **detail}


def auto_deploy(root, settings, api, *, schedule, lookup, run, wait_seconds, drain_seconds, now=None):
    """One idempotent agent pass.

    `schedule(request)` sends the owned schedule request and `lookup(request)`
    reads its receipt (None only when absence is proven for the request's own
    node, request ID and authority); `run` is
    the existing drain-window runner bound to one plan.
    """
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
            result = step(root, directory, settings, binding, api, schedule=schedule, lookup=lookup, run=run,
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


def step(root, directory, settings, binding, api, *, schedule, lookup, run, wait_seconds, drain_seconds, now):
    key = binding.get('package')
    try:
        release_packages.package(key)
    except ValueError:
        raise DeployError('autoDeploy.package must name a published package') from None
    try:
        kind = service_adapters.service_kind(settings)
    except UpdateError:
        raise DeployError('unknown service kind in maintenance.json; refusing') from None
    if 'kind' not in settings or service_adapters.package_for(kind) != key:
        # The runner's idle/health/rollback checks are per kind; a package is
        # only ever deployed to a service declared as that package's kind.
        raise DeployError('autoDeploy.package must match the declared service kind (coordinator or treedar)')
    delay = binding.get('drainDelaySeconds', 0)
    if type(delay) is not int or not 0 <= delay <= 86400:
        raise DeployError('autoDeploy.drainDelaySeconds must be 0..86400')
    state_path = directory / 'state.json'
    state = load(state_path)
    route = '/v1/prototype/nodes/' + settings['nodeId']
    if (directory / REFUSAL_FILE).exists():
        return {'status': 'held', 'reason': 'refusal_latched'}

    intent_path = directory / INTENT_FILE
    intent = load(intent_path) or None
    if intent is not None:
        # An earlier pass sent this owned request without a definite answer.
        # Reconcile it only through the read-only receipt lookup; never POST.
        request_id = intent['request']['requestId']
        try:
            receipt = lookup(intent['request'])
        except (SmokeError, OSError, ValueError, KeyError) as error:
            if definite(error):
                return latch(directory, state_path, state, 'lookup',
                             {'httpStatus': error.status, 'requestId': request_id})
            return {'status': 'held', 'reason': 'ownership_unproven', 'requestId': request_id}
        if receipt is None:
            # Absent right now at the original authority, but the timed-out
            # request may still commit. Keep the intent and only read again.
            unresolved = {'status': 'held', 'reason': 'schedule_unresolved', 'requestId': request_id,
                          'action': ('unresolved: before deleting ' + STATE_DIR + '/' + INTENT_FILE
                                     + ' to retry, confirm the original request has settled (the coordinator'
                                     ' request timeout has passed or its logs show the request finished) and'
                                     ' that no receipt or plan exists for it; a momentary absence is not enough')}
            if intent.get('unresolved') is None:
                intent['unresolved'] = {'since': utc(datetime.now(timezone.utc)), 'action': unresolved['action']}
                save(intent_path, intent)
            return unresolved
        try:
            bind(state, settings, intent['candidate'], intent['request'], receipt)
        except DeployError as error:
            return latch(directory, state_path, state, 'receipt', {'detail': str(error), 'requestId': request_id})
        save(state_path, state)
        intent_path.unlink()

    pending = state.get('inProgress')
    node = api.json('GET', route)['node']
    if pending is None:
        if node['state'] != 'active':
            # Only a plan this agent created and recorded is ever run, even if
            # another plan targets the same image.
            return {'status': 'not_eligible', 'nodeState': node['state'],
                    **({'reason': 'other_plan'} if node['state'] in ('scheduled', 'draining') else {})}
        candidate = resolve(key)
        if candidate['ref'] != 'refs/heads/main' or candidate['event'] != 'push':
            return {'status': 'ignored', 'reason': 'not_a_main_push', 'candidate': candidate}
        pin = pinned(root, settings)
        if candidate['imageId'] == pin:
            return {'status': 'up_to_date', 'image': pin, 'runId': candidate['runId']}
        current = identity(inspect(pin), key)
        if current is None:
            # First adoption of a package on a service is a reviewed manual
            # step; the agent only upgrades a service already running it or
            # the exact pin a person adopted.
            if not adopted(directory, settings, key, pin):
                raise DeployError('managed service is not running this package; adopt it manually first')
            current = {'runId': 0}
        floor = max(current['runId'], state.get('highestRunId', 0))
        if candidate['runId'] <= floor:
            return {'status': 'not_newer', 'runId': candidate['runId'], 'floor': floor}
        drain = utc(now + timedelta(seconds=delay))
        # Compare-and-swap on the node revision just read; the request is saved
        # before it is sent so a lost response is reconciled by reading its receipt.
        request = {'schemaVersion': 1, 'requestId': str(uuid.uuid4()),
                   'authorityId': api.json('GET', '/v1/prototype/authority')['authorityId'],
                   'expectedRevision': node['revision'], 'targetImage': candidate['imageId'],
                   'drainAt': drain, 'notBefore': drain, 'afterJobId': None}
        save(intent_path, {'candidate': candidate, 'request': request})
        try:
            receipt = schedule(request)
        except (SmokeError, OSError, ValueError, KeyError) as error:
            if definite(error):
                # Refused before any plan was stored: nothing was created. The
                # latch is saved first; the intent goes only once it is durable,
                # so an interruption leaves at least one of them and no re-POST.
                try:
                    result = latch(directory, state_path, state, 'schedule',
                                   {'httpStatus': error.status, 'requestId': request['requestId']})
                except OSError:
                    return {'status': 'held', 'reason': 'refusal_latch_unsaved', 'requestId': request['requestId']}
                intent_path.unlink()
                return result
            return {'status': 'held', 'reason': 'schedule_response_uncertain', 'requestId': request['requestId']}
        try:
            bind(state, settings, candidate, request, receipt)
        except DeployError as error:
            return latch(directory, state_path, state, 'receipt',
                         {'detail': str(error), 'requestId': request['requestId']})
        save(state_path, state)
        intent_path.unlink()
        pending = state['inProgress']
        node = api.json('GET', route)['node']

    ours = node.get('planId') == pending['planId'] and node.get('targetImage') == pending['imageId']
    if node['state'] in ('scheduled', 'draining') and not ours:
        # Cancelled and replaced by somebody else: leave theirs alone.
        state.update(cancelled=pending, inProgress=None)
        save(state_path, state)
        return {'status': 'not_eligible', 'nodeState': node['state'], 'reason': 'plan_replaced',
                'runId': pending['runId']}
    if node['state'] == 'active':
        if pinned(root, settings) == pending['imageId']:
            state.update(applied=pending, inProgress=None)
            save(state_path, state)
            return {'status': 'succeeded', 'image': pending['imageId'], 'runId': pending['runId']}
        # An operator cancelled the window: keep the run floor, never retry it.
        state.update(cancelled=pending, inProgress=None)
        save(state_path, state)
        return {'status': 'cancelled', 'runId': pending['runId']}
    if node['state'] not in ('scheduled', 'draining'):
        return {'status': 'not_eligible', 'nodeState': node['state'], 'runId': pending['runId']}

    receipts = directory / 'receipts'
    receipts.mkdir(mode=0o700, exist_ok=True)
    receipt = receipts / (str(pending['runId']) + '-' + pending['planId'])
    receipt.mkdir(mode=0o700, exist_ok=True)
    # The runner re-checks this exact plan and target on every poll and again
    # before ingress and the update.
    result = run(receipt, wait_seconds, drain_seconds, pending['planId'], pending['imageId'])
    if result.get('status') == 'succeeded':
        state.update(applied=pending, inProgress=None)
    elif result.get('status') == 'failed':
        # The runner already rolled back and left the node failed for review.
        state.update(failed=pending, inProgress=None)
    save(state_path, state)
    return {**result, 'runId': pending['runId'], 'commit': pending['commit'], 'receipt': receipt.name}


PLAN = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')


def bind(state, settings, candidate, request, receipt):
    """Record the plan the coordinator created for exactly this request."""
    if (not isinstance(receipt, dict) or receipt.get('requestId') != request['requestId']
            or receipt.get('nodeId') != settings['nodeId'] or receipt.get('targetImage') != candidate['imageId']
            or receipt.get('expectedRevision') != request['expectedRevision']
            or receipt.get('authorityId') != request['authorityId']
            or not isinstance(receipt.get('planId'), str) or not PLAN.fullmatch(receipt['planId'])):
        raise DeployError('schedule receipt does not bind this request; inspect the node plan')
    state.update(inProgress={**candidate, 'planId': receipt['planId'], 'requestId': request['requestId']},
                 highestRunId=candidate['runId'])


def pinned(root, settings):
    return json.loads((root / 'compose.json').read_text())['services'][settings['service']]['image']


def adopted(directory, settings, key, pin):
    """True only for the exact pin, service and package a person adopted."""
    record = load(directory / ADOPTION_FILE)
    return (record.get('schemaVersion') == 1 and record.get('package') == key
            and record.get('kind') == settings.get('kind') and record.get('image') == pin
            and record.get('nodeId') == settings.get('nodeId') and record.get('project') == settings.get('project')
            and record.get('service') == settings.get('service'))


def adopt(root, settings, api, *, kind, image, drain_delay=0, make_adapter=None, now=None):
    """Operator-run, one-time opt-in of a service whose pin lacks package labels.

    The operator names the current Compose pin they reviewed. This checks that
    it is the pin, that the service is running it and healthy by its kind's
    adapter, that the node is active with no plan and the agent has no open
    intent, refusal or update, then records `auto-deploy/adoption.json` and
    finally writes `kind` and `autoDeploy` into `maintenance.json`. It never
    pulls, schedules or replaces anything.
    """
    key = service_adapters.package_for(kind)
    if key is None:
        raise DeployError('only a coordinator or treedar service can be adopted')
    if settings.get('autoDeploy') is not None:
        raise DeployError('service already opted into auto-deploy; nothing to adopt')
    if settings.get('kind', kind) != kind:
        raise DeployError('maintenance.json declares another service kind')
    if type(drain_delay) is not int or not 0 <= drain_delay <= 86400:
        raise DeployError('drain delay must be 0..86400 seconds')
    directory = state_directory(root)
    with agent_lock(directory) as acquired:
        if not acquired:
            raise DeployError('an auto-deploy pass is running; retry when it ends')
        for name in (REFUSAL_FILE, INTENT_FILE):
            if (directory / name).exists():
                raise DeployError('inspect and resolve ' + STATE_DIR + '/' + name + ' before adoption')
        if load(directory / 'state.json').get('inProgress') is not None:
            raise DeployError('an auto-deploy update is in progress; inspect it before adoption')
        pin = pinned(root, settings)
        if image != pin:
            raise DeployError('--image must equal the current Compose pin exactly')
        item = inspect(pin)
        if item.get('Id') != pin:
            raise DeployError('current pin is not a cached immutable image')
        labels = (item.get('Config') or {}).get('Labels') or {}
        if labels.get(release_packages.PACKAGE_LABEL) not in (None, key):
            raise DeployError('current pin is labelled as another Processing package')
        labelled = identity(item, key)
        route = '/v1/prototype/nodes/' + settings['nodeId']
        if api.json('GET', route)['node']['state'] != 'active':
            raise DeployError('node must be active with no maintenance plan')
        authority = api.json('GET', '/v1/prototype/authority')['authorityId']
        try:
            adapter = (make_adapter or service_adapters.adapter)(
                kind, root, settings, directory, api=api, authority=authority, baseline=pin)
            running = adapter.inspect()['Image']
        except UpdateError as error:
            raise DeployError('service configuration refused: ' + str(error)) from None
        if running != pin:
            raise DeployError('running service is not the Compose pin')
        if not service_adapters.safe(adapter.adoptable, pin):
            raise DeployError('current service is not healthy by its kind check; fix it before adoption')
        record = {'schemaVersion': 1, 'kind': kind, 'package': key, 'nodeId': settings['nodeId'],
                  'project': settings['project'], 'service': settings['service'], 'image': pin,
                  'labelledRunId': labelled['runId'] if labelled else None,
                  'adoptedAt': utc(now or datetime.now(timezone.utc))}
        path = directory / ADOPTION_FILE
        existing = load(path)
        if existing and any(existing.get(name) != record[name]
                            for name in ('kind', 'package', 'nodeId', 'project', 'service', 'image')):
            raise DeployError('a different adoption is already recorded; inspect '
                              + STATE_DIR + '/' + ADOPTION_FILE)
        if not existing:
            save(path, record)
        else:
            record = existing
        # Opt in last: an interruption before this leaves the service manual.
        save(root / 'maintenance.json',
             {**settings, 'kind': kind, 'autoDeploy': {'package': key, 'drainDelaySeconds': drain_delay}})
        return {'status': 'adopted', 'kind': kind, 'package': key, 'image': pin,
                'labelledRunId': record['labelledRunId']}
