"""Schedule/run one container update on an installed processing node.

`schedule`/`run`: an operator stages an immutable image, schedules a plan, then
supervises `run` until its bounded wait ends. `auto-deploy`: the host timer's
step (ops/auto_deploy.py) pulls the newest verified main image by digest and
uses the same schedule/run path. Neither runs a moving tag, updates the OS,
terminates processes or replays an uncertain operation.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from processing.container_update import ComposeNode, execute, lock, UpdateError
from processing.node_gate import NodeGate, GateBusy, GateError as NodeGateError
from processing.odx_smoke_worker import Client, SmokeError
from ops.node_ingress import Ingress, GateError, locked, save
from ops.auto_deploy import auto_deploy, DeployError


def private_directory(value):
    root = Path(value).absolute()
    if root.resolve() != root or not root.is_dir() or root.stat().st_mode & 0o077:
        raise GateError('real private directory required')
    return root


@contextmanager
def idle_host_gate(api, node_id, engine_id, deadline):
    while True:
        jobs = api.json('GET', '/v1/prototype/jobs')['jobs']
        busy = any(j.get('nodeId') == node_id and j['status'] in ('running', 'held') for j in jobs)
        gate = None
        if not busy:
            candidate = NodeGate(engine_id)
            try:
                gate = candidate.__enter__()
            except GateBusy:
                pass
        if gate is not None:
            try:
                yield gate
            finally:
                gate.__exit__(None, None, None)
            return
        if time.monotonic() >= deadline:
            raise GateError('worker/held claim has not drained; no replacement authorized')
        time.sleep(min(2, max(0, deadline - time.monotonic())))


def run(root, api, operator, receipt, wait_seconds, drain_seconds=60, *, expected_plan_id=None,
        expected_target=None):
    """Drain, replace and reopen for the eligible plan.

    With `expected_plan_id`/`expected_target` (auto-deploy), every poll and
    every later revalidation must still see exactly that plan; any other plan
    stops before ingress or the update is touched.
    """
    if (expected_plan_id is None) != (expected_target is None):
        raise GateError('expected plan and target are bound together')
    settings = json.loads((root / 'maintenance.json').read_text())
    ingress = Ingress(root / 'ingress')
    node_id = settings['nodeId']
    args = SimpleNamespace(project=settings['project'], service=settings['service'],
        compose_file=str(root / 'compose.json'), node_origin='http://127.0.0.1:' + str(ingress.config['backendPort']),
        docker_engine_id=ingress.config['engineId'], node_port=3000)
    adapter = ComposeNode(args, receipt)
    deadline = time.monotonic() + wait_seconds
    with lock(receipt), locked(root / 'ingress'):
        if (receipt / 'managed-update.json').exists() or (receipt / 'update.json').exists():
            raise GateError('existing operation receipt; inspect rather than replay')
        while True:
            node = api.json('GET', '/v1/prototype/nodes/' + node_id)['node']
            if node['state'] not in ('scheduled', 'draining'):
                return {'status': 'not_eligible', 'nodeState': node['state']}
            if expected_plan_id is not None and (node.get('planId') != expected_plan_id
                                                 or node.get('targetImage') != expected_target):
                raise GateError('maintenance plan is not the bound plan; no ingress or update change')
            due = datetime.fromisoformat(node['notBefore'].replace('Z', '+00:00')) <= datetime.now(timezone.utc)
            if node['state'] == 'draining' and due:
                break
            if time.monotonic() >= deadline:
                return {'status': 'waiting', 'nodeState': node['state']}
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        item = adapter.inspect()
        backend = json.loads(ingress.docker(['inspect', ingress.config['backendName']]))[0]
        if backend['Id'] != item['Id']:
            raise GateError('ingress and updater select different backends')
        adapter.image(node['targetImage'])
        state = {'stage': 'ingress_close_pending', 'planId': node['planId'], 'targetImage': node['targetImage']}
        save(receipt / 'managed-update.json', state)
        ingress.change(True, seconds=drain_seconds)
        state['stage'] = 'ingress_closed'
        save(receipt / 'managed-update.json', state)
        # The coordinator fence stops claims; the canonical Engine lock excludes
        # another cooperative host worker during replacement and reopening.
        with idle_host_gate(api, node_id, args.docker_engine_id, deadline) as gate:
            binding = gate.snapshot()
            authority = api.json('GET', '/v1/prototype/authority')['authorityId']
            if (binding['reservation'] is not None or binding['nodeId'] != node_id
                    or binding['coordinatorOrigin'] != api.origin or binding['authorityId'] != authority):
                raise GateError('host gate binding/reservation mismatch')
            current = api.json('GET', '/v1/prototype/nodes/' + node_id)['node']
            if current['planId'] != node['planId'] or current['targetImage'] != node['targetImage']:
                raise GateError('maintenance plan changed while closing ingress')
            result = execute(api, operator, adapter, node_id, receipt,
                wait_seconds=max(0, deadline - time.monotonic()), health_seconds=30,
                expected_plan_id=node['planId'], expected_target=node['targetImage'])
            state.update(stage='update_returned', result=result)
            save(receipt / 'managed-update.json', state)
            if result['status'] != 'succeeded':
                return {**result, 'ingress': 'closed'}
            source = json.loads((root / 'compose.json').read_text())
            source['services'][settings['service']]['image'] = result['image']
            save(root / 'compose.json', source)
            state['stage'] = 'ingress_open_pending'
            save(receipt / 'managed-update.json', state)
            ingress.change(False, expected=result['image'], seconds=drain_seconds)
            state.update(stage='completed', persistentComposePin=result['image'])
            save(receipt / 'managed-update.json', state)
            return {**result, 'composePinUpdateRequired': False, 'ingress': 'open'}


def schedule(root, settings, api, operator, target_image, drain_at, not_before, after_job_id=None):
    node = api.json('GET', '/v1/prototype/nodes/' + settings['nodeId'])['node']
    receipt_args = SimpleNamespace(project=settings['project'], service=settings['service'],
        compose_file=str(root / 'compose.json'), node_origin='http://127.0.0.1:13004',
        docker_engine_id=Ingress(root / 'ingress').config['engineId'], node_port=3000)
    ComposeNode(receipt_args, root).image(target_image)
    return operator.json('POST', '/v1/prototype/nodes/' + settings['nodeId'] + '/schedule', {
        'expectedRevision': node['revision'], 'targetImage': target_image,
        'drainAt': drain_at, 'notBefore': not_before, 'afterJobId': after_job_id})


def schedule_owned(root, settings, operator, request):
    """Create a plan through the idempotent owned-schedule API.

    The caller persists `request` (with its requestId and expectedRevision)
    before the first attempt. Re-sending the identical request after a lost
    response returns the original receipt; any other state is a conflict.
    """
    receipt_args = SimpleNamespace(project=settings['project'], service=settings['service'],
        compose_file=str(root / 'compose.json'), node_origin='http://127.0.0.1:13004',
        docker_engine_id=Ingress(root / 'ingress').config['engineId'], node_port=3000)
    ComposeNode(receipt_args, root).image(request['targetImage'])
    return operator.json('POST', '/v1/prototype/nodes/' + settings['nodeId'] + '/schedule-owned', request)['receipt']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--credentials', required=True)
    actions = parser.add_subparsers(dest='action', required=True)
    actions.add_parser('status')
    schedule = actions.add_parser('schedule')
    schedule.add_argument('--target-image', required=True)
    schedule.add_argument('--drain-at', required=True)
    schedule.add_argument('--not-before', required=True)
    schedule.add_argument('--after-job-id')
    runner = actions.add_parser('run')
    runner.add_argument('--receipt', required=True)
    runner.add_argument('--wait-seconds', type=int, choices=range(0, 86401), default=86400)
    runner.add_argument('--drain-seconds', type=int, choices=range(1, 86401), default=3600)
    agent = actions.add_parser('auto-deploy', help='host timer step: verified main image through the drain window')
    agent.add_argument('--wait-seconds', type=int, choices=range(0, 86401), default=3600)
    agent.add_argument('--drain-seconds', type=int, choices=range(1, 86401), default=3600)
    args = parser.parse_args()
    try:
        root = private_directory(args.root)
        path = Path(args.credentials).absolute()
        if path.resolve() != path or not path.is_file() or path.stat().st_mode & 0o077:
            raise GateError('private credential file required')
        env = dict(line.split('=', 1) for line in path.read_text().splitlines() if '=' in line)
        settings = json.loads((root / 'maintenance.json').read_text())
        if settings['coordinator'] != 'http://127.0.0.1:18080':
            raise GateError('installed private coordinator tunnel required')
        api = Client(settings['coordinator'], env['PROCESSING_API_TOKEN'])
        secret = env['PROCESSING_PROTOTYPE_OPERATOR_TOKEN']
        if not secret or secret == env['PROCESSING_API_TOKEN']:
            raise GateError('distinct maintenance credential required')
        operator = Client(api.origin, secret)
        if args.action == 'status':
            node = api.json('GET', '/v1/prototype/nodes/' + settings['nodeId'])['node']
            result = {'node': node, 'ingressState': json.loads((root / 'ingress/state.json').read_text())['state']}
        elif args.action == 'schedule':
            result = schedule(root, settings, api, operator, args.target_image, args.drain_at,
                              args.not_before, args.after_job_id)
        elif args.action == 'auto-deploy':
            result = auto_deploy(root, settings, api,
                schedule=lambda request: schedule_owned(root, settings, operator, request),
                run=lambda receipt, wait, drain, plan_id, target: run(
                    root, api, operator, private_directory(receipt), wait, drain,
                    expected_plan_id=plan_id, expected_target=target),
                wait_seconds=args.wait_seconds, drain_seconds=args.drain_seconds)
        else:
            result = run(root, api, operator, private_directory(args.receipt), args.wait_seconds, args.drain_seconds)
        print(json.dumps(result, sort_keys=True))
        if result.get('status') in ('refused', 'stopped'):
            return 2
        return 1 if result.get('status') == 'failed' else 0
    except (GateError, NodeGateError, UpdateError, SmokeError, DeployError, OSError, ValueError, KeyError):
        print('Managed maintenance stopped. Inspect private receipt, ingress and coordinator gate; no automatic replay or reopen.')
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
