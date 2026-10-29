"""Schedule/run one reviewed update on an explicitly configured managed node.

Foreground automation only: stage an immutable image first, schedule a plan,
then supervise `run` until its bounded wait ends. No moving-tag pulls, OS updates,
process termination or automatic uncertain-operation replay.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import hashlib
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from processing.container_update import (ComposeNode,execute,lock,UpdateError,deferred_receipt,
                                         finalize_deferred,finish_matches)
from processing.node_gate import NodeGate, GateBusy, GateError as NodeGateError
from processing.odx_smoke_worker import Client, SmokeError
from ops.node_ingress import Ingress, GateError, locked, save


def load_settings(root,*,worker_aware=False):
    if type(worker_aware) is not bool:raise GateError('explicit maintenance mode required')
    if worker_aware:
        from ops.reference_worker_maintenance import load_profile
        settings,policy=load_profile(root)
        settings=dict(settings,workerProfileSha256=policy.sha256)
        return settings,policy
    if (root/'maintenance-worker-v2.json').exists():raise GateError('explicit worker maintenance v2 mode required')
    return json.loads((root/'maintenance.json').read_text()),None


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


def adapter_args(root,settings,ingress):
    engine=ingress.config.get('backendEngine','odx')
    if settings.get('expectedEngine',engine)!=engine:
        raise GateError('maintenance and ingress engine bindings differ')
    return SimpleNamespace(project=settings['project'],service=settings['service'],compose_file=str(root/'compose.json'),
        node_origin='http://127.0.0.1:'+str(ingress.config['backendPort']),docker_engine_id=ingress.config['engineId'],
        node_port=settings.get('nodePort',3000),node_token=ingress.config.get('backendToken',''),expected_engine=engine,
        upload_layout=settings.get('uploadLayout'),process_layout=settings.get('processLayout'))


def compose_base(source,service):
    clone=json.loads(json.dumps(source));clone['services'][service].pop('image',None)
    return hashlib.sha256(json.dumps(clone,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def operation_binding(api,settings,ingress,args):
    result=dict(nodeId=settings['nodeId'],project=settings['project'],service=settings['service'],coordinator=api.origin,
        backendName=ingress.config['backendName'],backendPort=ingress.config['backendPort'],engineId=args.docker_engine_id,
        expectedEngine=args.expected_engine,nodePort=args.node_port,
        proxyName=ingress.config['proxyName'],proxyImage=ingress.config['proxyImage'],listenPort=ingress.config['listenPort'])
    if 'workerProfileSha256' in settings:result['workerProfileSha256']=settings['workerProfileSha256']
    if 'ownedScheduleRequestId' in settings:result['ownedScheduleRequestId']=settings['ownedScheduleRequestId']
    return result


def sync_compose(root):
    for path in (root/'compose.json',root):
        fd=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:os.fsync(fd)
        finally:os.close(fd)


def owned_open_proof(ingress,state):
    """Read-only reconciliation; never reload nginx after an unknown open."""
    current=json.loads((ingress.root/'state.json').read_text())
    proxy=ingress.inspect()
    identifier=state['closedIngress']['containerId']
    if (current.get('state') not in ('opening','open') or current.get('containerId')!=identifier
            or current.get('expectedBackendImage')!=state['targetImage'] or proxy['Id']!=identifier
            or set(current.get('oldWorkers',[])) & ingress.workers(proxy)):
        raise GateError('owned ingress open state is not proven')
    backend=ingress.backend(state['targetImage'])
    status,public=ingress.http(ingress.config['listenPort'])
    if status!=200 or not isinstance(public,dict) or any(public.get(key)!=backend.get(key)
            for key in ('engine','engineVersion','version')):
        raise GateError('owned ingress health is not proven')
    return dict(state='open',containerId=identifier,oldWorkersDrained=True)


def finalize_success(root,api,operator,receipt,settings,ingress,adapter,state,drain_seconds,*,already_finished=False,worker=None,gate=None):
    private=deferred_receipt(receipt,settings['nodeId'],expected_plan_id=state['planId'],expected_target=state['targetImage'])
    source=json.loads((root/'compose.json').read_text())
    service=settings['service'];pin=source['services'][service]['image']
    if compose_base(source,service)!=state['composeBaseSha256'] or pin not in (state['oldImage'],state['targetImage']):
        raise GateError('authoritative Compose configuration changed')
    if pin!=state['targetImage']:
        if already_finished or state['stage'] not in ('update_returned','compose_pin_pending'):
            raise GateError('authoritative Compose pin no longer matches')
        state['stage']='compose_pin_pending';save(receipt/'managed-update.json',state)
        source['services'][service]['image']=state['targetImage'];save(root/'compose.json',source)
    # Re-fsync a readback after a lost pin checkpoint, without replacing config.
    sync_compose(root)
    if not (adapter.serving(state['targetImage']) if already_finished else adapter.healthy(state['targetImage'])):
        raise GateError('pinned replacement health changed')
    if worker is not None:
        if already_finished:worker.verify(state,adapter,idle=False);worker.verify_runtime(state)
        else:
            if state['worker']['phase']=='finish_pending':raise GateError('unknown worker-aware finish requires exact read-only reconciliation')
            worker.after_backend(state,receipt,gate,adapter)
            worker.publish_runtime(state,receipt,gate,adapter)
    if state['stage'] in ('update_returned','compose_pin_pending','compose_pinned'):
        state['stage']='compose_pinned';save(receipt/'managed-update.json',state)
        closed=json.loads((ingress.root/'state.json').read_text())
        if closed.get('state')!='closed' or closed.get('containerId')!=state['closedIngress']['containerId']:
            raise GateError('owned closed ingress no longer matches')
        if worker is not None:worker.fence(state,gate)
        state['stage']='ingress_open_pending';save(receipt/'managed-update.json',state)
        ingress.change(False,expected=state['targetImage'],seconds=drain_seconds)
    proof=owned_open_proof(ingress,state)
    final_source=json.loads((root/'compose.json').read_text())
    if (final_source['services'][service]['image']!=state['targetImage']
            or compose_base(final_source,service)!=state['composeBaseSha256']
            or not (adapter.serving(state['targetImage']) if already_finished else adapter.healthy(state['targetImage']))):
        raise GateError('final pinned backend verification changed')
    sync_compose(root)
    state.update(stage='finish_pending',openedIngress=proof)
    save(receipt/'managed-update.json',state)
    result=dict(healthy=True,image=state['targetImage'],previousImage=private['oldImage'],
        composePinUpdateRequired=False,persistentComposePin=state['targetImage'],ingress='open')
    if worker is not None and not already_finished:
        worker.fence(state,gate);worker.finish_intent(state,receipt)
    result=finalize_deferred(api,operator,settings['nodeId'],receipt,expected_plan_id=state['planId'],
        expected_target=state['targetImage'],result=result)
    state.update(stage='completed',persistentComposePin=state['targetImage'],result=result)
    save(receipt/'managed-update.json',state)
    return result


def run(root, api, operator, receipt, wait_seconds, drain_seconds=60,*,expected_plan_id=None,expected_target=None,worker_aware=False,owned_schedule_request_id=None):
    settings,worker_policy=load_settings(root,worker_aware=worker_aware)
    if worker_policy is not None:
        from ops.reference_worker_maintenance import WorkerMaintenance
        from processing.reference_protocol import _uuid
        _uuid(owned_schedule_request_id,'owned schedule request')
        settings['ownedScheduleRequestId']=owned_schedule_request_id
        worker=WorkerMaintenance(root,worker_policy,api,operator=operator,owned_schedule_request_id=owned_schedule_request_id)
        try:return _run(root,api,operator,receipt,wait_seconds,drain_seconds,expected_plan_id=expected_plan_id,expected_target=expected_target,settings=settings,worker=worker)
        finally:worker.close()
    return _run(root,api,operator,receipt,wait_seconds,drain_seconds,expected_plan_id=expected_plan_id,expected_target=expected_target,settings=settings)


def _run(root, api, operator, receipt, wait_seconds, drain_seconds=60,*,expected_plan_id=None,expected_target=None,settings,worker=None):
    node_id = settings['nodeId']
    if (expected_plan_id is None)!=(expected_target is None):
        raise GateError('frozen plan and target must be supplied together')
    def fence(value):
        if expected_plan_id is not None and (value.get('planId')!=expected_plan_id or value.get('targetImage')!=expected_target):
            raise GateError('expected maintenance plan or target changed')
    if expected_plan_id is not None:
        fence(api.json('GET','/v1/prototype/nodes/'+node_id)['node'])
    ingress = Ingress(root / 'ingress')
    args=adapter_args(root,settings,ingress)
    if worker is None:adapter = ComposeNode(args, receipt)
    else:
        from ops.reference_worker_maintenance import WorkerComposeNode
        adapter=WorkerComposeNode(args,receipt,worker)
    deadline = time.monotonic() + wait_seconds
    with lock(receipt), locked(root / 'ingress'):
        managed=receipt/'managed-update.json'
        binding=operation_binding(api,settings,ingress,args)
        if managed.exists():
            state=json.loads(managed.read_text())
            if (state.get('binding')!=binding or state.get('stage') not in ('ingress_closed','update_returned',
                    'compose_pin_pending','compose_pinned','ingress_open_pending','finish_pending','completed')):
                raise GateError('frozen managed operation binding changed')
            fence(state)
            private=deferred_receipt(receipt,node_id,expected_plan_id=state['planId'],expected_target=state['targetImage'])
            final=dict(healthy=True,image=state['targetImage'],previousImage=private['oldImage'],
                composePinUpdateRequired=False,persistentComposePin=state['targetImage'],ingress='open')
            node=api.json('GET','/v1/prototype/nodes/'+node_id)['node']
            fence(node)
            if finish_matches(node,private,final):
                return finalize_success(root,api,operator,receipt,settings,ingress,adapter,state,drain_seconds,already_finished=True,worker=worker)
            if (node.get('state')!='updating' or node.get('revision')!=private['reservedRevision']
                    or node.get('planId')!=state['planId'] or node.get('targetImage')!=state['targetImage']):
                raise GateError('frozen operation is no longer updating')
            if state['stage']=='ingress_closed':
                state.update(stage='update_returned',result={'status':'succeeded'})
                save(managed,state)
            with idle_host_gate(api,node_id,args.docker_engine_id,deadline) as gate:
                current=gate.snapshot()
                authority=api.json('GET','/v1/prototype/authority')['authorityId']
                if (current['reservation'] is not None or current['nodeId']!=node_id
                        or current['coordinatorOrigin']!=api.origin or current['authorityId']!=authority):
                    raise GateError('host gate binding/reservation mismatch')
                current_node=api.json('GET','/v1/prototype/nodes/'+node_id)['node']
                fence(current_node)
                if (current_node.get('state')!='updating' or current_node.get('revision')!=private['reservedRevision']
                        or current_node.get('planId')!=state['planId'] or current_node.get('targetImage')!=state['targetImage']):
                    raise GateError('frozen operation changed while acquiring host gate')
                return finalize_success(root,api,operator,receipt,settings,ingress,adapter,state,drain_seconds,worker=worker,gate=gate)
        if (receipt/'update.json').exists():
            raise GateError('unbound update receipt requires inspection')
        while True:
            node = api.json('GET', '/v1/prototype/nodes/' + node_id)['node']
            fence(node)
            if node['state'] not in ('scheduled', 'draining'):
                return {'status': 'not_eligible', 'nodeState': node['state']}
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
        source=json.loads((root/'compose.json').read_text())
        if source['services'][settings['service']]['image']!=item['Image']:
            raise GateError('authoritative Compose pin differs from current backend')
        state = dict(stage='waiting_for_jobs',planId=node['planId'],targetImage=node['targetImage'],binding=binding,
            oldImage=item['Image'],composeBaseSha256=compose_base(source,settings['service']))
        save(receipt / 'managed-update.json', state)
        # The coordinator is already draining and prevents new node claims.
        # Keep ingress open until existing uploads/status/result transfers drain;
        # closing it first would strand their queue completion indefinitely.
        with idle_host_gate(api, node_id, args.docker_engine_id, deadline) as gate:
            binding = gate.snapshot()
            authority = api.json('GET', '/v1/prototype/authority')['authorityId']
            if (binding['reservation'] is not None or binding['nodeId'] != node_id
                    or binding['coordinatorOrigin'] != api.origin or binding['authorityId'] != authority):
                raise GateError('host gate binding/reservation mismatch')
            current = api.json('GET', '/v1/prototype/nodes/' + node_id)['node']
            fence(current)
            if current['planId'] != node['planId'] or current['targetImage'] != node['targetImage']:
                raise GateError('maintenance plan changed while closing ingress')
            def close_reserved(reserved):
                current=api.json('GET','/v1/prototype/nodes/'+node_id)['node']
                fence(current)
                if (current.get('state')!='updating' or current.get('revision')!=reserved['reservedRevision']
                        or current.get('planId')!=state['planId'] or current.get('targetImage')!=state['targetImage']
                        or reserved['nodeId']!=node_id or reserved['planId']!=state['planId']
                        or reserved['targetImage']!=state['targetImage']):
                    raise GateError('owned reservation changed before ingress close')
                if worker is not None:worker.before_backend(state,receipt,gate,reserved,adapter)
                state['stage']='ingress_close_pending';save(receipt/'managed-update.json',state)
                closed=ingress.change(True,seconds=drain_seconds)
                if (type(closed) is not dict or closed.get('state')!='closed' or closed.get('oldWorkersDrained') is not True
                        or type(closed.get('containerId')) is not str):
                    raise GateError('owned ingress close was not proven')
                state.update(stage='ingress_closed',closedIngress=closed)
                save(receipt/'managed-update.json',state)
            result = execute(api, operator, adapter, node_id, receipt,
                wait_seconds=max(0, deadline - time.monotonic()), health_seconds=30,
                expected_plan_id=node['planId'], expected_target=node['targetImage'],defer_finish=True,on_reserved=close_reserved)
            state.update(stage='update_returned', result=result)
            save(receipt / 'managed-update.json', state)
            if result['status'] != 'succeeded':
                return {**result,'ingress':'closed' if state.get('closedIngress') else 'unchanged'}
            return finalize_success(root,api,operator,receipt,settings,ingress,adapter,state,drain_seconds,worker=worker,gate=gate)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--credentials', required=True)
    parser.add_argument('--worker-aware',action='store_true',help='explicit fenced maintenance-worker-v2 profile')
    actions = parser.add_subparsers(dest='action', required=True)
    actions.add_parser('status')
    schedule = actions.add_parser('schedule')
    schedule.add_argument('--target-image', required=True)
    schedule.add_argument('--drain-at', required=True)
    schedule.add_argument('--not-before', required=True)
    schedule.add_argument('--after-job-id')
    runner = actions.add_parser('run')
    runner.add_argument('--receipt', required=True)
    runner.add_argument('--owned-schedule-request-id')
    runner.add_argument('--wait-seconds', type=int, choices=range(0, 86401), default=86400)
    runner.add_argument('--drain-seconds', type=int, choices=range(1, 86401), default=3600)
    args = parser.parse_args()
    try:
        root = private_directory(args.root)
        path = Path(args.credentials).absolute()
        if path.resolve() != path or not path.is_file() or path.stat().st_mode & 0o077:
            raise GateError('private credential file required')
        env = dict(line.split('=', 1) for line in path.read_text().splitlines() if '=' in line)
        settings,_policy=load_settings(root,worker_aware=args.worker_aware)
        if settings['coordinator'] != 'http://127.0.0.1:18080':
            raise GateError('installed private coordinator tunnel required')
        api = Client(settings['coordinator'], env['PROCESSING_API_TOKEN'])
        secret = env['PROCESSING_PROTOTYPE_OPERATOR_TOKEN']
        if not secret or secret == env['PROCESSING_API_TOKEN']:
            raise GateError('distinct maintenance credential required')
        operator = Client(api.origin, secret)
        node = api.json('GET', '/v1/prototype/nodes/' + settings['nodeId'])['node']
        if args.action == 'status':
            result = {'node': node, 'ingressState': json.loads((root / 'ingress/state.json').read_text())['state']}
        elif args.action == 'schedule':
            if args.worker_aware:raise GateError('worker-aware scheduling requires the owned update supervisor')
            receipt_args=adapter_args(root,settings,Ingress(root/'ingress'))
            ComposeNode(receipt_args, root).image(args.target_image)
            result = operator.json('POST', '/v1/prototype/nodes/' + settings['nodeId'] + '/schedule', {
                'expectedRevision': node['revision'], 'targetImage': args.target_image,
                'drainAt': args.drain_at, 'notBefore': args.not_before, 'afterJobId': args.after_job_id})
        else:
            result = run(root, api, operator, private_directory(args.receipt), args.wait_seconds, args.drain_seconds,
                **({'worker_aware':True,'owned_schedule_request_id':args.owned_schedule_request_id} if args.worker_aware else {}))
        print(json.dumps(result, sort_keys=True))
        return 1 if result.get('status') == 'failed' else 0
    except (GateError, NodeGateError, UpdateError, SmokeError, OSError, ValueError, KeyError):
        print('Managed maintenance stopped. Inspect private receipt, ingress and coordinator gate; no automatic replay or reopen.')
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
