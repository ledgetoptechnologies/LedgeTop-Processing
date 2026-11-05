"""Schedule/run one reviewed update on an explicitly configured managed node.

Foreground automation only: stage an immutable image first, schedule a plan,
then supervise `run` until its bounded wait ends. No moving-tag pulls, OS updates,
forced process termination or automatic uncertain-operation replay.
"""
import argparse
import base64
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
                                         finalize_deferred,finish_matches,exact_engine_version,reviewed_worker_mode,wait_healthy)
from processing.node_gate import NodeGate, GateBusy, GateError as NodeGateError
from processing.odx_smoke_worker import Client, SmokeError
from ops.node_ingress import Ingress, GateError, locked, save


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
        worker_mode=settings.get('workerMode'),
        upload_layout=settings.get('uploadLayout'),process_layout=settings.get('processLayout'))


def compose_base(source,service):
    clone=json.loads(json.dumps(source));clone['services'][service].pop('image',None)
    return hashlib.sha256(json.dumps(clone,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def operation_binding(api,settings,ingress,args):
    return dict(nodeId=settings['nodeId'],project=settings['project'],service=settings['service'],coordinator=api.origin,
        backendName=ingress.config['backendName'],backendPort=ingress.config['backendPort'],engineId=args.docker_engine_id,
        expectedEngine=args.expected_engine,nodePort=args.node_port,workerMode=args.worker_mode,
        proxyName=ingress.config['proxyName'],proxyImage=ingress.config['proxyImage'],listenPort=ingress.config['listenPort'])


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
    if backend.get('engineVersion') != exact_engine_version(state.get('targetEngineVersion')):
        raise GateError('target engine version changed at ingress')
    status,public=ingress.http(ingress.config['listenPort'])
    if status!=200 or not isinstance(public,dict) or any(public.get(key)!=backend.get(key)
            for key in ('engine','engineVersion','version')):
        raise GateError('owned ingress health is not proven')
    return dict(state='open',containerId=identifier,oldWorkersDrained=True)


def finalize_success(root,api,operator,receipt,settings,ingress,adapter,state,drain_seconds,*,already_finished=False,worker_handover=None):
    if settings.get('workerMode')=='reference':
        from ops.reference_worker_handover import ReferenceWorkerHandover
        if not isinstance(worker_handover,ReferenceWorkerHandover):raise UpdateError('reference_worker_handover_unimplemented')
        worker_handover.verify_ready(require_idle=not already_finished)
    else:reviewed_worker_mode(settings.get('workerMode'))
    version = exact_engine_version(state.get('targetEngineVersion'))
    adapter.bind_engine_version(version)
    current_node = api.json('GET','/v1/prototype/nodes/'+settings['nodeId'])['node']
    if current_node.get('planId') != state['planId'] or current_node.get('targetImage') != state['targetImage'] or current_node.get('targetEngineVersion') != version:
        raise GateError('version-bound maintenance plan changed before reopen')
    if current_node.get('state') != ('active' if already_finished else 'updating'):
        raise GateError('maintenance state changed before reopen')
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
    if ingress.backend(state['targetImage']).get('engineVersion') != version:
        raise GateError('target engine version changed before ingress reopen')
    if state['stage'] in ('update_returned','compose_pin_pending','compose_pinned'):
        state['stage']='compose_pinned';save(receipt/'managed-update.json',state)
        closed=json.loads((ingress.root/'state.json').read_text())
        if closed.get('state')!='closed' or closed.get('containerId')!=state['closedIngress']['containerId']:
            raise GateError('owned closed ingress no longer matches')
        state['stage']='ingress_open_pending';save(receipt/'managed-update.json',state)
        ingress.change(False,expected=state['targetImage'],seconds=drain_seconds)
    proof=owned_open_proof(ingress,state)
    final_source=json.loads((root/'compose.json').read_text())
    if (final_source['services'][service]['image']!=state['targetImage']
            or compose_base(final_source,service)!=state['composeBaseSha256']
            or not (adapter.serving(state['targetImage']) if already_finished else adapter.healthy(state['targetImage']))):
        raise GateError('final pinned backend verification changed')
    sync_compose(root)
    if worker_handover is not None:worker_handover.verify_ready(require_idle=not already_finished)
    state.update(stage='finish_pending',openedIngress=proof)
    save(receipt/'managed-update.json',state)
    result=dict(healthy=True,image=state['targetImage'],previousImage=private['oldImage'],
        engineVersion=version,
        composePinUpdateRequired=False,persistentComposePin=state['targetImage'],ingress='open')
    if worker_handover is not None:result['workerHandoverSha256']=worker_handover.public_receipt_sha256()
    result=finalize_deferred(api,operator,settings['nodeId'],receipt,expected_plan_id=state['planId'],
        expected_target=state['targetImage'],result=result)
    state.update(stage='completed',persistentComposePin=state['targetImage'],result=result)
    save(receipt/'managed-update.json',state)
    return result


@contextmanager
def worker_factory_gate_yield(gate,handover,deadline):
    """Only yield this owned empty gate behind the still-closed queue/ingress."""
    handover._gate(gate)
    gate.__exit__(None,None,None)
    try:yield
    finally:
        while True:
            handover.guard()
            try:gate.__enter__();break
            except GateBusy:
                if time.monotonic()>=deadline:raise GateError('worker factory gate reacquisition unavailable')
                time.sleep(min(0.2,max(0,deadline-time.monotonic())))
        handover._gate(gate)


def run_reference(root,api,operator,receipt,wait_seconds,drain_seconds,*,expected_plan_id,expected_target):
    """Use the same planned update, physical gate and deferred finalization."""
    from ops.reference_worker_handover import ReferenceWorkerHandover,WorkerHandoverHeld,WorkerHandoverUncertain,private_bytes
    settings=json.loads((root/'maintenance.json').read_text());node_id=settings['nodeId']
    if settings.get('workerMode')!='reference':raise GateError('reference worker mode required')
    if (expected_plan_id is None)!=(expected_target is None):raise GateError('frozen plan and target required together')
    ingress=Ingress(root/'ingress');adapter=ComposeNode(adapter_args(root,settings,ingress),receipt)
    deadline=time.monotonic()+wait_seconds
    def worker_deadline():return time.monotonic()+min(30,max(1,drain_seconds))
    state={};allow_failed=False
    def fence(node):
        if expected_plan_id is not None and (node.get('planId'),node.get('targetImage'))!=(expected_plan_id,expected_target):
            raise GateError('expected maintenance plan or target changed')
    def guard():
        node=api.json('GET','/v1/prototype/nodes/'+node_id)['node'];fence(node)
        config=json.loads(private_bytes(Path(settings['referenceWorker']['configPath'])))
        if (config['nodeId']!=node_id or config['gateCoordinatorOrigin'].rstrip('/')!=api.origin
                or api.json('GET','/v1/prototype/authority')!={'authorityId':config['authorityId']}):
            raise GateError('worker coordinator binding changed')
        if (node.get('planId'),node.get('targetImage'),node.get('targetEngineVersion'))!=(
                state.get('planId'),state.get('targetImage'),state.get('targetEngineVersion')):
            raise GateError('reference maintenance plan changed')
        expected_states=('updating','failed') if allow_failed else ('updating',)
        if node.get('state') not in expected_states:raise GateError('reference node is no longer updating')
        reserved=state.get('reservedRevision')
        if reserved is not None:
            revision=reserved+(1 if node['state']=='failed' else 0)
            if node.get('revision')!=revision:raise GateError('reference maintenance revision changed')
        closed=json.loads((ingress.root/'state.json').read_text())
        if state.get('stage') in ('ingress_open_pending','finish_pending'):
            owned_open_proof(ingress,state)
            return
        if (closed.get('state')!='closed' or closed.get('containerId')!=state.get('closedIngress',{}).get('containerId')
                or ingress.inspect().get('Id')!=closed.get('containerId')
                or set(closed.get('oldWorkers',[])) & ingress.workers(ingress.inspect())):
            raise GateError('reference ingress is not provably closed')
    with lock(receipt),locked(root/'ingress'),ReferenceWorkerHandover(settings['referenceWorker'],receipt,guard=guard) as handover:
        managed=receipt/'managed-update.json';node=api.json('GET','/v1/prototype/nodes/'+node_id)['node'];fence(node)
        binding=operation_binding(api,settings,ingress,adapter.args)
        binding['referenceWorkerManifestSha256']=handover._manifest_sha
        if managed.exists():
            state.update(json.loads(managed.read_text()))
            if state.get('binding')!=binding:raise GateError('reference managed binding changed')
            if state.get('stage')=='failed_rolled_back':
                allow_failed=True;guard()
                if handover.state is None or handover.state.get('stage')!='rolled_back':raise GateError('worker rollback receipt unavailable')
                old=json.loads(base64.b64decode(handover.state['oldConfig']))
                handover.ready(expected_sha=handover.state['oldConfigSha256'],activation=old['maintenanceReadiness']['activation'],require_idle=False)
                return state['result']
            private=deferred_receipt(receipt,node_id,expected_plan_id=state['planId'],expected_target=state['targetImage'])
            final=dict(healthy=True,image=state['targetImage'],previousImage=private['oldImage'],engineVersion=state['targetEngineVersion'],
                composePinUpdateRequired=False,persistentComposePin=state['targetImage'],ingress='open',
                workerHandoverSha256=handover.public_receipt_sha256()) if handover.state and handover.state.get('stage')=='worker_ready' else None
            if final is not None and finish_matches(node,private,final):
                return finalize_success(root,api,operator,receipt,settings,ingress,adapter,state,drain_seconds,
                    already_finished=True,worker_handover=handover)
            if state.get('stage') in ('ingress_open_pending','finish_pending','completed'):
                # Never replay an uncertain reload or mutate worker/engine here.
                with idle_host_gate(api,node_id,adapter.args.docker_engine_id,deadline) as gate:
                    handover._gate(gate)
                    return finalize_success(root,api,operator,receipt,settings,ingress,adapter,state,drain_seconds,worker_handover=handover)
            if state.get('stage') not in ('ingress_closed','update_returned','compose_pin_pending','compose_pinned'):
                raise GateError('reference operation requires inspection')
            guard()
            if state['stage']=='ingress_closed':state['stage']='update_returned';save(managed,state)
        else:
            if (receipt/'update.json').exists():raise GateError('unbound reference update receipt')
            while True:
                node=api.json('GET','/v1/prototype/nodes/'+node_id)['node'];fence(node)
                if node['state'] not in ('scheduled','draining'):return dict(status='not_eligible',nodeState=node['state'])
                exact_engine_version(node.get('targetEngineVersion'))
                if node['state']=='draining' and datetime.fromisoformat(node['notBefore'].replace('Z','+00:00'))<=datetime.now(timezone.utc):break
                if time.monotonic()>=deadline:return dict(status='waiting',nodeState=node['state'])
                time.sleep(min(1,max(0,deadline-time.monotonic())))
            item=adapter.inspect();backend=json.loads(ingress.docker(['inspect',ingress.config['backendName']]))[0]
            source=json.loads((root/'compose.json').read_text())
            if backend['Id']!=item['Id'] or source['services'][settings['service']]['image']!=item['Image']:
                raise GateError('reference backend or Compose binding changed')
            state.update(stage='waiting_for_jobs',planId=node['planId'],targetImage=node['targetImage'],
                targetEngineVersion=node['targetEngineVersion'],oldImage=item['Image'],binding=binding,
                composeBaseSha256=compose_base(source,settings['service']))
            save(managed,state)
        with idle_host_gate(api,node_id,adapter.args.docker_engine_id,deadline) as gate:
            handover.authorize_plan(node_id,state,adapter)
            if not (receipt/'update.json').exists():
                def close_and_stop(reserved):
                    current=api.json('GET','/v1/prototype/nodes/'+node_id)['node'];fence(current)
                    if (current.get('state')!='updating' or current.get('revision')!=reserved['reservedRevision']
                            or current.get('planId')!=state['planId'] or current.get('targetEngineVersion')!=state['targetEngineVersion']):
                        raise GateError('reference reservation changed')
                    state.update(stage='ingress_close_pending',reservedRevision=reserved['reservedRevision']);save(managed,state)
                    closed=ingress.change(True,seconds=drain_seconds)
                    if closed.get('state')!='closed' or closed.get('oldWorkersDrained') is not True:raise GateError('reference ingress drain unavailable')
                    state.update(stage='ingress_closed',closedIngress=closed);save(managed,state)
                    handover.stop_old(gate,state,worker_deadline())
                result=execute(api,operator,adapter,node_id,receipt,wait_seconds=max(0,deadline-time.monotonic()),health_seconds=30,
                    expected_plan_id=state['planId'],expected_target=state['targetImage'],defer_finish=True,
                    on_reserved=close_and_stop,reference_handover=handover)
                state.update(stage='update_returned',result=result);save(managed,state)
                if result['status']!='succeeded':
                    allow_failed=True;guard();handover.restore_old_config(gate)
                    with worker_factory_gate_yield(gate,handover,worker_deadline()):handover.start_old(worker_deadline())
                    value=dict(result,workerRestored=True,ingress='closed')
                    state.update(stage='failed_rolled_back',result=value);save(managed,state);return value
            private=deferred_receipt(receipt,node_id,expected_plan_id=state['planId'],expected_target=state['targetImage'])
            adapter.bind_engine_version(state['targetEngineVersion'])
            if not adapter.healthy(state['targetImage']):raise GateError('qualified reference engine health changed')
            guard()
            if handover.state is None:raise GateError('reference worker stop evidence missing')
            handover._gate_owner=gate
            if handover.state['stage']=='worker_stopped':handover.pin_new(gate)
            handover.reconcile_pin()
            try:
                with worker_factory_gate_yield(gate,handover,worker_deadline()):handover.start_new(worker_deadline())
                return finalize_success(root,api,operator,receipt,settings,ingress,adapter,state,drain_seconds,worker_handover=handover)
            except WorkerHandoverUncertain:raise
            except (WorkerHandoverHeld,GateError,UpdateError):
                # Once authoritative Compose pinning starts, keep the new pin,
                # worker and engine aligned. Reconcile finalization; never claim
                # an old-image rollback while a target Compose pin may be durable.
                if state.get('stage') in ('compose_pin_pending','compose_pinned','ingress_open_pending','finish_pending','completed'):raise
                handover.stop_new(worker_deadline());handover.restore_old_config(gate)
                adapter.bind_engine_version(private['oldEngineVersion']);adapter.replace(private['oldImage'])
                if not wait_healthy(adapter,private['oldImage'],30):raise GateError('worker handover rollback engine unavailable')
                with worker_factory_gate_yield(gate,handover,worker_deadline()):handover.start_old(worker_deadline())
                result=dict(healthy=False,rollbackHealthy=True,restoredImage=private['oldImage'],workerRestored=True)
                operator.json('POST','/v1/prototype/nodes/'+node_id+'/finish',dict(planId=state['planId'],updateToken=private['updateToken'],status='failed',result=result))
                private.update(stage='completed',status='failed',engineQualifiedResult=private['result'],result=result)
                save(receipt/'update.json',private)
                value=dict(status='failed',nodeId=node_id,ingress='closed',**result)
                state.update(stage='failed_rolled_back',result=value);save(managed,state);return value


def run(root, api, operator, receipt, wait_seconds, drain_seconds=60,*,expected_plan_id=None,expected_target=None):
    settings = json.loads((root / 'maintenance.json').read_text())
    node_id = settings['nodeId']
    if settings.get('workerMode')=='reference' and 'referenceWorker' in settings:
        return run_reference(root,api,operator,receipt,wait_seconds,drain_seconds,
            expected_plan_id=expected_plan_id,expected_target=expected_target)
    reviewed_worker_mode(settings.get('workerMode'))
    if (expected_plan_id is None)!=(expected_target is None):
        raise GateError('frozen plan and target must be supplied together')
    def fence(value):
        if expected_plan_id is not None and (value.get('planId')!=expected_plan_id or value.get('targetImage')!=expected_target):
            raise GateError('expected maintenance plan or target changed')
    if expected_plan_id is not None:
        fence(api.json('GET','/v1/prototype/nodes/'+node_id)['node'])
    ingress = Ingress(root / 'ingress')
    args=adapter_args(root,settings,ingress)
    adapter = ComposeNode(args, receipt)
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
            version=exact_engine_version(state.get('targetEngineVersion'))
            adapter.bind_engine_version(version)
            private=deferred_receipt(receipt,node_id,expected_plan_id=state['planId'],expected_target=state['targetImage'])
            final=dict(healthy=True,image=state['targetImage'],previousImage=private['oldImage'],
                engineVersion=version,
                composePinUpdateRequired=False,persistentComposePin=state['targetImage'],ingress='open')
            node=api.json('GET','/v1/prototype/nodes/'+node_id)['node']
            fence(node)
            if node.get('targetEngineVersion') != version:
                raise GateError('frozen target engine version changed')
            if finish_matches(node,private,final):
                return finalize_success(root,api,operator,receipt,settings,ingress,adapter,state,drain_seconds,already_finished=True)
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
                return finalize_success(root,api,operator,receipt,settings,ingress,adapter,state,drain_seconds)
        if (receipt/'update.json').exists():
            raise GateError('unbound update receipt requires inspection')
        while True:
            node = api.json('GET', '/v1/prototype/nodes/' + node_id)['node']
            fence(node)
            if node['state'] not in ('scheduled', 'draining'):
                return {'status': 'not_eligible', 'nodeState': node['state']}
            version=exact_engine_version(node.get('targetEngineVersion'))
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
        state = dict(stage='waiting_for_jobs',planId=node['planId'],targetImage=node['targetImage'],targetEngineVersion=version,binding=binding,
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
            if current['planId'] != node['planId'] or current['targetImage'] != node['targetImage'] or current.get('targetEngineVersion') != version:
                raise GateError('maintenance plan changed while closing ingress')
            def close_reserved(reserved):
                current=api.json('GET','/v1/prototype/nodes/'+node_id)['node']
                fence(current)
                if (current.get('state')!='updating' or current.get('revision')!=reserved['reservedRevision']
                        or current.get('planId')!=state['planId'] or current.get('targetImage')!=state['targetImage']
                        or reserved['nodeId']!=node_id or reserved['planId']!=state['planId']
                        or reserved['targetImage']!=state['targetImage']):
                    raise GateError('owned reservation changed before ingress close')
                if current.get('targetEngineVersion') != version or reserved.get('targetEngineVersion') != version:
                    raise GateError('owned target engine version changed before ingress close')
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
            return finalize_success(root,api,operator,receipt,settings,ingress,adapter,state,drain_seconds)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--credentials', required=True)
    actions = parser.add_subparsers(dest='action', required=True)
    actions.add_parser('status')
    schedule = actions.add_parser('schedule')
    schedule.add_argument('--target-image', required=True)
    schedule.add_argument('--target-engine-version', required=True)
    schedule.add_argument('--drain-at', required=True)
    schedule.add_argument('--not-before', required=True)
    schedule.add_argument('--after-job-id')
    runner = actions.add_parser('run')
    runner.add_argument('--receipt', required=True)
    runner.add_argument('--wait-seconds', type=int, choices=range(0, 86401), default=86400)
    runner.add_argument('--drain-seconds', type=int, choices=range(1, 86401), default=3600)
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
        node = api.json('GET', '/v1/prototype/nodes/' + settings['nodeId'])['node']
        if args.action == 'status':
            result = {'node': node, 'ingressState': json.loads((root / 'ingress/state.json').read_text())['state']}
        elif args.action == 'schedule':
            receipt_args=adapter_args(root,settings,Ingress(root/'ingress'))
            ComposeNode(receipt_args, root).image(args.target_image)
            result = operator.json('POST', '/v1/prototype/nodes/' + settings['nodeId'] + '/schedule', {
                'expectedRevision': node['revision'], 'targetImage': args.target_image,
                'targetEngineVersion': exact_engine_version(args.target_engine_version),
                'drainAt': args.drain_at, 'notBefore': args.not_before, 'afterJobId': args.after_job_id})
        else:
            result = run(root, api, operator, private_directory(args.receipt), args.wait_seconds, args.drain_seconds)
        print(json.dumps(result, sort_keys=True))
        return 1 if result.get('status') == 'failed' else 0
    except (GateError, NodeGateError, UpdateError, SmokeError, OSError, ValueError, KeyError):
        print('Managed maintenance stopped. Inspect private receipt, ingress and coordinator gate; no automatic replay or reopen.')
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
