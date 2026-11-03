"""Held-only coordinator/node comparison; never authorizes resume or cleanup.

The mTLS peer and claim key authenticate a cooperating peer, not current
coordinator authority. Old-writer exclusion and durable epoch revocation remain
required before any observation can be used to resume a task.
"""
import hashlib
import json
import secrets
import uuid

from . import reference_protocol as protocol
from .node_gate import GateError
from .node_job_claim import verify_job_claim
from .viewer_tasks import TaskStore


class ReconciliationHeld(ValueError):
    pass


def inspect_checkpoint(store, identifier, envelope, client, *, claim_key,
                       node, authority, gate_generation, origin):
    """Compare a persisted pending coordinator checkpoint with the node journal.

    The result is a transient observation only. It deliberately does not
    acknowledge the checkpoint, release files, or alter task state.
    """
    task = store.task(identifier)
    checkpoint = store.guarded_dispatch_checkpoint(identifier)
    if (task.get('node_guarded') != 1 or not isinstance(checkpoint, dict)
            or checkpoint.get('state') != 'pending' or task.get('job') is None
            or task['job'].get('status') != 'held'):
        raise ReconciliationHeld('pending guarded checkpoint unavailable')
    if any(checkpoint.get(k) != task.get(k) for k in ('job_id', 'node_id', 'engine_uuid', 'generation')):
        raise ReconciliationHeld('checkpoint identity changed')
    try:
        claim = verify_job_claim(envelope, claim_key, node=node, authority=authority,
                                 gate_generation=gate_generation, task=task['engine_uuid'],
                                 origin=origin, admission=False)
        if (claim['viewerJobId'], claim['viewerGeneration'], claim['nodeId'], claim['environment']) != (
                task['job_id'], task['generation'], task['node_id'], task['environment']):
            raise ValueError()
        init_task = {k: task[k] for k in ('name', 'engine_uuid', 'outputs', 'options')}
        if claim['requestSha256'] != protocol.digest(init_task):
            raise ValueError()
        evidence = json.loads(checkpoint['evidence'])
        if json.dumps(evidence, sort_keys=True, separators=(',', ':'), allow_nan=False) != checkpoint['evidence']:
            raise ValueError()
        kind = checkpoint['operation']
        if kind == 'init':
            expected = hashlib.sha256(json.dumps(init_task, sort_keys=True, separators=(',', ':'),
                                                 allow_nan=False).encode()).hexdigest()
            if evidence != {'requestSha256': expected}:
                raise ValueError()
            request = dict(task=task['engine_uuid'], name=task['name'], outputs=task['outputs'], options=task['options'])
            suffix = ''
        elif kind == 'upload':
            if type(evidence) is not dict or set(evidence) != {'name', 'bytes', 'sha256'}:
                raise ValueError()
            matches = [f for f in task['files'] if all(f[k] == evidence[k] for k in evidence)]
            if len(matches) != 1:
                raise ValueError()
            suffix = hashlib.sha256(TaskStore._filename(evidence['name']).encode('ascii')).hexdigest()
            request = dict(task=task['engine_uuid'], **evidence)
        elif kind == 'commit':
            if evidence != {'engine_uuid': task['engine_uuid']}:
                raise ValueError()
            request, suffix = dict(task=task['engine_uuid']), ''
        else:
            raise ValueError()
        operation = dict(operationId=str(uuid.uuid5(uuid.UUID(task['engine_uuid']), kind + ':' + suffix)),
                         kind=kind, requestDigest=protocol.digest(request), nonce=secrets.token_hex(32))
        expected_ack = protocol.digest(dict(task=task['engine_uuid'], kind=kind,
            requestDigest=operation['requestDigest'], validation='native-engine-contract-passed'))
    except (ValueError, KeyError, TypeError, protocol.ProtocolError, GateError):
        raise ReconciliationHeld('checkpoint binding unavailable') from None
    result = client.inspect_bound_mutation(envelope, operation)
    if (result.get('state') == 'done' and result.get('ackDigest') != expected_ack):
        raise ReconciliationHeld('node acknowledgement differs')
    # The query is not atomic across machines. Refuse even an observation if
    # the local task or checkpoint changed while the network request ran.
    current = store.task(identifier)
    if (store.guarded_dispatch_checkpoint(identifier) != checkpoint or
            any(current.get(k) != task.get(k) for k in ('job_id','node_id','engine_uuid','generation','environment','node_guarded',
                                                         'name','options','outputs','files')) or
            not isinstance(current.get('job'), dict) or current['job'].get('status') != 'held'):
        raise ReconciliationHeld('checkpoint changed during inspection')
    return {'state': result['state'], 'held': True, 'operation': kind}
