"""Read-only exhaustion veto, never a reservation or positive admission proof.

Only the immutable non-retiring v2/v3 journals are understood. An unsealed
preclaim is excluded so observing the current claim cannot veto that same claim.
Retiring profiles need a different proof of their current reusable partition.
"""
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import time

from . import reference_indexed_fetch as indexed
from . import reference_protocol as p

PATH='/ltds/worker/v1/metadata-capacity'
SCOPE='retained_metadata_exhaustion_veto'
IDENTITY_FIELDS={'identityScope','nodeId','authorityId','workerId','workerInstanceId',
    'engineId','gateGeneration','engineVersion'}
FIELDS=IDENTITY_FIELDS|{'schemaVersion','scope','policyId','policySha256','capacity',
    'sealedRetainedJobs','state'}
SUPPORTED=(indexed.POLICY_ID,indexed.THREE_POLICY_ID)


def observation_state(code, body, *, identity):
    """Validate a negative-only observation against independent routing facts."""
    if (code!=200 or type(body) is not dict or set(body)!=FIELDS
            or type(identity) is not dict or set(identity)!=IDENTITY_FIELDS
            or any(type(body[k]) is not type(identity[k]) or body[k]!=identity[k]
                for k in IDENTITY_FIELDS)
            or type(body['schemaVersion']) is not int or body['schemaVersion']!=1
            or body['scope']!=SCOPE or body['policyId'] not in SUPPORTED):
        return 'unknown'
    descriptor=indexed.compiled_profile_descriptor(body['policyId'])
    limits=descriptor['limits']
    capacity=min(limits['retainedJobs'],(limits['databasePageCap']*limits['pageSize']
        -limits['nonJobControlBytes'])//limits['jobControlBytes'])
    count=body['sealedRetainedJobs']
    if (body['policySha256']!=descriptor['policySha256']
            or type(body['capacity']) is not int or body['capacity']!=capacity
            or type(count) is not int or not 0<=count<=capacity
            or body['state']!=('exhausted' if count==capacity else 'not_exhausted')):
        return 'unknown'
    return body['state']


def observe(driver):
    """Inspect the actual owned journal without opening a writer/advancing clock."""
    from .reference_driver import ReferenceDriver
    if type(driver) is not ReferenceDriver:
        raise ValueError('actual_metadata_driver_required')
    context=driver._indexed_context_initial
    if (type(context) is not indexed.IndexedFetchContext
            or driver.indexed_fetch_context is not context
            or type(driver._indexed) is not indexed.IndexedFetchJournal
            or driver._indexed.context is not context
            or driver._indexed.owner is not driver._state_owner
            or context.policy.policy_id not in SUPPORTED):
        raise ValueError('metadata_capacity_profile_unavailable')
    if not driver._lock.acquire(blocking=False):
        raise ValueError('metadata_capacity_busy')
    try:
        binding=json.loads(context.binding_json)
        owner=driver._state_owner
        owner.check(driver.path.parent)
        actual=indexed.read_only_profile(driver.path,bootstrap_id=context.bootstrap_id,
            binding=binding,owner=owner)
        if actual!=context:raise ValueError('metadata_capacity_context_changed')
        path=Path(driver.path);before=path.stat()
        facts=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        policy=context.policy
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
            deadline=time.monotonic()+.5
            db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
            db.execute('BEGIN')
            # Physical/profile/schema authority is validated on THIS actual DB.
            indexed.validate_profile_connection(db,bootstrap_id=context.bootstrap_id,
                binding=binding,root_identity=context.root_identity,
                driver_identity=context.driver_identity)
            stored=db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()
            if stored is None or stored[0]!=driver._indexed.driver_binding:
                raise ValueError('metadata_capacity_driver_binding_changed')
            db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
            rows=db.execute('''SELECT q.job,q.charge,j.stage,c.closure,s.sha
                FROM indexed_quota q LEFT JOIN driver_jobs j ON j.job=q.job
                LEFT JOIN indexed_claims c ON c.job=q.job
                LEFT JOIN indexed_seals s ON s.job=q.job ORDER BY q.job LIMIT ?''',
                (policy.capacity+1,)).fetchall()
            if len(rows)>policy.capacity:raise ValueError('metadata_capacity_rows_changed')
            sealed=0
            for job,charge,stage,claim_sha,seal_sha in rows:
                p._slug(job,'retained metadata job')
                if stage not in {'claim_pending','claimed','reservation_pending','awaiting_grant',
                        'fetching_pending','fetching','processing_pending','execution_pending',
                        'executing','delivering','failure_delivering','awaiting_acceptance',
                        'awaiting_failure_acceptance','completed'}:
                    raise ValueError('metadata_capacity_stage_changed')
                if type(charge) is not int or charge!=indexed.JOB_CHARGE:
                    raise ValueError('metadata_capacity_charge_changed')
                if claim_sha is None and seal_sha is None:
                    if stage!='claim_pending':raise ValueError('metadata_capacity_unsealed_job')
                else:
                    p._hash(claim_sha,'retained claim digest');p._hash(seal_sha,'retained seal digest')
                    if claim_sha!=seal_sha:raise ValueError('metadata_capacity_seal_changed')
                    sealed+=1
            counts=[db.execute('SELECT COUNT(*) FROM '+name).fetchone()[0]
                for name in ('driver_jobs','indexed_claims','indexed_seals')]
            if counts!=[len(rows),sealed,sealed]:raise ValueError('metadata_capacity_partition_changed')
        if facts(path.stat())!=facts(before):raise ValueError('metadata_capacity_snapshot_changed')
        if indexed.read_only_profile(path,bootstrap_id=context.bootstrap_id,binding=binding,owner=owner)!=context:
            raise ValueError('metadata_capacity_context_changed')
        owner.check(path.parent)
        descriptor=indexed.compiled_profile_descriptor(policy.policy_id)
        return dict(schemaVersion=1,scope=SCOPE,policyId=policy.policy_id,
            policySha256=descriptor['policySha256'],capacity=policy.capacity,
            sealedRetainedJobs=sealed,state='exhausted' if sealed==policy.capacity else 'not_exhausted')
    finally:
        driver._lock.release()
