"""Disabled coordinator-owned signed photo execution plan candidate."""
import base64
import hashlib
import json
import uuid

from .node_mutation_broker import ReferenceCoordinatorAuthority, sign_grant, sign_reference_issue, sign_reference_commit
from .reference_engine_worker import _reference_broker_init_body
from . import reference_protocol as protocol

REFERENCE_PLAN_QUALIFIED = False
_BINDING_DDL = ('CREATE TABLE reference_plan_node_binding_candidate ('
                'node_id TEXT PRIMARY KEY, authority_id TEXT NOT NULL, engine_id TEXT NOT NULL, gate_generation INTEGER NOT NULL)')


def install_plan_binding_offline(db, *, node_id, authority_id, engine_id, gate_generation, enabled=False):
    """Explicit coordinator-local provisioning only while no work is active."""
    import re
    if (enabled is not True or db.in_transaction or node_id not in ('cluster','main')
            or type(authority_id) is not str or str(uuid.UUID(authority_id))!=authority_id
            or type(engine_id) is not str or not re.fullmatch('[A-Za-z0-9:._-]{1,255}',engine_id)
            or type(gate_generation) is not int or gate_generation<1):
        raise ValueError('offline reference binding required')
    db.execute('BEGIN IMMEDIATE')
    try:
        if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held')").fetchone() or db.execute('SELECT 1 FROM broker_intents').fetchone():
            raise ValueError('idle empty reference binding required')
        schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_plan_node_binding_candidate'").fetchone()
        if schema is None:db.execute(_BINDING_DDL)
        elif schema[0]!=_BINDING_DDL:raise ValueError('reference binding schema changed')
        db.execute('INSERT INTO reference_plan_node_binding_candidate VALUES(?,?,?,?)',
                   (node_id,authority_id,engine_id,gate_generation))
        db.commit()
    except BaseException:
        db.rollback();raise


def issue_reference_plan(witness,job,*,claim_token,worker_id,worker_instance_id,claim_request_id,
                         secret,timeout=10):
    """Sign deterministic operation identities after rechecking actual claim."""
    if REFERENCE_PLAN_QUALIFIED is not True or type(witness) is not ReferenceCoordinatorAuthority:
        raise ValueError('reference plan is not qualified')
    if (type(job) is not dict or type(claim_token) is not str or not claim_token.isascii()
            or type(secret) is not bytes or len(secret)<32):
        raise ValueError('exact reference plan inputs required')
    manifest=job['submission']['manifest'];pipeline=job['submission']['pipeline']
    if (not pipeline['odm']['requested'] or pipeline['treedar']['requested'] or pipeline['gaussian_splat']['requested']
            or type(manifest['objects']) is not list or not 0<len(manifest['objects'])<=protocol.MAX_OBJECTS
            or any(obj['kind']!='image' for obj in manifest['objects'])):
        raise ValueError('photo-only reference plan required')
    with witness.queue._db() as db:
        schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_plan_node_binding_candidate'").fetchone()
        binding=db.execute('SELECT * FROM reference_plan_node_binding_candidate WHERE node_id=?',(job['nodeId'],)).fetchone() if schema is not None and schema[0]==_BINDING_DDL else None
        if binding is None or binding['authority_id']!=witness.authority:
            raise ValueError('trusted reference node binding unavailable')
        engine_id,gate_generation=binding['engine_id'],binding['gate_generation']
    def operation(label):return str(uuid.uuid5(uuid.UUID(job['jobId']),witness.epoch+':'+label))
    body,content_type=_reference_broker_init_body(job)
    grant=dict(profile='reference-init-v1',authorityId=witness.authority,coordinatorEpoch=witness.epoch,
        nodeId=job['nodeId'],engineId=engine_id,jobId=job['jobId'],engineTaskId=job['jobId'],
        environment=job['environment'],gateGeneration=gate_generation,taskGeneration=job['generation'],
        operationId=operation('init'),operation='init',bodySha256=hashlib.sha256(body).hexdigest(),contentType=content_type,
        workerId=worker_id,workerInstanceId=worker_instance_id,claimRequestId=claim_request_id,
        claimTokenSha256=hashlib.sha256(claim_token.encode('ascii')).hexdigest(),
        inputManifestSha256=job['manifestSha256'],pipelineDigest=job['pipelineDigest'])
    with witness.hold(grant,timeout,_remote_fenced=True):
        schema_now=witness._active_db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_plan_node_binding_candidate'").fetchone()
        if schema_now is None or schema_now[0]!=_BINDING_DDL:
            raise ValueError('trusted reference binding schema changed')
        binding_now=witness._active_db.execute('SELECT * FROM reference_plan_node_binding_candidate WHERE node_id=?',(job['nodeId'],)).fetchone()
        if binding_now is None or tuple(binding_now)!=(job['nodeId'],witness.authority,engine_id,gate_generation):
            raise ValueError('trusted reference node binding changed')
        stored=witness._active_db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?',(job['jobId'],)).fetchone()
        if stored is None or protocol.canonical_json(json.loads(stored['input_json'])) != protocol.canonical_json(job['submission']):
            raise ValueError('reference plan submission changed')
        uploads=[]
        for index,obj in enumerate(manifest['objects']):
            if index % 128 == 0:witness.check_deadline()
            op=operation('upload:'+obj['id'])
            uploads.append(dict(objectId=obj['id'],operationId=op,signature=sign_reference_issue(grant,obj['id'],op,secret)))
        commit=operation('commit')
        plan=dict(initGrant=grant,initSignature=sign_grant(grant,secret),initBodyBase64=base64.b64encode(body).decode(),
            uploads=uploads,commit=dict(operationId=commit,bodyBase64='',contentType='application/json',
                                        signature=sign_reference_commit(grant,commit,b'','application/json',secret)))
        if len(protocol.canonical_json(plan))>protocol.MAX_BODY_BYTES:
            raise ValueError('reference plan exceeds wire bound')
        witness.check_deadline()
        return plan
