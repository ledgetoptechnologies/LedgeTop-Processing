"""Disabled v2 NodeODM initialization boundary; not an engine workflow.

No upload, commit, poll, result retrieval or hold release exists here. The
caller must supply a private, durable, exclusively owned SQLite connection,
authenticated metadata, a durable unique operation key and a physical fence
against all uncooperative engine and filesystem writers. Unknown outcomes are
held forever for explicit reconciliation; they are never retried here.
"""
import os
import sqlite3
from . import reference_protocol as p
from .node_gate import NodeGate, GateError
from .reference_v2_node_preflight import operation_receipt
from .reference_v2_local_input import verify_complete_input_set

DDL="""CREATE TABLE v2_init_intents (job TEXT PRIMARY KEY, identity TEXT NOT NULL, phase TEXT NOT NULL)"""

class InitHeld(ValueError): pass


def initialize_once(*, enabled=False, db, engine, fence, journal, ledger,
        reservation_id, binding, manifest_bytes, engine_id, node_id, origin,
        authority_id, generation, worker_id, operation_key):
    """Issue at most one initialize after durable intent; never auto-retry.

    `engine.initialize` is injected and must target the configured node. A
    successful response is not proof that upload or commit occurred. On any
    response error or process death, the pending intent remains ambiguous.
    """
    if enabled is not True:raise InitHeld('v2 initialization disabled')
    if type(db) is not sqlite3.Connection or db.in_transaction:raise InitHeld('idle private database required')
    db.execute('PRAGMA synchronous=FULL')
    path=db.execute('PRAGMA database_list').fetchone()[2]
    if not path or not os.path.isabs(path):raise InitHeld('persistent database required')
    receipt=operation_receipt(operation_key=operation_key,binding=binding,reservation_id=reservation_id,
        engine_id=engine_id,node_id=node_id,origin=origin,authority_id=authority_id,generation=generation,worker_id=worker_id)
    identity=p.canonical_json(dict(binding=binding,reservationId=reservation_id,engineId=engine_id,nodeId=node_id,
        origin=origin,authorityId=authority_id,generation=generation,workerId=worker_id,receiptId=receipt)).decode()
    fence.check()
    with NodeGate(engine_id) as gate:
        state=gate.snapshot();owner=state['reservation']
        if ((state['nodeId'],state['coordinatorOrigin'],state['authorityId'],state['generation'])!=(node_id,origin,authority_id,generation)
            or owner is None or owner['receiptId']!=receipt or owner['completionDigest'] is not None
            or owner['job']!=dict(id=binding['jobId'],workerId=worker_id,nodeId=node_id,authorityId=authority_id)):
            raise InitHeld('exact retained node gate required')
        verify_complete_input_set(fence=fence,journal=journal,ledger=ledger,reservation_id=reservation_id,binding=binding,manifest_bytes=manifest_bytes)
        fence.check()
        try:
            db.execute('BEGIN IMMEDIATE')
            rows=db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name='v2_init_intents' OR name='v2_init_intents' ORDER BY type,name").fetchall()
            if not rows:
                db.execute(DDL)
                rows=db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name='v2_init_intents' OR name='v2_init_intents' ORDER BY type,name").fetchall()
            expected=[('index','sqlite_autoindex_v2_init_intents_1','v2_init_intents',None),('table','v2_init_intents','v2_init_intents',DDL)]
            if rows!=expected or db.execute("SELECT 1 FROM sqlite_temp_master WHERE tbl_name='v2_init_intents' OR name='v2_init_intents'").fetchone():
                raise InitHeld('initialize intent schema differs')
            row=db.execute('SELECT identity,phase FROM v2_init_intents WHERE job=?',(binding['jobId'],)).fetchone()
            if row is not None:
                if row[0]!=identity:raise InitHeld('initialize intent identity differs')
                raise InitHeld('initialize already attempted or outcome unknown')
            db.execute("INSERT INTO v2_init_intents VALUES(?,?,'pending')",(binding['jobId'],identity))
            db.commit()
        except Exception:
            db.rollback();raise
        fence.check()
        # The intent is durable before this call. Any failure, including death
        # before sending, is deliberately indistinguishable and never replayed.
        engine.initialize(dict(name=binding['jobId'],engine_uuid=binding['jobId'],outputs=None,options=[]))
        return {'state':'initialize_sent_outcome_requires_reconciliation','receiptId':receipt}
