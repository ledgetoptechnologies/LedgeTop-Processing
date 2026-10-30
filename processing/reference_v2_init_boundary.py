"""Disabled v2 NodeODM initialization boundary; not an engine workflow.

No upload, commit, poll, result retrieval or hold release exists here. The
caller must supply a private, durable, exclusively owned SQLite connection,
authenticated claim, externally pinned database inode/epoch, a durable unique operation key and a physical fence
against all uncooperative engine and filesystem writers. Unknown outcomes are
held forever for explicit reconciliation; they are never retried here.
"""
import os
import sqlite3
import stat
import ipaddress
from urllib.parse import urlsplit
from . import reference_protocol as p
from .node_gate import NodeGate, GateError
from .reference_v2_node_preflight import operation_receipt
from .reference_v2_local_input import verify_complete_input_set

META_DDL="""CREATE TABLE v2_init_epoch (id INTEGER PRIMARY KEY CHECK(id=1), epoch TEXT NOT NULL)"""
DDL="""CREATE TABLE v2_init_intents (job TEXT PRIMARY KEY, identity TEXT NOT NULL, phase TEXT NOT NULL)"""

class InitHeld(ValueError): pass


def initialize_once(*, enabled=False, db, engine, fence, journal, ledger,
        reservation_id, binding, manifest_bytes, engine_id, node_id, origin,
        authority_id, generation, worker_id, operation_key, expected_epoch, expected_inode, authenticated_claim):
    """Issue at most one initialize after durable intent; never auto-retry.

    `engine.initialize` is injected and must target the configured node. A
    successful response is not proof that upload or commit occurred. On any
    response error or process death, the pending intent remains ambiguous.
    """
    if enabled is not True:raise InitHeld('v2 initialization disabled')
    if type(db) is not sqlite3.Connection or db.in_transaction:raise InitHeld('idle private database required')
    db.execute('PRAGMA synchronous=FULL')
    path=db.execute('PRAGMA database_list').fetchone()[2]
    if not path or not os.path.isabs(path) or type(expected_inode) is not tuple or len(expected_inode)!=2:
        raise InitHeld('externally pinned persistent database required')
    try:
        entry=os.stat(path,follow_symlinks=False)
        if not stat.S_ISREG(entry.st_mode) or (entry.st_dev,entry.st_ino)!=expected_inode or entry.st_nlink!=1:
            raise InitHeld('intent database identity changed')
    except OSError:raise InitHeld('intent database unavailable') from None
    if type(expected_epoch) is not str or len(expected_epoch)!=64 or any(c not in '0123456789abcdef' for c in expected_epoch):
        raise InitHeld('externally pinned epoch required')
    try:
        p._closed(authenticated_claim,('jobId','manifestSha256','generation','engineId','engineOrigin','options'),'authenticated claim')
        if (authenticated_claim['jobId'],authenticated_claim['manifestSha256'],authenticated_claim['generation'],authenticated_claim['engineId']) != (binding['jobId'],binding['manifestSha256'],binding['generation'],engine_id):raise ValueError()
        endpoint=authenticated_claim['engineOrigin']; parsed=urlsplit(endpoint)
        if (parsed.scheme not in ('http','https') or not ipaddress.ip_address(parsed.hostname).is_loopback
            or parsed.username is not None or parsed.password is not None or parsed.path not in ('','/') or parsed.query or parsed.fragment or endpoint!=engine.origin):raise ValueError()
        options=authenticated_claim['options']
        if type(options) is not list or len(options)>256 or len(p.canonical_json(options))>32768:raise ValueError()
        for option in options:
            p._closed(option,('name','value'),'engine option')
            p._slug(option['name'],'engine option name')
            if type(option['value']) not in (str,int,bool) or len(p.canonical_json(option['value']))>1024:raise ValueError()
    except (ValueError,TypeError,KeyError,AttributeError):raise InitHeld('authenticated engine claim invalid') from None
    receipt=operation_receipt(operation_key=operation_key,binding=binding,reservation_id=reservation_id,
        engine_id=engine_id,node_id=node_id,origin=origin,authority_id=authority_id,generation=generation,worker_id=worker_id)
    identity=p.canonical_json(dict(binding=binding,reservationId=reservation_id,engineId=engine_id,nodeId=node_id,
        origin=origin,authorityId=authority_id,generation=generation,workerId=worker_id,receiptId=receipt,epoch=expected_epoch,claim=authenticated_claim)).decode()
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
            rows=[tuple(row) for row in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")]
            expected=[('index','sqlite_autoindex_v2_init_intents_1','v2_init_intents',None),('table','v2_init_epoch','v2_init_epoch',META_DDL),('table','v2_init_intents','v2_init_intents',DDL)]
            if rows!=expected or db.execute("SELECT 1 FROM sqlite_temp_master LIMIT 1").fetchone():
                raise InitHeld('preprovisioned initialize schema required')
            if db.execute('SELECT id,epoch FROM v2_init_epoch').fetchall()!=[(1,expected_epoch)]:
                raise InitHeld('initialize epoch differs')
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
        engine.initialize(dict(name=binding['jobId'],engine_uuid=binding['jobId'],outputs=None,options=options))
        return {'state':'initialize_sent_outcome_requires_reconciliation','receiptId':receipt}
