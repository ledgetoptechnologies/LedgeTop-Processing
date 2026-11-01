"""Inactive offline-migrated coordinator sender evidence experiment.

Evidence survives process restarts, but does not establish node receipt,
coordinator epoch adoption, or a cross-process security boundary.
"""
import hashlib
import threading
import time

DDL = "CREATE TABLE broker_sender_evidence (operation_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, epoch TEXT NOT NULL, grant_hash TEXT NOT NULL)"
TRIGGERS = {
    'broker_sender_evidence_update': "CREATE TRIGGER broker_sender_evidence_update BEFORE UPDATE ON broker_sender_evidence BEGIN SELECT RAISE(ABORT,'sender evidence immutable'); END",
    'broker_sender_evidence_delete': "CREATE TRIGGER broker_sender_evidence_delete BEFORE DELETE ON broker_sender_evidence BEGIN SELECT RAISE(ABORT,'sender evidence immutable'); END",
}

class EvidenceError(ValueError): pass

def attest(db):
    from .broker_intent_fence import attest as attest_intent
    attest_intent(db)
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='broker_sender_evidence'").fetchone()
    if row is None or row[0]!=DDL:raise EvidenceError('sender evidence schema unavailable')
    for name,sql in TRIGGERS.items():
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(name,)).fetchone()
        if row is None or row[0]!=sql:raise EvidenceError('sender evidence fence unavailable')

def install_offline(db, *, enabled=False):
    if enabled is not True or db.in_transaction:raise EvidenceError('offline migration required')
    db.execute('BEGIN IMMEDIATE')
    try:
        from .broker_intent_fence import attest as attest_intent
        attest_intent(db)
        if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held')").fetchone():raise EvidenceError('idle coordinator required')
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='broker_sender_evidence'").fetchone():raise EvidenceError('existing sender evidence requires inspection')
        db.execute(DDL)
        for sql in TRIGGERS.values():db.execute(sql)
        attest(db);db.commit()
    except BaseException:
        db.rollback();raise

class DurableUploadEvidence:
    def __init__(self, queue):
        from .prototype import Queue
        if type(queue) is not Queue or queue._lock.closed:raise EvidenceError('actual coordinator required')
        self.queue=queue;self._condition=threading.Condition()
        with queue._db() as db:attest(db)

    def _record_verified(self, grant, *, completion, owner, store, claim_token, item, deadline):
        from .guarded_native_engine import SenderCompletion
        from .node_mutation_broker import LocalUploadSender, _json
        from .remote_upload_transport import RemoteUploadSender
        if (type(completion) is not SenderCompletion or type(owner) is not RemoteUploadSender
                or completion.engine is not owner or completion.upload_id!=grant['operationId']
                or getattr(store,'queue',None) is not self.queue):raise EvidenceError('owned sender required')
        completion.verify(deadline)
        if LocalUploadSender._issued_item(store,grant,claim_token)!=item:raise EvidenceError('source identity changed')
        digest=hashlib.sha256(_json(grant)).hexdigest()
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');attest(db)
            row=db.execute('SELECT job_id,grant_hash,state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()
            if row is None or tuple(row)!=(grant['jobId'],digest,'started') or grant['coordinatorEpoch']!=self.queue.startup_id:
                raise EvidenceError('active sender intent required')
            existing=db.execute('SELECT job_id,epoch,grant_hash FROM broker_sender_evidence WHERE operation_id=?',(grant['operationId'],)).fetchone()
            value=(grant['jobId'],grant['coordinatorEpoch'],digest)
            if existing is None:db.execute('INSERT INTO broker_sender_evidence VALUES(?,?,?,?)',(grant['operationId'],*value))
            elif tuple(existing)!=value:raise EvidenceError('sender evidence changed')
        with self._condition:self._condition.notify_all()

    def wait_verified(self, grant, deadline):
        from .node_mutation_broker import _json
        digest=hashlib.sha256(_json(grant)).hexdigest()
        with self._condition:
            while True:
                with self.queue._db() as db:
                    attest(db)
                    row=db.execute('SELECT job_id,epoch,grant_hash FROM broker_sender_evidence WHERE operation_id=?',(grant['operationId'],)).fetchone()
                if row is not None:return tuple(row)==(grant['jobId'],grant['coordinatorEpoch'],digest)
                remaining=deadline-time.monotonic()
                if remaining<=0:return False
                self._condition.wait(remaining)
