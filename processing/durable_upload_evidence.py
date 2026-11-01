"""Inactive offline-migrated coordinator sender evidence experiment.

Evidence survives process restarts, but does not establish node receipt,
coordinator epoch adoption, or a cross-process security boundary.
"""
import hashlib
import threading
import time

DDL = "CREATE TABLE broker_sender_evidence (operation_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, epoch TEXT NOT NULL, grant_hash TEXT NOT NULL)"
RECEIPT_DDL = "CREATE TABLE broker_node_receipts (operation_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, epoch TEXT NOT NULL, grant_hash TEXT NOT NULL, receipt_json TEXT NOT NULL, receipt_sha256 TEXT NOT NULL)"
TRIGGERS = {
    'broker_sender_evidence_update': "CREATE TRIGGER broker_sender_evidence_update BEFORE UPDATE ON broker_sender_evidence BEGIN SELECT RAISE(ABORT,'sender evidence immutable'); END",
    'broker_sender_evidence_delete': "CREATE TRIGGER broker_sender_evidence_delete BEFORE DELETE ON broker_sender_evidence BEGIN SELECT RAISE(ABORT,'sender evidence immutable'); END",
    'broker_node_receipts_update': "CREATE TRIGGER broker_node_receipts_update BEFORE UPDATE ON broker_node_receipts BEGIN SELECT RAISE(ABORT,'node receipt immutable'); END",
    'broker_node_receipts_delete': "CREATE TRIGGER broker_node_receipts_delete BEFORE DELETE ON broker_node_receipts BEGIN SELECT RAISE(ABORT,'node receipt immutable'); END",
}

class EvidenceError(ValueError): pass

def attest(db):
    from .broker_intent_fence import attest as attest_intent
    attest_intent(db)
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='broker_sender_evidence'").fetchone()
    if row is None or row[0]!=DDL:raise EvidenceError('sender evidence schema unavailable')
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='broker_node_receipts'").fetchone()
    if row is None or row[0]!=RECEIPT_DDL:raise EvidenceError('node receipt schema unavailable')
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
        db.execute(RECEIPT_DDL)
        for sql in TRIGGERS.values():db.execute(sql)
        attest(db);db.commit()
    except BaseException:
        db.rollback();raise

def verify_existing_node_receipt(db, grant):
    import json
    from .node_mutation_broker import _json, _upload_receipt
    attest(db)
    digest=hashlib.sha256(_json(grant)).hexdigest()
    row=db.execute('SELECT job_id,epoch,grant_hash,receipt_json,receipt_sha256 FROM broker_node_receipts WHERE operation_id=?',(grant['operationId'],)).fetchone()
    proof=db.execute('SELECT job_id,epoch,grant_hash FROM broker_sender_evidence WHERE operation_id=?',(grant['operationId'],)).fetchone()
    if row is None or proof is None or tuple(proof)!=(grant['jobId'],grant['coordinatorEpoch'],digest):
        raise EvidenceError('completed upload receipt requires inspection')
    try:
        receipt=row['receipt_json'];payload=json.loads(receipt)
        canonical,receipt_hash=_upload_receipt(grant,payload['reply'],digest)
        if canonical!=receipt or tuple(row)!=(grant['jobId'],grant['coordinatorEpoch'],digest,receipt,receipt_hash):raise ValueError()
    except (ValueError,TypeError,KeyError):raise EvidenceError('completed upload receipt differs') from None

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

    def commit_node_receipt(self, grant, receipt):
        """Atomically retain a trusted node assertion and complete its exact intent."""
        import json
        from .node_mutation_broker import _json, _upload_receipt
        if type(receipt) is not str or len(receipt)>10000:raise EvidenceError('node receipt unavailable')
        try:
            payload=json.loads(receipt)
            digest=hashlib.sha256(_json(grant)).hexdigest()
            canonical,receipt_hash=_upload_receipt(grant,payload['reply'],digest)
            if canonical!=receipt:raise ValueError()
        except (ValueError,TypeError,KeyError):raise EvidenceError('node receipt differs') from None
        if grant['coordinatorEpoch']!=self.queue.startup_id:raise EvidenceError('coordinator epoch differs')
        uncertain=False
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');attest(db)
            intent=db.execute('SELECT job_id,grant_hash,state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()
            if intent is None or tuple(intent) not in ((grant['jobId'],digest,'started'),(grant['jobId'],digest,'complete')):
                raise EvidenceError('node receipt intent differs')
            old=db.execute('SELECT job_id,epoch,grant_hash,receipt_json,receipt_sha256 FROM broker_node_receipts WHERE operation_id=?',(grant['operationId'],)).fetchone()
            expected=(grant['jobId'],grant['coordinatorEpoch'],digest,receipt,receipt_hash)
            if old is None and intent['state']=='complete':
                changed=db.execute("UPDATE broker_intents SET state='receipt_uncertain' WHERE operation_id=? AND job_id=? AND grant_hash=? AND state='complete'",(grant['operationId'],grant['jobId'],digest)).rowcount
                if changed!=1:raise EvidenceError('node receipt intent changed')
                uncertain=True
            else:
                proof=db.execute('SELECT job_id,epoch,grant_hash FROM broker_sender_evidence WHERE operation_id=?',(grant['operationId'],)).fetchone()
                if proof is None or tuple(proof)!=(grant['jobId'],grant['coordinatorEpoch'],digest):raise EvidenceError('sender evidence differs')
                if old is None:db.execute('INSERT INTO broker_node_receipts VALUES(?,?,?,?,?,?)',(grant['operationId'],*expected))
                elif tuple(old)!=expected:raise EvidenceError('node receipt changed')
                if intent['state']=='started':
                    changed=db.execute("UPDATE broker_intents SET state='complete' WHERE operation_id=? AND job_id=? AND grant_hash=? AND state='started'",(grant['operationId'],grant['jobId'],digest)).rowcount
                    if changed!=1:raise EvidenceError('node receipt intent changed')
        if uncertain:raise EvidenceError('completed upload has no durable receipt; inspection required')

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
