"""Synthetic offline migration and fail-closed upload intent census."""
import hashlib
import io
import sqlite3
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from processing import node_mutation_broker as broker
from processing.authority_lease import AuthorityLease, record_required
from processing.broker_intent_fence import install_offline as install_intents
from processing.durable_upload_evidence import install_offline as install_evidence
from processing.prototype import Queue, VIEWER_PROFILE, PROFILE
from processing import upload_intent_census as census
from processing.upload_intent_census import CensusError, install_offline
from processing.viewer_tasks import TaskStore

class UploadCensusTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='lt-upload-census-')).resolve()
        self.addCleanup(lambda:print('retained synthetic upload census root:',self.root,flush=True))
        self.queue=Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True)
        self.addCleanup(self.queue.close)
        self.store=TaskStore(self.queue,self.root/'queue'/'viewer-tasks')
        with self.queue._db() as db:install_intents(db,enabled=True)
        with self.queue._db() as db:install_evidence(db,enabled=True)
        with patch.object(broker,'QUALIFIED',True):broker.install_upload_issuer_schema(self.queue)
    def install(self):
        with self.queue._db() as db:install_offline(db,enabled=True)
    def queued(self):
        return self.queue.submit({'requestId':str(uuid.uuid4()),'environment':'production','profile':PROFILE})[0]
    def upload_grant(self):
        return dict(operation='upload',operationId=str(uuid.uuid4()),jobId=str(uuid.uuid4()),
                    coordinatorEpoch=self.queue.startup_id,authorityId=self.queue.authority_id,
                    nodeId='main',engineId='test-engine',engineTaskId=str(uuid.uuid4()),environment='production',
                    gateGeneration=1,taskGeneration=1,bodySha256=hashlib.sha256(b'x').hexdigest(),
                    contentType='application/octet-stream',fileName='test.jpg',fileBytes=1)
    def row(self,kind=None):
        operation=str(uuid.uuid4());job=str(uuid.uuid4());digest='a'*64
        with self.queue._db() as db:
            db.execute("INSERT INTO broker_intents VALUES(?,?,?,'complete')",(operation,job,digest))
            if kind is not None:db.execute('INSERT INTO upload_intent_kinds VALUES(?,?,?,?)',(operation,job,digest,kind))
        return operation
    def test_offline_migration_holds_unclassified_legacy_history(self):
        self.queued();operation=self.row();self.queue.close()
        raw=sqlite3.connect(self.root/'queue'/'jobs.sqlite3');raw.row_factory=sqlite3.Row
        try:install_offline(raw,enabled=True)
        finally:raw.close()
        reopened=Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True)
        try:
            with reopened._db() as db:
                self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(operation,)).fetchone()[0],'receipt_uncertain')
                self.assertEqual(db.execute('SELECT kind FROM upload_intent_kinds WHERE operation_id=?',(operation,)).fetchone()[0],'legacy_unknown')
            self.assertIsNone(reopened.claim({'workerId':'synthetic'}))
        finally:reopened.close()
    def test_offline_migration_refuses_running_job_without_creating_marker(self):
        self.queued();self.assertIsNotNone(self.queue.claim({'workerId':'synthetic'}))
        with self.assertRaises(CensusError):self.install()
        with self.queue._db() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='upload_census_required'").fetchone())

    def test_oversized_receipt_is_held_without_parsing_unbounded_text(self):
        self.install();operation=self.row('upload_remote');self.queued()
        with self.queue._db() as db:
            row=db.execute('SELECT job_id,grant_hash FROM broker_intents WHERE operation_id=?',(operation,)).fetchone()
            db.execute('INSERT INTO broker_sender_evidence VALUES(?,?,?,?)',(operation,row['job_id'],self.queue.startup_id,row['grant_hash']))
            db.execute('INSERT INTO broker_node_receipts VALUES(?,?,?,?,?,?)',(operation,row['job_id'],self.queue.startup_id,row['grant_hash'],'x'*10001,'b'*64))
        self.assertIsNone(self.queue.claim({'workerId':'synthetic'}));self.queue.close()
        reopened=Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True,require_upload_census=True)
        try:
            with reopened._db() as db:self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(operation,)).fetchone()[0],'receipt_uncertain')
        finally:reopened.close()

    def test_mismatched_remote_receipt_blocks_admission_and_is_held_at_restart(self):
        self.install();operation=self.row('upload_remote');self.queued()
        with self.queue._db() as db:
            row=db.execute('SELECT job_id,grant_hash FROM broker_intents WHERE operation_id=?',(operation,)).fetchone()
            db.execute('INSERT INTO broker_sender_evidence VALUES(?,?,?,?)',(operation,row['job_id'],self.queue.startup_id,row['grant_hash']))
            db.execute('INSERT INTO broker_node_receipts VALUES(?,?,?,?,?,?)',(operation,row['job_id'],self.queue.startup_id,row['grant_hash'],'{}','b'*64))
        self.assertIsNone(self.queue.claim({'workerId':'synthetic'}));self.queue.close()
        reopened=Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True)
        try:
            with reopened._db() as db:self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(operation,)).fetchone()[0],'receipt_uncertain')
        finally:reopened.close()

    def test_known_init_and_local_completed_do_not_require_remote_receipt(self):
        self.install();self.row('init');self.row('upload_local');self.queued()
        self.assertIsNotNone(self.queue.claim({'workerId':'synthetic'}))
    def test_exact_remote_receipt_is_not_held(self):
        self.install();self.queued()
        grant=self.upload_grant()
        digest=hashlib.sha256(broker._json(grant)).hexdigest()
        receipt,receipt_hash=broker._upload_receipt(grant,{'success':True},digest)
        with self.queue._db() as db:
            db.execute("INSERT INTO broker_intents VALUES(?,?,?,'complete')",(grant['operationId'],grant['jobId'],digest))
            db.execute('INSERT INTO upload_intent_kinds VALUES(?,?,?,?)',(grant['operationId'],grant['jobId'],digest,'upload_remote'))
            db.execute('INSERT INTO broker_sender_evidence VALUES(?,?,?,?)',(grant['operationId'],grant['jobId'],grant['coordinatorEpoch'],digest))
            db.execute('INSERT INTO broker_node_receipts VALUES(?,?,?,?,?,?)',(grant['operationId'],grant['jobId'],grant['coordinatorEpoch'],digest,receipt,receipt_hash))
        self.assertIsNotNone(self.queue.claim({'workerId':'synthetic'}))

    def test_migration_preserves_exact_legacy_remote_receipt(self):
        grant=self.upload_grant()
        digest=hashlib.sha256(broker._json(grant)).hexdigest()
        receipt,receipt_hash=broker._upload_receipt(grant,{'success':True},digest)
        with self.queue._db() as db:
            db.execute("INSERT INTO broker_intents VALUES(?,?,?,'complete')",(grant['operationId'],grant['jobId'],digest))
            db.execute('INSERT INTO broker_sender_evidence VALUES(?,?,?,?)',(grant['operationId'],grant['jobId'],grant['coordinatorEpoch'],digest))
            db.execute('INSERT INTO broker_node_receipts VALUES(?,?,?,?,?,?)',(grant['operationId'],grant['jobId'],grant['coordinatorEpoch'],digest,receipt,receipt_hash))
        self.queue.close();raw=sqlite3.connect(self.root/'queue'/'jobs.sqlite3');raw.row_factory=sqlite3.Row
        try:install_offline(raw,enabled=True)
        finally:raw.close()
        reopened=Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True)
        try:
            with reopened._db() as db:
                self.assertEqual(db.execute('SELECT kind FROM upload_intent_kinds WHERE operation_id=?',(grant['operationId'],)).fetchone()[0],'upload_remote')
                self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()[0],'complete')
        finally:reopened.close()

    def test_missing_remote_receipt_blocks_admission_then_startup_holds(self):
        self.install();operation=self.row('upload_remote');self.queued()
        self.assertIsNone(self.queue.claim({'workerId':'synthetic'}));self.queue.close()
        reopened=Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True)
        try:
            with reopened._db() as db:self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(operation,)).fetchone()[0],'receipt_uncertain')
            self.assertIsNone(reopened.claim({'workerId':'synthetic'}))
        finally:reopened.close()
    def test_missing_classification_blocks_and_startup_holds(self):
        self.install();operation=self.row();self.queued()
        self.assertIsNone(self.queue.claim({'workerId':'synthetic'}));self.queue.close()
        reopened=Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True)
        try:
            with reopened._db() as db:self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(operation,)).fetchone()[0],'receipt_uncertain')
        finally:reopened.close()
    def test_unqualified_legacy_default_remains_compatible(self):
        self.queued();self.assertIsNotNone(self.queue.claim({'workerId':'synthetic'}))

    def test_capacity_prevents_new_intent_classification(self):
        self.install();self.row('init');grant=self.upload_grant();digest=hashlib.sha256(broker._json(grant)).hexdigest()
        with patch.object(census,'MAX_CENSUS_ROWS',1):
            with self.assertRaises(CensusError):
                with self.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE');census.register(db,grant,digest,remote=True)
        with self.queue._db() as db:
            self.assertIsNone(db.execute('SELECT 1 FROM upload_intent_kinds WHERE operation_id=?',(grant['operationId'],)).fetchone())

    def test_required_mode_refuses_fresh_unmigrated_database(self):
        root=self.root/'unmigrated'
        for _ in range(2):
            with self.assertRaises(CensusError):Queue(root,require_upload_census=True)

    def test_required_mode_refuses_both_census_tables_removed(self):
        self.install();self.queue.close()
        required=Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True,require_upload_census=True)
        try:
            with required._db() as db:
                db.execute('DROP TABLE upload_intent_kinds');db.execute('DROP TABLE upload_census_required')
            job,_=required.submit({'requestId':'required','environment':'production','profile':PROFILE})
            self.assertIsNone(required.claim({'workerId':'synthetic'}))
        finally:required.close()
        with self.assertRaises(CensusError):Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True,require_upload_census=True)

    def test_offline_migration_capacity_refuses_without_partial_schema(self):
        for _ in range(6):self.row()
        self.queue.close();raw=sqlite3.connect(self.root/'queue'/'jobs.sqlite3');raw.row_factory=sqlite3.Row
        try:
            with patch.object(census,'MAX_CENSUS_ROWS',5):
                with self.assertRaises(CensusError):install_offline(raw,enabled=True)
            self.assertIsNone(raw.execute("SELECT 1 FROM sqlite_master WHERE name='upload_census_required'").fetchone())
            self.assertEqual(raw.execute("SELECT COUNT(*) FROM broker_intents WHERE state='complete'").fetchone()[0],6)
        finally:raw.close()

    def test_capacity_limit_fails_closed_before_admission_and_startup(self):
        self.install();self.queued()
        for _ in range(6):self.row('init')
        with patch.object(census,'MAX_CENSUS_ROWS',5):
            self.assertIsNone(self.queue.claim({'workerId':'synthetic'}));self.queue.close()
            with self.assertRaises(CensusError):Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True,require_upload_census=True)

    def test_realistic_ten_thousand_remote_receipts_census(self):
        self.install();self.queued();intents=[];kinds=[];proofs=[];receipts=[]
        for _ in range(10000):
            grant=self.upload_grant();digest=hashlib.sha256(broker._json(grant)).hexdigest()
            receipt,receipt_hash=broker._upload_receipt(grant,{'success':True},digest)
            intents.append((grant['operationId'],grant['jobId'],digest,'complete'))
            kinds.append((grant['operationId'],grant['jobId'],digest,'upload_remote'))
            proofs.append((grant['operationId'],grant['jobId'],grant['coordinatorEpoch'],digest))
            receipts.append((grant['operationId'],grant['jobId'],grant['coordinatorEpoch'],digest,receipt,receipt_hash))
        with self.queue._db() as db:
            db.executemany('INSERT INTO broker_intents VALUES(?,?,?,?)',intents)
            db.executemany('INSERT INTO upload_intent_kinds VALUES(?,?,?,?)',kinds)
            db.executemany('INSERT INTO broker_sender_evidence VALUES(?,?,?,?)',proofs)
            db.executemany('INSERT INTO broker_node_receipts VALUES(?,?,?,?,?,?)',receipts)
        start=time.monotonic()
        with self.queue._db() as db:self.assertTrue(self.queue._transfer_admission_clear(db))
        print('synthetic 10000-remote-receipt census seconds:',round(time.monotonic()-start,3),flush=True)

    def test_missing_trigger_refuses_startup_and_releases_owned_lock(self):
        self.install()
        with self.queue._db() as db:db.execute('DROP TRIGGER upload_intent_kind_update')
        self.queue.close()
        for _ in range(2):
            with self.assertRaises(CensusError):Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True)
    def test_real_witness_registers_init_and_remote_upload_kinds(self):
        self.install();lease_dir=self.root/'lease';lease_dir.mkdir(mode=0o700)
        lease=AuthorityLease(lease_dir,self.queue.authority_id);self.queue.authority_lease=lease;self.addCleanup(lease.close)
        record_required(self.queue,lease)
        task=str(uuid.uuid4());self.store.initialize(task,'Synthetic',[],None,'production')
        self.store.upload(task,'test.jpg',io.BytesIO(b'x'),1);self.store.commit(task)
        claim=self.queue.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]})
        self.store.update(task,stage='running',phase='init_pending',node_id='main',engine_uuid=task,claim_token=claim['claimToken'])
        secret=b'synthetic-secret-'*4;witness=broker.LocalCoordinatorAuthority(self.queue)
        with patch.object(broker,'QUALIFIED',True):
            init,_=broker.issue_init_grant(self.queue,job_id=claim['id'],claim_token=claim['claimToken'],engine_id='test-engine',gate_generation=1,operation_id=str(uuid.uuid4()),body=b'x',content_type='application/octet-stream',secret=secret)
            with witness.hold(init,3,create=True):witness.completed(init)
            self.store.update(task,phase='upload_pending')
            upload,_=broker.issue_upload_grant(self.queue,store=self.store,job_id=claim['id'],claim_token=claim['claimToken'],engine_id='test-engine',gate_generation=1,operation_id=str(uuid.uuid4()),file_key='test.jpg',secret=secret)
            with witness.hold(upload,3,create=True,require_upload_receipt=True):pass
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT kind FROM upload_intent_kinds WHERE operation_id=?',(init['operationId'],)).fetchone()[0],'init')
            self.assertEqual(db.execute('SELECT kind FROM upload_intent_kinds WHERE operation_id=?',(upload['operationId'],)).fetchone()[0],'upload_remote')

if __name__=='__main__':unittest.main()
