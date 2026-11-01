"""Real API/store transactions against independent in-memory SQLite connections."""
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import unittest
import uuid

from processing.reference_api import ReferenceAPI
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI


class MemoryQueue:
    def __init__(self, uri): self.uri = uri
    def _known_node(self, value): return value
    def slug(self, value): return value
    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.uri, uri=True, timeout=.1)
        db.row_factory = sqlite3.Row
        try:
            with db: yield db
        finally: db.close()


class ActualContentionTests(unittest.TestCase):
    def test_busy_acceptance_then_exact_replay_and_worker_relay(self):
        fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())
        receipt, manifest = fixture['acceptance'], fixture['resultManifest']
        uri = 'file:actual-acceptance-' + uuid.uuid4().hex + '?mode=memory&cache=shared'
        keeper = sqlite3.connect(uri, uri=True, timeout=.1)
        locker = sqlite3.connect(uri, uri=True, timeout=.1)
        try:
            keeper.executescript('''
              CREATE TABLE jobs(id TEXT PRIMARY KEY, profile TEXT, status TEXT, result TEXT, claim_hash TEXT, held_reason TEXT);
              CREATE TABLE reference_jobs(job_id TEXT PRIMARY KEY, generation INTEGER, environment TEXT,
                input_json TEXT, manifest_sha TEXT, pipeline_digest TEXT, phase TEXT, node_id TEXT,
                transfer_json TEXT, result_json TEXT, error TEXT, updated_at TEXT);
              CREATE TABLE reference_acceptances(receipt_id TEXT PRIMARY KEY, job_id TEXT, digest TEXT, receipt_json TEXT);
              CREATE TABLE reference_claims(request_id TEXT, job_id TEXT, node_id TEXT, worker_id TEXT, instance_id TEXT, token TEXT);
              CREATE TABLE reference_transfer_grants(job_id TEXT, grant_json TEXT, digest TEXT);
              CREATE TABLE reference_outbox(payload_json TEXT);
              CREATE TABLE reference_worker_receipts(receipt_json TEXT);
            ''')
            keeper.execute("INSERT INTO jobs(id,profile,status) VALUES ('job-1','reference-pipeline-v1','succeeded')")
            keeper.execute('''INSERT INTO reference_jobs(job_id,generation,environment,input_json,manifest_sha,pipeline_digest,
                phase,node_id,result_json) VALUES (?,?,?,?,?,?,?,?,?)''', ('job-1',1,'staging','{}',receipt['inputManifestSha256'],
                receipt['pipelineDigest'],'awaiting_acceptance','node',json.dumps(manifest)))
            keeper.execute("INSERT INTO reference_claims VALUES ('request','job-1','node','worker','instance','claim')")
            keeper.commit()
            store = object.__new__(ReferenceStore)
            store.queue = MemoryQueue(uri)
            store.max_metadata_bytes = 1024 * 1024
            public = ReferenceAPI(store, bearer_tokens={'synthetic-viewer-token':'staging'},
                capability_fingerprint='a'*64, options_fingerprint='b'*64)
            worker = ReferenceWorkerAPI(store, object(), bearer_workers={'synthetic-worker-token':
                {'node_id':'node','worker_id':'worker','worker_instance_id':'instance'}})
            headers = {'Authorization':'Bearer synthetic-viewer-token'}
            path = '/ltds/v1/jobs/job-1/acceptance'
            body = json.dumps(receipt).encode()
            locker.execute('BEGIN IMMEDIATE')
            busy = public.handle('POST', path, headers, body, protected_transport=True)
            self.assertEqual((busy.status,busy.body), (503,{'error':'acceptance_store_busy'}))
            locker.rollback()
            created = public.handle('POST', path, headers, body, protected_transport=True)
            replay = public.handle('POST', path, headers, body, protected_transport=True)
            self.assertEqual((created.status,created.body['created']), (200,True))
            self.assertEqual((replay.status,replay.body['created']), (200,False))
            self.assertEqual(keeper.execute('SELECT COUNT(*) FROM reference_acceptances').fetchone()[0],1)
            locker.execute('BEGIN IMMEDIATE')
            locker.execute("UPDATE jobs SET status=status WHERE id='job-1'")
            worker_headers = {'Authorization':'Bearer synthetic-worker-token','X-Claim-Token':'claim'}
            worker_path = '/ltds/worker/v1/jobs/job-1/acceptance'
            busy_read = worker.handle('GET', worker_path, worker_headers, protected_transport=True)
            self.assertEqual((busy_read.status,busy_read.body), (503,{'error':'acceptance_store_busy'}))
            locker.rollback()
            restored = worker.handle('GET', worker_path, worker_headers, protected_transport=True)
            self.assertEqual(restored.status,200)
            self.assertEqual(restored.body['receipts'],[receipt])
        finally:
            locker.rollback(); locker.close(); keeper.close()


if __name__ == '__main__': unittest.main()
