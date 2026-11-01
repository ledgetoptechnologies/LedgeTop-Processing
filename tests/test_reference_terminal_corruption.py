"""Terminal acceptance corruption is a stored-state hold on both APIs."""
import json
import sqlite3
import unittest
import uuid

from processing.reference_api import ReferenceAPI
from processing.reference_failure import make_failure
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI
from tests.test_reference_acceptance_actual_contention import MemoryQueue


class TerminalCorruptionTests(unittest.TestCase):
    def setUp(self):
        uri='file:terminal-'+uuid.uuid4().hex+'?mode=memory&cache=shared'
        self.db=sqlite3.connect(uri,uri=True,timeout=.1)
        self.db.executescript('''
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
        self.store=object.__new__(ReferenceStore);self.store.queue=MemoryQueue(uri);self.store.max_metadata_bytes=1024*1024
        self.public=ReferenceAPI(self.store,bearer_tokens={'synthetic-viewer-token':'staging'},capability_fingerprint='a'*64,options_fingerprint='b'*64)
        self.worker=ReferenceWorkerAPI(self.store,object(),bearer_workers={'synthetic-worker-token':
            {'node_id':'node','worker_id':'worker','worker_instance_id':'instance'}})
        failure=make_failure(job_id='job-3',generation=1,environment='staging',input_manifest_sha256='a'*64,
            pipeline_digest='b'*64,terminal_state='failed',error_code='engine_failed')
        outcome={key:failure[key] for key in ('failureSha256','terminalState','errorCode')}
        self.db.execute('INSERT INTO jobs(id,profile,status,result) VALUES (?,?,?,?)',
            ('job-3','reference-pipeline-v1','failed',json.dumps(outcome)))
        self.db.execute('''INSERT INTO reference_jobs(job_id,generation,environment,input_json,manifest_sha,pipeline_digest,
            phase,node_id) VALUES (?,?,?,?,?,?,?,?)''',('job-3',1,'staging','{}','a'*64,'b'*64,'awaiting_acceptance','node'))
        self.db.execute('INSERT INTO reference_claims VALUES (?,?,?,?,?,?)',('request','job-3','node','worker','instance','claim'))
        self.db.execute('INSERT INTO reference_worker_failures VALUES (?,?,?,?,?,?,?)',
            ('job-3',1,failure['failureSha256'],'c'*64,json.dumps(failure),'{}','awaiting_viewer_failure_acceptance'))
        self.db.commit()
        self.receipt={'schemaVersion':1,'receiptId':'terminal-receipt','storageReceiptId':'terminal-storage',
            'jobId':'job-3','generation':1,'environment':'staging','inputManifestSha256':'a'*64,'pipelineDigest':'b'*64,
            'failureSha256':failure['failureSha256'],'acceptedAt':'2026-11-01T00:00:00.000Z','validationVersion':'ltds-reference-v1'}
        self.public_headers={'Authorization':'Bearer synthetic-viewer-token'}
        self.worker_headers={'Authorization':'Bearer synthetic-worker-token','X-Claim-Token':'claim'}
        self.path='/ltds/v1/jobs/job-3/terminal-acceptance'
        self.worker_path='/ltds/worker/v1/jobs/job-3/terminal-acceptance'

    def tearDown(self): self.db.close()

    def post(self):
        return self.public.handle('POST',self.path,self.public_headers,json.dumps(self.receipt).encode(),protected_transport=True)

    def get(self):
        return self.worker.handle('GET',self.worker_path,self.worker_headers,protected_transport=True)

    def test_tampered_terminal_receipt_holds_public_replay_and_worker_relay(self):
        self.assertTrue(self.post().body['created'])
        self.assertFalse(self.post().body['created'])
        self.assertTrue(self.get().body['terminalAccepted'])
        self.db.execute("UPDATE reference_terminal_acceptances SET receipt_json='{'");self.db.commit()
        before=self.db.execute('SELECT * FROM reference_terminal_acceptances').fetchall()
        for result in (self.post(),self.get()):
            self.assertEqual((result.status,result.body),(409,{'error':'stored_reference_inspection'}))
        self.assertEqual(self.db.execute('SELECT * FROM reference_terminal_acceptances').fetchall(),before)

    def test_foreign_bad_success_row_holds_terminal_acceptance_globally(self):
        self.db.execute('INSERT INTO reference_acceptances VALUES (?,?,?,?)',('foreign','other-job','0'*64,'{'))
        self.db.commit();before=self.db.execute('SELECT * FROM reference_acceptances').fetchall()
        result=self.post()
        self.assertEqual((result.status,result.body),(409,{'error':'stored_reference_inspection'}))
        self.assertEqual(self.db.execute('SELECT * FROM reference_acceptances').fetchall(),before)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM reference_terminal_acceptances').fetchone()[0],0)

    def test_bad_stored_failure_holds_public_and_private_without_mutation(self):
        self.db.execute("UPDATE reference_worker_failures SET failure_json='{'");self.db.commit()
        before=self.db.execute('SELECT * FROM reference_worker_failures').fetchall()
        for result in (self.post(),self.get()):
            self.assertEqual((result.status,result.body),(409,{'error':'stored_reference_inspection'}))
        self.assertEqual(self.db.execute('SELECT * FROM reference_worker_failures').fetchall(),before)


if __name__ == '__main__':unittest.main()
