"""Real API/store transactions against independent in-memory SQLite connections."""
from contextlib import contextmanager
import json
import hashlib
import http.client
import os
from pathlib import Path
import sqlite3
import subprocess
import threading
import unittest
import uuid
from urllib.request import urlopen
from unittest.mock import patch

from processing import reference_protocol as p
from processing.reference_api import ReferenceAPI
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_driver import DriverWait, ReferenceDriver
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_http import create_reference_http_server
from processing.scratch_policy import ScratchLedger


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

            # Real loopback HTTP adapters and the real driver's control client;
            # one retained receipt already exists from the contention exercise.
            class API:
                def handle(inner, method, route, request_headers, request_body, *, protected_transport):
                    target = worker if route.startswith('/ltds/worker/v1/') else public
                    return target.handle(method,route,request_headers,request_body,protected_transport=protected_transport)
            server = None
            thread = None
            started = False
            ledger_db = None
            try:
                server = create_reference_http_server(API(),'127.0.0.1',0)
                # Track and join actual handler threads, not only serve_forever.
                server.daemon_threads = False
                server.block_on_close = True
                thread = threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01})
                thread.start(); started = True
                ledger_db = sqlite3.connect(':memory:')
                ledger_db.row_factory = sqlite3.Row
                driver = object.__new__(ReferenceDriver)
                driver.origin = 'http://127.0.0.1:' + str(server.server_port)
                driver._bearer = 'synthetic-worker-token'
                driver._open = urlopen
                locker.execute('BEGIN IMMEDIATE')
                locker.execute("UPDATE jobs SET status=status WHERE id='job-1'")
                with self.assertRaises(DriverWait):
                    driver._request('GET','/jobs/job-1/acceptance',claim_token='claim')
                locker.rollback()
                status, partial = driver._request('GET','/jobs/job-1/acceptance',claim_token='claim')
                self.assertEqual((status,partial['fullyAccepted']), (200,False))

                # The second object is accepted over HTTP; discard the first
                # committed response body and retry the exact same receipt.
                final = dict(receipt, receiptId='receipt-2', storageReceiptId='storage-2',
                             acceptedObjectIds=['result-2'])
                wire = json.dumps(final).encode()
                # Shrink only this isolated process's wire ceiling: the single
                # request still fits, but the full two-receipt relay cannot.
                with patch.object(p,'MAX_BODY_BYTES',len(wire)+50):
                    connection = http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
                    try:
                        connection.request('POST',path,body=wire,headers=headers)
                        bounded = connection.getresponse()
                        self.assertEqual(bounded.status,409)
                        bounded.read()
                    finally: connection.close()
                    self.assertEqual(keeper.execute('SELECT COUNT(*) FROM reference_acceptances').fetchone()[0],1)
                connection = http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
                try:
                    connection.request('POST',path,body=wire,headers=headers)
                    discarded_response = connection.getresponse()
                    self.assertEqual(discarded_response.status,200)
                finally: connection.close()
                connection = http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
                try:
                    connection.request('POST',path,body=wire,headers=headers)
                    replay_http = connection.getresponse()
                    self.assertEqual(replay_http.status,200)
                    self.assertFalse(json.loads(replay_http.read())['created'])
                finally: connection.close()
                status, relayed = driver._request('GET','/jobs/job-1/acceptance',claim_token='claim')
                self.assertEqual((status,relayed['fullyAccepted']), (200,True))
                self.assertEqual(len(relayed['receipts']),2)

                # Apply the returned receipts through the actual worker and
                # scratch-ledger methods, backed by an in-memory test ledger.
                ledger_db.executescript('''CREATE TABLE results(reservation TEXT, identity TEXT);
                  CREATE TABLE receipts(id TEXT, storage_id TEXT, payload TEXT);
                  CREATE TABLE objects(reservation TEXT,id TEXT,accepted INTEGER);''')
                identity = {'job_id':'job-1','generation':1,'manifest_sha256':receipt['inputManifestSha256'],
                    'pipeline_sha256':receipt['pipelineDigest'], 'result_manifest_sha256':manifest['resultManifestSha256'],
                    'objects':[{'object_id':obj['id']} for obj in manifest['objects']]}
                ledger_db.execute('INSERT INTO results VALUES (?,?)',('reservation',json.dumps(identity)))
                ledger_db.executemany('INSERT INTO objects VALUES (?,?,NULL)',
                    [('reservation',obj['id']) for obj in manifest['objects']])
                ledger_db.commit()
                ledger = object.__new__(ScratchLedger)
                @contextmanager
                def transaction():
                    with ledger_db: yield ledger_db
                ledger._transaction = transaction
                ledger._now = lambda _db: 1
                node = object.__new__(ReferenceEngineWorker)
                node.ledger = ledger
                node._row = lambda _: {'result':json.dumps(manifest),'outbox_id':'outbox',
                    'identity':json.dumps({'job':{'generation':1,'environment':'staging',
                      'pipelineDigest':receipt['pipelineDigest'],'manifestSha256':receipt['inputManifestSha256']},
                      'reservation':'reservation'})}
                applied = node.apply_viewer_acceptance('job-1',relayed['receipts'])
                self.assertTrue(applied['fullyAccepted'])
                self.assertEqual(len(applied['acceptedObjectIds']),2)
                self.assertTrue(node.apply_viewer_acceptance('job-1',relayed['receipts'])['fullyAccepted'])
                self.assertEqual(ledger_db.execute('SELECT COUNT(*) FROM receipts').fetchone()[0],2)

                # Optional cross-repository qualification against an explicitly
                # selected Viewer checkout; never modifies or launches Viewer.
                viewer_dir = os.environ.get('LT_VIEWER_CANDIDATE_DIR')
                if viewer_dir:
                    modules = [Path(viewer_dir) / 'nativeReferenceReceiptCandidate.js',
                               Path(viewer_dir) / 'nativeReferenceProtocolCandidate.js']
                    before = [hashlib.sha256(file.read_bytes()).hexdigest() for file in modules]
                    candidate_manifest = dict(manifest,jobId='job-2')
                    candidate_manifest['resultManifestSha256'] = p.digest({k:v for k,v in candidate_manifest.items() if k!='resultManifestSha256'})
                    script = '''const fs=require('node:fs'),{DatabaseSync}=require('node:sqlite'),crypto=require('node:crypto');
const m=require(process.argv[1]),x=JSON.parse(fs.readFileSync(0,'utf8')),db=new DatabaseSync(':memory:');
db.exec('CREATE TABLE processing_tasks(id TEXT PRIMARY KEY, active_attempt_id TEXT); CREATE TABLE processing_attempts(id TEXT PRIMARY KEY, task_id TEXT);');
const a=crypto.randomUUID(); db.prepare('INSERT INTO processing_tasks VALUES(?,?)').run('task',a);db.prepare('INSERT INTO processing_attempts VALUES(?,?)').run(a,'task');
m.install(db,true);m.bind(db,{attemptId:a,bindingId:crypto.randomUUID(),jobId:x.manifest.jobId,generation:1,environment:'staging',inputManifestSha256:x.input,pipelineDigest:x.manifest.pipelineDigest},{authorizeBinding:()=>true});
m.holdReceipt(db,{attemptId:a,storageReceiptId:crypto.randomUUID(),receiptId:crypto.randomUUID(),manifest:x.manifest,objects:x.manifest.objects.map(o=>({id:o.id,byteSize:o.byteSize,sha256:o.sha256})),acceptedAt:'2026-11-01T00:00:00.000Z'},{authorizeReceipt:()=>true,verifyDurableStorage:()=>true});
process.stdout.write(db.prepare('SELECT acceptance_json FROM native_reference_outbox_candidate WHERE attempt_id=?').get(a).acceptance_json);db.close();'''
                    process = subprocess.run(['node','-e',script,str(modules[0])],
                        input=json.dumps({'manifest':candidate_manifest,'input':receipt['inputManifestSha256']}),
                        text=True,capture_output=True,timeout=10)
                    self.assertEqual(process.returncode,0,process.stderr[-2000:])
                    self.assertEqual(before,[hashlib.sha256(file.read_bytes()).hexdigest() for file in modules])
                    viewer_receipt = json.loads(process.stdout)
                    keeper.execute("INSERT INTO jobs(id,profile,status) VALUES ('job-2','reference-pipeline-v1','succeeded')")
                    keeper.execute('''INSERT INTO reference_jobs(job_id,generation,environment,input_json,manifest_sha,pipeline_digest,
                        phase,node_id,result_json) VALUES (?,?,?,?,?,?,?,?,?)''', ('job-2',1,'staging','{}',receipt['inputManifestSha256'],
                        receipt['pipelineDigest'],'awaiting_acceptance','node',json.dumps(candidate_manifest)))
                    keeper.execute("INSERT INTO reference_claims VALUES ('request-2','job-2','node','worker','instance','claim')")
                    keeper.commit()
                    viewer_path = '/ltds/v1/jobs/job-2/acceptance'
                    for expected_created in (True,False):
                        connection = http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
                        try:
                            connection.request('POST',viewer_path,body=process.stdout.encode(),headers=headers)
                            response = connection.getresponse()
                            self.assertEqual(response.status,200)
                            accepted = json.loads(response.read())
                            self.assertEqual((accepted['created'],accepted['fullyAccepted']), (expected_created,True))
                            self.assertEqual(set(accepted['acceptedObjectIds']),set(viewer_receipt['acceptedObjectIds']))
                        finally: connection.close()
                    _, viewer_relay = driver._request('GET','/jobs/job-2/acceptance',claim_token='claim')
                    self.assertEqual(viewer_relay['receipts'],[viewer_receipt])
                    identity2 = dict(identity,job_id='job-2',result_manifest_sha256=candidate_manifest['resultManifestSha256'])
                    ledger_db.execute('INSERT INTO results VALUES (?,?)',('reservation-2',json.dumps(identity2)))
                    ledger_db.executemany('INSERT INTO objects VALUES (?,?,NULL)',
                        [('reservation-2',obj['id']) for obj in candidate_manifest['objects']])
                    ledger_db.commit()
                    node._row = lambda _: {'result':json.dumps(candidate_manifest),'outbox_id':'outbox-2',
                        'identity':json.dumps({'job':{'generation':1,'environment':'staging',
                          'pipelineDigest':receipt['pipelineDigest'],'manifestSha256':receipt['inputManifestSha256']},
                          'reservation':'reservation-2'})}
                    self.assertTrue(node.apply_viewer_acceptance('job-2',viewer_relay['receipts'])['fullyAccepted'])
            finally:
                locker.rollback()
                if ledger_db is not None: ledger_db.close()
                if server is not None:
                    if started: server.shutdown()
                    handlers = list(server._threads) if isinstance(server._threads, list) else []
                    server.server_close()
                    if started:
                        thread.join(timeout=3)
                        self.assertFalse(thread.is_alive())
                    self.assertTrue(all(not handler.is_alive() for handler in handlers))
        finally:
            locker.rollback(); locker.close(); keeper.close()


if __name__ == '__main__': unittest.main()
