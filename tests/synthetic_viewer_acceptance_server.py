"""Synthetic native acceptance API server for a private network namespace.

Executed only under the separately reviewed container harness. No production data.
"""
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import ssl
import threading
import uuid

from processing.reference_api import ReferenceAPI
from processing.reference_http import create_reference_http_server
from processing.reference_store import ReferenceStore


class Queue:
    def __init__(self, uri): self.uri = uri
    def _known_node(self, node): return node
    def slug(self, value): return value
    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.uri, uri=True, timeout=2)
        db.row_factory = sqlite3.Row
        try:
            with db: yield db
        finally: db.close()


def main():
    fixture = json.loads(Path('/processing/contracts/reference-v1.contract.json').read_text())
    receipt, manifest = fixture['acceptance'], fixture['resultManifest']
    uri = 'file:acceptance-' + uuid.uuid4().hex + '?mode=memory&cache=shared'
    keeper = sqlite3.connect(uri, uri=True, timeout=2)
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
    keeper.commit()
    store = object.__new__(ReferenceStore); store.queue = Queue(uri); store.max_metadata_bytes = 1024*1024
    api = ReferenceAPI(store, bearer_tokens={'synthetic-viewer-token':'staging'},
                       capability_fingerprint='a'*64, options_fingerprint='b'*64)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('/viewer/apps/viewer/test/fixtures/readiness-tls/synthetic-only.crt',
                            '/viewer/apps/viewer/test/fixtures/readiness-tls/synthetic-only.key')
    class CountedAPI:
        def __init__(self): self.count = 0; self.lock = threading.Lock()
        def handle(self, method, route, headers, body, **kwargs):
            result = api.handle(method, route, headers, body, **kwargs)
            with self.lock:
                print('ACCEPTANCE_RESULT',result.status,result.body.get('created'),flush=True)
                self.count += 1
                if self.count == 2:
                    threading.Thread(target=server.shutdown).start()
            return result
    server = create_reference_http_server(CountedAPI(), host='127.0.0.1', port=17864, ssl_context=context, request_timeout=5)
    print('READY synthetic loopback 17864', flush=True)
    timer = threading.Timer(35, server.shutdown); timer.start()
    try: server.serve_forever(poll_interval=.05)
    finally:
        timer.cancel();timer.join();server.server_close()
        rows=keeper.execute('SELECT COUNT(*) FROM reference_acceptances').fetchone()[0]
        print('RETAINED_RECEIPTS',rows,flush=True);keeper.close()

if __name__ == '__main__': main()
