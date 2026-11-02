"""Explicit, synthetic-only Viewer client against real Processing loopback HTTP.

This test retains its owned temporary root and never touches a live service.
The Viewer source must be a specified clean commit; the fetch bridge is local
plaintext, not production TLS or physical custody. Run only after lifecycle
review and Viewer-owner coordination.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import threading
import unittest
import uuid
from unittest.mock import patch

from processing import reference_protocol as p
from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_http import create_reference_http_server
from processing.reference_store import ReferenceStore
from tests.paired_owned_windows import run_owned

ROOT=Path(__file__).resolve().parents[1]
PROCESSING_BASE='3e4c5eacfd4df22169b9b08ae5e4b90c9e7da57a'
VIEWER_COMMIT='7c526b49d78423372c29eac98b95a16a270c2bd3'
NODE_SHA='b3094d0b49f9ad602262a9921551737bb97637c05dd357a06ae98188d7290aa3'
GOLDEN_SHA='5c81c6973a1447efa64cd27dbac68393cea0395111fa087c5bd3c451b18dca41'
TOKEN='synthetic-viewer-token-only-00001'
VIEWER_PATHS=('apps/viewer/server', 'apps/viewer/test/fixtures/reference-v1-acceptance-sender.golden.json')

def git(cwd,*args):
    return subprocess.run(['git',*args],cwd=cwd,check=True,stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,timeout=10).stdout

class PairedV1ViewerHTTPTests(unittest.TestCase):
    def test_actual_viewer_client_against_processing_loopback(self):
        if os.name!='nt':self.skipTest('owned Windows Job Object watchdog required')
        viewer=os.environ.get('LT_VIEWER_PAIRED_DIR');commit=os.environ.get('LT_VIEWER_PAIRED_COMMIT')
        if not viewer or not commit:self.skipTest('explicit Viewer source and commit required')
        self.assertEqual(commit,VIEWER_COMMIT)
        viewer=Path(viewer).resolve(strict=True)
        self.assertEqual(git(viewer,'rev-parse','HEAD').decode().strip(),commit)
        self.assertEqual(git(viewer,'status','--porcelain','--',*VIEWER_PATHS),b'')
        git(ROOT,'diff','--quiet',PROCESSING_BASE,'--','processing')
        self.assertEqual(git(ROOT,'status','--porcelain','--','processing'),b'')
        node=Path(shutil.which('node') or '').resolve(strict=True)
        self.assertEqual(hashlib.sha256(node.read_bytes()).hexdigest(),NODE_SHA)
        golden=ROOT/'contracts/reference-v1-acceptance-sender.golden.json'
        self.assertEqual(hashlib.sha256(golden.read_bytes()).hexdigest(),GOLDEN_SHA)
        self.assertEqual(hashlib.sha256((viewer/VIEWER_PATHS[-1]).read_bytes()).hexdigest(),GOLDEN_SHA)
        fixture=json.loads(golden.read_text(encoding='utf8'))
        base=json.loads((ROOT/'contracts/reference-v1.contract.json').read_text(encoding='utf8'))
        state=Path(tempfile.mkdtemp(prefix='lt-paired-v1-http-')).resolve()
        print('RETAINED_SYNTHETIC_PAIRED_ROOT',state,flush=True)
        queue=Queue(state/'coordinator',reference_enabled=True);server=None;thread=None;started=False
        try:
            store=ReferenceStore(queue)
            raw=base['submission'];valid=p.validate_submission(raw,environment='staging',
                capability_fingerprint=raw['capabilityFingerprint'],
                options_fingerprint=raw['pipeline']['odm']['optionsFingerprint'],
                allowed_odm_options=set(raw['pipeline']['odm']['options']))
            with patch('processing.reference_store.uuid.uuid4',return_value=uuid.UUID(fixture['expected']['jobId'])):
                job,_=store.submit(valid)
            self.assertEqual(job['manifestSha256'],fixture['expected']['inputManifestSha256'])
            self.assertEqual(job['pipelineDigest'],fixture['expected']['pipelineDigest'])
            identity=dict(node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-1')
            claim=store.claim(job['jobId'],**identity)
            store.reserve(job['jobId'],base['transferRequest'],node_id='cluster')
            store.update_phase(job['jobId'],'fetching');store.update_phase(job['jobId'],'processing')
            store.record_worker_result(job['jobId'],fixture['resultManifest'],claim_token=claim['claimToken'],**identity)
            queue.complete(job['jobId'],{'claimToken':claim['claimToken'],'status':'succeeded','result':{}})
            store.update_phase(job['jobId'],'awaiting_acceptance')
            api=ReferenceAPI(store,bearer_tokens={TOKEN:'staging'},
                capability_fingerprint=raw['capabilityFingerprint'],
                options_fingerprint=raw['pipeline']['odm']['optionsFingerprint'])
            server=create_reference_http_server(api,request_timeout=2)
            thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.02})
            thread.start();started=True
            code,output=run_owned([str(node),str(ROOT/'tests/paired_v1_viewer_client.cjs'),
                str(viewer/'apps/viewer/server/referenceProvider.js'),str(golden),str(server.server_address[1])],
                timeout=20)
            self.assertEqual(code,0,output.decode('utf8','replace'))
            self.assertEqual(json.loads(output),{'replayed':True,'requestBodiesIdentical':True})
            self.assertEqual(hashlib.sha256(node.read_bytes()).hexdigest(),NODE_SHA)
            self.assertEqual(git(viewer,'rev-parse','HEAD').decode().strip(),commit)
            self.assertEqual(git(viewer,'status','--porcelain','--',*VIEWER_PATHS),b'')
            with queue._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_acceptances').fetchone()[0],1)
            self.assertEqual(store.get(job['jobId'],private=True)['phase'],'completed')
        finally:
            try:
                if server is not None:
                    try:
                        if started and thread.is_alive():server.shutdown()
                    finally:server.server_close()
                    if started:
                        thread.join(timeout=5)
                        if thread.is_alive():raise RuntimeError('owned listener did not stop')
            finally:queue.close()

if __name__=='__main__':unittest.main()
