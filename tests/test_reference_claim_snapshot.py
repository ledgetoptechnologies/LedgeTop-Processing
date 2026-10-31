"""Read-only worker observations against the actual queue and private API."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.request import Request, build_opener, ProxyHandler

from processing.prototype import Queue
from processing.reference_delivery import DeliveryReceiver
from processing.reference_http import create_reference_http_server
from processing.reference_protocol import validate_submission
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI, PREFIX

WORKER='snapshot-synthetic-worker-0001'
OTHER='snapshot-synthetic-worker-0002'
INSTANCE='snapshot-synthetic-worker-0003'
MAIN='snapshot-synthetic-worker-0004'


class ClaimSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.directory=Path(self.tmp.name)
        self.fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        self.identities={WORKER:dict(node_id='cluster',worker_id='worker',worker_instance_id='instance-one'),
            OTHER:dict(node_id='cluster',worker_id='other',worker_instance_id='instance-two'),
            INSTANCE:dict(node_id='cluster',worker_id='worker',worker_instance_id='instance-two'),
            MAIN:dict(node_id='main',worker_id='worker',worker_instance_id='instance-one')}
        self.open()
        raw=self.fixture['submission']
        self.job=self.store.submit(validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint']))[0]
        self.claim=self.store.claim(self.job['jobId'],**self.identities[WORKER],claim_request_id='original-claim')
        self.route=PREFIX+'/jobs/'+self.job['jobId']+'/claim-state'

    def open(self):
        self.queue=Queue(self.directory,reference_enabled=True,node_ids=('cluster','main'));self.addCleanup(self.queue.close)
        self.store=ReferenceStore(self.queue)
        self.api=ReferenceWorkerAPI(self.store,DeliveryReceiver(self.store),bearer_workers=self.identities)

    def request(self,*,worker=WORKER,claim_token=None,protected=True,body=b'',method='GET',extra=None):
        headers={'Authorization':'Bearer '+worker,'X-Claim-Token':self.claim['claimToken'] if claim_token is None else claim_token}
        headers.update(extra or {})
        return self.api.handle(method,self.route,headers,body,protected_transport=protected)

    def processing(self):
        self.store.reserve(self.job['jobId'],self.fixture['transferRequest'],node_id='cluster')
        self.store.update_phase(self.job['jobId'],'fetching')
        self.store.update_phase(self.job['jobId'],'processing')

    def test_exact_observation_contains_no_plaintext_claim_or_submission(self):
        response=self.request();self.assertEqual(response.status,200)
        self.assertEqual(response.body,dict(schemaVersion=1,protocol='ltds-parent-claim-v1',
            authorityId=self.queue.authority_id,jobId=self.job['jobId'],generation=1,environment='staging',
            nodeId='cluster',workerId='worker',workerInstanceId='instance-one',claimRequestId='original-claim',
            claimTokenSha256=hashlib.sha256(self.claim['claimToken'].encode()).hexdigest(),
            manifestSha256=self.claim['manifestSha256'],pipelineDigest=self.claim['pipelineDigest'],
            status='running',executionPhase='queued',heldReason=None))
        self.assertNotIn(self.claim['claimToken'],json.dumps(response.body))
        self.assertNotIn('submission',response.body)

    def test_repeated_reads_never_replay_claim_refresh_nodes_or_write_sqlite(self):
        self.processing();before=Path(self.queue.path).read_bytes();queries=[]
        original=self.queue._db
        @contextmanager
        def observed():
            with original() as db:
                db.set_trace_callback(queries.append)
                yield db
        with (patch.object(self.queue,'_db',observed),
                patch.object(self.store,'claim',side_effect=AssertionError('no claim replay')),
                patch.object(self.queue,'_refresh_nodes',side_effect=AssertionError('no maintenance mutation'))):
            first=self.request();second=self.request()
        self.assertEqual((first.status,second.status),(200,200));self.assertEqual(first.body,second.body)
        self.assertEqual(Path(self.queue.path).read_bytes(),before)
        self.assertEqual(first.body['executionPhase'],'processing')
        self.assertEqual(sum(q=='BEGIN' for q in queries),2)
        self.assertFalse(any(q.lstrip().split(' ',1)[0].upper() in ('UPDATE','INSERT','DELETE','CREATE','ALTER') for q in queries))

    def test_private_transport_and_both_credential_layers_are_required(self):
        self.assertEqual(self.request(protected=False).status,503)
        for worker in ('snapshot-synthetic-viewer-0005',OTHER,INSTANCE,MAIN):
            with self.subTest(worker=worker):self.assertEqual(self.request(worker=worker).status,401 if worker.startswith('snapshot-synthetic-viewer') else 409)
        for token in ('', 'wrong-token', 'x'*129, ['not-a-header']):
            with self.subTest(token=token):self.assertIn(self.request(claim_token=token).status,(401,409))
        self.assertEqual(self.request(extra={'x-claim-token':self.claim['claimToken']}).status,401)

    def test_observation_route_has_no_body_mutation_or_query_controls(self):
        self.assertEqual(self.request(body=b'{}').status,400)
        self.assertEqual(self.request(method='POST',body=b'{}').status,404)
        original=self.route;self.route+='?environment=production'
        self.assertEqual(self.request().status,404);self.route=original
        self.assertEqual(self.request().body['environment'],'staging')

    def test_consistency_drift_in_claim_queue_and_submission_is_refused(self):
        changes=[('jobs','node_id','main'),('jobs','worker_id','other'),('jobs','claim_hash','0'*64),
            ('jobs','environment','production'),('reference_claims','node_id','main'),
            ('reference_claims','instance_id','instance-two'),('reference_claims','token','wrong-token'),
            ('reference_claims','token','nonascii-\u00e9'),('reference_jobs','manifest_sha','0'*64),
            ('reference_jobs','pipeline_digest','0'*64),('reference_jobs','generation',1.5)]
        for table,column,value in changes:
            with self.subTest(table=table,column=column):
                key='id' if table=='jobs' else 'job_id'
                with self.queue._db() as db:
                    old=db.execute('SELECT '+column+' FROM '+table+' WHERE '+key+'=?',(self.job['jobId'],)).fetchone()[0]
                    db.execute('UPDATE '+table+' SET '+column+'=? WHERE '+key+'=?',(value,self.job['jobId']))
                try:self.assertEqual(self.request().status,409)
                finally:
                    with self.queue._db() as db:db.execute('UPDATE '+table+' SET '+column+'=? WHERE '+key+'=?',(old,self.job['jobId']))
        self.assertEqual(self.request().status,200)

    def test_real_coordinator_restart_retains_phase_and_claim_without_reexecution(self):
        self.processing();authority=self.queue.authority_id
        self.queue.close();self.open()
        before=Path(self.queue.path).read_bytes();response=self.request()
        self.assertEqual(response.status,200)
        self.assertEqual((response.body['status'],response.body['executionPhase'],response.body['heldReason']),
            ('held','processing','coordinator_restart'))
        self.assertEqual(response.body['authorityId'],authority)
        self.assertEqual(response.body['claimRequestId'],'original-claim')
        self.assertEqual(Path(self.queue.path).read_bytes(),before)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'],'held')

    def test_operator_hold_and_terminal_resolution_are_observations_not_new_authorization(self):
        self.processing();self.queue.operator(self.job['jobId'],'hold',{'note':'synthetic operator hold'})
        self.assertEqual((self.request().body['status'],self.request().body['heldReason']),('held','operator'))
        self.queue.operator(self.job['jobId'],'resolve',{'note':'synthetic stopped engine','workerStopped':True})
        observed=self.request()
        self.assertEqual(observed.status,200);self.assertEqual(observed.body['status'],'failed')
        self.assertIsNone(observed.body['heldReason'])

    def test_actual_loopback_http_returns_bound_envelope_without_claim_replay(self):
        server=create_reference_http_server(self.api);thread=threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            request=Request('http://127.0.0.1:'+str(server.server_port)+self.route,
                headers={'Authorization':'Bearer '+WORKER,'X-Claim-Token':self.claim['claimToken']})
            with build_opener(ProxyHandler({})).open(request,timeout=3) as response:
                body=json.loads(response.read())
            self.assertEqual(body,self.request().body)
        finally:
            server.shutdown();thread.join(5);server.server_close()
        self.assertFalse(thread.is_alive())
