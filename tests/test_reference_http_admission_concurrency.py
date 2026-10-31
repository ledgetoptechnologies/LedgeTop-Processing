"""Real loopback concurrency against the reference API and SQLite queue."""
from concurrent.futures import ThreadPoolExecutor
import copy
import http.client
import json
from pathlib import Path
import tempfile
import threading
import unittest

from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_http import create_reference_http_server
from processing.reference_store import ReferenceStore,PROFILE

TOKEN='synthetic-staging-token-0001'
FIXTURE=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())


class ConcurrentAdmissionHTTPTest(unittest.TestCase):
    def test_concurrent_exact_and_changed_capability_replays(self):
        with tempfile.TemporaryDirectory() as root:
            queue=Queue(root);queue.allowed_profiles.add(PROFILE);store=ReferenceStore(queue)
            def make_api(capability,options):
                return ReferenceAPI(store,bearer_tokens={TOKEN:'staging'},capability_fingerprint=capability,
                    options_fingerprint=options,allowed_odm_options={'pc-quality','projection'})
            class Delegator:
                adapter=make_api(FIXTURE['capabilities']['capabilityFingerprint'],FIXTURE['capabilities']['engines']['odm']['optionsFingerprint'])
                def handle(self,*args,**kwargs):return self.adapter.handle(*args,**kwargs)
            adapter=Delegator();server=create_reference_http_server(adapter,request_timeout=3)
            thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02});thread.start()
            def run_batch(payload):
                barrier=threading.Barrier(4)
                def submit(_):
                    barrier.wait(timeout=5)
                    connection=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=5)
                    try:
                        connection.request('POST','/ltds/v1/jobs',json.dumps(payload).encode(),{'Authorization':'Bearer '+TOKEN})
                        response=connection.getresponse();return response.status,json.loads(response.read())
                    finally:connection.close()
                with ThreadPoolExecutor(max_workers=4) as pool:return list(pool.map(submit,range(4)))
            try:
                initial=run_batch(FIXTURE['submission'])
                self.assertEqual(sorted(status for status,_ in initial),[200,200,200,201])
                identities={body['jobId'] for _,body in initial};self.assertEqual(len(identities),1)
                adapter.adapter=make_api('c'*64,'d'*64)
                replay=run_batch(FIXTURE['submission'])
                self.assertEqual([status for status,_ in replay],[200]*4)
                self.assertEqual({body['jobId'] for _,body in replay},identities)
                changed=copy.deepcopy(FIXTURE['submission']);changed['manifest']['revision']+=1
                self.assertTrue(all(status!=200 for status,_ in run_batch(changed)))
                self.assertEqual(len(store.list()),1)
            finally:
                server.shutdown();server.server_close();thread.join(5)
                self.assertFalse(thread.is_alive());queue.close()
