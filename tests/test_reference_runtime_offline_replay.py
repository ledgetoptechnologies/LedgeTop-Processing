"""Actual dynamic HTTP route with a failed NodeODM catalog lookup."""
import copy
import http.client
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
import uuid
from contextlib import contextmanager

from processing import reference_protocol as p
from processing.admission_drain import AdmissionClosed
from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_http import create_reference_http_server
from processing.reference_runtime import _DynamicAPI
from processing.reference_store import ReferenceStore,PROFILE

TOKEN='synthetic-staging-token-0001';OTHER='synthetic-production-token-0002'
FIXTURE=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())

class Gate:
    closed=False
    @contextmanager
    def admit(self):
        if self.closed:raise AdmissionClosed()
        yield

class DynamicOfflineReplayTests(unittest.TestCase):
    def test_committed_replay_without_catalog_respects_auth_and_drain(self):
        with tempfile.TemporaryDirectory() as root:
            queue=Queue(root);self.addCleanup(queue.close);queue.allowed_profiles.add(PROFILE);store=ReferenceStore(queue)
            body=FIXTURE['submission'];validated=p.validate_submission(body,environment='staging',capability_fingerprint=body['capabilityFingerprint'])
            job,created=store.submit(validated);self.assertTrue(created)
            viewer=ReferenceAPI(store,bearer_tokens={TOKEN:'staging',OTHER:'production'},capability_fingerprint='a'*64,
                options_fingerprint='b'*64,allowed_odm_options={'pc-quality','projection'})
            def offline():raise OSError('synthetic catalog offline')
            dynamic=_DynamicAPI(SimpleNamespace(viewer=viewer),None,SimpleNamespace(viewer_bearers={TOKEN:'staging',OTHER:'production'}),offline)
            gate=Gate();queue.admission_drain=gate
            server=create_reference_http_server(dynamic,request_timeout=2);self.addCleanup(server.server_close)
            thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02});thread.start()
            def request(payload,token=TOKEN):
                conn=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
                try:
                    conn.request('POST','/ltds/v1/jobs',json.dumps(payload).encode(),{'Authorization':'Bearer '+token})
                    response=conn.getresponse();return response.status,json.loads(response.read())
                finally:conn.close()
            try:
                status,replay=request(body);self.assertEqual((status,replay['jobId']),(200,job['jobId']))
                self.assertEqual(request(body,OTHER)[0],503)
                self.assertEqual(request(body,'bad-token-00000000')[0],401)
                fresh=copy.deepcopy(body);fresh['submissionId']=str(uuid.uuid4());self.assertEqual(request(fresh)[0],503)
                changed=copy.deepcopy(body);changed['manifest']['revision']+=1;self.assertEqual(request(changed)[0],503)
                gate.closed=True;self.assertEqual(request(body)[0],503)
                self.assertEqual(len(store.list()),1)
            finally:
                try:
                    server.shutdown();thread.join(5);self.assertFalse(thread.is_alive())
                finally:server.server_close();queue.close()
