import copy
import http.client
import json
from pathlib import Path
import tempfile
import threading
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from processing.prototype import Queue
from processing.reference_store import ReferenceStore, PROFILE
from processing.reference_prepared_api_candidate import PreparedSubmissionAPI, PREFIX

TOKEN='synthetic-viewer-principal-token-1'
OTHER='synthetic-viewer-principal-token-2'
LEGACY='synthetic-environment-token-00001'
STAGING='synthetic-viewer-staging-token-01'

class PreparedAPITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.queue = Queue(self.tmp.name); self.queue.allowed_profiles.add(PROFILE); self.addCleanup(self.queue.close)
        self.store = ReferenceStore(self.queue)
        self.fixture = json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        self.policy = dict(capabilityFingerprint=self.fixture['capabilities']['capabilityFingerprint'], optionsFingerprint=self.fixture['capabilities']['engines']['odm']['optionsFingerprint'], allowedOdmOptions=['pc-quality','projection'],availableEngines=['odm'])
        self.entries = [dict(token=TOKEN,principalId='viewer-a',environment='production'),dict(token=OTHER,principalId='viewer-b',environment='production'),dict(token=STAGING,principalId='viewer-a',environment='staging')]
        self.api = self.make_api()
        self.envelope = dict(schemaVersion=2,protocol='ltds-viewer-prepared-envelope-v2',submission=copy.deepcopy(self.fixture['submission']), viewerTaskId=str(uuid.uuid4()), viewerAttemptId=str(uuid.uuid4()),activationId=str(uuid.uuid4()))
    def make_api(self, entries=None, legacy=None):
        return PreparedSubmissionAPI(self.store,credentials=self.entries if entries is None else entries,
            legacy_bearer_tokens=[LEGACY] if legacy is None else legacy,policies={'production':self.policy,'staging':self.policy})
    def req(self, method, path, body=None, token=TOKEN, headers=None):
        return self.api.handle(method,PREFIX+path,headers or {'Authorization':'Bearer '+token},b'' if body is None else json.dumps(body).encode(),protected_transport=True)
    def test_prepare_replay_recovery_and_privacy(self):
        first=self.req('POST','/submissions',self.envelope); self.assertEqual(first.status,201)
        self.assertEqual(self.req('POST','/submissions',self.envelope).status,200)
        path='/submissions/'+self.envelope['submission']['submissionId']
        recovered=self.req('GET',path); self.assertEqual(recovered.status,200)
        self.assertEqual(recovered.body['jobId'],first.body['jobId'])
        self.assertEqual(recovered.body['viewerAttemptId'],self.envelope['viewerAttemptId'])
        self.assertEqual(self.req('GET',path,token=OTHER).status,404)
        self.assertEqual(self.req('GET',path,token=STAGING).status,404)
        self.assertEqual(self.req('POST','/submissions',self.envelope,token=OTHER).status,409)
        self.assertEqual(len(self.queue.list()),1)
        self.assertIsNone(self.store.next_for_node('cluster'))
        self.assertNotIn(TOKEN,json.dumps(recovered.body))
    def test_registry_conflicts_and_auth(self):
        with self.assertRaises(ValueError): self.make_api(self.entries+[self.entries[0]])
        with self.assertRaises(ValueError): self.make_api(self.entries+[dict(token=LEGACY,principalId='x',environment='staging')])
        for headers in ({'Authorization':'Bearer '+LEGACY}, {'Authorization':'Bearer '+TOKEN,'authorization':'Bearer '+OTHER}):
            self.assertEqual(self.req('POST','/submissions',self.envelope,headers=headers).status,401)
        self.assertEqual(self.queue.list(),[])
    def test_malformed_and_server_policy_do_not_create_jobs(self):
        for change in (lambda e:e.update(extra=True), lambda e:e['submission'].update(capabilityFingerprint='0'*64), lambda e:e['submission']['pipeline']['odm'].update(options={'unknown':True})):
            env=copy.deepcopy(self.envelope); change(env)
            self.assertEqual(self.req('POST','/submissions',env).status,400)
        self.assertEqual(self.queue.list(),[])
    def test_transport_duplicate_nested_json_and_size_are_rejected(self):
        from processing import reference_protocol as protocol
        headers={'Authorization':'Bearer '+TOKEN}
        duplicate=b'{"submission":{"schemaVersion":1,"schemaVersion":1}}'
        oversized=b' '*(protocol.MAX_BODY_BYTES+1)
        for body in (duplicate, oversized):
            result=self.api.handle('POST',PREFIX+'/submissions',headers,body,protected_transport=True)
            self.assertEqual(result.status,400)
        result=self.api.handle('POST',PREFIX+'/submissions',headers,json.dumps(self.envelope).encode(),protected_transport=False)
        self.assertEqual(result.status,503)
        self.assertEqual(self.queue.list(),[])

    def test_server_policy_is_copied_and_secrets_redacted_on_error(self):
        self.policy['capabilityFingerprint']='0'*64
        self.assertEqual(self.req('POST','/submissions',self.envelope).status,201)
        from unittest.mock import patch
        with patch.object(self.store, 'find_viewer_envelope_v2', side_effect=RuntimeError(TOKEN)):
            result=self.req('GET','/submissions/'+self.envelope['submission']['submissionId'])
        self.assertEqual((result.status,result.body),(500,{'error':'internal_error'}))
        self.assertNotIn(TOKEN,json.dumps(result.body))

    def test_loopback_http_transport(self):
        api=self.api
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body=self.rfile.read(int(self.headers.get('content-length','0')))
                result=api.handle('POST',self.path,self.headers,body,protected_transport=True)
                raw=json.dumps(result.body).encode(); self.send_response(result.status); self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)
            def log_message(self,*args): pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        try:
            payload=json.dumps(self.envelope).encode()
            conn=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=5)
            try:
                conn.putrequest('POST',PREFIX+'/submissions')
                conn.putheader('Authorization','Bearer '+TOKEN)
                conn.putheader('Authorization','Bearer '+OTHER)
                conn.putheader('Content-Length',str(len(payload)))
                conn.endheaders(payload)
                result=conn.getresponse()
                self.assertEqual(result.status,401)
                result.read()
            finally:
                conn.close()
            self.assertEqual(self.queue.list(),[])
            request=Request('http://127.0.0.1:%s%s/submissions'%(server.server_port,PREFIX), data=json.dumps(self.envelope).encode(),headers={'Authorization':'Bearer '+TOKEN},method='POST')
            with urlopen(request,timeout=5) as response:
                self.assertEqual(response.status,201); self.assertIn('jobId',json.load(response))
        finally:
            server.shutdown(); server.server_close(); thread.join(5)
        self.assertFalse(thread.is_alive())

if __name__=='__main__': unittest.main()
