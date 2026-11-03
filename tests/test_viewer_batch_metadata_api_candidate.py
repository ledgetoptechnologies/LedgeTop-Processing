import hashlib,http.client,json,tempfile,threading,unittest
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.viewer_batch_intent_candidate import initialize
from processing.viewer_batch_identity_candidate import Identity
from processing.viewer_batch_metadata_api_candidate import BatchMetadataAPI
from processing.reference_http import create_reference_http_server

class NetworkBatchTest(unittest.TestCase):
 def test_actual_loopback_auth_and_generation(self):
  with tempfile.TemporaryDirectory() as root:
   q=Queue(Path(root)/'q');server=None;thread=None
   try:
    store=TaskStore(q,Path(root)/'inputs');identity=Identity(store,[{'token':'a'*32,'principalId':'alice','environment':'production'},{'token':'b'*32,'principalId':'bob','environment':'production'},{'token':'c'*32,'principalId':'alice','environment':'staging'}],['z'*32],q.authority_id);identity.initialize();initialize(store)
    server=create_reference_http_server(BatchMetadataAPI(identity));thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    task='11111111-2222-4333-8444-555555555555';cap='A'*43;prefix='/ltds/held-batches/v1/tasks/'+task
    def request(path,body,token='a'*32,duplicate=False,credential=cap):
     data=json.dumps(body).encode();c=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=5)
     c.putrequest('POST',path);c.putheader('Authorization','Bearer '+token)
     if duplicate:c.putheader('Authorization','Bearer '+token)
     c.putheader('X-LedgeTop-Capability',credential);c.putheader('Content-Length',str(len(data)));c.endheaders(data);r=c.getresponse();status=r.status;r.read();c.close();return status
    self.assertEqual(request(prefix+'/reserve',{'expectedAuthorityId':q.authority_id},duplicate=True),400)
    self.assertEqual(request(prefix+'/reserve',{'expectedAuthorityId':'wrong'}),409)
    self.assertEqual(request(prefix+'/reserve',{'expectedAuthorityId':q.authority_id}),200)
    store.initialize(task,'x',[],None,'production',cap)
    payload={'expectedAuthorityId':q.authority_id,'generation':1,'batchId':'batch','files':[{'name':'a.jpg','bytes':3,'sha256':hashlib.sha256(b'abc').hexdigest()}]}
    for token in ('b'*32,'c'*32):self.assertEqual(request(prefix+'/prepare',payload,token),409)
    self.assertEqual(request(prefix+'/prepare',payload,'z'*32),401)
    self.assertEqual(request(prefix+'/prepare',payload,credential='B'*43),409)
    self.assertEqual(request(prefix+'/prepare',{**payload,'generation':2}),409)
    self.assertEqual(request(prefix+'/prepare',payload),200)
   finally:
    if server:server.shutdown();server.server_close()
    if thread:thread.join(timeout=5)
    q.close()
