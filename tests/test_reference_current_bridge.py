import hashlib,json,os,select,ssl,subprocess,sys,threading,time,unittest
from contextlib import contextmanager
from pathlib import Path
from tests.test_reference_joint_flow import ReferenceJointFlowTests, WORKER, VIEWER, READER, PINS as OLD_PINS
from tests.test_reference_sender import spawn_fixture_sender
from ops.native_node_fixture import ARCHIVE

PINS={
'referenceAcceptance.js':'c9971cd7c61bcc12ba9ea20487fb32c03ae0d04bb05661ebea17b1fb97d84ce4',
'referenceCaTransport.js':'3ea6ed955d40727d0c8d2bdc6d7f34fbb39c4802c130534da81c51465a3c9d19',
'referenceOptions.js':'1fa3564b6be07a46cb4695931528bea2a5465bc703ed8c562d55beb8d1a7e6c7',
'referenceProtocol.js':'17d505c268b53ff5e17c61bd6f89fff8081b9089d35ca00cef6d96906d934159',
'referenceProvider.js':'e829674ed52ab48a35810b9ebba7e8447d3d6b94d2944fe4eebbbc4fef7d5c48',
'referenceResultBridge.js':'8f45c2dd0c196a16a7984d8716fc400ad197bf8aa3f7fb7e339547b510b02105',
'referenceResultJournal.js':'e77f234cfff28f25baeadd0e6bdb9435cc21bc5ead54095262940280f352fd7c',
'referenceResultSpool.js':'ba37933234bdd03f321b6f9be46073de24cc46678be98148fba6f5b1682bc86a',
'referenceWireJson.js':'c63143addd3d9185cad70a310e711066114328f68c28c78af18581e2fb386ca0'}

@unittest.skipUnless(os.name=='posix' and os.environ.get('VIEWER_STAGED_ROOT'),'pinned staged Viewer required')
class CurrentBridgeTests(ReferenceJointFlowTests):
    @classmethod
    def setUpClass(cls):
        cls.cert=Path(os.environ['SYNTHETIC_TLS_ROOT'])/'cert.pem'
        cls.key=Path(os.environ['SYNTHETIC_TLS_ROOT'])/'key.pem'
        cls.viewer_root=Path(os.environ['VIEWER_REFERENCE_ROOT'])/'apps/viewer'
        for name,digest in OLD_PINS.items():
            if hashlib.sha256((cls.viewer_root/name).read_bytes()).hexdigest()!=digest:
                raise ValueError('old_broker_source_mismatch')
        cls.staged=Path(os.environ['VIEWER_STAGED_ROOT'])
        for name,digest in PINS.items():
            if hashlib.sha256((cls.staged/'server'/name).read_bytes()).hexdigest()!=digest:
                raise ValueError('staged_viewer_source_mismatch')

    def test_crashed_coordinator_then_current_viewer_read_no_acceptance(self):
        self.drop_result_ack=True
        self.delivered(until='delivering')
        # First delivery has committed but response was deliberately lost.
        result=self.driver.step()
        self.assertEqual(result['state'],'delivering')
        private=self.store.get(self.job,private=True)
        self.assertEqual(private['phase'],'awaiting_acceptance')
        self.assertEqual(len(self.completion_envelopes),1)
        manifest=private['resultManifest'];self.assertEqual(len(manifest['objects']),1)
        self.assertEqual(manifest['objects'][0]['sha256'],hashlib.sha256(ARCHIVE).hexdigest())
        self.assert_retained_before_acceptance()
        self.servers[self.control_origin][2]();self.queue.close()
        port=int(self.control_origin.rsplit(':',1)[1]);marker=self.root/'child-commit.json'
        worker_origin=self.control.viewer.result_proxy._origins['cluster']
        code=r"""
import json,os,ssl,sys,time
from contextlib import contextmanager
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.reference_control import ReferenceControlAPI
from processing.reference_result_proxy import ResultProxy
from processing.reference_http import create_reference_http_server
c=json.load(sys.stdin)
queue=Queue(c['root'],reference_enabled=True);store=ReferenceStore(queue)
@contextmanager
def admit(binding):yield
proxy=ResultProxy({'cluster':c['workerOrigin']},worker_bearers={'cluster':c['reader']},admit_read=admit,ssl_context=ssl.create_default_context(cafile=c['cert']))
api=ReferenceControlAPI(store,viewer_bearers={c['viewer']:'staging'},worker_bearers={c['worker']:{'node_id':'cluster','worker_id':'joint-worker','worker_instance_id':'joint-instance'}},capability_fingerprint=c['cap'],options_fingerprint=c['options'],result_proxy=proxy)
class Endpoint:
 def handle(self,method,path,headers,body,*,protected_transport=False):
  response=api.handle(method,path,headers,body,protected_transport=protected_transport)
  if path.endswith('/acceptance') and method=='POST':
   with open(c['audit'],'a') as f:f.write('unexpected-acceptance\\n')
  if c['stall'] and path=='/ltds/worker/v1/results':
   with open(c['marker'],'x') as f:
    json.dump({'delivery':json.loads(body)['delivery'],'receipt':response.body,'status':response.status},f);f.flush();os.fsync(f.fileno())
   print('COMMITTED',flush=True);time.sleep(20)
  return response
context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(c['cert'],c['key'])
server=create_reference_http_server(Endpoint(),port=c['port'],ssl_context=context)
print('READY',flush=True);server.serve_forever()
"""
        config=dict(root=str(self.root/'coordinator'),workerOrigin=worker_origin,reader=READER,viewer=VIEWER,worker=WORKER,
            cert=str(self.cert),key=str(self.key),port=port,marker=str(marker),audit=str(self.root/'ack-posts'),
            cap=self.control_options['capability_fingerprint'],options=self.control_options['options_fingerprint'])
        def start(stall):
            proc=spawn_fixture_sender(code,{**config,'stall':stall})
            try:
                if not select.select([proc.stdout],[],[],8)[0] or proc.stdout.readline().strip()!=b'READY':raise RuntimeError('server_not_ready')
                return proc
            except BaseException:
                if proc.poll() is None:proc.kill()
                proc.wait(timeout=3);proc.stdout.close();proc.stderr.close();raise
        def stop(proc):
            if proc.poll() is None:proc.kill()
            proc.wait(timeout=3);proc.stdout.close();proc.stderr.close()
        server=start(True);sender_thread=None
        try:
            row=self.outbox.get(self.completion_envelopes[0]['delivery']['outboxId'])
            self.outbox_now=row['next_at']
            outcome=[];sender_thread=threading.Thread(target=lambda:outcome.append(self.driver.step()));sender_thread.start()
            self.assertTrue(select.select([server.stdout],[],[],8)[0])
            self.assertEqual(server.stdout.readline().strip(),b'COMMITTED')
            evidence=json.loads(marker.read_text());self.assertEqual(evidence['status'],200)
            stop(server);server=None
            sender_thread.join(8);self.assertFalse(sender_thread.is_alive());self.assertEqual(outcome[0]['state'],'delivering')
            self.assertEqual(evidence['delivery'],self.completion_envelopes[0]['delivery'])
            server=start(False)
            row=self.outbox.get(evidence['delivery']['outboxId']);self.outbox_now=row['next_at']
            self.assertEqual(self.driver.step()['state'],'awaiting_acceptance')
            self.assertEqual(self.outbox.get(row['id'])['receipt'],evidence['receipt'])
            expected={'jobId':self.job,'generation':private['generation'],'environment':'staging',
                'inputManifestSha256':private['manifestSha256'],'pipelineDigest':private['pipelineDigest'],
                'resultManifestSha256':manifest['resultManifestSha256'],'requestedEngines':['odm']}
            work=self.root/'current-bridge';work.mkdir()
            env=dict(os.environ,VIEWER_STAGED_ROOT=str(self.staged),NODE_EXTRA_CA_CERTS=str(self.cert))
            request=json.dumps(dict(work=str(work),origin=self.control_origin,token=VIEWER,expected=expected)).encode()
            proc=subprocess.run(['node',str(Path(__file__).with_name('reference_current_bridge_synthetic.cjs'))],
                                input=request,env=env,capture_output=True,timeout=15)
            self.assertEqual(proc.returncode,0,proc.stderr.decode())
            answer=json.loads(proc.stdout);self.assertEqual(answer['sha256'],hashlib.sha256(ARCHIVE).hexdigest())
            self.assertEqual(answer['acknowledgement'],'pending');self.assertEqual(answer['pending'],1);self.assertEqual(answer['acceptancePosts'],0)
            self.assertFalse((self.root/'ack-posts').exists())
            self.assert_retained_before_acceptance()
        finally:
            try:
                if server is not None:stop(server)
            finally:
                if sender_thread is not None:
                    sender_thread.join(8);self.assertFalse(sender_thread.is_alive())


def load_tests(loader, tests, pattern):
    # Parent fixture also contains ACK/cleanup tests; never discover them here.
    return unittest.TestSuite([CurrentBridgeTests('test_crashed_coordinator_then_current_viewer_read_no_acceptance')])
