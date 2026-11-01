import hashlib,io,json,os,select,ssl,subprocess,sys,tempfile,threading,time,unittest,zipfile
from contextlib import contextmanager
from unittest.mock import patch
from pathlib import Path
from tests.test_reference_joint_flow import ReferenceJointFlowTests as _JointFlow, WORKER, VIEWER, READER, PINS as OLD_PINS
from tests.test_reference_sender import spawn_fixture_sender

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
class CurrentBridgeTests(_JointFlow):
    @classmethod
    def setUpClass(cls):
        # The inherited class hook reads Viewer exports. Never run it here:
        # direct unittest invocation calls class hooks before per-test setup.
        pass

    def setUp(self):
        if self._testMethodName!='test_crashed_coordinator_then_current_viewer_read_no_acceptance':
            raise unittest.SkipTest('inherited acceptance/cleanup fixture disabled here')
        self.cert=Path(os.environ['SYNTHETIC_TLS_ROOT'])/'cert.pem'
        self.key=Path(os.environ['SYNTHETIC_TLS_ROOT'])/'key.pem'
        self.viewer_root=Path(os.environ['VIEWER_REFERENCE_ROOT'])/'apps/viewer'
        for name,digest in OLD_PINS.items():
            if hashlib.sha256((self.viewer_root/name).read_bytes()).hexdigest()!=digest:
                raise ValueError('old_broker_source_mismatch')
        self.staged=Path(os.environ['VIEWER_STAGED_ROOT'])
        for name,digest in PINS.items():
            if hashlib.sha256((self.staged/'server'/name).read_bytes()).hexdigest()!=digest:
                raise ValueError('staged_viewer_source_mismatch')
        server=self.staged/'server';digest=hashlib.sha256();files=[]
        for path in sorted(server.rglob('*')):
            if path.is_symlink():raise ValueError('staged_source_symlink')
            if path.is_file():
                relative=path.relative_to(server).as_posix()
                digest.update((relative+'\0'+hashlib.sha256(path.read_bytes()).hexdigest()+'\n').encode());files.append(relative)
        if len(files)!=165 or digest.hexdigest()!='d5d5c0680da0e186034b6de2ad0d9974471fadd7b3f5d0506379fe04f5e99a2f':
            raise ValueError('staged_server_tree_changed')
        out=io.BytesIO()
        with zipfile.ZipFile(out,'w',compression=zipfile.ZIP_STORED) as archive:
            for name,data in {
                'odm_orthophoto/odm_orthophoto.tif':'synthetic',
                'odm_texturing/odm_textured_model_geo.glb':'glb',
                'odm_texturing/odm_textured_model_geo.obj':'mtllib model.mtl\nv 0 0 0\n',
                'odm_texturing/model.mtl':'newmtl surface\nmap_Kd texture.jpg\n',
                'odm_texturing/texture.jpg':'texture','entwine_pointcloud/ept.json':'{}',
                '3d_tiles/model/tileset.json':'{}'}.items():archive.writestr(name,data)
        self.archive=out.getvalue()
        for target in ('ops.native_node_fixture.ARCHIVE','tests.test_reference_joint_flow.ARCHIVE'):
            active=patch(target,self.archive);active.start();self.addCleanup(active.stop)
        super().setUp()

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
        self.assertEqual(manifest['objects'][0]['sha256'],hashlib.sha256(self.archive).hexdigest())
        self.assert_retained_before_acceptance()
        self.servers[self.control_origin][2]();self.queue.close()
        port=int(self.control_origin.rsplit(':',1)[1]);marker=self.root/'child-commit.json'
        worker_origin=self.control.viewer.result_proxy._origins['cluster']
        code=r"""
import json,os,ssl,sys,time
from contextlib import contextmanager
from unittest.mock import patch
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
            # Root-created synthetic configuration; custody executes as a separate unprivileged UID.
            lab=Path(self.stack.enter_context(tempfile.TemporaryDirectory(prefix='viewer-model-')));os.chmod(lab,0o755)
            custody=lab/'custody';custody.mkdir(mode=0o700);os.chown(custody,12345,12345)
            lockroot=lab/'custody-lock';lockroot.mkdir(mode=0o755)
            lockfile=lockroot/'viewer-source.lock';lockfile.touch(mode=0o660);os.chown(lockfile,0,12345);os.chmod(lockfile,0o660)
            def identity(path):
                fd=os.open(path,os.O_RDONLY|os.O_CLOEXEC)
                try:
                    info=os.fstat(fd)
                    fields=Path('/proc/self/fdinfo/'+str(fd)).read_text().splitlines()
                    mount=next(line.split(':',1)[1].strip() for line in fields if line.startswith('mnt_id:'))
                    return dict(dev=str(info.st_dev),ino=str(info.st_ino),mountId=mount)
                finally:os.close(fd)
            helper=Path('/usr/bin/python3.11')
            configdir=Path(self.stack.enter_context(tempfile.TemporaryDirectory(prefix='ref-config-',dir='/opt')))
            os.chmod(configdir,0o755);configpath=configdir/'custody.json'
            configuration=dict(schemaVersion=1,environment='staging',custodyUid=12345,custodyGid=12345,
                allowedGroupIds=[12345],custodyPath=str(custody),lockRootPath=str(lockroot),helperPath=str(helper),
                helperSha256=hashlib.sha256(helper.read_bytes()).hexdigest(),custody=identity(custody),
                lockRoot=identity(lockroot),lock=identity(lockfile))
            configpath.write_text(json.dumps(configuration));os.chown(configpath,0,12345);os.chmod(configpath,0o640)
            env=dict(os.environ,VIEWER_STAGED_ROOT=str(self.staged),NODE_EXTRA_CA_CERTS=str(self.cert),NODE_PATH='/src/apps/viewer/node_modules')
            request=json.dumps(dict(custody=str(custody),configPath=str(configpath),origin=self.control_origin,token=VIEWER,expected=expected)).encode()
            proc=subprocess.run(['/usr/bin/setpriv','--reuid=12345','--regid=12345','--clear-groups','--no-new-privs',
                                 'node',str(Path(__file__).with_name('reference_current_bridge_synthetic.cjs'))],
                                input=request,env=env,capture_output=True,timeout=30)
            self.assertEqual(proc.returncode,0,proc.stderr.decode())
            answer=json.loads(proc.stdout);self.assertEqual(answer['sha256'],hashlib.sha256(self.archive).hexdigest())
            self.assertEqual(answer['acknowledgement'],'pending');self.assertEqual(answer['pending'],1);self.assertEqual(answer['acceptancePosts'],0);self.assertIn(answer['modelStatus'],('staged','ready'))
            self.assertFalse((self.root/'ack-posts').exists())
            self.assert_retained_before_acceptance()
        finally:
            try:
                if server is not None:stop(server)
            finally:
                if sender_thread is not None:
                    sender_thread.join(8);self.assertFalse(sender_thread.is_alive())


del _JointFlow

class FixtureInvocationTests(unittest.TestCase):
    def test_inherited_acceptance_test_refuses_direct_invocation(self):
        case=CurrentBridgeTests('test_real_node_broker_provider_acceptance_and_python_worker_cleanup_order')
        result=unittest.TestResult();case.run(result)
        self.assertEqual(result.testsRun,1);self.assertEqual(len(result.skipped),1)
        self.assertFalse(result.errors or result.failures)

    @unittest.skipUnless(os.name=='posix','Linux test runner required')
    def test_unittest_subprocess_skips_before_any_export_access(self):
        env=dict(os.environ,VIEWER_STAGED_ROOT='/nonexistent-staged-fixture',
                 VIEWER_REFERENCE_ROOT='/nonexistent-old-fixture',SYNTHETIC_TLS_ROOT='/nonexistent-tls-fixture')
        case='tests.test_reference_current_bridge.CurrentBridgeTests.test_real_node_broker_provider_acceptance_and_python_worker_cleanup_order'
        result=subprocess.run([sys.executable,'-m','unittest','-v',case],env=env,capture_output=True,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr.decode())
        self.assertIn(b'skipped',result.stderr)
        self.assertNotIn(b'FileNotFoundError',result.stderr)


def load_tests(loader, tests, pattern):
    return unittest.TestSuite([CurrentBridgeTests('test_crashed_coordinator_then_current_viewer_read_no_acceptance'),
                               FixtureInvocationTests('test_inherited_acceptance_test_refuses_direct_invocation'),
                               FixtureInvocationTests('test_unittest_subprocess_skips_before_any_export_access')])
