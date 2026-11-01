import base64,http.client,json,tempfile,threading,time,unittest,uuid
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode
from processing import config as runtime_config,reference_protocol as ref
from processing.app import make_handler
from processing.config import Config
from processing.prototype import Queue
from tests.test_viewer_http import multipart,BOUNDARY

TOKEN='synthetic-production-token-'+'p'*32
STAGING='synthetic-staging-token-'+'s'*32
CAP=base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip('=')
OTHER=base64.urlsafe_b64encode(bytes(range(1,33))).decode().rstrip('=')
BODY=json.loads((Path(__file__).parent/'fixtures/native_input_manifest_vectors.json').read_text())['vectors'][0]['canonical'].encode()

class ManifestBindingHTTP(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.leases=self.root/'leases';self.leases.mkdir()
        self.state=self.root/'state';q=Queue(self.state,node_ids=('cluster',),viewer_enabled=True);q.close()
        self.config=Config('a'*32,'http://127.0.0.1:1','http://127.0.0.1:2',1000,20*1024**3,prototype_state_dir=str(self.state),
            prototype_node_ids=('cluster',),treedar_node_ids=('cluster',),viewer_enabled=True,
            viewer_token=TOKEN,viewer_staging_token=STAGING,authority_lease_dir=str(self.leases),viewer_manifest_binding=True)
        self.qualification=patch.object(runtime_config,'NATIVE_MANIFEST_BINDING_QUALIFIED',True);self.qualification.start();self.addCleanup(self.qualification.stop);self.addCleanup(self.tmp.cleanup)
        self.handler=None;self.server=None;self.thread=None;self.start()
    def start(self):
        self.handler=make_handler(self.config);self.server=ThreadingHTTPServer(('127.0.0.1',0),self.handler)
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.02});self.thread.start()
    def stop(self):
        if self.server is not None:
            self.server.shutdown();self.server.server_close();self.thread.join(5);self.assertFalse(self.thread.is_alive())
            self.handler.viewer_service.close();self.handler.prototype_queue.close();self.handler.prototype_queue.authority_lease.close()
            self.server=None
    def tearDown(self):
        try:self.stop()
        finally:self.qualification.stop();self.tmp.cleanup()
    def request(self,method,path,*,token=TOKEN,cap=CAP,body=b'',kind='application/json',extra=None):
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=5)
        headers={'Content-Type':kind,'X-LedgeTop-Capability':cap};headers.update(extra or {})
        try:
            conn.request(method,path+'?'+urlencode({'token':token}),body=body,headers=headers)
            response=conn.getresponse();return response.status,json.loads(response.read())
        finally:conn.close()
    def reserve(self,identifier,cap=CAP):
        status,_=self.request('POST','/task/ledgetop/reserve',cap=cap,kind='application/x-www-form-urlencoded',body=urlencode({'uuid':identifier}).encode());self.assertEqual(status,200)
    def binding(self,identifier,body=BODY,**kw):return self.request('POST','/task/ledgetop/input-manifest/'+identifier,body=body,**kw)
    def test_exact_replay_restart_auth_and_legacy_hold(self):
        identifier=str(uuid.uuid4());self.reserve(identifier)
        status,receipt=self.binding(identifier);self.assertEqual(status,200)
        self.assertEqual((receipt['generation'],receipt['state'],receipt['capacityReady'],receipt['manifestRevision']),(1,'identity_bound',False,1))
        self.assertNotIn(CAP,json.dumps(receipt));self.assertEqual(self.binding(identifier),(200,receipt))
        self.assertEqual(self.request('GET','/task/ledgetop/input-manifest/'+identifier)[1],receipt)
        self.assertEqual(self.binding(identifier,cap=OTHER)[0],404);self.assertEqual(self.binding(identifier,token=STAGING)[0],404)
        changed=json.loads(BODY);changed['objects'][0]['sha256']='a'*64;changed['sha256']=ref.manifest_sha256(changed)
        self.assertEqual(self.binding(identifier,body=ref.canonical_json(changed))[0],409)
        form=multipart([('name',None,b'fixture'),('options',None,b'[]')])
        self.assertEqual(self.request('POST','/task/new/init',body=form,kind='multipart/form-data; boundary='+BOUNDARY,extra={'set-uuid':identifier})[0],409)
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_tasks').fetchone()[0],0)
        self.stop();self.start();self.assertEqual(self.request('GET','/task/ledgetop/input-manifest/'+identifier)[1],receipt)
        self.stop()
        with self.assertRaisesRegex(RuntimeError,'native_manifest_binding_configuration_required'):
            make_handler(Config('a'*32,'','',1000,20*1024**3,prototype_state_dir=str(self.state),viewer_enabled=True,viewer_token=TOKEN,viewer_staging_token=STAGING,authority_lease_dir=str(self.leases)))
    def test_conflicts_concurrency_and_historical_artifacts(self):
        identifier=str(uuid.uuid4());self.reserve(identifier);results=[]
        def attempt():results.append(self.binding(identifier))
        threads=[threading.Thread(target=attempt) for _ in range(4)]
        for thread in threads:thread.start()
        for thread in threads:thread.join(5);self.assertFalse(thread.is_alive())
        self.assertEqual(len(results),4);self.assertTrue(all(x==results[0] for x in results));self.assertEqual(results[0][0],200)
        other=str(uuid.uuid4());self.reserve(other,OTHER);self.assertEqual(self.binding(other,cap=OTHER)[0],409)
        orphan=str(uuid.uuid4());self.reserve(orphan,OTHER);directory=self.handler.viewer_service.store.root/(orphan+'-1');directory.mkdir();(directory/'sentinel').write_bytes(b'retain')
        self.assertEqual(self.binding(orphan,cap=OTHER)[0],409);self.assertEqual((directory/'sentinel').read_bytes(),b'retain')
        historical=str(uuid.uuid4());self.reserve(historical,OTHER)
        self.handler.viewer_service.store.initialize(historical,'fixture',[],None,'production',OTHER)
        self.assertEqual(self.binding(historical,cap=OTHER)[0],409)
        self.assertEqual(self.handler.viewer_service.store.task(historical)['generation'],1)
        # Synthetic historical removed row; current reserved-task remove API
        # intentionally refuses without its separate cancellation flow.
        with self.handler.prototype_queue._db() as db:db.execute('UPDATE viewer_tasks SET removed=1 WHERE uuid=?',(historical,))
        self.assertEqual(self.binding(historical,cap=OTHER)[0],409)

    def test_wrong_capability_is_rejected_before_body_and_cancel_holds(self):
        identifier=str(uuid.uuid4());self.reserve(identifier)
        connection=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=3)
        try:
            connection.putrequest('POST','/task/ledgetop/input-manifest/'+identifier+'?'+urlencode({'token':TOKEN}))
            connection.putheader('Content-Type','application/json');connection.putheader('Content-Length','100000')
            connection.putheader('X-LedgeTop-Capability',OTHER);connection.endheaders()
            response=connection.getresponse();self.assertEqual(response.status,404);response.read()
        finally:connection.close()
        self.assertEqual(self.binding(identifier)[0],200)
        status,_=self.request('POST','/task/ledgetop/cancel',kind='application/x-www-form-urlencoded',body=urlencode({'uuid':identifier}).encode())
        self.assertEqual(status,200);self.assertEqual(self.binding(identifier)[0],409)
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_manifest_bindings WHERE uuid=?',(identifier,)).fetchone()[0],1)

    def test_distinct_tasks_compete_for_same_manifest_identity(self):
        first,second=str(uuid.uuid4()),str(uuid.uuid4());self.reserve(first,CAP);self.reserve(second,OTHER)
        value=json.loads(BODY);value['id']='attempt-race';value['sha256']=ref.manifest_sha256(value);body=ref.canonical_json(value)
        barrier=threading.Barrier(2);results=[]
        def attempt(identifier,cap):
            barrier.wait(3);results.append(self.binding(identifier,cap=cap,body=body)[0])
        threads=[threading.Thread(target=attempt,args=(first,CAP)),threading.Thread(target=attempt,args=(second,OTHER))]
        for thread in threads:thread.start()
        for thread in threads:thread.join(5);self.assertFalse(thread.is_alive())
        self.assertEqual(sorted(results),[200,409])
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute("SELECT COUNT(*) FROM native_manifest_bindings WHERE manifest_id='attempt-race'").fetchone()[0],1)

    def test_lost_http_response_replays_same_durable_record(self):
        identifier=str(uuid.uuid4());self.reserve(identifier)
        connection=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=3)
        connection.request('POST','/task/ledgetop/input-manifest/'+identifier+'?'+urlencode({'token':TOKEN}),body=BODY,
                           headers={'Content-Type':'application/json','X-LedgeTop-Capability':CAP})
        connection.close()  # Deliberately discard the server's response.
        deadline=time.monotonic()+5
        while True:
            status,receipt=self.request('GET','/task/ledgetop/input-manifest/'+identifier)
            if status==200:break
            if time.monotonic()>=deadline:self.fail('binding did not become observable')
            time.sleep(.02)
        self.assertEqual(self.binding(identifier),(200,receipt))
        self.stop();self.start();self.assertEqual(self.request('GET','/task/ledgetop/input-manifest/'+identifier),(200,receipt))
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_manifest_bindings').fetchone()[0],1)

    def test_history_capacity_holds_new_but_allows_exact_replay(self):
        first,second=str(uuid.uuid4()),str(uuid.uuid4());self.reserve(first,CAP);self.reserve(second,OTHER)
        other=json.loads(BODY);other['id']='other-attempt';other['sha256']=ref.manifest_sha256(other)
        with patch('processing.native_manifest_binding.MAX_RECORDS',1):
            status,receipt=self.binding(first);self.assertEqual(status,200)
            self.assertEqual(self.binding(second,cap=OTHER,body=ref.canonical_json(other))[0],409)
            self.assertEqual(self.binding(first),(200,receipt))

class ManifestBindingQualification(unittest.TestCase):
    def test_environment_cannot_enable_unqualified_feature(self):
        with patch.dict('os.environ',{'PROCESSING_API_TOKEN':'a'*32,'PROCESSING_VIEWER_MANIFEST_BINDING':'1'},clear=True):
            with self.assertRaisesRegex(ValueError,'not qualified'):runtime_config.from_environment()
    def test_direct_config_refuses_before_state_creation(self):
        with tempfile.TemporaryDirectory() as root:
            config=Config('a'*32,'','',1000,100,prototype_state_dir=str(Path(root)/'not-created'),viewer_enabled=True,viewer_token=TOKEN,authority_lease_dir=str(Path(root)/'lease'),viewer_manifest_binding=True)
            with self.assertRaisesRegex(RuntimeError,'not_qualified'):make_handler(config)
            self.assertEqual(list(Path(root).iterdir()),[])
    def test_direct_binding_requires_held_authority(self):
        from processing.native_manifest_binding import NativeManifestBindings
        from processing.viewer_tasks import TaskStore
        from processing.maintenance import Conflict
        with tempfile.TemporaryDirectory() as root:
            queue=Queue(Path(root)/'state');store=TaskStore(queue,Path(root)/'tasks')
            try:
                with self.assertRaisesRegex(Conflict,'not qualified'):NativeManifestBindings(store)
                with patch.object(runtime_config,'NATIVE_MANIFEST_BINDING_QUALIFIED',True):
                    with self.assertRaisesRegex(Conflict,'authority unavailable'):NativeManifestBindings(store)
                with queue._db() as db:self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='native_manifest_bindings'").fetchone())
            finally:queue.close()

if __name__=='__main__':unittest.main()
