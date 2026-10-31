import base64,io,json,tempfile,threading,unittest,uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlencode
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from pathlib import Path
from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore,VIEWER_PROFILE
from processing.viewer_http import ViewerFacade
from tests.test_viewer_http import MemoryHandler,multipart,STAGING
import tests.test_viewer_service as viewer_service_tests
from unittest.mock import patch

class ReservationTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  self.queue=Queue(self.root/'queue.sqlite',node_ids=('cluster','main'),treedar_node_ids=('main',));self.addCleanup(self.queue.close);self.queue.allowed_profiles.add(VIEWER_PROFILE);self.store=TaskStore(self.queue,self.root/'inputs');self.id=str(uuid.uuid4());self.cap=base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip('=')
 def reserve(self):return self.store.reserve(self.id,'staging',self.cap)
 def initialize(self):return self.store.initialize(self.id,'test',[],None,'staging',self.cap)
 def photo(self):return self.store.upload(self.id,'a.jpg',io.BytesIO(b'jpeg'),4,'staging',self.cap)
 def test_reservation_replay_and_cancel_are_durable_and_fenced(self):
  self.assertEqual(self.reserve()['state'],'reserved');self.assertEqual(self.reserve()['state'],'reserved')
  with self.assertRaises(KeyError):self.store.reserve(self.id,'production',self.cap)
  self.assertEqual(self.store.cancel_reservation(self.id,'staging',self.cap)['state'],'cancelled')
  reopened=TaskStore(self.queue,self.root/'inputs');self.assertEqual(reopened.reserve(self.id,'staging',self.cap)['state'],'cancelled')
  with self.assertRaises(Conflict):reopened.initialize(self.id,'test',[],None,'staging',self.cap)
  with self.assertRaises(KeyError):reopened.initialize(self.id,'test',[],None,'staging')
 def test_reserved_upload_commit_and_legacy_bypass(self):
  self.reserve();self.initialize()
  with self.assertRaises(KeyError):self.store.upload(self.id,'a.jpg',io.BytesIO(b'jpeg'),4)
  with self.assertRaises(KeyError):self.store.commit(self.id)
  with self.assertRaises(KeyError):self.store.cancel(self.id)
  with self.assertRaises(KeyError):self.store.remove(self.id)
  self.photo();self.store.commit(self.id,'staging',self.cap)
  self.assertEqual(self.store.cancel_reservation(self.id,'staging',self.cap)['state'],'cancelled')
  with self.assertRaises(Conflict):self.store.commit(self.id,'staging',self.cap)
  self.assertEqual(self.store.task(self.id)['job']['status'],'failed')
 def test_cancel_and_commit_race_cannot_leave_a_queued_job(self):
  self.reserve();self.initialize();self.photo();barrier=threading.Barrier(2);errors=[]
  def commit():
   barrier.wait()
   try:self.store.commit(self.id,'staging',self.cap)
   except Conflict:pass
   except Exception as e:errors.append(e)
  thread=threading.Thread(target=commit);thread.start();barrier.wait();self.store.cancel_reservation(self.id,'staging',self.cap);thread.join(5)
  self.assertFalse(thread.is_alive());self.assertFalse(errors);task=self.store.task(self.id);self.assertEqual(task['stage'],'cancelled');self.assertTrue(task['job'] is None or task['job']['status']=='failed')
 def test_cancel_and_init_race_never_revives(self):
  self.reserve();barrier=threading.Barrier(2);errors=[]
  def initialize():
   barrier.wait()
   try:self.initialize()
   except Conflict:pass
   except Exception as e:errors.append(e)
  thread=threading.Thread(target=initialize);thread.start();barrier.wait();self.store.cancel_reservation(self.id,'staging',self.cap);thread.join(5)
  self.assertFalse(thread.is_alive());self.assertFalse(errors)
  with self.assertRaises(Conflict):self.initialize()
  self.assertEqual(self.reserve()['state'],'cancelled')
 def test_inflight_upload_finishes_before_cancellation_and_cannot_restart(self):
  self.reserve();self.initialize();entered=threading.Event();release=threading.Event();errors=[]
  class Blocking(io.BytesIO):
   def read(inner,n=-1):
    if not entered.is_set():entered.set();release.wait(5)
    return super().read(n)
  def upload():
   try:self.store.upload(self.id,'a.jpg',Blocking(b'jpeg'),4,'staging',self.cap)
   except Exception as error:errors.append(type(error).__name__)
  def cancel():
   try:self.store.cancel_reservation(self.id,'staging',self.cap)
   except Exception as error:errors.append(type(error).__name__)
  uploader=threading.Thread(target=upload);uploader.start();self.assertTrue(entered.wait(5));canceller=threading.Thread(target=cancel);canceller.start();release.set();uploader.join(5);canceller.join(5)
  self.assertFalse(uploader.is_alive());self.assertFalse(canceller.is_alive());self.assertFalse(errors)
  with self.assertRaises(Conflict):self.store.upload(self.id,'b.jpg',io.BytesIO(b'jpeg'),4,'staging',self.cap)
 def test_independent_store_cancel_during_upload_prevents_file_commit(self):
  self.reserve();self.initialize();other=TaskStore(self.queue,self.root/'inputs');entered=threading.Event();release=threading.Event();errors=[]
  class Blocking(io.BytesIO):
   def read(inner,n=-1):
    if not entered.is_set():entered.set();release.wait(5)
    return super().read(n)
  def upload():
   try:self.store.upload(self.id,'a.jpg',Blocking(b'jpeg'),4,'staging',self.cap);errors.append('upload_accepted')
   except Conflict:pass
   except Exception as error:errors.append(type(error).__name__)
  thread=threading.Thread(target=upload);thread.start();self.assertTrue(entered.wait(5));other.cancel_reservation(self.id,'staging',self.cap);release.set();thread.join(5)
  self.assertFalse(thread.is_alive());self.assertFalse(errors);self.assertEqual(self.store.task(self.id)['images_count'],0)
 def test_running_reservation_never_false_confirms_cancel(self):
  self.reserve();self.initialize();self.photo();self.store.commit(self.id,'staging',self.cap)
  self.queue.claim({'workerId':'synthetic','nodeId':'cluster','profiles':[VIEWER_PROFILE]})
  with self.assertRaises(Conflict):self.store.cancel_reservation(self.id,'staging',self.cap)
  self.assertEqual(self.reserve()['state'],'reserved')
 def test_capacity_fails_closed_without_deleting_tombstones(self):
  with patch('processing.viewer_tasks.RESERVATION_LIMIT',1):
   self.reserve();self.store.cancel_reservation(self.id,'staging',self.cap)
   self.assertEqual(self.store.reservation_stats()['remaining'],0)
   with self.assertRaises(Conflict):self.store.reserve(str(uuid.uuid4()),'staging',self.cap)
   self.assertEqual(self.reserve()['state'],'cancelled')
 def test_unknown_and_wrong_capability_never_cancel(self):
  with self.assertRaises(KeyError):self.store.cancel_reservation(self.id,'staging',self.cap)
  self.reserve();wrong=base64.urlsafe_b64encode(bytes(reversed(range(32)))).decode().rstrip('=')
  with self.assertRaises(KeyError):self.store.cancel_reservation(self.id,'staging',wrong)
  self.assertEqual(self.reserve()['state'],'reserved')
class ReservationHTTPTests(ReservationTests):
 def test_http_early_cancel_blocks_late_and_legacy_init(self):
  store=self.store
  class Service:
   def reserve(self,i,e,c):return store.reserve(i,e,c)
   def cancel_reserved(self,i,e,c):return store.cancel_reservation(i,e,c)
   def initialize(self,i,n,o,outputs,e,c=None):return store.initialize(i,n,o,outputs,e,c)
  facade=ViewerFacade(Service(),'production-test-token-0123456789',STAGING,temp_dir=str(self.root))
  body=('uuid='+self.id).encode()
  def action(route,cap=self.cap):
   h=MemoryHandler(route+'?token='+STAGING,body,Content_Type='application/x-www-form-urlencoded',Content_Length=str(len(body)),X_LedgeTop_Capability=cap);facade.handle(h);return h
  self.assertEqual(action('/task/ledgetop/reserve').status,200);self.assertEqual(action('/task/ledgetop/cancel').status,200)
  init=multipart([('name',None,b'test'),('options',None,b'[]')])
  def late(cap):
   kw={'Content_Type':'multipart/form-data; boundary=viewer-test-boundary','Content_Length':str(len(init)),'set_uuid':self.id}
   if cap is not None:kw['X_LedgeTop_Capability']=cap
   h=MemoryHandler('/task/new/init?token='+STAGING,init,**kw);facade.handle(h);return h
  self.assertEqual(late(self.cap).status,409);self.assertEqual(late(None).status,404)
  self.assertNotIn(self.cap,action('/task/ledgetop/cancel').wfile.getvalue().decode())
  h=MemoryHandler('/task/ledgetop/reserve?token='+STAGING,body,Content_Type='application/x-www-form-urlencoded',Content_Length=str(len(body)),X_LedgeTop_Capability=self.cap);h.headers.add_header('X-LedgeTop-Capability',self.cap);facade.handle(h);self.assertEqual(h.status,400)

class ReservationDispatchTests(unittest.TestCase):
 setUp=viewer_service_tests.ViewerServiceTests.setUp
 tearDown=viewer_service_tests.ViewerServiceTests.tearDown
 make_queue_service=viewer_service_tests.ViewerServiceTests.make_queue_service
 probe=viewer_service_tests.ViewerServiceTests.probe
 def prepare(self):
  identifier=str(uuid.uuid4());cap=base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip('=')
  self.service.reserve(identifier,'staging',cap);self.service.initialize(identifier,'test',[],None,'staging',cap)
  self.service.upload(identifier,'a.jpg',io.BytesIO(b'jpeg'),4,'staging',cap);self.service.commit(identifier,'staging',cap)
  return identifier,cap
 def test_real_http_cancel_before_init_survives_service_restart(self):
  cap=base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip('=');identifier=str(uuid.uuid4())
  facade=ViewerFacade(self.service,'production-test-token-0123456789',STAGING,temp_dir=str(self.root))
  class Handler(BaseHTTPRequestHandler):
   def log_message(inner,*args):pass
   def do_POST(inner):facade.handle(inner)
  server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
  try:
   def post(route,body,headers):
    req=Request('http://127.0.0.1:'+str(server.server_port)+route+'?'+urlencode({'token':STAGING}),data=body,headers=headers)
    try:
     with urlopen(req,timeout=5) as response:return response.status,json.load(response)
    except HTTPError as error:return error.code,json.load(error)
   body=urlencode({'uuid':identifier}).encode();headers={'Content-Type':'application/x-www-form-urlencoded','X-LedgeTop-Capability':cap}
   self.assertEqual(post('/task/ledgetop/reserve',body,headers)[1]['state'],'reserved');self.assertEqual(post('/task/ledgetop/cancel',body,headers)[1]['state'],'cancelled')
   self.service.close();self.queue.close();self.make_queue_service();facade.service=self.service
   self.assertEqual(post('/task/ledgetop/reserve',body,headers)[1]['state'],'cancelled')
   init=multipart([('name',None,b'test'),('options',None,b'[]')]);headers={'Content-Type':'multipart/form-data; boundary=viewer-test-boundary','set-uuid':identifier,'X-LedgeTop-Capability':cap}
   self.assertEqual(post('/task/new/init',init,headers)[0],409)
  finally:server.shutdown();server.server_close();thread.join(5)
 def test_info_advertises_exact_reservation_contract(self):
  info=self.service.capabilities();extension=info['ledgetopReservations']
  self.assertEqual(extension['version'],1);self.assertEqual(extension['capabilityEncoding'],'base64url-32');self.assertEqual(extension['retention'],'indefinite');self.assertEqual(extension['runningCancellation'],'requires_inspection')
 def test_cancel_before_dispatch_prevents_any_engine_action(self):
  identifier,cap=self.prepare();self.assertEqual(self.service.cancel_reserved(identifier,'staging',cap)['state'],'cancelled');self.service.tick()
  self.assertEqual(self.engines['cluster'].calls,[]);self.assertEqual(self.engines['main'].calls,[])
 def test_dispatch_in_flight_cancellation_stays_unconfirmed(self):
  identifier,cap=self.prepare();entered=threading.Event();release=threading.Event();errors=[];engine=self.engines['cluster'];original=engine.initialize
  def blocked(task):
   entered.set();self.assertTrue(release.wait(5));return original(task)
  def cancel():
   try:self.service.cancel_reserved(identifier,'staging',cap);errors.append('false_success')
   except Conflict:pass
   except Exception as error:errors.append(type(error).__name__)
  with patch.object(engine,'initialize',side_effect=blocked):
   dispatch=threading.Thread(target=self.service.tick);dispatch.start();self.assertTrue(entered.wait(5));cancel_thread=threading.Thread(target=cancel);cancel_thread.start();release.set();dispatch.join(5);cancel_thread.join(5)
  self.assertFalse(dispatch.is_alive());self.assertFalse(cancel_thread.is_alive());self.assertFalse(errors);self.assertEqual(self.service.reserve(identifier,'staging',cap)['state'],'reserved')

if __name__=='__main__':unittest.main()
