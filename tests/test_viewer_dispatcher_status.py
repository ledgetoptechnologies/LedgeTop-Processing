import json, tempfile, threading, unittest
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from processing.app import make_handler
from processing.config import Config
from tests.test_service import TOKEN,start_server

class DispatcherStatusTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  self.handler=make_handler(Config(api_token=TOKEN,local_node_url='http://127.0.0.1:19001',main_node_url='http://127.0.0.1:19002',local_max_photos=1000,local_max_input_bytes=10000,prototype_state_dir=self.tmp.name,prototype_node_ids=('cluster','main'),viewer_enabled=True,viewer_token='synthetic-viewer-token',prototype_observer_token='synthetic-observer-token'))
  self.service=self.handler.viewer_service;self.addCleanup(self.handler.prototype_queue.close);self.addCleanup(self.service.close)
  self.server=start_server(self.handler);self.addCleanup(self.server.server_close);self.addCleanup(self.server.shutdown)
  self.url='http://127.0.0.1:%d'%self.server.server_port
 def get(self,path,token=TOKEN):
  with urlopen(Request(self.url+path,headers={'Authorization':'Bearer '+token}),timeout=4) as response:return response.status,json.load(response)
 def test_authenticated_sticky_sanitized_signal_and_lifecycle(self):
  path='/v1/prototype/viewer-dispatcher'
  with self.assertRaises(HTTPError) as error:self.get(path,'bad')
  self.assertEqual(error.exception.code,401)
  self.assertEqual(self.get(path)[1],dict(enabled=True,threadState='not_started',unexpectedFailureObserved=False))
  self.assertEqual(self.get(path,'synthetic-observer-token')[0],200)
  recovered=threading.Event();calls=[]
  def tick():
   calls.append(1)
   if len(calls)==1:raise RuntimeError('private task and secret')
   recovered.set()
  before=self.handler.prototype_queue.list()
  self.service.tick=tick;self.service.start()
  self.assertTrue(recovered.wait(5))
  with ThreadPoolExecutor(max_workers=3) as pool:
   statuses=list(pool.map(lambda _:self.get(path)[1],range(6)))
  for result in statuses:
   self.assertEqual(result,dict(enabled=True,threadState='running',unexpectedFailureObserved=True))
   self.assertNotIn('private',json.dumps(result))
  self.assertEqual(self.handler.prototype_queue.list(),before)
  self.assertEqual(self.get('/healthz')[0],200)
  self.service.close();self.assertEqual(self.get(path)[1]['threadState'],'stopped')
 def test_unexpected_thread_death_is_not_reported_running(self):
  self.service._thread=threading.Thread(target=lambda:None);self.service._thread.start();self.service._thread.join(2)
  self.assertEqual(self.get('/v1/prototype/viewer-dispatcher')[1]['threadState'],'not_running')

class DispatcherDisabledTests(unittest.TestCase):
 def test_disabled_status_is_authenticated(self):
  handler=make_handler(Config(api_token=TOKEN,local_node_url='http://127.0.0.1:19001',main_node_url='http://127.0.0.1:19002',local_max_photos=1000,local_max_input_bytes=10000))
  server=start_server(handler)
  try:
   url='http://127.0.0.1:%d/v1/prototype/viewer-dispatcher'%server.server_port
   with self.assertRaises(HTTPError) as error:urlopen(Request(url,headers={'Authorization':'Bearer bad'}),timeout=4)
   self.assertEqual(error.exception.code,401)
   with urlopen(Request(url,headers={'Authorization':'Bearer '+TOKEN}),timeout=4) as response:self.assertEqual(json.load(response),{'enabled':False})
  finally:server.shutdown();server.server_close()
