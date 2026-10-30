import copy,json,tempfile,threading,unittest,subprocess,sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request,urlopen
from processing.scratch_policy import ScratchLedger,PolicyConflict
from processing.viewer_node_capacity import CapacityService,create_loopback_server

TOKEN='synthetic-capacity-token-000000000000000'
class CapacityTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.data=self.root/'engine';self.data.mkdir();self.now=[1000];self.free=[1000]
  self.db=self.root/'private'/'ledger.sqlite';self.ledger=ScratchLedger(self.db,self.data,capacity_probe=lambda _:self.free[0],clock=lambda:self.now[0],safety_floor_bytes=100)
  self.s=CapacityService(self.ledger,token=TOKEN,output_budget=200,engine_storage_path=self.data,enabled=True,synthetic_mode=True)
  self.body=dict(jobId='job-1',generation=1,reservationId='reserve-1',manifestSha256='a'*64,inputBytes=300,environment='production',ttlSeconds=30)
 def tearDown(self):self.tmp.cleanup()
 def test_default_disabled_and_wrong_path(self):
  with self.assertRaises(ValueError):CapacityService(self.ledger,token=TOKEN,output_budget=200,engine_storage_path=self.data)
  with self.assertRaises(ValueError):CapacityService(self.ledger,token=TOKEN,output_budget=200,engine_storage_path=self.root,enabled=True,synthetic_mode=True)
  with self.assertRaises(ValueError):create_loopback_server(self.s,host='0.0.0.0')
 def test_real_filesystem_probe(self):
  other=ScratchLedger(self.root/'actual'/'ledger.sqlite',self.data,safety_floor_bytes=0)
  service=CapacityService(other,token=TOKEN,output_budget=1,engine_storage_path=self.data,enabled=True,synthetic_mode=True)
  body=dict(self.body,inputBytes=1)
  self.assertEqual(service.apply('/v1/reserve',body)['state'],'reserved')
 def test_concurrent_reservations_share_budget(self):
  other=dict(self.body,jobId='job-2',reservationId='reserve-2')
  def run(body):
   try:return self.s.apply('/v1/reserve',body)['state']
   except PolicyConflict:return 'held'
  with ThreadPoolExecutor(max_workers=2) as pool:values=list(pool.map(run,(self.body,other)))
  self.assertCountEqual(values,['reserved','held'])
 def test_start_rechecks_capacity_and_is_one_shot(self):
  self.s.apply('/v1/reserve',self.body);self.free[0]=400
  with self.assertRaises(PolicyConflict):self.s.apply('/v1/start',self.body)
  self.free[0]=1000;self.assertEqual(self.s.apply('/v1/start',self.body)['state'],'engine_started')
  with self.assertRaises(PolicyConflict):self.s.apply('/v1/start',self.body)
 def test_expiry_during_initial_probe_refuses_response(self):
  def slow_probe(_):self.now[0]+=31;return 1000
  self.ledger.probe=slow_probe
  with self.assertRaises(PolicyConflict):self.s.apply('/v1/reserve',self.body)
  self.now[0]=1000
  with self.assertRaises(PolicyConflict):self.s.apply('/v1/start',self.body)
 def test_expiry_during_capacity_probe_refuses_start(self):
  self.s.apply('/v1/reserve',self.body)
  def slow_probe(_):self.now[0]+=31;return 1000
  self.ledger.probe=slow_probe
  with self.assertRaises(PolicyConflict):self.s.apply('/v1/start',self.body)
  self.now[0]=1000
  self.ledger.probe=lambda _:1000
  with self.assertRaises(PolicyConflict):self.s.apply('/v1/start',self.body)
 def test_started_reservation_survives_restart_and_expiry(self):
  self.s.apply('/v1/reserve',self.body);self.s.apply('/v1/start',self.body);self.now[0]+=100
  ledger=ScratchLedger(self.db,self.data,capacity_probe=lambda _:self.free[0],clock=lambda:self.now[0],safety_floor_bytes=100)
  service=CapacityService(ledger,token=TOKEN,output_budget=200,engine_storage_path=self.data,enabled=True,synthetic_mode=True)
  with self.assertRaises(PolicyConflict):service.apply('/v1/start',self.body)
  other=dict(self.body,jobId='job-2',reservationId='reserve-2')
  with self.assertRaises(PolicyConflict):service.apply('/v1/reserve',other)
 def test_process_exit_after_start_preserves_uncertain_hold(self):
  self.s.apply('/v1/reserve',self.body)
  script="""import json,os,sys
from processing.scratch_policy import ScratchLedger
from processing.viewer_node_capacity import CapacityService
path,root,body=sys.argv[1],sys.argv[2],json.loads(sys.argv[3])
ledger=ScratchLedger(path,root,capacity_probe=lambda _:1000,clock=lambda:1000,safety_floor_bytes=100)
service=CapacityService(ledger,token='synthetic-capacity-token-000000000000000',output_budget=200,engine_storage_path=root,enabled=True,synthetic_mode=True)
service.apply('/v1/start',body)
os._exit(23)
"""
  result=subprocess.run([sys.executable,'-c',script,str(self.db),str(self.data),json.dumps(self.body)],timeout=10)
  self.assertEqual(result.returncode,23)
  ledger=ScratchLedger(self.db,self.data,capacity_probe=lambda _:1000,clock=lambda:1100,safety_floor_bytes=100)
  service=CapacityService(ledger,token=TOKEN,output_budget=200,engine_storage_path=self.data,enabled=True,synthetic_mode=True)
  with self.assertRaises(PolicyConflict):service.apply('/v1/start',self.body)
  with self.assertRaises(PolicyConflict):service.apply('/v1/reserve',dict(self.body,jobId='job-2',reservationId='reserve-2'))
 def test_http_loopback_auth_and_actual_ledger(self):
  server=create_loopback_server(self.s);thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01});thread.start()
  try:
   url='http://127.0.0.1:'+str(server.server_port)
   def post(path,token,body):
    request=Request(url+path,data=json.dumps(body).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
    with urlopen(request,timeout=3) as response:return response.status,json.load(response)
   with self.assertRaises(HTTPError) as err:post('/v1/reserve','wrong',self.body)
   self.assertEqual(err.exception.code,401);err.exception.close()
   self.assertEqual(post('/v1/reserve',TOKEN,self.body)[1]['state'],'reserved')
   self.free[0]=2000
   with self.assertRaises(HTTPError) as err:post('/v1/reserve',TOKEN,dict(self.body,reservationId='other-reservation'))
   self.assertEqual(err.exception.code,409);err.exception.close()
   self.assertEqual(post('/v1/start',TOKEN,self.body)[1]['state'],'engine_started')
   with self.assertRaises(HTTPError) as err:post('/v1/start',TOKEN,self.body)
   self.assertEqual(err.exception.code,409);err.exception.close()
  finally:server.shutdown();server.server_close();thread.join(5)
  self.assertFalse(thread.is_alive())

if __name__=='__main__':unittest.main()
