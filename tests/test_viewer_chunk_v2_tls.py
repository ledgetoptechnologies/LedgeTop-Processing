"""Isolated fixture-only v2 TLS tests; no promotion or custody."""
import http.client,json,os,signal,subprocess,sys,unittest
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_chunk_v2_spool import SyntheticV2Spool
from processing.viewer_synthetic_v2_sender import send
from tests.test_viewer_archive_spool import ViewerArchiveSpoolTests

class V2TlsTests(unittest.TestCase):
 def setUp(self):
  self.f=ViewerArchiveSpoolTests('test_authenticated_exact_spool_and_replay');self.addCleanup(self.f.doCleanups);self.f.setUp()
  self.api=self.f.fixture.api;self.api.viewer_v2_chunks=SyntheticV2Spool(self.f.spool,synthetic_fixture=True)
  self.job=self.f.fixture.job
  with self.f.fixture.queue._db() as db:self.token=db.execute('SELECT token FROM viewer_worker_claims WHERE job_id=?',(self.job,)).fetchone()[0]
 def request(self,op,*,token=None,bearer=None,version='2',body=b'',extra=None):
  conn=http.client.HTTPSConnection('127.0.0.1',self.f.fixture.server.server_port,context=self.f.fixture.client,timeout=5)
  try:
   headers={'Authorization':'Bearer '+(bearer or 'a'*32),'X-Claim-Token':token or self.token,'Content-Type':'application/octet-stream','X-Chunk-Version':version,**(extra or {})}
   conn.request('POST','/ltds/worker/v1/viewer-claims/'+self.job+'/chunks-v2/'+op,body,headers);r=conn.getresponse();return r.status,json.loads(r.read())
  finally:conn.close()
 def test_real_tls_sender_and_exact_replay_no_standard_receipt(self):
  with DaemonStateLock(self.f.base.base.base.base.worker) as owner:
   worker=self.f.base.worker(owner);value=send(worker,synthetic_fixture=True,chunk_bytes=16);self.assertEqual(value['state'],'sealed_no_receipt')
   self.assertEqual(send(worker,synthetic_fixture=True,chunk_bytes=16),value)
   with self.assertRaises(Exception):send(worker,synthetic_fixture=True,chunk_bytes=17)
  with self.f.fixture.queue._db() as db:
   self.assertIsNone(db.execute('SELECT 1 FROM viewer_worker_spool WHERE job_id=?',(self.job,)).fetchone())
 def test_auth_and_version_rejected_before_body(self):
  conn=http.client.HTTPSConnection('127.0.0.1',self.f.fixture.server.server_port,context=self.f.fixture.client,timeout=5)
  try:
   conn.putrequest('POST','/ltds/worker/v1/viewer-claims/'+self.job+'/chunks-v2/part-0')
   for k,v in {'Authorization':'Bearer wrong','X-Claim-Token':self.token,'Content-Type':'application/octet-stream','X-Chunk-Version':'2','Content-Length':'16'}.items():conn.putheader(k,v)
   conn.endheaders();self.assertEqual(conn.getresponse().status,401)
  finally:conn.close()
  self.assertEqual(self.request('begin',version='1',extra={'X-Chunk-Size':'16'})[0],409)
  with self.f.fixture.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_chunk_permits').fetchone()[0],0)
 def test_process_kill_after_finished_chunk_restarts_exactly(self):
  code='''import json,os,signal,sys
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
import processing.viewer_synthetic_v2_sender as sender
root,config,mode=sys.argv[1:];original=sender.strict_response
def observe(response):
 value=original(response)
 if mode=='kill' and isinstance(value,dict) and value.get('state')=='finished' and 'receipt' in value:os.kill(os.getpid(),signal.SIGKILL)
 return value
with DaemonStateLock(root) as owner:
 with patch.object(sender,'strict_response',observe):print(json.dumps(sender.send(ViewerNodeEnrollment(owner,**json.loads(config)),synthetic_fixture=True,chunk_bytes=16)))
'''
  root=self.f.base.base.base.base.worker;config=self.f.base.base.base.base.config;args=[sys.executable,'-c',code,str(root),json.dumps(config)]
  child=subprocess.run(args+['kill'],capture_output=True,timeout=20);self.assertEqual(child.returncode,-signal.SIGKILL,child.stderr)
  with self.f.fixture.queue._db() as db:self.assertEqual(db.execute("SELECT COUNT(*) FROM viewer_worker_chunk_permits WHERE state='finished'").fetchone()[0],1)
  child=subprocess.run(args+['replay'],capture_output=True,text=True,timeout=30);self.assertEqual(child.returncode,0,child.stderr);self.assertEqual(json.loads(child.stdout)['state'],'sealed_no_receipt')
 def test_partial_body_leaves_durable_active_permit_and_holds(self):
  import hashlib,time
  self.assertEqual(self.request('begin',extra={'X-Chunk-Size':'16'})[0],200)
  payload=b'x'*16;sha=hashlib.sha256(payload).hexdigest()
  extra={'X-Chunk-Index':'0','X-Chunk-Offset':'0','X-Chunk-SHA256':sha}
  conn=http.client.HTTPSConnection('127.0.0.1',self.f.fixture.server.server_port,context=self.f.fixture.client,timeout=5)
  try:
   conn.putrequest('POST','/ltds/worker/v1/viewer-claims/'+self.job+'/chunks-v2/part-0')
   for key,value in {'Authorization':'Bearer '+'a'*32,'X-Claim-Token':self.token,'Content-Type':'application/octet-stream','X-Chunk-Version':'2','Content-Length':'16',**extra}.items():conn.putheader(key,value)
   conn.endheaders();conn.send(payload[:1])
  finally:conn.close()
  deadline=time.monotonic()+5;row=None
  while time.monotonic()<deadline:
   with self.f.fixture.queue._db() as db:row=db.execute("SELECT state FROM viewer_worker_chunk_permits WHERE job_id=? AND operation_id='v2-0'",(self.job,)).fetchone()
   if row:break
   time.sleep(.05)
  self.assertIsNotNone(row);self.assertEqual(row['state'],'active')
  self.assertEqual(self.request('status-0',extra={**extra,'X-Chunk-Length':'16'})[0],409)
  self.assertEqual(self.request('begin',extra={'X-Chunk-Size':'16'})[0],409)
 def test_wrong_claim_rejected_before_body_and_nonce_not_consumed(self):
  import hashlib
  self.assertEqual(self.request('begin',extra={'X-Chunk-Size':'16'})[0],200)
  payload=b'x'*16;sha=hashlib.sha256(payload).hexdigest()
  conn=http.client.HTTPSConnection('127.0.0.1',self.f.fixture.server.server_port,context=self.f.fixture.client,timeout=5)
  try:
   conn.putrequest('POST','/ltds/worker/v1/viewer-claims/'+self.job+'/chunks-v2/part-0')
   for key,value in {'Authorization':'Bearer '+'a'*32,'X-Claim-Token':'wrong','Content-Type':'application/octet-stream','X-Chunk-Version':'2','Content-Length':'16','X-Chunk-Index':'0','X-Chunk-Offset':'0','X-Chunk-SHA256':sha}.items():conn.putheader(key,value)
   conn.endheaders();self.assertEqual(conn.getresponse().status,409)
  finally:conn.close()
  ident=self.f.identity();v2=self.api.viewer_v2_chunks
  permit=v2.admit(self.job,ident,self.token,0,0,16,sha)
  with self.assertRaises(Exception):v2.write_admitted(self.job,ident,self.token,0,0,payload,'wrong')
  self.assertEqual(v2.write_admitted(self.job,ident,self.token,0,0,payload,permit['permit'])['schemaVersion'],2)
 def test_hold_after_admission_keeps_operation_held(self):
  import hashlib,time
  self.assertEqual(self.request('begin',extra={'X-Chunk-Size':'16'})[0],200)
  payload=b'x'*16;sha=hashlib.sha256(payload).hexdigest()
  conn=http.client.HTTPSConnection('127.0.0.1',self.f.fixture.server.server_port,context=self.f.fixture.client,timeout=5)
  try:
   conn.putrequest('POST','/ltds/worker/v1/viewer-claims/'+self.job+'/chunks-v2/part-0')
   for key,value in {'Authorization':'Bearer '+'a'*32,'X-Claim-Token':self.token,'Content-Type':'application/octet-stream','X-Chunk-Version':'2','Content-Length':'16','X-Chunk-Index':'0','X-Chunk-Offset':'0','X-Chunk-SHA256':sha}.items():conn.putheader(key,value)
   conn.endheaders();deadline=time.monotonic()+5;row=None
   while time.monotonic()<deadline:
    with self.f.fixture.queue._db() as db:row=db.execute("SELECT state FROM viewer_worker_chunk_permits WHERE job_id=? AND operation_id='v2-0'",(self.job,)).fetchone()
    if row:break
    time.sleep(.05)
   self.assertIsNotNone(row);self.assertEqual(row['state'],'active')
   self.assertEqual(self.f.fixture.claims.mutation_hold(self.job,self.f.identity(),self.token)['state'],'held')
   conn.send(payload);self.assertEqual(conn.getresponse().status,409)
  finally:conn.close()
  with self.f.fixture.queue._db() as db:self.assertEqual(db.execute("SELECT state FROM viewer_worker_chunk_permits WHERE job_id=? AND operation_id='v2-0'",(self.job,)).fetchone()[0],'active')
 def test_v2_promotion_and_actual_viewer_http_replay(self):
  import threading
  from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
  from processing.config import Config
  from processing.viewer_service import ViewerService
  from processing.viewer_http import ViewerFacade
  self.f.spool.synthetic_v2_viewer=True
  with DaemonStateLock(self.f.base.base.base.base.worker) as owner:
   worker=self.f.base.worker(owner);receipt=send(worker,synthetic_fixture=True,chunk_bytes=16,promote=True)
   self.assertEqual(receipt['kind'],'viewer-coordinator-archive-receipt')
   self.assertEqual(send(worker,synthetic_fixture=True,chunk_bytes=16,promote=True),receipt)
  self.assertEqual(self.f.spool.lookup(self.job,self.f.identity(),self.token)['receipt'],receipt)
  service=ViewerService(self.f.fixture.queue,Config('legacy','http://127.0.0.1:1','http://127.0.0.1:2',1000,100000,viewer_enabled=True),store=self.f.fixture.store,engines={},viewer_spool=self.f.spool)
  self.addCleanup(service.close);facade=ViewerFacade(service,'viewer-token','staging-token')
  class Handler(BaseHTTPRequestHandler):
   def do_GET(self):
    if not facade.handle(self):self.send_error(404)
   def log_message(self,*args):pass
  server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.05});thread.start()
  try:
   conn=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=5)
   try:
    conn.request('GET','/task/22222222-2222-4222-8222-222222222222/download/all.zip?token=viewer-token');response=conn.getresponse()
    self.assertEqual(response.status,200);self.assertEqual(response.read(),self.f.base.base.archive)
   finally:conn.close()
  finally:server.shutdown();server.server_close();thread.join(5);self.assertFalse(thread.is_alive())
  self.assertEqual(self.f.fixture.queue.get(self.job)['status'],'running')
  allocation=next(self.f.root.glob('*.alloc'))
  with allocation.open('r+b') as stream:stream.write(b'X');stream.flush()
  with self.assertRaises(Exception):self.f.spool.lookup(self.job,self.f.identity(),self.token)
 def test_hold_during_v2_promotion_hash_blocks_receipt(self):
  import threading
  from concurrent.futures import ThreadPoolExecutor
  from unittest.mock import patch
  from processing.viewer_chunk_v2_journal import SyntheticV2ChunkJournal
  from processing.viewer_archive_spool import SpoolHeld
  self.f.spool.synthetic_v2_viewer=True
  with DaemonStateLock(self.f.base.base.base.base.worker) as owner:send(self.f.base.worker(owner),synthetic_fixture=True,chunk_bytes=16)
  entered=threading.Event();release=threading.Event();original=SyntheticV2ChunkJournal.finish
  def paused(journal,count):
   entered.set()
   if not release.wait(5):raise RuntimeError('barrier timed out')
   return original(journal,count)
  count=(len(self.f.base.base.archive)+15)//16
  with patch.object(SyntheticV2ChunkJournal,'finish',paused):
   with ThreadPoolExecutor(max_workers=2) as pool:
    future=pool.submit(self.api.viewer_v2_chunks.promote,self.job,self.f.identity(),self.token,count)
    try:
     self.assertTrue(entered.wait(5));hold=pool.submit(self.f.fixture.claims.mutation_hold,self.job,self.f.identity(),self.token)
     self.assertEqual(hold.result(timeout=2)['state'],'held')
    finally:release.set()
    with self.assertRaises(Exception):future.result(timeout=5)
  with self.f.fixture.queue._db() as db:self.assertIsNone(db.execute('SELECT 1 FROM viewer_worker_spool WHERE job_id=?',(self.job,)).fetchone())
 def test_sender_kill_before_and_after_v2_promotion_replays_exact_receipt(self):
  from pathlib import Path
  self.f.spool.synthetic_v2_viewer=True;receipt_path=Path(self.f.fixture.root)/'synthetic-v2-receipt.json'
  code='''import json,os,signal,sys
from pathlib import Path
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
import processing.viewer_synthetic_v2_sender as sender
root,config,mode,path=sys.argv[1:];original=sender.strict_response
def observe(response):
 value=original(response)
 if mode=='before' and isinstance(value,dict) and value.get('state')=='sealed_no_receipt':os.kill(os.getpid(),signal.SIGKILL)
 if mode=='after' and isinstance(value,dict) and value.get('kind')=='viewer-coordinator-archive-receipt':
  fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
  try:os.write(fd,json.dumps(value,sort_keys=True).encode());os.fsync(fd)
  finally:os.close(fd)
  os.kill(os.getpid(),signal.SIGKILL)
 return value
with DaemonStateLock(root) as owner:
 with patch.object(sender,'strict_response',observe):print(json.dumps(sender.send(ViewerNodeEnrollment(owner,**json.loads(config)),synthetic_fixture=True,chunk_bytes=16,promote=True)))
'''
  root=self.f.base.base.base.base.worker;config=self.f.base.base.base.base.config;args=[sys.executable,'-c',code,str(root),json.dumps(config)]
  child=subprocess.run(args+['before',str(receipt_path)],capture_output=True,timeout=30);self.assertEqual(child.returncode,-signal.SIGKILL,child.stderr)
  with self.f.fixture.queue._db() as db:self.assertIsNone(db.execute('SELECT 1 FROM viewer_worker_spool WHERE job_id=?',(self.job,)).fetchone())
  child=subprocess.run(args+['after',str(receipt_path)],capture_output=True,timeout=30);self.assertEqual(child.returncode,-signal.SIGKILL,child.stderr)
  expected=json.loads(receipt_path.read_text());child=subprocess.run(args+['replay',str(receipt_path)],capture_output=True,text=True,timeout=30)
  self.assertEqual(child.returncode,0,child.stderr);self.assertEqual(json.loads(child.stdout),expected)
  self.assertEqual(self.f.spool.lookup(self.job,self.f.identity(),self.token)['receipt'],expected)
 def test_active_mutation_blocks_v2_promotion_and_reader(self):
  self.f.spool.synthetic_v2_viewer=True
  with DaemonStateLock(self.f.base.base.base.base.worker) as owner:send(self.f.base.worker(owner),synthetic_fixture=True,chunk_bytes=16)
  count=(len(self.f.base.base.archive)+15)//16
  with self.f.fixture.queue._db() as db:db.execute("INSERT INTO viewer_worker_permits VALUES(?,?,'upload','active')",(self.job,'v2-active'))
  with self.assertRaises(Exception):self.api.viewer_v2_chunks.promote(self.job,self.f.identity(),self.token,count)
  with self.f.fixture.queue._db() as db:
   self.assertIsNone(db.execute('SELECT 1 FROM viewer_worker_spool WHERE job_id=?',(self.job,)).fetchone())
   db.execute("UPDATE viewer_worker_permits SET state='finished' WHERE job_id=? AND operation_id='v2-active'",(self.job,))
  self.api.viewer_v2_chunks.promote(self.job,self.f.identity(),self.token,count)
  with self.f.fixture.queue._db() as db:db.execute("INSERT INTO viewer_worker_permits VALUES(?,?,'upload','active')",(self.job,'v2-later'))
  with self.assertRaises(Exception):self.f.spool.lookup(self.job,self.f.identity(),self.token)
