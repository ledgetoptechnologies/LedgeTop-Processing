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
