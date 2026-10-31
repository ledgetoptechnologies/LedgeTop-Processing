"""Explicitly guarded, isolated >1-GiB synthetic v2 probe; not a production test."""
import hashlib,http.client,json,os,resource,stat,struct,threading,time,zlib
from email.message import Message
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing.config import Config
from processing.direct_output_slot import _identity
from processing.viewer_archive_spool import ViewerArchiveSpool
from processing.viewer_chunk_v2_spool import SyntheticV2Spool
from processing.viewer_http import ViewerFacade
from processing.viewer_local_result import capture
from processing.viewer_service import NativeEngine,ViewerService
from processing.viewer_synthetic_submission import submit
from processing.viewer_synthetic_v2_sender import send
from tests.test_viewer_node_enrollment import NodeEnrollmentTests
from tests.test_viewer_synthetic_submission import SyntheticSubmissionTests

WORK=Path('/work');PAYLOAD=1025*1024*1024

def guard():
 info=WORK.lstat(); marker=WORK/'owned-probe-marker'
 if (os.geteuid()==0 or not os.path.ismount(WORK) or not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid()
     or stat.S_IMODE(info.st_mode)!=0o700 or not marker.is_file() or marker.is_symlink()
     or marker.read_text()!=os.environ.get('OWNED_PROBE_ID') or set(os.listdir('/sys/class/net'))!={'lo'}):
  raise RuntimeError('isolated owned work mount and network-none required')
 memory=Path('/sys/fs/cgroup/memory.max').read_text().strip();cpu=Path('/sys/fs/cgroup/cpu.max').read_text().split()
 if memory=='max' or int(memory)>768*1024*1024 or len(cpu)!=2 or cpu[0]=='max' or int(cpu[0])*2>int(cpu[1]):raise RuntimeError('resource limits required')
 v=os.statvfs(WORK)
 if v.f_bavail*v.f_frsize<6*1024**3:raise RuntimeError('insufficient scratch space')
 if os.environ.get('TMPDIR')!='/work':raise RuntimeError('temporary root mismatch')

class SyntheticZip:
 def __init__(self):
  name=b'synthetic-zeroes.bin';block=b'\0'*(1024*1024);crc=0
  for _ in range(PAYLOAD//len(block)):crc=zlib.crc32(block,crc)
  crc &= 0xffffffff
  self.prefix=struct.pack('<IHHHHHIIIHH',0x04034b50,20,0,0,0,0,crc,PAYLOAD,PAYLOAD,len(name),0)+name
  central=struct.pack('<IHHHHHHIIIHHHHHII',0x02014b50,20,20,0,0,0,0,crc,PAYLOAD,PAYLOAD,len(name),0,0,0,0,0,0)+name
  self.tail=central+struct.pack('<IHHHHIIH',0x06054b50,0,0,1,1,len(central),len(self.prefix)+PAYLOAD,0)
  self.length=len(self.prefix)+PAYLOAD+len(self.tail);self.position=0;self.headers=Message();self.headers['Content-Length']=str(self.length)
 def read(self,amount):
  if amount<=0:return b''
  remaining=min(amount,self.length-self.position);out=bytearray()
  while remaining:
   if self.position<len(self.prefix):part=self.prefix[self.position:min(len(self.prefix),self.position+remaining)]
   elif self.position<len(self.prefix)+PAYLOAD:part=b'\0'*min(remaining,len(self.prefix)+PAYLOAD-self.position)
   else:
    offset=self.position-len(self.prefix)-PAYLOAD;part=self.tail[offset:offset+remaining]
   if not part:raise RuntimeError('synthetic stream stalled')
   out.extend(part);self.position+=len(part);remaining-=len(part)
  return bytes(out)
 def close(self):pass

def allocated(path,expected):
 st=path.stat()
 if st.st_size!=expected or st.st_blocks*512<expected:raise RuntimeError('physical allocation not evidenced')
 return st.st_blocks*512

def main():
 guard();start=time.monotonic();stream=SyntheticZip();budget=stream.length
 print('synthetic_archive_bytes='+str(budget),flush=True)
 original=NodeEnrollmentTests.setUp
 def setup(enrollment):
  original(enrollment);enrollment.config['output_budget']=budget;enrollment.config['max_budget']=budget
 fixture=SyntheticSubmissionTests('test_exact_submission_once_and_replay_without_cleanup')
 try:
  with patch.object(NodeEnrollmentTests,'setUp',setup):fixture.setUp()
  coordinator=fixture.base.base.fixture
  root=coordinator.root/'v2-large-spool';root.mkdir(mode=0o700);fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
  try:root_identity=_identity(fd)
  finally:os.close(fd)
  spool=ViewerArchiveSpool(coordinator.claims,root=root,root_identity=root_identity,max_archive_bytes=budget,enabled=True,synthetic_v2_viewer=True)
  coordinator.api.viewer_v2_chunks=SyntheticV2Spool(spool,synthetic_fixture=True)
  with DaemonStateLock(fixture.base.base.worker) as owner:
   inputs=fixture.prepare(owner);engine=NativeEngine(fixture.origin);submit(inputs,engine,synthetic_fixture=True)
   with patch.object(NativeEngine,'download',return_value=stream):record=capture(inputs.worker,engine,synthetic_fixture=True)
   node=next(fixture.base.base.output.glob('*.alloc'));node_allocated=allocated(node,budget)
   print('node_capture_complete',flush=True)
   receipt=send(inputs.worker,synthetic_fixture=True,chunk_bytes=1024*1024,promote=True)
   if receipt['archiveBytes']!=budget or receipt['archiveSha256']!=record['archiveSha256']:raise RuntimeError('receipt mismatch')
  coordinator_allocated=allocated(next(root.glob('*.alloc')),budget)
  print('tls_transfer_and_promotion_complete',flush=True)
  service=ViewerService(coordinator.queue,Config('legacy','http://127.0.0.1:1','http://127.0.0.1:2',1000,100000,viewer_enabled=True),store=coordinator.store,engines={},viewer_spool=spool)
  facade=ViewerFacade(service,'viewer-synthetic','viewer-staging')
  class Handler(BaseHTTPRequestHandler):
   def do_GET(self):
    if not facade.handle(self):self.send_error(404)
   def log_message(self,*_args):pass
  server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.05});thread.start()
  try:
   conn=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=30)
   try:
    conn.request('GET','/task/22222222-2222-4222-8222-222222222222/download/all.zip?token=viewer-synthetic');r=conn.getresponse()
    if r.status!=200:raise RuntimeError('Viewer download failed')
    digest=hashlib.sha256();count=0
    while True:
     chunk=r.read(1024*1024)
     if not chunk:break
     count+=len(chunk);digest.update(chunk)
    if count!=budget or digest.hexdigest()!=record['archiveSha256']:raise RuntimeError('Viewer bytes mismatch')
   finally:conn.close()
  finally:server.shutdown();server.server_close();thread.join(5);service.close()
  with coordinator.queue._db() as db:
   permits=db.execute("SELECT COUNT(*) FROM viewer_worker_chunk_permits WHERE job_id=? AND operation_id LIKE 'v2-%' AND state='finished'",(coordinator.job,)).fetchone()[0]
   if permits!=(budget+1024*1024-1)//(1024*1024):raise RuntimeError('chunk count mismatch')
  result=dict(result='synthetic_v2_large_success',archiveBytes=budget,sha256=record['archiveSha256'],chunkCount=permits,nodeAllocatedBytes=node_allocated,spoolAllocatedBytes=coordinator_allocated,maxRssKiB=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,elapsedSeconds=round(time.monotonic()-start,2),viewerCustodyAcknowledged=False)
  print(json.dumps(result,sort_keys=True),flush=True)
 finally:fixture.doCleanups()
if __name__=='__main__':main()
