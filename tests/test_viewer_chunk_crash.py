"""Synthetic process-kill coverage for chunk promotion; no live services."""
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import ssl
import subprocess
import sys
import tempfile
import time
import unittest

from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.viewer_worker_claim import ViewerWorkerClaims
from processing.viewer_archive_spool import ViewerArchiveSpool,SpoolHeld
from processing.viewer_chunk_spool import SyntheticChunkSpool

@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux allocation required')
class ChunkCrashTests(unittest.TestCase):
    def test_coordinator_kill_after_promotion_commit_leaves_receipt_and_restart_hold(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);ready=root/'ready.json'
            code='''import hashlib,json,os,signal,sys
from pathlib import Path
from tests.test_viewer_chunk_spool import ChunkSpoolTests
root=Path(sys.argv[1]);test=ChunkSpoolTests('test_exact_coordinator_chunk_replay_and_final_receipt');test.setUp()
f=test.base.fixture;test.chunk.begin(test.job,test.identity,test.token);test.chunk.write(test.job,test.identity,test.token,0,0,test.archive);test.chunk.finish(test.job,test.identity,test.token,1)
f.api.viewer_chunks=test.chunk
original=test.chunk.promote
def kill_after_commit(*args,**kwargs):
 value=original(*args,**kwargs)
 os.kill(os.getpid(),signal.SIGKILL)
 return value
test.chunk.promote=kill_after_commit
(root/'ready.json').write_text(json.dumps(dict(root=str(f.root),spool=str(test.base.root),identity=test.base.root_identity,job=test.job,token=test.token,port=f.server.server_port,cert=str(f.cert),sha=hashlib.sha256(test.archive).hexdigest(),length=len(test.archive),nodeOutput=str(test.base.base.base.base.base.output),nodeInputs=str(test.base.base.base.base.inputs))))
import time
while True:time.sleep(1)
'''
            env={**os.environ,'TMPDIR':temporary,'PYTHONDONTWRITEBYTECODE':'1'}
            child=subprocess.Popen([sys.executable,'-c',code,temporary],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True,start_new_session=True)
            try:
                deadline=time.monotonic()+15
                while not ready.exists() and time.monotonic()<deadline:
                    if child.poll() is not None:self.fail('coordinator fixture exited: '+child.stderr.read())
                    time.sleep(.05)
                self.assertTrue(ready.exists());info=json.loads(ready.read_text())
                ctx=ssl.create_default_context(cafile=info['cert'])
                conn=http.client.HTTPSConnection('127.0.0.1',info['port'],context=ctx,timeout=8)
                try:
                    headers={'Authorization':'Bearer '+'a'*32,'X-Claim-Token':info['token'],'X-Chunk-Count':'1','Content-Type':'application/octet-stream'}
                    with self.assertRaises(Exception):
                        conn.request('POST','/ltds/worker/v1/viewer-claims/'+info['job']+'/chunks/promote',b'',headers)
                        conn.getresponse().read()
                finally:conn.close()
                self.assertEqual(child.wait(timeout=5),-signal.SIGKILL)
                state=Path(info['root'])/'state'
                queue=Queue(state,node_ids=('cluster','main'),viewer_enabled=True,reference_enabled=True)
                try:
                    store=TaskStore(queue,state/'viewer-tasks')
                    claims=ViewerWorkerClaims(queue,store,local_max_photos=1000,local_max_input_bytes=100000,node_environments={'cluster':('production','staging'),'main':('production','staging')})
                    spool=ViewerArchiveSpool(claims,root=info['spool'],root_identity=info['identity'],max_archive_bytes=65536,enabled=True)
                    identity=dict(node_id='cluster',worker_id='worker-a',worker_instance_id='instance-a')
                    job=queue.get(info['job']);self.assertEqual(job['status'],'held');self.assertEqual(job['heldReason'],'coordinator_restart')
                    receipt=spool.lookup(info['job'],identity,info['token'])['receipt']
                    self.assertEqual((receipt['archiveSha256'],receipt['archiveBytes']),(info['sha'],info['length']))
                    with self.assertRaises(SpoolHeld):SyntheticChunkSpool(spool,synthetic_fixture=True).promote(info['job'],identity,info['token'],1)
                    with queue._db() as db:
                        self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_spool WHERE job_id=?',(info['job'],)).fetchone()[0],1)
                        self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_chunk_sessions WHERE job_id=?',(info['job'],)).fetchone()[0],1)
                    self.assertEqual(len(list(Path(info['spool']).glob('*.alloc'))),1)
                    self.assertEqual(len(list(Path(info['nodeOutput']).glob('*.alloc'))),1)
                    self.assertEqual(len(list(Path(info['nodeInputs']).glob('*.alloc'))),1)
                finally:queue.close()
            finally:
                if child.poll() is None:os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=5)
                if child.stderr:child.stderr.close()
