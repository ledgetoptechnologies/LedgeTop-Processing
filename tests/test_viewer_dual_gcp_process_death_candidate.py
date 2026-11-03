"""Actual child termination test against synthetic single-coordinator DB."""
import base64,copy,json,queue,subprocess,sys,threading,unittest
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE
from tests.test_viewer_dual_gcp_atomic_candidate import ViewerDualGcpAtomicTests

SCRIPT=r'''import base64,json,os,sys
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE
from processing.viewer_dual_gcp_atomic_candidate import stage
a=json.loads(sys.stdin.readline()); q=Queue(a['root']);q.allowed_profiles.add(PROFILE)
a['bundle']['streams']={k:base64.b64decode(v) for k,v in a['bundle']['streams'].items()}
def fault(phase):
 if phase==a['phase']:
  print('ready',flush=True);sys.stdin.read(1)
stage(ReferenceStore(q,jcs_runtime_enabled=True),a['submission'],a['statement'],principal='viewer-a',environment='staging',configured_authority=a['authority'],synthetic_viewer_provenance=a['provenance'],synthetic_bundle=a['bundle'],_fault=fault)
os._exit(73)
'''

class RealTermination(ViewerDualGcpAtomicTests):
 def test_real_kill_every_phase_then_retry_and_post_commit_restart(self):
  bundle=copy.deepcopy(self.bundle);bundle['streams']={k:base64.b64encode(v).decode() for k,v in bundle['streams'].items()}
  root=str(self.queue.path.parent);data=dict(root=root,submission=self.submission,statement=self.statement,authority=self.queue.authority_id,provenance=self.provenance,bundle=bundle)
  self.queue.close()
  for phase in ('binding','source','selection','evidence','file','semantic'):
   payload=json.dumps(dict(data,phase=phase))+'\n'
   child=subprocess.Popen([sys.executable,'-c',SCRIPT],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
   try:
    child.stdin.write(payload);child.stdin.flush()
    output=queue.Queue();reader=threading.Thread(target=lambda:output.put(child.stdout.readline()),daemon=True);reader.start()
    self.assertEqual(output.get(timeout=20).strip(),'ready',msg=phase)
    child.kill();child.wait(timeout=10);reader.join(timeout=5);self.assertFalse(reader.is_alive());self.assertNotEqual(child.returncode,0)
   finally:
    if child.poll() is None:child.kill();child.wait(timeout=10)
    child.communicate(timeout=5)
   self.queue=Queue(root);self.queue.allowed_profiles.add(PROFILE);self.store=ReferenceStore(self.queue,jcs_runtime_enabled=True)
   self.assertEqual(self.counts(),(0,0),msg=phase);self.queue.close()
  # A successful transaction exits abruptly after commit without sending a response.
  result=subprocess.run([sys.executable,'-c',SCRIPT],input=json.dumps(dict(data,phase='none'))+'\n',capture_output=True,text=True,timeout=30)
  self.assertEqual(result.returncode,73,msg=result.stderr)
  self.queue=Queue(root);self.queue.allowed_profiles.add(PROFILE);self.store=ReferenceStore(self.queue,jcs_runtime_enabled=True);self.addCleanup(self.queue.close)
  self.assertEqual(self.counts(),(1,1));self.assertFalse(self.atomic()['created']);self.assertEqual(self.counts(),(1,1))
