import copy,io,json,sqlite3,subprocess,sys,unittest,uuid
from pathlib import Path
from processing import reference_protocol as p
from processing.prepared_envelope_jcs_candidate import PROTOCOL
from processing.viewer_dual_digest_binding_candidate import VERSION,PROVENANCE_FIELDS,BindingHeld,initialize,stage
from processing.reference_store import ReferenceStore
from tests.test_paged_snapshot_signature_candidate import PagedSnapshotSignatureTests
from tests.test_actual_viewer_gcp_stream_pair import ViewerGcpPair,decoded


class ViewerDualDigestTests(unittest.TestCase):
 statement_hash=PagedSnapshotSignatureTests.statement_hash
 @classmethod
 def setUpClass(cls):ViewerGcpPair.setUpClass();cls.viewer=ViewerGcpPair.x
 def setUp(self):
  PagedSnapshotSignatureTests.setUp(self)
  x=self.viewer;env=copy.deepcopy(self.env);env['submission']['submissionId']=str(uuid.uuid4());env['viewerTaskId']=x['taskId'];env['viewerAttemptId']=x['attemptId'];env['activationId']=str(uuid.uuid4())
  job,_=self.store.prepare_viewer_envelope_jcs_v3(json.dumps(env).encode(),authenticated_principal='viewer-a',authenticated_environment='staging',trusted_policy=self.policy,expected_authority_id=self.queue.authority_id)
  found=self.store.find_viewer_envelope_v2(env['submission']['submissionId'],authenticated_principal='viewer-a',authenticated_environment='staging',expected_protocol=PROTOCOL,expected_authority_id=self.queue.authority_id)
  self.submission=env['submission']['submissionId']
  keys=[k for k in PROVENANCE_FIELDS if k.startswith(('gcpEvidence','gcpFile'))]
  provenance={k:copy.deepcopy(x[k]) for k in keys}
  provenance.update(viewerDatasetId=x['datasetId'],viewerDatasetManifestSha256=x['datasetManifestSha256'],viewerTaskId=x['taskId'],viewerAttemptId=x['attemptId'],viewerInputManifestSha256=x['inputManifestSha256'],sourcePageHashes=x['sourcePageHashes'],sourcePageBytes=[len(v)-1 for v in decoded(x['sourceFrames']).splitlines(keepends=True)],sourceCount=x['sourceCount'],selectedPageHashes=x['selectedPageHashes'],selectedPageBytes=[len(v)-1 for v in decoded(x['selectedFrames']).splitlines(keepends=True)],selectedCount=x['selectedCount'])
  self.provenance=provenance
  self.statement=dict(version=VERSION,schemaVersion=1,principalId='viewer-a',environment='staging',coordinatorAuthorityId=self.queue.authority_id,submissionId=self.submission,jobId=job['jobId'],generation=job['generation'],activationId=env['activationId'],preparedEnvelopeSha256=found['envelopeSha256'],transportManifestSha256=env['submission']['manifest']['sha256'],pipelineDigest=p.digest(env['submission']['pipeline']),**provenance)
  with self.queue._db() as db:initialize(db)
 def call(self,statement=None,provenance=None,fault=None):
  return stage(self.store,self.submission,statement or self.statement,principal='viewer-a',environment='staging',configured_authority=self.queue.authority_id,synthetic_viewer_provenance=provenance or self.provenance,_fault=fault)
 def test_distinct_actual_viewer_and_reference_commitments_replay(self):
  from tests.test_gcp_synthetic_atomic_candidate import AtomicGcp
  from processing.viewer_selected_frames_candidate import verify_dataset_count_v2
  AtomicGcp.x=self.viewer;bundle=AtomicGcp().bundle();binding=bundle['trusted_binding']
  reconstruction=verify_dataset_count_v2(io.BytesIO(bundle['streams']['source']),io.BytesIO(bundle['streams']['selection']),source_hashes=binding['sourcePageHashes'],source_count=binding['sourceCount'],selected_hashes=binding['selectedPageHashes'],input_manifest_sha256=binding['inputManifestSha256'],processing_closure=bundle['processing_closure'],processing_pages=bundle['processing_pages'],closure_sha256=binding['processingClosureSha256'],trusted_binding=binding,expected_binding_sha256=bundle['expected_binding_sha256'])
  self.assertEqual(reconstruction['inputManifestSha256'],self.statement['viewerInputManifestSha256'])
  self.assertNotEqual(self.statement['viewerDatasetManifestSha256'],self.statement['transportManifestSha256'])
  self.assertTrue(self.call()['created']);self.store=ReferenceStore(self.queue,jcs_runtime_enabled=True)
  self.assertFalse(self.call()['created'])
 def test_swapped_collapsed_or_changed_viewer_input_rejected(self):
  for mode in ('swap','collapse','input','pin'):
   s=copy.deepcopy(self.statement)
   if mode=='swap':s['viewerDatasetManifestSha256'],s['transportManifestSha256']=s['transportManifestSha256'],s['viewerDatasetManifestSha256']
   elif mode=='collapse':s['viewerDatasetManifestSha256']=s['transportManifestSha256']
   elif mode=='input':s['viewerInputManifestSha256']='f'*64
   else:s['sourcePageHashes'][0]='f'*64
   with self.subTest(mode=mode),self.assertRaises(BindingHeld):self.call(s)
  with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM synthetic_viewer_dataset_transport_v1').fetchone()[0],0)
 def test_late_failure_rolls_back_binding(self):
  def fault():raise sqlite3.OperationalError('synthetic late failure')
  with self.assertRaises(BindingHeld):self.call(fault=fault)
  with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM synthetic_viewer_dataset_transport_v1').fetchone()[0],0)
  self.assertTrue(self.call()['created'])
 def test_exact_replay_in_new_process(self):
  self.call();arg=dict(root=str(self.queue.path.parent),submission=self.submission,statement=self.statement,authority=self.queue.authority_id,provenance=self.provenance)
  self.queue.close()
  script='''import json,sys
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE
from processing.viewer_dual_digest_binding_candidate import stage
a=json.load(sys.stdin);q=Queue(a['root']);q.allowed_profiles.add(PROFILE)
try:print(json.dumps(stage(ReferenceStore(q,jcs_runtime_enabled=True),a['submission'],a['statement'],principal='viewer-a',environment='staging',configured_authority=a['authority'],synthetic_viewer_provenance=a['provenance'])))
finally:q.close()
'''
  result=subprocess.run([sys.executable,'-c',script],input=json.dumps(arg),capture_output=True,text=True,timeout=20)
  self.assertEqual(result.returncode,0,msg=result.stderr)
  self.assertFalse(json.loads(result.stdout)['created'])
