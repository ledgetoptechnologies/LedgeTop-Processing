import base64,copy,hashlib,json,sqlite3,subprocess,sys,unittest,uuid
from processing import reference_protocol as p
from processing.prepared_envelope_jcs_candidate import PROTOCOL
from processing.viewer_selected_frames_candidate import binding_digest
from processing.viewer_dual_gcp_atomic_candidate import initialize,stage,AtomicHeld
from processing.viewer_dual_digest_binding_candidate import initialize as initialize_binding
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE
from tests.test_viewer_dual_digest_binding_candidate import ViewerDualDigestTests
from tests.test_gcp_synthetic_atomic_candidate import AtomicGcp


class ViewerDualGcpAtomicTests(ViewerDualDigestTests):
 def setUp(self):
  super().setUp();self.queue.close();self.queue=Queue(self.temp.name+'/isolated');self.addCleanup(self.queue.close);self.queue.allowed_profiles.add(PROFILE);self.store=ReferenceStore(self.queue,jcs_runtime_enabled=True)
  with self.queue._db() as db:initialize_binding(db)
  self.statement['coordinatorAuthorityId']=self.queue.authority_id
  AtomicGcp.x=self.viewer;bundle=AtomicGcp().bundle();bundle.pop('bundle_id')
  env=copy.deepcopy(self.env);env['submission']['submissionId']=str(uuid.uuid4());env['viewerTaskId']=self.viewer['taskId'];env['viewerAttemptId']=self.viewer['attemptId'];env['activationId']=str(uuid.uuid4())
  manifest=env['submission']['manifest'];objects=[obj for page in bundle['processing_pages'] for obj in page['body']['objects']]
  manifest['objects']=objects;manifest['objectCount']=len(objects);manifest['totalBytes']=sum(x['byteSize'] for x in objects);manifest['inputCounts']=dict(images=sum(x['kind']=='image' for x in objects),lidarObjects=sum(x['kind']=='lidar' for x in objects),points=0);manifest['sha256']=p.manifest_sha256(manifest)
  job,_=self.store.prepare_viewer_envelope_jcs_v3(json.dumps(env).encode(),authenticated_principal='viewer-a',authenticated_environment='staging',trusted_policy=self.policy,expected_authority_id=self.queue.authority_id)
  found=self.store.find_viewer_envelope_v2(env['submission']['submissionId'],authenticated_principal='viewer-a',authenticated_environment='staging',expected_protocol=PROTOCOL,expected_authority_id=self.queue.authority_id)
  self.submission=env['submission']['submissionId'];self.statement.update(submissionId=self.submission,jobId=job['jobId'],generation=job['generation'],activationId=env['activationId'],preparedEnvelopeSha256=found['envelopeSha256'],transportManifestSha256=manifest['sha256'],pipelineDigest=p.digest(env['submission']['pipeline']))
  b=bundle['trusted_binding'];b.update(datasetId=self.statement['viewerDatasetId'],datasetManifestSha256=self.statement['viewerDatasetManifestSha256'],principalId='viewer-a',environment='staging',authorityId=self.queue.authority_id,viewerTaskId=self.statement['viewerTaskId'],viewerAttemptId=self.statement['viewerAttemptId'],pipelineDigest=self.statement['pipelineDigest'])
  bundle['expected_binding_sha256']=binding_digest(b);self.bundle=bundle
  with self.queue._db() as db:initialize(db)
 def atomic(self,bundle=None,statement=None,fault=None):
  return stage(self.store,self.submission,statement or self.statement,principal='viewer-a',environment='staging',configured_authority=self.queue.authority_id,synthetic_viewer_provenance=self.provenance,synthetic_bundle=bundle or self.bundle,_fault=fault)
 def counts(self):
  with self.queue._db() as db:return tuple(db.execute('SELECT (SELECT count(*) FROM synthetic_viewer_dataset_transport_v1),(SELECT count(*) FROM synthetic_gcp_bundle)').fetchone())
 def test_actual_viewer_commit_and_replay(self):
  self.assertNotEqual(self.statement['viewerDatasetManifestSha256'],self.statement['transportManifestSha256'])
  self.assertTrue(self.atomic()['created']);self.store=type(self.store)(self.queue,jcs_runtime_enabled=True)
  self.assertFalse(self.atomic()['created']);self.assertEqual(self.counts(),(1,1))
 def test_each_fault_and_truncated_content_roll_back(self):
  for phase in ('binding','source','selection','evidence','file','semantic'):
   def fault(at):
    if at==phase:raise sqlite3.OperationalError('synthetic fault')
   with self.subTest(phase=phase),self.assertRaises(AtomicHeld):self.atomic(fault=fault)
   self.assertEqual(self.counts(),(0,0))
  for name in ('source','selection','evidence','file'):
   bundle=copy.deepcopy(self.bundle);bundle['streams'][name]=bundle['streams'][name][:-1]
   with self.subTest(stream=name),self.assertRaises(AtomicHeld):self.atomic(bundle)
   self.assertEqual(self.counts(),(0,0))
 def test_omitted_or_altered_transport_is_not_accepted(self):
  bundle=copy.deepcopy(self.bundle);bundle['processing_pages'][0]['body']['objects'].pop()
  with self.assertRaises(AtomicHeld):self.atomic(bundle)
  self.assertEqual(self.counts(),(0,0))
 def test_mutated_viewer_id_with_recomputed_pins_and_closure_rejected(self):
  from processing.viewer_selected_frames_candidate import DOMAIN
  from processing.large_paged_manifest_candidate import build,_digest
  bundle=copy.deepcopy(self.bundle);statement=copy.deepcopy(self.statement);provenance=copy.deepcopy(self.provenance)
  frame=json.loads(bundle['streams']['selection']);frame['records'][0]['pageObjectId']='forged-id'
  wire=p.canonical_json(frame);pin=hashlib.sha256(DOMAIN+wire).hexdigest()
  bundle['streams']['selection']=wire+b'\n';bundle['statement']['selectedPageHashes']=[pin];bundle['statement']['selectedPageBytes']=[len(wire)]
  statement['selectedPageHashes']=[pin];statement['selectedPageBytes']=[len(wire)];provenance['selectedPageHashes']=[pin];provenance['selectedPageBytes']=[len(wire)]
  objects=[obj for page in bundle['processing_pages'] for obj in page['body']['objects']];objects[0]['id']='forged-id'
  closure,pages=build(objects);bundle['processing_closure']=closure;bundle['processing_pages']=pages
  bundle['trusted_binding']['selectedPageHashes']=[pin];bundle['trusted_binding']['processingClosureSha256']=_digest(closure);bundle['expected_binding_sha256']=binding_digest(bundle['trusted_binding'])
  with self.assertRaises(AtomicHeld):stage(self.store,self.submission,statement,principal='viewer-a',environment='staging',configured_authority=self.queue.authority_id,synthetic_viewer_provenance=provenance,synthetic_bundle=bundle)
  self.assertEqual(self.counts(),(0,0))
 def test_generated_namespace_collision_and_classification_mutation_hold(self):
  from processing.viewer_paged_frame_candidate import DOMAIN
  for mode in ('collision','classification'):
   bundle=copy.deepcopy(self.bundle);statement=copy.deepcopy(self.statement);provenance=copy.deepcopy(self.provenance)
   frame=json.loads(bundle['streams']['source']);record=next(x for x in frame['records'] if x['type']=='dataset_file' and x['datasetFileId']=='photo-00000')
   if mode=='collision':record['datasetFileId']='gcp_'+statement['viewerAttemptId']
   else:record['kind']='auxiliary'
   wire=p.canonical_json(frame);pin=hashlib.sha256(DOMAIN+wire).hexdigest()
   bundle['streams']['source']=wire+b'\n';bundle['statement']['sourcePageHashes']=[pin];bundle['statement']['sourcePageBytes']=[len(wire)]
   statement['sourcePageHashes']=[pin];statement['sourcePageBytes']=[len(wire)];provenance['sourcePageHashes']=[pin];provenance['sourcePageBytes']=[len(wire)]
   bundle['trusted_binding']['sourcePageHashes']=[pin];bundle['expected_binding_sha256']=binding_digest(bundle['trusted_binding'])
   with self.subTest(mode=mode),self.assertRaises(AtomicHeld):stage(self.store,self.submission,statement,principal='viewer-a',environment='staging',configured_authority=self.queue.authority_id,synthetic_viewer_provenance=provenance,synthetic_bundle=bundle)
   self.assertEqual(self.counts(),(0,0))
 def test_process_reopen_exact_replay(self):
  self.atomic();bundle=copy.deepcopy(self.bundle);bundle['streams']={k:base64.b64encode(v).decode() for k,v in bundle['streams'].items()}
  data=dict(root=str(self.queue.path.parent),submission=self.submission,statement=self.statement,authority=self.queue.authority_id,provenance=self.provenance,bundle=bundle)
  self.queue.close()
  script='''import base64,json,sys
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE
from processing.viewer_dual_gcp_atomic_candidate import stage
a=json.load(sys.stdin);q=Queue(a['root']);q.allowed_profiles.add(PROFILE)
a['bundle']['streams']={k:base64.b64decode(v) for k,v in a['bundle']['streams'].items()}
try:print(json.dumps(stage(ReferenceStore(q,jcs_runtime_enabled=True),a['submission'],a['statement'],principal='viewer-a',environment='staging',configured_authority=a['authority'],synthetic_viewer_provenance=a['provenance'],synthetic_bundle=a['bundle'])))
finally:q.close()
'''
  result=subprocess.run([sys.executable,'-c',script],input=json.dumps(data),capture_output=True,text=True,check=True,timeout=20)
  self.assertFalse(json.loads(result.stdout)['created'])
