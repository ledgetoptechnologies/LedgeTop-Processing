"""Synthetic metadata-only 10k real Viewer broker/Processing renewal test."""
import copy,json,os,subprocess,time,unittest,uuid
from pathlib import Path
from processing import reference_protocol as p
from processing.source_range_transfer import validate_source_grant
from tests.source_renewal_10k_support import RealViewerSourceRenewalTests,digest

class Real10k(unittest.TestCase):
 def test_real_issue_renew_replay_reopen_10k(self):
  viewer=Path(os.environ['LEDGETOP_VIEWER_SOURCE_DIR']);self.assertEqual(subprocess.check_output(['git','rev-parse','HEAD'],cwd=viewer,text=True).strip(),'4bb8d33ade8c253ee8a6b21ef1607c01e620c34d');self.assertFalse(subprocess.check_output(['git','status','--porcelain'],cwd=viewer,text=True).strip())
  h=RealViewerSourceRenewalTests('runTest');h.setUp();self.addCleanup(h.doCleanups);start=time.monotonic();host=h.start(inject=False);job=h.queued_job(host)
  self.assertEqual(len(job['submission']['manifest']['objects']),10000)
  submission_bytes=len(p.canonical_json(job['submission']));transfer_bytes=len(p.canonical_json(job['transferRequest']));self.assertLess(submission_bytes,p.MAX_BODY_BYTES)
  grant=host.control(op='dispatch',transferRequest=job['transferRequest'])['grant'];verified=validate_source_grant(grant,job=job,clock=h.clock);self.assertEqual(len(verified.objects),10000)
  h.advance(host,600);renewal={'protocol':'source-range-v2-renewal','renewalId':'renewal-'+uuid.uuid4().hex,'sequence':1,'previousGrantSha256':digest(grant),'previousGrant':grant}
  request_bytes=len(p.canonical_json(renewal));status,response=h.client(host).renew_grant(renewal);self.assertEqual(status,200,response);self.assertEqual(len(response['grant']['objectIds']),10000);self.assertEqual(h.client(host).renew_grant(renewal),(200,response))
  host.stop();host=h.start(inject=False);self.assertEqual(h.client(host).renew_grant(renewal),(200,response));self.assertEqual(len(h.usage(host)[0]),3)
  long=copy.deepcopy(job['submission'])
  for index,obj in enumerate(long['manifest']['objects']):
   obj['id']='i'+str(index).zfill(5)+'x'*122
   obj['name']='i'+str(index).zfill(5)+'x'*245+'.jpg'
   obj['mediaType']='a'*125+'/x'
   p._slug(obj['id'],'object');p._basename(obj['name']);p._string(obj['mediaType'],'mediaType',127)
  long['manifest']['sha256']=p.manifest_sha256(long['manifest'])
  long_size=len(p.canonical_json(long));self.assertGreater(long_size,p.MAX_BODY_BYTES)
  with self.assertRaisesRegex(p.ProtocolError,'body exceeds 4 MiB'):p.validate_submission(long,environment='staging',capability_fingerprint='0'*64)
  print('synthetic_long_ids_submission_bytes',long_size)
  print('synthetic_10k_sizes',json.dumps({'submission':submission_bytes,'transfer':transfer_bytes,'grant':len(p.canonical_json(grant)),'renewal':request_bytes,'seconds':round(time.monotonic()-start,2)}))
