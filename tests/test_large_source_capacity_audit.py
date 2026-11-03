"""Synthetic sizing only: does not admit work or read image bytes."""
import copy
import json
import unittest
from pathlib import Path
from processing import reference_protocol as p
from processing import prepared_envelope_jcs_candidate as jcs
from processing.source_range_transfer import build_source_transfer_request

class LargeSourceCapacityAudit(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
  cls.submission=copy.deepcopy(fixture['submission'])
  manifest=cls.submission['manifest']
  manifest['id']='dataset-synthetic'
  manifest['objects']=[dict(id=f'img-{i:05}',name=f'DJI_{i:05}.JPG',byteSize=9089354,sha256='a'*64,kind='image',mediaType='image/jpeg') for i in range(16832)]
  manifest['objectCount']=len(manifest['objects']);manifest['inputCounts']['images']=len(manifest['objects'])
  manifest['totalBytes']=sum(o['byteSize'] for o in manifest['objects']);manifest['sha256']=p.manifest_sha256(manifest)
 def test_real_v1_and_jcs_gates_hold_large_metadata(self):
  wire=p.canonical_json(self.submission)
  self.assertLess(len(wire),p.MAX_BODY_BYTES)
  with self.assertRaisesRegex(ValueError,'invalid object count'):
   p.validate_submission(self.submission,environment='staging',capability_fingerprint=self.submission['capabilityFingerprint'])
  with self.assertRaisesRegex(ValueError,'JSON node limit'):jcs._parse(wire)
 def test_builder_estimates_ranges_and_retry_budgets_but_does_not_admit(self):
  m=self.submission['manifest']
  job={'submission':self.submission,'manifestSha256':m['sha256'],'environment':'staging','generation':1}
  args=dict(reservation_id='reservation-1',expires_at='2030-01-01T00:00:00.000Z',nonce='nonce-1',clock=lambda:0)
  one=build_source_transfer_request(job,**args);retry=build_source_transfer_request(job,**args)
  self.assertEqual(one,retry)
  self.assertEqual(len(one['objectIds']),16832);self.assertEqual(one['requestBudget'],33664)
  self.assertEqual(one['attemptBudget'],100992);self.assertEqual(one['byteBudget'],3*m['totalBytes'])
  self.assertEqual(m['totalBytes'],152992006528)

if __name__=='__main__':unittest.main()
