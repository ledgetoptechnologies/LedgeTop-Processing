import copy,hashlib,json,unittest
from pathlib import Path
from processing import reference_protocol as p
from processing.reference_input_grant_v2_draft import verify_candidate_grant,GrantDraftError,DOMAIN

class GrantCandidateVectors(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.v=json.loads((Path(__file__).resolve().parents[1]/'contracts/input-v2-transfer-vectors.json').read_text(encoding='utf8'))
  cls.g=cls.v['canonicalGrant'];cls.raw=cls.g['canonicalUtf8'].encode('utf8')
 def test_literal_bytes_and_digest(self):
  self.assertEqual(p.canonical_json(self.g['payload']),self.raw)
  self.assertEqual(verify_candidate_grant(self.raw)['sha256'],self.g['sha256'])
  self.assertEqual(hashlib.sha256(DOMAIN+self.raw).hexdigest(),self.g['sha256'])
 def test_closed_schema_uuid_bounds_and_dates(self):
  cases=[('extra','unexpected'),('jobId','job-1'),('generation',0),('generation',9007199254740992),('rangeBytes',True),('issuedAt','2026-10-30T12:00:00Z'),('issuedAt','0000-01-01T00:00:00.000Z'),('expiresAt','2026-10-30T12:00:00.000+00:00'),('environment','prod'),('manifestSha256','A'*64)]
  for field,value in cases:
   candidate=copy.deepcopy(self.g['payload']);candidate[field]=value
   with self.subTest(field=field,value=value),self.assertRaises(GrantDraftError):verify_candidate_grant(p.canonical_json(candidate))
  candidate=copy.deepcopy(self.g['payload']);candidate['expiresAt']=candidate['issuedAt']
  with self.assertRaises(GrantDraftError):verify_candidate_grant(p.canonical_json(candidate))
 def test_raw_json_mutations_rejected(self):
  duplicate=self.raw[:-1]+b',"jobId":"11111111-2222-4333-8444-555555555555"}'
  for raw in (duplicate,b'\xef\xbb\xbf'+self.raw,b' '+self.raw,self.raw.replace(b'"protocolVersion":2',b'"protocolVersion":2.0'),self.raw.replace(b'"generation":1',b'"generation":1e0'),self.raw.replace(b'"generation":1',b'"generation":-0')):
   with self.subTest(raw=raw[:24]),self.assertRaises(GrantDraftError):verify_candidate_grant(raw)
 def test_shared_invalid_raw_vectors(self):
  for vector in self.v['invalidRawVectors']:
   with self.subTest(name=vector['name']),self.assertRaises(GrantDraftError):verify_candidate_grant(vector['utf8'].encode('utf8'))

 def test_range_and_budget_examples(self):
  v={x['name']:x for x in self.v['vectors']};r=self.v['rangeBytes']
  for name in ('empty-object','one-byte-beyond-range'):
   item=v[name];self.assertEqual(item['baseRequests'],max(1,(item['objectBytes']+r-1)//r));self.assertEqual(item['requestBudget'],item['baseRequests']+item['retryRequests']);self.assertEqual(item['byteBudget'],item['baseBytes']+item['retryBytes'])
  first=v['first-full-range-lost-insufficient-retry-bytes'];self.assertEqual(first['retry']['remainingBytes'],first['byteBudget']-first['firstRequest']['chargedBytes']);self.assertGreater(first['retry']['end']-first['retry']['start'],first['retry']['remainingBytes'])
  http=v['half-open-http-inclusive'];self.assertEqual(http['requestRange'],f"bytes={http['start']}-{http['endExclusive']-1}");self.assertEqual(http['contentRange'],f"bytes {http['start']}-{http['endExclusive']-1}/{http['objectBytes']}");self.assertEqual(http['contentLength'],http['endExclusive']-http['start'])
  lost=v['lost-last-byte-then-retry'];self.assertEqual(sum(x['chargeBytes'] for x in lost['requests']),lost['chargedBytes']);self.assertEqual(len(lost['requests']),lost['chargedRequests'])
  empty=v['zero-byte-http'];self.assertEqual((empty['rangeHeader'],empty['status'],empty['contentLength']),(None,200,0))
  replay=v['same-request-id-replay'];self.assertFalse(replay['replay']['emitBody']);self.assertEqual(replay['replay']['additionalChargeRequests'],0)
  expiry=v['expiry-observed-before-clock-rollback'];self.assertTrue(expiry['expiryObservationCommittedBeforeDenial']);self.assertEqual(expiry['newAdmission'],'deny')
  unknown=v['unknown-commit'];self.assertFalse(unknown['emitBody']);self.assertEqual(unknown['action'],'reconcile_then_fresh_request_and_charge')
  self.assertEqual(v['status-identity-mismatch']['result'],'deny')
  concurrent=v['concurrent-budget-precharge'];self.assertEqual(concurrent['acceptedCount']+concurrent['deniedCount'],len(concurrent['simultaneousRequests']));self.assertLessEqual(concurrent['chargedBytes'],concurrent['byteBudget'])
