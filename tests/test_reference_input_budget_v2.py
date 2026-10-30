import copy
import json
from pathlib import Path
import unittest
from processing import reference_protocol as p
from processing.reference_input_budget_v2 import plan_input_budget, decode_budget_proposal, MAX_OBJECTS

class InputBudgetTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/input-budget-v2-fixture.json').read_text(encoding='utf-8-sig'))
        self.proposal = self.fixture['input']

    def test_shared_viewer_vector_including_empty_and_partial_range(self):
        result = plan_input_budget(self.proposal)
        self.assertEqual({k: result[k] for k in self.fixture['expected']}, self.fixture['expected'])
        for key in ('jobId','generation','environment','reservationId','manifestSha256','capabilityFingerprint','expiresAt'):
            self.assertEqual(result[key], self.proposal[key])

    def test_unsafe_or_duplicate_values_fail_closed(self):
        cases = []
        for field, value in [('protocolVersion',True), ('rangeBytes',0), ('rangeBytes',16*1024*1024+1), ('retryRequests',True), ('retryBytes',-1), ('expiresAt','2030-01-01T00:00:00Z'), ('unknown',1)]:
            changed = copy.deepcopy(self.proposal); changed[field] = value; cases.append(changed)
        changed = copy.deepcopy(self.proposal); changed['objects'][1]['id'] = 'empty'; cases.append(changed)
        changed = copy.deepcopy(self.proposal); changed['objects'][1]['byteSize'] = p.MAX_SAFE_INTEGER; cases.append(changed)
        changed = copy.deepcopy(self.proposal); changed['retryRequests'] = p.MAX_SAFE_INTEGER; cases.append(changed)
        changed = copy.deepcopy(self.proposal); changed['objects'] = [changed['objects'][0]] * (MAX_OBJECTS+1); cases.append(changed)
        for case in cases:
            with self.subTest(case=list(case)), self.assertRaises(p.ProtocolError): plan_input_budget(case)

    def test_above_v1_cap(self):
        self.proposal['objects'] = [{'id':f'obj-{i}', 'byteSize':0, 'sha256':'a'*64} for i in range(10001)]
        self.proposal['retryBytes'] = 0
        self.assertEqual(plan_input_budget(self.proposal)['requiredRequests'],10001)

    def test_canonical_raw_rejections(self):
        raw = p.canonical_json(self.proposal)
        self.assertEqual(decode_budget_proposal(raw), plan_input_budget(self.proposal))
        cases = [raw.replace(b'"protocolVersion":2', b'"protocolVersion":2.0'),
                 raw.replace(b'"protocolVersion":2', b'"protocolVersion":2e0'),
                 raw.replace(b'"retryBytes":5', b'"retryBytes":-0'),
                 raw.replace(b'"protocolVersion":2', b'"protocolVersion":2,"protocolVersion":2'),
                 raw.replace(b'2026-', b'0000-'), raw + b' ', raw[:-1] + b',"x":"\xff"}']
        for case in cases:
            with self.subTest(raw=case[:60]), self.assertRaises(p.ProtocolError): decode_budget_proposal(case)

    def test_retry_amplification(self):
        for field, value in [('retryRequests',9),('retryBytes',41)]:
            changed = copy.deepcopy(self.proposal); changed[field] = value
            with self.assertRaises(p.ProtocolError): plan_input_budget(changed)
        self.proposal['objects'] = [self.proposal['objects'][0]]
        self.proposal['retryBytes'] = 1
        with self.assertRaises(p.ProtocolError): plan_input_budget(self.proposal)

    def test_generation_zero(self):
        self.proposal['generation'] = 0
        self.assertEqual(plan_input_budget(self.proposal)['generation'], 0)

if __name__ == '__main__': unittest.main()
