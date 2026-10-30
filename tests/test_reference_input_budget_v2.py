import copy
import json
from pathlib import Path
import unittest
from processing import reference_protocol as p
from processing.reference_input_budget_v2 import plan_input_budget, MAX_OBJECTS

class InputBudgetTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/input-budget-v2-fixture.json').read_text(encoding='utf-8'))
        self.proposal = self.fixture['proposal']

    def test_shared_vector_including_empty_and_partial_range(self):
        result = plan_input_budget(self.proposal)
        self.assertEqual(result, self.fixture['expected'])
        self.assertEqual((result['requiredRequests'], result['requestBudget'], result['byteBudget']), (4, 7, 41))

    def test_identity_and_object_changes_bind_digest(self):
        baseline = plan_input_budget(self.proposal)['proposalSha256']
        for field, value in [('jobId','other'), ('generation',8), ('environment','production'), ('reservationId','other'), ('manifestSha256','e'*64), ('capabilityFingerprint','f'*64), ('expiresAt','2031-01-01T00:00:00Z'), ('retryRequests',4), ('rangeBytes',15)]:
            changed = copy.deepcopy(self.proposal)
            changed[field] = value
            self.assertNotEqual(baseline, plan_input_budget(changed)['proposalSha256'], field)
        changed = copy.deepcopy(self.proposal)
        changed['objects'][1]['sha256'] = 'e'*64
        self.assertNotEqual(baseline, plan_input_budget(changed)['proposalSha256'])

    def test_unsafe_or_duplicate_values_fail_closed(self):
        cases = []
        for field, value in [('protocolVersion',True), ('rangeBytes',0), ('rangeBytes',16*1024*1024+1), ('retryRequests',True), ('retryBytes',-1), ('expiresAt','badZ'), ('unknown',1)]:
            changed = copy.deepcopy(self.proposal); changed[field] = value; cases.append(changed)
        changed = copy.deepcopy(self.proposal); changed['objects'][1]['id'] = 'empty'; cases.append(changed)
        changed = copy.deepcopy(self.proposal); changed['objects'][1]['byteSize'] = p.MAX_SAFE_INTEGER; cases.append(changed)
        changed = copy.deepcopy(self.proposal); changed['retryRequests'] = p.MAX_SAFE_INTEGER; cases.append(changed)
        changed = copy.deepcopy(self.proposal); changed['objects'] = [changed['objects'][0]] * (MAX_OBJECTS+1); cases.append(changed)
        for case in cases:
            with self.subTest(case=list(case)), self.assertRaises(p.ProtocolError): plan_input_budget(case)

    def test_above_v1_cap(self):
        self.proposal['objects'] = [{'id':f'obj-{i}', 'byteSize':0, 'sha256':'a'*64} for i in range(10001)]
        self.assertEqual(plan_input_budget(self.proposal)['requiredRequests'],10001)

if __name__ == '__main__': unittest.main()
