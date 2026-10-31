import copy
import hashlib
import json
import unittest

from processing.direct_outbox_contract import require_same_binding, validate_binding
from processing.reference_protocol import ProtocolError


def binding():
    return dict(schemaVersion=1, authorityId='11111111-1111-4111-8111-111111111111',
        gateGeneration=1, gateReservationId='gate-1', nodeId='cluster', workerId='worker-1',
        engineId='engine-1', environment='production',
        viewerTaskId='22222222-2222-4222-8222-222222222222', viewerGeneration=1,
        jobId='33333333-3333-4333-8333-333333333333',
        engineTaskId='44444444-4444-4444-8444-444444444444',
        inputSetSha256='a'*64, outputBudgetBytes=1000000)


class DirectOutboxContractTests(unittest.TestCase):
    def test_binding_digest_is_canonical_and_order_independent(self):
        value=binding()
        expected=hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',', ':'),
                                         ensure_ascii=False).encode()).hexdigest()
        self.assertEqual(validate_binding(value),expected)
        self.assertEqual(validate_binding(dict(reversed(list(value.items())))),expected)
        self.assertEqual(require_same_binding(value,copy.deepcopy(value)),expected)

    def test_each_valid_identity_change_is_detected(self):
        value=binding()
        changes=dict(schemaVersion=2,authorityId='55555555-5555-4555-8555-555555555555',
            gateGeneration=2,gateReservationId='gate-2',nodeId='main',workerId='worker-2',
            engineId='engine-2',environment='staging',
            viewerTaskId='66666666-6666-4666-8666-666666666666',viewerGeneration=2,
            jobId='77777777-7777-4777-8777-777777777777',
            engineTaskId='88888888-8888-4888-8888-888888888888',
            inputSetSha256='b'*64,outputBudgetBytes=2000000)
        for field,new_value in changes.items():
            changed=copy.deepcopy(value);changed[field]=new_value
            with self.subTest(field=field),self.assertRaises(ProtocolError):
                require_same_binding(value,changed)

    def test_malformed_identity_and_untrusted_receipt_flags_are_rejected(self):
        for field,value in (('schemaVersion',True),('authorityId','NOT-A-UUID'),
            ('gateGeneration',False),('viewerGeneration',0),('outputBudgetBytes',0),
            ('nodeId','../main'),('environment','unknown'),('inputSetSha256','A'*64)):
            invalid=binding();invalid[field]=value
            with self.subTest(field=field),self.assertRaises(ProtocolError):validate_binding(invalid)
        for invalid in ({**binding(),'durable':True},
                        {k:v for k,v in binding().items() if k!='engineId'}):
            with self.assertRaises(ProtocolError):validate_binding(invalid)

    def test_mutating_inputs_after_validation_never_alters_prior_digest(self):
        value=binding(); previous=validate_binding(value);value['engineId']='other'
        self.assertNotEqual(previous,validate_binding(value))
