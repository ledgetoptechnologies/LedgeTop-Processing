import copy
import os
import unittest
import uuid

from processing.direct_worker_boundary import DirectBoundaryDenied
from processing.direct_worker_grant_v2 import DirectWorkerGrant
from processing.scratch_policy import ScratchLedger, PolicyConflict
from tests.test_direct_worker_receiver_v2 import WorkerV2Tests


@unittest.skipIf(os.name=='nt','Linux scratch fixture required')
class DirectWorkerGrantTests(unittest.TestCase):
    def test_exact_principal_binding_survives_ledger_reopen(self):
        f=WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        grant=DirectWorkerGrant(f.boundary,coordinator_principal='coord-a')
        payload=dict(operationId=str(uuid.uuid4()),item=copy.deepcopy(f.item))
        with self.assertRaises(DirectBoundaryDenied):grant.require('coord-a',f.headers)
        with self.assertRaises(DirectBoundaryDenied):grant.authorize('coord-b',f.headers,payload)
        result=grant.authorize('coord-a',f.headers,payload)
        self.assertFalse(result['sourceCustodyAccepted']);self.assertFalse(result['processingAuthorized'])
        self.assertEqual(grant.authorize('coord-a',f.headers,payload),result)
        changed=copy.deepcopy(payload);changed['item']['generation']+=1
        with self.assertRaises(DirectBoundaryDenied):grant.authorize('coord-a',f.headers,changed)
        changed=copy.deepcopy(payload);changed['operationId']=str(uuid.uuid4())
        with self.assertRaises(PolicyConflict):grant.authorize('coord-a',f.headers,changed)
        f.boundary.ledger=ScratchLedger(f.root/'private'/'ledger.sqlite',f.root,
            capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)
        grant.require('coord-a',f.headers)
        with self.assertRaises(DirectBoundaryDenied):grant.require('coord-b',f.headers)
