import copy
import tempfile
import unittest
from pathlib import Path
from processing.reference_source_grant_v3_candidate import SyntheticRangeLedger, validate

class SourceGrantV3Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'ledger.sqlite'
        self.ledger = SyntheticRangeLedger(str(self.path)); self.addCleanup(self.ledger.close)
        self.grant = dict(schemaVersion=3, protocol='ltds-source-grant-v3',
            accounting=dict(environment='production',jobId='job-1',generation=1,viewerAttemptId='attempt-1',reservationId='reserve-1',manifestSha256='a'*64,sourceVersion='source-1'),
            authority=dict(activationDigest='b'*64,coordinatorAuthorityId='authority-1',executionEpoch='epoch-1'),
            policy=dict(chunkBytes=4,requestBudget=2,byteBudget=8,attemptBudget=1),
            expiresAt=300,bearer='synthetic-bearer',nonce='nonce-1',
            objects=[dict(id='photo-1',byteSize=8,sha256='c'*64)])
        self.state = {key:copy.deepcopy(self.grant[key]) for key in ('accounting','authority','policy','objects')}
    def charge(self, request='request-1', offset=0, attempt='attempt-a'):
        return self.ledger.charge(self.grant,trusted_state=self.state,now=100,
            request_id=request,attempt_id=attempt,object_id='photo-1',offset=offset,length=4)
    def test_epoch_rotation_keeps_spend_and_replay(self):
        one = self.charge()
        self.assertEqual((one['requests'],one['bytes']),(1,4))
        stale = copy.deepcopy(self.grant)
        self.state['authority']['executionEpoch'] = 'epoch-2'
        with self.assertRaises(ValueError): validate(stale,trusted_state=self.state,now=100)
        self.grant['authority']['executionEpoch'] = 'epoch-2'
        self.grant.update(expiresAt=400,bearer='renewed-bearer',nonce='nonce-2')
        replay = self.charge()
        self.assertFalse(replay['created']); self.assertEqual(replay['identity'],one['identity'])
        self.assertEqual((replay['requests'],replay['bytes']),(1,4))
        two = self.charge('request-2',4)
        self.assertEqual((two['requests'],two['bytes']),(2,8))
        with self.assertRaises(ValueError): self.charge('request-3')
    def test_auth_and_binding_fail_closed(self):
        for section, key, val in [('authority','activationDigest','f'*64),('authority','coordinatorAuthorityId','other'),('authority','executionEpoch','other'),('accounting','sourceVersion','other')]:
            grant=copy.deepcopy(self.grant); grant[section][key]=val
            with self.assertRaises(ValueError): validate(grant,trusted_state=self.state,now=100)
        for key in ('bearer','nonce','expiresAt','objects','policy'):
            grant=copy.deepcopy(self.grant); del grant[key]
            with self.assertRaises(ValueError): validate(grant,trusted_state=self.state,now=100)
        with self.assertRaises(ValueError): validate(self.grant,trusted_state=self.state,now=300)
    def test_closure_budget_attempt_and_request_id(self):
        self.charge()
        with self.assertRaises(ValueError): self.charge(offset=4)
        with self.assertRaises(ValueError): self.charge('request-2',4,'attempt-b')
        with self.assertRaises(ValueError): self.ledger.charge(self.grant,trusted_state=self.state,now=100,request_id='other',attempt_id='attempt-a',object_id='unknown',offset=0,length=4)
        with self.assertRaises(ValueError): self.ledger.charge(self.grant,trusted_state=self.state,now=100,request_id='other',attempt_id='attempt-a',object_id='photo-1',offset=4,length=5)
    def test_restart_retains_counts(self):
        self.charge(); self.ledger.close()
        self.ledger = SyntheticRangeLedger(str(self.path)); self.addCleanup(self.ledger.close)
        self.grant['authority']['executionEpoch']='epoch-2';self.state['authority']['executionEpoch']='epoch-2'
        result=self.charge('request-2',4)
        self.assertEqual((result['requests'],result['bytes']),(2,8))

if __name__ == '__main__': unittest.main()
