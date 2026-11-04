import copy
import hashlib
import unittest
import uuid

from processing import reference_protocol as p
from processing.source_range_transfer import CHUNK_BYTES, build_source_transfer_request, validate_source_grant, TransferError
from processing.source_grant_authority_v3_candidate import AuthorityGrantHeld, held_range_request_id, inspect_held


class AuthoritySourceGrantTests(unittest.TestCase):
    def setUp(self):
        self.attempt=str(uuid.uuid4());self.jobid=str(uuid.uuid4())
        obj={'id':'photo-1','name':'photo.jpg','byteSize':3,'sha256':hashlib.sha256(b'abc').hexdigest(),'kind':'image','mediaType':'image/jpeg'}
        manifest={'id':'dataset-fixture','revision':1,'totalBytes':3,'objectCount':1,'inputCounts':{'images':1,'lidarObjects':0,'points':0},'objects':[obj]}
        manifest['sha256']=p.manifest_sha256(manifest)
        self.job={'jobId':self.jobid,'generation':1,'environment':'production','manifestSha256':manifest['sha256'],
                  'sourceViewerAttemptId':self.attempt,'submission':{'manifest':manifest}}
        v2=build_source_transfer_request(self.job,reservation_id='reserve-1',expires_at='2035-01-01T00:00:00.000Z',nonce='nonce-1',clock=lambda:100)
        self.binding={'coordinatorAuthorityId':str(uuid.uuid4()),'executionEpoch':str(uuid.uuid4()),'activationSha256':'a'*64}
        self.trusted={**self.binding,'jobId':self.jobid,'generation':1,'environment':'production','viewerAttemptId':self.attempt}
        self.transfer={**v2,**self.binding,'protocol':'source-range-v3'}
        self.job['transferRequest']=self.transfer
        self.grant={**self.transfer,'expiresAt':'2034-01-01T00:00:00.000Z','jobId':self.jobid,'environment':'production','token':'x'*43}

    def inspect(self,grant=None,job=None,trusted=None,prior=()):
        return inspect_held(grant or self.grant,job=job or self.job,trusted=trusted or self.trusted,prior_identity_wires=prior,clock=lambda:100)

    def test_held_exact_replay_and_request_identity(self):
        first=self.inspect();again=self.inspect(prior=(first['identityJson'],))
        self.assertEqual(first['identitySha256'],again['identitySha256'])
        self.assertEqual(held_range_request_id(first,first['objects'][0],0,3),held_range_request_id(again,again['objects'][0],0,3))
        self.assertEqual(first['state'],'held')

    def test_authority_epoch_and_activation_changes_refused(self):
        for key in self.binding:
            bad=copy.deepcopy(self.grant);bad[key]='b'*64 if key=='activationSha256' else str(uuid.uuid4())
            with self.subTest(key=key),self.assertRaises(AuthorityGrantHeld):self.inspect(grant=bad)
            bad_job=copy.deepcopy(self.job);bad_job['transferRequest'][key]=bad[key]
            with self.subTest(job=key),self.assertRaises(AuthorityGrantHeld):self.inspect(job=bad_job)

    def test_cross_job_replay_and_new_epoch_cannot_reset_lineage(self):
        old=self.inspect()
        other=dict(self.trusted,jobId=str(uuid.uuid4()))
        with self.assertRaises(AuthorityGrantHeld):self.inspect(trusted=other)
        next_binding=dict(self.binding,executionEpoch=str(uuid.uuid4()))
        next_job=copy.deepcopy(self.job);next_job['transferRequest'].update(next_binding)
        next_grant=dict(self.grant,**next_binding);next_trusted=dict(self.trusted,**next_binding)
        with self.assertRaises(AuthorityGrantHeld):self.inspect(grant=next_grant,job=next_job,trusted=next_trusted,prior=(old['identityJson'],))

    def test_legacy_validator_and_closed_shapes_remain_unchanged(self):
        with self.assertRaises(TransferError):validate_source_grant(self.grant,job=self.job,clock=lambda:100)
        bad=dict(self.grant,unexpected=True)
        with self.assertRaises(AuthorityGrantHeld):self.inspect(grant=bad)
        old_job=copy.deepcopy(self.job);old_grant=copy.deepcopy(self.grant)
        for obj in (old_job['transferRequest'],old_grant):
            for key in self.binding:obj.pop(key)
            obj['protocol']='source-range-v2'
        self.assertEqual(validate_source_grant(old_grant,job=old_job,clock=lambda:100).identity.viewer_attempt_id,self.attempt)


if __name__=='__main__': unittest.main()
