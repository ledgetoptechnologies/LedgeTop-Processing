import base64
import copy
import unittest
import uuid

from processing import reference_protocol as p
from processing.viewer_import_confirmation import ImportConfirmationRejected
from processing.viewer_odm_import_confirmation_scope_v3_candidate import DOMAIN,PROTOCOL,ScopedImportVerifier
from tests.test_viewer_odm_import_confirmation_v2 import OdmImportConfirmationV2Tests


class ScopedImportV3Tests(unittest.TestCase):
    def setUp(self):
        self.fixture=OdmImportConfirmationV2Tests('test_signed_viewer_assertions_are_not_prebound_processing_expectations')
        self.fixture.setUp()
        self.scope=dict(coordinatorAuthorityId=str(uuid.uuid4()),viewerPrincipalId='viewer-a',
                        activationId=str(uuid.uuid4()),preparedEnvelopeSha256='a'*64)
        self.verifier=ScopedImportVerifier(provisioned_scope=self.scope,provisioned_import_keys=self.fixture.import_keys,
            provisioned_copy_keys=self.fixture.copy_signer.trusted_keys,durable_expected=self.fixture.expected,
            durable_manifest=self.fixture.manifest,durable_accepted_envelopes=[self.fixture.envelope])
        self.envelope=dict(protocol=PROTOCOL,schemaVersion=3,issuerId=self.fixture.copy_signer.issuer_id,
            keyId='import-key',**self.scope,confirmationV2=copy.deepcopy(self.fixture.confirmation))
        self.sign()

    def sign(self,domain=DOMAIN):
        body={k:v for k,v in self.envelope.items() if k!='signature'}
        self.envelope['signature']=base64.urlsafe_b64encode(self.fixture.import_key.sign(domain+p.canonical_json(body))).decode().rstrip('=')

    def test_exact_signed_replay_is_held(self):
        first=self.verifier.inspect(self.envelope)
        self.assertEqual(first['state'],'held')
        self.assertEqual(first,self.verifier.inspect(self.envelope,prior_sha256=first['confirmationSha256']))
        self.envelope['confirmationV2']['modelId']='other-model'
        with self.assertRaises(ImportConfirmationRejected):self.verifier.inspect(self.envelope)

    def test_signed_wrong_scope_is_rejected(self):
        for key in self.scope:
            original=self.envelope[key]
            self.envelope[key]=('b'*64 if key=='preparedEnvelopeSha256' else
                                'other-principal' if key=='viewerPrincipalId' else str(uuid.uuid4()))
            self.sign()
            with self.subTest(key=key),self.assertRaisesRegex(ImportConfirmationRejected,'binding_mismatch'):
                self.verifier.inspect(self.envelope)
            self.envelope[key]=original
        self.sign()

    def test_forged_domain_or_inner_or_changed_replay_is_rejected(self):
        self.sign(domain=b'WRONG-DOMAIN\0')
        with self.assertRaisesRegex(ImportConfirmationRejected,'invalid_scoped_signature'):self.verifier.inspect(self.envelope)
        self.sign();self.envelope['confirmationV2']['viewerAttemptId']='other-attempt';self.sign()
        with self.assertRaises(ImportConfirmationRejected):self.verifier.inspect(self.envelope)
        self.envelope['confirmationV2']=copy.deepcopy(self.fixture.confirmation);self.sign()
        with self.assertRaisesRegex(ImportConfirmationRejected,'scoped_replay_changed'):
            self.verifier.inspect(self.envelope,prior_sha256='0'*64)

    def test_closed_shape_and_wrong_job_never_succeed(self):
        bad=dict(self.envelope,extra='x')
        with self.assertRaises(ImportConfirmationRejected):self.verifier.inspect(bad)
        expected=dict(self.fixture.expected,jobId='other-job')
        verifier=ScopedImportVerifier(provisioned_scope=self.scope,provisioned_import_keys=self.fixture.import_keys,
            provisioned_copy_keys=self.fixture.copy_signer.trusted_keys,durable_expected=expected,
            durable_manifest=self.fixture.manifest,durable_accepted_envelopes=[self.fixture.envelope])
        with self.assertRaises(ImportConfirmationRejected):verifier.inspect(self.envelope)


if __name__=='__main__':unittest.main()
