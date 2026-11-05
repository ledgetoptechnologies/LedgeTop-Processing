import copy
import unittest

from processing import reference_protocol as p
from processing.source_acceptance_binding import validate_source_acceptance_binding


class SourceAcceptanceBindingTests(unittest.TestCase):
    def setUp(self):
        uid = lambda n: f'00000000-0000-4000-8000-{n:012d}'
        self.binding = dict(coordinatorAuthorityId=uid(1), viewerPrincipalId='viewer-staging',
            submissionId=uid(2), activationId=uid(3), viewerTaskId=uid(4), viewerAttemptId=uid(2),
            providerTaskId=uid(5), preparedEnvelopeSha256='a'*64, jobId=uid(5), generation=1,
            environment='staging', inputManifestSha256='b'*64, pipelineDigest='c'*64,
            sourceManifestSha256='d'*64, derivedManifestSha256='e'*64,
            sourceReceiptSha256='f'*64, derivedReceiptSha256='0'*64)
        self.job = dict(sourceIntakeVersion=2, jobId=uid(5), generation=1, environment='staging',
            manifestSha256='b'*64, pipelineDigest='c'*64, authorityId=uid(1), attemptId=uid(2))

    def test_closed_binding_is_detached_and_has_no_unsealed_result_digest(self):
        result = validate_source_acceptance_binding(self.binding, self.job)
        self.assertEqual(result, self.binding)
        result['viewerPrincipalId'] = 'changed'
        self.assertEqual(self.binding['viewerPrincipalId'], 'viewer-staging')
        with self.assertRaises(p.ProtocolError):
            validate_source_acceptance_binding({**self.binding, 'resultManifestSha256': '1'*64}, self.job)

    def test_job_authority_generation_attempt_and_manifest_cannot_drift(self):
        for key in ('jobId', 'generation', 'environment', 'manifestSha256', 'pipelineDigest', 'authorityId', 'attemptId'):
            changed = {**self.job, key: 2 if key == 'generation' else 'other'}
            with self.subTest(key=key), self.assertRaises(p.ProtocolError):
                validate_source_acceptance_binding(self.binding, changed)
        for version in (True, 1, None, '2'):
            with self.subTest(version=version), self.assertRaises(p.ProtocolError):
                validate_source_acceptance_binding(self.binding, {**self.job, 'sourceIntakeVersion': version})

    def test_unrelated_or_malformed_source_identity_is_rejected(self):
        for key, value in (('extra', True), ('generation', True), ('viewerAttemptId', self.binding['activationId']),
                           ('providerTaskId', self.binding['activationId']), ('sourceReceiptSha256', 'not-a-hash'),
                           ('viewerPrincipalId', 'viewer/other'), ('activationId', 'not-a-uuid')):
            with self.subTest(key=key), self.assertRaises(p.ProtocolError):
                validate_source_acceptance_binding({**self.binding, key: value}, self.job)
        missing = copy.deepcopy(self.binding)
        del missing['sourceReceiptSha256']
        with self.assertRaises(p.ProtocolError):
            validate_source_acceptance_binding(missing, self.job)
