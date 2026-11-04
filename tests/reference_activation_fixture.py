"""Synthetic trusted Viewer binding for tests that exercise post-activation code.

This helper does not authenticate a real Viewer and is never used by runtime.
Tests of missing or invalid activation must call the store directly instead.
"""
import copy
import json
import uuid
from unittest.mock import patch


def activate_reference_job(store, job):
    stored = store.get(job['jobId'], private=True)
    submission = stored['submission']
    def fixture_uuid(label):
        return str(uuid.uuid5(uuid.NAMESPACE_URL, 'synthetic-viewer-' + label + '-' + job['jobId']))
    binding = dict(schemaVersion=1, protocol='ltds-viewer-activation-v1',
        activationId=fixture_uuid('activation'), principalId='synthetic-viewer',
        submissionId=submission['submissionId'], jobId=job['jobId'], generation=job['generation'],
        environment=job['environment'], viewerTaskId=fixture_uuid('task'),
        viewerAttemptId=fixture_uuid('attempt'), requestDigest=submission['requestDigest'],
        manifestSha256=job['manifestSha256'], pipelineDigest=job['pipelineDigest'])
    return store.activate_viewer_binding(binding, authenticated_principal='synthetic-viewer',
                                         authenticated_environment=job['environment'])


def prepare_reference_job(store, validated):
    return store.prepare_viewer_submission(validated, authenticated_principal='synthetic-viewer',
                                           authenticated_environment=validated['environment'])


def activated_prepared_v3_job(store, fixture, validated, *, engines=('odm',)):
    """Submit and activate through the authenticated prepared-v3 boundary.

    The store must have jcs_runtime_enabled. The source contract is patched only
    around activation, as in the prepared scheduler tests; a test that claims
    still needs its own synthetic future grant patch.
    """
    from processing.prepared_envelope_jcs_candidate import PROTOCOL
    from processing.reference_prepared_api_candidate import PreparedSubmissionAPI
    from processing.reference_store import ReferenceStore
    environment, authority = validated['environment'], store.queue.authority_id
    raw = {key: copy.deepcopy(validated[key]) for key in
           ('schemaVersion', 'submissionId', 'manifest', 'pipeline', 'capabilityFingerprint')}
    capabilities = fixture['capabilities']
    fingerprints = dict(capabilityFingerprint=capabilities['capabilityFingerprint'],
                        optionsFingerprint=capabilities['engines']['odm']['optionsFingerprint'],
                        availableEngines=list(engines))
    legacy = dict(fingerprints, allowedOdmOptions=['resolution'])
    policy = dict(fingerprints, optionDefinitions={'resolution': {'type': 'float', 'domain': {'min': -100, 'max': 100}}})
    token = 'synthetic-prepared-test-token-001'
    api = PreparedSubmissionAPI(store, credentials=[dict(token=token, principalId='viewer-a', environment=environment)],
                                legacy_bearer_tokens=[], policies={'staging': legacy, 'production': legacy},
                                jcs_policies={'staging': policy, 'production': policy}, expected_authority_id=authority)
    def post(path, body):
        return api.handle('POST', path, {'Authorization': 'Bearer ' + token, 'X-LTDS-Expected-Coordinator-Authority': authority},
                          json.dumps(body).encode(), protected_transport=True)
    envelope = dict(schemaVersion=3, protocol=PROTOCOL, submission=raw, viewerTaskId=str(uuid.uuid4()),
                    viewerAttemptId=str(uuid.uuid4()), activationId=str(uuid.uuid4()))
    response = post('/ltds/prepared/v3/submissions', envelope)
    if response.status != 201:
        raise AssertionError(response.body)
    found = store.find_viewer_envelope_v2(raw['submissionId'], authenticated_principal='viewer-a',
                                          authenticated_environment=environment, expected_protocol=PROTOCOL)
    activation = dict(activationId=envelope['activationId'], viewerTaskId=envelope['viewerTaskId'],
                      viewerAttemptId=envelope['viewerAttemptId'], envelopeSha256=found['envelopeSha256'])
    with patch.object(ReferenceStore, '_v3_source_contract_ready', return_value=True):
        result = post('/ltds/prepared/v3/submissions/' + raw['submissionId'] + '/activation', activation)
    if result.status != 200:
        raise AssertionError(result.body)
    return response.body
