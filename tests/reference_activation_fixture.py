"""Synthetic trusted Viewer binding for tests that exercise post-activation code.

This helper does not authenticate a real Viewer and is never used by runtime.
Tests of missing or invalid activation must call the store directly instead.
"""
import uuid


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
