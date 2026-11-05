"""Validate the coordinator's immutable source binding in a V2 worker claim.

The claim arrives over the authenticated worker API. This validates that its
source identity agrees with the job; it does not authenticate arbitrary caller
metadata or establish source custody on its own.
"""
from copy import deepcopy

from . import reference_protocol as p
from .viewer_copy_receipt_v2 import BINDING_FIELDS

SOURCE_BINDING_FIELDS = tuple(key for key in BINDING_FIELDS if key != 'resultManifestSha256')
HASH_FIELDS = ('preparedEnvelopeSha256', 'inputManifestSha256', 'pipelineDigest',
               'sourceManifestSha256', 'derivedManifestSha256',
               'sourceReceiptSha256', 'derivedReceiptSha256')


def validate_source_acceptance_binding(binding, job):
    p._closed(binding, SOURCE_BINDING_FIELDS, 'source acceptance binding')
    p._require(type(job) is dict and type(job.get('sourceIntakeVersion')) is int
               and job['sourceIntakeVersion'] == 2, 'V2 source claim required')
    for key in ('coordinatorAuthorityId', 'submissionId', 'activationId', 'viewerTaskId',
                'viewerAttemptId', 'providerTaskId', 'jobId'):
        p._uuid(binding[key], key)
    p._slug(binding['viewerPrincipalId'], 'Viewer principal')
    p._integer(binding['generation'], 'source binding generation')
    p._environment(binding['environment'])
    for key in HASH_FIELDS:
        p._hash(binding[key], key)
    p._require(binding['submissionId'] == binding['viewerAttemptId']
               and binding['providerTaskId'] == binding['jobId'], 'source attempt/job identity mismatch')
    expected = dict(jobId=job['jobId'], generation=job['generation'], environment=job['environment'],
                    inputManifestSha256=job['manifestSha256'], pipelineDigest=job['pipelineDigest'])
    p._require(all(binding[key] == value for key, value in expected.items()),
               'source acceptance job binding mismatch')
    if 'authorityId' in job:
        p._require(binding['coordinatorAuthorityId'] == job['authorityId'], 'source authority mismatch')
    if 'attemptId' in job:
        p._require(binding['viewerAttemptId'] == job['attemptId'], 'source attempt mismatch')
    return deepcopy(binding)
