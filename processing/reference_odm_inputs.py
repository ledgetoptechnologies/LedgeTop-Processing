"""Admit only images and the coordinator's exact V2 attempt-derived GCP list.

The claim is received over the authenticated coordinator channel. Its source
binding is the authority for the already verified Viewer attestation; this
module does not authenticate arbitrary caller-created metadata or file names.
"""
from copy import deepcopy

from . import reference_protocol as p


class ODMInputError(ValueError):
    pass


def validate_odm_input_closure(job, *, authority_id):
    """Return admitted objects without widening legacy image-only execution.

    The one auxiliary object must reconstruct the exact accepted derived
    manifest and match its frozen GCP snapshot. Ordinary auxiliary files,
    including a raw file named gcp_list.txt, never acquire execution authority.
    Scratch/transfer bytes still include every admitted object; image counts
    continue to come from the manifest's independently validated image count.
    """
    try:
        submission = job['submission']
        objects = submission['manifest']['objects']
        if type(objects) is not list:
            raise ValueError()
        # Preserve the existing all-image V1/V2 path and its caller validation.
        if all(type(obj) is dict and obj.get('kind') == 'image' for obj in objects):
            return deepcopy(objects)
        if (type(job.get('sourceIntakeVersion')) is not int or job['sourceIntakeVersion'] != 2
                or type(submission.get('sourceIntakeVersion')) is not int or submission['sourceIntakeVersion'] != 2
                or job.get('sourceState') != 'retained' or job.get('derivedState') != 'retained'
                or submission['pipeline']['odm']['requested'] is not True
                or submission['pipeline']['treedar']['requested'] is not False):
            raise ValueError()
        from .source_acceptance_binding import validate_source_acceptance_binding
        from .reference_source_intake import _validate_derived_manifest
        binding = validate_source_acceptance_binding(job.get('sourceAcceptanceBinding'), job)
        p._uuid(authority_id, 'configured coordinator authority')
        if (binding['coordinatorAuthorityId'] != authority_id
                or job.get('attemptId') != binding['viewerAttemptId']
                or any(job.get(field) != binding[field] for field in
                       ('sourceManifestSha256', 'derivedManifestSha256', 'sourceReceiptSha256', 'derivedReceiptSha256'))):
            raise ValueError()
        derived = job['derivedReceipt']
        p._closed(derived, ('schemaVersion', 'receiptType', 'receiptId', 'jobId', 'attemptId', 'generation',
                           'environment', 'derivedManifestSha256', 'objects', 'retainedAt'), 'derived receipt')
        if (type(derived['schemaVersion']) is not int or derived['schemaVersion'] != 1
                or derived['receiptType'] != 'attempt_derived_input'
                or p.digest(derived) != binding['derivedReceiptSha256']
                or derived['derivedManifestSha256'] != binding['derivedManifestSha256']
                or any(derived[key] != expected for key, expected in
                       (('jobId', job['jobId']), ('attemptId', job['attemptId']),
                        ('generation', job['generation']), ('environment', job['environment'])))):
            raise ValueError()
        p._uuid(derived['receiptId'], 'derived receipt ID')
        p._string(derived['retainedAt'], 'derived receipt retention timestamp')
        manifest = dict(schemaVersion=1, attemptId=job['attemptId'],
                        sha256=derived['derivedManifestSha256'], objects=derived['objects'])
        admitted = _validate_derived_manifest(manifest, job['attemptId'])
        if len(admitted) != 1:
            raise ValueError()
        auxiliary = [obj for obj in objects if type(obj) is dict and obj.get('kind') != 'image']
        if (len(auxiliary) != 1 or len(objects) < 2
                or any(type(obj) is not dict or obj.get('kind') not in ('image', 'auxiliary') for obj in objects)
                or len({obj['id'] for obj in objects}) != len(objects)
                or len({obj['name'].casefold() for obj in objects}) != len(objects)):
            raise ValueError()
        obj = auxiliary[0]
        frozen = admitted.get(obj['id'])
        if (frozen is None or frozen['byteSize'] <= 0 or obj['kind'] != 'auxiliary' or obj['mediaType'] != 'text/plain'
                or any(obj[key] != frozen[key] for key in ('id', 'name', 'byteSize', 'sha256'))):
            raise ValueError()
        # The raw source receipt is independent of the attempt-derived closure.
        # In particular a retained raw GCP CSV cannot be relabeled as this file.
        source = job['sourceReceipt']
        if (type(source) is not dict or p.digest(source) != binding['sourceReceiptSha256']
                or source.get('receiptType') != 'source_retention'
                or source.get('retentionGuarantee') != 'until_explicit_source_release_v1'
                or source.get('sourceManifestSha256') != binding['sourceManifestSha256']
                or source.get('pipelineManifestSha256') != job['manifestSha256']
                or source.get('pipelineDigest') != job['pipelineDigest']
                or any(source.get(key) != expected for key, expected in
                       (('jobId', job['jobId']), ('attemptId', job['attemptId']),
                        ('generation', job['generation']), ('environment', job['environment'])))
                or type(source.get('objects')) is not list
                or any(type(item) is not dict or item.get('id') == obj['id'] for item in source['objects'])):
            raise ValueError()
        return deepcopy(objects)
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
        raise ODMInputError('odm_input_closure_unavailable') from None
