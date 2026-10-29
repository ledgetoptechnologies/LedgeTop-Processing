"""Original option-definition metadata, never execution or source authority.

The private coordinator response is bound to an already persisted original
claim. The worker still reads its actual engine's options immediately before
the first engine mutation. No flag names or definition schemas are frozen here.
"""
from . import reference_protocol as p

MAX_EVIDENCE_BYTES = 512 * 1024
MAX_DEFINITION_BYTES = 262144
FIELDS = frozenset(('schemaVersion', 'authorityId', 'jobId', 'generation',
    'environment', 'nodeId', 'workerId', 'workerInstanceId', 'claimRequestId',
    'manifestSha256', 'pipelineDigest', 'capabilityFingerprint',
    'optionsFingerprint', 'catalogSha256', 'selectedDefinitions'))


class OptionEvidenceHeld(ValueError):
    pass


def validate_option_evidence(value, *, claim, claim_request_id, authority_id,
        worker_id, worker_instance_id, fresh_definitions):
    """Check original binding and exact selected definition bytes; no I/O."""
    try:
        p._closed(value, FIELDS, 'original option evidence')
        if type(value['schemaVersion']) is not int or value['schemaVersion'] != 1:
            raise ValueError()
        if len(p.canonical_json(value)) > MAX_EVIDENCE_BYTES:
            raise ValueError()
        expected = dict(authorityId=authority_id, workerId=worker_id,
            workerInstanceId=worker_instance_id, claimRequestId=claim_request_id,
            **{key: claim[key] for key in ('jobId', 'generation', 'environment',
                'nodeId', 'manifestSha256', 'pipelineDigest')},
            capabilityFingerprint=claim['submission']['capabilityFingerprint'],
            optionsFingerprint=claim['submission']['pipeline']['odm']['optionsFingerprint'])
        p._integer(value['generation'], 'generation')
        for key, original in expected.items():
            if type(value[key]) is not type(original) or value[key] != original:
                raise ValueError()
        p._hash(value['catalogSha256'], 'catalog digest')
        selected = value['selectedDefinitions']
        if type(selected) is not list or len(selected) > 2048:
            raise ValueError()
        names = sorted(claim['submission']['pipeline']['odm']['options'])
        if (any(type(item) is not dict or type(item.get('name')) is not str for item in selected)
                or [item['name'] for item in selected] != names
                or len(p.canonical_json(selected)) > MAX_DEFINITION_BYTES
                or type(fresh_definitions) is not dict):
            raise ValueError()
        fresh = [fresh_definitions[name] for name in names]
        if p.canonical_json(selected) != p.canonical_json(fresh):
            raise ValueError()
        return True
    except (ValueError, TypeError, KeyError, RecursionError):
        raise OptionEvidenceHeld('original_engine_option_evidence_unavailable') from None
