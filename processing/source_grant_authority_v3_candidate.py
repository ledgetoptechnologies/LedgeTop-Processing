"""Unmounted source-range-v3 identity proposal, permanently held.

No authentication, signed activation verification, transport, ledger migration or
source fetching is implemented here. The `trusted` argument is a placeholder for
independently authenticated coordinator/activation evidence, never HTTP input.
"""
import copy
import hashlib
import json

from . import reference_protocol as p
from .source_range_transfer import _request_id, validate_source_grant

PROTOCOL='source-range-v3'
BINDING=('coordinatorAuthorityId','executionEpoch','activationSha256')
TRUSTED=('coordinatorAuthorityId','executionEpoch','activationSha256',
         'jobId','generation','environment','viewerAttemptId')


class AuthorityGrantHeld(ValueError):
    pass


def inspect_held(grant, *, job, trusted, prior_identity_wires=(), clock=None):
    """Validate and derive proposed range IDs; never authorize source fetch."""
    try:
        p._closed(trusted, TRUSTED, 'independent coordinator evidence')
        for key in ('coordinatorAuthorityId','executionEpoch','viewerAttemptId'):
            p._uuid(trusted[key],key)
        p._hash(trusted['activationSha256'],'activation digest')
        p._uuid(trusted['jobId'],'job ID');p._integer(trusted['generation'],'generation')
        p._environment(trusted['environment'])
        if type(job) is not dict or type(grant) is not dict or type(job.get('transferRequest')) is not dict:
            raise ValueError('closed job and grant required')
        transfer=job['transferRequest']
        if transfer.get('protocol')!=PROTOCOL or grant.get('protocol')!=PROTOCOL:
            raise ValueError('v3 protocol required')
        for key in BINDING:
            if grant.get(key)!=trusted[key] or transfer.get(key)!=trusted[key]:
                raise ValueError('independent authority binding differs')
        if ((job.get('jobId'),job.get('generation'),job.get('environment'),job.get('sourceViewerAttemptId')) !=
                (trusted['jobId'],trusted['generation'],trusted['environment'],trusted['viewerAttemptId'])):
            raise ValueError('independent job binding differs')
        # Validate the full closed existing v2 envelope without changing its
        # implementation or allowing a v3 grant through the legacy path.
        old_job=copy.deepcopy(job);old_grant=copy.deepcopy(grant)
        for obj in (old_job['transferRequest'],old_grant):
            for key in BINDING: obj.pop(key)
            obj['protocol']='source-range-v2'
        kwargs={'job':old_job}
        if clock is not None:kwargs['clock']=clock
        checked=validate_source_grant(old_grant,**kwargs)
        identity={**checked.identity.canonical(), 'protocol':PROTOCOL,
                  **{key:trusted[key] for key in BINDING}}
        wire=json.dumps(identity,sort_keys=True,separators=(',',':'),ensure_ascii=True)
        if type(prior_identity_wires) not in (list,tuple) or len(prior_identity_wires)>10000:
            raise ValueError('bounded source lineage required')
        for prior in prior_identity_wires:
            if type(prior) is not str or len(prior)>8192:raise ValueError('invalid lineage')
            value=json.loads(prior)
            if type(value) is not dict or json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True)!=prior:
                raise ValueError('noncanonical lineage')
            if (value.get('jobId'),value.get('generation'),value.get('environment')) == (trusted['jobId'],trusted['generation'],trusted['environment']) and prior!=wire:
                raise ValueError('prior budget identity requires reconciliation')
        return {'state':'held','reason':'source_authority_unqualified',
                'identityJson':wire,'identitySha256':hashlib.sha256(wire.encode()).hexdigest(),
                'objects':checked.objects}
    except (KeyError,TypeError,ValueError,AttributeError) as exc:
        raise AuthorityGrantHeld('source authority candidate rejected') from exc


def held_range_request_id(inspection, obj, start, length):
    if type(inspection) is not dict or inspection.get('state')!='held' or obj not in inspection.get('objects',()):
        raise AuthorityGrantHeld('held source object required')
    if type(start) is not int or type(length) is not int or start<0 or length<1 or start+length>obj.byte_size:
        raise AuthorityGrantHeld('valid held range required')
    return _request_id(inspection['identityJson'],obj,start,length)
