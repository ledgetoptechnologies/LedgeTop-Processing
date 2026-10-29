"""Pure complete historical-grant validation, without any live-time authority.

Checks the immutable job/reservation/object closure, never whether a token is
still usable. Does not authenticate an issuer or grant custody/download rights.
"""
import copy
from datetime import datetime
import json
import re

from . import reference_protocol as p

MAX_NORMALIZED_BYTES=p.MAX_BODY_BYTES+16384
GRANT_FIELDS=('reservationId','generation','expiresAt','manifestSha256','byteBudget',
              'nonce','jobId','environment','objectIds','token')
RESERVATION_FIELDS=('reservationId','generation','expiresAt','manifestSha256','byteBudget','nonce')


class HistoricalGrantHeld(ValueError):pass


def decode_history(raw,*,limit):
    """Bounded duplicate-rejecting durable decoder; no private values in errors."""
    if type(raw) is not str or len(raw.encode('utf-8'))>limit:raise HistoricalGrantHeld('historical_record_bound')
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise HistoricalGrantHeld('historical_duplicate_key')
            out[key]=value
        return out
    def nonfinite(_):raise HistoricalGrantHeld('historical_nonfinite_number')
    try:return json.loads(raw,object_pairs_hook=pairs,parse_constant=nonfinite)
    except (ValueError,UnicodeError,RecursionError):raise HistoricalGrantHeld('historical_json_held') from None


def validate_historical_grant(grant,*,job):
    """Exact v1 shape and original closure; expiry is parsed, never compared now.

    job supplies original normalized submission and original transfer reservation
    from the same durable snapshot, not caller-provided new scope.
    """
    try:
        p._closed(grant,GRANT_FIELDS,'historical grant')
        if len(p.canonical_json(grant))>p.MAX_BODY_BYTES:raise HistoricalGrantHeld()
        submission=job['submission']
        raw={key:submission[key] for key in ('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint')}
        checked=p.validate_submission(raw,environment=job['environment'],
            capability_fingerprint=submission['capabilityFingerprint'],available_engines=('odm','treedar'))
        if p.canonical_json(checked)!=p.canonical_json(submission):raise HistoricalGrantHeld()
        p._integer(job['generation'],'generation');p._slug(job['jobId'],'job')
        if (checked['manifestSha256'],checked['pipelineDigest'])!=(job['manifestSha256'],job['pipelineDigest']):
            raise HistoricalGrantHeld()
        request=job['transferRequest'];p._closed(request,RESERVATION_FIELDS,'historical reservation')
        for value in (request,grant):
            for key in ('reservationId','nonce'):p._slug(value[key],key)
            p._integer(value['generation'],'generation');p._integer(value['byteBudget'],'byte budget')
            p._hash(value['manifestSha256'],'manifest digest')
            expiry=value['expiresAt']
            if type(expiry) is not str or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z',expiry):
                raise HistoricalGrantHeld()
            datetime.fromisoformat(expiry.replace('Z','+00:00'))
        for key in RESERVATION_FIELDS:
            if type(grant[key]) is not type(request[key]) or grant[key]!=request[key]:raise HistoricalGrantHeld()
        p._slug(grant['jobId'],'job');p._environment(grant['environment'])
        if (grant['jobId'],grant['generation'],grant['environment'],grant['manifestSha256'])!=(
                job['jobId'],job['generation'],job['environment'],job['manifestSha256']):raise HistoricalGrantHeld()
        manifest=submission['manifest'];total=manifest['totalBytes']
        if not total<=grant['byteBudget']<=min(p.MAX_SAFE_INTEGER,3*total):raise HistoricalGrantHeld()
        ids=grant['objectIds']
        if type(ids) is not list or len(ids)!=len(manifest['objects']) or len(ids)>p.MAX_OBJECTS:raise HistoricalGrantHeld()
        for value in ids:p._slug(value,'object ID')
        if len(set(ids))!=len(ids) or set(ids)!={o['id'] for o in manifest['objects']}:raise HistoricalGrantHeld()
        token=grant['token']
        if type(token) is not str or not 1<=len(token)<=4096 or any(ord(c)<33 or ord(c)>126 for c in token):raise HistoricalGrantHeld()
        return copy.deepcopy(grant)
    except (ValueError,TypeError,KeyError,AttributeError,OverflowError,RecursionError):
        raise HistoricalGrantHeld('historical_grant_held') from None
