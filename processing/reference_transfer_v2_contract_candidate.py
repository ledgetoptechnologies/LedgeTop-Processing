"""Unqualified v2 admission syntax/hash candidate; no listener or authorization.

Only whole/empty transfers are recognized; range proofs and reconciliation are
not qualified. A successful parse confers no right to access or stream data.
"""
import base64
import hashlib
import json
import re
from . import reference_protocol as p

QUALIFIED=False
DOMAIN=b'ltds-reference-request-v2\0'
MAX_WIRE_BYTES=16384

class V2ContractError(ValueError):pass


def parse_admission(raw):
    if QUALIFIED is not True:raise V2ContractError('v2 contract is not qualified')
    if type(raw) is not bytes or not 0<len(raw)<=MAX_WIRE_BYTES:raise V2ContractError('invalid v2 wire')
    def unique(pairs):
        result={}
        for key,value in pairs:
            if key in result:raise ValueError()
            result[key]=value
        return result
    def integer(text):
        if text=='-0':raise ValueError()
        value=int(text)
        if not 0<=value<=p.MAX_SAFE_INTEGER:raise ValueError()
        return value
    try:
        value=json.loads(raw.decode('utf-8',errors='strict'),object_pairs_hook=unique,parse_int=integer,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if type(value) is not dict or set(value)!={'version','requestId','deliveryId','binding'} or type(value['version']) is not int or value['version']!=2:
            raise ValueError()
        for key in ('requestId','deliveryId'):
            identifier=value[key]
            if type(identifier) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{43}',identifier):raise ValueError()
            decoded=base64.urlsafe_b64decode(identifier+'=')
            if len(decoded)!=32 or base64.urlsafe_b64encode(decoded).decode().rstrip('=')!=identifier:raise ValueError()
        binding=value['binding']
        fields={'attemptId','authorityId','claimLineageSha256','environment','epoch','generation','jobId','manifestSha256','objectId','objectLength','objectSha256','range','reservationId','workerInstanceId'}
        if type(binding) is not dict or set(binding)!=fields:raise ValueError()
        for key in ('attemptId','authorityId','epoch','jobId'):p._uuid(binding[key],key)
        for key in ('objectId','reservationId','workerInstanceId'):p._slug(binding[key],key)
        for key in ('claimLineageSha256','manifestSha256','objectSha256'):p._hash(binding[key],key)
        p._environment(binding['environment']);p._integer(binding['generation'],'generation')
        p._integer(binding['objectLength'],'objectLength',allow_zero=True)
        range_value=binding['range']
        if type(range_value) is not dict or set(range_value)!={'kind'}:raise ValueError()
        if range_value['kind']=='whole':
            if binding['objectLength']==0:raise ValueError()
        elif range_value['kind']=='empty':
            if binding['objectLength']!=0 or binding['objectSha256']!=hashlib.sha256(b'').hexdigest():raise ValueError()
        else:raise ValueError()
        encoded=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')
        if raw!=encoded:raise ValueError()
        request=dict(value);request.pop('deliveryId')
        canonical=json.dumps(request,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')
        return value,hashlib.sha256(DOMAIN+canonical).hexdigest()
    except (ValueError,TypeError,KeyError,UnicodeError,RecursionError):raise V2ContractError('invalid v2 admission') from None
