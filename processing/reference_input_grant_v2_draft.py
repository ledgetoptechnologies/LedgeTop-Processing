"""Unagreed, inactive PR68 candidate grant byte verifier.

No authentication, token, source membership, issuer, grant validity at runtime,
request charging, transport or authority is implemented. The local 16 KiB
bound is a prototype safeguard, not a negotiated wire limit.
"""
import hashlib,json,re
from datetime import datetime
from . import reference_protocol as p

DOMAIN=b'ledgetop.reference.input-grant.v2\0'
MAX_LOCAL_BYTES=16384
FIELDS=frozenset(('protocolVersion','jobId','generation','environment','sourceId','manifestSha256',
 'capabilityFingerprint','transferEpoch','grantId','issuedAt','expiresAt','rangeBytes','totalRequestBudget','totalByteBudget'))

class GrantDraftError(ValueError):pass

def _integer_token(token):
 if not re.fullmatch(r'0|[1-9][0-9]*',token) or len(token)>16:raise GrantDraftError('invalid integer')
 value=int(token)
 if value>p.MAX_SAFE_INTEGER:raise GrantDraftError('unsafe integer')
 return value

def _bad_number(_):raise GrantDraftError('integer token required')
def _pairs(pairs):
 result={}
 for key,value in pairs:
  if key in result:raise GrantDraftError('duplicate key')
  result[key]=value
 return result

def _date(value):
 if type(value) is not str or re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z',value) is None:raise GrantDraftError('invalid UTC date')
 try:return datetime.fromisoformat(value[:-1]+'+00:00')
 except ValueError:raise GrantDraftError('invalid UTC date') from None

def verify_candidate_grant(raw):
 if type(raw) is not bytes or len(raw)>MAX_LOCAL_BYTES or raw.startswith(b'\xef\xbb\xbf'):raise GrantDraftError('bounded canonical bytes required')
 try:
  value=json.loads(raw.decode('utf8','strict'),object_pairs_hook=_pairs,parse_int=_integer_token,parse_float=_bad_number,parse_constant=_bad_number)
  if type(value) is not dict or set(value)!=FIELDS:raise GrantDraftError('closed payload required')
  if p.canonical_json(value)!=raw:raise GrantDraftError('noncanonical encoding')
  if type(value['protocolVersion']) is not int or value['protocolVersion']!=2:raise GrantDraftError('unsupported version')
  p._uuid(value['jobId'],'jobId');p._environment(value['environment'])
  for name in ('sourceId','transferEpoch','grantId'):p._slug(value[name],name)
  for name in ('manifestSha256','capabilityFingerprint'):p._hash(value[name],name)
  for name in ('generation','rangeBytes'):p._integer(value[name],name)
  for name in ('totalRequestBudget','totalByteBudget'):p._integer(value[name],name,allow_zero=True)
  if _date(value['expiresAt'])<=_date(value['issuedAt']):raise GrantDraftError('invalid grant interval')
  return {'payload':value,'sha256':hashlib.sha256(DOMAIN+raw).hexdigest()}
 except (UnicodeError,ValueError,TypeError,KeyError,RecursionError) as error:
  if isinstance(error,GrantDraftError):raise
  raise GrantDraftError('invalid grant candidate') from None
