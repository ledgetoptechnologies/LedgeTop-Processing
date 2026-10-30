"""Dedicated authenticated v2 metadata admission; never schedules execution."""
import hmac
from types import MappingProxyType
from . import reference_protocol as p
from .maintenance import Conflict
from .reference_api import Response
from .reference_input_budget_v2 import decode_canonical_input_v2, MAX_METADATA_BYTES, MAX_OBJECTS

ROUTE='/ltds-reference-v2/metadata-submissions'
CAPABILITIES_ROUTE='/ltds-reference-v2/capabilities'

class V2MetadataAPI:
    def __init__(self, ledger, *, principals, policy):
        if type(principals) is not dict or not principals or not callable(policy): raise ValueError('scoped principals required')
        checked={}
        for token,binding in principals.items():
            if type(token) is not str or not 32<=len(token)<=4096 or not token.isascii() or any(ord(c)<33 or ord(c)>126 for c in token): raise ValueError('invalid credential')
            p._closed(binding,('principalId','environment'),'principal')
            p._slug(binding['principalId'],'principal');p._environment(binding['environment'])
            checked[token]=(binding['principalId'],binding['environment'])
        self._principals=MappingProxyType(checked);self.ledger=ledger;self.policy=policy
    def _identity(self,headers):
        if not hasattr(headers,'items'):return None
        values=[v for k,v in headers.items() if type(k) is str and k.lower()=='authorization']
        if len(values)!=1 or type(values[0]) is not str or not values[0].startswith('Bearer '):return None
        token=values[0][7:]
        if not token.isascii() or not 32<=len(token)<=4096:return None
        found=None
        for configured,binding in self._principals.items():
            if hmac.compare_digest(token,configured):found=binding
        return found
    def handle(self,method,path,headers,body=b'',*,protected_transport=False):
        if path not in (ROUTE,CAPABILITIES_ROUTE):return Response(404,{'error':'not_found'})
        if protected_transport is not True:return Response(503,{'error':'protected_transport_required'})
        identity=self._identity(headers)
        if identity is None:return Response(401,{'error':'unauthorized'})
        if path==CAPABILITIES_ROUTE:
            if method!='GET':return Response(405,{'error':'method_not_allowed'})
            try:
                capability,options,definitions=self.policy()
                return Response(200,{'protocol':'ltds-reference-v2','schemaVersion':2,'environment':identity[1],
                    'capabilityFingerprint':capability,'optionsFingerprint':options,'optionsFingerprintScope':'full_safe_engine_policy','options':definitions,
                    'limits':{'metadataBytes':MAX_METADATA_BYTES,'objects':MAX_OBJECTS},
                    'metadataAdmission':True,'schedulingAvailable':False,'transferAvailable':False,'optionValueEncoding':'boolean_or_nonnegative_safe_integer'})
            except Exception:return Response(503,{'error':'engine_catalog_unavailable'})
        if method!='POST':return Response(405,{'error':'method_not_allowed'})
        try:
            request=decode_canonical_input_v2(body)
            result=self.ledger.find_exact_submission_replay(request,principal=identity[0],environment=identity[1])
            if result is None:
                capability,options,definitions=self.policy()
                by_name={item['name']:item for item in definitions}
                for name,value in request['pipeline']['odm']['options'].items():
                    definition=by_name.get(name)
                    kind=definition.get('type') if definition is not None else None
                    valid=((kind in ('bool','boolean') and type(value) is bool)
                        or (kind in ('int','integer') and type(value) is int and 0<=value<=p.MAX_SAFE_INTEGER))
                    if not valid: raise ValueError('invalid option type')
                result=self.ledger.submit(request,principal=identity[0],environment=identity[1],capability_fingerprint=capability,options_fingerprint=options,allowed_odm_options=set(by_name))
            return Response(201 if result['created'] else 200,{'jobId':result['jobId'],'generation':result['generation'],'state':'metadata_only','scheduled':False})
        except (p.ProtocolError,ValueError,TypeError,KeyError,AttributeError):return Response(400,{'error':'invalid_request'})
        except Conflict:return Response(409,{'error':'reference_conflict'})
        except Exception:return Response(503,{'error':'metadata_unavailable'})
