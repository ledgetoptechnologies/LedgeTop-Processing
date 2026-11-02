"""Inactive candidate lexical/schema/named-digest checks, never authorization.

Only the exact locally pinned schema vocabulary is supported. No issuer, payload,
archive, scientific, cross-record lifecycle or admission authority exists here.
"""
from dataclasses import dataclass,field
from datetime import datetime,timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import threading
from types import MappingProxyType


MAX_BYTES=4194304
MAX_DEPTH=32
MAX_ARRAY=10000
MAX_OBJECT=256  # Largest object permitted anywhere by the pinned used schema.
MAX_INTEGER=9007199254740991
STATUS='UNAGREED_INACTIVE_STRUCTURAL_ONLY'
ACCEPTANCE_HASH_PREFIX=b'LTDS-PARTIAL-ACCEPTANCE-V2\x00'
ROOT=Path(__file__).resolve().parents[1]/'contracts'/'reference-v2-candidate'
PINS=MappingProxyType({
    'contract.schema.json':'850b38441911a518fd6b3add9b74b4f7e2e6d8607cca50372c38f785a01c6412',
    'hash-domains.json':'0ae9dda18cf7f2d77eb359e5a373221562f95397e82de0dbd1167799af67bc85',
    'semantic-rules.json':'873306dc707a63954dd6b0ad329a5ec1d20641f87dad0bb8cde774d9267667f2',
    'invalid/index.json':'9de96a65367f76e752d05993136e5fd2e897bc9f37bd8ea08eccdc00fb3a978a'})
SELF_DIGESTS=MappingProxyType({'InputManifest':'sha256','ResultManifest':'resultManifestSha256',
    'ParentCompletion':'completionSha256','PointcloudZipManifest':'manifestSha256',
    'OdmArchiveClosureManifest':'manifestSha256','ImmutableMetadataIntent':'intentSha256',
    'AuthorizedResponse':'receiptSha256','DeniedResponse':'receiptSha256',
    'SealedStageAdmission':'admissionSha256'})
UNSAFE_KEYS=frozenset(('__proto__','prototype','constructor'))
_CACHE=None
_CACHE_LOCK=threading.Lock()


class CandidateError(ValueError):
    def __init__(self,code,path='$'):
        self.code,self.path=code,path
        super().__init__(code+': '+path)


def _require(condition,code,path='$'):
    if not condition:raise CandidateError(code,path)


class _FloatToken(float):
    """Keep JSON numeric token class, including mathematically integral floats."""


def _preflight(raw):
    _require(type(raw) is bytes,'RAW_BYTES_REQUIRED')
    _require(len(raw)<=MAX_BYTES,'METADATA_BYTES_EXCEEDED')
    _require(not raw.startswith(b'\xef\xbb\xbf'),'UTF8_BOM_REFUSED')
    stack=[];quoted=False;escaped=False
    for byte in raw:
        if quoted:
            if escaped:escaped=False
            elif byte==92:escaped=True
            elif byte==34:quoted=False
            continue
        if byte in (32,9,10,13):continue
        if stack and stack[-1][0]==91 and stack[-1][2] and byte not in (93,44):
            stack[-1][1]+=1;stack[-1][2]=False
            _require(stack[-1][1]<=MAX_ARRAY,'ARRAY_MEMBERS_EXCEEDED')
        if byte==34:quoted=True
        elif byte in (91,123):
            stack.append([byte,0,byte==91]);_require(len(stack)<=MAX_DEPTH,'DEPTH_EXCEEDED')
        elif byte in (93,125):
            _require(bool(stack) and stack[-1][0]==(91 if byte==93 else 123),'INVALID_JSON')
            if byte==93:_require(not stack[-1][2] or stack[-1][1]==0,'INVALID_JSON')
            stack.pop()
        elif byte==44 and stack and stack[-1][0]==91:
            _require(not stack[-1][2],'INVALID_JSON');stack[-1][2]=True
        elif byte==58 and stack and stack[-1][0]==123:
            stack[-1][1]+=1;_require(stack[-1][1]<=MAX_OBJECT,'OBJECT_MEMBERS_EXCEEDED')
    _require(not quoted and not stack,'INVALID_JSON')


def _integer_token(token):
    _require(token!='-0','NEGATIVE_ZERO')
    digits=token[1:] if token.startswith('-') else token
    _require(len(digits)<=16,'UNSAFE_INTEGER')
    number=int(token);_require(abs(number)<=MAX_INTEGER,'UNSAFE_INTEGER');return number


def _float_token(token):
    number=_FloatToken(token);_require(math.isfinite(number),'NONFINITE_NUMBER')
    _require(number!=0 or not token.startswith('-'),'NEGATIVE_ZERO');return number


def _pairs(items):
    result={}
    for key,value in items:
        _require(key not in result,'DUPLICATE_KEY');_require(key not in UNSAFE_KEYS,'UNSAFE_KEY')
        result[key]=value
    return result


def _unicode(value):
    if type(value) is str:
        try:value.encode('utf8','strict')
        except UnicodeError:raise CandidateError('INVALID_UNICODE') from None
    elif type(value) is dict:
        for key,item in value.items():_unicode(key);_unicode(item)
    elif type(value) is list:
        for item in value:_unicode(item)


def _decode(raw):
    _preflight(raw)
    try:
        text=raw.decode('utf8','strict')
        value=json.loads(text,object_pairs_hook=_pairs,parse_int=_integer_token,
            parse_float=_float_token,parse_constant=lambda _:(_ for _ in ()).throw(CandidateError('NONFINITE_NUMBER')))
        _unicode(value);return value
    except CandidateError:raise
    except (ValueError,UnicodeError,RecursionError,OverflowError):raise CandidateError('INVALID_JSON') from None


def canonical_metadata(value):
    """Restricted metadata codec only; never derive the opaque pipeline digest."""
    pieces=[];size=0
    def emit(text):
        nonlocal size
        data=text.encode('utf8','strict');size+=len(data);_require(size<=MAX_BYTES,'METADATA_BYTES_EXCEEDED');pieces.append(data)
    def visit(item,depth):
        if item is None:emit('null')
        elif type(item) is bool:emit('true' if item else 'false')
        elif type(item) is int:
            _require(0<=item<=MAX_INTEGER,'CANONICAL_INTEGER_REQUIRED');emit(str(item))
        elif type(item) is str:
            _require(len(item)<=MAX_BYTES,'METADATA_BYTES_EXCEEDED')
            try:emit(json.dumps(item,ensure_ascii=False,allow_nan=False))
            except UnicodeError:raise CandidateError('INVALID_UNICODE') from None
        elif type(item) in (dict,list):
            _require(depth<MAX_DEPTH,'DEPTH_EXCEEDED')
            if type(item) is list:
                _require(len(item)<=MAX_ARRAY,'ARRAY_MEMBERS_EXCEEDED');emit('[')
                for index,child in enumerate(item):
                    if index:emit(',')
                    visit(child,depth+1)
                emit(']')
            else:
                _require(len(item)<=MAX_OBJECT,'OBJECT_MEMBERS_EXCEEDED')
                _require(all(type(k) is str and k not in UNSAFE_KEYS and k and all(32<=ord(c)<=126 for c in k) for k in item),'CANONICAL_ASCII_KEYS_REQUIRED')
                emit('{')
                for index,key in enumerate(sorted(item)):
                    if index:emit(',')
                    visit(key,depth+1);emit(':');visit(item[key],depth+1)
                emit('}')
        else:raise CandidateError('CANONICAL_INTEGER_METADATA_ONLY')
    visit(value,0);return b''.join(pieces)


def _read_pinned(name):
    _require(name in PINS,'UNTRUSTED_SCHEMA_RESOURCE')
    path=ROOT/name;fd=None
    try:
        _require(path.resolve()==path and not any(part.is_symlink() for part in (path,*path.parents)),'PINNED_RESOURCE_PATH_CHANGED')
        before=path.lstat();_require(stat.S_ISREG(before.st_mode) and before.st_size<=262144,'PINNED_RESOURCE_BOUND')
        fd=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0))
        # Windows stat/fstat can expose different historical ctime meanings.
        # The exact raw digest remains mandatory on every resource load.
        snapshot=lambda info:(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns if os.name!='nt' else 0)
        _require(snapshot(before)==snapshot(os.fstat(fd)),'PINNED_RESOURCE_CHANGED')
        chunks=[];total=0
        while True:
            block=os.read(fd,min(65536,before.st_size-total+1))
            if not block:break
            total+=len(block);_require(total<=before.st_size,'PINNED_RESOURCE_CHANGED');chunks.append(block)
        data=b''.join(chunks)
        _require(total==before.st_size and snapshot(before)==snapshot(os.fstat(fd))==snapshot(path.lstat()),'PINNED_RESOURCE_CHANGED')
        _require(hashlib.sha256(data).hexdigest()==PINS[name],'PINNED_RESOURCE_DIGEST_MISMATCH');return data
    except CandidateError:raise
    except (OSError,ValueError):raise CandidateError('PINNED_RESOURCE_UNAVAILABLE') from None
    finally:
        if fd is not None:os.close(fd)


_ANNOTATIONS=frozenset(('$schema','$id','title','description','x-status','x-sourcePins','x-wholeMessageBytes','x-maximumDepth','x-unsafeKeys','x-semanticRules','x-note'))
_KEYWORDS=frozenset(('$defs','$ref','type','const','enum','properties','patternProperties','additionalProperties','required','items','minItems','maxItems','uniqueItems','minLength','maxLength','pattern','format','minimum','maximum','maxProperties','oneOf','anyOf','allOf','not','if','then'))
_KINDS=frozenset(('object','array','null','boolean','number','integer','string'))


def _compile_schema(value):
    """Compile only this candidate's vocabulary; unsupported forms fail closed."""
    _require(type(value) is dict and type(value.get('$defs')) is dict,'INVALID_PINNED_SCHEMA')
    names=frozenset(value['$defs'])
    def compile_node(node):
        _require(type(node) is dict and not set(node)-_KEYWORDS-_ANNOTATIONS,'UNSUPPORTED_SCHEMA_KEYWORD')
        result={}
        for key,item in node.items():
            if key in _ANNOTATIONS:continue
            if key=='$defs':result[key]=MappingProxyType({name:compile_node(child) for name,child in item.items()})
            elif key=='$ref':
                _require(type(item) is str and item.startswith('#/$defs/') and item[8:] in names and len(node)==1,'UNSUPPORTED_SCHEMA_REFERENCE');result[key]=item[8:]
            elif key=='type':_require(type(item) is str and item in _KINDS,'UNSUPPORTED_SCHEMA_TYPE');result[key]=item
            elif key in ('properties','patternProperties'):
                _require(type(item) is dict,'INVALID_PINNED_SCHEMA')
                result[key]=tuple((re.compile(name) if key=='patternProperties' else name,compile_node(child)) for name,child in item.items())
            elif key=='additionalProperties':
                _require(item is False or type(item) is dict,'UNSUPPORTED_ADDITIONAL_PROPERTIES');result[key]=False if item is False else compile_node(item)
            elif key in ('items','not','if','then'):result[key]=compile_node(item)
            elif key in ('oneOf','anyOf','allOf'):
                _require(type(item) is list and bool(item),'INVALID_PINNED_SCHEMA');result[key]=tuple(compile_node(child) for child in item)
            elif key=='required':
                _require(type(item) is list and all(type(v) is str for v in item) and len(item)==len(set(item)),'INVALID_PINNED_SCHEMA');result[key]=frozenset(item)
            elif key=='pattern':_require(type(item) is str,'INVALID_PINNED_SCHEMA');result[key]=re.compile(item)
            elif key=='format':_require(item=='date-time','UNSUPPORTED_SCHEMA_FORMAT');result[key]=item
            elif key=='const':_require(item is None or type(item) in (str,int,bool),'UNSUPPORTED_SCHEMA_CONST');result[key]=item
            elif key=='enum':
                _require(type(item) is list and bool(item) and all(type(v) is str for v in item),'UNSUPPORTED_SCHEMA_ENUM');result[key]=tuple(item)
            elif key=='uniqueItems':_require(item is True,'UNSUPPORTED_UNIQUE_ITEMS');result[key]=True
            else:
                _require(type(item) is int and 0<=item<=MAX_INTEGER,'INVALID_PINNED_SCHEMA');result[key]=item
        _require(('then' in result)==('if' in result),'UNSUPPORTED_SCHEMA_CONDITION')
        return MappingProxyType(result)
    return compile_node(value)


def _number(value):return type(value) is int or type(value) in (float,_FloatToken) and math.isfinite(value)


def _fingerprint(value):
    if value is None:return ('null',)
    if type(value) is bool:return ('bool',value)
    if _number(value):return ('number',value,1) if type(value) is int else ('number',*float(value).as_integer_ratio())
    if type(value) is str:return ('string',value)
    if type(value) is list:return ('array',tuple(_fingerprint(v) for v in value))
    return ('object',tuple((key,_fingerprint(value[key])) for key in sorted(value)))


def _evaluate(node,value,defs,path='$'):
    if '$ref' in node:return _evaluate(defs[node['$ref']],value,defs,path)
    kind=node.get('type')
    if kind:
        matched={'object':type(value) is dict,'array':type(value) is list,'null':value is None,'boolean':type(value) is bool,
            'number':_number(value),'integer':type(value) is int,'string':type(value) is str}[kind]
        _require(matched,'SCHEMA_TYPE',path)
    if 'const' in node:_require(type(value) is type(node['const']) and value==node['const'],'SCHEMA_CONST',path)
    if 'enum' in node:_require(type(value) is str and value in node['enum'],'SCHEMA_ENUM',path)
    if type(value) is dict:
        props=dict(node.get('properties',()));patterns=node.get('patternProperties',());keys=set(value)
        _require(node.get('required',frozenset())<=keys,'SCHEMA_REQUIRED',path)
        _require(len(value)<=node.get('maxProperties',MAX_OBJECT),'SCHEMA_MAX_PROPERTIES',path)
        for key,child in value.items():
            targets=([props[key]] if key in props else [])+[schema for regex,schema in patterns if regex.search(key)]
            if not targets:
                additional=node.get('additionalProperties')
                _require(additional is not False,'SCHEMA_UNKNOWN_PROPERTIES',path)
                if additional is not None:targets=[additional]
            for target in targets:_evaluate(target,child,defs,path+'.'+key if key in props else path+'.<dynamic>')
    if type(value) is list:
        _require(node.get('minItems',0)<=len(value)<=node.get('maxItems',MAX_ARRAY),'SCHEMA_ARRAY_LENGTH',path)
        if node.get('uniqueItems'):
            seen=set()
            for item in value:
                identity=_fingerprint(item);_require(identity not in seen,'SCHEMA_UNIQUE_ITEMS',path);seen.add(identity)
        if 'items' in node:
            for index,item in enumerate(value):_evaluate(node['items'],item,defs,path+'['+str(index)+']')
    if type(value) is str:
        _require(node.get('minLength',0)<=len(value)<=node.get('maxLength',MAX_BYTES),'SCHEMA_STRING_LENGTH',path)
        if 'pattern' in node:_require(node['pattern'].search(value) is not None,'SCHEMA_PATTERN',path)
        if node.get('format')=='date-time':
            try:
                date=datetime.fromisoformat(value.replace('Z','+00:00'))
                matched=date.tzinfo is not None and date.utcoffset().total_seconds()==0 and date.isoformat(timespec='milliseconds').replace('+00:00','Z')==value
            except (ValueError,OverflowError):matched=False
            _require(matched,'SCHEMA_DATE_TIME',path)
    if _number(value):_require(node.get('minimum',-math.inf)<=value<=node.get('maximum',math.inf),'SCHEMA_NUMERIC_RANGE',path)
    for mode in ('anyOf','oneOf'):
        if mode in node:
            count=0
            for child in node[mode]:
                if _matches(child,value,defs):count+=1
                if mode=='anyOf' and count:break
            _require(count>=1 if mode=='anyOf' else count==1,'SCHEMA_'+mode.upper(),path)
    if 'not' in node:_require(not _matches(node['not'],value,defs),'SCHEMA_NOT',path)
    for child in node.get('allOf',()):_evaluate(child,value,defs,path)
    if 'if' in node and _matches(node['if'],value,defs):_evaluate(node['then'],value,defs,path)


def _matches(node,value,defs):
    try:_evaluate(node,value,defs);return True
    except CandidateError:return False


def _bundle():
    global _CACHE
    with _CACHE_LOCK:
        if _CACHE is None:
            schema=_compile_schema(_decode(_read_pinned('contract.schema.json')))
            domains=_decode(_read_pinned('hash-domains.json'));rules=_decode(_read_pinned('semantic-rules.json'))
            actual={v['type']:v['field'] for v in domains['selfDigests']}
            _require(actual==dict(SELF_DIGESTS),'NAMED_DIGEST_DOMAINS_CHANGED')
            _require(domains.get('crossMessageDigests')==[dict(type='AcceptedCopyAck',field='acceptanceSha256',prefixAscii='LTDS-PARTIAL-ACCEPTANCE-V2',prefixTerminatorHex='00',value='complete PartialAcceptance record',omission='none',formula='SHA256(ASCII(prefix) || 0x00 || canonicalUTF8(value))')],'CROSS_MESSAGE_DIGEST_DOMAINS_CHANGED')
            _require(len(schema['$defs'])==61,'PINNED_SCHEMA_DEFINITIONS_CHANGED')
            deferred=tuple(rule['id'] for rule in rules['rules'] if rule['phase'] not in ('lexical','schema') and rule['id']!='STRUCTURED_DIGEST')
            _CACHE=(schema,deferred)
        return _CACHE


def _check_digests(schema,value,defs,*,record_type=None):
    found=[];seen=set()
    def visit(node,item,path,typename=None):
        if '$ref' in node:return visit(defs[node['$ref']],item,path,node['$ref'])
        if typename in SELF_DIGESTS and (typename,path) not in seen:
            key=SELF_DIGESTS[typename];seen.add((typename,path))
            calculated=hashlib.sha256(canonical_metadata({k:v for k,v in item.items() if k!=key})).hexdigest()
            _require(calculated==item[key],'NAMED_SELF_DIGEST_MISMATCH',path+'.'+key);found.append(typename+'@'+path)
        if type(item) is dict:
            props=dict(node.get('properties',()));patterns=node.get('patternProperties',())
            for key,child in item.items():
                targets=([props[key]] if key in props else [])+[target for regex,target in patterns if regex.search(key)]
                if not targets and type(node.get('additionalProperties')) is not bool and node.get('additionalProperties') is not None:targets=[node['additionalProperties']]
                for target in targets:visit(target,child,path+'.'+key if key in props else path+'.<dynamic>')
        elif type(item) is list and 'items' in node:
            for index,child in enumerate(item):visit(node['items'],child,path+'['+str(index)+']')
        for mode in ('anyOf','oneOf'):
            for target in node.get(mode,()):
                if _matches(target,item,defs):visit(target,item,path)
        for target in node.get('allOf',()):visit(target,item,path)
        if 'if' in node and _matches(node['if'],item,defs):visit(node['then'],item,path)
    visit(schema,value,'$',record_type);return tuple(found)


@dataclass(frozen=True)
class StructuralCandidateRecord:
    record_type:str
    raw_bytes:bytes=field(repr=False)
    verified_self_digests:tuple[str,...]
    unimplemented_gates:tuple[str,...]
    status:str=field(default=STATUS,init=False)
    schema_sha256:str=field(default=PINS['contract.schema.json'],init=False)

    @property
    def value(self):return _decode(self.raw_bytes)  # A fresh copy; cannot mutate the receipt.

    def __bool__(self):raise TypeError('Structural candidate records are not boolean acceptance or authority.')


def validate_candidate(raw,*,record_type):
    """Lexical + used structural schema + named self-digests, NOTHING ELSE."""
    value=_decode(raw);schema,deferred=_bundle();defs=schema['$defs']
    _require(type(record_type) is str and record_type in defs,'UNKNOWN_CANDIDATE_RECORD_TYPE')
    node=defs[record_type];_evaluate(node,value,defs)
    checked=_check_digests(node,value,defs,record_type=record_type)
    return StructuralCandidateRecord(record_type,raw,checked,deferred)


def validate_accepted_copy_ack(acceptance_raw,ack_raw):
    """Check the proposed ACK's cross-record binding, never issuer authority."""
    acceptance=validate_candidate(acceptance_raw,record_type='PartialAcceptance').value
    ack=validate_candidate(ack_raw,record_type='AcceptedCopyAck')
    value=ack.value
    expected_hash=hashlib.sha256(ACCEPTANCE_HASH_PREFIX+canonical_metadata(acceptance)).hexdigest()
    checks=(
        (value['ackOperationId']==acceptance['ackOperationId'],'ACCEPTANCE_ACK_OPERATION_MISMATCH','$.ackOperationId'),
        (value['acceptanceReceiptId']==acceptance['receiptId'],'ACCEPTANCE_ACK_RECEIPT_MISMATCH','$.acceptanceReceiptId'),
        (value['acceptanceSha256']==expected_hash,'ACCEPTANCE_ACK_DIGEST_MISMATCH','$.acceptanceSha256'),
        (value['binding']==acceptance['binding'],'ACCEPTANCE_ACK_BINDING_MISMATCH','$.binding'),
        (value['resultSetId']==acceptance['resultSetId'],'ACCEPTANCE_ACK_RESULT_SET_MISMATCH','$.resultSetId'),
        (value['resultManifestSha256']==acceptance['resultManifestSha256'],'ACCEPTANCE_ACK_MANIFEST_MISMATCH','$.resultManifestSha256'),
        (value['acceptedArtifactIds']==[item['id'] for item in acceptance['acceptedArtifacts']],
         'ACCEPTANCE_ACK_ARTIFACTS_MISMATCH','$.acceptedArtifactIds'),
        (value['storageReceiptId']==acceptance['storageReceiptId'],'ACCEPTANCE_ACK_STORAGE_MISMATCH','$.storageReceiptId'),
    )
    for valid,code,path in checks:_require(valid,code,path)
    return ack


def candidate_definitions():return tuple(sorted(_bundle()[0]['$defs']))


def coverage_report():
    schema,deferred=_bundle();index=_decode(_read_pinned('invalid/index.json'))
    cases=tuple(dict(id=case['id'],declared_phase=case['phase'],coverage='APPLICABLE_LEXICAL_STRUCTURAL' if case['phase'] in ('lexical','schema') else 'UNIMPLEMENTED_GATE',required_rule=case['rule']) for case in index['cases'])
    return dict(status=STATUS,schema_sha256=PINS['contract.schema.json'],supported_definitions=candidate_definitions(),
        implemented_layers=('bounded_strict_json','pinned_used_schema_vocabulary','named_metadata_self_digests'),
        named_self_digest_domains=dict(SELF_DIGESTS),unimplemented_gates=deferred,
        not_verified=('opaque_pipeline_digest_authority','requested_products_cross_record_digest','physical_file_hashes','archive_members_and_semantics','issuer_actor_custody_revocation','claim_reservation_admission','delivery_execution_settlement_cleanup'),
        cases=cases,warning='Structural rejection may happen earlier. UNIMPLEMENTED_GATE never means acceptance or semantic conformance.')
