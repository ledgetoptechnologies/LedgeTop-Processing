"""Generate first-cutover expectations from reviewed source/config, never DB rows."""
import ast
from datetime import datetime
import hashlib
import hmac
import ipaddress
import json
from pathlib import Path
import re
import sqlite3
from urllib.parse import urlsplit

from . import coordinator_predecessor_census as old

SOURCES=('processing/coordinator_freeze.py','processing/reference_store.py',
 'processing/node_inventory.py','processing/reference_recovery.py',
 'processing/reference_source_readers.py','processing/reference_terminal.py',
 'processing/reference_failure.py','processing/prototype.py','processing/reference_runtime.py',
 'processing/config.py','processing/reference_protocol.py','processing/authority_lease.py',
 'processing/app.py','processing/runtime_source_custody.py','processing/maintenance.py')
BINDINGS=('bootstrapStartupId','currentStartupId','planId','ownerSha256','requestIds',
 'operationBinding','sourceReaderBinding','sourceCustody','bootstrapWindow','finalWindow')
BASE_ADDITIONS=frozenset(('coordinator_freeze_state','coordinator_freeze_receipts',
 'coordinator_operation_pins','coordinator_operation_epochs','processing_node_inventory',
 'reference_recovery_startup','reference_recovery_receipts','reference_source_intakes',
 'reference_source_object_receipts','reference_viewer_receipts','reference_viewer_transfers',
 'reference_source_grant_tokens'))
READERS=frozenset(('source_reader_binding','source_reader_epochs','source_reader_pins','source_reader_fences'))


def require(value,reason):
    if not value:raise old.CensusRefused(reason)


def _schema_statements(root, hashes):
    require(type(hashes) is dict and set(hashes)==set(SOURCES),'reviewed_source_hashes_required')
    result={}
    for relative in SOURCES:
        path=Path(root)/relative
        require(path.resolve()==path.absolute() and not path.is_symlink(),'original_reviewed_source_required')
        info=path.stat();require(info.st_size<=1048576,'reviewed_source_budget_exceeded')
        raw=path.read_bytes();require(len(raw)<=1048576 and hashlib.sha256(raw).hexdigest()==hashes[relative],
                                   'reviewed_candidate_source_changed')
        for node in ast.walk(ast.parse(raw)):
            if not isinstance(node,ast.Constant) or type(node.value) is not str:continue
            for statement in node.value.split(';'):
                statement=statement.strip()
                match=re.match(r'^CREATE (?:TABLE|INDEX) IF NOT EXISTS ([a-z_]+)\s*\(',statement)
                if not match:continue
                name=match.group(1)
                # The current SourceReaders literal is its complete CREATE;
                # later ALTER fallback is not used on this first new schema.
                if name in result:require(result[name]==statement,'ambiguous_reviewed_ddl')
                result[name]=statement
    return result


def _origin(value):
    parsed=urlsplit(value)
    require(parsed.scheme in ('http','https') and parsed.hostname and not parsed.username and not parsed.password
            and parsed.path in ('','/') and not parsed.query and not parsed.fragment,'reviewed_engine_origin_required')
    host=parsed.hostname
    try:host=ipaddress.ip_address(host).compressed
    except ValueError:host=host.rstrip('.').encode('idna').decode('ascii').lower()
    return [parsed.scheme,host,parsed.port or (443 if parsed.scheme=='https' else 80)]


def _inventory(bootstrap,final,endpoints,authority):
    require(bootstrap['schemaVersion']==2 and final['schemaVersion']==6 and
            bootstrap['workers']==final['workers'] and bootstrap['recoveryBindings']==final['recoveryBindings'] and
            'sourceRelease' not in final and final.get('viewerAcceptance')=={},'closed_first_startup_config_required')
    require(type(endpoints) is list and 1<=len(endpoints)<=8 and
            all(type(item) is dict and set(item)=={'id','url'} and
                type(item['id']) is str and re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}',item['id'])
                for item in endpoints) and
            len({item['id'] for item in endpoints})==len(endpoints) and
            type(bootstrap['workers']) is dict and type(bootstrap['recoveryBindings']) is dict and
            set(bootstrap['workers'])=={item['id'] for item in endpoints} and
            set(bootstrap['recoveryBindings'])=={item['id'] for item in endpoints},
            'exact_enrolled_inventory_required')
    nodes={};physical={}
    for endpoint in endpoints:
        require(set(endpoint)=={'id','url'},'closed_node_endpoint_required')
        node=endpoint['id'];worker=bootstrap['workers'][node];raw=bootstrap['recoveryBindings'][node]
        require(set(raw)=={'engineId','authorityId','coordinatorOrigin','gateGeneration','outputBudget'} and
                set(worker)=={'bearer','workerId','instanceId','resultOrigin','resultBearer'} and
                raw['authorityId']==authority,'configured_authority_changed')
        _origin(worker['resultOrigin']);_origin(raw['coordinatorOrigin'])
        recovery=dict(node_id=node,engine_id=raw['engineId'],authority_id=raw['authorityId'],
            coordinator_origin=raw['coordinatorOrigin'],gate_generation=raw['gateGeneration'],output_budget=raw['outputBudget'])
        physical[node]=recovery
        nodes[node]=dict(engineOrigin=_origin(endpoint['url']),physical=recovery,
            workerId=worker['workerId'],instanceId=worker['instanceId'],resultOrigin=worker['resultOrigin'])
    document=dict(nodeOrder=[item['id'] for item in endpoints],nodes=nodes)
    # This profile is ASCII/int only, making stdlib canonical JSON identical to
    # the runtime RFC8785 representation without importing executable runtime.
    require(all(ord(c)<128 for c in json.dumps(document,ensure_ascii=False)),'ascii_inventory_required')
    return document,physical


def build_startup_policy(root, *, expected_source_hashes, candidate_image_id, candidate_source_commit,
                         bootstrap_config_bytes, final_config_bytes, node_endpoints, authority_id,
                         reviewed_schema_bytes=None):
    from .coordinator_candidate_projection import PROFILE,PHASES
    require(type(candidate_image_id) is str and re.fullmatch(r'sha256:[a-f0-9]{64}',candidate_image_id) and
            type(candidate_source_commit) is str and re.fullmatch(r'[a-f0-9]{40}',candidate_source_commit),
            'immutable_candidate_required')
    schema_bytes=old.SCHEMA_PATH.read_bytes() if reviewed_schema_bytes is None else reviewed_schema_bytes
    require(type(schema_bytes) is bytes and len(schema_bytes)<=65536,'bounded_predecessor_schema_required')
    reviewed=old._json(schema_bytes.decode('utf-8'))
    require(hashlib.sha256(old._canonical(reviewed)).hexdigest()==old.SCHEMA_SHA256,'predecessor_schema_artifact_changed')
    for raw in (bootstrap_config_bytes,final_config_bytes):
        require(type(raw) is bytes and len(raw)<=1048576,'bounded_private_configuration_required')
    bootstrap=old._json(bootstrap_config_bytes.decode('utf-8'));final=old._json(final_config_bytes.decode('utf-8'))
    inventory,recovery=_inventory(bootstrap,final,node_endpoints,authority_id)
    statements=_schema_statements(root,expected_source_hashes)
    memory=sqlite3.connect(':memory:')
    try:
        for kind,name,_,sql in reviewed['objects']:
            if kind=='table' and name!='sqlite_sequence':memory.execute(sql)
        for kind,_,_,sql in reviewed['objects']:
            if kind=='index' and sql:memory.execute(sql)
        columns={name:[row[1] for row in memory.execute('PRAGMA table_info("'+name+'")')]
                 for kind,name,_,_ in reviewed['objects'] if kind=='table'}
        memory.execute('ALTER TABLE nodes ADD COLUMN target_engine_version TEXT')
        for name in sorted(BASE_ADDITIONS):require(name in statements,'reviewed_startup_ddl_missing');memory.execute(statements[name])
        bootstrap_objects=[list(row) for row in memory.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        for name in sorted(READERS):require(name in statements,'reviewed_reader_ddl_missing');memory.execute(statements[name])
        # Index SQL has ON table before its columns, so it is deliberately not
        # accepted by the TABLE-only literal extractor.
        memory.execute('CREATE INDEX source_reader_job_activity ON source_reader_pins(job_id,generation,state,operation)')
        final_objects=[list(row) for row in memory.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
    finally:memory.close()
    phases={phase:dict(objects=bootstrap_objects if phase in ('bootstrap-frozen','rollback-bootstrap') else final_objects,
                       rows={},files={},directories={}) for phase in PHASES}
    policy=dict(schemaVersion=1,profile=PROFILE,candidateImageId=candidate_image_id,candidateSourceCommit=candidate_source_commit,
        oldColumns=columns,additionalColumns={'nodes':{'target_engine_version':None}},bindingNames=list(BINDINGS),phases=phases,
        startup=dict(version=1,authorityId=authority_id,sourceHashes=expected_source_hashes,
            bootstrapConfigSha256=hashlib.sha256(bootstrap_config_bytes).hexdigest(),
            finalConfigSha256=hashlib.sha256(final_config_bytes).hexdigest(),inventory=inventory,recovery=recovery))
    wire=old._canonical(policy);require(len(wire)<=1048576,'bounded_startup_policy_required')
    return wire


def _window(value):
    require(type(value) is dict and set(value)=={'earliest','latest'},'independent_startup_window_required')
    parsed=[]
    for item in (value['earliest'],value['latest']):
        require(type(item) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',item),'canonical_startup_time_required')
        parsed.append(datetime.strptime(item,'%Y-%m-%d %H:%M:%S'))
    require(0<=(parsed[1]-parsed[0]).total_seconds()<=120,'bounded_startup_window_required')
    return value


def materialize(policy, binding, phase, key):
    from .coordinator_candidate_projection import freeze_expectations
    require(set(binding)==set(BINDINGS),'closed_startup_context_required')
    startup=policy['startup']
    require(type(startup) is dict and set(startup)=={'version','authorityId','sourceHashes','bootstrapConfigSha256',
            'finalConfigSha256','inventory','recovery'} and type(startup['version']) is int and startup['version']==1 and
            type(startup['sourceHashes']) is dict and set(startup['sourceHashes'])==set(SOURCES) and
            all(type(value) is str and old.HEX.fullmatch(value) for value in startup['sourceHashes'].values()) and
            all(type(startup[name]) is str and old.HEX.fullmatch(startup[name])
                for name in ('bootstrapConfigSha256','finalConfigSha256')),'known_startup_policy_required')
    _window(binding['bootstrapWindow'])
    is_final=phase in ('final-frozen','rollback-final');closed=phase!='final-frozen'
    if is_final:_window(binding['finalWindow'])
    else:require(binding['finalWindow'] is None,'future_final_window_must_be_absent')
    current=binding['currentStartupId']
    require((is_final and current!=binding['bootstrapStartupId']) or
            (not is_final and current==binding['bootstrapStartupId']),'startup_phase_identity_changed')
    operation=binding['operationBinding'];wire=old._canonical(operation).decode('ascii')
    reserved=4096-(len(current)+len(wire)+64+len('active') if is_final else 0)
    result=dict(policy['phases'][phase]);rows=freeze_expectations(authority_id=startup['authorityId'],
        plan_id=binding['planId'],owner_sha256=binding['ownerSha256'],bootstrap_startup_id=binding['bootstrapStartupId'],
        current_startup_id=current,request_ids=binding['requestIds'],operation_binding=operation,
        reserved_control_bytes=reserved,current_closed=closed)
    inventory_wire=old._canonical(startup['inventory']).decode('ascii')
    rows['processing_node_inventory']=[dict(revision=1,inventory_json=inventory_wire,
        inventory_sha256=hashlib.sha256(inventory_wire.encode()).hexdigest(),
        created_at={'timestampWindow':'bootstrapWindow'})]
    recovery_sha=hashlib.sha256(old._canonical(startup['recovery'])).hexdigest()
    rows['reference_recovery_startup']=[dict(singleton=1,epoch_id=current,
        authority_id=startup['authorityId'],binding_sha256=recovery_sha)]
    for table in BASE_ADDITIONS:rows.setdefault(table,[])
    files={};directories={}
    if is_final:
        physical=binding['sourceReaderBinding'];observation=binding['sourceCustody']
        require(type(physical) is dict and set(physical)=={'authorityId','database','databaseIdentity','root','rootIdentity','leaseIdentity','marker'},
                'independent_source_reader_binding_required')
        require(physical['authorityId']==startup['authorityId'] and physical['database']==operation['database'] and
                physical['databaseIdentity']==operation['databaseIdentity'] and
                physical['root']==operation['root']+'/source-readers','source_physical_authority_changed')
        require(type(physical['marker']) is str and re.fullmatch(r'[a-f0-9]{128}',physical['marker']),
                'independent_source_marker_required')
        marker=bytes.fromhex(physical['marker']);require(re.fullmatch(b'[a-f0-9]{64}',marker),'source_marker_bytes_invalid')
        physical_wire=old._canonical(physical).decode('ascii');physical_sha=hashlib.sha256(physical_wire.encode()).hexdigest()
        require(type(observation) is dict and observation.get('protocol')=='ltds-runtime-source-custody-v1' and
                observation.get('status')=='verified' and observation.get('authorityId')==startup['authorityId'] and
                observation.get('startupId')==current and observation.get('bindingSha256')==physical_sha and
                all(observation.get(name) is True for name in ('sourceReadersPresent','sessionVerified','leaseVerified','journalMatches')) and
                type(observation.get('epoch')) is str and re.fullmatch(r'[a-f0-9]{64}',observation['epoch']),
                'authenticated_actual_source_epoch_required')
        epoch=observation['epoch']
        rows.update(source_reader_binding=[dict(id=1,binding=physical_wire,epoch=epoch)],
            source_reader_epochs=[dict(epoch=epoch,state='closed' if closed else 'active')],
            source_reader_pins=[],source_reader_fences=[])
        files['source-readers/lease']=dict(bytes=64,content=hmac.new(key,b'file\0'+marker,hashlib.sha256).hexdigest(),mode=0o600)
        directories['source-readers']=dict(mode=0o700)
    else:require(binding['sourceReaderBinding'] is None and binding['sourceCustody'] is None,'unexpected_bootstrap_source_authority')
    result.update(rows=rows,files=files,directories=directories)
    return result


def match_rows(name, actual, expected, binding):
    if name!='processing_node_inventory':return sorted(map(old._canonical,actual))==sorted(map(old._canonical,expected))
    if len(actual)!=1 or len(expected)!=1:return False
    actual=dict(actual[0]);expected=dict(expected[0]);timestamp=actual.pop('created_at',None)
    if expected.pop('created_at',None)!={'timestampWindow':'bootstrapWindow'}:return False
    window=_window(binding['bootstrapWindow'])
    try:datetime.strptime(timestamp,'%Y-%m-%d %H:%M:%S')
    except (ValueError,TypeError):return False
    return type(timestamp) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',timestamp) is not None and window['earliest']<=timestamp<=window['latest'] and actual==expected
