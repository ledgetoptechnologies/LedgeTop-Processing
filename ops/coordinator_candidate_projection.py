"""Read-only, exact-declaration first-cutover projection verifier.

A reviewed deployment pins policy bytes before mutation. This helper never learns
an allowlist from the candidate database, acquires a lease, or repairs state.
Physical writer exclusion and authenticated gate observations belong to the
transport. An unreviewed policy is not a qualification artifact.
"""
import hashlib
import hmac
import json
import os
from pathlib import Path
import sqlite3
import stat
import time
import uuid

from . import coordinator_predecessor_census as old

PROFILE = 'first-enrolled-exact-declared-projection-v1'
PHASES = ('bootstrap-frozen', 'final-frozen', 'rollback-bootstrap', 'rollback-final')
EMPTY_PAYLOAD_TABLES = frozenset((
 'reference_source_intakes','reference_source_object_receipts','reference_viewer_receipts',
 'reference_viewer_transfers','reference_source_grant_tokens','reference_recovery_receipts',
 'reference_worker_acceptance_observations','source_reader_pins','source_reader_fences',
 'source_releases','source_release_objects','reference_source_grant_renewals',
 'reference_source_range_usage','reference_source_range_requests','reference_source_object_pins',
 'coordinator_operation_pins'))


def _require(value, reason):
    if not value: raise old.CensusRefused(reason)


def _identifier(value):
    _require(type(value) is str and value and value.replace('_','').isalnum() and value.isascii(),
             'closed_sql_identifier_required')
    return '"' + value + '"'


def _bounded_canonical(value, limit, depth=0):
    """Bound nested evidence before constructing its canonical byte copy."""
    _require(depth<=16,'declared_binding_depth_exceeded')
    if type(value) is str:
        size=2
        for char in value:
            code=ord(char)
            size+=(2 if char in '\\"' or char in '\b\f\n\r\t' else
                   6 if code<32 or 126<code<=65535 else 12 if code>65535 else 1)
            _require(size<=limit,'declared_binding_budget_exceeded')
        return size
    if type(value) in (dict,list):
        size=2
        items=value.items() if type(value) is dict else value
        for index,item in enumerate(items):
            size+=int(index>0)
            if type(value) is dict:
                key,item=item;_require(type(key) is str,'declared_binding_key_required')
                size+=_bounded_canonical(key,limit-size,depth+1)+1
            size+=_bounded_canonical(item,limit-size,depth+1)
            _require(size<=limit,'declared_binding_budget_exceeded')
        return size
    _require(value is None or type(value) in (int,bool),'closed_declared_binding_value_required')
    size=len(old._canonical(value));_require(size<=limit,'declared_binding_budget_exceeded')
    return size


def _instantiate(value, binding):
    if type(value) is dict:
        if set(value) == {'binding'}:
            _require(value['binding'] in binding, 'declared_binding_missing')
            return binding[value['binding']]
        if set(value) == {'canonical'}:
            return old._canonical(_instantiate(value['canonical'],binding)).decode('ascii')
        if set(value) == {'sha256'}:
            return hashlib.sha256(old._canonical(_instantiate(value['sha256'],binding))).hexdigest()
        return {key:_instantiate(item,binding) for key,item in value.items()}
    if type(value) is list:return [_instantiate(item,binding) for item in value]
    return value


def freeze_expectations(*, authority_id, plan_id, owner_sha256, bootstrap_startup_id,
                        current_startup_id, request_ids, operation_binding,
                        reserved_control_bytes, current_closed):
    """Reconstruct genuine three-RPC freeze rows without consulting SQLite.

    All arguments are transport/configuration evidence already bound to the
    operation. The reserved amount must be computed from declared startup
    allocations; it is not inferred from observed remaining bytes.
    """
    for value in (authority_id,plan_id,bootstrap_startup_id,current_startup_id):
        _require(type(value) is str and str(uuid.UUID(value))==value,'canonical_gate_identity_required')
    _require(type(owner_sha256) is str and old.HEX.fullmatch(owner_sha256) and
             type(operation_binding) is dict and operation_binding.get('authorityId')==authority_id and
             type(request_ids) is dict and set(request_ids)=={'intent','drain','seal'} and
             len(set(request_ids.values()))==3 and type(current_closed) is bool and
             type(reserved_control_bytes) is int and 2048<=reserved_control_bytes<=4096,
             'closed_gate_evidence_required')
    receipts=[]
    for index,(action,phase) in enumerate((('intent','intent'),('drain','draining'),('seal','frozen'))):
        request_id=request_ids[action]
        _require(type(request_id) is str and str(uuid.UUID(request_id))==request_id,'canonical_gate_request_required')
        request=dict(requestId=request_id,planId=plan_id,expectedRevision=index,
                     action=action,startupId=bootstrap_startup_id)
        receipt=dict(authorityId=authority_id,startupId=bootstrap_startup_id,planId=plan_id,
                     revision=index+1,phase=phase,requestId=request_id)
        request_wire=old._canonical(request).decode('ascii')
        receipts.append(dict(request_id=request_id,plan_id=plan_id,owner_sha256=owner_sha256,
            request_sha256=hashlib.sha256(request_wire.encode('ascii')).hexdigest(),
            request_json=request_wire,receipt_json=old._canonical(receipt).decode('ascii')))
    wire=old._canonical(operation_binding).decode('ascii');sha=hashlib.sha256(wire.encode('ascii')).hexdigest()
    epochs=[]
    for epoch in dict.fromkeys((bootstrap_startup_id,current_startup_id)):
        state='closed' if epoch!=current_startup_id or current_closed else 'active'
        epochs.append(dict(epoch=epoch,binding_json=wire,binding_sha256=sha,state=state,retired_by=None))
    return dict(coordinator_freeze_state=[dict(singleton=1,revision=3,plan_id=plan_id,
        phase='frozen',owner_sha256=owner_sha256,owner_epoch=bootstrap_startup_id,
        reserved_control_bytes=reserved_control_bytes)],coordinator_freeze_receipts=receipts,
        coordinator_operation_pins=[],coordinator_operation_epochs=epochs)


def verify_projection(root, *, baseline, policy_bytes, expected_policy_sha256, expected_baseline_sha256, key,
                      phase, gate_binding, uid, max_metadata_bytes=64*1024*1024,
                      max_artifact_bytes=1024**4, max_duration_seconds=300, reviewed_schema_bytes=None):
    """Compare exact legacy projections/files and exact declared startup rows.

    gate_binding must come from independent authenticated gate/physical evidence,
    never a scan of the rows being compared. The transport binds its evidence to
    the journal intent. This pure function cannot establish that provenance.
    """
    _require(phase in PHASES and type(policy_bytes) is bytes and len(policy_bytes)<=1024*1024,
             'bounded_declared_projection_required')
    _require(type(key) is bytes and len(key)==32 and type(gate_binding) is dict,
             'private_census_key_and_binding_required')
    _bounded_canonical(gate_binding,1048576)
    _require(type(max_metadata_bytes) is int and max_metadata_bytes>0 and
             type(max_artifact_bytes) is int and max_artifact_bytes>0,'positive_census_budget_required')
    _require(hashlib.sha256(policy_bytes).hexdigest()==expected_policy_sha256,
             'declared_transform_digest_changed')
    policy=old._json(policy_bytes.decode('utf-8'))
    fields={'schemaVersion','profile','candidateImageId','candidateSourceCommit',
            'oldColumns','additionalColumns','bindingNames','phases'}
    _require(type(policy) is dict and set(policy) in (fields,fields|{'startup'}) and type(policy['schemaVersion']) is int and policy['schemaVersion']==1 and
             policy['profile']==PROFILE and set(policy['phases'])==set(PHASES),
             'closed_declared_projection_policy_required')
    _require(type(policy['candidateImageId']) is str and policy['candidateImageId'].startswith('sha256:') and
             old.HEX.fullmatch(policy['candidateImageId'][7:]) and
             type(policy['candidateSourceCommit']) is str and len(policy['candidateSourceCommit'])==40 and
             all(c in '0123456789abcdef' for c in policy['candidateSourceCommit']), 'immutable_candidate_required')
    _require(type(policy['bindingNames']) is list and len(set(policy['bindingNames']))==len(policy['bindingNames']) and
             set(gate_binding)==set(policy['bindingNames']), 'exact_declared_binding_required')
    _require(type(baseline) is dict and baseline.get('profile')==old.PROFILE and baseline.get('sourceCommit')==old.SOURCE and
             baseline.get('retainedFilesValidated') is True and baseline.get('validationScope')=='full-retained-custody' and
             type(baseline.get('directories')) is dict,
             'full_predecessor_custody_required')
    _require(hashlib.sha256(old._canonical(baseline)).hexdigest()==expected_baseline_sha256,
             'full_baseline_digest_changed')
    # Pin the old schema/column list independently of the candidate policy.
    if reviewed_schema_bytes is None:reviewed_schema_bytes=old.SCHEMA_PATH.read_bytes()
    _require(type(reviewed_schema_bytes) is bytes and len(reviewed_schema_bytes)<=65536,'reviewed_schema_bytes_required')
    reviewed=old._json(reviewed_schema_bytes.decode('utf-8'))
    _require(hashlib.sha256(old._canonical(reviewed)).hexdigest()==old.SCHEMA_SHA256,'predecessor_schema_artifact_changed')
    _require(set(baseline['tables'])=={row[1] for row in reviewed['objects'] if row[0]=='table'},
             'legacy_table_set_changed')
    memory=sqlite3.connect(':memory:')
    try:
        for kind,name,table,sql in reviewed['objects']:
            if kind=='table' and name!='sqlite_sequence' and sql:memory.execute(sql)
        old_columns={name:[row[1] for row in memory.execute('PRAGMA table_info('+_identifier(name)+')')]
                     for name in baseline['tables']}
    finally:memory.close()
    _require(policy['oldColumns']==old_columns,'legacy_projection_columns_changed')
    if 'startup' in policy:
        from .coordinator_startup_policy import materialize
        _require(type(baseline.get('metadataBytes')) is int and
                 baseline['metadataBytes']+len(policy_bytes)+4*len(old._canonical(gate_binding))+1048576<64*1048576,
                 'first_cutover_startup_quota_margin_unproven')
        declared=materialize(policy,gate_binding,phase,key)
    else:declared=_instantiate(policy['phases'][phase],gate_binding)
    _require(type(declared) is dict and set(declared)=={'objects','rows','files','directories'},
             'closed_phase_declaration_required')
    for group in ('files','directories'):
        _require(type(declared[group]) is dict,'closed_declared_paths_required')
        for name in declared[group]:
            _require(type(name) is str and 0<len(name)<=4096 and not name.startswith('/') and
                     '\\' not in name and all(part not in ('','.','..') for part in name.split('/')),
                     'relative_declared_path_required')
    root=Path(root).absolute();root_info=old._owned(root,uid,directory=True,root=True)
    _require(stat.S_IMODE(root_info.st_mode)==baseline['rootMode'],'retained_root_mode_changed')
    database=root/'jobs.sqlite3';before=old._owned(database,uid)
    _require(stat.S_IMODE(before.st_mode)==0o600 and
             all(not (root/('jobs.sqlite3'+suffix)).exists() for suffix in ('-wal','-shm','-journal')),
             'offline_rollback_database_required')
    _require(before.st_size<=8*max_metadata_bytes+100000*1024+1048576,
             'candidate_physical_budget_exceeded')
    budget=old._ReadBudget(max_duration_seconds,max_artifact_bytes+before.st_size)
    db=sqlite3.connect(database.as_uri()+'?mode=ro&immutable=1',uri=True);db.row_factory=sqlite3.Row
    try:
        db.set_progress_handler(lambda:int(time.monotonic()>=budget.deadline),1000)
        db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,65536)
        db.execute('PRAGMA query_only=ON');db.execute('PRAGMA temp_store=MEMORY');db.execute('BEGIN')
        schema_bytes=db.execute("SELECT COALESCE(SUM(length(CAST(sql AS BLOB))+length(name)+length(type)+length(tbl_name)),0) FROM sqlite_master").fetchone()[0]
        _require(schema_bytes<=1048576,'candidate_schema_budget_exceeded')
        objects=[list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        _require(objects==declared['objects'],'undeclared_candidate_schema')
        tables=sorted(item[1] for item in objects if item[0]=='table')
        _require(set(declared['rows'])==set(tables)-set(old_columns),'undeclared_startup_table')
        _require(policy['additionalColumns'] in ({}, {'nodes':{'target_engine_version':None}}),'unknown_legacy_column_change')
        raw_bytes=row_count=0;coordinates={}
        for name in tables:
            columns=[row[1] for row in db.execute('PRAGMA table_info('+_identifier(name)+')')]
            if name in old_columns:
                extra=policy['additionalColumns'].get(name,{})
                _require(columns==old_columns[name]+list(extra),'legacy_columns_not_additive')
            lengths='+'.join('COALESCE(length(CAST('+_identifier(column)+' AS BLOB)),0)' for column in columns)
            coordinates[name]=[]
            for identity,size in db.execute('SELECT rowid,'+lengths+' FROM '+_identifier(name)):
                budget.check();raw_bytes+=size;row_count+=1
                _require(raw_bytes<=max_metadata_bytes and row_count<=100000,'candidate_metadata_budget_exceeded')
                coordinates[name].append(identity)
        db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,max(65536,max_metadata_bytes))
        _require(db.execute('PRAGMA quick_check').fetchone()[0]=='ok' and
                 db.execute('PRAGMA foreign_key_check').fetchone() is None,'candidate_database_integrity_failed')
        logical=0;table_proofs={}
        for name in tables:
            encoded=[];values=[]
            for identity in coordinates[name]:
                value=dict(db.execute('SELECT * FROM '+_identifier(name)+' WHERE rowid=?',(identity,)).fetchone())
                wire=old._row_bytes(value,max_metadata_bytes-logical,budget);logical+=len(wire)
                if name in old_columns:
                    for column,expected in policy['additionalColumns'].get(name,{}).items():
                        _require(type(value[column]) is type(expected) and value[column]==expected,'undeclared_added_column_value')
                    value={column:value[column] for column in old_columns[name]}
                values.append(value);encoded.append(old._canonical(value))
            encoded.sort()
            proof=hmac.new(key,b'table\0'+name.encode()+b'\0',hashlib.sha256)
            for wire in encoded:proof.update(len(wire).to_bytes(8,'big'));proof.update(wire)
            if name in old_columns:
                _require(baseline['tables'][name]==dict(count=len(values),content=proof.hexdigest()),'legacy_rows_changed')
            else:
                if name in EMPTY_PAYLOAD_TABLES:_require(values==[],'new_payload_before_cutover')
                if 'startup' in policy:
                    from .coordinator_startup_policy import match_rows
                    matched=match_rows(name,values,declared['rows'][name],gate_binding)
                else:matched=sorted(map(old._canonical,values))==sorted(map(old._canonical,declared['rows'][name]))
                _require(matched,'undeclared_startup_rows')
            table_proofs[name]=dict(count=len(values),content=proof.hexdigest())
        page=db.execute('PRAGMA page_size').fetchone()[0]
        _require(before.st_size<=4*max_metadata_bytes+(4*row_count+256)*page,'candidate_physical_budget_exceeded')
        inventory={};directories={};seen=set();total=0;path_count=0;seen_dirs=set()
        database_id=hmac.new(key,b'path\0jobs.sqlite3',hashlib.sha256).hexdigest()
        for path in root.rglob('*'):
            budget.check();path_count+=1;relative=path.relative_to(root).as_posix();info=path.lstat()
            _require(path_count<=100000 and len(relative)<=4096,'candidate_path_budget_exceeded')
            _require(not path.is_symlink(),'candidate_symlink_refused')
            if stat.S_ISDIR(info.st_mode):
                old._owned(path,uid,directory=True);directories[relative]=old._same(info)
                path_id=hmac.new(key,b'path\0'+relative.encode(),hashlib.sha256).hexdigest()
                expected=baseline['directories'].get(path_id)
                if expected is None:expected=declared['directories'].get(relative)
                _require(expected==dict(mode=stat.S_IMODE(info.st_mode)),'undeclared_directory_or_mode')
                seen_dirs.add(path_id);continue
            path_id=hmac.new(key,b'path\0'+relative.encode(),hashlib.sha256).hexdigest()
            if path_id==database_id:continue
            expected=baseline['files'].get(path_id)
            if expected is None:
                _require(relative in declared['files'],'unknown_candidate_path')
                expected=declared['files'][relative]
            actual=old._hash_file(path,uid,max_artifact_bytes-total,key,budget)
            actual={k:v for k,v in actual.items() if k!='sha256'}
            _require(actual==expected,'retained_or_declared_file_changed')
            inventory[path_id]=actual;seen.add(path_id);total+=actual['bytes']
            _require(total<=max_artifact_bytes,'candidate_artifact_budget_exceeded')
        _require(set(baseline['files'])-{database_id}<=seen,'retained_file_missing')
        _require(set(baseline['directories'])<=seen_dirs,'retained_directory_missing')
        _require(all((root/path).is_file() for path in declared['files']) and
                 all((root/path).is_dir() for path in declared['directories']),'declared_path_missing')
        _require(all(old._same((root/path).lstat())==identity for path,identity in directories.items()) and
                 old._same(database.lstat())==old._same(before) and old._same(root.lstat())==old._same(root_info),
                 'candidate_changed_during_projection')
        return dict(schemaVersion=1,profile=PROFILE,phase=phase,retainedFilesValidated=True,
                    baselineSha256=hashlib.sha256(old._canonical(baseline)).hexdigest(),
                    policySha256=expected_policy_sha256,gateBindingSha256=hashlib.sha256(old._canonical(gate_binding)).hexdigest(),
                    tablesSha256=hashlib.sha256(old._canonical(table_proofs)).hexdigest(),
                    filesSha256=hashlib.sha256(old._canonical(inventory)).hexdigest(),
                    metadataBytes=logical,artifactBytes=total,writerExclusionProven=False,
                    candidateImageId=policy['candidateImageId'],candidateSourceCommit=policy['candidateSourceCommit'])
    finally:db.close()
