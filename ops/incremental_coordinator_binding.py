"""Read-only incremental coordinator qualification; contains no activation API.

Historical native-to-reference update helpers keep their original strict ABI.
This module binds a new operation to an already reference-enabled predecessor
and retains its complete owned-maintenance receipt history.
"""
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat

from processing.maintenance import Maintenance,SCHEDULE_FIELDS,canonical_uuid,utc_stamp


class IncrementalHeld(ValueError):pass


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(value):return hashlib.sha256(encoded(value)).hexdigest()
def require(condition,reason):
    if not condition:raise IncrementalHeld(reason)

def sql_shape(sql):
    """Preserve quoted bytes; this is not a full SQLite token parser.

    Removing unquoted whitespace can conflate distinct SQL tokens; this helper
    cannot by itself qualify schema for a production activation decision.
    """
    require(type(sql) is str,'schema_sql_required')
    out=[];quoted=None;i=0
    while i<len(sql):
        char=sql[i]
        if quoted is None and (sql[i:i+2] in ('--','/*')):raise IncrementalHeld('schema_comments_refused')
        if quoted is None and char in ("'",'"','`','['):
            out.append(char)
            quoted=']' if char=='[' else char
        elif quoted is not None and char==quoted:
            out.append(char)
            if quoted!=']' and i+1<len(sql) and sql[i+1]==quoted:
                out.append(quoted);i+=2;continue
            quoted=None
        elif quoted is not None or not char.isspace():out.append(char)
        i+=1
    require(quoted is None,'unterminated_schema_literal')
    return ''.join(out)

# Exact qualified preparation profiles; this is not live activation permission.
BASELINE_OWNED_COUNTS = {
    ('671fd2a6bde40088bdfad91ccf67eab45ee3d8d7', '6bbd468e1177746cb695e43195e75355b0681f23'): 2,
    ('6bbd468e1177746cb695e43195e75355b0681f23', 'd738edb209972a1723fa5af374578951b2a8448c'): 4,
}


@dataclass(frozen=True)
class IncrementalBinding:
    predecessor_container:str
    predecessor_image:str
    predecessor_source:str
    candidate_source:str
    authority_id:str
    application_environment_sha256:str
    private_config_sha256:str
    main_container:str
    main_image:str
    cluster_container:str
    cluster_image:str
    jobs_rows_sha256:str
    owned_history_sha256:str
    schema_sha256:str

    def __post_init__(self):
        for value in (self.predecessor_container,self.main_container,self.cluster_container,self.application_environment_sha256,self.private_config_sha256,self.jobs_rows_sha256,self.owned_history_sha256,self.schema_sha256):require(type(value) is str and re.fullmatch('[a-f0-9]{64}',value),'immutable_local_identity_required')
        for value in (self.predecessor_image,self.main_image,self.cluster_image):require(type(value) is str and re.fullmatch('sha256:[a-f0-9]{64}',value),'immutable_image_required')
        for value in (self.predecessor_source,self.candidate_source):require(type(value) is str and re.fullmatch('[a-f0-9]{40}',value),'full_source_required')
        canonical_uuid(self.authority_id)
        require(self.predecessor_source!=self.candidate_source and self.main_container!=self.cluster_container,'distinct_upgrade_identity_required')

    def check_baseline(self,census):
        expected = BASELINE_OWNED_COUNTS.get((self.predecessor_source, self.candidate_source))
        require(expected is not None and census['authorityId']==self.authority_id and census['sqliteQuickCheck']=='ok' and len(census['ownedRows'])==expected,'installed_baseline_changed')
        require(census['tables']['jobs']['sha256']==self.jobs_rows_sha256 and digest(census['ownedRows'])==self.owned_history_sha256 and digest(census['schema'])==self.schema_sha256,'installed_history_or_schema_changed')
        require(all(row['state']=='active' for row in census['nodeRows'].values()),'other_maintenance_present')

    def checkpoint_identity(self):
        """Data for a future explicit existing-updater profile, not an executor."""
        return dict(old=dict(containerId=self.predecessor_container,image=self.predecessor_image),oldSourceCommit=self.predecessor_source,sourceCommit=self.candidate_source,authorityId=self.authority_id,engineImages=dict(main=self.main_image,cluster=self.cluster_image),mainContainerId=self.main_container,clusterContainerId=self.cluster_container,applicationEnvironmentSha256=self.application_environment_sha256,privateConfigSha256=self.private_config_sha256,jobsRowsSha256=self.jobs_rows_sha256,ownedHistorySha256=self.owned_history_sha256,schemaSha256=self.schema_sha256)


TABLES={'jobs','sqlite_sequence','coordinator_identity','nodes','viewer_tasks','viewer_files','viewer_incarnations','reference_jobs','reference_acceptances','reference_outbox','reference_claims','reference_worker_receipts','reference_transfer_grants','reference_worker_failures','reference_terminal_acceptances','owned_maintenance_schedules'}
PILOT_RESERVATION_SQL = """CREATE TABLE viewer_reservations (
                uuid TEXT PRIMARY KEY, environment TEXT NOT NULL, capability_hash TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('reserved','cancelled')))"""
F24_OPTION_EVIDENCE_SQL = """CREATE TABLE viewer_option_evidence (
                uuid TEXT NOT NULL, generation INTEGER NOT NULL,
                definitions TEXT NOT NULL, sha256 TEXT NOT NULL,
                PRIMARY KEY(uuid,generation))"""
OWNED_COLUMNS=('request_id','node_id','authority_id','request_sha256','request_json','receipt_json')
RECEIPT_FIELDS={'schemaVersion','requestId','authorityId','nodeId','requestSha256','expectedRevision','revision','planId','state','drainAt','notBefore','afterJobId','targetImage','receiptSha256'}


def _inventory(root):
    paths=[]
    with os.scandir(root) as entries:
        for entry in entries:
            require(len(paths)<4,'state_inventory_bound')
            paths.append(root/entry.name)
    return paths


def owned_row(record,authority):
    """Qualify historical bytes; a valid old receipt grants no current control."""
    require(type(record) is dict and set(record)==set(OWNED_COLUMNS),'owned_row_shape_changed')
    require(all(type(value) is str and len(value.encode())<=8192 for value in record.values()),'owned_row_bound')
    request=json.loads(record['request_json']);receipt=json.loads(record['receipt_json'])
    require(type(request) is dict and set(request)=={'schemaVersion','requestId','authorityId',*SCHEDULE_FIELDS},'owned_request_shape_changed')
    canonical_uuid(record['request_id']);canonical_uuid(authority)
    require(request['schemaVersion']==1 and type(request['schemaVersion']) is int and request['requestId']==record['request_id'] and record['authority_id']==authority==request['authorityId'],'owned_request_identity_changed')
    require(record['node_id'] in ('cluster','main') and request['afterJobId'] is None,'idle_owned_history_required')
    require(type(request['expectedRevision']) is int and 0<=request['expectedRevision']<2**53-1,'owned_revision_changed')
    Maintenance._validate_schedule(request)
    require(encoded(request).decode()==record['request_json'] and digest(dict(nodeId=record['node_id'],request=request))==record['request_sha256'],'owned_request_bytes_changed')
    require(type(receipt) is dict and set(receipt)==RECEIPT_FIELDS,'owned_receipt_shape_changed')
    canonical_uuid(receipt['planId'])
    expected={key:request[key] for key in ('schemaVersion','requestId','authorityId','expectedRevision','afterJobId','targetImage')}
    expected.update(nodeId=record['node_id'],requestSha256=record['request_sha256'],revision=request['expectedRevision']+1,state='draining',planId=receipt['planId'],drainAt=utc_stamp(request['drainAt'])[1],notBefore=utc_stamp(request['notBefore'])[1])
    expected['receiptSha256']=digest(expected)
    require(encoded(receipt)==encoded(expected) and encoded(receipt).decode()==record['receipt_json'],'owned_receipt_bytes_changed')
    return dict(record)


def volume_census(root,*,authority,uid=10001,pilot_reservations=False,pilot_f24=False,legacy_task_rows=False):
    require(type(pilot_reservations) is bool and type(pilot_f24) is bool and type(legacy_task_rows) is bool,'explicit_pilot_census_required')
    require(not pilot_f24 or pilot_reservations,'f24_requires_reservation_profile')
    """Bounded current schema1 idle census; read-only SQL/files, no gate creation."""
    root=Path(root).absolute();info=root.lstat()
    require(root.resolve()==root and not any(p.is_symlink() for p in (root,*root.parents)) and stat.S_ISDIR(info.st_mode) and info.st_uid==uid and info.st_gid==uid,'owned_state_root_required')
    files=[];fingerprints={}
    paths=_inventory(root)
    for path in sorted(paths):
        relative=path.relative_to(root).as_posix();before=path.lstat()
        fingerprints[path]=(before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)
        require(relative in ('jobs.sqlite3','coordinator.lock','viewer-tasks','viewer-spool') and before.st_uid==uid and before.st_gid==uid and not stat.S_ISLNK(before.st_mode),'unexpected_state_file')
        if stat.S_ISDIR(before.st_mode):
            with os.scandir(path) as entries:require(next(entries,None) is None,'native_directories_not_empty')
            files.append(dict(path=relative,kind='directory',mode=stat.S_IMODE(before.st_mode)));continue
        require(stat.S_ISREG(before.st_mode) and before.st_nlink==1 and before.st_size<=64*1024**2,'bounded_owned_state_file_required')
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        try:
            opened=os.fstat(fd);require((opened.st_dev,opened.st_ino)==(before.st_dev,before.st_ino),'state_file_replaced')
            sha=hashlib.sha256();size=0;header=b''
            while True:
                data=os.read(fd,65536)
                if not data:break
                if not size:header=data[:100]
                size+=len(data);require(size<=before.st_size,'state_file_grew');sha.update(data)
            after=path.lstat();require(size==before.st_size and (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)==(before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns),'state_file_changed')
        finally:os.close(fd)
        if relative=='jobs.sqlite3':
            # SQLite mode=ro can still create WAL shared-memory files. Refuse
            # non-DELETE physical format before making any SQLite connection.
            require(len(header)==100 and header[:16]==b'SQLite format 3\0' and header[16:20]==b'\x10\x00\x01\x01','delete_mode_state_header_required')
        files.append(dict(path=relative,kind='file',mode=stat.S_IMODE(before.st_mode),bytes=size,sha256=sha.hexdigest()))
    require(any(item['path']=='jobs.sqlite3' for item in files),'state_database_required')
    db=sqlite3.connect('file:'+str(root/'jobs.sqlite3')+'?mode=ro',uri=True,timeout=3)
    try:
        db.execute('BEGIN');require(db.execute('PRAGMA quick_check').fetchone()[0]=='ok','state_integrity_failed')
        schema=[list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        expected_tables = TABLES | ({'viewer_reservations'} if pilot_reservations else set()) | ({'viewer_option_evidence'} if pilot_f24 else set())
        require({r[1] for r in schema if r[0]=='table'}==expected_tables and not any(r[0] in ('trigger','view') for r in schema),'coordinator_schema_changed')
        if pilot_reservations:
            reservation = [r for r in schema if r[0]=='table' and r[1]=='viewer_reservations']
            require(len(reservation)==1 and ''.join(reservation[0][3].split())==''.join(PILOT_RESERVATION_SQL.split()),'pilot_reservation_schema_changed')
            if pilot_f24:require(sql_shape(reservation[0][3])==sql_shape(PILOT_RESERVATION_SQL),'f24_reservation_schema_changed')
        if pilot_f24:
            evidence=[r for r in schema if r[0]=='table' and r[1]=='viewer_option_evidence']
            require(len(evidence)==1 and sql_shape(evidence[0][3])==sql_shape(F24_OPTION_EVIDENCE_SQL),'f24_evidence_schema_changed')
        require(all(r[0]!='index' or r[1].startswith('sqlite_autoindex_') or r[1]=='viewer_engine_uuid' for r in schema),'coordinator_index_changed')
        require(db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()[0]==authority,'coordinator_authority_changed')
        tables={}
        for name in sorted(expected_tables):
            count=db.execute('SELECT COUNT(*) FROM "'+name+'"').fetchone()[0]
            cap=64 if name=='owned_maintenance_schedules' or (name=='viewer_tasks' and legacy_task_rows) else 12 if name=='jobs' else 2 if name=='nodes' else 1 if name in ('sqlite_sequence','coordinator_identity') else 0
            require(count<=cap,'table_census_bound')
            columns=[list(row) for row in db.execute('PRAGMA table_info("'+name+'")')]
            total=' + '.join('COALESCE(length(CAST("'+column[1]+'" AS BLOB)),0)' for column in columns)
            require(db.execute('SELECT COALESCE(SUM('+total+'),0) FROM "'+name+'"').fetchone()[0]<=1024**2,'table_byte_bound')
            rows=[list(row) for row in db.execute('SELECT * FROM "'+name+'"')];rows.sort(key=encoded)
            require(len(encoded(rows))<=1024**2,'table_byte_bound')
            tables[name]=dict(count=count,sha256=digest(rows),columns=columns)
            if pilot_f24 and name=='viewer_tasks':
                require(columns[-1][1:] == ['option_evidence_required','INTEGER',1,'0',0],'f24_marker_column_changed')
                require(all(row[-1] == 0 and type(row[-1]) is int for row in rows),'f24_legacy_marker_changed')
                projected=[row[:-1] for row in rows];projected.sort(key=encoded)
                tables[name]['legacyRowsSha256']=digest(projected)
        require(tables['jobs']['count']==12 and db.execute("SELECT COUNT(*) FROM jobs WHERE status NOT IN ('succeeded','failed')").fetchone()[0]==0,'reviewed_terminal_history_required')
        require(tuple(r[1] for r in db.execute('PRAGMA table_info(owned_maintenance_schedules)'))==OWNED_COLUMNS,'owned_columns_changed')
        owned=[owned_row(dict(zip(OWNED_COLUMNS,row)),authority) for row in db.execute('SELECT * FROM owned_maintenance_schedules ORDER BY request_id')]
        node_columns=[r[1] for r in db.execute('PRAGMA table_info(nodes)')]
        nodes={row[0]:dict(zip(node_columns,row)) for row in db.execute('SELECT * FROM nodes ORDER BY id')}
        require(set(nodes)=={'cluster','main'},'node_inventory_changed')
        require(set(_inventory(root))==set(fingerprints),'state_inventory_changed_during_census')
        for path,original in fingerprints.items():
            after=path.lstat();require((after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)==original,'state_changed_during_sql_census')
        return dict(authorityId=authority,schema=schema,tables=tables,files=files,nodeRows=nodes,ownedRows=owned,rootMode=stat.S_IMODE(info.st_mode),sqliteQuickCheck='ok')
    finally:db.close()


def compare_f24_migration(observed, baseline):
    """Compare d738→f24 additive schema offline; never grant activation.

    The caller must separately fence all writers. This bounded comparison is
    not a backup, rollback authorization, or a live admission fence. The
    existing census still refuses retained task directories and files.
    """
    require(type(observed) is dict and type(baseline) is dict,'f24_census_required')
    candidate=json.loads(encoded(observed));original=json.loads(encoded(baseline))
    for name in ('viewer_reservations','viewer_option_evidence'):
        added=[row for row in candidate.get('schema',[]) if row[2]==name]
        require(len(added)==2 and {row[0] for row in added}=={'table','index'},'f24_schema_delta_changed')
        expected_sql=PILOT_RESERVATION_SQL if name=='viewer_reservations' else F24_OPTION_EVIDENCE_SQL
        table_rows=[row for row in added if row[0]=='table']
        require(len(table_rows)==1 and sql_shape(table_rows[0][3])==sql_shape(expected_sql),'f24_new_table_schema_changed')
        require(any(row[0]=='index' and row[1]=='sqlite_autoindex_'+name+'_1' and row[3] is None for row in added),'f24_new_index_changed')
        table=candidate.get('tables',{}).get(name)
        require(type(table) is dict and table.get('count')==0 and table.get('sha256')==digest([]),'f24_new_tables_not_empty')
        require(name not in original.get('tables',{}),'f24_predecessor_schema_changed')
        candidate['schema']=[row for row in candidate['schema'] if row[2]!=name]
        del candidate['tables'][name]
    oldrows=[row for row in original['schema'] if row[0]=='table' and row[1]=='viewer_tasks']
    newrows=[row for row in candidate['schema'] if row[0]=='table' and row[1]=='viewer_tasks']
    require(len(oldrows)==len(newrows)==1,'f24_task_schema_missing')
    oldsql=oldrows[0][3];require(type(oldsql) is str and oldsql.endswith(')'),'f24_predecessor_schema_changed')
    expected=oldsql[:-1]+', option_evidence_required INTEGER NOT NULL DEFAULT 0 CHECK(option_evidence_required IN (0,1)))'
    require(sql_shape(newrows[0][3])==sql_shape(expected),'f24_task_schema_changed')
    newrows[0][3]=oldsql
    current=candidate['tables']['viewer_tasks'];prior=original['tables']['viewer_tasks']
    require(current['columns'][-1][1:] == ['option_evidence_required','INTEGER',1,'0',0],'f24_marker_column_changed')
    require(current.pop('legacyRowsSha256',None)==prior['sha256'],'f24_legacy_rows_changed')
    current['sha256']=prior['sha256'];current['columns']=current['columns'][:-1]
    for value in (candidate,original):
        value['files']=[{k:v for k,v in item.items() if item['path']!='jobs.sqlite3' or k not in ('bytes','sha256')} for item in value['files']]
    require(candidate==original,'f24_immutable_state_changed')


def compare_pilot_reservation_migration(observed, baseline):
    """Compare an offline empty-reservation migration; never authorize cutover.

    Callers must independently fence writers and prove exact source/image and
    volume identities. This profile refuses any reservation row or schema
    difference beyond the one expected empty table and its automatic index.
    """
    require(type(observed) is dict and type(baseline) is dict,'pilot_census_required')
    candidate=json.loads(encoded(observed))
    original=json.loads(encoded(baseline))
    added=[row for row in candidate.get('schema',[]) if row[2]=='viewer_reservations']
    require(len(added)==2 and {row[0] for row in added}=={'table','index'},'pilot_schema_delta_changed')
    table=candidate.get('tables',{}).get('viewer_reservations')
    require(type(table) is dict and table.get('count')==0 and table.get('sha256')==digest([]),'pilot_reservations_must_be_empty')
    require('viewer_reservations' not in original.get('tables',{}),'pilot_predecessor_schema_changed')
    candidate['schema']=[row for row in candidate['schema'] if row[2]!='viewer_reservations']
    del candidate['tables']['viewer_reservations']
    for value in (candidate, original):
        value['files']=[{k:v for k,v in item.items() if item['path']!='jobs.sqlite3' or k not in ('bytes','sha256')} for item in value['files']]
    require(candidate==original,'pilot_immutable_state_changed')


def compare_transition(observed,baseline,*,binding,plans=(),cancel_intents=(),reopened=()):
    """Allow exact operation receipts/drain/cancel CAS; retain all predecessor rows.

    plans is a tuple of the actual FULL-persisted request/receipt rows, never a
    matching target-image guess. This checker neither issues nor replays them.
    """
    require(type(binding) is IncrementalBinding,'explicit_incremental_binding_required');binding.check_baseline(baseline)
    authority=baseline['authorityId'];require(observed['authorityId']==authority,'census_authority_changed')
    require(type(plans) is tuple and len(plans)<=2 and type(cancel_intents) is tuple and len(cancel_intents)<=2 and type(reopened) is tuple and len(set(reopened))==len(reopened) and set(reopened)<= {'cluster','main'},'closed_transition_required')
    cancellations={}
    for intent in cancel_intents:
        require(type(intent) is dict and set(intent)=={'nodeId','expectedRevision','planId'} and intent['nodeId'] in ('cluster','main') and intent['nodeId'] not in cancellations,'closed_owned_cancel_intent_required')
        cancellations[intent['nodeId']]=intent
    old={r['request_id']:owned_row(r,authority) for r in baseline['ownedRows']}
    additions={};nodes={key:dict(value) for key,value in baseline['nodeRows'].items()}
    for value in plans:
        record=owned_row(value,authority);request=json.loads(record['request_json']);receipt=json.loads(record['receipt_json']);node=record['node_id']
        require(request['targetImage']==dict(main=binding.main_image,cluster=binding.cluster_image)[node],'operation_backend_image_changed')
        require(record['request_id'] not in old and record['request_id'] not in additions and all(r['node_id']!=node for r in additions.values()),'operation_receipt_collision')
        original=nodes[node];require(original['state']=='active' and original['revision']==request['expectedRevision'],'operation_node_cas_changed')
        original.update(state='draining',revision=receipt['revision'],plan_id=receipt['planId'],drain_at=receipt['drainAt'],not_before=receipt['notBefore'],after_job_id=None,target_image=receipt['targetImage'],result=None,update_hash=None,finish_status=None)
        if node in cancellations:require(encoded(cancellations[node])==encoded(dict(nodeId=node,expectedRevision=receipt['revision'],planId=receipt['planId'])),'owned_cancel_cas_changed')
        if node in reopened:
            require(node in cancellations,'reopen_requires_owned_cancel_intent');original.update(state='active',revision=receipt['revision']+1)
        additions[record['request_id']]=record
    require(set(reopened)<={r['node_id'] for r in additions.values()},'reopen_requires_owned_plan')
    require(set(cancellations)<={r['node_id'] for r in additions.values()},'cancel_requires_owned_plan')
    require(observed['ownedRows']==sorted([*old.values(),*additions.values()],key=lambda r:r['request_id']),'owned_history_or_operation_changed')
    require(observed['nodeRows']==nodes,'unowned_node_transition')
    for value in (baseline,observed):
        for table,rows in (('nodes',list(value['nodeRows'].values())),('owned_maintenance_schedules',value['ownedRows'])):
            columns=value['tables'][table]['columns'];names=[row[1] for row in columns]
            materialized=[[row[key] for key in names] for row in rows];materialized.sort(key=encoded)
            require(value['tables'][table]['count']==len(rows) and value['tables'][table]['sha256']==digest(materialized),'mutable_table_census_inconsistent')
            require(columns==baseline['tables'][table]['columns'],'mutable_table_columns_changed')
    def stable(value):
        result=json.loads(encoded(value));result.pop('ownedRows');result.pop('nodeRows')
        result['tables'].pop('nodes');result['tables'].pop('owned_maintenance_schedules')
        result['files']=[{k:v for k,v in item.items() if item['path']!='jobs.sqlite3' or k not in ('bytes','sha256')} for item in result['files']]
        return result
    require(stable(observed)==stable(baseline),'immutable_census_changed')
