"""Unqualified, explicit offline intake DB migration experiment.

Not a filesystem fence. Never call on live state. Old SQLite connections without
our function fail closed on guarded writes; arbitrary SQL writers can forge it.
The prototype requires empty Viewer state and cannot migrate existing projects.
"""
import sqlite3
from .viewer_tasks import INTAKE_JOURNAL_DDL, INTAKE_INDEX_DDL

QUALIFIED = False
AUTHORIZE = 'viewer_intake_writer_v1'

TRIGGERS = {}
for table in ('viewer_tasks','viewer_files'):
    for action in ('INSERT','UPDATE','DELETE'):
        name=f'viewer_intake_guard_{table}_{action.lower()}_v1'
        TRIGGERS[name]=(f'CREATE TRIGGER {name} BEFORE {action} ON {table} '
                        f'WHEN {AUTHORIZE}() IS NOT 1 BEGIN SELECT RAISE(ABORT, '
                        "'viewer intake writer required'); END")

# INSERT OR REPLACE must not bypass DELETE triggers when recursive_triggers is off.
for table, predicate in (
    ('viewer_tasks', 'uuid=NEW.uuid'),
    ('viewer_files', 'uuid=NEW.uuid AND name_key=NEW.name_key'),
):
    name=f'viewer_intake_guard_{table}_replace_v1'
    TRIGGERS[name]=(f'CREATE TRIGGER {name} BEFORE INSERT ON {table} '
                    f'WHEN EXISTS(SELECT 1 FROM {table} WHERE {predicate}) '
                    "BEGIN SELECT RAISE(ABORT, 'existing intake identity'); END")

# Bind the prior snapshot to actual rows before any input row is deleted.
_TASK_SNAPSHOT_COLUMNS=('uuid','generation','directory','name','options','outputs','environment',
    'stage','phase','job_id','node_id','engine_uuid','claim_token','error','progress',
    'output_lines','archive_path','removed','created_at','updated_at','option_evidence_required')
_FILE_SNAPSHOT_COLUMNS=('uuid','name_key','name','path','bytes','sha256','is_photo')
_task_differences=' OR '.join(f"(json_type(NEW.metadata, '$.{c}') IS NULL OR json_extract(NEW.metadata, '$.{c}') IS NOT t.{c})" for c in _TASK_SNAPSHOT_COLUMNS)
_file_differences=' OR '.join(f"(json_type(j.value, '$.{c}') IS NULL OR json_extract(j.value, '$.{c}') IS NOT f.{c})" for c in _FILE_SNAPSHOT_COLUMNS)
TRIGGERS['viewer_intake_guard_incarnation_insert_v1']=(
    'CREATE TRIGGER viewer_intake_guard_incarnation_insert_v1 BEFORE INSERT ON viewer_incarnations '
    f"WHEN {AUTHORIZE}() IS NOT 1 OR EXISTS(SELECT 1 FROM viewer_incarnations WHERE uuid=NEW.uuid AND generation=NEW.generation) "
    "OR json_valid(NEW.metadata) IS NOT 1 OR json_valid(NEW.files) IS NOT 1 "
    "OR json_type(NEW.metadata) IS NOT 'object' OR json_type(NEW.files) IS NOT 'array' "
    f"OR (SELECT COUNT(*) FROM json_each(NEW.metadata)) IS NOT {len(_TASK_SNAPSHOT_COLUMNS)} "
    f"OR EXISTS(SELECT 1 FROM json_each(NEW.files) j WHERE json_type(j.value) IS NOT 'object' OR (SELECT COUNT(*) FROM json_each(j.value)) IS NOT {len(_FILE_SNAPSHOT_COLUMNS)}) "
    "OR NOT EXISTS(SELECT 1 FROM viewer_tasks t WHERE t.uuid=NEW.uuid AND t.generation=NEW.generation "
    f"AND NEW.job_id IS t.job_id AND NOT ({_task_differences})) "
    "OR (SELECT COUNT(*) FROM json_each(NEW.files)) IS NOT (SELECT COUNT(*) FROM viewer_files WHERE uuid=NEW.uuid) "
    "OR EXISTS(SELECT 1 FROM viewer_files f WHERE f.uuid=NEW.uuid AND NOT EXISTS(SELECT 1 FROM json_each(NEW.files) j "
    f"WHERE NOT ({_file_differences}))) "
    "BEGIN SELECT RAISE(ABORT, 'invalid intake snapshot'); END")
for action in ('UPDATE','DELETE'):
    name=f'viewer_intake_guard_incarnation_{action.lower()}_v1'
    TRIGGERS[name]=(f'CREATE TRIGGER {name} BEFORE {action} ON viewer_incarnations '
                    "BEGIN SELECT RAISE(ABORT, 'intake snapshot immutable'); END")

_prior_file_mismatch=' OR '.join(f"(json_type(j.value, '$.{c}') IS NULL OR json_extract(j.value, '$.{c}') IS NOT OLD.{c})" for c in _FILE_SNAPSHOT_COLUMNS)
TRIGGERS['viewer_intake_guard_file_delete_snapshot_v1']=(
    'CREATE TRIGGER viewer_intake_guard_file_delete_snapshot_v1 BEFORE DELETE ON viewer_files '
    "WHEN NOT EXISTS(SELECT 1 FROM viewer_tasks t JOIN viewer_incarnations i ON i.uuid=t.uuid AND i.generation=t.generation "
    "JOIN json_each(i.files) j WHERE t.uuid=OLD.uuid "
    f"AND NOT ({_prior_file_mismatch})) "
    "BEGIN SELECT RAISE(ABORT, 'matching intake snapshot required'); END")
for action in ('INSERT','UPDATE'):
    name=f'viewer_intake_guard_file_{action.lower()}_snapshot_v1'
    TRIGGERS[name]=(f'CREATE TRIGGER {name} BEFORE {action} ON viewer_files '
                    "WHEN EXISTS(SELECT 1 FROM viewer_tasks t JOIN viewer_incarnations i "
                    "ON i.uuid=t.uuid AND i.generation=t.generation WHERE t.uuid=NEW.uuid) "
                    "BEGIN SELECT RAISE(ABORT, 'snapshotted intake immutable'); END")
_old_task_diff=' OR '.join(f"(json_type(i.metadata, '$.{c}') IS NULL OR json_extract(i.metadata, '$.{c}') IS NOT OLD.{c})" for c in _TASK_SNAPSHOT_COLUMNS)

TRIGGERS['viewer_intake_guard_generation_v1']=(
    'CREATE TRIGGER viewer_intake_guard_generation_v1 BEFORE UPDATE ON viewer_tasks '
    'WHEN (NEW.generation IS NOT OLD.generation OR NEW.directory IS NOT OLD.directory) AND ('
    "OLD.removed IS NOT 1 OR NEW.removed IS NOT 0 OR typeof(OLD.generation) IS NOT 'integer' "
    "OR NEW.generation IS NOT OLD.generation+1 OR typeof(NEW.directory) IS NOT 'text' "
    "OR NOT (NEW.directory=NEW.uuid||'-'||NEW.generation OR (length(NEW.directory)=length(NEW.uuid||'-'||NEW.generation)+17 "
    "AND substr(NEW.directory,1,length(NEW.uuid||'-'||NEW.generation)+1)=NEW.uuid||'-'||NEW.generation||'-' "
    "AND substr(NEW.directory,-16) NOT GLOB '*[^0-9a-f]*')) "
    "OR EXISTS(SELECT 1 FROM viewer_tasks WHERE uuid IS NOT OLD.uuid AND directory=NEW.directory) "
    "OR NEW.environment IS NOT OLD.environment OR NEW.stage IS NOT 'uploading' OR NEW.job_id IS NOT NULL "
    "OR EXISTS(SELECT 1 FROM viewer_files WHERE uuid=OLD.uuid) "
    f"OR NOT EXISTS(SELECT 1 FROM viewer_incarnations i WHERE i.uuid=OLD.uuid AND i.generation=OLD.generation AND i.job_id IS OLD.job_id AND NOT ({_old_task_diff})) "
    "OR EXISTS(SELECT 1 FROM viewer_intake_journal_v1 WHERE uuid=OLD.uuid AND generation=OLD.generation AND state!='committed')) "
    "BEGIN SELECT RAISE(ABORT, 'invalid intake generation'); END")

TRIGGERS['viewer_intake_guard_insert_v1']=(
    'CREATE TRIGGER viewer_intake_guard_insert_v1 BEFORE INSERT ON viewer_intake_journal_v1 '
    f"WHEN {AUTHORIZE}() IS NOT 1 OR NEW.state IS NOT 'prepared' OR NEW.sha256 IS NOT NULL "
    "OR typeof(NEW.operation) IS NOT 'text' OR typeof(NEW.uuid) IS NOT 'text' OR typeof(NEW.generation) IS NOT 'integer' OR NEW.generation<1 OR typeof(NEW.name_key) IS NOT 'text' OR typeof(NEW.directory) IS NOT 'text' OR typeof(NEW.temporary) IS NOT 'text' OR typeof(NEW.final) IS NOT 'text' OR NEW.temporary IS NEW.final OR typeof(NEW.bytes) IS NOT 'integer' OR NEW.bytes<1 OR EXISTS(SELECT 1 FROM viewer_intake_journal_v1 WHERE operation=NEW.operation OR (uuid=NEW.uuid AND generation=NEW.generation AND name_key=NEW.name_key) OR (directory=NEW.directory AND (temporary IN (NEW.temporary,NEW.final) OR final IN (NEW.temporary,NEW.final)))) BEGIN SELECT RAISE(ABORT, 'invalid intake insert'); END")
TRIGGERS['viewer_intake_guard_update_v1']=(
    'CREATE TRIGGER viewer_intake_guard_update_v1 BEFORE UPDATE ON viewer_intake_journal_v1 '
    f'WHEN {AUTHORIZE}() IS NOT 1 OR NEW.operation IS NOT OLD.operation OR NEW.uuid IS NOT OLD.uuid '
    'OR NEW.generation IS NOT OLD.generation OR NEW.name_key IS NOT OLD.name_key '
    'OR NEW.directory IS NOT OLD.directory OR NEW.temporary IS NOT OLD.temporary '
    'OR NEW.final IS NOT OLD.final OR NEW.bytes IS NOT OLD.bytes '
    "OR NOT ((OLD.state='prepared' AND NEW.state='sealed' AND typeof(NEW.sha256)='text' "
    "AND length(NEW.sha256)=64 AND NEW.sha256 NOT GLOB '*[^0-9a-f]*') "
    "OR (OLD.state='sealed' AND NEW.state='cleanup' AND NEW.sha256 IS OLD.sha256) "
    "OR (OLD.state='cleanup' AND NEW.state='committed' AND NEW.sha256 IS OLD.sha256)) "
    "BEGIN SELECT RAISE(ABORT, 'invalid intake transition'); END")
TRIGGERS['viewer_intake_guard_delete_v1']=(
    'CREATE TRIGGER viewer_intake_guard_delete_v1 BEFORE DELETE ON viewer_intake_journal_v1 '
    "BEGIN SELECT RAISE(ABORT, 'intake evidence cannot be deleted'); END")

class MigrationUnavailable(RuntimeError):
    pass

def _schema(db, kind, name):
    result=db.execute('SELECT sql FROM sqlite_master WHERE type=? AND name=?',(kind,name)).fetchone()
    return result[0] if result else None

def attest(db):
    expected=[('table','viewer_intake_journal_v1',INTAKE_JOURNAL_DDL),
              ('index','viewer_intake_unfinished_v1',INTAKE_INDEX_DDL)]
    expected += [('trigger',name,sql) for name,sql in TRIGGERS.items()]
    if any(_schema(db,kind,name)!=sql for kind,name,sql in expected):
        raise MigrationUnavailable('intake schema or trigger changed')
    return True

def install_empty_offline(db):
    """Caller must first independently fence all writers and back up state.

    Conservatively refuses any existing Viewer tasks/files or intake artifacts.
    This does not verify that a filesystem fence exists and is not a qualified
    production migration. Schema creation is one SQLite transaction.
    """
    if not QUALIFIED: raise MigrationUnavailable('intake migration not qualified')
    if db.in_transaction: raise MigrationUnavailable('offline connection required')
    db.execute('BEGIN IMMEDIATE')
    try:
        for table in ('viewer_tasks','viewer_files'):
            if _schema(db,'table',table) is None or db.execute(f'SELECT 1 FROM {table} LIMIT 1').fetchone():
                raise MigrationUnavailable('empty Viewer state required')
        if db.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'viewer_intake_%' LIMIT 1").fetchone():
            raise MigrationUnavailable('existing intake schema requires inspection')
        db.execute(INTAKE_JOURNAL_DDL);db.execute(INTAKE_INDEX_DDL)
        for sql in TRIGGERS.values():db.execute(sql)
        attest(db)
        db.commit()
    except BaseException:
        db.rollback()
        raise

def authorize_connection(db):
    """Test-only cooperative DB capability; caller must own a fenced lease."""
    if not QUALIFIED: raise MigrationUnavailable('intake migration not qualified')
    attest(db)
    db.create_function(AUTHORIZE,0,lambda:1)
