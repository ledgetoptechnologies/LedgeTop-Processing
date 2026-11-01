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
