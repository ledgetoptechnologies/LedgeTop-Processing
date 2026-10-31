"""Exact optional inventory revision schema. No installation on import."""
from .maintenance import Conflict

TABLE = "CREATE TABLE viewer_inventory_revision (id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL CHECK(typeof(revision)='integer' AND revision>=0))"
TRIGGERS = {}
TABLES = ('viewer_files', 'viewer_upload_intents', 'viewer_tasks', 'viewer_reservations')
for table in TABLES:
    for event in ('INSERT', 'UPDATE', 'DELETE'):
        name = f'viewer_inventory_{table}_{event.lower()}'
        TRIGGERS[name] = (f"CREATE TRIGGER {name} AFTER {event} ON {table} BEGIN "
            "UPDATE viewer_inventory_revision SET revision=revision+1 WHERE id=1 AND revision<9007199254740991; "
            "SELECT CASE WHEN changes()!=1 THEN RAISE(ABORT,'inventory revision unavailable') END; END")


def validate(db, *, required=False):
    rows = db.execute("SELECT name,type,sql,tbl_name FROM sqlite_master WHERE name GLOB 'viewer_inventory_*'").fetchall()
    if not rows:
        if required:
            raise Conflict('inventory revision schema unavailable')
        return None
    actual = {r[0]:(r[1],r[2],r[3]) for r in rows}
    expected = {'viewer_inventory_revision':('table',TABLE,'viewer_inventory_revision'),
                **{name:('trigger',sql,next(t for t in TABLES if name.startswith('viewer_inventory_'+t+'_'))) for name,sql in TRIGGERS.items()}}
    if actual != expected:
        raise Conflict('inventory revision schema unavailable')
    if db.execute("SELECT 1 FROM sqlite_master WHERE tbl_name='viewer_inventory_revision' AND type IN ('index','trigger') LIMIT 1").fetchone():
        raise Conflict('unexpected inventory revision object')
    triggers = db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name IN (?,?,?,?)", TABLES).fetchall()
    if {r[0] for r in triggers} != set(TRIGGERS):
        raise Conflict('unexpected inventory table trigger')
    row = db.execute('SELECT id,revision,typeof(revision) FROM viewer_inventory_revision').fetchall()
    if len(row)!=1 or row[0][0]!=1 or row[0][2]!='integer' or not 0 <= row[0][1] <= 9007199254740991:
        raise Conflict('inventory revision unavailable')
    return row[0][1]


def install(db):
    if validate(db) is None:
        db.execute(TABLE)
        db.execute('INSERT INTO viewer_inventory_revision(id,revision) VALUES(1,0)')
        for sql in TRIGGERS.values():
            db.execute(sql)
    return validate(db, required=True)
