"""Explicit isolated initialization of the unqualified staged-claim schema.

The caller must independently fence every writer and qualify upgrade, backup,
restore and rollback. This helper supplies none of those guarantees and never
authorizes receiver activation. Existing experimental schemas are not migrated:
their missing historical capability verifiers cannot be reconstructed safely.
"""

ACK_SQL = '''CREATE TABLE viewer_staged_acks (
                task_id TEXT NOT NULL, generation INTEGER NOT NULL, environment TEXT NOT NULL,
                principal_id TEXT NOT NULL, audience TEXT NOT NULL, namespace TEXT NOT NULL,
                attempt_id TEXT NOT NULL UNIQUE, staging_commit_id TEXT NOT NULL UNIQUE,
                capability_sha256 TEXT NOT NULL,
                assertion_sha256 TEXT NOT NULL, assertion_json TEXT NOT NULL,
                PRIMARY KEY(task_id,generation))'''
MIGRATION_SQL = '''CREATE TABLE viewer_staged_ack_migrations (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL,
                migration_id TEXT NOT NULL)'''
MIGRATION_ID = 'staged-claim-v1-capability-verifier'
TABLES = {'viewer_staged_acks':ACK_SQL,'viewer_staged_ack_migrations':MIGRATION_SQL}


def validate_staged_ack_schema(db):
    """Require exact known DDL, index identities and singleton marker."""
    objects=[tuple(row) for row in db.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name IN (?,?) ORDER BY type,name",tuple(TABLES))]
    expected=[('index','sqlite_autoindex_viewer_staged_acks_'+str(i),'viewer_staged_acks',None) for i in (1,2,3)]
    expected += [('table',name,name,sql) for name,sql in TABLES.items()]
    if objects!=sorted(expected,key=lambda row:(row[0],row[1])):
        raise ValueError('staged acknowledgment schema requires inspection')
    marker=db.execute('SELECT singleton,version,migration_id FROM viewer_staged_ack_migrations').fetchall()
    if len(marker)!=1 or tuple(marker[0])!=(1,1,MIGRATION_ID):
        raise ValueError('staged acknowledgment migration marker changed')


def initialize_staged_ack_schema(queue):
    """Initialize absent tables atomically; refuse old, partial or foreign state."""
    with queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        existing=db.execute("SELECT name FROM sqlite_master WHERE name IN (?,?) OR tbl_name IN (?,?)",
                            (*TABLES,*TABLES)).fetchall()
        if existing:
            validate_staged_ack_schema(db)
            return False
        db.execute(ACK_SQL);db.execute(MIGRATION_SQL)
        db.execute('INSERT INTO viewer_staged_ack_migrations VALUES (1,1,?)',(MIGRATION_ID,))
        validate_staged_ack_schema(db)
        return True
