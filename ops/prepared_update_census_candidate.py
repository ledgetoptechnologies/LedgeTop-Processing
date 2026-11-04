"""UNMOUNTED prepared-schema census; not an updater or rollback authorization.

This validates only the six prepared tables. Callers must independently census
all other schema, rows, files, node state, authority and retained bytes under the
existing lease and bind both views to the same database snapshot. It provides no
proof that any predecessor executable can read the migrated volume.
"""
import sqlite3
from . import update_reference_coordinator as updater
from processing import reference_store as store

TABLES = ('reference_viewer_activation','reference_viewer_submission_owners',
          'reference_viewer_activation_schema','reference_viewer_envelopes_v2',
          'reference_viewer_v3_copy_hold','reference_viewer_v3_policy')
DDL = (store._ACTIVATION_SCHEMA,store._OWNER_SCHEMA,store._SCHEMA_MARKER,
       store._ENVELOPE_V2_SCHEMA,store._V3_COPY_HOLD_SCHEMA,store._V3_POLICY_SCHEMA)


class PreparedCensusHeld(ValueError):
    pass


def _expected():
    db=sqlite3.connect(':memory:')
    try:
        for statement in DDL:db.execute(statement)
        return [list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
    finally:db.close()


def census_prepared(db):
    """Caller owns an existing read transaction; never creates or changes state."""
    if not isinstance(db,sqlite3.Connection) or not db.in_transaction:
        raise PreparedCensusHeld('existing read snapshot required')
    try:
        items=[list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        existing={row[1] for row in items if row[0]=='table'} & set(TABLES)
        if not existing:
            return {'present':False,'schema':[],'marker':None}
        if existing!=set(TABLES):raise PreparedCensusHeld('partial prepared schema')
        schema=[row for row in items if row[2] in TABLES]
        if updater.normalized_schema(schema)!=updater.normalized_schema(_expected()):
            raise PreparedCensusHeld('prepared schema differs')
        for name in TABLES:
            rows=db.execute('SELECT * FROM "'+name+'"').fetchall()
            if name=='reference_viewer_activation_schema':
                if [tuple(row) for row in rows]!=[(1,1)]:raise PreparedCensusHeld('prepared schema marker differs')
            elif rows:
                raise PreparedCensusHeld('prepared records require reconciliation')
        return {'present':True,'schema':schema,'marker':[1,1]}
    except sqlite3.Error:
        raise PreparedCensusHeld('prepared schema unavailable') from None


def verify_candidate_additions(before, after):
    """Only candidate inspection can accept this all-or-nothing migration."""
    if (before != {'present':False,'schema':[],'marker':None}
            or type(after) is not dict or after.get('present') is not True
            or set(after)!={'present','schema','marker'} or after['marker'] != [1,1]
            or updater.normalized_schema(after['schema'])!=updater.normalized_schema(_expected())):
        raise PreparedCensusHeld('unreviewed prepared migration')


def require_predecessor_rollback_proof(observed):
    """No proof protocol exists yet; never authorize an older binary here."""
    if observed != {'present':False,'schema':[],'marker':None}:
        raise PreparedCensusHeld('predecessor prepared-schema compatibility unqualified')
