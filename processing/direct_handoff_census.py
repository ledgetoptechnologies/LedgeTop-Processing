"""Closed-schema and logical-custody census for the unwired handoff journal."""
import re
from .maintenance import Conflict

SESSION_ALLOWANCE=131072
ROW_ALLOWANCE=2048
MAX_OBJECTS=20000
SCHEMAS={
 'direct_handoff_sessions': [('reservation','TEXT',0,1),('principal','TEXT',1,0),('binding','TEXT',1,0),('allowance','INTEGER',1,0),('state','TEXT',1,0),('closure','BLOB',0,0),('object_count','INTEGER',1,0)],
 'direct_handoff_objects': [('reservation','TEXT',1,1),('object_id','TEXT',1,2),('sha','TEXT',1,0),('size','INTEGER',1,0),('state','TEXT',1,0),('response','BLOB',0,0)],
}
TRIGGERS={
 'direct_handoff_input_update': "CREATE TRIGGER direct_handoff_input_update BEFORE UPDATE OF job_id,input_json,manifest_sha,pipeline_digest ON reference_jobs WHEN EXISTS (SELECT 1 FROM direct_handoff_sessions s JOIN direct_intake_reservations r ON r.reservation_id=s.reservation WHERE r.job_id=OLD.job_id) BEGIN SELECT RAISE(ABORT,'direct handoff input locked'); END",
 'direct_handoff_input_delete': "CREATE TRIGGER direct_handoff_input_delete BEFORE DELETE ON reference_jobs WHEN EXISTS (SELECT 1 FROM direct_handoff_sessions s JOIN direct_intake_reservations r ON r.reservation_id=s.reservation WHERE r.job_id=OLD.job_id) BEGIN SELECT RAISE(ABORT,'direct handoff input locked'); END",
 'direct_handoff_input_insert': "CREATE TRIGGER direct_handoff_input_insert BEFORE INSERT ON reference_jobs WHEN EXISTS (SELECT 1 FROM direct_handoff_sessions s JOIN direct_intake_reservations r ON r.reservation_id=s.reservation WHERE r.job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'direct handoff input locked'); END",
 'direct_handoff_session_delete': "CREATE TRIGGER direct_handoff_session_delete BEFORE DELETE ON direct_handoff_sessions BEGIN SELECT RAISE(ABORT,'direct handoff retention locked'); END",
 'direct_handoff_session_insert': "CREATE TRIGGER direct_handoff_session_insert BEFORE INSERT ON direct_handoff_sessions WHEN EXISTS (SELECT 1 FROM direct_handoff_sessions WHERE reservation=NEW.reservation) BEGIN SELECT RAISE(ABORT,'direct handoff session already exists'); END",
 'direct_handoff_session_count': "CREATE TRIGGER direct_handoff_session_count BEFORE UPDATE OF object_count ON direct_handoff_sessions WHEN NEW.object_count!=OLD.object_count+1 OR NOT EXISTS (SELECT 1 FROM direct_handoff_objects WHERE rowid=last_insert_rowid() AND reservation=OLD.reservation) BEGIN SELECT RAISE(ABORT,'direct handoff counter transition invalid'); END",
 'direct_handoff_object_parent': "CREATE TRIGGER direct_handoff_object_parent BEFORE INSERT ON direct_handoff_objects WHEN NOT EXISTS (SELECT 1 FROM direct_handoff_sessions WHERE reservation=NEW.reservation AND state='receiving' AND object_count<(allowance-131072)/2048) OR EXISTS (SELECT 1 FROM direct_handoff_objects WHERE reservation=NEW.reservation AND object_id=NEW.object_id) OR typeof(NEW.object_id)!='text' OR length(NEW.object_id)<1 OR length(NEW.object_id)>128 OR typeof(NEW.sha)!='text' OR length(NEW.sha)!=64 OR typeof(NEW.size)!='integer' OR NEW.size<1 OR NEW.state!='intent' OR NEW.response IS NOT NULL BEGIN SELECT RAISE(ABORT,'direct handoff parent or object invalid'); END",
 'direct_handoff_object_insert': "CREATE TRIGGER direct_handoff_object_insert AFTER INSERT ON direct_handoff_objects BEGIN UPDATE direct_handoff_sessions SET object_count=object_count+1 WHERE reservation=NEW.reservation; END",
 'direct_handoff_object_update': "CREATE TRIGGER direct_handoff_object_update BEFORE UPDATE ON direct_handoff_objects WHEN NEW.reservation!=OLD.reservation OR NEW.object_id!=OLD.object_id OR NEW.sha!=OLD.sha OR NEW.size!=OLD.size OR OLD.state!='intent' OR NEW.state!='observed' OR typeof(NEW.response)!='blob' OR length(NEW.response)>768 BEGIN SELECT RAISE(ABORT,'direct handoff object transition invalid'); END",
 'direct_handoff_object_delete': "CREATE TRIGGER direct_handoff_object_delete BEFORE DELETE ON direct_handoff_objects BEGIN SELECT RAISE(ABORT,'direct handoff object retention locked'); END",
}


def _normalized(sql):
    return re.sub(r'\s+',' ',sql.strip()).replace(' IF NOT EXISTS ',' ')


def verify_triggers(db):
    actual={row['name']:_normalized(row['sql']) for row in db.execute(
        "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND name IN ("+
        ','.join('?' for _ in TRIGGERS)+")",tuple(TRIGGERS))}
    if actual!={name:_normalized(sql) for name,sql in TRIGGERS.items()}:
        raise Conflict('direct handoff input guards changed')


def verify_census(db, *, full=False):
    names={row['name'] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    present=set(SCHEMAS)&names
    if not present:return 0
    if present!=set(SCHEMAS):raise Conflict('direct handoff schema requires inspection')
    for table,expected in SCHEMAS.items():
        columns=[(r['name'],r['type'].upper(),r['notnull'],r['pk']) for r in db.execute('PRAGMA table_info('+table+')')]
        if columns!=expected:raise Conflict('direct handoff schema requires inspection')
    verify_triggers(db)
    if db.execute('''SELECT 1 FROM direct_handoff_sessions WHERE typeof(allowance)!='integer'
        OR allowance<? OR allowance>? OR (allowance-?)%?!=0
        OR typeof(object_count)!='integer' OR object_count<0 OR object_count>(allowance-131072)/2048
        OR state NOT IN ('receiving','closure_pending','closure_observed')
        OR (state='closure_observed')!=(closure IS NOT NULL)
        OR (closure IS NOT NULL AND (typeof(closure)!='blob' OR length(closure)>65536)) LIMIT 1''',
        (SESSION_ALLOWANCE,SESSION_ALLOWANCE+ROW_ALLOWANCE*MAX_OBJECTS,SESSION_ALLOWANCE,ROW_ALLOWANCE)).fetchone():
        raise Conflict('direct handoff allowance invalid')
    if db.execute('''SELECT 1 FROM direct_handoff_sessions s LEFT JOIN direct_intake_reservations r
        ON r.reservation_id=s.reservation WHERE r.reservation_id IS NULL LIMIT 1''').fetchone():
        raise Conflict('direct handoff reservation missing')
    if not full:
        return db.execute('SELECT COALESCE(SUM(allowance),0) FROM direct_handoff_sessions').fetchone()[0]
    if db.execute('''SELECT 1 FROM direct_handoff_objects o LEFT JOIN direct_handoff_sessions s ON s.reservation=o.reservation
        WHERE s.reservation IS NULL OR o.state NOT IN ('intent','observed')
        OR (o.state='observed')!=(o.response IS NOT NULL)
        OR (o.response IS NOT NULL AND (typeof(o.response)!='blob' OR length(o.response)>768))
        OR typeof(o.size)!='integer' OR o.size<1 OR typeof(o.object_id)!='text'
        OR length(o.object_id)<1 OR length(o.object_id)>128 OR typeof(o.sha)!='text' OR length(o.sha)!=64 LIMIT 1''').fetchone():
        raise Conflict('direct handoff object census invalid')
    if db.execute('''SELECT 1 FROM direct_handoff_sessions s LEFT JOIN direct_handoff_objects o ON o.reservation=s.reservation
        GROUP BY s.reservation HAVING count(o.object_id)!=s.object_count LIMIT 1''').fetchone():
        raise Conflict('direct handoff object budget exceeded')
    return db.execute('SELECT COALESCE(SUM(allowance),0) FROM direct_handoff_sessions').fetchone()[0]
