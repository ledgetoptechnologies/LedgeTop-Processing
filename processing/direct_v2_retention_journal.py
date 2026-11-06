"""Coordinator's local worker-fence observation; not Viewer custody."""
import hmac
import json
from . import reference_protocol as p
from .maintenance import Conflict
from .direct_worker_receipt_v2 import verify_retention

SCHEMA='''CREATE TABLE direct_v2_retention_observations (
 reservation TEXT PRIMARY KEY,observation BLOB NOT NULL)'''
TRIGGERS=(
 "CREATE TRIGGER direct_v2_retention_no_replace BEFORE INSERT ON direct_v2_retention_observations WHEN EXISTS (SELECT 1 FROM direct_v2_retention_observations WHERE reservation=NEW.reservation) BEGIN SELECT RAISE(ABORT,'V2 retention observation immutable'); END",
 "CREATE TRIGGER direct_v2_retention_no_update BEFORE UPDATE ON direct_v2_retention_observations BEGIN SELECT RAISE(ABORT,'V2 retention observation immutable'); END",
 "CREATE TRIGGER direct_v2_retention_no_delete BEFORE DELETE ON direct_v2_retention_observations BEGIN SELECT RAISE(ABORT,'V2 retention observation immutable'); END",
)
MAX_OBSERVATIONS=4096

def validate(db):
    columns={r['name'] for r in db.execute('PRAGMA table_info(coordinator_identity)')}
    version=0 if 'direct_v2_retention_version' not in columns else db.execute(
        'SELECT direct_v2_retention_version FROM coordinator_identity WHERE singleton=1').fetchone()[0]
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='direct_v2_retention_observations'").fetchone()
    if version==0 and row is None and 'direct_v2_retention_count' not in columns:return
    # Version 1 used a different signed field and signature domain. Existing
    # journals are held for an explicit migration; never reinterpret or erase.
    if (type(version) is not int or version!=2 or 'direct_v2_retention_count' not in columns
            or row is None or ' '.join(row['sql'].split())!=' '.join(SCHEMA.split())):
        raise Conflict('V2 retention journal schema requires inspection')
    for sql in TRIGGERS:
        trigger=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(sql.split()[2],)).fetchone()
        if trigger is None or ' '.join(trigger['sql'].split())!=' '.join(sql.split()):
            raise Conflict('V2 retention journal guards require inspection')
    count=db.execute('SELECT COUNT(*) FROM direct_v2_retention_observations').fetchone()[0]
    recorded=db.execute('SELECT direct_v2_retention_count FROM coordinator_identity WHERE singleton=1').fetchone()[0]
    if type(recorded) is not int or recorded!=count or not 0<=count<=MAX_OBSERVATIONS:
        raise Conflict('V2 retention journal count requires inspection')
    if db.execute('''SELECT 1 FROM direct_v2_retention_observations o LEFT JOIN direct_v2_handoff_sessions s
       ON s.reservation=o.reservation WHERE s.reservation IS NULL OR s.state!='closure_observed'
       OR typeof(o.observation)!='blob' OR length(o.observation)>4096 LIMIT 1''').fetchone():
        raise Conflict('V2 retention journal requires inspection')

def ensure(db):
    if not db.in_transaction:raise Conflict('V2 retention transaction required')
    validate(db)
    columns={r['name'] for r in db.execute('PRAGMA table_info(coordinator_identity)')}
    if 'direct_v2_retention_version' not in columns:
        db.execute('ALTER TABLE coordinator_identity ADD COLUMN direct_v2_retention_version INTEGER NOT NULL DEFAULT 0')
    version=db.execute('SELECT direct_v2_retention_version FROM coordinator_identity WHERE singleton=1').fetchone()[0]
    if version==0:
        db.execute('ALTER TABLE coordinator_identity ADD COLUMN direct_v2_retention_count INTEGER NOT NULL DEFAULT 0')
        db.execute(SCHEMA)
        for sql in TRIGGERS:db.execute(sql)
        db.execute('UPDATE coordinator_identity SET direct_v2_retention_version=2 WHERE singleton=1')
    validate(db)

def save(db, *, reservation, signed_closure, observation, sender):
    """Use the existing session allowance; exact replay only, never custody."""
    ensure(db)
    session=db.execute('SELECT state,closure FROM direct_v2_handoff_sessions WHERE reservation=?',(reservation,)).fetchone()
    closure_wire=p.canonical_json(signed_closure)
    if session is None or session['state']!='closure_observed' or session['closure']!=closure_wire:
        raise Conflict('V2 closure not durably observed')
    checked=verify_retention(observation,signed_closure=signed_closure,expected_closure=sender._expected_closure(),
        issuer_id=sender._issuer,key_id=sender._key,public_key=sender._public)
    wire=p.canonical_json(checked)
    if len(wire)>4096:raise Conflict('V2 retention observation too large')
    old=db.execute('SELECT observation FROM direct_v2_retention_observations WHERE reservation=?',(reservation,)).fetchone()
    if old is None:
        count=db.execute('SELECT direct_v2_retention_count FROM coordinator_identity WHERE singleton=1').fetchone()[0]
        if count>=MAX_OBSERVATIONS:raise Conflict('V2 retention journal capacity exceeded')
        db.execute('INSERT INTO direct_v2_retention_observations VALUES(?,?)',(reservation,wire))
        db.execute('UPDATE coordinator_identity SET direct_v2_retention_count=? WHERE singleton=1',(count+1,))
    elif not hmac.compare_digest(old['observation'],wire):raise Conflict('V2 retention observation changed')
    return checked
