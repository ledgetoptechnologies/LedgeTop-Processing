"""Disabled migration prototype for durable mixed-version enqueue order.

No scheduler consumes this table; it does not authorize v2 execution.
"""
from .maintenance import Conflict

TABLE = "CREATE TABLE global_enqueue_order (seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL CHECK(kind IN ('v1','v2')), job_id TEXT NOT NULL, environment TEXT NOT NULL CHECK(environment IN ('production','staging')), UNIQUE(kind,job_id))"
INDEX = 'CREATE INDEX global_order_by_environment ON global_enqueue_order(environment,seq)'
TRIGGERS = {
 'global_order_v1_insert': "CREATE TRIGGER global_order_v1_insert AFTER INSERT ON jobs BEGIN SELECT RAISE(ABORT,'v2 enqueue identity mismatch') WHERE (NEW.profile='reference-pipeline-v2' AND (NOT EXISTS(SELECT 1 FROM global_enqueue_order WHERE kind='v2' AND job_id=NEW.id AND environment=NEW.environment) OR NOT EXISTS(SELECT 1 FROM reference_v2_staged_jobs WHERE job_id=NEW.id AND environment=NEW.environment))) OR (NEW.profile!='reference-pipeline-v2' AND EXISTS(SELECT 1 FROM global_enqueue_order WHERE kind='v2' AND job_id=NEW.id)); INSERT INTO global_enqueue_order(kind,job_id,environment) SELECT 'v1',NEW.id,NEW.environment WHERE NEW.profile!='reference-pipeline-v2'; END",
 'global_order_v1_collision': "CREATE TRIGGER global_order_v1_collision BEFORE INSERT ON jobs WHEN EXISTS(SELECT 1 FROM jobs WHERE id=NEW.id OR request_id=NEW.request_id) BEGIN SELECT RAISE(ABORT,'enqueue replacement forbidden'); END",
 'global_order_v1_identity': "CREATE TRIGGER global_order_v1_identity BEFORE UPDATE OF id,environment,profile ON jobs WHEN NEW.id IS NOT OLD.id OR NEW.environment IS NOT OLD.environment OR NEW.profile IS NOT OLD.profile BEGIN SELECT RAISE(ABORT,'enqueue identity immutable'); END",
 'global_order_v2_insert': "CREATE TRIGGER global_order_v2_insert AFTER INSERT ON reference_v2_staged_jobs BEGIN SELECT RAISE(ABORT,'cross-kind enqueue collision') WHERE EXISTS(SELECT 1 FROM global_enqueue_order WHERE kind='v1' AND job_id=NEW.job_id); INSERT INTO global_enqueue_order(kind,job_id,environment) VALUES('v2',NEW.job_id,NEW.environment); END",
 'global_order_v2_identity': "CREATE TRIGGER global_order_v2_identity BEFORE UPDATE OF job_id,environment ON reference_v2_staged_jobs WHEN NEW.job_id IS NOT OLD.job_id OR NEW.environment IS NOT OLD.environment BEGIN SELECT RAISE(ABORT,'enqueue identity immutable'); END",
 'global_order_immutable_insert': "CREATE TRIGGER global_order_immutable_insert BEFORE INSERT ON global_enqueue_order WHEN EXISTS(SELECT 1 FROM global_enqueue_order WHERE (kind=NEW.kind AND job_id=NEW.job_id) OR (NEW.seq>0 AND seq=NEW.seq)) BEGIN SELECT RAISE(ABORT,'enqueue order immutable'); END",
 'global_order_immutable_update': "CREATE TRIGGER global_order_immutable_update BEFORE UPDATE ON global_enqueue_order BEGIN SELECT RAISE(ABORT,'enqueue order immutable'); END",
 'global_order_immutable_delete': "CREATE TRIGGER global_order_immutable_delete BEFORE DELETE ON global_enqueue_order BEGIN SELECT RAISE(ABORT,'enqueue order immutable'); END",
}

class GlobalOrder:
 def __init__(self, queue, *, enabled=False):
  if enabled is not True: raise Conflict('global enqueue order disabled')
  self.queue=queue
  with queue._db() as db:
   db.execute('BEGIN IMMEDIATE')
   row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='global_enqueue_order'").fetchone()
   v2=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_v2_staged_jobs'").fetchone()
   if row is None:
    if v2 and db.execute('SELECT 1 FROM reference_v2_staged_jobs LIMIT 1').fetchone():
     raise Conflict('existing v2 submission order cannot be reconstructed')
    if not v2:
     from .reference_v2_metadata_ledger import _DDL
     db.execute(_DDL)
     v2=True
    db.execute(TABLE)
    db.execute(INDEX)
    db.execute("INSERT INTO global_enqueue_order(kind,job_id,environment) SELECT 'v1',id,environment FROM jobs ORDER BY seq")
    for name in ('global_order_v1_insert','global_order_v1_collision','global_order_v1_identity','global_order_immutable_insert','global_order_immutable_update','global_order_immutable_delete'):db.execute(TRIGGERS[name])
    if v2:self.attach_v2(db)
   self.verify(db)
  queue._global_order=self

 @staticmethod
 def attach_v2(db):
  for name in ('global_order_v2_insert','global_order_v2_identity'):
   found=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(name,)).fetchone()
   if found is None:db.execute(TRIGGERS[name])
   elif found[0]!=TRIGGERS[name]:raise Conflict('v2 order trigger changed')

 def verify(self, db):
  row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='global_enqueue_order'").fetchone()
  if row is None or row[0]!=TABLE:raise Conflict('global order schema changed')
  extras=db.execute("SELECT name,sql FROM sqlite_master WHERE tbl_name='global_enqueue_order' AND (type='trigger' OR (type='index' AND sql IS NOT NULL))").fetchall()
  if any((name!='global_order_by_environment' or sql!=INDEX) and (name not in TRIGGERS or sql!=TRIGGERS[name]) for name,sql in extras):raise Conflict('global order schema changed')
  idx=db.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name='global_order_by_environment'").fetchone()
  if idx is None or idx[0]!=INDEX:raise Conflict('global order index changed')
  source_triggers=db.execute("SELECT name,sql FROM sqlite_master WHERE tbl_name IN ('jobs','reference_v2_staged_jobs') AND type='trigger'").fetchall()
  allowed={'global_order_v1_insert','global_order_v1_collision','global_order_v1_identity','global_order_v2_insert','global_order_v2_identity'}
  if any(name not in allowed or sql!=TRIGGERS[name] for name,sql in source_triggers):raise Conflict('enqueue source schema changed')
  names=['global_order_v1_insert','global_order_v1_collision','global_order_v1_identity','global_order_immutable_insert','global_order_immutable_update','global_order_immutable_delete']
  from .reference_v2_metadata_ledger import _DDL as V2_DDL
  v2=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_v2_staged_jobs'").fetchone()
  if not v2 or v2[0] != V2_DDL:raise Conflict('v2 staging schema unavailable')
  names+=['global_order_v2_insert','global_order_v2_identity']
  for name in names:
   row=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(name,)).fetchone()
   if row is None or row[0]!=TRIGGERS[name]:raise Conflict('global order trigger changed')
  if db.execute("SELECT 1 FROM jobs j LEFT JOIN global_enqueue_order o ON o.kind=CASE WHEN j.profile='reference-pipeline-v2' THEN 'v2' ELSE 'v1' END AND o.job_id=j.id AND o.environment=j.environment WHERE o.seq IS NULL LIMIT 1").fetchone():raise Conflict('v1 enqueue order unavailable')
  if v2 and db.execute("SELECT 1 FROM reference_v2_staged_jobs v LEFT JOIN global_enqueue_order o ON o.kind='v2' AND o.job_id=v.job_id AND o.environment=v.environment WHERE o.seq IS NULL LIMIT 1").fetchone():raise Conflict('v2 enqueue order unavailable')

 def snapshot(self, *, limit=1000):
  if type(limit) is not int or not 1 <= limit <= 1000: raise ValueError('bounded snapshot required')
  with self.queue._db() as db:
   db.execute('BEGIN');self.verify(db)
   rows=db.execute("SELECT seq,kind,job_id,environment FROM global_enqueue_order ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END,seq LIMIT ?",(limit+1,)).fetchall()
   if len(rows)>limit:raise Conflict('global order snapshot exceeds bound')
   return [dict(row) for row in rows]

 def pending_page(self, *, limit=100, after=None):
  """Observe pending IDs with keyset pagination; no eligibility authorization.

  Each page is a new read snapshot. The caller must restart from the beginning
  to observe production arriving after it advanced into the staging partition.
  """
  if type(limit) is not int or not 1<=limit<=1000:raise ValueError('bounded page required')
  if after is not None and (type(after) is not tuple or len(after)!=2 or after[0] not in ('production','staging') or type(after[1]) is not int or after[1]<1):raise ValueError('invalid cursor')
  with self.queue._db() as db:
   db.execute('BEGIN');self.verify(db);rows=[]
   for environment in ('production','staging'):
    if after is not None and after[0]=='staging' and environment=='production':continue
    minimum=after[1] if after is not None and after[0]==environment else 0
    remaining=limit-len(rows)
    found=db.execute("""SELECT o.seq,o.kind,o.job_id,o.environment FROM global_enqueue_order o
      WHERE o.environment=? AND o.seq>? AND
       ((o.kind='v1' AND EXISTS(SELECT 1 FROM jobs j WHERE j.id=o.job_id AND j.status='queued'))
         OR (o.kind='v2' AND EXISTS(SELECT 1 FROM reference_v2_staged_jobs v WHERE v.job_id=o.job_id)
             AND (NOT EXISTS(SELECT 1 FROM jobs j WHERE j.id=o.job_id) OR EXISTS(SELECT 1 FROM jobs j WHERE j.id=o.job_id AND j.profile='reference-pipeline-v2' AND j.status='queued'))))
      ORDER BY o.seq LIMIT ?""",(environment,minimum,remaining)).fetchall()
    rows.extend(dict(row) for row in found)
    if len(rows)==limit:break
   cursor=(rows[-1]['environment'],rows[-1]['seq']) if rows else None
   return {'entries':rows,'after':cursor}
