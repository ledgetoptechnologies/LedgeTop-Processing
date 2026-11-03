"""Inactive synthetic intent journal. No endpoint; does not grant replay authority."""
import hashlib
import json
import re
from .maintenance import Conflict

DDL = "CREATE TABLE held_batch_intent_v1 (batch_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, generation INTEGER NOT NULL, environment TEXT NOT NULL, manifest TEXT NOT NULL, digest TEXT NOT NULL)"
TRIGGERS = ("CREATE TRIGGER held_batch_intent_no_replace BEFORE INSERT ON held_batch_intent_v1 WHEN EXISTS(SELECT 1 FROM held_batch_intent_v1 WHERE batch_id=NEW.batch_id) BEGIN SELECT RAISE(ABORT,'immutable'); END", "CREATE TRIGGER held_batch_intent_no_update BEFORE UPDATE ON held_batch_intent_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END", "CREATE TRIGGER held_batch_intent_no_delete BEFORE DELETE ON held_batch_intent_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END")

def attest(db):
    expected = {'held_batch_intent_v1': DDL, 'held_batch_intent_no_replace': TRIGGERS[0], 'held_batch_intent_no_update': TRIGGERS[1], 'held_batch_intent_no_delete': TRIGGERS[2]}
    found={r['name']:r['sql'] for r in db.execute("SELECT name,sql FROM sqlite_master WHERE name='held_batch_intent_v1' OR (type='trigger' AND tbl_name='held_batch_intent_v1')")}
    if found != expected: raise Conflict('intent schema incompatible')
    cols=[tuple(r) for r in db.execute('PRAGMA table_info(held_batch_intent_v1)')]
    if [(r[1],r[2],r[3],r[5]) for r in cols] != [('batch_id','TEXT',0,1),('task_id','TEXT',1,0),('generation','INTEGER',1,0),('environment','TEXT',1,0),('manifest','TEXT',1,0),('digest','TEXT',1,0)]: raise Conflict('intent schema incompatible')
    indexes=list(db.execute('PRAGMA index_list(held_batch_intent_v1)'))
    if len(indexes)!=1 or indexes[0]['unique']!=1 or indexes[0]['origin']!='pk' or [r['name'] for r in db.execute('PRAGMA index_info('+indexes[0]['name']+')')]!=['batch_id']: raise Conflict('intent index incompatible')
    if list(db.execute('PRAGMA foreign_key_list(held_batch_intent_v1)')): raise Conflict('intent foreign keys incompatible')

def bounded_intent(db,batch_id):
    columns=(('batch_id',80),('task_id',36),('environment',10),('manifest',65536),('digest',64))
    for column,limit in columns:
        if db.execute(f"SELECT 1 FROM held_batch_intent_v1 WHERE batch_id=? AND (typeof({column})!='text' OR length({column})>? OR instr({column},char(0))!=0)",(batch_id,limit)).fetchone(): raise Conflict('intent corrupt')
        if db.execute(f"SELECT 1 FROM held_batch_intent_v1 WHERE batch_id=? AND length(CAST({column} AS BLOB))>?",(batch_id,limit)).fetchone(): raise Conflict('intent corrupt')
    if db.execute("SELECT 1 FROM held_batch_intent_v1 WHERE batch_id=? AND (typeof(generation)!='integer' OR generation<1 OR length(digest)!=64)",(batch_id,)).fetchone(): raise Conflict('intent corrupt')
    row=db.execute('SELECT * FROM held_batch_intent_v1 WHERE batch_id=?',(batch_id,)).fetchone()
    if row is None:return None
    raw=row['manifest']
    if not raw.isascii() or hashlib.sha256(raw.encode('ascii')).hexdigest()!=row['digest']: raise Conflict('intent corrupt')
    return row

def initialize(store):
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        names=list(db.execute("SELECT name FROM sqlite_master WHERE name='held_batch_intent_v1' OR (type='trigger' AND tbl_name='held_batch_intent_v1')"))
        if not names:
            db.execute(DDL)
            for sql in TRIGGERS:db.execute(sql)
        attest(db)

def prepare(store, task, environment, capability, generation, batch_id, files, *, identity=None):
    if not isinstance(batch_id,str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,80}',batch_id) or type(generation) is not int or generation < 1 or type(files) is not list or not 1 <= len(files) <= 100:
        raise ValueError('invalid batch')
    canonical = []; seen = set()
    for item in files:
        if type(item) is not dict or set(item) != {'name','bytes','sha256'}: raise ValueError('invalid item')
        key = store._filename(item['name'])
        if key in seen or type(item['bytes']) is not int or not 1 <= item['bytes'] <= store.max_input_bytes or not isinstance(item['sha256'],str) or not re.fullmatch(r'[0-9a-f]{64}',item['sha256']): raise ValueError('invalid item')
        seen.add(key); canonical.append({'name':item['name'],'bytes':item['bytes'],'sha256':item['sha256']})
    manifest=json.dumps(canonical,ensure_ascii=True,separators=(',',':'))
    digest=hashlib.sha256(manifest.encode('ascii')).hexdigest()
    with store._lock, store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        attest(db)
        if identity is not None:
            from .viewer_batch_identity_candidate import check_owner
            check_owner(db,store.queue,task,environment,identity)
        if store._reservation(db,task,environment,capability) is None: raise Conflict('reservation required')
        row=store._row(db,task)
        if row['generation']!=generation or row['environment']!=environment or row['stage']!='uploading' or row['job_id']: raise Conflict('task unavailable')
        old=bounded_intent(db,batch_id)
        identity=(task,generation,environment,manifest,digest)
        if old:
            if tuple(old[k] for k in ('task_id','generation','environment','manifest','digest'))!=identity: raise Conflict('batch differs')
        else: db.execute('INSERT INTO held_batch_intent_v1 VALUES(?,?,?,?,?,?)',(batch_id,*identity))
    return {'state':'held_no_replay_authority','digest':digest}

def upload_held(store, task, environment, capability, generation, batch_id, name, stream, length, *, identity=None):
    with store._lock, store.queue._db() as db:
        db.execute('BEGIN')
        attest(db)
        if identity is not None:
            from .viewer_batch_identity_candidate import check_owner
            check_owner(db,store.queue,task,environment,identity)
        if store._reservation(db,task,environment,capability) is None: raise Conflict('reservation required')
        row=store._row(db,task)
        if row['generation']!=generation or row['environment']!=environment or row['stage']!='uploading' or row['job_id']: raise Conflict('task unavailable')
        intent=bounded_intent(db,batch_id)
        if intent is None or (intent['task_id'],intent['environment'],intent['generation'])!=(task,environment,generation): raise Conflict('intent unavailable')
        raw=intent['manifest']
        if not isinstance(raw,str) or len(raw)>65536 or not raw.isascii() or hashlib.sha256(raw.encode('ascii')).hexdigest()!=intent['digest']: raise Conflict('intent corrupt')
        def pairs(values):
            result={}
            for key,value in values:
                if key in result: raise ValueError('duplicate key')
                result[key]=value
            return result
        try: files=json.loads(raw,object_pairs_hook=pairs)
        except (ValueError,RecursionError): raise Conflict('intent corrupt') from None
        if type(files) is not list or not 1<=len(files)<=100 or json.dumps(files,ensure_ascii=True,separators=(',',':'))!=raw: raise Conflict('intent corrupt')
        seen=set()
        for item in files:
            if type(item) is not dict or set(item)!={'name','bytes','sha256'} or type(item['name']) is not str or type(item['bytes']) is not int or not 1<=item['bytes']<=store.max_input_bytes or type(item['sha256']) is not str or not re.fullmatch(r'[0-9a-f]{64}',item['sha256']): raise Conflict('intent corrupt')
            key=store._filename(item['name'])
            if key in seen: raise Conflict('intent corrupt')
            seen.add(key)
        matches=[x for x in files if x['name']==name]
        if len(matches)!=1 or matches[0]['bytes']!=length: raise Conflict('input differs')
        expected_sha256=matches[0]['sha256']
    return store.upload(task,name,stream,length,environment,capability,expected_generation=generation,expected_sha256=expected_sha256,expected_batch=(batch_id,intent['digest']),expected_identity=identity)
