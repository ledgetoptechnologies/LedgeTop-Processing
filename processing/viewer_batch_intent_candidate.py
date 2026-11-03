"""Inactive synthetic intent journal. No endpoint; does not grant replay authority."""
import hashlib
import json
import re
from .maintenance import Conflict

DDL = "CREATE TABLE held_batch_intent_v1 (batch_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, generation INTEGER NOT NULL, environment TEXT NOT NULL, manifest TEXT NOT NULL, digest TEXT NOT NULL)"
TRIGGERS = ("CREATE TRIGGER held_batch_intent_no_update BEFORE UPDATE ON held_batch_intent_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END", "CREATE TRIGGER held_batch_intent_no_delete BEFORE DELETE ON held_batch_intent_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END")

def initialize(store):
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        expected = {'held_batch_intent_v1': DDL, 'held_batch_intent_no_update': TRIGGERS[0], 'held_batch_intent_no_delete': TRIGGERS[1]}
        found = {r['name']:r['sql'] for r in db.execute("SELECT name,sql FROM sqlite_master WHERE name IN ('held_batch_intent_v1','held_batch_intent_no_update','held_batch_intent_no_delete')")}
        if not found:
            db.execute(DDL)
            for sql in TRIGGERS: db.execute(sql)
        elif found != expected:
            raise Conflict('intent schema incompatible')

def prepare(store, task, environment, capability, generation, batch_id, files):
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
        if store._reservation(db,task,environment,capability) is None: raise Conflict('reservation required')
        row=store._row(db,task)
        if row['generation']!=generation or row['environment']!=environment or row['stage']!='uploading' or row['job_id']: raise Conflict('task unavailable')
        old=db.execute('SELECT * FROM held_batch_intent_v1 WHERE batch_id=?',(batch_id,)).fetchone()
        identity=(task,generation,environment,manifest,digest)
        if old:
            if tuple(old[k] for k in ('task_id','generation','environment','manifest','digest'))!=identity: raise Conflict('batch differs')
        else: db.execute('INSERT INTO held_batch_intent_v1 VALUES(?,?,?,?,?,?)',(batch_id,*identity))
    return {'state':'held_no_replay_authority','digest':digest}

def upload_held(store, task, environment, capability, generation, batch_id, name, stream, length):
    with store._lock, store.queue._db() as db:
        db.execute('BEGIN')
        if store._reservation(db,task,environment,capability) is None: raise Conflict('reservation required')
        row=store._row(db,task)
        if row['generation']!=generation or row['environment']!=environment or row['stage']!='uploading' or row['job_id']: raise Conflict('task unavailable')
        intent=db.execute('SELECT * FROM held_batch_intent_v1 WHERE batch_id=?',(batch_id,)).fetchone()
        if intent is None or (intent['task_id'],intent['environment'],intent['generation'])!=(task,environment,generation): raise Conflict('intent unavailable')
        raw=intent['manifest']
        if not isinstance(raw,str) or hashlib.sha256(raw.encode('ascii')).hexdigest()!=intent['digest']: raise Conflict('intent corrupt')
        files=json.loads(raw)
        matches=[x for x in files if x['name']==name]
        if len(matches)!=1 or matches[0]['bytes']!=length: raise Conflict('input differs')
        expected_sha256=matches[0]['sha256']
    return store.upload(task,name,stream,length,environment,capability,expected_generation=generation,expected_sha256=expected_sha256)
