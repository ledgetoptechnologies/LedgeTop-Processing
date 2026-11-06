"""Private reservation-scoped source page intake; never a custody ACK."""
import base64
import binascii
import hashlib
import hmac
import json
import re
import threading
from cryptography.exceptions import InvalidSignature
from . import reference_protocol as p
from .direct_v2_reservation_schema import validate_receiving
from .direct_v2_snapshot_verifier import _decode, SNAPSHOT_DOMAIN, PAGE_DOMAIN, INTENT_FIELDS
from .maintenance import Conflict
from .reference_api import Response

MAX_SESSIONS=128
MAX_PAGES=64
MAX_PAGE_BYTES=256*1024
MAX_FRAME_BYTES=12*1024*1024
MAX_INTENT_BYTES=1024*1024
# Logical metadata planning allowance, not a filesystem/WAL quota.
SESSION_OVERHEAD=512*1024
TABLES={
 'direct_snapshot_sessions':'''CREATE TABLE direct_snapshot_sessions (
  reservation TEXT PRIMARY KEY,snapshot TEXT NOT NULL,principal TEXT NOT NULL,environment TEXT NOT NULL,
  intent BLOB NOT NULL,signature BLOB NOT NULL,frame_bytes INTEGER NOT NULL,allowance INTEGER NOT NULL)''',
 'direct_snapshot_pages':'''CREATE TABLE direct_snapshot_pages (
  reservation TEXT NOT NULL,page_index INTEGER NOT NULL,page BLOB NOT NULL,
  PRIMARY KEY(reservation,page_index))''',
}
TRIGGERS=tuple(f"CREATE TRIGGER {table}_{action} BEFORE {event} ON {table} {condition} BEGIN SELECT RAISE(ABORT,'direct snapshot immutable'); END"
 for table,action,event,condition in (
 ('direct_snapshot_sessions','no_replace','INSERT','WHEN EXISTS (SELECT 1 FROM direct_snapshot_sessions WHERE reservation=NEW.reservation)'),
 ('direct_snapshot_sessions','no_update','UPDATE',''),('direct_snapshot_sessions','no_delete','DELETE',''),
 ('direct_snapshot_pages','no_replace','INSERT','WHEN EXISTS (SELECT 1 FROM direct_snapshot_pages WHERE reservation=NEW.reservation AND page_index=NEW.page_index)'),
 ('direct_snapshot_pages','no_update','UPDATE',''),('direct_snapshot_pages','no_delete','DELETE','')))

# Separate append-only journal: old source-only sessions remain readable and
# selected bytes are charged before the first selected page is admitted.
SELECTED_TABLES={
 'direct_selected_sessions':"CREATE TABLE direct_selected_sessions (reservation TEXT PRIMARY KEY,frame_bytes INTEGER NOT NULL,allowance INTEGER NOT NULL)",
 'direct_selected_pages':"CREATE TABLE direct_selected_pages (reservation TEXT NOT NULL,page_index INTEGER NOT NULL,page BLOB NOT NULL,PRIMARY KEY(reservation,page_index))",
}
SELECTED_TRIGGERS=tuple(f"CREATE TRIGGER {table}_{action} BEFORE {event} ON {table} {condition} BEGIN SELECT RAISE(ABORT,'direct selected immutable'); END"
 for table,action,event,condition in (
 ('direct_selected_sessions','no_replace','INSERT','WHEN EXISTS (SELECT 1 FROM direct_selected_sessions WHERE reservation=NEW.reservation)'),
 ('direct_selected_sessions','no_update','UPDATE',''),('direct_selected_sessions','no_delete','DELETE',''),
 ('direct_selected_pages','no_replace','INSERT','WHEN EXISTS (SELECT 1 FROM direct_selected_pages WHERE reservation=NEW.reservation AND page_index=NEW.page_index)'),
 ('direct_selected_pages','no_update','UPDATE',''),('direct_selected_pages','no_delete','DELETE','')))

def selected_metadata_bytes(db):
    cols={r['name'] for r in db.execute('PRAGMA table_info(coordinator_identity)')}
    names={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    present=set(SELECTED_TABLES)&names
    if not present and not ({'direct_selected_version','direct_selected_session_count','direct_selected_page_count'}&cols):return 0
    if present!=set(SELECTED_TABLES) or not {'direct_selected_version','direct_selected_session_count','direct_selected_page_count'}<=cols:raise Conflict('selected journal requires inspection')
    identity=db.execute('SELECT direct_selected_version,direct_selected_session_count,direct_selected_page_count FROM coordinator_identity WHERE singleton=1').fetchone()
    for name,sql in SELECTED_TABLES.items():
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()
        if row is None or ' '.join(row['sql'].split())!=' '.join(sql.split()):raise Conflict('selected schema changed')
    for sql in SELECTED_TRIGGERS:
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(sql.split()[2],)).fetchone()
        if row is None or ' '.join(row['sql'].split())!=' '.join(sql.split()):raise Conflict('selected guards changed')
    n=db.execute('SELECT COUNT(*) FROM direct_selected_sessions').fetchone()[0]
    pages=db.execute('SELECT COUNT(*) FROM direct_selected_pages').fetchone()[0]
    if tuple(identity)!=(1,n,pages) or n>MAX_SESSIONS or pages>MAX_SESSIONS*MAX_PAGES:raise Conflict('selected count changed')
    if db.execute("""SELECT 1 FROM direct_selected_sessions x LEFT JOIN direct_snapshot_sessions s ON s.reservation=x.reservation
      WHERE s.reservation IS NULL OR typeof(x.frame_bytes)!='integer' OR x.frame_bytes<1 OR x.frame_bytes>?
      OR typeof(x.allowance)!='integer' OR x.allowance!=x.frame_bytes+? OR CASE WHEN json_valid(CAST(s.intent AS TEXT))
      THEN COALESCE(json_type(CAST(s.intent AS TEXT),'$.selectedFrameBytes')!='integer' OR json_extract(CAST(s.intent AS TEXT),'$.selectedFrameBytes')!=x.frame_bytes,1) ELSE 1 END LIMIT 1""",(MAX_FRAME_BYTES,SESSION_OVERHEAD)).fetchone():raise Conflict('selected session changed')
    if db.execute("""SELECT 1 FROM direct_selected_pages p LEFT JOIN direct_selected_sessions s ON s.reservation=p.reservation
      WHERE s.reservation IS NULL OR typeof(p.page_index)!='integer' OR p.page_index<0 OR p.page_index>=? OR typeof(p.page)!='blob' OR length(p.page)=0 OR length(p.page)>? LIMIT 1""",(MAX_PAGES,MAX_PAGE_BYTES)).fetchone():raise Conflict('selected page changed')
    if db.execute("""SELECT 1 FROM direct_selected_pages p JOIN direct_selected_sessions s ON s.reservation=p.reservation
       GROUP BY p.reservation HAVING SUM(length(p.page)+1)>MAX(s.frame_bytes) LIMIT 1""").fetchone():raise Conflict('selected budget changed')
    return db.execute('SELECT COALESCE(SUM(allowance),0) FROM direct_selected_sessions').fetchone()[0]

def ensure_selected(db):
    if not db.in_transaction:raise Conflict('selected transaction required')
    selected_metadata_bytes(db)
    cols={r['name'] for r in db.execute('PRAGMA table_info(coordinator_identity)')}
    if 'direct_selected_version' not in cols:
        for name in ('direct_selected_version','direct_selected_session_count','direct_selected_page_count'):
            db.execute('ALTER TABLE coordinator_identity ADD COLUMN '+name+' INTEGER NOT NULL DEFAULT 0')
        for sql in SELECTED_TABLES.values():db.execute(sql)
        for sql in SELECTED_TRIGGERS:db.execute(sql)
        db.execute('UPDATE coordinator_identity SET direct_selected_version=1 WHERE singleton=1')
    selected_metadata_bytes(db)

OBS_SCHEMA="CREATE TABLE direct_snapshot_observations (reservation TEXT PRIMARY KEY,snapshot TEXT NOT NULL,principal TEXT NOT NULL,environment TEXT NOT NULL,source_sha TEXT NOT NULL,selected_sha TEXT NOT NULL,pipeline_sha TEXT NOT NULL,observation BLOB NOT NULL)"
OBS_TRIGGERS=(
 "CREATE TRIGGER direct_snapshot_observations_no_replace BEFORE INSERT ON direct_snapshot_observations WHEN EXISTS (SELECT 1 FROM direct_snapshot_observations WHERE reservation=NEW.reservation) BEGIN SELECT RAISE(ABORT,'snapshot observation immutable'); END",
 "CREATE TRIGGER direct_snapshot_observations_no_update BEFORE UPDATE ON direct_snapshot_observations BEGIN SELECT RAISE(ABORT,'snapshot observation immutable'); END",
 "CREATE TRIGGER direct_snapshot_observations_no_delete BEFORE DELETE ON direct_snapshot_observations BEGIN SELECT RAISE(ABORT,'snapshot observation immutable'); END")
OBS_ALLOWANCE=8192

def observation_metadata_bytes(db):
    cols={r['name'] for r in db.execute('PRAGMA table_info(coordinator_identity)')}
    exists=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='direct_snapshot_observations'").fetchone()
    if not exists and not ({'direct_snapshot_observation_version','direct_snapshot_observation_count'}&cols):return 0
    if not exists or not {'direct_snapshot_observation_version','direct_snapshot_observation_count'}<=cols or ' '.join(exists['sql'].split())!=' '.join(OBS_SCHEMA.split()):raise Conflict('snapshot observation schema changed')
    for sql in OBS_TRIGGERS:
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(sql.split()[2],)).fetchone()
        if row is None or ' '.join(row['sql'].split())!=' '.join(sql.split()):raise Conflict('snapshot observation guards changed')
    version,count=db.execute('SELECT direct_snapshot_observation_version,direct_snapshot_observation_count FROM coordinator_identity WHERE singleton=1').fetchone()
    rows=db.execute('SELECT * FROM direct_snapshot_observations').fetchall()
    if type(version) is not int or version!=1 or type(count) is not int or count!=len(rows) or count>MAX_SESSIONS:raise Conflict('snapshot observation count changed')
    for row in rows:
        if (any(type(row[k]) is not str or not re.fullmatch('[a-f0-9]{64}',row[k]) for k in ('source_sha','selected_sha','pipeline_sha'))
                or type(row['observation']) is not bytes or not 1<=len(row['observation'])<=4096):raise Conflict('snapshot observation invalid')
        parent=db.execute('SELECT snapshot,principal,environment,intent FROM direct_snapshot_sessions WHERE reservation=?',(row['reservation'],)).fetchone()
        if (parent is None or tuple(parent)[:3]!=(row['snapshot'],row['principal'],row['environment'])
                or hashlib.sha256(parent['intent']).hexdigest()!=row['source_sha']):raise Conflict('snapshot observation parent changed')
        try:
            intent=_decode(parent['intent'],MAX_INTENT_BYTES);observation=_decode(row['observation'],4096)
            if (type(observation) is not dict or set(observation)!={'state','sourceSnapshotSha256','datasetFileCount','selectedCount','sourceCustodyAccepted','processingAuthorized'}
                    or observation['state']!='source_and_selected_pages_verified_not_custody'
                    or observation['sourceSnapshotSha256']!=row['source_sha']
                    or type(observation['datasetFileCount']) is not int or observation['datasetFileCount']!=intent['datasetFileCount']
                    or type(observation['selectedCount']) is not int or observation['selectedCount']!=intent['selectedCount']
                    or observation['sourceCustodyAccepted'] is not False or observation['processingAuthorized'] is not False):raise ValueError()
        except (TypeError,ValueError,KeyError):raise Conflict('snapshot observation invalid') from None
    return count*OBS_ALLOWANCE

def ensure_observations(db):
    if not db.in_transaction:raise Conflict('snapshot observation transaction required')
    observation_metadata_bytes(db)
    cols={r['name'] for r in db.execute('PRAGMA table_info(coordinator_identity)')}
    if 'direct_snapshot_observation_version' not in cols:
        db.execute('ALTER TABLE coordinator_identity ADD COLUMN direct_snapshot_observation_version INTEGER NOT NULL DEFAULT 0')
        db.execute('ALTER TABLE coordinator_identity ADD COLUMN direct_snapshot_observation_count INTEGER NOT NULL DEFAULT 0')
        db.execute(OBS_SCHEMA)
        for sql in OBS_TRIGGERS:db.execute(sql)
        db.execute('UPDATE coordinator_identity SET direct_snapshot_observation_version=1 WHERE singleton=1')
    observation_metadata_bytes(db)

def metadata_bytes(db):
    cols={r['name'] for r in db.execute('PRAGMA table_info(coordinator_identity)')}
    marker=0 if 'direct_snapshot_version' not in cols else db.execute('SELECT direct_snapshot_version FROM coordinator_identity WHERE singleton=1').fetchone()[0]
    names={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}; present=set(TABLES)&names
    if marker==0 and not present and 'direct_snapshot_session_count' not in cols and 'direct_snapshot_page_count' not in cols:return 0
    # V1 precharged a fixed allowance and used a different schema. Never
    # reinterpret or erase old journals; a reviewed migration is required.
    if (type(marker) is not int or marker!=2 or present!=set(TABLES)
        or not {'direct_snapshot_session_count','direct_snapshot_page_count'}<=cols):raise Conflict('snapshot journal requires inspection')
    for name,sql in TABLES.items():
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()
        if row is None or ' '.join(row['sql'].split())!=' '.join(sql.split()):raise Conflict('snapshot schema changed')
    for sql in TRIGGERS:
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(sql.split()[2],)).fetchone()
        if row is None or ' '.join(row['sql'].split())!=' '.join(sql.split()):raise Conflict('snapshot guards changed')
    counts=db.execute('SELECT direct_snapshot_session_count,direct_snapshot_page_count FROM coordinator_identity WHERE singleton=1').fetchone()
    sessions=db.execute('SELECT COUNT(*) FROM direct_snapshot_sessions').fetchone()[0]
    pages=db.execute('SELECT COUNT(*) FROM direct_snapshot_pages').fetchone()[0]
    if (type(counts[0]) is not int or type(counts[1]) is not int or counts[0]!=sessions or counts[1]!=pages
        or sessions>MAX_SESSIONS or pages>MAX_SESSIONS*MAX_PAGES):raise Conflict('snapshot count changed')
    bad=db.execute('''SELECT 1 FROM direct_snapshot_sessions WHERE typeof(intent)!='blob' OR length(intent)>? OR length(intent)=0
        OR typeof(signature)!='blob' OR length(signature)!=64 OR typeof(frame_bytes)!='integer' OR frame_bytes<1 OR frame_bytes>?
        OR typeof(allowance)!='integer' OR allowance!=length(intent)+frame_bytes+?
        OR CASE WHEN json_valid(CAST(intent AS TEXT))
             THEN COALESCE(json_type(CAST(intent AS TEXT),'$.sourceFrameBytes')!='integer'
                  OR json_extract(CAST(intent AS TEXT),'$.sourceFrameBytes')!=frame_bytes,1)
             ELSE 1 END LIMIT 1''',
        (MAX_INTENT_BYTES,MAX_FRAME_BYTES,SESSION_OVERHEAD)).fetchone()
    bad_page=db.execute('''SELECT 1 FROM direct_snapshot_pages p LEFT JOIN direct_snapshot_sessions s ON s.reservation=p.reservation
        WHERE s.reservation IS NULL OR typeof(page_index)!='integer' OR page_index<0 OR page_index>=?
        OR typeof(page)!='blob' OR length(page)=0 OR length(page)>? LIMIT 1''',(MAX_PAGES,MAX_PAGE_BYTES)).fetchone()
    overflow=db.execute('''SELECT 1 FROM direct_snapshot_pages p JOIN direct_snapshot_sessions s ON s.reservation=p.reservation
        GROUP BY p.reservation HAVING SUM(length(p.page)+1)>MAX(s.frame_bytes) LIMIT 1''').fetchone()
    if bad or bad_page or overflow:raise Conflict('snapshot journal bounds changed')
    return db.execute('SELECT COALESCE(SUM(allowance),0) FROM direct_snapshot_sessions').fetchone()[0]

def ensure(db):
    if not db.in_transaction:raise Conflict('snapshot transaction required')
    metadata_bytes(db)
    cols={r['name'] for r in db.execute('PRAGMA table_info(coordinator_identity)')}
    if 'direct_snapshot_version' not in cols:
        db.execute('ALTER TABLE coordinator_identity ADD COLUMN direct_snapshot_version INTEGER NOT NULL DEFAULT 0')
        db.execute('ALTER TABLE coordinator_identity ADD COLUMN direct_snapshot_session_count INTEGER NOT NULL DEFAULT 0')
        db.execute('ALTER TABLE coordinator_identity ADD COLUMN direct_snapshot_page_count INTEGER NOT NULL DEFAULT 0')
        for sql in TABLES.values():db.execute(sql)
        for sql in TRIGGERS:db.execute(sql)
        db.execute('UPDATE coordinator_identity SET direct_snapshot_version=2 WHERE singleton=1')
    metadata_bytes(db)

def _b64(value,maximum):
    if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9_-]+',value) or len(value)>((maximum+2)//3)*4:
        raise ValueError('invalid encoding')
    try:raw=base64.urlsafe_b64decode(value+'='*((-len(value))%4))
    except (ValueError,binascii.Error):raise ValueError('invalid encoding') from None
    if len(raw)>maximum or base64.urlsafe_b64encode(raw).decode().rstrip('=')!=value:raise ValueError('noncanonical encoding')
    return raw

class SnapshotPageJournal:
    def __init__(self,store,reservations):
        from .direct_v2_reservation import DirectV2Reservations
        if not isinstance(reservations,DirectV2Reservations) or reservations.store is not store or reservations.queue is not store.queue:
            raise ValueError('exact direct coordinator required')
        self.store,self.queue,self.reservations=store,store.queue,reservations
        self._completion=threading.BoundedSemaphore(1)

    def _scope(self,db,reservation,principal,environment):
        row=validate_receiving(db,reservation)
        if row['principal']!=principal or row['current_environment']!=environment:raise Conflict('snapshot unavailable')
        return row

    def _dispatch_from_listener(self,scope,reservation,snapshot,operation,body):
        # This method is private to the listener; never map a client header to
        # scope. The listener obtains scope from its verified mTLS certificate.
        if type(scope) is not tuple or len(scope)!=2:return Response(403,{'error':'snapshot_unavailable'})
        principal,environment=scope
        try:
            if operation=='begin':return Response(200,self._begin(reservation,snapshot,principal,environment,body))
            if operation.startswith('pages/'):return Response(200,self._page(reservation,snapshot,principal,environment,int(operation[6:]),body))
            if operation.startswith('selected-pages/'):return Response(200,self._selected_page(reservation,snapshot,principal,environment,int(operation[15:]),body))
            if operation in ('complete','complete-selected'):
                if body:raise ValueError('empty completion required')
                if not self._completion.acquire(blocking=False):return Response(429,{'error':'snapshot_busy'})
                try:return Response(200,self._complete(reservation,snapshot,principal,environment,include_selected=operation=='complete-selected'))
                finally:self._completion.release()
            return Response(404,{'error':'not_found'})
        except (ValueError,TypeError,KeyError,Conflict):return Response(409,{'error':'snapshot_unavailable'})

    def _begin(self,reservation,snapshot,principal,environment,body):
        request=p.decode_submission(body);p._closed(request,('intentBase64url','signatureBase64url'),'snapshot begin')
        raw=_b64(request['intentBase64url'],MAX_INTENT_BYTES);signature=_b64(request['signatureBase64url'],64)
        if len(signature)!=64:raise ValueError('signature required')
        intent=_decode(raw,MAX_INTENT_BYTES)
        if type(intent) is not dict or set(intent)!=INTENT_FIELDS or intent['protocol']!='ltds-viewer-held-source-snapshot-v1' or intent['snapshotId']!=snapshot:
            raise ValueError('snapshot identity invalid')
        hashes=intent['sourcePageHashes'];size=intent['sourceFrameBytes']
        if (type(hashes) is not list or not 1<=len(hashes)<=MAX_PAGES or any(type(x) is not str or not re.fullmatch(r'[a-f0-9]{64}',x) for x in hashes)
            or type(size) is not int or not len(hashes)<=size<=min(MAX_FRAME_BYTES,len(hashes)*(MAX_PAGE_BYTES+1))
            or intent['gcp'] is not None):raise ValueError('unsupported source pages')
        with self.queue.coordinator_operation('source-io-admission'),self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');row=self._scope(db,reservation,principal,environment)
            attestation=json.loads(row['source_attestation_json'])
            key=self.reservations._snapshot_pins.get((principal,environment,intent['issuerId'],intent['keyId']))
            if key is None or hashlib.sha256(raw).hexdigest()!=attestation['proof']['sourceSnapshotSha256']:
                raise Conflict('snapshot identity unavailable')
            try:key.verify(signature,SNAPSHOT_DOMAIN+raw)
            except InvalidSignature:raise Conflict('snapshot identity unavailable') from None
            ensure(db)
            prior=db.execute('SELECT * FROM direct_snapshot_sessions WHERE reservation=?',(reservation,)).fetchone()
            if prior:
                if (prior['snapshot'],prior['principal'],prior['environment'],prior['intent'],prior['signature'])!=(snapshot,principal,environment,raw,signature):
                    raise Conflict('snapshot session changed')
                return {'state':'snapshot_receiving','sourceCustodyAccepted':False}
            count=db.execute('SELECT direct_snapshot_session_count FROM coordinator_identity WHERE singleton=1').fetchone()[0]
            if count>=MAX_SESSIONS:raise Conflict('snapshot journal capacity reached')
            allowance=len(raw)+size+SESSION_OVERHEAD
            self.store._quota(db,allowance)
            db.execute('INSERT INTO direct_snapshot_sessions VALUES(?,?,?,?,?,?,?,?)',(reservation,snapshot,principal,environment,raw,signature,size,allowance))
            db.execute('UPDATE coordinator_identity SET direct_snapshot_session_count=direct_snapshot_session_count+1 WHERE singleton=1')
            return {'state':'snapshot_receiving','sourceCustodyAccepted':False}

    def _page(self,reservation,snapshot,principal,environment,index,body):
        if type(index) is not int or not 0<=index<MAX_PAGES or type(body) is not bytes or not 0<len(body)<=MAX_PAGE_BYTES:raise ValueError('invalid source page')
        frame=_decode(body,MAX_PAGE_BYTES)
        if (type(frame) is not dict or set(frame)!={'version','index','records'} or frame['version']!='ltds-viewer-source-frames-v1'
                or type(frame['index']) is not int or frame['index']!=index or type(frame['records']) is not list or not 1<=len(frame['records'])<=512):raise ValueError('invalid source page')
        with self.queue.coordinator_operation('source-io-admission'),self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');self._scope(db,reservation,principal,environment);metadata_bytes(db)
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='direct_snapshot_sessions'").fetchone():
                raise Conflict('snapshot session unavailable')
            session=db.execute('SELECT * FROM direct_snapshot_sessions WHERE reservation=?',(reservation,)).fetchone()
            if session is None or (session['snapshot'],session['principal'],session['environment'])!=(snapshot,principal,environment):raise Conflict('snapshot session unavailable')
            intent=_decode(session['intent'],MAX_INTENT_BYTES)
            if type(intent.get('sourceFrameBytes')) is not int or intent['sourceFrameBytes']!=session['frame_bytes']:
                raise Conflict('source frame reservation changed')
            if index>=len(intent['sourcePageHashes']) or hashlib.sha256(PAGE_DOMAIN+body).hexdigest()!=intent['sourcePageHashes'][index]:raise Conflict('source page differs')
            old=db.execute('SELECT page FROM direct_snapshot_pages WHERE reservation=? AND page_index=?',(reservation,index)).fetchone()
            if old:
                if not hmac.compare_digest(old['page'],body):raise Conflict('source page differs')
            else:
                total=db.execute('SELECT COALESCE(SUM(length(page)+1),0) FROM direct_snapshot_pages WHERE reservation=?',(reservation,)).fetchone()[0]
                if total+len(body)+1>intent['sourceFrameBytes'] or total+len(body)+1>MAX_FRAME_BYTES:raise Conflict('source frame budget exceeded')
                db.execute('INSERT INTO direct_snapshot_pages VALUES(?,?,?)',(reservation,index,body))
                db.execute('UPDATE coordinator_identity SET direct_snapshot_page_count=direct_snapshot_page_count+1 WHERE singleton=1')
            return {'state':'snapshot_receiving','sourceCustodyAccepted':False}

    def _selected_page(self,reservation,snapshot,principal,environment,index,body):
        from .direct_selected_gcp_frames import SELECTED_DOMAIN
        if type(index) is not int or not 0<=index<MAX_PAGES or type(body) is not bytes or not 0<len(body)<=MAX_PAGE_BYTES:raise ValueError('invalid selected page')
        frame=_decode(body,MAX_PAGE_BYTES)
        if (type(frame) is not dict or set(frame)!={'version','index','records'} or frame['version']!='ltds-viewer-selected-frames-v1'
                or type(frame['index']) is not int or frame['index']!=index or type(frame['records']) is not list or not 1<=len(frame['records'])<=512):raise ValueError('invalid selected page')
        with self.queue.coordinator_operation('source-io-admission'),self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');self._scope(db,reservation,principal,environment);metadata_bytes(db)
            session=db.execute('SELECT * FROM direct_snapshot_sessions WHERE reservation=?',(reservation,)).fetchone()
            if session is None or (session['snapshot'],session['principal'],session['environment'])!=(snapshot,principal,environment):raise Conflict('snapshot session unavailable')
            intent=_decode(session['intent'],MAX_INTENT_BYTES);hashes=intent['selectedPageHashes'];size=intent['selectedFrameBytes']
            if (intent['gcp'] is not None or type(hashes) is not list or not 1<=len(hashes)<=MAX_PAGES
                    or any(type(x) is not str or not re.fullmatch('[a-f0-9]{64}',x) for x in hashes)
                    or type(size) is not int or not len(hashes)<=size<=min(MAX_FRAME_BYTES,len(hashes)*(MAX_PAGE_BYTES+1))
                    or index>=len(hashes) or hashlib.sha256(SELECTED_DOMAIN+body).hexdigest()!=hashes[index]):raise Conflict('selected page differs')
            ensure_selected(db)
            selected=db.execute('SELECT * FROM direct_selected_sessions WHERE reservation=?',(reservation,)).fetchone()
            if selected is None:
                if db.execute('SELECT direct_selected_session_count FROM coordinator_identity WHERE singleton=1').fetchone()[0]>=MAX_SESSIONS:raise Conflict('selected capacity reached')
                self.store._quota(db,size+SESSION_OVERHEAD)
                db.execute('INSERT INTO direct_selected_sessions VALUES(?,?,?)',(reservation,size,size+SESSION_OVERHEAD))
                db.execute('UPDATE coordinator_identity SET direct_selected_session_count=direct_selected_session_count+1 WHERE singleton=1')
            elif selected['frame_bytes']!=size:raise Conflict('selected size changed')
            old=db.execute('SELECT page FROM direct_selected_pages WHERE reservation=? AND page_index=?',(reservation,index)).fetchone()
            if old:
                if not hmac.compare_digest(old['page'],body):raise Conflict('selected page differs')
            else:
                total=db.execute('SELECT COALESCE(SUM(length(page)+1),0) FROM direct_selected_pages WHERE reservation=?',(reservation,)).fetchone()[0]
                if total+len(body)+1>size:raise Conflict('selected frame budget exceeded')
                db.execute('INSERT INTO direct_selected_pages VALUES(?,?,?)',(reservation,index,body))
                db.execute('UPDATE coordinator_identity SET direct_selected_page_count=direct_selected_page_count+1 WHERE singleton=1')
            return {'state':'selected_pages_receiving','sourceCustodyAccepted':False,'processingAuthorized':False}

    @staticmethod
    def _inspection_binding(db, reservation, job_id):
        """Capture exact durable verifier inputs; no claim of current custody."""
        values=[]
        for table,key,value in (('direct_v2_reservations','reservation_id',reservation),
                                ('jobs','id',job_id),('reference_jobs','job_id',job_id),
                                ('reference_source_intakes','job_id',job_id)):
            row=db.execute('SELECT * FROM '+table+' WHERE '+key+'=?',(value,)).fetchone()
            if row is None:raise Conflict('snapshot verifier binding unavailable')
            values.append(tuple(row))
        return tuple(values)

    def _complete(self,reservation,snapshot,principal,environment,*,include_selected=False):
        with self.queue.coordinator_operation('source-io-admission'):
            with self.queue._db() as db:
                db.execute('BEGIN');scope=self._scope(db,reservation,principal,environment);metadata_bytes(db)
                binding=self._inspection_binding(db,reservation,scope['job_id']) if include_selected else None
                if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='direct_snapshot_sessions'").fetchone():
                    raise Conflict('snapshot session unavailable')
                session=db.execute('SELECT * FROM direct_snapshot_sessions WHERE reservation=?',(reservation,)).fetchone()
                if session is None or (session['snapshot'],session['principal'],session['environment'])!=(snapshot,principal,environment):raise Conflict('snapshot session unavailable')
                intent=_decode(session['intent'],MAX_INTENT_BYTES)
                if type(intent.get('sourceFrameBytes')) is not int or intent['sourceFrameBytes']!=session['frame_bytes']:
                    raise Conflict('source frame reservation changed')
                rows=db.execute('SELECT page_index,page FROM direct_snapshot_pages WHERE reservation=? ORDER BY page_index',(reservation,)).fetchall()
                if len(rows)!=len(intent['sourcePageHashes']) or any(row['page_index']!=i for i,row in enumerate(rows)):raise Conflict('source pages incomplete')
                frames=b''.join(row['page']+b'\n' for row in rows)
                if len(frames)!=intent['sourceFrameBytes']:raise Conflict('source frames incomplete')
                raw,signature=session['intent'],session['signature']
                selected_frames=None
                if include_selected:
                    selected_metadata_bytes(db)
                    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='direct_selected_sessions'").fetchone():raise Conflict('selected pages unavailable')
                    selected=db.execute('SELECT frame_bytes FROM direct_selected_sessions WHERE reservation=?',(reservation,)).fetchone()
                    if selected is None or selected['frame_bytes']!=intent['selectedFrameBytes']:raise Conflict('selected pages unavailable')
                    selected_rows=db.execute('SELECT page_index,page FROM direct_selected_pages WHERE reservation=? ORDER BY page_index',(reservation,)).fetchall()
                    if len(selected_rows)!=len(intent['selectedPageHashes']) or any(row['page_index']!=i for i,row in enumerate(selected_rows)):raise Conflict('selected pages incomplete')
                    selected_frames=b''.join(row['page']+b'\n' for row in selected_rows)
                    if len(selected_frames)!=selected['frame_bytes']:raise Conflict('selected frames incomplete')
            result=self.reservations.inspect_held_snapshot(reservation,authenticated_principal=principal,
                issuer_id=intent['issuerId'],key_id=intent['keyId'],intent_bytes=raw,signature=signature,source_frames=frames,selected_frames=selected_frames)
            if include_selected:
                # Historical inspection only. Neither this journal nor the
                # returned observation attests current custody or permits release.
                if (type(result) is not dict or result.get('state')!='source_and_selected_pages_verified_not_custody'
                        or result.get('sourceCustodyAccepted') is not False or result.get('processingAuthorized') is not False
                        or result.get('sourceSnapshotSha256')!=hashlib.sha256(raw).hexdigest()
                        or type(result.get('selectedCount')) is not int or result['selectedCount']!=intent['selectedCount']):raise Conflict('snapshot inspection unavailable')
                source_sha=hashlib.sha256(raw).hexdigest();selected_sha=hashlib.sha256(selected_frames).hexdigest()
                wire=p.canonical_json(result)
                if len(wire)>4096:raise Conflict('snapshot inspection exceeds allowance')
                with self.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE');row=self._scope(db,reservation,principal,environment)
                    if self._inspection_binding(db,reservation,row['job_id'])!=binding:raise Conflict('snapshot verifier binding changed')
                    metadata_bytes(db);selected_metadata_bytes(db);ensure_observations(db)
                    parent=db.execute('SELECT snapshot,principal,environment,intent,signature FROM direct_snapshot_sessions WHERE reservation=?',(reservation,)).fetchone()
                    if parent is None or tuple(parent)!=(snapshot,principal,environment,raw,signature):raise Conflict('snapshot identity changed')
                    saved_source=db.execute('SELECT page FROM direct_snapshot_pages WHERE reservation=? ORDER BY page_index',(reservation,)).fetchall()
                    if b''.join(x['page']+b'\n' for x in saved_source)!=frames:raise Conflict('source pages changed')
                    saved_frames=db.execute('SELECT page FROM direct_selected_pages WHERE reservation=? ORDER BY page_index',(reservation,)).fetchall()
                    if hashlib.sha256(b''.join(x['page']+b'\n' for x in saved_frames)).hexdigest()!=selected_sha:raise Conflict('selected pages changed')
                    pipeline_sha=row['pipeline_manifest_sha']
                    if type(pipeline_sha) is not str or not re.fullmatch('[a-f0-9]{64}',pipeline_sha):raise Conflict('pipeline binding unavailable')
                    old=db.execute('SELECT * FROM direct_snapshot_observations WHERE reservation=?',(reservation,)).fetchone()
                    values=(snapshot,principal,environment,source_sha,selected_sha,pipeline_sha,wire)
                    if old:
                        if tuple(old)[1:]!=values:raise Conflict('snapshot observation replay changed')
                    else:
                        self.store._quota(db,OBS_ALLOWANCE)
                        db.execute('INSERT INTO direct_snapshot_observations VALUES(?,?,?,?,?,?,?,?)',(reservation,*values))
                        db.execute('UPDATE coordinator_identity SET direct_snapshot_observation_count=direct_snapshot_observation_count+1 WHERE singleton=1')
            return result
