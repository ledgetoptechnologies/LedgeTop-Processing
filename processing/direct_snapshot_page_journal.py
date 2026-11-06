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

MAX_SESSIONS=4
MAX_PAGES=64
MAX_PAGE_BYTES=256*1024
MAX_FRAME_BYTES=12*1024*1024
MAX_INTENT_BYTES=1024*1024
ALLOWANCE=MAX_FRAME_BYTES+MAX_INTENT_BYTES+65536
TABLES={
 'direct_snapshot_sessions':'''CREATE TABLE direct_snapshot_sessions (
  reservation TEXT PRIMARY KEY,snapshot TEXT NOT NULL,principal TEXT NOT NULL,environment TEXT NOT NULL,
  intent BLOB NOT NULL,signature BLOB NOT NULL,allowance INTEGER NOT NULL)''',
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

def metadata_bytes(db):
    cols={r['name'] for r in db.execute('PRAGMA table_info(coordinator_identity)')}
    marker=0 if 'direct_snapshot_version' not in cols else db.execute('SELECT direct_snapshot_version FROM coordinator_identity WHERE singleton=1').fetchone()[0]
    names={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}; present=set(TABLES)&names
    if marker==0 and not present and 'direct_snapshot_session_count' not in cols and 'direct_snapshot_page_count' not in cols:return 0
    if (type(marker) is not int or marker!=1 or present!=set(TABLES)
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
        OR typeof(signature)!='blob' OR length(signature)!=64 OR allowance!=? LIMIT 1''',(MAX_INTENT_BYTES,ALLOWANCE)).fetchone()
    bad_page=db.execute('''SELECT 1 FROM direct_snapshot_pages p LEFT JOIN direct_snapshot_sessions s ON s.reservation=p.reservation
        WHERE s.reservation IS NULL OR typeof(page_index)!='integer' OR page_index<0 OR page_index>=?
        OR typeof(page)!='blob' OR length(page)=0 OR length(page)>? LIMIT 1''',(MAX_PAGES,MAX_PAGE_BYTES)).fetchone()
    if bad or bad_page:raise Conflict('snapshot journal bounds changed')
    return sessions*ALLOWANCE

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
        db.execute('UPDATE coordinator_identity SET direct_snapshot_version=1 WHERE singleton=1')
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
        if row['principal']!=principal or row['environment']!=environment:raise Conflict('snapshot unavailable')
        return row

    def _dispatch_from_listener(self,scope,reservation,snapshot,operation,body):
        # This method is private to the listener; never map a client header to
        # scope. The listener obtains scope from its verified mTLS certificate.
        if type(scope) is not tuple or len(scope)!=2:return Response(403,{'error':'snapshot_unavailable'})
        principal,environment=scope
        try:
            if operation=='begin':return Response(200,self._begin(reservation,snapshot,principal,environment,body))
            if operation.startswith('pages/'):return Response(200,self._page(reservation,snapshot,principal,environment,int(operation[6:]),body))
            if operation=='complete':
                if body:raise ValueError('empty completion required')
                if not self._completion.acquire(blocking=False):return Response(429,{'error':'snapshot_busy'})
                try:return Response(200,self._complete(reservation,snapshot,principal,environment))
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
            self.store._quota(db,ALLOWANCE)
            db.execute('INSERT INTO direct_snapshot_sessions VALUES(?,?,?,?,?,?,?)',(reservation,snapshot,principal,environment,raw,signature,ALLOWANCE))
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

    def _complete(self,reservation,snapshot,principal,environment):
        with self.queue.coordinator_operation('source-io-admission'),self.queue._db() as db:
            db.execute('BEGIN');self._scope(db,reservation,principal,environment);metadata_bytes(db)
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='direct_snapshot_sessions'").fetchone():
                raise Conflict('snapshot session unavailable')
            session=db.execute('SELECT * FROM direct_snapshot_sessions WHERE reservation=?',(reservation,)).fetchone()
            if session is None or (session['snapshot'],session['principal'],session['environment'])!=(snapshot,principal,environment):raise Conflict('snapshot session unavailable')
            intent=_decode(session['intent'],MAX_INTENT_BYTES)
            rows=db.execute('SELECT page_index,page FROM direct_snapshot_pages WHERE reservation=? ORDER BY page_index',(reservation,)).fetchall()
            if len(rows)!=len(intent['sourcePageHashes']) or any(row['page_index']!=i for i,row in enumerate(rows)):raise Conflict('source pages incomplete')
            frames=b''.join(row['page']+b'\n' for row in rows)
            if len(frames)!=intent['sourceFrameBytes']:raise Conflict('source frames incomplete')
            raw,signature=session['intent'],session['signature']
        return self.reservations.inspect_held_snapshot(reservation,authenticated_principal=principal,
            issuer_id=intent['issuerId'],key_id=intent['keyId'],intent_bytes=raw,signature=signature,source_frames=frames)
