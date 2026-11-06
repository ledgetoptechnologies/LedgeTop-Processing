"""Unwired coordinator V2 handoff journal; no Viewer custody acknowledgement."""
import hashlib
import hmac
import json

from . import reference_protocol as p
from .direct_v2_reservation_schema import validate_receiving
from .direct_worker_sender_v2 import DirectWorkerSenderV2
from .maintenance import Conflict

SESSION_ALLOWANCE=131072
OBJECT_ALLOWANCE=2048
TABLES={
    'direct_v2_handoff_sessions': '''CREATE TABLE direct_v2_handoff_sessions (
        reservation TEXT PRIMARY KEY,principal TEXT NOT NULL,binding TEXT NOT NULL,
        allowance INTEGER NOT NULL,state TEXT NOT NULL,closure BLOB)''',
    'direct_v2_handoff_objects': '''CREATE TABLE direct_v2_handoff_objects (
        reservation TEXT NOT NULL,object_id TEXT NOT NULL,byte_size INTEGER NOT NULL,
        sha256 TEXT NOT NULL,state TEXT NOT NULL,observation BLOB,
        PRIMARY KEY(reservation,object_id))''',
}


def _normalize(sql):return ' '.join(sql.split())


def check_schema(db):
    for name,sql in TABLES.items():
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()
        if row is None or type(row['sql']) is not str or _normalize(row['sql'])!=_normalize(sql):
            raise Conflict('V2 handoff journal schema requires inspection')


def metadata_bytes(db):
    columns={row['name'] for row in db.execute('PRAGMA table_info(coordinator_identity)')}
    version=0 if 'direct_v2_handoff_version' not in columns else db.execute(
        'SELECT direct_v2_handoff_version FROM coordinator_identity WHERE singleton=1').fetchone()[0]
    names={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    present=set(TABLES)&names
    if version==0 and not present:return 0
    if type(version) is not int or version!=1 or present!=set(TABLES):
        raise Conflict('V2 handoff journal marker requires inspection')
    check_schema(db)
    invalid=db.execute('''SELECT 1 FROM direct_v2_handoff_sessions WHERE typeof(allowance)!='integer'
        OR allowance<? OR allowance>? OR (allowance-?)%?!=0 OR state NOT IN ('receiving','closure_pending','closure_observed')
        OR (state='closure_observed')!=(closure IS NOT NULL) OR (closure IS NOT NULL AND
        (typeof(closure)!='blob' OR length(closure)>65536)) LIMIT 1''',
        (SESSION_ALLOWANCE,SESSION_ALLOWANCE+OBJECT_ALLOWANCE*10001,SESSION_ALLOWANCE,OBJECT_ALLOWANCE)).fetchone()
    if invalid:raise Conflict('V2 handoff journal allowance invalid')
    return db.execute('SELECT COALESCE(SUM(allowance),0) FROM direct_v2_handoff_sessions').fetchone()[0]


def ensure_schema(db):
    if not db.in_transaction:raise Conflict('V2 handoff journal transaction required')
    columns={row['name'] for row in db.execute('PRAGMA table_info(coordinator_identity)')}
    if 'direct_v2_handoff_version' not in columns:
        db.execute('ALTER TABLE coordinator_identity ADD COLUMN direct_v2_handoff_version INTEGER NOT NULL DEFAULT 0')
    version=db.execute('SELECT direct_v2_handoff_version FROM coordinator_identity WHERE singleton=1').fetchone()[0]
    names={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    present=set(TABLES)&names
    if version==0 and not present:
        for sql in TABLES.values():db.execute(sql)
        db.execute('UPDATE coordinator_identity SET direct_v2_handoff_version=1 WHERE singleton=1')
    metadata_bytes(db)


class DirectV2Handoff:
    """Trusted internal adapter around TLS; the runtime must authenticate callers."""
    def __init__(self,store,sender,*,principal,bearer):
        if not isinstance(sender,DirectWorkerSenderV2):raise ValueError('verified V2 worker sender required')
        p._slug(principal,'principal')
        if type(bearer) is not str or not 32<=len(bearer)<=4096 or not bearer.isascii():
            raise ValueError('scoped V2 bearer required')
        self.store,self.queue,self.sender=store,store.queue,sender
        self.item=dict(sender._item);self.principal,self._bearer=principal,bearer
        self.binding=hashlib.sha256(p.canonical_json(self.item)).hexdigest()
        self.ids=sender._inventory.object_ids(reservation_binding=sender._reservation)
        with self.queue.coordinator_operation('source-io-admission'),self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');ensure_schema(db)

    def _auth(self,token,protected_transport):
        if protected_transport is not True or type(token) is not str or not token.isascii() or not hmac.compare_digest(token,self._bearer):
            raise Conflict('V2 handoff authorization required')

    def _check(self,db):
        row=validate_receiving(db,self.item['reservationId'])
        if row['principal']!=self.principal or p.canonical_json(self.item)!=row['item_json']:
            raise Conflict('V2 handoff reservation changed')
        metadata_bytes(db)

    def _session(self,db):
        self._check(db)
        row=db.execute('SELECT * FROM direct_v2_handoff_sessions WHERE reservation=?',(self.item['reservationId'],)).fetchone()
        allowance=SESSION_ALLOWANCE+OBJECT_ALLOWANCE*len(self.ids)
        if row is None:
            self.store._quota(db,allowance)
            db.execute("INSERT INTO direct_v2_handoff_sessions VALUES(?,?,?,?,'receiving',NULL)",
                       (self.item['reservationId'],self.principal,self.binding,allowance))
            return 'receiving'
        if row['principal']!=self.principal or row['binding']!=self.binding or row['allowance']!=allowance:
            raise Conflict('V2 handoff session changed')
        return row['state']

    def _verify_observation(self,value,obj):
        expected=dict(protocol='ltds-direct-local-object-v2',reservationId=self.item['reservationId'],
            scratchBindingSha256=self.sender._sha,objectId=obj['objectId'],byteSize=obj['byteSize'],
            sha256=obj['sha256'],localObjectDurable=True,sourceCustodyAccepted=False,transferComplete=False)
        if (type(value) is not dict or set(value)!=set(expected)|{'replayed'} or type(value['replayed']) is not bool
                or p.canonical_json({key:val for key,val in value.items() if key!='replayed'})!=p.canonical_json(expected)):
            raise Conflict('V2 worker observation changed')
        return value

    def send_object(self,token,*,protected_transport,object_id,reader):
        self._auth(token,protected_transport)
        obj=self.sender._inventory.object(object_id,reservation_binding=self.sender._reservation)
        with self.queue.coordinator_operation('source-io'):
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE')
                if self._session(db)!='receiving':raise Conflict('V2 handoff closed')
                row=db.execute('SELECT * FROM direct_v2_handoff_objects WHERE reservation=? AND object_id=?',
                               (self.item['reservationId'],object_id)).fetchone()
                if row is not None:
                    if (row['byte_size'],row['sha256'])!=(obj['byteSize'],obj['sha256']):raise Conflict('V2 object binding changed')
                    if row['state']=='observed' and row['observation'] is not None:
                        return self._verify_observation(json.loads(row['observation']),obj)
                    raise Conflict('V2 object outcome uncertain; reconciliation required')
                db.execute("INSERT INTO direct_v2_handoff_objects VALUES(?,?,?,?,'intent',NULL)",
                           (self.item['reservationId'],object_id,obj['byteSize'],obj['sha256']))
            result=self._verify_observation(self.sender.put(object_id,reader),obj)
            wire=p.canonical_json(result)
            if len(wire)>768:raise Conflict('V2 object observation too large')
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');self._check(db)
                changed=db.execute("UPDATE direct_v2_handoff_objects SET state='observed',observation=? WHERE reservation=? AND object_id=? AND state='intent' AND observation IS NULL",
                                   (wire,self.item['reservationId'],object_id)).rowcount
                if changed!=1:raise Conflict('V2 object outcome uncertain')
            return result

    def complete(self,token,*,protected_transport):
        self._auth(token,protected_transport)
        with self.queue.coordinator_operation('source-io'):
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');state=self._session(db)
                if state=='closure_observed':
                    from .direct_worker_receipt_v2 import verify
                    saved=db.execute('SELECT closure FROM direct_v2_handoff_sessions WHERE reservation=?',
                                     (self.item['reservationId'],)).fetchone()['closure']
                    return verify(json.loads(saved),expected_closure=self.sender._expected_closure(),
                        issuer_id=self.sender._issuer,key_id=self.sender._key,public_key=self.sender._public)
                if state not in ('receiving','closure_pending'):raise Conflict('V2 closure outcome uncertain')
                rows=db.execute('SELECT * FROM direct_v2_handoff_objects WHERE reservation=?',
                                (self.item['reservationId'],)).fetchall()
                if len(rows)!=len(self.ids):raise Conflict('V2 source inventory incomplete')
                for row in rows:
                    obj=self.sender._inventory.object(row['object_id'],reservation_binding=self.sender._reservation)
                    if (row['state']!='observed' or row['observation'] is None or
                            (row['byte_size'],row['sha256'])!=(obj['byteSize'],obj['sha256'])):
                        raise Conflict('V2 source inventory incomplete')
                    self._verify_observation(json.loads(row['observation']),obj)
                if state=='receiving':
                    db.execute("UPDATE direct_v2_handoff_sessions SET state='closure_pending' WHERE reservation=?",
                               (self.item['reservationId'],))
            result=self.sender.complete();wire=p.canonical_json(result)
            if len(wire)>65536:raise Conflict('V2 closure observation too large')
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');self._check(db)
                changed=db.execute("UPDATE direct_v2_handoff_sessions SET state='closure_observed',closure=? WHERE reservation=? AND state='closure_pending' AND closure IS NULL",
                                   (wire,self.item['reservationId'])).rowcount
                if changed!=1:raise Conflict('V2 closure outcome uncertain')
            return result
