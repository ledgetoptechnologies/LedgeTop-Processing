"""Disabled v2 client delivery journal. No HTTP or automatic retries.

A durable dispatch marker is irreversible. Synthetic reconciliation/adoption
proof formats here are placeholders, not the Viewer protocol. External writers,
old binaries and restored database copies are not fenced.
"""
import hashlib
import hmac
import json
import os
from pathlib import Path
import sqlite3
import stat
from contextlib import contextmanager
from . import reference_transfer_v2_contract_candidate as contract

QUALIFIED=False
EVIDENCE_QUALIFIED=False
ABSENCE_QUALIFIED=False
ADOPTION_QUALIFIED=False
DOMAIN=b'ltds-v2-client-proof-synthetic-v1\0'
ADOPTION_DOMAIN=b'ltds-v2-client-adoption-synthetic-v1\0'
DDL_REQUEST=('CREATE TABLE v2_requests (request_id TEXT PRIMARY KEY, request_hash TEXT NOT NULL, canonical TEXT NOT NULL)')
DDL_DELIVERY=("CREATE TABLE v2_deliveries (delivery_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, admission TEXT NOT NULL, "
              "state TEXT NOT NULL CHECK(state IN ('prepared','dispatch_started','recorded','absent','unknown')), observation TEXT)")
DDL_ADOPTION=('CREATE TABLE v2_adoptions (request_id TEXT NOT NULL, recovery_generation INTEGER NOT NULL, '
              'record TEXT NOT NULL, PRIMARY KEY(request_id,recovery_generation))')
DDL_STREAM=("CREATE TABLE v2_local_streams (delivery_id TEXT PRIMARY KEY, identity TEXT NOT NULL, "
            "file_name TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('started','verified'))) ")
DDL_STREAM=DDL_STREAM.rstrip()
SCHEMA=(('v2_requests',DDL_REQUEST),('v2_deliveries',DDL_DELIVERY),('v2_adoptions',DDL_ADOPTION),('v2_local_streams',DDL_STREAM))

class JournalHeld(ValueError):pass

def _encode(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)

def _mac(secret,domain,value):return hmac.new(secret,domain+_encode(value).encode('ascii'),hashlib.sha256).hexdigest()

class V2ClientJournal:
    def __init__(self,path,owner,*,enabled=False):
        from ops.reference_worker_daemon import DaemonStateLock
        if QUALIFIED is not True or enabled is not True or type(owner) is not DaemonStateLock or os.name!='posix':
            raise JournalHeld('v2 client journal is not qualified')
        self.path=Path(path).absolute();self.owner=owner;self.pid=os.getpid()
        if self.path.parent!=owner.root or self.path.resolve()!=self.path or self.path.is_symlink():raise JournalHeld('v2 journal path unavailable')
        owner.check(self.path.parent)
        fd=os.open(self.path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
        try:
            opened=os.fstat(fd);named=self.path.lstat()
            if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink!=1 or opened.st_uid!=os.geteuid()
                    or stat.S_IMODE(opened.st_mode)!=0o600 or (opened.st_dev,opened.st_ino)!=(named.st_dev,named.st_ino)):
                raise JournalHeld('v2 journal identity unavailable')
            self.identity=(opened.st_dev,opened.st_ino)
        finally:os.close(fd)
        with self._tx() as db:
            present=[db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone() for name,_ in SCHEMA]
            if any(present) and not all(present):raise JournalHeld('v2 journal schema incomplete')
            if not any(present):
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1").fetchone():
                    raise JournalHeld('v2 journal requires new empty state')
                for _,ddl in SCHEMA:db.execute(ddl)
            else:self._attest(db)

    @staticmethod
    def _attest(db):
        for name,ddl in SCHEMA:
            row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()
            if row is None or row[0]!=ddl:raise JournalHeld('v2 journal schema changed')

    def _check(self):
        if QUALIFIED is not True or self.pid!=os.getpid():raise JournalHeld('v2 journal owner changed')
        self.owner.check(self.path.parent)
        info=self.path.lstat()
        if not stat.S_ISREG(info.st_mode) or (info.st_dev,info.st_ino)!=self.identity:raise JournalHeld('v2 journal file changed')

    @contextmanager
    def _tx(self):
        self._check();db=sqlite3.connect(self.path,timeout=5);db.row_factory=sqlite3.Row
        try:
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            yield db
            self._check();db.commit()
        except BaseException:
            db.rollback();raise
        finally:db.close()

    def prepare(self,raw):
        """Create or exactly replay inert local intent; never dispatch."""
        admission,request_hash=contract.parse_admission(raw)
        request=dict(admission);delivery_id=request.pop('deliveryId');canonical=_encode(request)
        with self._tx() as db:
            self._attest(db)
            db.execute('INSERT OR IGNORE INTO v2_requests VALUES(?,?,?)',(request['requestId'],request_hash,canonical))
            row=db.execute('SELECT request_hash,canonical FROM v2_requests WHERE request_id=?',(request['requestId'],)).fetchone()
            if row is None or tuple(row)!=(request_hash,canonical):raise JournalHeld('v2 request identity changed')
            existing=db.execute('SELECT delivery_id FROM v2_deliveries WHERE request_id=?',(request['requestId'],)).fetchone()
            if existing is not None and existing['delivery_id']!=delivery_id:
                raise JournalHeld('v2 new delivery authority is not qualified')
            db.execute("INSERT OR IGNORE INTO v2_deliveries VALUES(?,?,?,'prepared',NULL)",(delivery_id,request['requestId'],raw.decode('ascii')))
            row=db.execute('SELECT request_id,admission,state FROM v2_deliveries WHERE delivery_id=?',(delivery_id,)).fetchone()
            if row is None or (row['request_id'],row['admission'])!=(request['requestId'],raw.decode('ascii')):
                raise JournalHeld('v2 delivery identity changed')
            return row['state']

    def start_once(self,request_id,delivery_id):
        """Mark uncertainty before dispatch; same delivery can never start twice."""
        with self._tx() as db:
            self._attest(db)
            if db.execute('SELECT 1 FROM v2_adoptions WHERE request_id=? LIMIT 1',(request_id,)).fetchone():
                raise JournalHeld('v2 original authority has been superseded')
            row=db.execute('''SELECT d.admission,d.state,r.request_hash,r.canonical FROM v2_deliveries d
                JOIN v2_requests r ON r.request_id=d.request_id
                WHERE d.request_id=? AND d.delivery_id=?''',(request_id,delivery_id)).fetchone()
            if row is None or row['state']!='prepared':raise JournalHeld('v2 delivery requires reconciliation')
            try:
                admission,digest=contract.parse_admission(row['admission'].encode('ascii'))
                canonical=dict(admission);canonical.pop('deliveryId')
                valid=(admission['requestId']==request_id and admission['deliveryId']==delivery_id
                    and digest==row['request_hash'] and _encode(canonical)==row['canonical'])
            except Exception:
                valid=False
            if not valid:raise JournalHeld('v2 persisted dispatch identity changed')
            if db.execute("UPDATE v2_deliveries SET state='dispatch_started' WHERE request_id=? AND delivery_id=? AND state='prepared'",(request_id,delivery_id)).rowcount!=1:
                raise JournalHeld('v2 delivery requires reconciliation')

    def observe_synthetic(self,record,signature,*,secret):
        """Test-only authenticated placeholder; never treat as Viewer proof."""
        if EVIDENCE_QUALIFIED is not True or type(secret) is not bytes or len(secret)<32:
            raise JournalHeld('v2 reconciliation proof is not qualified')
        if type(record) is dict and record.get('status')=='absent' and ABSENCE_QUALIFIED is not True:
            raise JournalHeld('v2 authoritative absence is not qualified')
        fields={'requestId','requestHash','deliveryId','status','chargedBytes','ledgerEpoch','revision','recoveryGeneration','fenced',
                'authorityId','authorityEpoch','claimLineageSha256'}
        if (type(record) is not dict or set(record)!=fields or record['status'] not in ('recorded','absent','unknown')
                or type(record['chargedBytes']) is not int or not 0<=record['chargedBytes']<=2**53-1
                or type(record['revision']) is not int or record['revision']<1
                or type(record['recoveryGeneration']) is not int or record['recoveryGeneration']!=0
                or type(record['ledgerEpoch']) is not str or not record['ledgerEpoch']
                or type(record['fenced']) is not bool or type(signature) is not str
                or not hmac.compare_digest(_mac(secret,DOMAIN,record),signature)
                or (record['status']=='absent' and (not record['fenced'] or record['chargedBytes']!=0))):
            raise JournalHeld('v2 reconciliation evidence invalid')
        with self._tx() as db:
            self._attest(db)
            row=db.execute('''SELECT d.request_id,d.state,d.observation,d.admission,r.request_hash FROM v2_deliveries d
                JOIN v2_requests r ON r.request_id=d.request_id WHERE d.delivery_id=?''',(record['deliveryId'],)).fetchone()
            if row is None or (row['request_id'],row['request_hash'])!=(record['requestId'],record['requestHash']):
                raise JournalHeld('v2 reconciliation identity changed')
            if db.execute('SELECT 1 FROM v2_adoptions WHERE request_id=? LIMIT 1',(record['requestId'],)).fetchone():
                raise JournalHeld('v2 original authority has been superseded')
            if db.execute("SELECT 1 FROM v2_local_streams WHERE delivery_id=? AND state='started'",(record['deliveryId'],)).fetchone():
                raise JournalHeld('v2 local stream requires fencing')
            binding=json.loads(row['admission'])['binding']
            if ((record['authorityId'],record['authorityEpoch'],record['claimLineageSha256']) !=
                    (binding['authorityId'],binding['epoch'],binding['claimLineageSha256'])
                    or (record['status']=='recorded' and record['chargedBytes']!=binding['objectLength'])):
                raise JournalHeld('v2 reconciliation authority changed')
            encoded=_encode(record);state=record['status']
            if row['state'] in ('recorded','absent','unknown'):
                if row['state']==state and row['observation']==encoded:return state
                previous=json.loads(row['observation'])
                if (row['state']!='unknown' or record['ledgerEpoch']!=previous['ledgerEpoch']
                        or record['revision']<=previous['revision']):raise JournalHeld('v2 reconciliation changed')
                db.execute('UPDATE v2_deliveries SET state=?,observation=? WHERE delivery_id=?',(state,encoded,record['deliveryId']))
                return state
            if row['state']!='dispatch_started':raise JournalHeld('v2 dispatch not started')
            db.execute('UPDATE v2_deliveries SET state=?,observation=? WHERE delivery_id=?',(state,encoded,record['deliveryId']))
            return state

    def adopt_synthetic(self,record,signature,*,secret):
        """Test-only adoption audit, not authorization to send or reconcile."""
        if ADOPTION_QUALIFIED is not True or type(secret) is not bytes or len(secret)<32:
            raise JournalHeld('v2 recovery adoption is not qualified')
        fields={'requestId','requestHash','recoveryGeneration','recoveryActor','authorityId','authorityEpoch',
                'originalEpoch','originalClaimLineageSha256','priorFenced'}
        if (type(record) is not dict or set(record)!=fields or type(record['recoveryGeneration']) is not int
                or record['recoveryGeneration']<1 or type(record['recoveryActor']) is not str or not record['recoveryActor']
                or type(record['authorityEpoch']) is not str or not record['authorityEpoch'] or record['priorFenced'] is not True
                or type(signature) is not str or not hmac.compare_digest(_mac(secret,ADOPTION_DOMAIN,record),signature)):
            raise JournalHeld('v2 recovery adoption invalid')
        with self._tx() as db:
            self._attest(db)
            row=db.execute('SELECT request_hash,canonical FROM v2_requests WHERE request_id=?',(record['requestId'],)).fetchone()
            if row is None or row[0]!=record['requestHash']:raise JournalHeld('v2 recovery binding changed')
            binding=json.loads(row['canonical'])['binding']
            if ((record['authorityId'],record['originalEpoch'],record['originalClaimLineageSha256']) !=
                    (binding['authorityId'],binding['epoch'],binding['claimLineageSha256'])
                    or record['recoveryActor']==binding['workerInstanceId']):
                raise JournalHeld('v2 recovery binding changed')
            if db.execute("""SELECT 1 FROM v2_local_streams s JOIN v2_deliveries d ON d.delivery_id=s.delivery_id
                    WHERE d.request_id=? AND s.state='started' LIMIT 1""",(record['requestId'],)).fetchone():
                raise JournalHeld('v2 local stream requires fencing')
            last=db.execute('SELECT MAX(recovery_generation) FROM v2_adoptions WHERE request_id=?',(record['requestId'],)).fetchone()[0] or 0
            encoded=_encode(record)
            existing=db.execute('SELECT record FROM v2_adoptions WHERE request_id=? AND recovery_generation=?',(record['requestId'],record['recoveryGeneration'])).fetchone()
            if existing is not None:
                if existing[0]!=encoded:raise JournalHeld('v2 recovery adoption changed')
                return
            if record['recoveryGeneration']!=last+1:raise JournalHeld('v2 recovery generation changed')
            db.execute('INSERT INTO v2_adoptions VALUES(?,?,?)',(record['requestId'],record['recoveryGeneration'],encoded))
