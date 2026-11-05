"""Authenticated, immutable LiDAR provenance metadata from the configured broker.

No acquisition observations or receipts are invented here. A verified TLS read
of the grant-bound NAS issuer is the authentication boundary; local verification
only recognizes that exact persisted receipt during its fresh-read/grant window.
Metadata requests neither spend nor expand the input object's byte budget.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
import stat
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request

from . import reference_protocol as p
from .reference_transfer import BrokerClient, ValidatedGrant, _now
from .treedar_admission import AdmissionPolicy, _provenance

PREFIX = '/api/processing/reference/v1/provenance/'


class ProvenanceUnavailable(ValueError):
    """Bounded diagnostic only; never includes grants, endpoints or responses."""


class LidarProvenanceClient:
    def __init__(self, broker, *, state_path, clock=time.time, max_metadata_bytes=65536,
                 max_concurrent=2, freshness_seconds=300, policy=None):
        if not isinstance(broker, BrokerClient):
            raise ValueError('actual configured broker client required')
        if (type(max_metadata_bytes) is not int or not 1024 <= max_metadata_bytes <= p.MAX_BODY_BYTES
                or type(max_concurrent) is not int or not 1 <= max_concurrent <= 8
                or type(freshness_seconds) is not int or not 1 <= freshness_seconds <= 300):
            raise ValueError('bounded provenance metadata policy required')
        self.broker, self.origin, self.clock = broker, broker.origin, clock
        self.maximum, self.freshness = max_metadata_bytes, freshness_seconds
        self.policy = policy or AdmissionPolicy()
        if not isinstance(self.policy, AdmissionPolicy):
            raise ValueError('explicit LiDAR admission policy required')
        self._reads = threading.BoundedSemaphore(max_concurrent)
        self._inflight_lock,self._inflight = threading.Lock(),set()
        self.path = Path(state_path).absolute()
        self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        if any(item.is_symlink() for item in (self.path,*self.path.parents)):
            raise ValueError('owned private provenance journal required')
        parent = self.path.parent.stat()
        if os.name != 'nt' and (parent.st_uid != os.geteuid() or parent.st_mode & 0o077):
            raise ValueError('owned private provenance journal required')
        descriptor = os.open(self.path, os.O_CREAT|os.O_RDWR|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0), 0o600)
        try:
            info,current = os.fstat(descriptor),self.path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or (info.st_dev,info.st_ino)!=(current.st_dev,current.st_ino)
                    or (os.name!='nt' and info.st_uid!=os.geteuid())):
                raise ValueError('owned regular provenance journal required')
            if os.name!='nt':os.fchmod(descriptor,0o600)
        finally:os.close(descriptor)
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS provenance_binding (id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)')
            db.execute('''CREATE TABLE IF NOT EXISTS provenance_receipts (
                receipt_id TEXT PRIMARY KEY,job_id TEXT NOT NULL,generation INTEGER NOT NULL,
                wrapper TEXT NOT NULL,receipt TEXT NOT NULL,fetched_at REAL NOT NULL,
                grant_expires REAL NOT NULL,usable INTEGER NOT NULL DEFAULT 0,UNIQUE(job_id,generation))''')
            db.execute('CREATE TABLE IF NOT EXISTS provenance_clock (id INTEGER PRIMARY KEY CHECK(id=1),highwater REAL NOT NULL)')
            identity = p.canonical_json(dict(origin=self.origin,maximum=self.maximum,
                freshness=self.freshness,approvedDatums=self.policy.approved_vertical_datums)).decode()
            old=db.execute('SELECT value FROM provenance_binding WHERE id=1').fetchone()
            if old and old[0]!=identity:raise ValueError('provenance issuer or policy changed')
            db.execute('INSERT OR IGNORE INTO provenance_binding VALUES(1,?)',(identity,))

    @contextmanager
    def _db(self):
        db=sqlite3.connect(self.path,timeout=10)
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                db.execute('BEGIN IMMEDIATE')
                yield db
        finally:db.close()

    def _observed_time(self):
        now=_now(self.clock)
        with self._db() as db:
            old=db.execute('SELECT highwater FROM provenance_clock WHERE id=1').fetchone()
            if old and now<old[0]:raise ProvenanceUnavailable('provenance_clock_rollback')
            db.execute('INSERT INTO provenance_clock VALUES(1,?) ON CONFLICT(id) DO UPDATE SET highwater=excluded.highwater',(now,))
        return now

    def _bindings(self,job,grant):
        from .reference_source_intake_canonical import pipeline_digest_for_job
        if not isinstance(grant,ValidatedGrant) or self.broker.origin!=self.origin:
            raise ProvenanceUnavailable('configured_provenance_grant_required')
        _now(lambda:grant.expires)
        if (type(grant.token) is not str or not 1<=len(grant.token)<=4096
                or not all(0x21<=ord(char)<=0x7e for char in grant.token)):
            raise ProvenanceUnavailable('configured_provenance_grant_required')
        p._slug(job['jobId'],'job ID');p._integer(job['generation'],'generation');p._environment(job['environment'])
        pipeline,manifest=job['submission']['pipeline'],job['submission']['manifest']
        if (not pipeline['treedar']['requested'] or pipeline['gaussian_splat']['requested']
                or pipeline_digest_for_job(job)!=job['pipelineDigest']
                or p.manifest_sha256(manifest)!=job['manifestSha256'] or manifest['sha256']!=job['manifestSha256']):
            raise ProvenanceUnavailable('provenance_job_binding_changed')
        receipt_id=pipeline['treedar']['provenanceReceiptId'];p._slug(receipt_id,'provenance receipt ID')
        ids=pipeline['treedar']['inputObjectIds']
        if type(ids) is not list or len(ids)!=1:raise ProvenanceUnavailable('one_lidar_provenance_object_required')
        obj=next((value for value in manifest['objects'] if value['id']==ids[0]),None)
        if obj is None or obj['kind']!='lidar' or not obj['name'].lower().endswith(('.las','.laz')):
            raise ProvenanceUnavailable('bound_lidar_object_required')
        request=job['transferRequest']
        if (grant.job_id,grant.reservation_id,grant.generation,grant.environment)!=(
                job['jobId'],request['reservationId'],job['generation'],job['environment']):
            raise ProvenanceUnavailable('provenance_grant_identity_mismatch')
        expected={(item['id'],item['byteSize'],item['sha256']) for item in manifest['objects']}
        actual={(item.id,item.byte_size,item.sha256) for item in grant.objects}
        if actual!=expected or len(grant.objects)!=len(manifest['objects']):
            raise ProvenanceUnavailable('provenance_grant_closure_mismatch')
        p._slug(obj['id'],'object ID');p._hash(obj['sha256'],'object SHA');p._integer(obj['byteSize'],'object bytes')
        return receipt_id,obj

    def fetch(self,job,grant):
        """Read current issuer state each time; never fall back to cached success."""
        if not self._reads.acquire(blocking=False):raise ProvenanceUnavailable('provenance_read_capacity')
        response=None;receipt_id=None;owns_read=False
        try:
            receipt_id,obj=self._bindings(job,grant)
            with self._inflight_lock:
                if receipt_id in self._inflight:raise ProvenanceUnavailable('provenance_receipt_read_inflight')
                self._inflight.add(receipt_id);owns_read=True
            now=self._observed_time()
            if grant.expires<=now:raise ProvenanceUnavailable('provenance_grant_expired')
            with self._db() as db:
                old=db.execute('SELECT * FROM provenance_receipts WHERE receipt_id=? OR (job_id=? AND generation=?)',
                    (receipt_id,job['jobId'],job['generation'])).fetchall()
                for row in old:
                    prior=json.loads(row['wrapper'])
                    if ((row['receipt_id'],row['job_id'],row['generation'])!=(receipt_id,job['jobId'],job['generation'])
                            or prior['inputManifestSha256']!=job['manifestSha256'] or prior['pipelineDigest']!=job['pipelineDigest']):
                        raise ProvenanceUnavailable('sealed_provenance_cannot_rebind')
                # A read failure/revocation cannot fall back to a previous green
                # cached verification. Immutable evidence remains for audit.
                db.execute('UPDATE provenance_receipts SET usable=0 WHERE receipt_id=?',(receipt_id,))
            url=self.origin+PREFIX+receipt_id
            headers={'Authorization':'Bearer '+grant.token,'Accept':'application/json','Accept-Encoding':'identity',
                'X-LTDS-Job-Id':job['jobId'],'X-LTDS-Generation':str(job['generation']),
                'X-LTDS-Environment':job['environment'],'X-LTDS-Input-Manifest-Sha256':job['manifestSha256'],
                'X-LTDS-Pipeline-Digest':job['pipelineDigest'],'X-LTDS-Object-Id':obj['id'],
                'X-LTDS-Object-Sha256':obj['sha256'],'X-LTDS-Object-Bytes':str(obj['byteSize'])}
            deadline=time.monotonic()+min(self.broker.timeout,30)
            response=self.broker._open(Request(url,headers=headers,method='GET'),timeout=min(self.broker.timeout,30))
            if (response.status!=200 or response.geturl()!=url
                    or response.headers.get('Content-Type','').split(';')[0].strip().lower()!='application/json'
                    or response.headers.get('Content-Encoding','identity').lower()!='identity'):
                raise ProvenanceUnavailable('provenance_response_unavailable')
            declared=response.headers.get('Content-Length')
            if declared is not None and (not declared.isascii() or not declared.isdecimal() or int(declared)>self.maximum):
                raise ProvenanceUnavailable('provenance_metadata_limit')
            chunks,total=[],0
            read=getattr(response,'read1',response.read)
            while True:
                remaining=deadline-time.monotonic()
                if remaining<=0 or grant.expires<=_now(self.clock):
                    raise ProvenanceUnavailable('provenance_read_deadline')
                socket=getattr(getattr(getattr(response,'fp',None),'raw',None),'_sock',None)
                if socket is not None:socket.settimeout(min(remaining,max(0.001,grant.expires-_now(self.clock))))
                chunk=read(min(16384,self.maximum-total+1))
                if not chunk:break
                total+=len(chunk)
                if total>self.maximum:raise ProvenanceUnavailable('provenance_metadata_limit')
                chunks.append(chunk)
            if time.monotonic()>=deadline:
                raise ProvenanceUnavailable('provenance_read_deadline')
            body=b''.join(chunks)
            if len(body)>self.maximum or declared is not None and int(declared)!=len(body):
                raise ProvenanceUnavailable('provenance_metadata_limit')
            wrapper=p.decode_submission(body)
            p._closed(wrapper,('schemaVersion','inputManifestSha256','pipelineDigest','provenance'),'provenance wrapper')
            if (type(wrapper['schemaVersion']) is not int or wrapper['schemaVersion']!=1
                    or wrapper['inputManifestSha256']!=job['manifestSha256'] or wrapper['pipelineDigest']!=job['pipelineDigest']):
                raise ProvenanceUnavailable('provenance_wrapper_binding_changed')
            receipt=_provenance(wrapper['provenance'],lambda _:True,job_id=job['jobId'],generation=job['generation'],
                environment=job['environment'],object_id=obj['id'],expected_sha256=obj['sha256'],expected_bytes=obj['byteSize'],
                allow_synthetic=False,policy=self.policy)
            if receipt['receiptId']!=receipt_id:raise ProvenanceUnavailable('provenance_receipt_identity_changed')
            observed=self._observed_time()
            if grant.expires<=observed:raise ProvenanceUnavailable('provenance_grant_expired')
            encoded,checked=p.canonical_json(wrapper).decode(),p.canonical_json(receipt).decode()
            with self._db() as db:
                old=db.execute('SELECT * FROM provenance_receipts WHERE receipt_id=? OR (job_id=? AND generation=?)',
                               (receipt_id,job['jobId'],job['generation'])).fetchall()
                if any(row['receipt_id']!=receipt_id or row['wrapper']!=encoded for row in old):
                    raise ProvenanceUnavailable('sealed_provenance_cannot_rebind')
                db.execute('''INSERT INTO provenance_receipts VALUES(?,?,?,?,?,?,?,1)
                    ON CONFLICT(receipt_id) DO UPDATE SET fetched_at=excluded.fetched_at,grant_expires=excluded.grant_expires,usable=1''',
                    (receipt_id,job['jobId'],job['generation'],encoded,checked,observed,grant.expires))
            return p.decode_submission(checked)
        except ProvenanceUnavailable:raise
        except HTTPError as error:
            error.close()
            raise ProvenanceUnavailable('provenance_issuer_unavailable') from None
        except Exception:
            raise ProvenanceUnavailable('provenance_issuer_unavailable') from None
        finally:
            if response is not None:
                try:response.close()
                except Exception:pass
            if owns_read:
                with self._inflight_lock:self._inflight.discard(receipt_id)
            self._reads.release()

    def verify(self,receipt):
        """Recognize only an exact fresh authenticated read, never a declaration."""
        try:
            if self.broker.origin!=self.origin:return False
            now=self._observed_time()
            if type(receipt) is not dict:return False
            receipt_id=receipt.get('receiptId');p._slug(receipt_id,'receipt ID')
            with self._db() as db:
                row=db.execute('SELECT * FROM provenance_receipts WHERE receipt_id=?',(receipt_id,)).fetchone()
            return (row is not None and row['usable']==1 and row['receipt']==p.canonical_json(receipt).decode()
                    and 0<=now-row['fetched_at']<=self.freshness and now<row['grant_expires'])
        except Exception:return False
