"""Disabled local original-LiDAR spatial evidence; no stage/result authorization.

Requires the exact already-issued combined TreeDar intent and configured fresh
provenance client. A local proof is not authenticated Viewer custody or evidence
that a declared acquisition was independently observed in the field.
"""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
from dataclasses import asdict
from . import reference_protocol as p
from .reference_provenance import LidarProvenanceClient
from .reference_transfer import ValidatedGrant
from .reference_treedar_stage import TreeDarNativeStage
from .reference_parent_guard import ProductionParentGuard
from .treedar_admission import validate_lidar
from .treedar_engine import _file, _sync_directory

QUALIFIED=False
DDL=('CREATE TABLE spatial_receipts (job_id TEXT PRIMARY KEY, intent_sha TEXT NOT NULL, '
     'payload TEXT NOT NULL, proof_sha TEXT NOT NULL)')

class SpatialHeld(ValueError):pass

class LocalSpatialReceipt:
    def __init__(self,path,*,stage,issuer,enabled=False):
        if (QUALIFIED is not True or enabled is not True or type(stage) is not TreeDarNativeStage
                or type(issuer) is not LidarProvenanceClient or type(stage.guard) is not ProductionParentGuard):
            raise SpatialHeld('local spatial receipt is not qualified')
        if (getattr(stage.journal.verify_provenance,'__self__',None) is not issuer
                or getattr(stage.journal.verify_provenance,'__func__',None) is not LidarProvenanceClient.verify
                or issuer.policy!=stage.policy.admission):
            raise SpatialHeld('exact configured issuer and policy required')
        self.stage,self.issuer,self.owner=stage,issuer,stage.owner
        self._broker=issuer.broker;self._opener=issuer.broker._open;self._issuer_policy=p.digest(asdict(issuer.policy))
        issuer_stat=issuer.path.lstat();self._issuer_identity=(issuer_stat.st_dev,issuer_stat.st_ino)
        self.pid=os.getpid();self.path=Path(path).absolute();self.owner.check(stage.root)
        protected=(stage.path,stage.lock_path,stage.journal.path,stage.journal.lock_path,Path(stage.ledger.path).absolute(),issuer.path)
        if (self.path.parent!=stage.root or self.path.resolve()!=self.path
                or self.path in protected or self.path in {Path(str(item)+suffix) for item in protected for suffix in ('-journal','-wal','-shm','.lock')}):
            raise SpatialHeld('dedicated owned spatial journal required')
        fd=os.open(self.path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
        try:
            opened=os.fstat(fd);named=self.path.lstat()
            if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink!=1 or opened.st_uid!=os.geteuid()
                    or stat.S_IMODE(opened.st_mode)!=0o600 or (opened.st_dev,opened.st_ino)!=(named.st_dev,named.st_ino)):
                raise SpatialHeld('owned spatial journal required')
            self.identity=(opened.st_dev,opened.st_ino)
        finally:os.close(fd)
        with self._db() as db:
            row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='spatial_receipts'").fetchone()
            if row is None:
                if db.execute("SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 1").fetchone():
                    raise SpatialHeld('empty spatial journal required')
                db.execute(DDL)
            elif row[0]!=DDL:raise SpatialHeld('spatial journal schema changed')
        _sync_directory(self.path.parent)

    def _check(self):
        if QUALIFIED is not True or self.pid!=os.getpid():raise SpatialHeld('spatial owner changed')
        self.owner.check(self.stage.root);self.stage._check();info=self.path.lstat()
        issuer_stat=self.issuer.path.lstat()
        if (not stat.S_ISREG(info.st_mode) or (info.st_dev,info.st_ino)!=self.identity
                or not stat.S_ISREG(issuer_stat.st_mode) or (issuer_stat.st_dev,issuer_stat.st_ino)!=self._issuer_identity
                or self.issuer.broker is not self._broker or self._broker._open is not self._opener
                or p.digest(asdict(self.issuer.policy))!=self._issuer_policy
                or getattr(self.stage.journal.verify_provenance,'__self__',None) is not self.issuer
                or getattr(self.stage.journal.verify_provenance,'__func__',None) is not LidarProvenanceClient.verify):
            raise SpatialHeld('spatial binding changed')

    def _db(self):
        from contextlib import contextmanager
        @contextmanager
        def transaction():
            self._check();db=sqlite3.connect(self.path,timeout=5);db.row_factory=sqlite3.Row
            try:
                db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE');yield db
                self._check();db.commit()
            except BaseException:db.rollback();raise
            finally:db.close()
        return transaction()

    def qualify(self,intent,job,grant):
        """Return a freshly revalidated local receipt, without starting any stage."""
        self._check()
        if type(grant) is not ValidatedGrant:raise SpatialHeld('exact transfer grant required')
        with self.stage._operation():
            issued,plan,prior_intent,prior_proof=self.stage._intent(intent)
            parent=issued['parentBinding'];obj=issued['stage']['inputObjects'][0]
            if (type(job) is not dict or any(job.get(k)!=parent[k] for k in ('jobId','generation','environment','manifestSha256','pipelineDigest'))
                    or job.get('nodeId')!=parent['nodeId'] or job.get('status')!='running'):
                raise SpatialHeld('original running parent required')
            self.stage._guard(parent,False);self.stage._local(parent);self.stage.prior.verify(prior_intent,prior_proof)
            receipt=self.issuer.fetch(job,grant)
            if receipt!=issued['provenance'] or receipt['sourceKind']!='lidar' or not self.issuer.verify(receipt):
                raise SpatialHeld('fresh original lidar provenance required')
            self.stage._provenance(issued,plan)
            supplied=self.stage.inputs.resolve(issued)
            if type(supplied) is not dict or set(supplied)!={obj['id']}:raise SpatialHeld('original lidar path required')
            relative=supplied[obj['id']];self.stage.ledger._owned_path(parent['reservationId'],relative)
            source=self.stage.ledger.root/relative
            admission=validate_lidar(source,job_id=parent['jobId'],generation=parent['generation'],environment=parent['environment'],
                object_id=obj['id'],expected_sha256=obj['sha256'],expected_bytes=obj['byteSize'],provenance=receipt,
                verify_provenance=self.issuer.verify,policy=self.stage.policy.admission,allow_synthetic=False,include_crs_definition=True)
            if (admission['sourceKind']!='lidar' or admission['genuineAcquisition'] is not True
                    or type(admission['crs']) is not str or not admission['crs'] or type(admission.get('crsWkt')) is not str
                    or not 0<len(admission['crsWkt'].encode('utf-8'))<=65536
                    or hashlib.sha256(admission['crsWkt'].encode('utf-8')).hexdigest()!=admission['crsWktSha256']):
                raise SpatialHeld('strict local spatial evidence required')
            self.stage._check();self.stage._guard(parent,False);self.stage._local(parent);self.stage.prior.verify(prior_intent,prior_proof)
            if self.issuer.fetch(job,grant)!=receipt or not self.issuer.verify(receipt):raise SpatialHeld('provenance changed during validation')
            self.stage._provenance(issued,plan)
            _file(source,obj['byteSize'],expected_bytes=obj['byteSize'],expected_sha=obj['sha256'])
            record=dict(schemaVersion=1,scope='local_provenance_and_las_structure_not_remote_custody',
                parent={k:parent[k] for k in ('jobId','generation','environment','manifestSha256','pipelineDigest','reservationId')},
                intentSha256=issued['intentSha256'],priorOdmProofSha256=prior_proof['proofSha256'],
                policySha256=p.digest(asdict(self.stage.policy.admission)),provenance=receipt,admission=admission)
            encoded=p.canonical_json(record).decode();proof_sha=hashlib.sha256(encoded.encode()).hexdigest()
            with self._db() as db:
                row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='spatial_receipts'").fetchone()
                if row is None or row[0]!=DDL:raise SpatialHeld('spatial journal schema changed')
                old=db.execute('SELECT intent_sha,payload,proof_sha FROM spatial_receipts WHERE job_id=?',(parent['jobId'],)).fetchone()
                expected=(issued['intentSha256'],encoded,proof_sha)
                if old is None:db.execute('INSERT INTO spatial_receipts VALUES(?,?,?,?)',(parent['jobId'],*expected))
                elif tuple(old)!=expected:raise SpatialHeld('spatial receipt changed')
            self.stage._check();return dict(record=record,proofSha256=proof_sha,transport='held',parentSettled=False)
