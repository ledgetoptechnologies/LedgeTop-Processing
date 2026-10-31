"""Disabled authenticated Viewer archive spool; no HTTP ingress or Viewer ACK.

The caller must supply the authenticated worker principal and claim token from
the protected transport. This module reserves real local bytes but cannot prove
storage hardware durability, source authenticity or downstream Viewer custody.
"""
import hashlib
import json
import os

from . import reference_protocol as p
from .direct_output_slot import _identity,_open_root,open_slot
from .viewer_worker_claim import ViewerWorkerClaims


class SpoolHeld(RuntimeError):pass


class ViewerArchiveSpool:
    def __init__(self,claims,*,root,root_identity,max_archive_bytes,enabled=False):
        if type(claims) is not ViewerWorkerClaims or enabled is not True:raise SpoolHeld('spool_disabled')
        p._integer(max_archive_bytes,'spool limit')
        self.claims=claims;self.root=str(root);self.root_identity=json.loads(p.canonical_json(root_identity));self.max_bytes=max_archive_bytes
        fd=_open_root(self.root,self.root_identity)
        try:os.fsync(fd)
        finally:os.close(fd)
        self.policy=p.canonical_json(dict(root=self.root,rootIdentity=self.root_identity,maxArchiveBytes=self.max_bytes)).decode()
        with self.claims.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');self.claims._schema(db)
            old=db.execute('SELECT policy FROM viewer_worker_spool_config WHERE singleton=1').fetchone()
            if old and old['policy']!=self.policy:raise SpoolHeld('spool_policy_changed')
            if not old:
                if db.execute('SELECT 1 FROM viewer_worker_spool LIMIT 1').fetchone():raise SpoolHeld('spool_policy_missing')
                db.execute('INSERT INTO viewer_worker_spool_config VALUES(1,?)',(self.policy,))

    def _metadata(self,db,job_id,identity,token):
        self.claims._authorize_inventory(db,job_id,identity,token,allow_fence_hold=True)
        policy=db.execute('SELECT policy FROM viewer_worker_spool_config WHERE singleton=1').fetchone()
        row=db.execute('SELECT payload,receipt FROM viewer_worker_result_metadata WHERE job_id=?',(job_id,)).fetchone()
        if not policy or policy['policy']!=self.policy or not row:raise SpoolHeld('metadata_unavailable')
        metadata=p.decode_submission(row['payload'].encode());receipt=p.decode_submission(row['receipt'].encode())
        if (p.canonical_json(metadata).decode()!=row['payload'] or receipt.get('payloadSha256')!=p.digest(metadata)
                or metadata.get('jobId')!=job_id or type(metadata.get('archiveBytes')) is not int
                or not 1<=metadata['archiveBytes']<=self.max_bytes):raise SpoolHeld('metadata_changed')
        p._hash(metadata['archiveSha256'],'archive hash')
        return metadata

    @staticmethod
    def _slot_identity(slot):
        return p.canonical_json(dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))).decode()

    @staticmethod
    def _digest(slot,length):
        slot._check();digest=hashlib.sha256();offset=0
        while offset<length:
            data=os.pread(slot._fd,min(1024*1024,length-offset),offset)
            if not data:raise SpoolHeld('spool_truncated')
            digest.update(data);offset+=len(data)
        slot._check();return digest.hexdigest()

    def lookup(self,job_id,identity,token):
        """Authenticated status; sealed replay is rehashed before returning."""
        p._uuid(job_id,'job ID')
        with self.claims.queue._db() as db:
            db.execute('BEGIN');metadata=self._metadata(db,job_id,identity,token)
            row=db.execute('SELECT * FROM viewer_worker_spool WHERE job_id=?',(job_id,)).fetchone()
            if not row:return dict(state='missing')
            if row['state']!='sealed':return dict(state='held')
            metadata_sha=p.digest(metadata);slot_key=p.digest(dict(kind='viewer-coordinator-spool-v1',jobId=job_id,metadataSha256=metadata_sha))
            if row['metadata_sha']!=metadata_sha or row['slot_key']!=slot_key:raise SpoolHeld('spool_binding_changed')
            recorded=row['slot_identity'];saved=row['receipt']
        slot=open_slot(self.root,self.root_identity,slot_key,metadata['archiveBytes'],max_budget=self.max_bytes,create=False)
        try:
            receipt=dict(schemaVersion=1,kind='viewer-coordinator-archive-receipt',jobId=job_id,metadataSha256=metadata_sha,
                         archiveBytes=metadata['archiveBytes'],archiveSha256=metadata['archiveSha256'],slotKey=slot_key)
            if (self._slot_identity(slot)!=recorded or saved!=p.canonical_json(receipt).decode()
                    or self._digest(slot,metadata['archiveBytes'])!=metadata['archiveSha256']):
                raise SpoolHeld('spool_identity_or_content_changed')
            return dict(state='sealed',receipt=receipt)
        finally:slot.close()

    def receive(self,job_id,identity,token,chunks,*,expected_length=None):
        """Preauthorize before consuming chunks; unfinished attempts always hold."""
        p._uuid(job_id,'job ID')
        with self.claims.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');metadata=self._metadata(db,job_id,identity,token)
            if expected_length is not None and (type(expected_length) is not int or expected_length!=metadata['archiveBytes']):
                raise SpoolHeld('spool_length_mismatch')
            metadata_sha=p.digest(metadata);slot_key=p.digest(dict(kind='viewer-coordinator-spool-v1',jobId=job_id,metadataSha256=metadata_sha))
            row=db.execute('SELECT * FROM viewer_worker_spool WHERE job_id=?',(job_id,)).fetchone()
            if row:
                if row['metadata_sha']!=metadata_sha or row['slot_key']!=slot_key or row['state']!='sealed':
                    raise SpoolHeld('spool_requires_inspection')
                recorded=row['slot_identity'];saved=row['receipt'];create=False
            else:
                db.execute("INSERT INTO viewer_worker_spool VALUES(?,?,?,'creating',NULL,NULL)",(job_id,metadata_sha,slot_key))
                recorded=saved=None;create=True
        slot=open_slot(self.root,self.root_identity,slot_key,metadata['archiveBytes'],max_budget=self.max_bytes,create=create)
        try:
            actual=self._slot_identity(slot)
            receipt=dict(schemaVersion=1,kind='viewer-coordinator-archive-receipt',jobId=job_id,
                         metadataSha256=metadata_sha,archiveBytes=metadata['archiveBytes'],archiveSha256=metadata['archiveSha256'],slotKey=slot_key)
            encoded=p.canonical_json(receipt).decode()
            if not create:
                if recorded!=actual or saved!=encoded or self._digest(slot,metadata['archiveBytes'])!=metadata['archiveSha256']:
                    raise SpoolHeld('spool_identity_or_content_changed')
                return receipt
            with self.claims.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');self._metadata(db,job_id,identity,token)
                if db.execute("UPDATE viewer_worker_spool SET state='receiving',slot_identity=? WHERE job_id=? AND state='creating' AND slot_identity IS NULL",(actual,job_id)).rowcount!=1:
                    raise SpoolHeld('spool_state_changed')
            total=0;digest=hashlib.sha256()
            for data in chunks:
                if type(data) is not bytes or not data or len(data)>1024*1024 or total+len(data)>metadata['archiveBytes']:
                    raise SpoolHeld('spool_stream_invalid')
                slot.write_at(total,data);digest.update(data);total+=len(data)
            if total!=metadata['archiveBytes'] or digest.hexdigest()!=metadata['archiveSha256']:
                raise SpoolHeld('spool_stream_mismatch')
            slot.sync()
            if self._digest(slot,total)!=metadata['archiveSha256']:raise SpoolHeld('spool_content_changed')
            with self.claims.queue._db() as db:
                db.execute('BEGIN IMMEDIATE')
                if p.digest(self._metadata(db,job_id,identity,token))!=metadata_sha:raise SpoolHeld('metadata_changed')
                if db.execute("UPDATE viewer_worker_spool SET state='sealed',receipt=? WHERE job_id=? AND state='receiving' AND slot_identity=?",(encoded,job_id,actual)).rowcount!=1:
                    raise SpoolHeld('spool_state_changed')
            slot._check();return receipt
        finally:slot.close()
