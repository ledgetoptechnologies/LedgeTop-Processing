"""Disabled synthetic coordinator-owned chunk spool; no network ingress.

A separate session row establishes new-vs-legacy authority. Existing receiving
rows without that row are never adopted. No migration or cleanup is provided.
"""
import json
import hashlib
import os
import secrets
import threading
from . import reference_protocol as p
from .direct_output_slot import open_slot
from .viewer_archive_spool import ViewerArchiveSpool,SpoolHeld
from .viewer_chunk_journal import SyntheticChunkJournal,open_synthetic_chunk_slot
from .direct_slot_capture import _exists

class SyntheticChunkSpool:
    def __init__(self,spool,*,synthetic_fixture=False):
        if synthetic_fixture is not True or type(spool) is not ViewerArchiveSpool:raise SpoolHeld('synthetic_fixture_required')
        self.spool=spool;self._pid=os.getpid();self._active={};self._permit_lock=threading.Lock()

    def _binding(self,metadata):
        return dict(jobId=metadata['jobId'],taskId=metadata['viewerTaskId'],generation=metadata['viewerGeneration'],
                    environment=metadata['environment'],archiveBytes=metadata['archiveBytes'],archiveSha256=metadata['archiveSha256'])

    def _snapshot(self,db,job,identity,token):
        metadata=self.spool._metadata(db,job,identity,token);sha=p.digest(metadata)
        key=p.digest(dict(kind='viewer-coordinator-spool-v1',jobId=job,metadataSha256=sha))
        session=db.execute('SELECT * FROM viewer_worker_chunk_sessions WHERE job_id=?',(job,)).fetchone()
        spool=db.execute('SELECT * FROM viewer_worker_spool WHERE job_id=?',(job,)).fetchone()
        if session and (not spool or session['metadata_sha']!=sha or session['slot_key']!=key
                or spool['metadata_sha']!=sha or spool['slot_key']!=key):raise SpoolHeld('chunk_binding_changed')
        return metadata,sha,key,session,spool

    @staticmethod
    def _admit_operation(db,job):
        """Linearize new synthetic work with the coordinator hold transaction."""
        status=db.execute('SELECT status FROM jobs WHERE id=?',(job,)).fetchone()
        fence=db.execute('SELECT stop_requested FROM viewer_worker_fences WHERE job_id=?',(job,)).fetchone()
        if not status or status['status']!='running' or (fence and fence['stop_requested']):
            raise SpoolHeld('chunk_admission_held')
        if db.execute("SELECT 1 FROM viewer_worker_chunk_permits WHERE job_id=? AND state='active'",(job,)).fetchone():
            raise SpoolHeld('chunk_permit_requires_inspection')

    def begin(self,job,identity,token):
        with self.spool._admit():
            with self.spool.claims.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');metadata,sha,key,session,row=self._snapshot(db,job,identity,token)
                self._admit_operation(db,job)
                if session is not None:
                    if session['state']!='ready' or not row or row['state']!='receiving' or not session['slot_identity'] or session['slot_identity']!=row['slot_identity']:
                        raise SpoolHeld('chunk_session_requires_inspection')
                    _,_,_,existing_slot,_=self._open(job,identity,token)
                    existing_slot.close()
                    return dict(state='ready')
                if row is not None:raise SpoolHeld('legacy_spool_requires_inspection')
                db.execute("INSERT INTO viewer_worker_spool VALUES(?,?,?,'creating',NULL,NULL)",(job,sha,key))
                db.execute("INSERT INTO viewer_worker_chunk_sessions VALUES(?,?,?,NULL,'creating')",(job,sha,key))
            slot=open_slot(self.spool.root,self.spool.root_identity,key,metadata['archiveBytes'],max_budget=self.spool.max_bytes,create=True)
            try:
                journal=SyntheticChunkJournal(slot,self._binding(metadata),legacy_state='new',synthetic_fixture=True)
                actual=self.spool._slot_identity(slot)
                with self.spool.claims.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE');current,csha,ckey,session,row=self._snapshot(db,job,identity,token)
                    if csha!=sha or ckey!=key or not session or session['state']!='creating' or not row or row['state']!='creating':raise SpoolHeld('chunk_state_changed')
                    if db.execute("UPDATE viewer_worker_chunk_sessions SET state='ready',slot_identity=? WHERE job_id=? AND state='creating'",(actual,job)).rowcount!=1:raise SpoolHeld('chunk_state_changed')
                    if db.execute("UPDATE viewer_worker_spool SET state='receiving',slot_identity=? WHERE job_id=? AND state='creating'",(actual,job)).rowcount!=1:raise SpoolHeld('chunk_state_changed')
                return dict(state='ready')
            finally:slot.close()

    def _open(self,job,identity,token):
        with self.spool.claims.queue._db() as db:
            db.execute('BEGIN');metadata,sha,key,session,row=self._snapshot(db,job,identity,token)
            if not session or not row or session['state'] not in ('ready','sealed') or row['state'] not in ('receiving','sealed') or session['slot_identity']!=row['slot_identity'] or not session['slot_identity']:
                raise SpoolHeld('chunk_session_requires_inspection')
            recorded=session['slot_identity']
        # A missing header on an existing session is always ambiguous. The
        # helper must not manufacture one after a crash or deletion.
        from .direct_output_slot import _open_slot
        slot=_open_slot(self.spool.root,self.spool.root_identity,key,metadata['archiveBytes'],max_budget=self.spool.max_bytes,allow_chunk_journal=True)
        try:
            if self.spool._slot_identity(slot)!=recorded or not _exists(slot._root,slot._name+'.chunks-v1.header'):
                raise SpoolHeld('chunk_slot_changed')
            journal=SyntheticChunkJournal(slot,self._binding(metadata),legacy_state='new',synthetic_fixture=True)
            return metadata,sha,key,slot,journal
        except BaseException:slot.close();raise

    def admit_chunk(self,job,identity,token,operation_id,index,offset,length,sha256):
        """Persist a synthetic operation permit before reading any body.

        Active permits never auto-resume after interruption. This is a direct
        call only, not a network-facing authentication boundary.
        """
        from .viewer_chunk_journal import MAX_CHUNK,MAX_CHUNKS
        p._slug(operation_id,'chunk operation');p._hash(sha256,'chunk sha256')
        if type(index) is not int or not 0<=index<MAX_CHUNKS or type(offset) is not int or offset<0 or type(length) is not int or not 1<=length<=MAX_CHUNK:
            raise SpoolHeld('chunk_spec_invalid')
        spec=p.canonical_json(dict(index=index,offset=offset,length=length,sha256=sha256)).decode()
        with self.spool.claims.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');metadata,_,_,session,row=self._snapshot(db,job,identity,token)
            self._admit_operation(db,job)
            if not session or not row or session['state']!='ready' or row['state']!='receiving' or offset+length>metadata['archiveBytes']:
                raise SpoolHeld('chunk_state_changed')
            prior=db.execute('SELECT specification,state FROM viewer_worker_chunk_permits WHERE job_id=? AND operation_id=?',(job,operation_id)).fetchone()
            if prior:
                if (prior['specification'],prior['state'])!=(spec,'finished'):raise SpoolHeld('chunk_permit_requires_inspection')
                return dict(state='finished')
            if db.execute("SELECT 1 FROM viewer_worker_chunk_permits WHERE job_id=? AND state='active'",(job,)).fetchone():
                raise SpoolHeld('chunk_permit_requires_inspection')
            db.execute("INSERT INTO viewer_worker_chunk_permits VALUES(?,?,?,'active')",(job,operation_id,spec))
        nonce=secrets.token_hex(16)
        with self._permit_lock:self._active[(job,operation_id)]=nonce
        return dict(state='active',permit=nonce)

    def write_admitted(self,job,identity,token,operation_id,permit_nonce,index,offset,data):
        """Finish a previously admitted operation even if a hold follows."""
        from .viewer_chunk_journal import MAX_CHUNK
        if type(data) is not bytes or not 1<=len(data)<=MAX_CHUNK or os.getpid()!=self._pid:raise SpoolHeld('chunk_spec_invalid')
        with self._permit_lock:
            expected_nonce=self._active.pop((job,operation_id),None)
        if not expected_nonce or not isinstance(permit_nonce,str) or not secrets.compare_digest(expected_nonce,permit_nonce):
            raise SpoolHeld('chunk_permit_requires_inspection')
        spec=p.canonical_json(dict(index=index,offset=offset,length=len(data),sha256=hashlib.sha256(data).hexdigest())).decode()
        with self.spool._admit():
            metadata,sha,key,slot,journal=self._open(job,identity,token)
            try:
                with self.spool.claims.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE');_,current_sha,current_key,session,row=self._snapshot(db,job,identity,token)
                    permit=db.execute('SELECT specification,state FROM viewer_worker_chunk_permits WHERE job_id=? AND operation_id=?',(job,operation_id)).fetchone()
                    if (current_sha!=sha or current_key!=key or session['state']!='ready' or row['state']!='receiving'
                            or not permit or permit['specification']!=spec or permit['state'] not in ('active','finished')):
                        raise SpoolHeld('chunk_permit_changed')
                receipt=journal.write(index,offset,data)
                with self.spool.claims.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE');self._snapshot(db,job,identity,token)
                    permit=db.execute('SELECT specification,state FROM viewer_worker_chunk_permits WHERE job_id=? AND operation_id=?',(job,operation_id)).fetchone()
                    if not permit or permit['specification']!=spec or permit['state'] not in ('active','finished'):raise SpoolHeld('chunk_permit_changed')
                    if permit['state']=='active':db.execute("UPDATE viewer_worker_chunk_permits SET state='finished' WHERE job_id=? AND operation_id=?",(job,operation_id))
                return receipt
            finally:slot.close()

    def write(self,job,identity,token,index,offset,data):
        with self.spool._admit():
            metadata,sha,key,slot,journal=self._open(job,identity,token)
            try:
                with self.spool.claims.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE');_,current_sha,current_key,session,row=self._snapshot(db,job,identity,token)
                    self._admit_operation(db,job)
                    if current_sha!=sha or current_key!=key or session['state']!='ready' or row['state']!='receiving':raise SpoolHeld('chunk_state_changed')
                return journal.write(index,offset,data)
            finally:slot.close()

    def finish(self,job,identity,token,count):
        with self.spool._admit():
            metadata,sha,key,slot,journal=self._open(job,identity,token)
            try:
                with self.spool.claims.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE');self._snapshot(db,job,identity,token);self._admit_operation(db,job)
                journal.finish(count)
                receipt=dict(schemaVersion=1,kind='synthetic-chunk-seal',jobId=job,metadataSha256=sha,
                             archiveBytes=metadata['archiveBytes'],archiveSha256=metadata['archiveSha256'],slotKey=key)
                encoded=p.canonical_json(receipt).decode();actual=self.spool._slot_identity(slot)
                with self.spool.claims.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE');_,current_sha,current_key,session,row=self._snapshot(db,job,identity,token)
                    if current_sha!=sha or current_key!=key or session['slot_identity']!=actual or row['slot_identity']!=actual:raise SpoolHeld('chunk_state_changed')
                    if session['state']=='sealed' and row['state']=='receiving':return receipt
                    if session['state']!='ready' or row['state']!='receiving':raise SpoolHeld('chunk_state_changed')
                    db.execute("UPDATE viewer_worker_chunk_sessions SET state='sealed' WHERE job_id=?",(job,))
                return receipt
            finally:slot.close()
