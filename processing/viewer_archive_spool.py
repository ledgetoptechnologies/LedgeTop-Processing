"""Disabled authenticated Viewer archive spool; no HTTP ingress or Viewer ACK.

The caller must supply the authenticated worker principal and claim token from
the protected transport. This module reserves real local bytes but cannot prove
storage hardware durability, source authenticity or downstream Viewer custody.
"""
import hashlib
import json
import os
import threading
from contextlib import contextmanager

from . import reference_protocol as p
from .direct_output_slot import _identity,_open_root,open_slot
from .viewer_worker_claim import ViewerWorkerClaims


class SpoolHeld(RuntimeError):pass


class _ViewerReader:
    def __init__(self,spool,slot,task,receipt):
        self.spool,self.slot,self.task,self.receipt=spool,slot,task,receipt
        self.ledgetop_archive_sha256=receipt['archiveSha256'];self.length=receipt['archiveBytes']
        self.position=0;self.digest=hashlib.sha256();self.closed=False

    def read(self,size):
        try:return self._read(size)
        except Exception:raise OSError('viewer archive requires inspection') from None

    def _read(self,size):
        if self.closed or type(size) is not int or not 1<=size<=1024*1024:raise SpoolHeld('viewer_reader_invalid')
        self.spool._viewer_snapshot(self.task);self.slot._check()
        if self.position==self.length:return b''
        data=os.pread(self.slot._fd,min(size,self.length-self.position),self.position)
        if not data:raise SpoolHeld('viewer_archive_truncated')
        self.position+=len(data);self.digest.update(data);self.slot._check();self.spool._viewer_snapshot(self.task)
        if self.position==self.length:
            if self.digest.hexdigest()!=self.ledgetop_archive_sha256 or self.spool._digest(self.slot,self.length)!=self.ledgetop_archive_sha256:
                raise SpoolHeld('viewer_archive_changed')
        return data

    def close(self):
        if not self.closed:
            self.closed=True
            try:self.slot.close()
            finally:self.spool._work.release()


class ViewerArchiveSpool:
    def __init__(self,claims,*,root,root_identity,max_archive_bytes,enabled=False,synthetic_v2_viewer=False):
        if type(claims) is not ViewerWorkerClaims or enabled is not True or type(synthetic_v2_viewer) is not bool:raise SpoolHeld('spool_disabled')
        self.synthetic_v2_viewer=synthetic_v2_viewer
        p._integer(max_archive_bytes,'spool limit')
        self.claims=claims;self.root=str(root);self.root_identity=json.loads(p.canonical_json(root_identity));self.max_bytes=max_archive_bytes
        self._work=threading.BoundedSemaphore(2)
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

    def _slot_key_for_metadata(self,db,job,metadata,identity,token):
        standard=p.digest(dict(kind='viewer-coordinator-spool-v1',jobId=job,metadataSha256=p.digest(metadata)))
        has_table=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_chunk_v2_slots'").fetchone()
        row=db.execute('SELECT * FROM viewer_chunk_v2_slots WHERE job_id=?',(job,)).fetchone() if has_table else None
        has_geometry=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_chunk_v2_geometry'").fetchone()
        geometry_row=db.execute('SELECT 1 FROM viewer_chunk_v2_geometry WHERE job_id=?',(job,)).fetchone() if has_geometry else None
        if not row:
            if geometry_row:raise SpoolHeld('v2_slot_missing')
            return standard
        if self.synthetic_v2_viewer is not True:raise SpoolHeld('v2_viewer_disabled')
        from .viewer_chunk_v2_spool import SyntheticV2Spool
        v2=SyntheticV2Spool(self,synthetic_fixture=True);v2._schema(db)
        binding,encoded,key=v2._binding(db,job,identity,token)
        phase=db.execute('SELECT phase FROM viewer_chunk_v2_geometry WHERE job_id=?',(job,)).fetchone()
        status=db.execute('SELECT status FROM jobs WHERE id=?',(job,)).fetchone()
        fence=db.execute('SELECT stop_requested FROM viewer_worker_fences WHERE job_id=?',(job,)).fetchone()
        if (not status or status['status']!='running' or (fence and fence['stop_requested'])
                or db.execute("SELECT 1 FROM viewer_worker_chunk_permits WHERE job_id=? AND state='active'",(job,)).fetchone()
                or db.execute("SELECT 1 FROM viewer_worker_permits WHERE job_id=? AND state='active'",(job,)).fetchone()
                or not phase or phase['phase']!='sealed' or row['state']!='sealed' or row['binding']!=encoded
                or row['slot_key']!=key or not row['slot_identity']
                or db.execute('SELECT 1 FROM viewer_worker_chunk_sessions WHERE job_id=?',(job,)).fetchone()):
            raise SpoolHeld('v2_promotion_changed')
        return key

    def _viewer_snapshot(self,task,db=None):
        """Revalidate against the caller's transaction when supplied."""
        if db is None:
            with self.claims.queue._db() as connection:
                connection.execute('BEGIN')
                return self._viewer_snapshot(task,connection)
        self.claims._schema(db)
        current=db.execute('SELECT * FROM viewer_tasks WHERE uuid=? AND removed=0',(task['uuid'],)).fetchone()
        if not current or any(current[key]!=task[key] for key in ('uuid','generation','environment','job_id','node_id','engine_uuid','claim_token','stage','phase')):
            raise SpoolHeld('viewer_task_changed')
        claim=db.execute('SELECT * FROM viewer_worker_claims WHERE job_id=?',(task['job_id'],)).fetchone()
        if not claim:raise SpoolHeld('viewer_claim_unavailable')
        identity=dict(node_id=claim['node_id'],worker_id=claim['worker_id'],worker_instance_id=claim['instance_id'])
        metadata=self._metadata(db,task['job_id'],identity,claim['token'])
        if (metadata['viewerTaskId']!=task['uuid'] or metadata['viewerGeneration']!=task['generation']
                or metadata['environment']!=task['environment'] or metadata['engineTaskId']!=task['engine_uuid']):
            raise SpoolHeld('viewer_metadata_changed')
        metadata_sha=p.digest(metadata);slot_key=self._slot_key_for_metadata(db,task['job_id'],metadata,identity,claim['token'])
        row=db.execute('SELECT * FROM viewer_worker_spool WHERE job_id=?',(task['job_id'],)).fetchone()
        receipt=dict(schemaVersion=1,kind='viewer-coordinator-archive-receipt',jobId=task['job_id'],metadataSha256=metadata_sha,
                     archiveBytes=metadata['archiveBytes'],archiveSha256=metadata['archiveSha256'],slotKey=slot_key)
        if (not row or row['state']!='sealed' or row['slot_key']!=slot_key or row['metadata_sha']!=metadata_sha
                or row['receipt']!=p.canonical_json(receipt).decode() or not isinstance(row['slot_identity'],str)):
            raise SpoolHeld('viewer_spool_unavailable')
        return receipt,row['slot_identity']

    def _open_receipt_slot(self,job,metadata,key,recorded):
        """Open only a fully verified promoted chunk slot or legacy spool."""
        with self.claims.queue._db() as db:
            db.execute('BEGIN');session=db.execute('SELECT * FROM viewer_worker_chunk_sessions WHERE job_id=?',(job,)).fetchone()
            has_v2=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_chunk_v2_slots'").fetchone()
            v2row=db.execute('SELECT * FROM viewer_chunk_v2_slots WHERE job_id=?',(job,)).fetchone() if has_v2 else None
            if v2row:
                if session or self.synthetic_v2_viewer is not True:raise SpoolHeld('v2_viewer_disabled')
                claim=db.execute('SELECT * FROM viewer_worker_claims WHERE job_id=?',(job,)).fetchone()
                if not claim:raise SpoolHeld('viewer_claim_unavailable')
                identity=dict(node_id=claim['node_id'],worker_id=claim['worker_id'],worker_instance_id=claim['instance_id'])
                from .viewer_chunk_v2_spool import SyntheticV2Spool
                v2=SyntheticV2Spool(self,synthetic_fixture=True);v2._schema(db)
                binding,encoded,v2key=v2._binding(db,job,identity,claim['token'])
                phase=db.execute('SELECT phase FROM viewer_chunk_v2_geometry WHERE job_id=?',(job,)).fetchone()
                if (not phase or phase['phase']!='sealed' or v2row['state']!='sealed' or v2row['binding']!=encoded
                    or v2key!=key or v2row['slot_identity']!=recorded):raise SpoolHeld('v2_promotion_changed')
        if v2row:
            from .direct_output_slot import _open_slot
            from .direct_slot_capture import _exists
            from .viewer_chunk_v2_journal import SyntheticV2ChunkJournal
            slot=_open_slot(self.root,self.root_identity,key,metadata['archiveBytes'],max_budget=self.max_bytes,allow_chunk_journal=True)
            try:
                if self._slot_identity(slot)!=recorded or not _exists(slot._root,slot._name+'.chunks-v2.header'):raise SpoolHeld('v2_slot_changed')
                journal=SyntheticV2ChunkJournal(slot,binding,legacy_state='new',synthetic_fixture=True)
                journal.verify_sealed(binding['chunkCount']);return slot
            except BaseException:slot.close();raise
        if not session:
            return open_slot(self.root,self.root_identity,key,metadata['archiveBytes'],max_budget=self.max_bytes,create=False)
        if session['state']!='sealed' or session['slot_key']!=key or session['slot_identity']!=recorded or session['metadata_sha']!=p.digest(metadata):
            raise SpoolHeld('chunk_promotion_changed')
        from .direct_output_slot import _open_slot
        from .direct_slot_capture import _exists,_read_record,_check_record
        from .viewer_chunk_journal import SyntheticChunkJournal
        slot=_open_slot(self.root,self.root_identity,key,metadata['archiveBytes'],max_budget=self.max_bytes,allow_chunk_journal=True)
        try:
            if self._slot_identity(slot)!=recorded or not _exists(slot._root,slot._name+'.chunks-v1.header'):raise SpoolHeld('chunk_slot_changed')
            binding=dict(jobId=job,taskId=metadata['viewerTaskId'],generation=metadata['viewerGeneration'],environment=metadata['environment'],archiveBytes=metadata['archiveBytes'],archiveSha256=metadata['archiveSha256'])
            journal=SyntheticChunkJournal(slot,binding,legacy_state='new',synthetic_fixture=True)
            seal,fd=_read_record(slot,journal.seal)
            try:_check_record(slot,fd,journal.seal,seal)
            finally:os.close(fd)
            journal.finish(seal.get('count'))
            return slot
        except BaseException:slot.close();raise

    def open_for_viewer(self,task):
        """Return a locked verified stream, never Viewer custody or completion."""
        if not self._work.acquire(blocking=False):raise SpoolHeld('spool_capacity_unavailable')
        slot=None
        try:
            receipt,identity=self._viewer_snapshot(task)
            with self.claims.queue._db() as db:
                claim=db.execute('SELECT * FROM viewer_worker_claims WHERE job_id=?',(task['job_id'],)).fetchone()
                if not claim:raise SpoolHeld('viewer_claim_unavailable')
                worker=dict(node_id=claim['node_id'],worker_id=claim['worker_id'],worker_instance_id=claim['instance_id'])
                metadata=self._metadata(db,task['job_id'],worker,claim['token'])
            slot=self._open_receipt_slot(task['job_id'],metadata,receipt['slotKey'],identity)
            if self._slot_identity(slot)!=identity or self._digest(slot,receipt['archiveBytes'])!=receipt['archiveSha256']:
                raise SpoolHeld('viewer_archive_changed')
            self._viewer_snapshot(task)
            return _ViewerReader(self,slot,dict(task),receipt),receipt['archiveBytes']
        except BaseException:
            try:
                if slot is not None:
                    try:slot.close()
                    except BaseException:pass  # Preserve the original failure.
            finally:self._work.release()
            raise

    def revalidate_staged_ack_locked(self,db,task,reader):
        """Revalidate a held reader under the caller's BEGIN IMMEDIATE barrier.

        This supplies a building block only; it neither commits an assertion nor
        proves Viewer custody. The caller must hold the reader until commit.
        """
        if (not db.in_transaction or not isinstance(reader,_ViewerReader)
                or reader.spool is not self or reader.closed or reader.task != task):
            raise SpoolHeld('viewer_reader_invalid')
        job=db.execute('SELECT status FROM jobs WHERE id=?',(task['job_id'],)).fetchone()
        fence=db.execute('SELECT stop_requested FROM viewer_worker_fences WHERE job_id=?',(task['job_id'],)).fetchone()
        if not job or job['status']!='running' or (fence and fence['stop_requested']):
            raise SpoolHeld('viewer_job_held')
        if not self._completion_evidence(db,task,reader):
            raise SpoolHeld('viewer_completion_unavailable')
        receipt,identity=self._viewer_snapshot(task,db)
        reader.slot._check()
        if self._slot_identity(reader.slot)!=identity or receipt!=reader.receipt:
            raise SpoolHeld('viewer_archive_changed')
        return receipt

    def completion_ready(self,task):
        """Point-in-time synthetic projection; does not finish or release a job."""
        # Hash outside the coordinator write barrier while retaining the
        # cooperative slot lock. A hold during that read wins below.
        with self.claims.queue._db() as db:
            job=db.execute('SELECT status FROM jobs WHERE id=?',(task['job_id'],)).fetchone()
            fence=db.execute('SELECT stop_requested FROM viewer_worker_fences WHERE job_id=?',(task['job_id'],)).fetchone()
            if (not job or job['status']!='running' or (fence and fence['stop_requested'])
                    or not db.execute('SELECT 1 FROM viewer_worker_completions WHERE job_id=?',(task['job_id'],)).fetchone()):return False
        reader,_=self.open_for_viewer(task)
        try:
            return self._completion_ready_locked(task,reader)
        finally:reader.close()

    def _completion_ready_locked(self,task,reader):
        with self.claims.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            return self._completion_evidence(db,task,reader)

    def _completion_evidence(self,db,task,reader,*,allow_restart_hold=False):
        """Shared evidence gate; caller holds verified reader and write barrier."""
        self.claims._schema(db)
        job=db.execute('SELECT status,held_reason FROM jobs WHERE id=?',(task['job_id'],)).fetchone()
        fence=db.execute('SELECT stop_requested FROM viewer_worker_fences WHERE job_id=?',(task['job_id'],)).fetchone()
        active=db.execute("SELECT 1 FROM viewer_worker_permits WHERE job_id=? AND state='active'",(task['job_id'],)).fetchone()
        row=db.execute('SELECT * FROM viewer_worker_completions WHERE job_id=?',(task['job_id'],)).fetchone()
        grant=db.execute('SELECT grant_value FROM viewer_worker_grants WHERE job_id=?',(task['job_id'],)).fetchone()
        enrollment=db.execute('SELECT attestation FROM viewer_worker_enrollments WHERE job_id=?',(task['job_id'],)).fetchone()
        commit=db.execute("SELECT state,operation FROM viewer_worker_permits WHERE job_id=? AND operation_id='commit'",(task['job_id'],)).fetchone()
        if (not job or not (job['status']=='running' or (allow_restart_hold and job['status']=='held' and job['held_reason']=='coordinator_restart')) or (fence and fence['stop_requested']) or active or not row
                or not grant or not enrollment or not commit or (commit['state'],commit['operation'])!=('finished','commit')):return False
        observation=p.decode_submission(row['observation'].encode());spool_receipt=p.decode_submission(row['spool_receipt'].encode())
        receipt=p.decode_submission(row['receipt'].encode())
        grant_value=p.decode_submission(grant['grant_value'].encode());enrollment_value=p.decode_submission(enrollment['attestation'].encode())
        from .direct_outbox_contract import validate_binding
        expected_observation=dict(schemaVersion=1,kind='viewer-engine-completion-observation',
            bindingSha256=validate_binding(enrollment_value['binding']),grantSha256=p.digest(grant_value),
            jobId=task['job_id'],viewerTaskId=task['uuid'],viewerGeneration=task['generation'],
            engineTaskId=task['engine_uuid'],environment=task['environment'],engineStatus=40)
        expected=dict(schemaVersion=1,kind='viewer-completion-attestation-receipt',jobId=task['job_id'],
                      observationSha256=p.digest(observation),spoolReceiptSha256=p.digest(spool_receipt))
        if (p.canonical_json(observation).decode()!=row['observation'] or p.canonical_json(spool_receipt).decode()!=row['spool_receipt']
                or p.canonical_json(receipt)!=p.canonical_json(expected)
                or p.canonical_json(observation)!=p.canonical_json(expected_observation)):
            return False
        actual,identity=self._viewer_snapshot(task,db)
        if p.canonical_json(actual)!=p.canonical_json(spool_receipt):return False
        reader.slot._check()
        if self._slot_identity(reader.slot)!=identity or reader.receipt!=actual:return False
        return True

    @contextmanager
    def _admit(self):
        if not self._work.acquire(blocking=False):raise SpoolHeld('spool_capacity_unavailable')
        try:yield
        finally:self._work.release()

    def lookup(self,job_id,identity,token):
        with self._admit():return self._lookup(job_id,identity,token)

    def _lookup(self,job_id,identity,token):
        """Authenticated status; sealed replay is rehashed before returning."""
        p._uuid(job_id,'job ID')
        with self.claims.queue._db() as db:
            db.execute('BEGIN');metadata=self._metadata(db,job_id,identity,token)
            row=db.execute('SELECT * FROM viewer_worker_spool WHERE job_id=?',(job_id,)).fetchone()
            if not row:return dict(state='missing')
            if row['state']!='sealed':return dict(state='held')
            metadata_sha=p.digest(metadata);slot_key=self._slot_key_for_metadata(db,job_id,metadata,identity,token)
            if row['metadata_sha']!=metadata_sha or row['slot_key']!=slot_key:raise SpoolHeld('spool_binding_changed')
            recorded=row['slot_identity'];saved=row['receipt']
        slot=self._open_receipt_slot(job_id,metadata,slot_key,recorded)
        try:
            receipt=dict(schemaVersion=1,kind='viewer-coordinator-archive-receipt',jobId=job_id,metadataSha256=metadata_sha,
                         archiveBytes=metadata['archiveBytes'],archiveSha256=metadata['archiveSha256'],slotKey=slot_key)
            if (self._slot_identity(slot)!=recorded or saved!=p.canonical_json(receipt).decode()
                    or self._digest(slot,metadata['archiveBytes'])!=metadata['archiveSha256']):
                raise SpoolHeld('spool_identity_or_content_changed')
            return dict(state='sealed',receipt=receipt)
        finally:slot.close()

    def receive(self,job_id,identity,token,chunks,*,expected_length=None):
        with self._admit():return self._receive(job_id,identity,token,chunks,expected_length=expected_length)

    def _receive(self,job_id,identity,token,chunks,*,expected_length=None):
        """Preauthorize before consuming chunks; unfinished attempts always hold."""
        p._uuid(job_id,'job ID')
        with self.claims.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');metadata=self._metadata(db,job_id,identity,token)
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_chunk_v2_geometry'").fetchone():
                if db.execute('SELECT 1 FROM viewer_chunk_v2_geometry WHERE job_id=?',(job_id,)).fetchone():
                    raise SpoolHeld('v2_session_requires_inspection')
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_chunk_v2_slots'").fetchone():
                if db.execute('SELECT 1 FROM viewer_chunk_v2_slots WHERE job_id=?',(job_id,)).fetchone():
                    raise SpoolHeld('v2_session_requires_inspection')
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
