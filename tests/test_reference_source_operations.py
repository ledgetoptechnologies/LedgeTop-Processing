"""Synthetic Linux source writer/auditor custody; no deletion or NAS access."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from processing.maintenance import Conflict
from processing.reference_api import ReferenceAPI
from processing.reference_source_readers import SourceReaderError
from tests.test_reference_source_readers import source_fixture


def completion(store, job, spool):
    with store.queue._db() as db:
        row = db.execute('SELECT * FROM reference_source_intakes WHERE job_id=?', (job,)).fetchone()
    return dict(generation=1, source_manifest_sha256=row['source_manifest_sha'],
        derived_manifest_sha256=row['derived_manifest_sha'],
        pipeline_manifest_sha256=row['pipeline_manifest_sha'], source_spool=spool)


def uncertain_operation(directory):
    with source_fixture(directory) as (queue, store, spool, job, _, _):
        from processing import reference_protocol as protocol
        api=ReferenceAPI(store,bearer_tokens={'synthetic-staging-token':'staging'},
            capability_fingerprint='a'*64,options_fingerprint='a'*64,
            source_spool=spool,source_attestation_keys={},authority_id=queue.authority_id)
        values=completion(store,job['jobId'],spool)
        body=protocol.canonical_json(dict(generation=1,
            sourceManifestSha256=values['source_manifest_sha256'],
            derivedManifestSha256=values['derived_manifest_sha256'],
            pipelineManifestSha256=values['pipeline_manifest_sha256']))
        calls = []
        def close(fd):
            calls.append(fd)
            raise OSError('synthetic ambiguous close')
        # Inject only around a real source hash's owned descriptor close.
        with patch('processing.source_spool.os.close', side_effect=close):
            response=api.handle('POST',f"/ltds/v2/jobs/{job['jobId']}/source/complete",
                {'Authorization':'Bearer synthetic-staging-token'},body,protected_transport=True)
        refused = False
        try:
            queue.close()
        except SourceReaderError:
            refused = True
        print(json.dumps(dict(refused=refused, attempts=len(calls),
            active=store.source_readers().active_count(job['jobId'], 1),jobId=job['jobId'],
            status=response.status)), flush=True)
        # The test-only process exits, not retries an ambiguously closed FD.
        os._exit(0)


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux process lease required')
class SourceOperationTests(unittest.TestCase):
    def test_source_errors_reach_retryable_capacity_mount_and_conflict_responses(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (queue, store, spool, job, _, image):
            from processing.source_spool import SourceSpoolError
            from processing import reference_protocol as protocol
            import hashlib
            with queue._db() as db:
                db.execute("UPDATE reference_source_intakes SET source_state='receiving',derived_state='receiving'")
            api=ReferenceAPI(store,bearer_tokens={'synthetic-staging-token':'staging'},
                capability_fingerprint='a'*64,options_fingerprint='a'*64,
                source_spool=spool,source_attestation_keys={},authority_id=queue.authority_id)
            headers={'Authorization':'Bearer synthetic-staging-token','Content-SHA256':hashlib.sha256(b'abc').hexdigest()}
            values=completion(store,job['jobId'],spool)
            body=protocol.canonical_json(dict(generation=1,
                sourceManifestSha256=values['source_manifest_sha256'],
                derivedManifestSha256=values['derived_manifest_sha256'],
                pipelineManifestSha256=values['pipeline_manifest_sha256']))
            for code,status in (('spool_capacity_unavailable',507),('spool_mount_unavailable',503),
                    ('spool_mount_changed',503),('source_object_conflict',409),
                    ('source_object_receipt_mismatch',409),('source_object_too_large',413)):
                with self.subTest(code=code):
                    with patch.object(spool,'receive_object',side_effect=SourceSpoolError(code)):
                        response=api.stream_source_object(f"/ltds/v2/jobs/{job['jobId']}/source/{image}?generation=1",headers,io.BytesIO(b'abc'),3,protected_transport=True)
                    self.assertEqual(response.status,status)
                    with patch.object(spool,'verify_object',side_effect=SourceSpoolError(code)):
                        response=api.handle('POST',f"/ltds/v2/jobs/{job['jobId']}/source/complete",headers,body,protected_transport=True)
                    self.assertEqual(response.status,status)
                    self.assertEqual(store.source_readers().active_count(job['jobId'],1),0)

    def test_fence_blocks_complete_and_audit_before_any_filesystem_access(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, _, _):
            readers = store.source_readers()
            readers.fence(job['jobId'], 1, 'synthetic-release')
            with patch.object(spool, 'verify_object') as verify, patch.object(spool, 'audit_job') as audit:
                with self.assertRaisesRegex(Conflict, 'source_release_fenced'):
                    store.complete_source_intake(job['jobId'], **completion(store, job['jobId'], spool))
                self.assertEqual(store.reconcile_source_spool(spool)['held'], 1)
                verify.assert_not_called(); audit.assert_not_called()
            with store.queue._db() as db:
                self.assertEqual(db.execute('SELECT source_state FROM reference_source_intakes').fetchone()[0], 'accepted')

    def test_completion_fence_during_hash_keeps_pin_and_refuses_ack(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (queue, store, spool, job, _, _):
            readers = store.source_readers()
            original = spool.verify_object
            entered, resume = threading.Event(), threading.Event()
            results = []
            def verify(**kwargs):
                entered.set()
                if not resume.wait(5): raise RuntimeError('test barrier timed out')
                return original(**kwargs)
            def run():
                try: store.complete_source_intake(job['jobId'], **completion(store, job['jobId'], spool))
                except Exception as error: results.append(error)
            with patch.object(spool, 'verify_object', side_effect=verify):
                thread = threading.Thread(target=run)
                thread.start()
                try:
                    self.assertTrue(entered.wait(5))
                    self.assertGreater(readers.active_count(job['jobId'], 1), 0)
                    with self.assertRaises(SourceReaderError): queue.close()
                    readers.fence(job['jobId'], 1, 'synthetic-release')
                finally:
                    resume.set(); thread.join(5)
                self.assertFalse(thread.is_alive())
            self.assertEqual(len(results), 1)
            self.assertIsInstance(results[0], Conflict)
            self.assertEqual(readers.active_count(job['jobId'], 1), 0)

    def test_upload_fence_during_stream_retains_pin_then_refuses_receipt(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (queue, store, spool, job, _, image):
            readers = store.source_readers()
            with queue._db() as db:
                db.execute("UPDATE reference_source_intakes SET source_state='receiving',derived_state='receiving',reserved_bytes=3 WHERE job_id=?", (job['jobId'],))
                db.execute('DELETE FROM reference_source_object_receipts WHERE job_id=? AND object_id=?', (job['jobId'], image))
            (spool.root / spool.storage_key(job['jobId'], 1, image)).unlink()
            api = ReferenceAPI(store, bearer_tokens={'synthetic-staging-token':'staging'},
                capability_fingerprint='a'*64, options_fingerprint='a'*64,
                source_spool=spool, source_attestation_keys={}, authority_id=queue.authority_id)
            entered, resume = threading.Event(), threading.Event()
            class Body(io.BytesIO):
                def read(self, size=-1):
                    entered.set()
                    if not resume.wait(5): raise RuntimeError('test barrier timed out')
                    return super().read(size)
            responses = []
            import hashlib
            path=f"/ltds/v2/jobs/{job['jobId']}/source/{image}?generation=1"
            headers={'Authorization':'Bearer synthetic-staging-token', 'Content-SHA256':hashlib.sha256(b'abc').hexdigest()}
            thread=threading.Thread(target=lambda: responses.append(api.stream_source_object(path,headers,Body(b'abc'),3,protected_transport=True)))
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                self.assertEqual(readers.active_count(job['jobId'],1),1)
                with self.assertRaises(SourceReaderError): queue.close()
                readers.fence(job['jobId'],1,'synthetic-release')
            finally:
                resume.set(); thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(responses[0].status,409)
            self.assertEqual(readers.active_count(job['jobId'],1),0)
            # A completed file may survive the fenced upload; never fake an ACK.
            with queue._db() as db:
                self.assertIsNone(db.execute('SELECT 1 FROM reference_source_object_receipts WHERE job_id=? AND object_id=?',(job['jobId'],image)).fetchone())
            with patch.object(spool,'receive_object') as receive:
                self.assertEqual(api.stream_source_object(path,headers,io.BytesIO(b'abc'),3,protected_transport=True).status,409)
                receive.assert_not_called()

    def test_audit_fence_during_io_drains_without_overwriting_custody(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, _, _):
            original=spool.audit_job
            def race(**kwargs):
                self.assertGreater(store.source_readers().active_count(job['jobId'],1),0)
                result=original(**kwargs)
                store.source_readers().fence(job['jobId'],1,'synthetic-release')
                return result
            with patch.object(spool,'audit_job',side_effect=race):
                self.assertEqual(store.reconcile_source_spool(spool)['held'],1)
            with store.queue._db() as db:
                self.assertEqual(db.execute('SELECT source_state FROM reference_source_intakes').fetchone()[0],'accepted')
            self.assertEqual(store.source_readers().active_count(job['jobId'],1),0)

    def test_operation_quota_refusal_precedes_filesystem_effect(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, _, _):
            with store.queue._db() as db: used=store._metadata_used_bytes(db)
            store.max_metadata_bytes=used
            with patch.object(spool,'verify_object') as verify:
                with self.assertRaises(Conflict):
                    store.complete_source_intake(job['jobId'],**completion(store,job['jobId'],spool))
                verify.assert_not_called()
            self.assertEqual(store.source_readers().active_count(job['jobId'],1),0)

    def test_audit_excludes_other_threads_io_but_allows_actual_nested_adoption(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, grant, image):
            readers=store.source_readers()
            with store.source_operation(job['jobId'],generation=1,operation='source-put'):
                outcomes=[]
                with patch.object(spool,'audit_job') as audit:
                    thread=threading.Thread(target=lambda: outcomes.append(store.reconcile_source_spool(spool)))
                    thread.start(); thread.join(5)
                    self.assertFalse(thread.is_alive())
                    self.assertEqual(outcomes[0]['held'],1)
                    audit.assert_not_called()
            with store.source_operation(job['jobId'],generation=1,operation='source-audit'):
                # Same actual thread may adopt receipts under its audit custody.
                with store.source_operation(job['jobId'],generation=1,operation='source-receipt'):
                    self.assertEqual(readers.active_count(job['jobId'],1),2)
                errors=[]
                def read():
                    try: store.source_object_for_grant(grant['token'],image,source_spool=spool)
                    except Exception as error: errors.append(error)
                with patch.object(spool,'open_verified_object') as opened:
                    thread=threading.Thread(target=read); thread.start(); thread.join(5)
                    self.assertFalse(thread.is_alive())
                    opened.assert_not_called()
                self.assertEqual(len(errors),1)
                self.assertRegex(str(errors[0]),'source_operation_busy')
                # A different thread cannot claim another thread's operation scope.
                handle=next(h for h in readers._handles.values() if h.operation=='source-audit')
                def steal():
                    try:
                        with readers.operation_scope(handle): pass
                    except Exception as error: errors.append(error)
                thread=threading.Thread(target=steal); thread.start(); thread.join(5)
                self.assertRegex(str(errors[-1]),'source_operation_scope_invalid')
            self.assertEqual(readers.active_count(job['jobId'],1),0)

    def test_completion_rejects_changed_durable_receipt_after_hash(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, _, image):
            original=spool.verify_object
            changed=False
            def race(**kwargs):
                nonlocal changed
                result=original(**kwargs)
                if not changed:
                    changed=True
                    with store.queue._db() as db:
                        db.execute("UPDATE reference_source_object_receipts SET storage_key='changed' WHERE job_id=? AND object_id=?",(job['jobId'],image))
                return result
            with patch.object(spool,'verify_object',side_effect=race):
                with self.assertRaisesRegex(Conflict,'source receipt set changed'):
                    store.complete_source_intake(job['jobId'],**completion(store,job['jobId'],spool))
            self.assertEqual(store.source_readers().active_count(job['jobId'],1),0)

    def test_hash_rejects_changed_inode_metadata_even_when_bytes_match(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, _, image):
            from types import SimpleNamespace
            original=os.fstat
            count=0
            def race(fd):
                nonlocal count
                count+=1
                info=original(fd)
                if count==2:
                    fields=('st_dev','st_ino','st_size','st_mode','st_nlink','st_uid','st_mtime_ns','st_ctime_ns')
                    values={field:getattr(info,field) for field in fields}
                    values['st_ctime_ns']+=1
                    return SimpleNamespace(**values)
                return info
            import hashlib
            with patch('processing.source_spool.os.fstat',side_effect=race):
                self.assertFalse(spool._hash_file(spool.root/spool.storage_key(job['jobId'],1,image),3,hashlib.sha256(b'abc').hexdigest()))

    def test_ambiguous_source_close_is_not_retried_or_unpinned(self):
        with tempfile.TemporaryDirectory() as directory:
            child=subprocess.run([sys.executable,'-c','from tests.test_reference_source_operations import uncertain_operation; import sys; uncertain_operation(sys.argv[1])',directory],capture_output=True,text=True,timeout=15)
            self.assertEqual(child.returncode,0,child.stderr)
            result=json.loads(child.stdout)
            self.assertEqual({key:result[key] for key in ('refused','attempts','active','status')},dict(refused=True,attempts=1,active=1,status=503))
            from processing.prototype import Queue
            from processing.reference_store import ReferenceStore
            queue=Queue(directory,reference_enabled=True)
            try:
                self.assertEqual(ReferenceStore(queue).source_readers().active_count(result['jobId'],1),0)
                with queue._db() as db:
                    self.assertIsNotNone(db.execute("SELECT 1 FROM source_reader_pins WHERE operation='source-complete' AND state='superseded'").fetchone())
            finally: queue.close()


if __name__ == '__main__': unittest.main()
