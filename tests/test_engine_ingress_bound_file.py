"""Target these tests explicitly; inherited guard fixture tests also exist."""
import hashlib
from pathlib import Path
import socket
import threading
import time
import uuid
from types import SimpleNamespace
from unittest.mock import patch

from processing import engine_upload_contract
from processing.engine_ingress_guard import AdmissionError
from processing.maintenance import Conflict
from processing.guarded_native_engine import SenderCompletion
from tests.test_engine_ingress_guard import GuardTests


class BoundFileTests(GuardTests):
    def install_fence(self):
        # The parent fixture creates its synthetic running claim immediately.
        # Install the experimental offline fence while this test-owned queue
        # is idle, then restore the synthetic claim before any transfer starts.
        from processing.transfer_fence import install_offline
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='queued' WHERE id=?", (self.claim['id'],))
        with self.queue._db() as db:
            install_offline(db, enabled=True)
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='running' WHERE id=?", (self.claim['id'],))

    def sender(self, upload, source, writer, chunks, release=None):
        stop, done, success = threading.Event(), threading.Event(), threading.Event()
        errors = []
        def run():
            try:
                for index, chunk in enumerate(chunks):
                    if index and release is not None and not release.wait(3):
                        raise RuntimeError('synthetic sender timed out')
                    if stop.is_set(): raise RuntimeError('synthetic sender stopped')
                    writer.sendall(chunk)
                writer.shutdown(socket.SHUT_WR)
                success.set()
            except Exception as exc:
                errors.append(exc)
            finally:
                done.set()
        thread = threading.Thread(target=run)
        completion = SenderCompletion(source=source, writer=writer, thread=thread, stop=stop,
                                      done=done, success=success, errors=errors,
                                      engine=SimpleNamespace(_unreaped=[]), upload_id=upload)
        thread.start()
        if release is None and not done.wait(3):
            raise RuntimeError('synthetic sender did not finish')
        return completion
    def prepare(self):
        self.install_fence()
        self.init();self.store.update(self.task,phase='upload_pending')
        upload=str(uuid.uuid4())
        self.store.prepare_guarded_engine_upload(self.claim['id'],self.claim['claimToken'],'fixture.jpg',upload)
        return upload

    def test_qualification_precedes_schema_mutation(self):
        with self.assertRaises(Conflict):
            self.store.prepare_guarded_engine_upload(self.claim['id'],self.claim['claimToken'],'fixture.jpg',str(uuid.uuid4()))
        with self.queue._db() as db:self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='viewer_engine_upload_intents'").fetchone())

    def test_preparation_rejects_environment_identity_mismatch(self):
        with patch.object(engine_upload_contract,'QUALIFIED',True):
            self.init();self.store.update(self.task,phase='upload_pending')
            with self.queue._db() as db:db.execute("UPDATE viewer_tasks SET environment='staging' WHERE uuid=?",(self.task,))
            with self.assertRaises(Conflict):
                self.store.prepare_guarded_engine_upload(self.claim['id'],self.claim['claimToken'],'fixture.jpg',str(uuid.uuid4()))
            with self.queue._db() as db:self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='viewer_engine_upload_intents'").fetchone())

    def test_preparation_rejects_node_identity_mismatch(self):
        with patch.object(engine_upload_contract,'QUALIFIED',True):
            self.init();self.store.update(self.task,phase='upload_pending')
            with self.queue._db() as db:db.execute("UPDATE viewer_tasks SET node_id='main' WHERE uuid=?",(self.task,))
            with self.assertRaises(Conflict):
                self.store.prepare_guarded_engine_upload(self.claim['id'],self.claim['claimToken'],'fixture.jpg',str(uuid.uuid4()))
            with self.queue._db() as db:self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='viewer_engine_upload_intents'").fetchone())

    def test_exact_db_file_drives_deterministic_multipart(self):
        with patch.object(engine_upload_contract,'QUALIFIED',True):
            upload=self.prepare();source,writer=socket.socketpair()
            completion=self.sender(upload,source,writer,[b'fixture'])
            try:
                headers=self.headers(**{'X-LedgeTop-Upload-ID':upload,'X-Filename':'spoof.jpg'})
                self.assertEqual(self.guard.stream_bound_upload('/task/new/upload/'+self.engine,headers,source,completion=completion),(200,b'{"success":true}'))
                boundary='processing-'+uuid.UUID(upload).hex
                expected=(f'--{boundary}\r\nContent-Disposition: form-data; name="images"; filename="fixture.jpg"\r\nContent-Type: application/octet-stream\r\n\r\n'.encode()+b'fixture'+f'\r\n--{boundary}--\r\n'.encode())
                self.assertEqual(self.received_bodies[-1],expected)
                with self.queue._db() as db:
                    row=db.execute('SELECT task_uuid,generation,name_key,bytes,sha256,state FROM viewer_engine_upload_intents').fetchone()
                    self.assertEqual(tuple(row),(self.task,1,'fixture.jpg',7,hashlib.sha256(b'fixture').hexdigest(),'complete'))
                    self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'complete')
                with self.assertRaises(Conflict):self.store.prepare_guarded_engine_upload(self.claim['id'],self.claim['claimToken'],'fixture.jpg',str(uuid.uuid4()))
                with self.assertRaises(AdmissionError):self.guard.mutate('POST','/task/new/upload/'+self.engine,self.headers(),b'x')
            finally:completion.stop()

    def test_changed_db_file_or_generation_holds_before_body(self):
        with patch.object(engine_upload_contract,'QUALIFIED',True):
            upload=self.prepare();source,writer=socket.socketpair()
            completion=self.sender(upload,source,writer,[b'fixture'])
            try:
                with self.queue._db() as db:db.execute('UPDATE viewer_tasks SET generation=2 WHERE uuid=?',(self.task,))
                with self.assertRaises(AdmissionError):self.guard.stream_bound_upload('/task/new/upload/'+self.engine,self.headers(**{'X-LedgeTop-Upload-ID':upload}),source,completion=completion)
                self.assertTrue(completion.stopped)
                with self.queue._db() as db:
                    db.execute('UPDATE viewer_tasks SET generation=1 WHERE uuid=?',(self.task,))
                    db.execute("UPDATE viewer_files SET sha256=? WHERE uuid=?",('a'*64,self.task))
                source,writer=socket.socketpair();completion=self.sender(upload,source,writer,[b'fixture'])
                with self.assertRaises(AdmissionError):self.guard.stream_bound_upload('/task/new/upload/'+self.engine,self.headers(**{'X-LedgeTop-Upload-ID':upload}),source,completion=completion)
                self.assertEqual(self.calls,['/task/new/init'])
                with self.queue._db() as db:self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0],'prepared')
            finally:completion.stop()

    def test_wrong_file_bytes_hold_journal_and_block_new_upload(self):
        with patch.object(engine_upload_contract,'QUALIFIED',True):
            upload=self.prepare();source,writer=socket.socketpair()
            completion=self.sender(upload,source,writer,[b'changed'])
            try:
                with self.assertRaises(AdmissionError):self.guard.stream_bound_upload('/task/new/upload/'+self.engine,self.headers(**{'X-LedgeTop-Upload-ID':upload}),source,completion=completion)
                with self.queue._db() as db:
                    self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0],'started')
                    self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')
                with self.assertRaises(Conflict):self.store.prepare_guarded_engine_upload(self.claim['id'],self.claim['claimToken'],'fixture.jpg',str(uuid.uuid4()))
                with self.assertRaises(AdmissionError):self.guard.stream_bound_upload('/task/new/upload/'+self.engine,self.headers(**{'X-LedgeTop-Upload-ID':upload}),source,completion=completion)
            finally:completion.stop()

    def test_db_file_change_after_started_commit_holds_before_source(self):
        import sqlite3
        from processing.engine_ingress_guard import _resume_upload_transaction
        with patch.object(engine_upload_contract,'QUALIFIED',True):
            upload=self.prepare();source,writer=socket.socketpair()
            completion=self.sender(upload,source,writer,[b'fixture'])
            def interposed(db,deadline):
                external=sqlite3.connect(self.queue.path,timeout=1)
                try:
                    external.execute('BEGIN IMMEDIATE')
                    external.execute('UPDATE viewer_files SET sha256=? WHERE uuid=?',('a'*64,self.task))
                    external.commit()
                finally:external.close()
                return _resume_upload_transaction(db,deadline)
            try:
                with patch('processing.engine_ingress_guard._resume_upload_transaction',side_effect=interposed):
                    with self.assertRaises(AdmissionError):self.guard.stream_bound_upload('/task/new/upload/'+self.engine,self.headers(**{'X-LedgeTop-Upload-ID':upload}),source,completion=completion)
                self.assertTrue(completion.stopped);self.assertEqual(self.calls,['/task/new/init'])
                with self.queue._db() as db:
                    self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0],'started')
                    self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')
            finally:completion.stop()

    def test_file_boundary_collision_withholds_complete_marker_after_prefix(self):
        with patch.object(engine_upload_contract,'QUALIFIED',True):
            self.install_fence()
            self.init();self.store.update(self.task,phase='upload_pending')
            upload=str(uuid.uuid4());needle=b'--processing-'+uuid.UUID(upload).hex.encode('ascii')
            payload=b'x'*10+needle+b'not-a-real-boundary'
            target=Path(self.store.task(self.task)['files'][0]['path']);target.write_bytes(payload)
            with self.queue._db() as db:
                db.execute('UPDATE viewer_files SET bytes=?,sha256=? WHERE uuid=?',(len(payload),hashlib.sha256(payload).hexdigest(),self.task))
            self.store.prepare_guarded_engine_upload(self.claim['id'],self.claim['claimToken'],'fixture.jpg',upload)
            import array,fcntl,termios
            source,writer=socket.socketpair();observed=threading.Event();self.observer=observed.set;outcome=[]
            release=threading.Event();completion=self.sender(upload,source,writer,[payload[:15],payload[15:]],release)
            def transfer():
                try:self.guard.stream_bound_upload('/task/new/upload/'+self.engine,self.headers(**{'X-LedgeTop-Upload-ID':upload}),source,completion=completion)
                except Exception as error:outcome.append(error)
            thread=threading.Thread(target=transfer)
            try:
                first=15;thread.start();deadline=time.monotonic()+2
                while time.monotonic()<deadline:
                    waiting=array.array('i',[0]);fcntl.ioctl(source.fileno(),termios.FIONREAD,waiting,True)
                    if waiting[0]==0:break
                    time.sleep(.01)
                self.assertEqual(waiting[0],0,'first source fragment was not consumed')
                release.set();thread.join(3)
                self.assertFalse(thread.is_alive());self.assertEqual(len(outcome),1);self.assertIsInstance(outcome[0],AdmissionError)
                self.assertTrue(observed.wait(2))
                body=self.received_bodies[-1];forwarded=body.partition(b'\r\n\r\n')[2]
                self.assertTrue(forwarded.startswith(payload[:15]));self.assertNotIn(needle,forwarded)
                with self.queue._db() as db:
                    self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0],'started')
                    self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')
            finally:
                release.set();completion.stop();thread.join(3);self.assertFalse(thread.is_alive())
