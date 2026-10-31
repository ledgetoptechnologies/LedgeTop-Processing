import errno
import gc
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import weakref
import threading
import signal
import warnings
from contextlib import contextmanager

from processing.reference_readers import ReaderSession, ReaderSessionError
from processing.scratch_policy import ScratchLedger


@unittest.skipUnless(sys.platform.startswith('linux'),'real Linux flock/process/FD semantics required')
class ReaderSessionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder=Path(self.temp.name)
        self.root=self.folder/'scratch'
        self.root.mkdir(mode=0o700)
        self.ledger=ScratchLedger(self.folder/'private'/'scratch.sqlite',self.root,
            capacity_probe=lambda _:100000,safety_floor_bytes=0,cleanup_grace_seconds=0,clock=lambda:100)
        self.ledger.reserve(job_id='j1',generation=1,reservation_id='r1',manifest_sha256='a'*64,
            input_bytes=0,output_budget=3,environment='staging',ttl_seconds=10,worker_ready=True)
        self.ledger.set_engine_state('r1','engine_started')
        self.file=self.root/'jobs'/'r1'/'o1.bin'
        self.file.parent.mkdir(parents=True,mode=0o700)
        self.file.write_bytes(b'abc')
        import hashlib
        self.sha=hashlib.sha256(b'abc').hexdigest()
        self.ledger.register_result('r1',job_id='j1',generation=1,manifest_sha256='a'*64,
            pipeline_sha256='b'*64,result_manifest_sha256='c'*64,
            objects=[dict(object_id='o1',bytes=3,sha256=self.sha,relative_path='jobs/r1/o1.bin')])

    @contextmanager
    def db(self):
        db=sqlite3.connect(self.ledger.path)
        db.row_factory=sqlite3.Row
        try:
            with db:yield db
        finally:db.close()

    def child(self,body,*,check=True):
        script='''import os,sys,sqlite3
from processing.scratch_policy import ScratchLedger
from processing.reference_readers import ReaderSession,ReaderSessionError
ledger=ScratchLedger(sys.argv[1],sys.argv[2],capacity_probe=lambda _:100000,safety_floor_bytes=0,cleanup_grace_seconds=0)
'''+body
        return subprocess.run([sys.executable,'-B','-c',script,str(self.ledger.path),str(self.root),str(self.file)],
            stdin=subprocess.DEVNULL,capture_output=True,text=True,timeout=15,check=check)

    def test_real_child_crash_owned_pins_reaped_but_legacy_foreign_kinds_retained(self):
        for reference,kind,state in (('legacy','read','active'),('legacy-uncertain','read','uncertain'),
                ('worker','worker','active'),('worker-uncertain','worker','uncertain'),
                ('downstream','downstream','active'),('downstream-uncertain','downstream','uncertain')):
            self.ledger.set_pin('r1','o1',reference,kind=kind,state=state)
        result=self.child("""session=ReaderSession(ledger).__enter__()
handle=session.pin('r1','o1')
fd=os.open(sys.argv[3],os.O_RDONLY)
handle.attach_fd(fd)
print(session.epoch,flush=True)
os._exit(0)
""")
        old_epoch=result.stdout.strip()
        with self.db() as db:
            db.execute("INSERT INTO pins VALUES('r1','o1','foreign','read','active')")
            db.execute("INSERT INTO reader_owned_pins VALUES('foreign','r1','o1','unregistered-epoch','active',NULL)")
            db.execute("INSERT INTO reader_owned_pins VALUES('worker','r1','o1',?,'active',NULL)",(old_epoch,))
        # Reader reaping must never observe the ledger's wall clock.
        self.ledger.clock=lambda:(_ for _ in ()).throw(AssertionError('clock observed'))
        with ReaderSession(self.ledger) as session:
            self.assertNotEqual(session.epoch,old_epoch)
            self.assertEqual(session.active_count,0)
            with self.db() as db:
                pins={row['reference']:(row['kind'],row['state']) for row in db.execute('SELECT * FROM pins')}
                owned=db.execute("SELECT * FROM reader_owned_pins WHERE epoch=? AND reference!='worker'",(old_epoch,)).fetchone()
                self.assertEqual(owned['state'],'superseded')
                self.assertEqual(owned['superseded_by'],session.epoch)
                self.assertEqual(pins[owned['reference']],('read','released'))
                self.assertEqual(pins['foreign'],('read','active'))
                self.assertEqual(pins['worker'],('worker','active'))
                self.assertEqual(pins['downstream-uncertain'],('downstream','uncertain'))
                self.assertEqual(pins['legacy-uncertain'],('read','uncertain'))

    def test_live_child_and_same_process_double_owner_are_exclusive(self):
        with ReaderSession(self.ledger) as session:
            with self.assertRaisesRegex(ReaderSessionError,'reader_session_busy'):
                ReaderSession(self.ledger).__enter__()
            result=self.child("""try:
    ReaderSession(ledger).__enter__()
except ReaderSessionError as error:
    print(str(error))
else:
    raise AssertionError('two reader owners admitted')
""")
            self.assertIn('reader_session_busy',result.stdout)
            self.assertEqual(session.active_count,0)

    def test_attached_live_fd_blocks_release_closed_fd_releases_idempotently(self):
        with ReaderSession(self.ledger) as session:
            handle=session.pin('r1','o1')
            fd=os.open(self.file,os.O_RDONLY)
            handle.attach_fd(fd)
            with self.assertRaisesRegex(ReaderSessionError,'still_open'):
                handle.release()
            with self.assertRaises(ReaderSessionError):session.close()
            os.close(fd)
            handle.release();handle.release()
            self.assertEqual(session.active_count,0)

    def test_close_fd_known_success_survives_fd_number_reuse(self):
        with ReaderSession(self.ledger) as session:
            handle=session.pin('r1','o1')
            fd=os.open(self.file,os.O_RDONLY);handle.attach_fd(fd)
            handle.close_fd(fd)
            replacement=os.open(self.file,os.O_RDONLY)
            try:
                if replacement!=fd:
                    os.dup2(replacement,fd)
                handle.release()
                self.assertEqual(session.active_count,0)
                self.assertEqual(os.read(fd,3),b'abc')
            finally:
                if replacement!=fd:os.close(fd)
                os.close(replacement)

    def test_external_close_then_reuse_is_conservatively_refused(self):
        with ReaderSession(self.ledger) as session:
            handle=session.pin('r1','o1')
            fd=os.open(self.file,os.O_RDONLY);handle.attach_fd(fd);os.close(fd)
            replacement=os.open(self.file,os.O_RDONLY)
            if replacement!=fd:os.dup2(replacement,fd)
            with self.assertRaisesRegex(ReaderSessionError,'still_open_or_reused'):handle.release()
            if replacement!=fd:os.close(fd)
            os.close(replacement)
            handle.release()

    def test_abort_unopened_never_releases_attached_descriptor(self):
        with ReaderSession(self.ledger) as session:
            unopened=session.pin('r1','o1');unopened.abort_unopened()
            handle=session.pin('r1','o1')
            fd=os.open(self.file,os.O_RDONLY);handle.attach_fd(fd)
            with self.assertRaisesRegex(ReaderSessionError,'already_attached'):handle.abort_unopened()
            handle.close_fd();handle.release()

    def test_failed_release_commit_retains_handle_and_lease_until_exact_retry(self):
        with ReaderSession(self.ledger) as session:
            handle=session.pin('r1','o1')
            fd=os.open(self.file,os.O_RDONLY);handle.attach_fd(fd);handle.close_fd()
            original=session._commit
            def lost_reply(db):
                original(db)
                raise OSError('synthetic commit response loss')
            with patch.object(session,'_commit',lost_reply):
                with self.assertRaises(OSError):handle.release()
            self.assertEqual(session.active_count,1)
            with self.assertRaises(ReaderSessionError):session.close()
            result=self.child("""try:ReaderSession(ledger).__enter__()
except ReaderSessionError as error:print(str(error))
""")
            self.assertIn('reader_session_busy',result.stdout)
            handle.release()
            self.assertEqual(session.active_count,0)

    def test_unknown_fd_close_keeps_pin_handle_and_strong_lease(self):
        with ReaderSession(self.ledger) as session:
            handle=session.pin('r1','o1')
            fd=os.open(self.file,os.O_RDONLY);handle.attach_fd(fd)
            with patch('processing.reference_readers.os.close',side_effect=OSError(errno.EIO,'synthetic close')):
                with self.assertRaisesRegex(ReaderSessionError,'close_uncertain'):handle.close_fd()
            self.assertEqual(session.active_count,1)
            with self.assertRaises(ReaderSessionError):session.close()
            handle.close_fd();handle.release()

    def test_lost_python_references_do_not_drop_active_session_lease(self):
        session=ReaderSession(self.ledger).__enter__()
        handle=session.pin('r1','o1')
        ref=weakref.ref(session)
        del session
        gc.collect()
        self.assertIsNotNone(ref())
        with self.assertRaises(ReaderSessionError):ReaderSession(self.ledger).__enter__()
        handle.abort_unopened()
        ref().close()

    def test_close_checks_durable_orphan_mapping_and_inconsistent_released_owner(self):
        with ReaderSession(self.ledger) as session:
            handle=session.pin('r1','o1')
            # Simulate acquisition commit succeeded but no usable handle reached
            # the caller; durable census must still prevent unlocking the epoch.
            session._handles.clear()
            with self.assertRaisesRegex(ReaderSessionError,'durable_reader_handles'):session.close()
            handle.abort_unopened()
            with self.db() as db:db.execute("UPDATE pins SET state='active' WHERE reference=?",(handle._reference,))
            with self.assertRaisesRegex(ReaderSessionError,'durable_reader_handles'):session.close()
            with self.db() as db:db.execute("UPDATE pins SET state='released' WHERE reference=?",(handle._reference,))

    def test_stale_epoch_callback_is_rejected(self):
        old=ReaderSession(self.ledger).__enter__()
        handle=old.pin('r1','o1');handle.abort_unopened();old.close()
        with ReaderSession(self.ledger):
            with self.assertRaises(ReaderSessionError):handle.abort_unopened()

    def test_forked_child_cannot_pin_release_or_unlock_inherited_session(self):
        with ReaderSession(self.ledger) as session:
            handle=session.pin('r1','o1')
            with warnings.catch_warnings():
                warnings.simplefilter('ignore',DeprecationWarning)
                pid=os.fork()
            if pid==0:
                try:
                    for operation in (session.check,session.close,handle.abort_unopened):
                        try:operation()
                        except ReaderSessionError:pass
                        else:os._exit(9)
                    os._exit(0)
                except BaseException:os._exit(10)
            _,status=os.waitpid(pid,0)
            self.assertEqual(os.waitstatus_to_exitcode(status),0)
            with self.assertRaises(ReaderSessionError):ReaderSession(self.ledger).__enter__()
            handle.abort_unopened()

    def test_fork_guard_precedes_mutex_owned_by_vanished_parent_thread(self):
        with ReaderSession(self.ledger) as session:
            locked,release=threading.Event(),threading.Event()
            def hold():
                with session._mutex:
                    locked.set()
                    release.wait(5)
            thread=threading.Thread(target=hold)
            thread.start()
            self.assertTrue(locked.wait(2))
            with warnings.catch_warnings():
                warnings.simplefilter('ignore',DeprecationWarning)
                pid=os.fork()
            if pid==0:
                signal.alarm(2)
                try:session.check()
                except ReaderSessionError:os._exit(0)
                os._exit(9)
            release.set();thread.join(2)
            _,status=os.waitpid(pid,0)
            self.assertEqual(os.waitstatus_to_exitcode(status),0)

    def test_lock_replacement_symlink_or_hardlink_never_reaps_pins(self):
        result=self.child("""session=ReaderSession(ledger).__enter__()
handle=session.pin('r1','o1')
original=session.lock_path.with_name('original-lease')
session.lock_path.rename(original)
session.lock_path.write_bytes(b'0'*64)
session.lock_path.chmod(0o600)
try:session.check()
except ReaderSessionError:print('replacement refused',flush=True)
else:os._exit(12)
os._exit(0)
""")
        self.assertIn('replacement refused',result.stdout)
        lease=self.ledger.path.with_name(self.ledger.path.name+'.readers.lock')
        original=lease.with_name('original-lease')
        with self.assertRaisesRegex(ReaderSessionError,'binding_changed'):ReaderSession(self.ledger).__enter__()
        with self.db() as db:self.assertEqual(db.execute("SELECT state FROM pins WHERE kind='read'").fetchone()[0],'active')
        lease.unlink()
        lease.symlink_to(original)
        with self.assertRaises((ReaderSessionError,OSError)):ReaderSession(self.ledger).__enter__()
        lease.unlink()
        lease.hardlink_to(original)
        with self.assertRaises(ReaderSessionError):ReaderSession(self.ledger).__enter__()

    def test_child_crash_before_first_epoch_commit_recovers_only_empty_domain(self):
        result=self.child("""session=ReaderSession(ledger)
def crash(db):os._exit(0)
session._commit=crash
session.__enter__()
""")
        self.assertEqual(result.returncode,0)
        with ReaderSession(self.ledger) as session:self.assertEqual(session.active_count,0)

    def test_missing_binding_with_existing_owned_domain_is_never_adopted(self):
        self.child("""session=ReaderSession(ledger).__enter__()
session.pin('r1','o1')
os._exit(0)
""")
        lease=self.ledger.path.with_name(self.ledger.path.name+'.readers.lock')
        before=lease.read_bytes()
        with self.db() as db:db.execute('DELETE FROM reader_session_binding')
        with self.assertRaisesRegex(ReaderSessionError,'unbound_reader_ownership_requires_review'):
            ReaderSession(self.ledger).__enter__()
        self.assertEqual(lease.read_bytes(),before)
        with self.db() as db:self.assertEqual(db.execute("SELECT state FROM pins WHERE kind='read'").fetchone()[0],'active')

    def test_unknown_pin_commit_response_retains_owner_until_real_process_exit(self):
        result=self.child("""session=ReaderSession(ledger).__enter__()
original=session._commit
def lost_reply(db):
    original(db)
    raise OSError('synthetic pin reply loss')
session._commit=lost_reply
try:session.pin('r1','o1')
except OSError:pass
else:os._exit(10)
if session.active_count!=1:os._exit(11)
try:session.close()
except ReaderSessionError:print('uncertain retained',flush=True)
else:os._exit(12)
os._exit(0)
""")
        self.assertIn('uncertain retained',result.stdout)
        with ReaderSession(self.ledger):
            with self.db() as db:self.assertEqual(db.execute("SELECT state FROM pins WHERE kind='read'").fetchone()[0],'released')

    def test_partial_unbound_initial_marker_recovers_without_touching_anonymous_pin(self):
        self.ledger.set_pin('r1','o1','anonymous',kind='read',state='uncertain')
        lease=self.ledger.path.with_name(self.ledger.path.name+'.readers.lock')
        lease.write_bytes(b'partial');lease.chmod(0o600)
        with ReaderSession(self.ledger):
            with self.db() as db:self.assertEqual(db.execute("SELECT state FROM pins WHERE reference='anonymous'").fetchone()[0],'uncertain')

    def test_cleanup_fence_blocks_pin_without_phantom_handle(self):
        self.ledger.set_engine_state('r1','terminal',terminal_attested=True)
        self.ledger.accept_receipt('r1',receipt_id='ack',storage_receipt_id='nas',job_id='j1',generation=1,
            manifest_sha256='a'*64,pipeline_sha256='b'*64,result_manifest_sha256='c'*64,accepted_object_ids=['o1'])
        self.ledger.begin_cleanup('r1','o1',generation=1,sha256=self.sha)
        with ReaderSession(self.ledger) as session:
            with self.assertRaisesRegex(ReaderSessionError,'reader_object_unavailable'):session.pin('r1','o1')
            self.assertEqual(session.active_count,0)


if __name__=='__main__':unittest.main()
