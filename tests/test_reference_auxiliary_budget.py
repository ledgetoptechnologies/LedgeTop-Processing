"""Inactive v4 reservation candidates, not daemon/capacity qualification.

Positive metadata-counter tests use an explicitly synthetic free-space snapshot
so they do not claim the container tmpfs prepays the candidate global budget.
"""
from contextlib import closing
import hashlib
import os
from pathlib import Path
import sqlite3
import signal
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch

from processing import reference_auxiliary_budget as a


class AuxiliaryBudgetDescriptorTests(unittest.TestCase):
    def test_sql_identity_guards_match_closed_ids_including_embedded_nul(self):
        with closing(sqlite3.connect(':memory:')) as db:
            db.executescript(a.SCHEMA)
            for value in ('x\x00foreign/path','x\x00','foreign/path','é','-prefix','',None,'a'*129):
                for table,columns in [('job_holds',(value,'claim')),('job_holds',('job',value)),
                                      ('reader_holds',(value,'pin'))]:
                    with self.assertRaises(sqlite3.IntegrityError):
                        db.execute('INSERT INTO '+table+' VALUES(?,?)',columns)
    def test_candidate_is_detached_default_off_and_accounts_fifth_store(self):
        value=a.candidate_descriptor();self.assertIs(value['qualifiedForDaemon'],False)
        self.assertEqual(set(value['storeCaps']),set(a.NAMES))
        self.assertEqual(value['controlCap'],4*1024**2)
        self.assertGreater(value['storeCaps']['scratch.sqlite'],1024**3)
        value['storeCaps'].clear();self.assertEqual(len(a.candidate_descriptor()['storeCaps']),4)

    def test_fabricated_profile_or_budget_cannot_mint_authority(self):
        with self.assertRaises(a.AuxiliaryHeld):a.initialize_fresh_budget(object(),object())
        with self.assertRaises(a.AuxiliaryHeld):a.AuxiliaryBudget()
        with self.assertRaises(a.AuxiliaryHeld):a.reject_unbudgeted(Path('/missing/worker.sqlite'),object())


@unittest.skipUnless(os.name=='posix','actual private v4 owner/SQLite lifecycle requires POSIX')
class AuxiliaryBudgetActualStoreTests(unittest.TestCase):
    def setUp(self):
        from tests.test_reference_metadata_retirement import CompletedV4Fixture
        self.fixture=CompletedV4Fixture();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.root=self.fixture.root
        # SHAPE counters only. An independent actual-free/device test is below;
        # this fixture does not qualify physical reservation or legal job fit.
        self.free=patch.object(a.shutil,'disk_usage',return_value=type('Usage',(),{'free':2**50})())
        self.free.start();self.addCleanup(self.free.stop)
        self.budget=a.initialize_fresh_budget(self.fixture.context,self.fixture.owner)

    def image(self):
        return {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.glob('*.sqlite')}

    def test_latch_reopen_exact_and_normal_default_writers_refuse_before_clock(self):
        from processing.worker_outbox import WorkerOutbox
        from processing.reference_failure import FailureOutbox
        from processing.scratch_policy import ScratchLedger
        from processing.reference_readers import ReaderSession
        before=self.image()
        for make in (lambda:WorkerOutbox(self.root/'outbox.sqlite'),
                     lambda:FailureOutbox(self.root/'failure-outbox.sqlite'),
                     lambda:ScratchLedger(self.root/'scratch.sqlite',self.fixture.scratch),
                     lambda:ReaderSession(self.fixture.f.ledger)):
            with self.assertRaisesRegex(a.AuxiliaryHeld,'latched_budget_required'):make()
        self.assertEqual(before,self.image())
        reopened=a.open_budget(self.fixture.context,self.fixture.owner)
        self.assertEqual(reopened.latch,self.budget.latch)
        with self.assertRaisesRegex(a.AuxiliaryHeld,'lifecycle_bounds_unqualified'):
            reopened.require_lifecycle_qualification()

    def test_job_holds_stress_idempotent_permanent_and_33rd_refuses(self):
        for index in range(32):
            self.budget.reserve_job('job-'+str(index),'claim-'+str(index))
            self.budget.reserve_job('job-'+str(index),'claim-'+str(index))
        before=self.image()
        with self.assertRaisesRegex(a.AuxiliaryHeld,'job_limit'):self.budget.reserve_job('job-32','claim-32')
        with self.assertRaisesRegex(a.AuxiliaryHeld,'claim_changed'):self.budget.reserve_job('job-0','foreign')
        self.assertEqual(before,self.image())
        with closing(sqlite3.connect(self.budget.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM job_holds').fetchone()[0],32)
            with self.assertRaises(sqlite3.IntegrityError):db.execute('DELETE FROM job_holds')

    def test_reader_history_stress_is_finite_and_reopen_never_resets_it(self):
        for i in range(a.EPOCHS):self.budget.reserve_reader('epoch-'+str(i),'epoch')
        for i in range(a.PINS):self.budget.reserve_reader('pin-'+str(i),'pin')
        reopened=a.open_budget(self.fixture.context,self.fixture.owner)
        before=self.image()
        for reference,kind in [('epoch-new','epoch'),('pin-new','pin')]:
            with self.assertRaisesRegex(a.AuxiliaryHeld,'reader_limit'):reopened.reserve_reader(reference,kind)
        self.assertEqual(before,self.image())

    def test_replaced_store_wal_unknown_journal_and_foreign_latch_hold(self):
        store=self.root/'outbox.sqlite';before=self.image()
        wal=Path(str(store)+'-wal');wal.write_bytes(b'foreign');wal.chmod(0o600)
        with self.assertRaisesRegex(a.AuxiliaryHeld,'wal_held'):self.budget.reserve_job('one','one')
        wal.unlink();self.assertEqual(before,self.image())
        journal=Path(str(store)+'-journal');journal.write_bytes(b'unknown');journal.chmod(0o600)
        with self.assertRaisesRegex(a.AuxiliaryHeld,'store_recovery_required'):
            with self.budget.writer(store):pass
        journal.unlink();self.assertEqual(before,self.image())
        saved=store.with_suffix('.saved');store.rename(saved);store.write_bytes(saved.read_bytes());store.chmod(0o600)
        with self.assertRaisesRegex(a.AuxiliaryHeld,'store_identity_changed'):self.budget.reserve_job('one','one')

    def test_durable_malformed_job_and_reader_rows_hold_on_reopen_without_writes(self):
        # Original schema is unchanged: nullable TEXT primary keys and valid
        # SQL strings alone do not establish the closed protocol identities.
        for table,values in [('job_holds',(None,'claim')),('job_holds',('foreign/path','claim')),
                             ('job_holds',('job','x\x00foreign/path')),
                             ('reader_holds',(None,'pin')),('reader_holds',('foreign/path','epoch')),
                             ('reader_holds',('x\x00','pin'))]:
            with closing(sqlite3.connect(self.budget.path)) as db:
                # Model persisted corruption without altering the schema. The
                # real fresh schema rejects these inserts normally.
                db.execute('PRAGMA ignore_check_constraints=ON')
                db.execute('INSERT INTO '+table+' VALUES(?,?)',values);db.commit()
            before=self.image()
            with self.assertRaises(a.AuxiliaryHeld):a.open_budget(self.fixture.context,self.fixture.owner)
            self.assertEqual(before,self.image())
            # New independent empty state for each case; no production reset.
            self.fixture.doCleanups()
            from tests.test_reference_metadata_retirement import CompletedV4Fixture
            self.fixture=CompletedV4Fixture();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
            self.root=self.fixture.root;self.budget=a.initialize_fresh_budget(self.fixture.context,self.fixture.owner)

    def test_all_four_actual_writer_connections_apply_page_fuses_and_reader_hook(self):
        from processing.scratch_policy import ScratchLedger
        from processing.worker_outbox import WorkerOutbox
        from processing.reference_failure import FailureOutbox
        from processing.reference_engine_worker import ReferenceEngineWorker
        from processing.reference_readers import ReaderSession
        b=self.budget
        ledger=ScratchLedger(self.root/'scratch.sqlite',self.fixture.scratch,safety_floor_bytes=1024,
            cleanup_grace_seconds=0,input_copy_accounting=True,auxiliary_budget=b)
        outbox=WorkerOutbox(self.root/'outbox.sqlite',auxiliary_budget=b)
        failure=FailureOutbox(self.root/'failure-outbox.sqlite',auxiliary_budget=b)
        old=self.fixture.f.worker
        worker=ReferenceEngineWorker(self.root/'worker.sqlite',ledger=ledger,outbox=outbox,engine=old.engine,
            engine_id=old.engine_id,node_id=old.node_id,authority_id=old.authority_id,coordinator_origin=old.origin,
            gate_generation=old.gate_generation,worker_id=old.worker_id,engine_version=old.version,auxiliary_budget=b)
        for name,transaction in [('scratch.sqlite',ledger._transaction),('outbox.sqlite',outbox._transaction),
                                 ('failure-outbox.sqlite',failure._transaction),('worker.sqlite',worker._db)]:
            with transaction() as db:self.assertEqual(db.execute('PRAGMA max_page_count').fetchone()[0],a.CAPS[name]//4096)
        with ReaderSession(ledger):pass
        with closing(sqlite3.connect(b.path)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM reader_holds WHERE kind='epoch'").fetchone()[0],1)

    def test_actual_page_cap_rolls_back_and_held_free_space_preserves_bytes(self):
        store=self.root/'worker.sqlite';before=self.image()
        with patch.object(a.shutil,'disk_usage',return_value=type('Usage',(),{'free':0})()):
            with self.assertRaisesRegex(a.AuxiliaryHeld,'physical_capacity_held'):
                self.budget.reserve_job('held','held')
        self.assertEqual(before,self.image())
        with self.budget.writer(store) as configure:
            with closing(sqlite3.connect(store)) as db:
                configure(db);pages=db.execute('PRAGMA page_count').fetchone()[0]
                db.execute('PRAGMA max_page_count='+str(pages))
                with self.assertRaises(sqlite3.OperationalError):
                    with db:
                        db.execute('BEGIN IMMEDIATE')
                        db.execute('CREATE TABLE excess(value BLOB)')
                        db.execute('INSERT INTO excess VALUES(zeroblob(1048576))')
        self.assertEqual(before,self.image())

    def test_separate_reopen_serializes_reader_admission_behind_actual_writer_scope(self):
        other=a.open_budget(self.fixture.context,self.fixture.owner)
        entered=threading.Event();done=threading.Event();errors=[]
        def reader():
            entered.set()
            try:other.reserve_reader('concurrent-reader','pin')
            except BaseException as error:errors.append(error)
            finally:done.set()
        child=threading.Thread(target=reader)
        with self.budget.writer(self.root/'worker.sqlite'):
            child.start();self.assertTrue(entered.wait(3));self.assertFalse(done.wait(.1))
        child.join(5);self.assertFalse(child.is_alive());self.assertEqual(errors,[])
        self.assertTrue(done.is_set())

    def test_old_profiles_never_adopt_latch_or_change_any_store(self):
        from dataclasses import replace
        import json
        from processing.reference_protocol import canonical_json
        from processing import reference_indexed_fetch as j
        before=self.image()
        for version,policy in [(2,j.POLICY_ID),(3,j.THREE_POLICY_ID)]:
            binding=json.loads(self.fixture.context.binding_json)
            binding['inputCopyAccounting']={'schemaVersion':version,'fetchJournal':{'schemaVersion':1,'policyId':policy}}
            binding['indexedFetchJournal']=j.compiled_profile_descriptor(policy)
            context=replace(self.fixture.context,binding_json=canonical_json(binding))
            with self.assertRaisesRegex(a.AuxiliaryHeld,'fresh_v4'):a.open_budget(context,self.fixture.owner)
            self.assertEqual(before,self.image())

    def test_actual_process_death_after_fsynced_hold_preserves_exact_idempotent_record(self):
        import json
        import select
        from ops.reference_worker_daemon import DaemonStateLock
        from processing import reference_indexed_fetch as j
        self.fixture.owner.__exit__(None,None,None)
        script='''import json,signal,sys
from pathlib import Path
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing import reference_indexed_fetch as j
from processing import reference_auxiliary_budget as a
binding=json.loads(sys.argv[2]);root=Path(sys.argv[1])
with DaemonStateLock(root) as owner:
 context=j.read_only_profile(root/'driver.sqlite',bootstrap_id=sys.argv[3],binding=binding,owner=owner)
 budget=a.open_budget(context,owner)
 # Synthetic capacity solely isolates durable metadata/ownership from the
 # separate unqualified physical allowance in this disposable test.
 with patch.object(a.shutil,'disk_usage',return_value=type('Usage',(),{'free':2**50})()):budget.reserve_job('crash-job','crash-claim')
 print('committed',flush=True)
 signal.pause()
'''
        child=subprocess.Popen([sys.executable,'-B','-c',script,str(self.root),
            self.fixture.context.binding_json.decode(),self.fixture.context.bootstrap_id],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            self.assertTrue(select.select([child.stdout],[],[],10)[0])
            self.assertEqual(child.stdout.readline(),b'committed\n')
            child.kill();self.assertEqual(child.wait(timeout=10),-signal.SIGKILL)
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=10)
            child.stdout.close();child.stderr.close()
        self.fixture.owner=DaemonStateLock(self.root).__enter__()
        self.addCleanup(self.fixture.owner.__exit__,None,None,None)
        context=j.read_only_profile(self.root/'driver.sqlite',bootstrap_id=self.fixture.context.bootstrap_id,
            binding=json.loads(self.fixture.context.binding_json),owner=self.fixture.owner)
        reopened=a.open_budget(context,self.fixture.owner)
        before=self.image();reopened.reserve_job('crash-job','crash-claim');self.assertEqual(before,self.image())
        with closing(sqlite3.connect(reopened.path)) as db:
            self.assertEqual(db.execute('SELECT job,claim FROM job_holds').fetchall(),[('crash-job','crash-claim')])


if __name__=='__main__':unittest.main()
