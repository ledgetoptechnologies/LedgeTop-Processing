"""Actual subprocess dirty-page rollback fixtures; no installed worker or engine."""
from contextlib import closing
import copy
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest
from unittest.mock import patch

from ops import bootstrap_worker_input_accounting as bootstrap
from ops import reference_worker_daemon as daemon
from processing import reference_indexed_recovery as recovery


@unittest.skipUnless(os.name=='posix','actual POSIX private roots and child crash')
class IndexedRecoveryTests(unittest.TestCase):
    def setUp(self):
        from tests.test_reference_worker_input_bootstrap import InputBootstrapTests
        InputBootstrapTests.setUp(self)
        self.config['inputCopyAccounting']={'schemaVersion':2,'fetchJournal':{'schemaVersion':1,'policyId':'indexed-worker-fetch-v1'}}
        bootstrap.bootstrap_input_copy_accounting(self.config)
        self.path=self.state/'driver.sqlite';self.journal=Path(str(self.path)+'-journal')
        intent=bootstrap._file(self.state/bootstrap.DIRECTORY/'intent.json')[0];b=intent['binding']
        self.binding={**b['physical'],'origin':b['origins']['coordinatorOrigin'],'brokerOrigin':b['origins']['brokerOrigin'],
            'engineOrigin':b['origins']['engineOrigin'],'gateOrigin':b['origins']['gateCoordinatorOrigin'],
            **{k:b['policy'][k] for k in ('outputBudget','transferTtl','safetyFloor','environments')},
            'inputCopyAccounting':{'schemaVersion':1},'indexedFetchJournal':b['indexedFetchJournal']}
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('INSERT INTO driver_binding VALUES(1,?)',(json.dumps(self.binding),));db.commit()
        self.initial=self.path.read_bytes();self.inode=self.path.stat().st_ino
        self.ledger=(self.state/'scratch.sqlite').read_bytes()

    def crash_writer(self):
        code='''import sqlite3,os,sys
c=sqlite3.connect(sys.argv[1]);c.execute('PRAGMA synchronous=FULL');c.execute('PRAGMA cache_size=5');c.execute('BEGIN IMMEDIATE')
for n in range(10000):c.execute('INSERT INTO readiness_tasks VALUES(?)',('dirty-'+str(n)+'x'*100,))
os._exit(77)
'''
        result=subprocess.run([sys.executable,'-c',code,str(self.path)],check=False)
        self.assertEqual(result.returncode,77)
        self.assertTrue(self.journal.exists())
        self.assertEqual(self.journal.read_bytes()[:8],recovery.MAGIC)
        self.assertNotEqual(self.path.read_bytes(),self.initial) # real page spills, not sidecar-only
        self.assertGreater(self.journal.stat().st_size,512)

    def run_recovery(self,config=None):
        with daemon.DaemonStateLock(self.state) as owner:
            return recovery.recover_indexed_driver(config or self.config,owner)

    def assert_restored(self):
        self.assertEqual(self.path.read_bytes(),self.initial)
        self.assertEqual(self.path.stat().st_ino,self.inode)
        self.assertFalse(self.journal.exists())
        self.assertEqual((self.state/'scratch.sqlite').read_bytes(),self.ledger)
        with daemon.DaemonStateLock(self.state) as owner:
            self.assertTrue(bootstrap.require_worker_input_profile(self.config,owner))
        capsule=self.state/recovery.DIRECTORY
        self.assertEqual(set(p.name for p in capsule.iterdir()),{'state.json'})

    def test_actual_spilled_transaction_rolls_back_before_startup_and_is_idempotent(self):
        self.crash_writer();self.assertTrue(self.run_recovery());self.assert_restored()
        self.assertFalse(self.run_recovery());self.assert_restored()
        # A second genuine crash uses the same bounded capsule, no history copies.
        self.crash_writer();self.assertTrue(self.run_recovery());self.assert_restored()

    def test_resume_after_each_durable_phase_preserves_original_and_clock(self):
        for phase in ('copying','validated','original-started','complete'):
            with self.subTest(phase=phase):
                self.crash_writer();write=recovery.state_write
                def stop(directory,value):
                    write(directory,value)
                    if value['phase']==phase:raise OSError('synthetic stop at durable boundary')
                with patch.object(recovery,'state_write',side_effect=stop):
                    with self.assertRaises(OSError):self.run_recovery()
                self.run_recovery();self.assert_restored()

    def test_wrong_bound_profile_is_rejected_on_copy_without_original_writes(self):
        bad=dict(self.binding,nodeId='foreign-node')
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('UPDATE driver_binding SET binding=?',(json.dumps(bad),));db.commit()
        self.initial=self.path.read_bytes();self.crash_writer()
        original=self.path.read_bytes();journal=self.journal.read_bytes()
        with self.assertRaises(ValueError):self.run_recovery()
        self.assertEqual(self.path.read_bytes(),original);self.assertEqual(self.journal.read_bytes(),journal)
        self.assertEqual((self.state/'scratch.sqlite').read_bytes(),self.ledger)

    def test_config_drift_and_missing_completion_refuse_before_capsule(self):
        self.crash_writer();before=self.path.read_bytes();journal=self.journal.read_bytes()
        changed=dict(self.config,workerInstanceId='foreign-instance')
        with self.assertRaises(ValueError):self.run_recovery(changed)
        (self.state/bootstrap.DIRECTORY/'completed.json').unlink()
        with self.assertRaises((ValueError,FileNotFoundError)):self.run_recovery()
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),journal)
        self.assertFalse((self.state/recovery.DIRECTORY).exists())

    def test_insufficient_capacity_holds_original_and_never_opens_copy(self):
        self.crash_writer();before=self.path.read_bytes();journal=self.journal.read_bytes()
        with patch.object(recovery.shutil,'disk_usage',return_value=type('Disk',(),{'free':0})()),patch.object(recovery,'recovered_validate',side_effect=AssertionError('no SQLite recovery')):
            with self.assertRaises(ValueError):self.run_recovery()
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),journal)

    def test_scratch_hot_journal_stays_held(self):
        self.crash_writer();before=self.path.read_bytes();journal=self.journal.read_bytes()
        (self.state/'scratch.sqlite-journal').write_bytes(b'held');(self.state/'scratch.sqlite-journal').chmod(0o600)
        with self.assertRaises(ValueError):self.run_recovery()
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),journal)

    def test_unknown_capsule_and_linked_journal_are_not_adopted(self):
        self.crash_writer();before=self.path.read_bytes();journal=self.journal.read_bytes()
        directory=self.state/recovery.DIRECTORY;directory.mkdir(mode=0o700);(directory/'unknown').write_bytes(b'retain')
        with self.assertRaises(ValueError):self.run_recovery()
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),journal)
        (directory/'unknown').unlink();directory.rmdir()
        os.link(self.journal,self.state/'journal-alias')
        with self.assertRaises(ValueError):self.run_recovery()
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),journal)

    def test_normal_profile_verifier_remains_readonly_and_held_until_recovery(self):
        self.crash_writer();before=self.path.read_bytes();journal=self.journal.read_bytes()
        with daemon.DaemonStateLock(self.state) as owner:
            with self.assertRaisesRegex(ValueError,'recovery_required'):
                bootstrap.require_worker_input_profile(self.config,owner)
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),journal)

    def test_factory_recovers_before_gate_engine_or_ledger_constructor(self):
        self.crash_writer()
        with patch.object(daemon,'NodeGate',side_effect=RuntimeError('stop before external gate')),patch.object(daemon,'ScratchLedger',side_effect=AssertionError('must not open ledger')),patch.object(daemon,'NativeEngine',side_effect=AssertionError('must not contact engine')):
            with self.assertRaisesRegex(RuntimeError,'stop before external gate'):daemon.build_worker(self.config)
        self.assert_restored()

    def test_factory_invalid_recovery_never_constructs_ledger_or_engine(self):
        self.crash_writer();before=self.path.read_bytes();journal=self.journal.read_bytes()
        config=dict(self.config,workerInstanceId='wrong')
        with patch.object(daemon,'NodeGate',side_effect=AssertionError('must not open gate')),patch.object(daemon,'ScratchLedger',side_effect=AssertionError('must not open ledger')),patch.object(daemon,'NativeEngine',side_effect=AssertionError('must not contact engine')):
            with self.assertRaises(ValueError):daemon.build_worker(config)
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),journal)

    def test_journal_replacement_after_copy_validation_is_held(self):
        self.crash_writer();write=recovery.state_write
        def stop(directory,value):
            write(directory,value)
            if value['phase']=='validated':raise OSError('stop after validation')
        with patch.object(recovery,'state_write',side_effect=stop):
            with self.assertRaises(OSError):self.run_recovery()
        raw=self.journal.read_bytes();self.journal.unlink();self.journal.write_bytes(raw);self.journal.chmod(0o600)
        before=self.path.read_bytes()
        with self.assertRaises(ValueError):self.run_recovery()
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),raw)

    def test_partial_copy_is_rebuilt_only_with_same_original_fingerprints(self):
        self.crash_writer();original=recovery.copied_file
        def stop(source,target,cap):
            if target.name=='copy.sqlite':
                target.write_bytes(b'partial');target.chmod(0o600);raise OSError('interrupted private copy')
            return original(source,target,cap)
        with patch.object(recovery,'copied_file',side_effect=stop):
            with self.assertRaises(OSError):self.run_recovery()
        self.assertTrue(self.run_recovery());self.assert_restored()

    def test_cold_or_super_journal_is_held_without_original_sqlite_open(self):
        self.crash_writer()
        with self.journal.open('r+b') as stream:stream.seek(0);stream.write(b'\0'*8)
        before=self.path.read_bytes();journal=self.journal.read_bytes()
        with self.assertRaises(ValueError):self.run_recovery()
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),journal)
        with self.journal.open('r+b') as stream:stream.seek(0);stream.write(recovery.MAGIC);stream.seek(0,2);stream.write(recovery.MAGIC)
        journal=self.journal.read_bytes()
        with self.assertRaises(ValueError):self.run_recovery()
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),journal)

    def test_capacity_preserves_retained_control_and_outstanding_bytes(self):
        # Read the real validated ledger, not constructor-induced clock updates.
        from processing.scratch_policy import ScratchLedger
        ledger=ScratchLedger(self.state/'scratch.sqlite',self.scratch,safety_floor_bytes=0,cleanup_grace_seconds=10,input_copy_accounting=True)
        # A real reservation supplies a physical outstanding hold.
        ledger.reserve(job_id='job',generation=1,reservation_id='hold',manifest_sha256='a'*64,input_bytes=6,output_budget=300,environment='staging',ttl_seconds=300,worker_ready=True)
        ledger.record_allocated('hold',6)
        ledger.prepare_control_charge('hold',charge_id='control',scope_sha256='e'*64,parent_binding_sha256='f'*64,expected_allocated=6,allowance_bytes=10,maximum_scope_charges=1)
        self.ledger=(self.state/'scratch.sqlite').read_bytes();self.crash_writer()
        intent=bootstrap._file(self.state/bootstrap.DIRECTORY/'intent.json')[0]
        directory=self.state/recovery.DIRECTORY;directory.mkdir(mode=0o700)
        with patch.object(recovery.shutil,'disk_usage',return_value=type('Disk',(),{'free':recovery.indexed.PHYSICAL_ALLOWANCE*2+recovery.CONTROL_ALLOWANCE+299})()):
            # Physical original DB/journal blocks reduce metadata hold. Select
            # an exact boundary from those actual blocks to isolate reservation.
            allocated=sum(p.stat().st_blocks*512 for p in (self.path,self.journal))
            free=recovery.indexed.PHYSICAL_ALLOWANCE*2+recovery.CONTROL_ALLOWANCE-allocated+299
            with patch.object(recovery.shutil,'disk_usage',return_value=type('Disk',(),{'free':free})()):
                with self.assertRaises(ValueError):recovery.capacity(self.config,intent,self.path,directory)
                self.assertEqual((self.state/'scratch.sqlite').read_bytes(),self.ledger)

    def test_committed_claim_and_clock_survive_uncommitted_attempt_cursor_advance(self):
        from processing import reference_indexed_fetch as indexed
        from tests.test_reference_indexed_fetch import IndexedJournalTests
        claim=IndexedJournalTests.claim(self,count=2);claim['nodeId']=self.config['nodeId']
        with daemon.DaemonStateLock(self.state) as owner:
            context=bootstrap.require_worker_indexed_context(self.config,owner)
            journal=indexed.IndexedFetchJournal(self.path,context,owner,clock=lambda:100.)
            with closing(sqlite3.connect(self.path)) as db:
                db.row_factory=sqlite3.Row
                db.execute('BEGIN IMMEDIATE');journal.reserve_slot(db,'job1')
                db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES('job1','request1','fetching','{}')")
                reservation=dict(reservationId='r1',generation=claim['generation'],expiresAt='1970-01-01T00:05:00Z',manifestSha256=claim['manifestSha256'],byteBudget=2,nonce='n1')
                grant={**reservation,'jobId':'job1','environment':claim['environment'],'objectIds':[o['id'] for o in claim['submission']['manifest']['objects']],'token':'synthetic-only'}
                projected={k:claim[k] for k in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken')}
                projected['transferRequest']=reservation
                journal.seal_claim(db,'job1','request1',claim);journal.seal_reservation(db,'job1',reservation);journal.seal_grant(db,'job1',grant)
                normalized_inputs=[dict(object_id=o['id'],byte_size=o['byteSize'],sha256=o['sha256'],relative_path='jobs/r1/'+o['id']+'.input') for o in claim['submission']['manifest']['objects']]
                import hashlib
                closure=hashlib.sha256(json.dumps(sorted(normalized_inputs,key=lambda o:o['object_id']),sort_keys=True,separators=(',',':')).encode()).hexdigest()
                identity=dict(jobId='job1',generation=claim['generation'],environment=claim['environment'],inputManifestSha256=claim['manifestSha256'],pipelineDigest=claim['pipelineDigest'],inputsSha256=closure)
                checkpoint=dict(schemaVersion=1,binding=identity,state='bound',next=0,byteCount=0)
                db.execute('UPDATE driver_jobs SET data=? WHERE job=\'job1\'',(json.dumps(dict(claim=projected,reservation=reservation,indexedGrant=True,inputAccounting=checkpoint)),))
                db.commit()
        self.initial=self.path.read_bytes()
        code='''import sqlite3,os,sys
c=sqlite3.connect(sys.argv[1]);c.execute('PRAGMA synchronous=FULL');c.execute('PRAGMA cache_size=5');c.execute('BEGIN IMMEDIATE')
c.execute("UPDATE indexed_fetch SET next=1,bytes=1,attempts=1,revision=2,clock=101 WHERE job='job1'")
c.execute("INSERT INTO indexed_attempts VALUES('job1',0,1,?,'confirmed')",('a'*32,))
for n in range(10000):c.execute('INSERT INTO readiness_tasks VALUES(?)',('spill-'+str(n)+'x'*100,))
os._exit(77)
'''
        self.assertEqual(subprocess.run([sys.executable,'-c',code,str(self.path)]).returncode,77)
        self.assertNotEqual(self.path.read_bytes(),self.initial)
        self.assertEqual(self.journal.read_bytes()[:8],recovery.MAGIC)
        self.assertTrue(self.run_recovery());self.assert_restored()
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT next,bytes,attempts,revision,clock FROM indexed_fetch').fetchone(),(0,0,0,0,100.))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_attempts').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_claims').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_seals').fetchone()[0],1)

    def test_first_atomic_state_write_interruption_and_partial_buffer_resume(self):
        self.crash_writer()
        with patch.object(recovery.os,'replace',side_effect=OSError('crash before first rename')):
            with self.assertRaises(OSError):self.run_recovery()
        directory=self.state/recovery.DIRECTORY
        self.assertEqual(set(p.name for p in directory.iterdir()),{'state.tmp'})
        (directory/'state.tmp').write_bytes(b'{partial')
        self.assertTrue(self.run_recovery());self.assert_restored()

    def test_corrupt_committed_cursor_cannot_authorize_original_recovery(self):
        self.test_committed_claim_and_clock_survive_uncommitted_attempt_cursor_advance()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('UPDATE indexed_fetch SET next=1,bytes=1');db.commit()
        self.initial=self.path.read_bytes();self.crash_writer()
        before=self.path.read_bytes();journal=self.journal.read_bytes()
        with self.assertRaises(ValueError):self.run_recovery()
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),journal)
        self.assertEqual((self.state/'scratch.sqlite').read_bytes(),self.ledger)

    def test_completed_capsule_readonly_census_never_runs_recovery(self):
        self.crash_writer();self.run_recovery();self.assert_restored()
        intent=bootstrap._file(self.state/bootstrap.DIRECTORY/'intent.json')[0]
        kwargs=dict(bootstrap_id=intent['bootstrapId'],binding_sha256=intent['bindingSha256'],
                    root_identity=bootstrap._identity(self.state.stat()),driver_identity=bootstrap._identity(self.path.stat()))
        before=self.path.read_bytes()
        with patch.object(recovery,'recovered_validate',side_effect=AssertionError('readonly census')):
            self.assertEqual(recovery.inspect_completed_recovery_capsule(self.state,**kwargs)['phase'],'complete')
            with self.assertRaises(ValueError):recovery.inspect_completed_recovery_capsule(self.state,**{**kwargs,'bootstrap_id':'foreign'})
        self.assertEqual(self.path.read_bytes(),before)

    def test_actual_sigkill_during_original_pager_rollback_resumes(self):
        import signal,time
        # Both the table and its primary index dirty many real pages, providing
        # an observable pager rollback interval without mocking SQLite IO.
        with closing(sqlite3.connect(self.path)) as db:
            db.executemany('INSERT INTO readiness_tasks VALUES(?)',(('clean-'+str(n).zfill(6)+'x'*940,) for n in range(12000)));db.commit()
        self.initial=self.path.read_bytes()
        code="import sqlite3,os,sys;c=sqlite3.connect(sys.argv[1]);c.execute('PRAGMA synchronous=FULL');c.execute('PRAGMA cache_size=5');c.execute('BEGIN IMMEDIATE');c.execute(\"UPDATE readiness_tasks SET uuid=replace(uuid,'clean-','dirty-')\");os._exit(77)"
        self.assertEqual(subprocess.run([sys.executable,'-c',code,str(self.path)]).returncode,77)
        dirty=self.path.read_bytes();self.assertNotEqual(dirty,self.initial)
        changed=[i for i in range(0,min(len(dirty),len(self.initial)),4096) if dirty[i:i+4096]!=self.initial[i:i+4096]]
        offsets=changed[::max(1,len(changed)//96)]
        self.assertGreater(len(offsets),20)
        script='''import json,sys
from pathlib import Path
from ops.reference_worker_daemon import DaemonStateLock
from processing import reference_indexed_recovery as r
config=json.loads(sys.argv[1]);original=r.recovered_validate
def barrier(path,*args):
 if path==Path(config['stateRoot'])/'driver.sqlite':
  print('ORIGINAL_READY',flush=True);sys.stdin.readline()
 return original(path,*args)
r.recovered_validate=barrier
with DaemonStateLock(config['stateRoot']) as owner:r.recover_indexed_driver(config,owner)
'''
        child=subprocess.Popen([sys.executable,'-c',script,json.dumps(self.config)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        self.addCleanup(lambda:child.kill() if child.poll() is None else None)
        self.assertEqual(child.stdout.readline().strip(),'ORIGINAL_READY')
        descriptor=os.open(self.path,os.O_RDONLY)
        try:
            child.stdin.write('\n');child.stdin.flush()
            deadline=time.monotonic()+5;observed=False
            while time.monotonic()<deadline and child.poll() is None:
                matched=sum(os.pread(descriptor,4096,i)==self.initial[i:i+4096] for i in offsets)
                if 0<matched<len(offsets) and self.journal.exists():
                    os.kill(child.pid,signal.SIGKILL);observed=True;break
            child.wait(timeout=5)
            self.assertTrue(observed,'must observe partially restored pages while journal still exists')
            self.assertEqual(child.returncode,-signal.SIGKILL)
        finally:
            os.close(descriptor);child.stdin.close();child.stdout.close();child.stderr.close()
        self.assertTrue(self.journal.exists())
        self.assertTrue(self.run_recovery());self.assert_restored()

    def test_real_pre_spill_zero_header_crash_remains_explicit_hold(self):
        code="import sqlite3,os,sys;c=sqlite3.connect(sys.argv[1]);c.execute('PRAGMA cache_size=10000');c.execute('BEGIN IMMEDIATE');c.execute(\"INSERT INTO readiness_tasks VALUES('unspilled')\");os._exit(77)"
        self.assertEqual(subprocess.run([sys.executable,'-c',code,str(self.path)]).returncode,77)
        self.assertEqual(self.path.read_bytes(),self.initial)
        self.assertTrue(self.journal.exists());self.assertEqual(self.journal.read_bytes()[:8],b'\0'*8)
        raw=self.journal.read_bytes()
        with self.assertRaises(ValueError):self.run_recovery()
        self.assertEqual(self.path.read_bytes(),self.initial);self.assertEqual(self.journal.read_bytes(),raw)

    def test_committed_object_grant_claim_and_quota_corruption_refuse_on_copy(self):
        from processing import reference_indexed_fetch as indexed
        for kind in ('object','grant','claim','quota'):
            with self.subTest(kind=kind):
                # Each iteration owns a fresh complete synthetic worker namespace.
                self.setUp();self.test_committed_claim_and_clock_survive_uncommitted_attempt_cursor_advance()
                table={'object':'indexed_objects','grant':'indexed_grants','claim':'indexed_claims','quota':'indexed_quota'}[kind]
                with closing(sqlite3.connect(self.path)) as db:
                    trigger=table+'_update';db.execute('DROP TRIGGER '+trigger)
                    if kind=='object':db.execute("UPDATE indexed_objects SET descriptor='{}' WHERE seq=0")
                    elif kind=='grant':
                        raw=indexed.encoded({'foreign':'grant'}).decode();db.execute('UPDATE indexed_grants SET record=?,sha=?',(raw,indexed.sha(raw.encode())))
                    elif kind=='claim':
                        raw=db.execute('SELECT envelope FROM indexed_claims').fetchone()[0];value=json.loads(raw);value['jobId']='foreign-job';raw=indexed.encoded(value).decode()
                        db.execute('UPDATE indexed_claims SET envelope=?,sha=?',(raw,indexed.sha(raw.encode())))
                    else:db.execute('UPDATE indexed_quota SET charge=1')
                    db.execute(next(sql for sql in indexed.TRIGGERS if sql.startswith('CREATE TRIGGER '+trigger+' ')));db.commit()
                self.initial=self.path.read_bytes();self.crash_writer();before=self.path.read_bytes();journal=self.journal.read_bytes()
                with self.assertRaises(ValueError):self.run_recovery()
                self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),journal)

    def test_completed_census_rejects_malformed_fingerprint_fields(self):
        self.crash_writer();self.run_recovery();directory=self.state/recovery.DIRECTORY
        state=recovery.state_read(directory);state['original']={}
        (directory/'state.json').write_bytes(recovery.indexed.encoded(state))
        intent=bootstrap._file(self.state/bootstrap.DIRECTORY/'intent.json')[0]
        with self.assertRaises(ValueError):recovery.inspect_completed_recovery_capsule(self.state,
            bootstrap_id=intent['bootstrapId'],binding_sha256=intent['bindingSha256'],
            root_identity=bootstrap._identity(self.state.stat()),driver_identity=bootstrap._identity(self.path.stat()))

    def test_closed_claim_projection_and_unknown_stage_refuse_before_original_writes(self):
        for corruption in ('submission','resultManifest','foreign-invalid-stage','combined_prepared_held'):
            with self.subTest(corruption=corruption):
                self.setUp();self.test_committed_claim_and_clock_survive_uncommitted_attempt_cursor_advance()
                with closing(sqlite3.connect(self.path)) as db:
                    if corruption in ('foreign-invalid-stage','combined_prepared_held'):
                        db.execute('UPDATE driver_jobs SET stage=?',(corruption,))
                    else:
                        small=json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0])
                        small['claim'][corruption]={'foreign':'unsealed-overlay'}
                        db.execute('UPDATE driver_jobs SET data=?',(json.dumps(small),))
                    db.commit()
                self.initial=self.path.read_bytes();self.crash_writer()
                before=self.path.read_bytes();rollback=self.journal.read_bytes()
                with self.assertRaises(ValueError):self.run_recovery()
                self.assertEqual(self.path.read_bytes(),before)
                self.assertEqual(self.journal.read_bytes(),rollback)
                self.assertEqual((self.state/'scratch.sqlite').read_bytes(),self.ledger)

    def test_all_actual_optional_claim_projection_keys_survive_recovery(self):
        from processing.reference_driver import _INDEXED_MUTABLE_CLAIM
        self.test_committed_claim_and_clock_survive_uncommitted_attempt_cursor_advance()
        with closing(sqlite3.connect(self.path)) as db:
            small=json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0])
            small['claim'].update(phase='fetching',status='running',error=None,
                transferGrantAcknowledgement=None,resultManifestSha256='d'*64,executionPhase='odm')
            self.assertTrue(set(_INDEXED_MUTABLE_CLAIM)<=set(small['claim']))
            db.execute('UPDATE driver_jobs SET data=?',(json.dumps(small),));db.commit()
        self.initial=self.path.read_bytes();self.crash_writer()
        self.assertTrue(self.run_recovery());self.assert_restored()

    def test_local_reservation_intent_without_public_transfer_is_a_legal_stage(self):
        from processing import reference_indexed_fetch as indexed
        self.test_committed_claim_and_clock_survive_uncommitted_attempt_cursor_advance()
        with closing(sqlite3.connect(self.path)) as db:
            small=json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0])
            small['claim'].pop('transferRequest');small.pop('indexedGrant')
            db.execute('DROP TRIGGER indexed_grants_delete');db.execute('DELETE FROM indexed_grants')
            db.execute(next(sql for sql in indexed.TRIGGERS if sql.startswith('CREATE TRIGGER indexed_grants_delete ')))
            db.execute('DELETE FROM indexed_fetch')
            db.execute("UPDATE driver_jobs SET stage='reservation_pending',data=?",(json.dumps(small),));db.commit()
        self.initial=self.path.read_bytes();self.crash_writer()
        self.assertTrue(self.run_recovery());self.assert_restored()

    def test_only_claim_pending_may_lack_the_sealed_claim(self):
        for stage in ('claim_pending','claimed','foreign-invalid-stage'):
            with self.subTest(stage=stage):
                self.setUp()
                with closing(sqlite3.connect(self.path)) as db:
                    db.execute('INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,?,?)',('pending','request',stage,'{}'));db.commit()
                self.initial=self.path.read_bytes();self.crash_writer()
                before=self.path.read_bytes();rollback=self.journal.read_bytes()
                if stage=='claim_pending':self.assertTrue(self.run_recovery());self.assert_restored()
                else:
                    with self.assertRaises(ValueError):self.run_recovery()
                    self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),rollback)
                    self.assertEqual((self.state/'scratch.sqlite').read_bytes(),self.ledger)

    def test_fetching_foreign_top_level_and_all_postfetch_incomplete_prefixes_refuse(self):
        for stage in ('foreign-top-level',*sorted(recovery.POST_FETCH_STAGES)):
            with self.subTest(stage=stage):
                self.setUp();self.test_committed_claim_and_clock_survive_uncommitted_attempt_cursor_advance()
                with closing(sqlite3.connect(self.path)) as db:
                    if stage=='foreign-top-level':
                        small=json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0]);small['foreign']='unexpected-hot-fetch-key'
                        db.execute('UPDATE driver_jobs SET data=?',(json.dumps(small),))
                    else:db.execute('UPDATE driver_jobs SET stage=?',(stage,))
                    db.commit()
                self.initial=self.path.read_bytes();self.crash_writer();before=self.path.read_bytes();rollback=self.journal.read_bytes()
                with self.assertRaises(ValueError):self.run_recovery()
                self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),rollback)
                self.assertEqual((self.state/'scratch.sqlite').read_bytes(),self.ledger)

    def complete_fetch_prefix(self):
        self.test_committed_claim_and_clock_survive_uncommitted_attempt_cursor_advance()
        with closing(sqlite3.connect(self.path)) as db:
            for seq in range(2):db.execute("INSERT INTO indexed_attempts VALUES('job1',?,1,?,'confirmed')",(seq,str(seq)*32))
            db.execute("UPDATE indexed_fetch SET next=2,bytes=2,attempts=2,revision=4,clock=100 WHERE job='job1'");db.commit()

    def test_fetching_at_count_and_postfetch_complete_prefix_windows_are_legal(self):
        for stage in ('fetching',*sorted(recovery.POST_FETCH_STAGES)):
            with self.subTest(stage=stage):
                self.setUp();self.complete_fetch_prefix()
                with closing(sqlite3.connect(self.path)) as db:
                    small=json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0])
                    if stage!='fetching':small['inputAccounting'].update(next=2,byteCount=2)
                    db.execute('UPDATE driver_jobs SET stage=?,data=?',(stage,json.dumps(small)));db.commit()
                self.initial=self.path.read_bytes();self.crash_writer();self.assertTrue(self.run_recovery());self.assert_restored()

    def test_fetching_checkpoint_uses_manifest_identity_and_anchored_zero_projection(self):
        for field,value in (('next',1),('byteCount',1),('binding',{'foreign':'identity'}),('schemaVersion',True),('state','binding_pending')):
            with self.subTest(field=field):
                self.setUp();self.test_committed_claim_and_clock_survive_uncommitted_attempt_cursor_advance()
                with closing(sqlite3.connect(self.path)) as db:
                    small=json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0]);small['inputAccounting'][field]=value
                    db.execute('UPDATE driver_jobs SET data=?',(json.dumps(small),));db.commit()
                self.initial=self.path.read_bytes();self.crash_writer();before=self.path.read_bytes();rollback=self.journal.read_bytes()
                with self.assertRaises(ValueError):self.run_recovery()
                self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.journal.read_bytes(),rollback)
                self.assertEqual((self.state/'scratch.sqlite').read_bytes(),self.ledger)
