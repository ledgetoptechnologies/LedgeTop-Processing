"""Synthetic runtime boundary, actual v2 bootstrap/SQLite/service and owned locks."""
from contextlib import closing
import hashlib
import os
from pathlib import Path
import sqlite3
import unittest
from ops import reference_worker_maintenance as m
from ops import reference_worker_indexed_maintenance as indexed
from ops import bootstrap_worker_input_accounting as bootstrap
from ops.reference_worker_daemon import DaemonStateLock
from ops.node_ingress import save
from tests import test_reference_worker_maintenance as fixture

@unittest.skipUnless(os.name=='posix','actual POSIX owner/service namespace')
class IndexedMaintenanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.WorkerMaintenanceTests.setUpClass()
        cls.addClassCleanup(fixture.WorkerMaintenanceTests.doClassCleanups)
    def setUp(self):
        self.f=fixture.WorkerMaintenanceTests();original=self.f.start_worker
        def start():
            self.f.config['inputCopyAccounting']={'schemaVersion':2,'fetchJournal':{'schemaVersion':1,'policyId':'indexed-worker-fetch-v1'}}
            if not self.f.worker_state.exists():
                save(self.f.config_path,self.f.config)
                bootstrap.bootstrap_input_copy_accounting(self.f.config)
            original()
        self.f.start_worker=start;self.addCleanup(self.f.doCleanups);self.f.setUp()
    def controller(self):
        controller=m.WorkerMaintenance(self.f.managed,self.f.policy,self.f.api,runner=self.f.runner)
        self.addCleanup(controller.close);return controller
    def stopped(self):
        c=self.controller();c.mounts();self.f.stop_worker()
        c._owner=DaemonStateLock(self.f.worker_state).__enter__()
        context=indexed.stopped_context(c);return c,context
    def snapshot(self):
        return {str(p.relative_to(self.f.worker_state)):hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.f.worker_state.rglob('*') if p.is_file()}
    def test_actual_empty_v2_owned_stop_update_restart(self):
        from dataclasses import replace,asdict
        # The actual production policy default is30s. A measured2.4–3.27s
        # CLI snapshot can consume the old fixture's entire3s retry window.
        # Only this positive fixture adopts the existing default; the real
        # busy-deadline/same-owner continuation test below keeps3s unchanged.
        default=m.WorkerPolicy.__dataclass_fields__['timeout_seconds'].default
        self.assertEqual(default,30)
        self.f.policy=replace(self.f.policy,timeout_seconds=default)
        profile=m.read_json(self.f.managed/'maintenance-worker-v2.json')
        profile['worker']=asdict(self.f.policy);save(self.f.managed/'maintenance-worker-v2.json',profile)
        result,folder=self.f.run_update()
        self.assertEqual(result['status'],'succeeded');self.assertEqual(self.f.events,['stop','backend','start','open'])
        self.assertEqual(self.f.queue.get_node('cluster')['state'],'active')
    def test_busy_deadline_retains_barrier_and_same_owner_retry_never_restarts_twice(self):
        from unittest.mock import patch
        from processing.node_gate import NodeGate
        captured=[];held=[];original_init=m.WorkerMaintenance.__init__;original_runner=self.f.runner
        old_guard=self.f.driver._step_guard
        self.assertTrue(old_guard.acquire(timeout=5))
        def capture(controller,*args,**kwargs):
            original_init(controller,*args,**kwargs);captured.append(controller)
        def runner(command,**kwargs):
            response=original_runner(command,**kwargs)
            if command[0]=='/usr/bin/docker' and command[3]=='start':
                self.assertTrue(self.f.driver._lock.acquire(timeout=5));held.append(self.f.driver._lock)
            return response
        def release_held():
            if held:held.pop().release()
        self.addCleanup(release_held);self.f.runner=runner
        try:
            with patch.object(m.WorkerMaintenance,'__init__',capture):
                with self.assertRaisesRegex(m.WorkerMaintenanceHeld,'^worker_not_quiescent$'):self.f.run_update()
        finally:old_guard.release()
        self.assertEqual(len(captured),1);controller=captured[0]
        folder=next(self.f.managed.glob('op-*'));state=m.read_json(folder/'managed-update.json')
        self.assertEqual(state['worker']['phase'],'start_pending');self.assertTrue(state['worker']['startIssued'])
        self.assertEqual(self.f.events.count('start'),1)
        self.assertEqual(m.read_json(self.f.ingress.root/'state.json')['state'],'closed')
        self.assertEqual(self.f.queue.get_node('cluster')['state'],'updating')
        release_held()
        # Synchronize the next real empty boundary; don't fabricate health or
        # waive busy. The listener still executes its actual SQL snapshot.
        guard=self.f.driver._step_guard;self.assertTrue(guard.acquire(timeout=5))
        try:
            with NodeGate('test-engine') as gate:
                self.assertIsNone(gate.snapshot()['reservation'])
                controller.after_backend(state,folder,gate,self.f.adapter)
            self.assertEqual(state['worker']['phase'],'verified')
            self.assertEqual(self.f.events.count('start'),1)
            self.assertEqual(self.f.queue.get_node('cluster')['state'],'updating')
            self.assertEqual(m.read_json(self.f.ingress.root/'state.json')['state'],'closed')
        finally:guard.release()

    def test_stopped_v2_census_and_history_are_read_only_and_normalized(self):
        c,ctx=self.stopped();before=self.snapshot()
        census=m.offline_census(self.f.worker_state,indexed_context=ctx)
        history=m.immutable_history(self.f.worker_state,indexed_context=ctx)
        self.assertIn('indexed_attempts',census['driver.sqlite'])
        self.assertEqual(history['driver.sqlite'],census['driver.sqlite']['__indexed_history__'])
        self.assertEqual(self.snapshot(),before)
    def test_context_cannot_keep_finished_controller_alive(self):
        import gc,weakref
        c,ctx=self.stopped();reference=weakref.ref(c);c.close()
        self._cleanups=[item for item in self._cleanups if item[0]!=c.close]
        del c;gc.collect();self.assertIsNone(reference())
        with self.assertRaises(indexed.IndexedMaintenanceHeld):
            indexed._check(ctx,self.f.worker_state,running=True)
    def test_saved_json_is_not_mapping_authority(self):
        c,ctx=self.stopped()
        with self.assertRaises(indexed.IndexedMaintenanceHeld):
            m.offline_census(self.f.worker_state,indexed_context={})
        indexed._PROOFS.pop(c)
        with self.assertRaisesRegex(indexed.IndexedMaintenanceHeld,'indexed_stopped_mapping_unproven'):
            indexed.stopped_context(c)
    def test_unknown_schema_and_sidecar_hold_without_repair(self):
        c,ctx=self.stopped()
        with closing(sqlite3.connect(self.f.worker_state/'driver.sqlite')) as db,db:db.execute('CREATE TABLE unexpected(value TEXT)')
        before=self.snapshot()
        with self.assertRaises(ValueError):m.offline_census(self.f.worker_state,indexed_context=ctx)
        self.assertEqual(self.snapshot(),before)
    def test_bootstrap_receipt_tamper_holds_without_repair(self):
        c,ctx=self.stopped();path=self.f.worker_state/'input-copy-bootstrap'/'completed.json'
        raw=path.read_bytes();path.write_bytes(raw.replace(b'"schemaVersion":2',b'"schemaVersion":1'))
        before=self.snapshot()
        with self.assertRaises(ValueError):m.offline_census(self.f.worker_state,indexed_context=ctx)
        self.assertEqual(self.snapshot(),before)
    def test_owner_loss_holds(self):
        c,ctx=self.stopped();c.close()
        with self.assertRaises(Exception):m.offline_census(self.f.worker_state,indexed_context=ctx)
    def test_orphan_history_in_bound_empty_profile_holds(self):
        import uuid
        c,ctx=self.stopped()
        with closing(sqlite3.connect(self.f.worker_state/'driver.sqlite')) as db,db:
            db.execute("INSERT INTO indexed_attempts VALUES('orphan',0,1,?,'started')",(uuid.uuid4().hex,))
        with self.assertRaisesRegex(ValueError,'indexed_orphan_record'):m.offline_census(self.f.worker_state,indexed_context=ctx)

    def test_unknown_rollback_sidecar_is_retained_and_held(self):
        c,ctx=self.stopped();path=self.f.worker_state/'driver.sqlite-journal';path.write_bytes(b'unknown');path.chmod(0o600)
        before=self.snapshot()
        with self.assertRaises(ValueError):m.offline_census(self.f.worker_state,indexed_context=ctx)
        self.assertEqual(self.snapshot(),before)
    def test_actual_completed_recovery_capsule_is_censused_read_only(self):
        from tests.test_reference_indexed_recovery import IndexedRecoveryTests
        from processing import reference_indexed_recovery as recovery
        c,ctx=self.stopped();self.path=self.f.worker_state/'driver.sqlite';self.journal=Path(str(self.path)+'-journal')
        self.initial=self.path.read_bytes();self.inode=self.path.stat().st_ino
        IndexedRecoveryTests.crash_writer(self)
        self.assertTrue(recovery.recover_indexed_driver(self.f.config,c._owner))
        before=self.snapshot();census=m.offline_census(self.f.worker_state,indexed_context=ctx)
        history=m.immutable_history(self.f.worker_state,indexed_context=ctx)
        self.assertEqual(history['driver.sqlite'],census['driver.sqlite']['__indexed_history__'])
        self.assertEqual(history['driver.sqlite']['bootstrap']['recovery']['phase'],'complete')
        self.assertEqual(before,self.snapshot())
        extra=self.f.worker_state/'indexed-driver-recovery'/'copy.sqlite';extra.write_bytes(b'leftover');extra.chmod(0o600)
        with self.assertRaises(ValueError):m.offline_census(self.f.worker_state,indexed_context=ctx)

    def test_pending_recovery_capsule_holds(self):
        c,ctx=self.stopped();path=self.f.worker_state/'indexed-driver-recovery';path.mkdir(mode=0o700)
        before=self.snapshot()
        with self.assertRaises((ValueError,FileNotFoundError,ImportError)):m.offline_census(self.f.worker_state,indexed_context=ctx)
        self.assertEqual(self.snapshot(),before)
    def test_missing_bootstrap_receipt_holds(self):
        c,ctx=self.stopped();(self.f.worker_state/'input-copy-bootstrap'/'indexed-created.json').unlink()
        before=self.snapshot()
        with self.assertRaises(ValueError):m.offline_census(self.f.worker_state,indexed_context=ctx)
        self.assertEqual(self.snapshot(),before)
    def test_changed_config_binding_cannot_reuse_mapping(self):
        c,ctx=self.stopped();config=self.f.config.copy();config['outputBudget']+=1;save(self.f.config_path,config)
        with self.assertRaises(ValueError):m.offline_census(self.f.worker_state,indexed_context=ctx)
    def test_unowned_version_or_config_inode_change_invalidates_proof(self):
        c,ctx=self.stopped();config=self.f.config.copy();config['engineVersion']='unowned-version';save(self.f.config_path,config)
        with self.assertRaisesRegex(ValueError,'indexed_configuration_identity_changed'):
            m.offline_census(self.f.worker_state,indexed_context=ctx)
    def test_changed_mounted_root_inode_holds(self):
        c,ctx=self.stopped();old=self.f.scratch;renamed=old.with_name(old.name+'-retained');old.rename(renamed);old.mkdir(mode=0o700)
        with self.assertRaisesRegex(ValueError,'indexed_mount_identity_changed'):
            m.offline_census(self.f.worker_state,indexed_context=ctx)

    def test_attempt_hash_bound_accepts_10010_and_rejects_10011(self):
        # Actual SQLite rows; this unit proves only hashing's explicit bound,
        # not valid attempt closure. inspect_driver separately audits closure.
        with closing(sqlite3.connect(':memory:')) as db:
            db.row_factory=sqlite3.Row;db.execute('CREATE TABLE indexed_attempts(id INTEGER PRIMARY KEY,value TEXT)')
            db.executemany('INSERT INTO indexed_attempts VALUES(?,?)',((i,'x') for i in range(10010)))
            first=indexed._hash_rows(db,'indexed_attempts',10010)
            db.execute("UPDATE indexed_attempts SET value='y' WHERE id=10009")
            self.assertNotEqual(first,indexed._hash_rows(db,'indexed_attempts',10010))
            db.execute("INSERT INTO indexed_attempts VALUES(10010,'z')")
            with self.assertRaises(indexed.IndexedMaintenanceHeld):indexed._hash_rows(db,'indexed_attempts',10010)

@unittest.skipUnless(os.name=='posix','actual POSIX journal owner')
class IndexedTerminalAuditTests(unittest.TestCase):
    def setUp(self):
        from tests.test_reference_driver_indexed_fetch import IndexedDriverTests
        self.t=IndexedDriverTests();self.addCleanup(self.t.doCleanups);self.t.setUp();self.f=self.t.f
        self.job=self.f.submit();self.f.advance(self.job,'awaiting_acceptance')
        result=self.f.store.get(self.job['jobId'],private=True)['resultManifest']
        receipt=dict(schemaVersion=1,receiptId='success-viewer',storageReceiptId='success-storage',jobId=self.job['jobId'],generation=1,environment='staging',
            inputManifestSha256=self.job['manifestSha256'],pipelineDigest=self.job['pipelineDigest'],resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=[o['id'] for o in result['objects']],acceptedAt='2026-10-27T22:00:00Z',validationVersion='ltds-reference-v1')
        self.f.store.accept(self.job['jobId'],receipt)
        self.f.driver._indexed_background_due=0;self.assertEqual(self.f.driver.step()['state'],'completed')
    def audit(self):
        # Existing fixtures have different bootstrap provenance; test this one
        # logical terminal audit directly with its actual immutable context.
        import json
        ro=indexed._ReadOnlyJournal(dict(bootstrapId=self.t.context.bootstrap_id,binding=json.loads(self.t.context.binding_json)),self.f.root,self.t.path)
        with closing(sqlite3.connect(self.t.path.as_uri()+'?mode=ro&immutable=1',uri=True)) as driver,closing(sqlite3.connect(self.f.ledger.path.as_uri()+'?mode=ro&immutable=1',uri=True)) as scratch:
            driver.row_factory=scratch.row_factory=sqlite3.Row
            row=driver.execute('SELECT * FROM driver_jobs').fetchone()
            indexed.audit_terminal_job(driver,ro,row,scratch)
    def test_real_completed_claim_scratch_and_acceptance_audit(self):self.audit()
    def test_original_request_tamper_holds(self):
        with self.f.driver._db() as db:db.execute("UPDATE driver_jobs SET claim_request='changed'")
        with self.assertRaisesRegex(ValueError,'indexed_claim_request_changed'):self.audit()
    def change_grant(self,change):
        import json
        from processing import reference_indexed_fetch as journal
        with closing(sqlite3.connect(self.t.path)) as db,db:
            value=json.loads(db.execute('SELECT record FROM indexed_grants').fetchone()[0]);value=change(value)
            raw=journal.encoded(value).decode()
            db.execute('DROP TRIGGER indexed_grants_update')
            db.execute('UPDATE indexed_grants SET record=?,sha=?',(raw,journal.sha(raw.encode())))
            db.execute(next(s for s in journal.TRIGGERS if s.startswith('CREATE TRIGGER indexed_grants_update ')))
    def test_foreign_canonical_grant_with_recomputed_digest_holds(self):
        self.change_grant(lambda g:{'foreign':'grant'})
        with self.assertRaises(ValueError):self.audit()
    def test_historical_grant_wrong_identity_budget_and_closure_hold(self):
        import copy,json
        with self.f.driver._db() as db:original=json.loads(db.execute('SELECT record FROM indexed_grants').fetchone()[0])
        for key,value in [('jobId','foreign-job'),('environment','production'),('generation',2),('byteBudget',0),('nonce','foreign-nonce'),('objectIds',['foreign-object'])]:
            with self.subTest(field=key):
                changed=copy.deepcopy(original);changed[key]=value;self.change_grant(lambda g:changed)
                with self.assertRaises(ValueError):self.audit()
        self.change_grant(lambda g:original);self.audit()
    def test_historical_grant_expired_now_is_valid_at_persisted_highwater(self):
        from unittest.mock import patch
        with patch('time.time',return_value=10**10):self.audit()
    def test_grant_already_expired_at_persisted_fetch_highwater_holds(self):
        import json
        from datetime import datetime
        with self.f.driver._db() as db:
            grant=json.loads(db.execute('SELECT record FROM indexed_grants').fetchone()[0])
            expiry=datetime.fromisoformat(grant['expiresAt'].replace('Z','+00:00')).timestamp()
            db.execute('UPDATE indexed_fetch SET clock=?',(expiry,))
        with self.assertRaisesRegex(ValueError,'expired'):self.audit()
    def test_completed_partial_fetched_history_is_explicitly_held(self):
        with self.f.driver._db() as db:db.execute('UPDATE indexed_fetch SET next=0,bytes=0')
        with self.assertRaises(ValueError):self.audit()
    def test_injected_submission_cannot_replace_sealed_original(self):
        import json
        with self.f.driver._db() as db:
            raw=json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0]);raw['claim']['submission']={'forged':True}
            db.execute('UPDATE driver_jobs SET data=?',(json.dumps(raw),))
        with self.assertRaisesRegex(ValueError,'indexed_maintenance_projection_changed'):self.audit()
    def test_input_progress_projection_drift_holds(self):
        import json
        with self.f.driver._db() as db:
            raw=json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0]);raw['inputAccounting']['byteCount']+=1
            db.execute('UPDATE driver_jobs SET data=?',(json.dumps(raw),))
        with self.assertRaisesRegex(ValueError,'indexed_input_projection_changed'):self.audit()
    def test_missing_scratch_copy_holds(self):
        with self.f.ledger._transaction() as db:db.execute('DELETE FROM input_copies')
        with self.assertRaisesRegex(ValueError,'indexed_scratch_closure_changed'):self.audit()
    def test_unaccepted_result_holds(self):
        with self.f.ledger._transaction() as db:db.execute('UPDATE objects SET accepted=NULL')
        with self.assertRaisesRegex(ValueError,'indexed_viewer_acceptance_missing'):self.audit()
    def test_unowned_future_attempt_holds(self):
        import uuid
        with self.f.driver._db() as db:db.execute("INSERT INTO indexed_attempts VALUES(?,1,1,?,'started')",(self.job['jobId'],uuid.uuid4().hex))
        with self.assertRaises(ValueError):self.audit()

if __name__=='__main__':unittest.main()
