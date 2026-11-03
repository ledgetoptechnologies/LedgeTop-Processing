"""Real SQLite/gate/reader/outbox migrations; Docker transport only is synthetic."""
import copy
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import uuid

from ops.reference_worker_daemon import DaemonStateLock
from processing import reference_protocol as p
from processing.node_gate import NodeGate
from processing.reference_engine_worker import WorkerHeld,ReferenceEngineWorker
import stat
from processing.reference_readers import ReaderSession
from processing.reference_results import ReferenceResults,ResultUnavailable,ResultAuthorization
from processing.reference_terminal import TerminalService
from processing.treedar_delivery import TreeDarDelivery
from processing.treedar_engine import TreeDarEngine
from processing.treedar_runtime import TreeDarRuntimeManager,runtime_descriptor
from tests import test_treedar_delivery as delivery_fixture
from tests.test_treedar_engine import FakeDocker
from tests.viewer_receipt_signer import SyntheticViewerSigner


class FrozenV1Delivery(ReferenceEngineWorker):
    # Verbatim constructor from c439b703ca7b8be21a162b9a67062eeb803862c3.
    # This old code has no runtime-manager or pointer awareness whatsoever.
    def __init__(self, path, *, controller, outbox, failure_outbox):
        self.controller = controller
        self.ledger, self.outbox, self.failure_outbox = controller.ledger, outbox, failure_outbox
        config = controller.config
        self.engine = controller
        self.engine_id, self.node_id, self.worker_id = config.engine_id, config.node_id, config.worker_id
        self.authority_id, self.origin = config.authority_id, config.coordinator_origin
        self.gate_generation, self.version = config.gate_generation, config.engine_version
        self.path = Path(path).absolute()
        self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        if any(item.is_symlink() for item in (self.path, *self.path.parents)):
            raise WorkerHeld('owned_private_delivery_journal_required')
        parent = self.path.parent.stat()
        if os.name != 'nt' and (parent.st_uid != os.geteuid() or parent.st_mode & 0o077):
            raise WorkerHeld('private_delivery_journal_permissions_required')
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | getattr(os,'O_NOFOLLOW',0), 0o600)
        try:
            info, current = os.fstat(descriptor), self.path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or (info.st_dev,info.st_ino) != (current.st_dev,current.st_ino)
                    or (os.name != 'nt' and info.st_uid != os.geteuid())):
                raise WorkerHeld('owned_delivery_journal_required')
            if os.name != 'nt':
                os.fchmod(descriptor,0o600)
        finally:
            os.close(descriptor)
        with self._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS executions (
                job TEXT PRIMARY KEY, identity TEXT NOT NULL, receipt TEXT NOT NULL,
                phase TEXT NOT NULL, result TEXT, outbox_id TEXT,
                proof TEXT NOT NULL, outcome TEXT NOT NULL,
                failure_json TEXT, failure_outbox_id TEXT)''')
            db.execute('CREATE TABLE IF NOT EXISTS delivery_binding (id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)')
            binding = p.canonical_json(dict(controllerPath=str(controller.path),nodeId=self.node_id,
                workerId=self.worker_id,engineId=self.engine_id,authorityId=self.authority_id,
                origin=self.origin,gateGeneration=self.gate_generation,imageId=config.image_id,
                sourceSha256=config.source_sha256,engineVersion=self.version,
                ledgerPath=str(self.ledger.path),outboxPath=str(outbox.path),failureOutboxPath=str(failure_outbox.path))).decode()
            old = db.execute('SELECT value FROM delivery_binding WHERE id=1').fetchone()
            if old and old[0] != binding:
                raise WorkerHeld('delivery_controller_binding_changed')
            db.execute('INSERT OR IGNORE INTO delivery_binding VALUES(1,?)',(binding,))


@unittest.skipUnless(sys.platform.startswith('linux'),'Linux owner/read leases and physical gate required')
class TreeDarRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.f=delivery_fixture.TreeDarDeliveryTests(methodName='runTest')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.path=self.f.delivery.path
        c=self.f.config
        self.physical=dict(nodeId=c.node_id,workerId=c.worker_id,engineId=c.engine_id,
            authorityId=c.authority_id,origin=c.coordinator_origin,gateGeneration=c.gate_generation)
        self.old_runtime=runtime_descriptor(self.f.controller,self.path)
        self.owner=DaemonStateLock(self.path.parent).__enter__()
        self.addCleanup(lambda:self.owner.__exit__(None,None,None))
        self.reader=ReaderSession(self.f.ledger).__enter__()
        self.addCleanup(self.reader.close)
        self.manager=self.open_manager()
        self.config=replace(c,engine_version='0.0.2',image_id='sha256:'+'c'*64)
        self.docker=FakeDocker(self.config)
        self.candidate=TreeDarEngine(self.manager.controller_path(self.config),ledger=self.f.ledger,
            config=self.config,verify_provenance=lambda _:True,runner=self.docker,clock=lambda:self.f.fixture.now)

    def open_manager(self,**kwargs):
        return TreeDarRuntimeManager(self.path,ledger=self.f.ledger,outbox=self.f.outbox,
            failure_outbox=self.f.failures,physical_binding=self.physical,allow_legacy_v1=True,**kwargs)

    def finish(self,failed=False):
        if failed:
            self.f.docker.state,self.f.docker.exit_code='exited',1
        else:
            self.f.success()
        self.f.delivery.poll(self.f.job['jobId'])
        receiver=self.f.failure_receiver if failed else self.f.receiver
        receipt=receiver.receive(self.f.claim_delivery(failed=failed),**self.f.identity)
        self.f.delivery.acknowledge_receipt(self.f.job['jobId'],receipt)
        return receipt

    def census(self,db,current):
        # Production callback additionally verifies complete driver/router SQL;
        # these tests isolate manager closure against real durable components.
        jobs={}
        for row in db.execute('SELECT * FROM executions'):
            binding=json.loads(row['identity'])['job']
            jobs[row['job']]=dict(binding=binding,runtime=self.old_runtime,
                stage='awaiting_acceptance' if row['outcome']=='succeeded' else 'awaiting_failure_acceptance',
                outboxId=row['outbox_id'],failureOutboxId=row['failure_outbox_id'])
        return dict(schemaVersion=1,jobs=jobs,driverSha256=p.digest(jobs),routerSha256=p.digest(jobs))

    def activate(self,**kwargs):
        snapshot=self.manager.snapshot()
        with NodeGate(self.config.engine_id) as gate:
            return self.manager.activate_runtime(self.candidate,expected_epoch=snapshot['epochId'],
                expected_revision=snapshot['revision'],owner=self.owner,reader_session=self.reader,
                gate=gate,census=kwargs.pop('census',self.census),**kwargs)

    def delivery(self,controller=None):
        return TreeDarDelivery(self.path,controller=controller,outbox=self.f.outbox,
            failure_outbox=self.f.failures,runtime_manager=self.manager)

    def test_legacy_success_upgrades_pending_acceptance_reads_without_old_executable(self):
        self.finish()
        before=self.f.delivery._row(self.f.job['jobId'])
        published=self.activate()
        self.assertEqual(published['revision'],1)
        self.assertEqual(published['runtime']['engineVersion'],'0.0.2')
        # Old installed source and Docker are unavailable. Retained data reads
        # must not inspect either one; the historical controller DB is evidence.
        self.f.fixture.package.rename(self.f.root/'old-source-unavailable')
        self.f.docker.fail='image'
        retained=self.delivery().retained_view(self.f.job['jobId'],self.old_runtime)
        with patch.object(self.f.controller,'poll',side_effect=AssertionError('old controller used')):
            self.assertEqual(retained.poll(self.f.job['jobId']),before['outbox_id'])
            retained.acknowledge_delivery(self.f.job['jobId'])
            metadata=ReferenceResults(retained).metadata(self.f.job['jobId'])
            self.assertEqual(metadata['resultManifestSha256'],json.loads(before['result'])['resultManifestSha256'])
            obj=metadata['objects'][0]
            auth=ResultAuthorization(node_id='main',worker_id=self.f.config.worker_id,job_id=self.f.job['jobId'],
                generation=1,environment='staging',result_manifest_sha256=metadata['resultManifestSha256'],
                object_ids=(obj['id'],),share_approved=True)
            stream=ReferenceResults(retained,reader_session=self.reader).open(self.f.job['jobId'],generation=1,
                object_id=obj['id'],result_manifest_sha256=metadata['resultManifestSha256'],authorization=auth)
            self.assertEqual(b''.join(stream.chunks()),b'synthetic output')
            result=json.loads(before['result'])
            coverage=retained.apply_viewer_acceptance(self.f.job['jobId'],[
                self.f.acceptance(result,[obj['id'] for obj in result['objects']])])
            self.assertTrue(coverage['fullyAccepted'])
            # Metadata coverage alone does not establish a verified Viewer copy.
            self.assertEqual(self.f.ledger.cleanup_candidates(),[])
            # This legacy synthetic TreeDar result is not a Viewer v2 closure;
            # even a correctly signed invented receipt must not release it.
            signer=SyntheticViewerSigner();retained.viewer_receipt_keys=signer.trusted_keys
            acceptance=self.f.acceptance(result,[obj['id'] for obj in result['objects']])
            with self.assertRaises(WorkerHeld):retained.apply_viewer_acceptance(self.f.job['jobId'],[signer.envelope(acceptance,result)])
        self.assertEqual(self.f.ledger.cleanup_candidates(),[])
        self.assertEqual(self.manager.runtime_for_job(self.f.job['jobId'])[0],self.old_runtime)
        self.assertTrue(self.f.controller.path.exists())
        self.assertFalse(any(call[0] in ('create','start','stop','rm') for call in self.docker.calls))

    def test_failure_pending_viewer_ack_migrates_and_metadata_replays_without_controller(self):
        receipt=self.finish(failed=True)
        self.activate()
        retained=self.delivery().retained_view(self.f.job['jobId'],self.old_runtime)
        self.assertEqual(retained.completion(self.f.job['jobId'])['kind'],'failure')
        retained.acknowledge_receipt(self.f.job['jobId'],receipt)
        self.assertEqual(self.f.failure_receiver.get(self.f.job['jobId'],1)['deliveryState'],
            'awaiting_viewer_failure_acceptance')
        with self.assertRaises(ResultUnavailable):
            ReferenceResults(retained).metadata(self.f.job['jobId'])
        self.assertTrue(self.f.input.exists())
        service=TerminalService(self.f.store)
        failure=service.outcome(self.f.job['jobId'],'staging')
        acceptance=dict(schemaVersion=1,receiptId='terminal-viewer',storageReceiptId='terminal-storage',
            **{key:failure[key] for key in ('jobId','generation','environment','inputManifestSha256','pipelineDigest','failureSha256')},
            acceptedAt='2026-10-28T10:00:00Z',validationVersion=p.VALIDATION_VERSION)
        self.assertTrue(service.accept(self.f.job['jobId'],acceptance,'staging')['terminalAccepted'])
        retained.acknowledge_receipt(self.f.job['jobId'],receipt)

    def test_new_job_once_then_changed_runtime_cannot_resume_old_job(self):
        self.finish();self.activate()
        delivery=self.delivery(self.candidate)
        job=copy.deepcopy(self.f.job)
        job['jobId']=str(uuid.uuid4())
        reservation='new-reservation'
        self.f.ledger.reserve(job_id=job['jobId'],generation=1,reservation_id=reservation,
            manifest_sha256=job['manifestSha256'],input_bytes=3,output_budget=1024**2,
            environment='staging',ttl_seconds=1000,worker_ready=True)
        folder=self.f.root/'jobs'/reservation
        folder.mkdir(mode=0o700);(folder/'cloud.input').write_bytes(b'abc')
        self.f.ledger.record_allocated(reservation,3)
        provenance=dict(self.f.provenance,jobId=job['jobId'])
        for _ in range(2):
            delivery.start(job,reservation_id=reservation,input_paths={'lidar1':'jobs/'+reservation+'/cloud.input'},
                provenance=provenance,allow_synthetic=True)
        self.assertEqual(sum(call[0]=='start' for call in self.docker.calls),1)
        self.assertEqual(self.manager.runtime_for_job(job['jobId'])[0],delivery.runtime_descriptor())
        with self.assertRaisesRegex(WorkerHeld,'job_requires_retired'):
            delivery._active_controller(self.f.job['jobId'])
        old=self.delivery(self.f.controller)
        self.assertEqual(old.runtime_descriptor(),self.old_runtime)
        with self.assertRaisesRegex(WorkerHeld,'active_treedar_runtime_changed'):
            old.start(job,reservation_id=reservation,input_paths={'lidar1':'jobs/'+reservation+'/cloud.input'},
                provenance=provenance,allow_synthetic=True)
        with self.assertRaisesRegex(WorkerHeld,'managed_treedar_runtime_required'):
            self.f.delivery.poll(self.f.job['jobId'])
        # An ordinary same-runtime restart does not activate or run an idle
        # census: exact active descriptor+per-job pins permit safe intent poll.
        restarted=TreeDarEngine(self.candidate.path,ledger=self.f.ledger,config=self.config,
            verify_provenance=lambda _:True,runner=self.docker,clock=lambda:self.f.fixture.now)
        self.manager=self.open_manager()
        self.manager.assert_active(runtime_descriptor(restarted,self.path))
        restarted_delivery=self.delivery(restarted)
        self.assertIsNone(restarted_delivery.poll(job['jobId']))
        restarted_delivery.start(job,reservation_id=reservation,input_paths={'lidar1':'jobs/'+reservation+'/cloud.input'},
            provenance=provenance,allow_synthetic=True)
        self.assertEqual(sum(call[0]=='start' for call in self.docker.calls),1)

    def test_pending_engine_or_metadata_ack_or_local_checkpoint_blocks(self):
        with self.assertRaisesRegex(WorkerHeld,'idle_gate'):
            self.activate()
        self.f.success();identifier=self.f.delivery.poll(self.f.job['jobId'])
        receipt=self.f.receiver.receive(self.f.claim_delivery(),**self.f.identity)
        # Even a coordinator-persisted ACK isn't locally durable yet.
        with self.assertRaises(WorkerHeld):self.activate()
        with patch.object(self.f.delivery,'_phase',side_effect=OSError('phase gap')):
            with self.assertRaises(OSError):self.f.delivery.acknowledge_receipt(self.f.job['jobId'],receipt)
        with self.assertRaisesRegex(WorkerHeld,'historical_terminal_evidence_changed'):
            self.activate()
        self.f.delivery.poll(self.f.job['jobId'])
        self.activate()
        self.assertEqual(self.f.outbox.get(identifier)['state'],'acknowledged')

    def test_census_missing_job_wrong_runtime_or_unknown_controller_blocks(self):
        self.finish()
        original=self.manager.snapshot()
        for field in ('missing','runtime','stage'):
            def bad(db,current):
                value=self.census(db,current)
                if field=='missing':value['jobs']={}
                elif field=='runtime':value['jobs'][self.f.job['jobId']]['runtime']=dict(self.old_runtime,imageId='sha256:'+'d'*64)
                else:value['jobs'][self.f.job['jobId']]['stage']='processing'
                return value
            with self.subTest(field=field),self.assertRaises(WorkerHeld):self.activate(census=bad)
            self.assertEqual(self.manager.snapshot(),original)
        with self.f.controller._db() as db:
            db.execute("UPDATE treedar_executions SET phase='start_pending'")
        with self.assertRaisesRegex(WorkerHeld,'unsettled_historical_controller_intent'):
            self.activate()

    def test_before_commit_fault_rolls_back_epoch_and_legacy_pins(self):
        self.finish();original=self.manager.snapshot()
        with patch.object(self.manager,'_commit',side_effect=OSError('before commit')):
            with self.assertRaises(OSError):self.activate()
        self.assertEqual(self.manager.snapshot(),original)
        with self.manager._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM treedar_job_runtimes').fetchone()[0],0)
        self.activate()

    def test_frozen_v1_constructor_accepts_before_but_refuses_after_atomic_upgrade(self):
        self.finish()
        def frozen():
            return FrozenV1Delivery(self.path,controller=self.f.controller,outbox=self.f.outbox,failure_outbox=self.f.failures)
        frozen()  # Actual historical code, no new-code runtime checks.
        with self.f.delivery._db() as db:
            original=db.execute('SELECT value FROM delivery_binding WHERE id=1').fetchone()[0]
        snapshot=self.manager.snapshot()
        with patch.object(self.manager,'_commit',side_effect=OSError('before publish')):
            with NodeGate(self.config.engine_id) as gate:
                with self.assertRaises(OSError):
                    self.manager.activate_runtime(self.candidate,expected_epoch=snapshot['epochId'],expected_revision=0,
                        owner=self.owner,reader_session=self.reader,gate=gate,census=self.census)
        frozen()
        self.activate()
        with self.assertRaisesRegex(WorkerHeld,'delivery_controller_binding_changed'):frozen()
        with self.manager._db() as db:
            self.assertEqual(db.execute('SELECT value FROM treedar_legacy_binding_evidence WHERE id=1').fetchone()[0],original)
            marker=json.loads(db.execute('SELECT value FROM delivery_binding WHERE id=1').fetchone()[0])
            self.assertEqual(marker,dict(json.loads(original),runtimeManagementVersion=1))
        self.open_manager().assert_active(runtime_descriptor(self.candidate,self.path))

    def test_fresh_managed_publish_also_fences_old_constructor_and_records_absence(self):
        self.finish()
        path=self.path.parent/'fresh-managed.sqlite'
        manager=TreeDarRuntimeManager(path,ledger=self.f.ledger,outbox=self.f.outbox,
            failure_outbox=self.f.failures,physical_binding=self.physical)
        TreeDarDelivery(path,outbox=self.f.outbox,failure_outbox=self.f.failures,runtime_manager=manager)
        empty=lambda db,current:dict(schemaVersion=1,jobs={},driverSha256=p.digest({}),routerSha256=p.digest({}))
        with NodeGate(self.config.engine_id) as gate:
            manager.activate_runtime(self.candidate,expected_epoch=None,expected_revision=0,owner=self.owner,
                reader_session=self.reader,gate=gate,census=empty)
        with self.assertRaisesRegex(WorkerHeld,'delivery_controller_binding_changed'):
            FrozenV1Delivery(path,controller=self.f.controller,outbox=self.f.outbox,failure_outbox=self.f.failures)
        with manager._db() as db:
            self.assertEqual(db.execute('SELECT value FROM treedar_legacy_binding_evidence WHERE id=1').fetchone()[0],'null')
        reopened=TreeDarRuntimeManager(path,ledger=self.f.ledger,outbox=self.f.outbox,
            failure_outbox=self.f.failures,physical_binding=self.physical)
        self.assertEqual(reopened.snapshot(),manager.snapshot())
        with manager._db() as db:db.execute('DELETE FROM treedar_runtime_active')
        with self.assertRaisesRegex(WorkerHeld,'legacy_runtime_management_fence_changed'):
            TreeDarRuntimeManager(path,ledger=self.f.ledger,outbox=self.f.outbox,
                failure_outbox=self.f.failures,physical_binding=self.physical)

    def test_unregistered_candidate_intent_blocks_without_physical_mutation(self):
        self.finish();old=self.manager.snapshot()
        with self.candidate._db() as db:
            db.execute("INSERT INTO treedar_executions VALUES(?,?,?, ?,NULL,'create_pending',NULL,NULL)",
                (str(uuid.uuid4()),'{}','unregistered','ltds-unregistered'))
        with self.assertRaisesRegex(WorkerHeld,'unregistered_candidate_controller_intent'):
            self.activate()
        self.assertEqual(self.manager.snapshot(),old)
        self.assertFalse(any(call[0] in ('create','start','stop','rm') for call in self.docker.calls))

    def test_changed_ledger_objects_or_durable_ack_prevents_publication(self):
        self.finish()
        with self.f.ledger._transaction() as db:
            db.execute("UPDATE objects SET sha=? WHERE reservation=?",('f'*64,self.f.reservation))
        with self.assertRaisesRegex(WorkerHeld,'historical_result_objects_changed'):
            self.activate()

    def test_census_and_retained_reads_do_not_write_old_journals_or_clock(self):
        self.finish()
        before={path:path.read_bytes() for path in (self.f.controller.path,self.f.outbox.path,self.f.failures.path,self.f.ledger.path)}
        self.activate()
        self.delivery().retained_view(self.f.job['jobId'],self.old_runtime)
        self.assertEqual(before,{path:path.read_bytes() for path in before})

    def test_after_commit_uncertainty_resolves_exact_pointer_then_stale_cas_refused(self):
        self.finish();old=self.manager.snapshot()
        original=self.manager._commit
        def lost(db):original(db);raise OSError('lost commit response')
        with patch.object(self.manager,'_commit',side_effect=lost):
            # Pass expected values directly; snapshot itself also commits.
            with NodeGate(self.config.engine_id) as gate:
                result=self.manager.activate_runtime(self.candidate,expected_epoch=old['epochId'],expected_revision=0,
                    owner=self.owner,reader_session=self.reader,gate=gate,census=self.census)
        self.assertEqual(result,self.manager.snapshot())
        with NodeGate(self.config.engine_id) as gate,self.assertRaisesRegex(WorkerHeld,'compare_and_swap'):
            self.manager.activate_runtime(self.candidate,expected_epoch=old['epochId'],expected_revision=0,
                owner=self.owner,reader_session=self.reader,gate=gate,census=self.census)

    def test_wrong_or_lost_capabilities_and_changed_physical_binding_refuse(self):
        self.finish()
        with NodeGate(self.config.engine_id) as gate:
            with self.assertRaisesRegex(WorkerHeld,'exclusive_runtime_owner'):
                old=self.manager.snapshot()
                self.manager.activate_runtime(self.candidate,expected_epoch=old['epochId'],expected_revision=0,
                    owner=object(),reader_session=self.reader,gate=gate,census=self.census)
        binding=dict(self.physical,workerId='wrong-worker')
        with self.assertRaisesRegex(WorkerHeld,'physical_binding_changed'):
            TreeDarRuntimeManager(self.path,ledger=self.f.ledger,outbox=self.f.outbox,
                failure_outbox=self.f.failures,physical_binding=binding,allow_legacy_v1=True)
        with patch.object(self.reader,'check',side_effect=ValueError('lease lost')):
            with self.assertRaises(ValueError):self.activate()

    def test_changed_retained_pin_or_result_fails_without_old_controller(self):
        self.finish();self.activate()
        data=self.delivery()
        with self.assertRaisesRegex(WorkerHeld,'retained_treedar_runtime_changed'):
            data.retained_view(self.f.job['jobId'],dict(self.old_runtime,engineVersion='wrong'))
        with data._db() as db:
            row=db.execute('SELECT result FROM executions').fetchone()
            result=json.loads(row[0]);result['engineVersions']['treedar']='0.0.2'
            result['resultManifestSha256']=p.digest({key:value for key,value in result.items() if key!='resultManifestSha256'})
            db.execute('UPDATE executions SET result=?',(p.canonical_json(result).decode(),))
        with self.assertRaisesRegex(WorkerHeld,'sealed_delivery_runtime_changed'):
            data.retained_view(self.f.job['jobId'],self.old_runtime)

    def test_active_pointer_without_managed_marker_or_evidence_is_rejected(self):
        self.finish();self.activate()
        with self.manager._db() as db:
            original=db.execute('SELECT value FROM treedar_legacy_binding_evidence WHERE id=1').fetchone()[0]
            managed=db.execute('SELECT value FROM delivery_binding WHERE id=1').fetchone()[0]
            db.execute('UPDATE delivery_binding SET value=?',(original,))
        with self.assertRaisesRegex(WorkerHeld,'legacy_runtime_management_fence_changed'):
            self.manager.assert_active(runtime_descriptor(self.candidate,self.path))
        with self.assertRaisesRegex(WorkerHeld,'legacy_runtime_management_fence_changed'):
            self.open_manager()
        with self.manager._db() as db:db.execute('UPDATE delivery_binding SET value=?',(managed,))
        with self.manager._db() as db:db.execute('DELETE FROM treedar_legacy_binding_evidence')
        with self.assertRaisesRegex(WorkerHeld,'legacy_runtime_management_fence_changed'):
            self.open_manager()

    def test_legacy_default_constructor_and_explicit_backfill_optin_remain_strict(self):
        self.finish()
        with self.assertRaisesRegex(WorkerHeld,'delivery_controller_binding_changed'):
            TreeDarDelivery(self.path,controller=self.candidate,outbox=self.f.outbox,failure_outbox=self.f.failures)
        denied=TreeDarRuntimeManager(self.path,ledger=self.f.ledger,outbox=self.f.outbox,
            failure_outbox=self.f.failures,physical_binding=self.physical)
        self.assertIsNone(denied.snapshot())
        with NodeGate(self.config.engine_id) as gate,self.assertRaisesRegex(WorkerHeld,'explicit_legacy_runtime_adoption_required'):
            denied.activate_runtime(self.candidate,expected_epoch=None,expected_revision=0,
                owner=self.owner,reader_session=self.reader,gate=gate,census=self.census)


if __name__=='__main__':unittest.main()
