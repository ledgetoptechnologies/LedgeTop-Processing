"""Authenticated cleanup only in owned temporary synthetic Linux fixtures."""
import base64
import copy
import hashlib
import json
import os
import subprocess
from pathlib import Path
import sys
import threading
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from processing import reference_protocol as p
from processing.maintenance import Conflict
from processing.reference_api import ReferenceAPI
from processing.reference_source_readers import SourceReaderError
from processing.source_release import SourceReleaseService,ReleaseSettings,MINIMUM_NS
from processing import source_release_contract as wire
from tests import test_reference_acceptance_observed as observation_fixture
from tests.test_reference_v2_store_acceptance import VIEWER_BEARER


def crash_release(directory,boundary):
    """One real child owns its original leases and exits inside synthetic I/O."""
    from types import SimpleNamespace
    from processing.source_release_filesystem import SourceReleaseFilesystem
    temporary=SimpleNamespace(name=directory,cleanup=lambda:None)
    with patch('tests.test_reference_acceptance_observed.tempfile.TemporaryDirectory',return_value=temporary):
        case=SourceReleaseBackendTests('runTest');case.setUp()
    case.ready_for_physical_phase()
    state=dict(config=case.config,request=case.request,principal=case.principal,spool=str(case.spool.root))
    path=Path(directory)/'resume-synthetic.json';path.write_bytes(p.canonical_json(state));path.chmod(0o600)
    def advance():
        for _ in range(len(case.auth['eligibleObjects'])+4):case.service.run_once()
    if boundary in ('before_rename','after_rename'):
        original=SourceReleaseFilesystem._rename_owned
        def rename(instance,*args):
            if boundary=='before_rename':os._exit(42)
            original(instance,*args);os._exit(42)
        with patch.object(SourceReleaseFilesystem,'_rename_owned',autospec=True,side_effect=rename):advance()
    elif boundary=='after_unlink':
        original=os.unlink
        def unlink(name,*args,**kwargs):
            result=original(name,*args,**kwargs)
            if kwargs.get('dir_fd') is not None and str(name).endswith('.blob'):os._exit(42)
            return result
        with patch('processing.source_release_filesystem.os.unlink',side_effect=unlink):advance()
    elif boundary=='after_removed_commit':
        original=SourceReleaseFilesystem.remove_one
        def removed(instance,*args,**kwargs):
            original(instance,*args,**kwargs);os._exit(42)
        with patch.object(SourceReleaseFilesystem,'remove_one',autospec=True,side_effect=removed):advance()
    elif boundary=='after_unlinking_phase_before_intent':
        original=SourceReleaseFilesystem.remove_one
        def phase_gap(instance,auth,obj,row,persist,effect,**kwargs):
            def capture(phase,physical):
                persist(phase,physical)
                if phase=='unlinking':os._exit(42)
            return original(instance,auth,obj,row,capture,effect,**kwargs)
        with patch.object(SourceReleaseFilesystem,'remove_one',autospec=True,side_effect=phase_gap):advance()
    elif boundary=='after_terminal_commit':
        for _ in range(len(case.auth['eligibleObjects'])+3):
            case.service.run_once()
            receipt=case.lookup()['receipt']
            if receipt:
                (Path(directory)/'expected-receipt.json').write_bytes(p.canonical_json(receipt))
                os._exit(42)
    raise AssertionError('synthetic crash boundary was not reached')


def stopped_service_release(directory):
    """Real service/SIGTERM lifecycle; only its NAS rename is synthetic-blocked."""
    import signal
    from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
    from types import SimpleNamespace
    from processing.app import CoordinatorService
    from processing.source_release_filesystem import SourceReleaseFilesystem
    temporary=SimpleNamespace(name=directory,cleanup=lambda:None)
    with patch('tests.test_reference_acceptance_observed.tempfile.TemporaryDirectory',return_value=temporary):
        case=SourceReleaseBackendTests('runTest');case.setUp()
    case.ready_for_physical_phase()
    with case.queue._db() as db:db.execute('UPDATE source_releases SET clock_ns=?',(time.monotonic_ns(),))
    runtime,_=case._runtime();stop=threading.Event();root=Path(directory)
    signal.signal(signal.SIGTERM,lambda *_:stop.set())
    class Handler(BaseHTTPRequestHandler):
        viewer_service=None
        reference_listener=runtime
        prototype_queue=case.queue
    native=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    original=SourceReleaseFilesystem._rename_owned
    def blocked(instance,*args):
        (root/'blocked.json').write_bytes(p.canonical_json(dict(
            mainLease=str(case.queue._lock.name),sourceLease=str(case.store.source_readers().lock_path))))
        while not (root/'allow-rename').exists():time.sleep(0.01)
        return original(instance,*args)
    drain=runtime.drain_source_release
    def draining():
        (root/'draining').write_text('source shutdown barrier')
        drain()
    runtime.drain_source_release=draining
    with patch.object(SourceReleaseFilesystem,'_rename_owned',autospec=True,side_effect=blocked):
        outcome=CoordinatorService(native,Handler).run(stop)
    if outcome!=0:raise AssertionError('actual service failed clean shutdown')
    if runtime._source_release_thread is not None or runtime._source_timer_thread is not None:
        raise AssertionError('source threads were not joined')
    if not case.queue._lock.closed or not runtime._source_readers._closed:
        raise AssertionError('original leases not closed after drain')
    case.tearDown();case.doCleanups()
    (root/'clean-exit').write_text('joined and released')


@unittest.skipUnless(sys.platform.startswith('linux'),'real Linux nofollow/noreplace/process lease')
class SourceReleaseBackendTests(unittest.TestCase):
    def setUp(self):
        self.case=observation_fixture.AcceptanceObservationStoreTests('runTest');self.case.setUp()
        self.queue,self.store,self.spool=self.case.queue,self.case.store,self.case.state['spool']
        observed=self.case.post();self.assertEqual(observed.status,200,observed.body)
        self.observed=observed.body
        self.key=Ed25519PrivateKey.generate()
        with self.queue._db() as db:row=self.store._row(db,self.case.job['jobId'])
        proof=json.loads(row['source_attestation_json'])['proof'];self.principal=proof['principalId']
        qualification=dict(mountId=self.spool.mount_id,renameNoReplace=True,directoryFsync=True)
        for prefix,identity in (('root',self.spool._root_identity),('mount',self.spool._mount_identity),('marker',self.spool._marker_identity)):
            qualification[prefix+'Device']=str(identity[0]);qualification[prefix+'Inode']=str(identity[1])
        self.config=dict(enabled=True,trustedKeys=[dict(environment='staging',issuerId='separate-release-issuer',
            keyId='separate-release-key',principalId=self.principal,publicKeyPem=self.key.public_key().public_bytes(
                serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode('ascii'))],
            principalBindings=[dict(environment='staging',principalId=self.principal,
                bearerSha256=hashlib.sha256(VIEWER_BEARER.encode()).hexdigest())],filesystemQualification=qualification)
        self.settings=ReleaseSettings(self.config,viewer_bearers={VIEWER_BEARER:'staging'},spool=self.spool)
        self.clock=[1_000_000_000]
        self.service=SourceReleaseService(self.store,self.spool,self.settings,monotonic_ns=lambda:self.clock[0])
        self.auth=dict(schemaVersion=1,protocol=wire.PROTOCOL,releaseId=str(uuid.uuid4()),issuerId='separate-release-issuer',
            keyId='separate-release-key',principalId=self.principal,coordinatorAuthorityId=self.queue.authority_id,
            jobId=row['job_id'],generation=row['generation'],environment=row['environment'],
            viewerTaskId=row['viewer_task_id'],providerTaskId=row['provider_task_id'],viewerAttemptId=proof['viewerAttemptId'],
            activationId=proof['activationId'],sourceSnapshotId=str(uuid.uuid4()),preparedEnvelopeSha256=proof['preparedEnvelopeSha256'],
            sourceSnapshotSha256=proof['sourceSnapshotSha256'],inputManifestSha256=row['manifest_sha'],pipelineDigest=row['pipeline_digest'],
            resultManifestSha256=json.loads(row['result_json'])['resultManifestSha256'],sourceManifestSha256=row['source_manifest_sha'],
            derivedManifestSha256=row['derived_manifest_sha'],sourceReceiptSha256=row['source_receipt_sha'],
            derivedReceiptSha256=row['derived_receipt_sha'],workerObservationId=self.observed['observationId'],
            workerObservationAckSha256=self.observed['ackSha256'],receipts=self.case.intent['receipts'],
            readyProof=dict(kind='verified_outputs_ready',attemptId=proof['viewerAttemptId'],modelId=str(uuid.uuid4()),
                modelVersionId=str(uuid.uuid4()),outputInventorySha256='f'*64,verifiedAt='2026-11-01T00:00:00.000Z'),
            policy=copy.deepcopy(wire.POLICY),eligibleObjects=wire.closure(json.loads(row['source_manifest_json']),json.loads(row['derived_manifest_json'])))
        self.auth['eligibleClosureSha256']=p.digest(self.auth['eligibleObjects'])
        # Separate owned synthetic Viewer originals; the release executor never
        # receives this root. This is not production Viewer/NAS qualification.
        originals=Path(self.case.temporary.name)/'viewer-originals';originals.mkdir(mode=0o700)
        self.originals={}
        for obj in json.loads(row['source_manifest_json'])['objects']:
            body=(self.spool.root/self.spool.storage_key(row['job_id'],1,obj['id'])).read_bytes()
            target=originals/obj['id'];target.write_bytes(body);target.chmod(0o600)
            self.originals[target]=body
        self.api=ReferenceAPI(self.store,bearer_tokens={VIEWER_BEARER:'staging'},capability_fingerprint='a'*64,
            options_fingerprint='a'*64,source_release_service=self.service)
        self.request=self.sign(self.auth)

    def tearDown(self):self.case.tearDown()

    def sign(self,auth):
        message=wire.DOMAIN+p.canonical_json(auth)
        return dict(authorization=copy.deepcopy(auth),signature=base64.urlsafe_b64encode(self.key.sign(message)).decode().rstrip('='),authorizationSha256=hashlib.sha256(message).hexdigest())

    def post(self,request=None):
        return self.api.handle('POST',f"/ltds/v2/jobs/{self.auth['jobId']}/source/releases",
            {'Authorization':'Bearer '+VIEWER_BEARER},p.canonical_json(request or self.request),protected_transport=True)

    def lookup(self):return self.service.lookup(self.auth['jobId'],self.auth['releaseId'],principal=self.principal)

    def ready_for_physical_phase(self):
        self.assertEqual(self.post().status,201)
        # Physical-mechanics tests explicitly seed the journal's elapsed policy.
        # A separate timer test qualifies the real 24-hour monotonic progression.
        with self.queue._db() as db:db.execute('UPDATE source_releases SET remaining_ns=1')
        self.clock[0]+=1

    def finish(self):
        for _ in range(len(self.auth['eligibleObjects'])+3):self.service.run_once()
        return self.lookup()

    def test_signed_owner_closure_is_required_and_replay_is_stable(self):
        self.assertEqual(self.post().status,201)
        initial=self.lookup()
        self.assertEqual(initial['state'],'waiting_policy')
        self.store.max_metadata_bytes=1
        self.assertEqual(self.post().status,200)
        self.assertEqual(self.lookup(),initial)
        changed=copy.deepcopy(self.auth);changed['readyProof']['modelId']=str(uuid.uuid4())
        self.assertEqual(self.post(self.sign(changed)).status,409)
        self.assertIsNone(self.service.lookup(self.auth['jobId'],self.auth['releaseId'],principal='other-principal'))

    def test_unapproved_wrong_owner_and_permanent_closures_do_not_create_intents(self):
        for field in ('principalId','workerObservationAckSha256','sourceSnapshotSha256','inputManifestSha256'):
            auth=copy.deepcopy(self.auth);auth[field]='other-principal' if field=='principalId' else '0'*64
            with self.subTest(field=field):self.assertNotIn(self.post(self.sign(auth)).status,(200,201))
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT count(*) FROM source_releases').fetchone()[0],0)
        auth=copy.deepcopy(self.auth);auth['eligibleObjects']=[];auth['eligibleClosureSha256']=p.digest([])
        self.assertEqual(self.post(self.sign(auth)).status,409)

        with self.queue._db() as db:
            row=self.store._row(db,self.auth['jobId'])
            permanent=wire.closure(json.loads(row['source_manifest_json']),json.loads(row['derived_manifest_json']),retained=True)
        auth=copy.deepcopy(self.auth)
        auth['eligibleObjects']=sorted(auth['eligibleObjects']+permanent,key=lambda obj:(obj['objectKind'],obj['id']))
        auth['eligibleClosureSha256']=p.digest(auth['eligibleObjects'])
        self.assertEqual(self.post(self.sign(auth)).status,409)

    def test_failed_cancelled_and_full_metadata_quota_never_authorize_source_deletion(self):
        from processing.source_release import reservation_bytes
        from processing.source_release_filesystem import SourceReleaseFilesystem
        for state in ('failed','cancelled','held'):
            with self.subTest(state=state),self.queue._db() as db:
                db.execute('UPDATE jobs SET status=? WHERE id=?',(state,self.auth['jobId']))
            self.assertEqual(self.post().status,409)
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='succeeded' WHERE id=?",(self.auth['jobId'],))
            row=self.store._row(db,self.auth['jobId'])
            preserved=wire.closure(json.loads(row['source_manifest_json']),json.loads(row['derived_manifest_json']),retained=True)
            used=self.store._metadata_used_bytes(db)
        self.store.max_metadata_bytes=used+reservation_bytes(self.request,preserved)-1
        with patch.object(SourceReleaseFilesystem,'remove_one') as remove:
            self.assertEqual(self.post().status,409);remove.assert_not_called()
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT count(*) FROM source_releases').fetchone()[0],0)
        for obj in self.auth['eligibleObjects']:
            self.assertTrue((self.spool.root/self.spool.storage_key(self.auth['jobId'],1,obj['id'])).is_file())

    def test_full_policy_never_credits_wall_forward_large_gaps_or_restart_epoch(self):
        self.assertEqual(self.post().status,201)
        self.clock[0]+=MINIMUM_NS
        self.assertFalse(self.service.run_once())
        self.assertEqual(self.lookup()['remainingMinimumAgeSeconds'],86400)
        with self.queue._db() as db:db.execute("UPDATE source_releases SET clock_epoch='0000000000000000000000000000000000000000000000000000000000000000'")
        self.clock[0]+=300*1_000_000_000
        self.assertFalse(self.service.run_once())
        self.assertEqual(self.lookup()['remainingMinimumAgeSeconds'],86400)
        for _ in range(287):
            self.clock[0]+=300*1_000_000_000
            self.service.run_once()
        self.assertEqual(self.lookup()['remainingMinimumAgeSeconds'],300)
        for obj in self.auth['eligibleObjects']:
            self.assertTrue((self.spool.root/self.spool.storage_key(self.auth['jobId'],1,obj['id'])).is_file())
        self.clock[0]+=300*1_000_000_000
        self.service.run_once()
        self.assertEqual(self.lookup()['remainingMinimumAgeSeconds'],0)

    def test_exact_physical_cleanup_keeps_raw_gcp_and_every_result_receipt(self):
        self.ready_for_physical_phase()
        with self.queue._db() as db:
            result_before=db.execute('SELECT result_json FROM reference_jobs').fetchone()[0]
            receipt_count=db.execute('SELECT count(*) FROM reference_viewer_receipts').fetchone()[0]
            preserved=json.loads(db.execute('SELECT preserved_json FROM source_releases').fetchone()[0])
        state=self.finish()
        self.assertEqual(state['state'],'released',state)
        for obj in self.auth['eligibleObjects']:
            self.assertFalse((self.spool.root/self.spool.storage_key(self.auth['jobId'],1,obj['id'])).exists())
        self.assertTrue(preserved)
        for obj in preserved:self.assertTrue((self.spool.root/self.spool.storage_key(self.auth['jobId'],1,obj['id'])).is_file())
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT result_json FROM reference_jobs').fetchone()[0],result_before)
            self.assertEqual(db.execute('SELECT count(*) FROM reference_viewer_receipts').fetchone()[0],receipt_count)
        self.assertEqual(self.post().status,200)
        self.assertEqual(self.lookup(),state)
        self.assertEqual({path:path.read_bytes() for path in self.originals},self.originals)

    def test_custody_hold_and_changed_qualification_stop_before_effects(self):
        self.ready_for_physical_phase()
        self.store._hold_source_intake(self.auth['jobId'])
        self.service.run_once()
        self.assertEqual(self.lookup()['state'],'held')
        self.assertEqual(self.lookup()['holdReason'],'custody_held')
        for obj in self.auth['eligibleObjects']:
            self.assertTrue((self.spool.root/self.spool.storage_key(self.auth['jobId'],1,obj['id'])).is_file())

    def _assert_blocked_io(self,kind):
        from processing.source_release_filesystem import SourceReleaseFilesystem
        from processing.coordinator_freeze import CoordinatorFrozen
        self.ready_for_physical_phase()
        self.service.run_once()  # Seal the one initial inode/namespace inventory.
        entered,resume,wrote=threading.Event(),threading.Event(),threading.Event()
        errors=[]
        if kind=='rename':
            target=patch.object(SourceReleaseFilesystem,'_rename_owned')
            original=SourceReleaseFilesystem._rename_owned
        else:
            target=patch('processing.source_release_filesystem.os.'+kind)
            original=getattr(os,kind)
        def pause(*args,**kwargs):
            entered.set()
            if not resume.wait(5):raise RuntimeError('test filesystem barrier timed out')
            return original(*args,**kwargs)
        def run():
            try:self.service.run_once()
            except Exception as error:errors.append(error)
        def unrelated_write():
            with self.queue._db() as db:
                db.execute('CREATE TABLE IF NOT EXISTS release_concurrency_probe(value INTEGER)')
                db.execute('INSERT INTO release_concurrency_probe VALUES(1)')
            wrote.set()
        # autospec supplies the real FS instance to the injected class method.
        if kind=='rename':target=patch.object(SourceReleaseFilesystem,'_rename_owned',autospec=True,side_effect=pause)
        else:target=patch('processing.source_release_filesystem.os.'+kind,side_effect=pause)
        with target:
            thread=threading.Thread(target=run);thread.start()
            try:
                self.assertTrue(entered.wait(3))
                writer=threading.Thread(target=unrelated_write);writer.start()
                self.assertTrue(wrote.wait(2),'global queue DB stalled behind NAS I/O')
                writer.join(2);self.assertFalse(writer.is_alive())
                with self.assertRaises((SourceReaderError,CoordinatorFrozen)):self.queue.close()
                self.assertGreater(self.store.source_readers().active_count(self.auth['jobId'],1),0)
            finally:
                resume.set();thread.join(6)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors,[])
        self.assertEqual(self.store.source_readers().active_count(self.auth['jobId'],1),0)

    def test_blocked_rename_does_not_hold_global_queue(self):self._assert_blocked_io('rename')
    def test_blocked_unlink_does_not_hold_global_queue(self):self._assert_blocked_io('unlink')
    def test_blocked_directory_fsync_does_not_hold_global_queue(self):self._assert_blocked_io('fsync')

    def test_batch_stops_next_effect_on_freeze_intent_and_retires_actual_pins(self):
        from processing.source_release_filesystem import SourceReleaseFilesystem
        self.ready_for_physical_phase();self.service.run_once()
        gate=self.queue._coordinator_freeze;plan=str(uuid.uuid4())
        def change(action,**kwargs):
            return gate.transition(dict(requestId=str(uuid.uuid4()),planId=plan,action=action,
                expectedRevision=gate.status()['revision'],startupId=self.queue.startup_id),
                owner_secret='synthetic-release-freeze',**kwargs)
        original=SourceReleaseFilesystem._rename_owned
        def race(instance,*args):
            change('intent');return original(instance,*args)
        with patch.object(SourceReleaseFilesystem,'_rename_owned',autospec=True,side_effect=race),patch('processing.source_release_filesystem.os.unlink') as unlink:
            self.service.run_io_batch();unlink.assert_not_called()
        self.assertEqual(gate.status()['operations'],0)
        self.assertEqual(self.store.source_readers().active_count(self.auth['jobId'],1),0)
        with self.queue._db() as db:
            rows=db.execute('SELECT phase,physical_json FROM source_release_objects ORDER BY object_kind,object_id').fetchall()
            self.assertEqual(rows[0]['phase'],'quarantining')
            self.assertEqual(json.loads(rows[0]['physical_json'])['effectIntent']['outcome'],'completed')
            self.assertTrue(all(row['phase']=='listed' for row in rows[1:]))
        change('drain');self.assertEqual(change('seal')['phase'],'frozen')
        change('resume',healthy=True)
        self.service.run_io_batch()
        self.assertEqual(self.lookup()['state'],'released')

    def test_freeze_between_status_read_and_new_effect_transaction_stops_rename(self):
        from processing.source_release_filesystem import SourceReleaseFilesystem
        self.ready_for_physical_phase();self.service.run_once()
        gate=self.queue._coordinator_freeze;original=gate.status;plan=str(uuid.uuid4());raced=[]
        def status():
            result=original();caller=sys._getframe(1)
            if caller.f_code.co_name=='effect' and caller.f_locals.get('kind')=='rename' and not raced:
                raced.append(True)
                gate.transition(dict(requestId=str(uuid.uuid4()),planId=plan,action='intent',
                    expectedRevision=result['revision'],startupId=self.queue.startup_id),owner_secret='synthetic-race-owner')
            return result  # Deliberately return the stale open snapshot.
        with patch.object(gate,'status',new=status),\
                patch.object(SourceReleaseFilesystem,'_rename_owned') as rename,\
                patch('processing.source_release_filesystem.os.unlink') as unlink:
            self.service.run_io_batch();rename.assert_not_called();unlink.assert_not_called()
        self.assertEqual(raced,[True]);self.assertEqual(original()['operations'],0)
        with self.queue._db() as db:
            rows=db.execute('SELECT phase,physical_json FROM source_release_objects ORDER BY object_kind,object_id').fetchall()
            self.assertEqual(rows[0]['phase'],'quarantining')
            self.assertNotIn('effectIntent',json.loads(rows[0]['physical_json']))
        for obj in self.auth['eligibleObjects']:
            self.assertTrue((self.spool.root/self.spool.storage_key(self.auth['jobId'],1,obj['id'])).exists())

    def test_actual_service_sigterm_retains_process_and_both_leases_until_io_drains(self):
        import fcntl
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            child=subprocess.Popen([sys.executable,'-B','-c',
                'from tests.test_source_release_backend import stopped_service_release; import sys; stopped_service_release(sys.argv[1])',
                directory],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                deadline=time.monotonic()+10
                while not (root/'blocked.json').exists() and child.poll() is None and time.monotonic()<deadline:time.sleep(0.01)
                self.assertTrue((root/'blocked.json').exists(),'child never reached owned synthetic rename')
                leases=json.loads((root/'blocked.json').read_text())
                child.terminate()  # SIGTERM uses the real service stop Event.
                deadline=time.monotonic()+3
                while not (root/'draining').exists() and child.poll() is None and time.monotonic()<deadline:time.sleep(0.01)
                self.assertTrue((root/'draining').exists())
                time.sleep(1.3)  # Exceed the old bounded close/refusal boundary.
                self.assertIsNone(child.poll(),'service abandoned its active source I/O')
                for lease in leases.values():
                    with open(lease,'a+b') as competitor:
                        with self.assertRaises(BlockingIOError):fcntl.flock(competitor,fcntl.LOCK_EX|fcntl.LOCK_NB)
                (root/'allow-rename').write_text('unblock owned synthetic I/O')
                stdout,stderr=child.communicate(timeout=10)
                self.assertEqual(child.returncode,0,stderr)
                self.assertTrue((root/'clean-exit').exists())
                for lease in leases.values():
                    with open(lease,'a+b') as competitor:
                        fcntl.flock(competitor,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(competitor,fcntl.LOCK_UN)
            finally:
                (root/'allow-rename').touch()
                if child.poll() is None:
                    try:child.communicate(timeout=10)
                    except subprocess.TimeoutExpired:child.kill();child.communicate(timeout=5)

    def test_batch_reuses_one_real_operation_and_bounded_release_pin_journal(self):
        self.ready_for_physical_phase();self.service.run_once()
        gate=self.queue._coordinator_freeze
        with patch.object(gate,'_quota',wraps=gate._quota) as admission:
            self.service.run_io_batch()
            self.assertEqual(admission.call_count,1)
        self.assertEqual(self.lookup()['state'],'released')
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM coordinator_operation_pins').fetchone()[0],0)
            self.assertEqual(db.execute("SELECT count(*) FROM source_reader_pins WHERE operation='source-release'").fetchone()[0],1)

    def test_reusable_release_pin_never_overwrites_uncertain_live_handle(self):
        self.ready_for_physical_phase();self.service.run_once()
        readers=self.store.source_readers()
        with self.queue.coordinator_operation('source-release'):
            handle=readers.reserve_release(self.auth['jobId'],1,self.auth['releaseId'],authorize=lambda db:None)
            try:
                # A stale durable released marker cannot override this process's
                # still-owned uncertain outcome; no second handle is exposed.
                with self.queue._db() as db:
                    db.execute("UPDATE source_reader_pins SET state='released' WHERE reference=?",(handle.reference,))
                handle.uncertain=True
                with self.assertRaisesRegex(SourceReaderError,'source_operation_busy'):
                    readers.reserve_release(self.auth['jobId'],1,self.auth['releaseId'],authorize=lambda db:None)
                self.assertIs(readers._handles[handle.reference],handle)
            finally:
                # Test-only resolution: no descriptor or physical effect existed.
                handle.uncertain=False;handle.close()

    def test_hold_after_rename_intent_blocks_unlink_and_retains_exact_journal(self):
        from processing.source_release_filesystem import SourceReleaseFilesystem
        self.ready_for_physical_phase()
        self.service.run_once()
        original=SourceReleaseFilesystem._rename_owned
        def race(instance,*args):
            # Commit after the per-effect authority intent, while FS is outside DB.
            self.store._hold_source_intake(self.auth['jobId'])
            return original(instance,*args)
        with patch.object(SourceReleaseFilesystem,'_rename_owned',autospec=True,side_effect=race),patch('processing.source_release_filesystem.os.unlink') as unlink:
            self.service.run_once();unlink.assert_not_called()
        self.assertEqual(self.lookup()['state'],'held')
        with self.queue._db() as db:
            row=db.execute("SELECT phase,physical_json FROM source_release_objects WHERE phase!='planned'").fetchone()
            self.assertEqual(row['phase'],'quarantining')
            self.assertEqual(json.loads(row['physical_json'])['effectIntent']['kind'],'rename')
            self.assertEqual(json.loads(row['physical_json'])['effectIntent']['outcome'],'completed')

    def _assert_permanent_failure_before_terminal(self,missing):
        self.ready_for_physical_phase()
        for _ in range(len(self.auth['eligibleObjects'])+1):self.service.run_once()
        with self.queue._db() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM source_release_objects WHERE phase!='removed'").fetchone())
            preserved=json.loads(db.execute('SELECT preserved_json FROM source_releases').fetchone()[0])
        obj=preserved[0];target=self.spool.root/self.spool.storage_key(self.auth['jobId'],1,obj['id'])
        if missing:target.unlink()
        else:target.write_bytes(b'corrupt synthetic GCP')
        self.service.run_once()
        state=self.lookup()
        self.assertEqual(state['state'],'held')
        self.assertIsNone(state['receipt'])
        self.assertEqual({path:path.read_bytes() for path in self.originals},self.originals)

    def test_missing_raw_gcp_after_removals_prevents_terminal_ack(self):self._assert_permanent_failure_before_terminal(True)
    def test_corrupt_raw_gcp_after_removals_prevents_terminal_ack(self):self._assert_permanent_failure_before_terminal(False)

    def test_real_process_crash_recovery_preserves_exact_namespace_and_ack(self):
        from processing.prototype import Queue
        from processing.reference_store import ReferenceStore
        from processing.source_spool import SourceSpool
        for boundary in ('before_rename','after_rename','after_unlink','after_unlinking_phase_before_intent','after_removed_commit','after_terminal_commit'):
            with self.subTest(boundary=boundary),tempfile.TemporaryDirectory() as directory:
                child=subprocess.run([sys.executable,'-B','-c',
                    'from tests.test_source_release_backend import crash_release; import sys; crash_release(sys.argv[1],sys.argv[2])',
                    directory,boundary],capture_output=True,text=True,timeout=20)
                self.assertEqual(child.returncode,42,child.stderr)
                state=json.loads((Path(directory)/'resume-synthetic.json').read_text())
                with patch('processing.source_spool.os.path.ismount',return_value=True):
                    queue=Queue(directory,reference_enabled=True)
                    try:
                        store=ReferenceStore(queue)
                        spool=SourceSpool(state['spool'],mount_id='a'*64,safety_floor_bytes=0,max_object_bytes=1024,chunk_bytes=3)
                        settings=ReleaseSettings(state['config'],viewer_bearers={VIEWER_BEARER:'staging'},spool=spool)
                        service=SourceReleaseService(store,spool,settings,monotonic_ns=lambda:1_000_000_000)
                        # Only exact registered old main/source lease epochs retire.
                        self.assertEqual(queue._coordinator_freeze.status()['phase'],'open')
                        self.assertEqual(store.reconcile_source_spool(spool)['held'],0)
                        for _ in range(5):service.run_once()
                        auth=state['request']['authorization']
                        result=service.lookup(auth['jobId'],auth['releaseId'],principal=state['principal'])
                        self.assertEqual(result['state'],'released',result)
                        if boundary=='after_terminal_commit':
                            self.assertEqual(p.canonical_json(result['receipt']),(Path(directory)/'expected-receipt.json').read_bytes())
                        self.assertEqual(store.reconcile_source_spool(spool)['held'],0)
                        _,created=service.submit(auth['jobId'],state['request'],principal=state['principal'])
                        self.assertFalse(created)
                        unrelated,_=queue.submit(dict(requestId='after-release-crash',profile='treedar-synthetic-v1',environment='staging'))
                        claimed=queue.claim(dict(workerId='after-crash-worker',profiles=['treedar-synthetic-v1']))
                        self.assertEqual(claimed['id'],unrelated['id'])
                        queue.complete(claimed['id'],dict(claimToken=claimed['claimToken'],status='succeeded',result={}))
                    finally:queue.close()

    def test_missing_copy_after_unlinking_phase_without_issued_intent_is_not_success(self):
        from processing.prototype import Queue
        from processing.reference_store import ReferenceStore
        from processing.source_spool import SourceSpool
        with tempfile.TemporaryDirectory() as directory:
            child=subprocess.run([sys.executable,'-B','-c',
                'from tests.test_source_release_backend import crash_release; import sys; crash_release(sys.argv[1],sys.argv[2])',
                directory,'after_unlinking_phase_before_intent'],capture_output=True,text=True,timeout=20)
            self.assertEqual(child.returncode,42,child.stderr)
            state=json.loads((Path(directory)/'resume-synthetic.json').read_text())
            with patch('processing.source_spool.os.path.ismount',return_value=True):
                queue=Queue(directory,reference_enabled=True)
                try:
                    store=ReferenceStore(queue);auth=state['request']['authorization']
                    spool=SourceSpool(state['spool'],mount_id='a'*64,safety_floor_bytes=0,max_object_bytes=1024,chunk_bytes=3)
                    with queue._db() as db:
                        item=db.execute("SELECT * FROM source_release_objects WHERE phase='unlinking'").fetchone()
                        self.assertNotIn('effectIntent',json.loads(item['physical_json']))
                    # Deliberate unexpected loss of one owned synthetic copy.
                    name=item['object_kind']+'-'+hashlib.sha256(item['object_id'].encode()).hexdigest()+'.blob'
                    copy_path=spool.root/auth['jobId']/'1'/('.ltds-release-'+auth['releaseId'])/name
                    copy_path.unlink()
                    self.assertEqual(store.reconcile_source_spool(spool)['held'],1)
                    settings=ReleaseSettings(state['config'],viewer_bearers={VIEWER_BEARER:'staging'},spool=spool)
                    service=SourceReleaseService(store,spool,settings,monotonic_ns=lambda:1_000_000_000)
                    service.run_once()
                    result=service.lookup(auth['jobId'],auth['releaseId'],principal=state['principal'])
                    self.assertEqual(result['state'],'held');self.assertIsNone(result['receipt'])
                    self.assertTrue(any((Path(directory)/'viewer-originals').iterdir()))
                finally:queue.close()

    def _seed_timer_headers(self,count):
        """Metadata-only clock fixtures, not additional accepted physical jobs.

        Their missing execution lineage would fail the full I/O validator. Only
        the scalar timer batch is exercised; no source files are synthesized.
        """
        from processing.source_release import TABLE_COLUMNS
        columns=TABLE_COLUMNS['source_releases']
        with self.queue._db() as db:
            template=dict(db.execute('SELECT * FROM source_releases WHERE release_id=?',(self.auth['releaseId'],)).fetchone())
            for _ in range(count):
                row=dict(template,job_id=str(uuid.uuid4()),release_id=str(uuid.uuid4()))
                auth=copy.deepcopy(self.auth);auth.update(jobId=row['job_id'],providerTaskId=row['job_id'],releaseId=row['release_id'])
                request=self.sign(auth)
                row.update(request_json=p.canonical_json(request).decode('ascii'),request_sha256=request['authorizationSha256'],
                    request_canonical_sha256=p.digest(request),remaining_ns=MINIMUM_NS,clock_ns=self.clock[0],
                    namespace_json=None,state='waiting_policy',hold_reason=None)
                db.execute('INSERT INTO source_releases('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')',tuple(row[name] for name in columns))

    def test_151_waiting_timers_progress_in_one_bounded_batch_independent_of_io_lock(self):
        self.assertEqual(self.post().status,201)
        self._seed_timer_headers(150)
        self.service._mutex.acquire()  # Model another cleanup operation waiting on NAS.
        try:
            for _ in range(288):
                self.clock[0]+=300*1_000_000_000
                self.service.checkpoint_timers()
        finally:self.service._mutex.release()
        with self.queue._db() as db:
            rows=db.execute('SELECT remaining_ns FROM source_releases').fetchall()
            self.assertEqual(len(rows),151)
            self.assertTrue(all(row['remaining_ns']==0 for row in rows))
            self.assertEqual(db.execute('SELECT count(*) FROM source_release_objects WHERE phase!=\'planned\'').fetchone()[0],0)

    def _runtime(self,enabled=True):
        from dataclasses import replace
        import ssl
        from tests.test_reference_runtime import ReferenceRuntimeTests
        from processing.reference_runtime import ReferenceRuntime
        from processing.config import ProcessingNode
        host=ReferenceRuntimeTests('runTest');host.setUp();self.addCleanup(host.doCleanups)
        config=replace(host.config,main_node_url='',prototype_node_ids=('cluster',),treedar_node_ids=('cluster',),
            processing_nodes=(ProcessingNode('cluster',host.config.local_node_url),))
        settings=host.settings()
        release=self.settings
        if not enabled:
            config_value=copy.deepcopy(self.config);config_value.update(enabled=False,filesystemQualification=None)
            release=ReleaseSettings(config_value,viewer_bearers={VIEWER_BEARER:'staging'},spool=self.spool)
        settings=replace(settings,workers={'cluster':settings.workers['cluster']},recovery_bindings=None,
            viewer_bearers={VIEWER_BEARER:'staging'},source_spool=self.spool,
            source_attestation_keys=self.case.state['source_case'].trusted_keys,source_release=release)
        class Server:
            def __init__(self):self.stop=threading.Event();self.closed=False
            def serve_forever(self,**kwargs):self.stop.wait()
            def shutdown(self):self.stop.set()
            def server_close(self):self.closed=True
        server=Server()
        with patch('processing.reference_runtime.ssl.SSLContext.load_cert_chain'),\
                patch('processing.reference_runtime.verified_client_context',return_value=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)):
            runtime=ReferenceRuntime(self.queue,config,settings,discover=lambda:host.snapshot,
                server_factory=lambda *args,**kwargs:server)
        return runtime,server

    def test_runtime_joins_single_cleanup_thread_without_stalling_viewer_delivery(self):
        from processing.reference_runtime import RuntimeConfigurationError
        from processing.source_release_filesystem import SourceReleaseFilesystem
        self.ready_for_physical_phase()
        with self.queue._db() as db:db.execute('UPDATE source_releases SET clock_ns=?',(time.monotonic_ns(),))
        runtime,server=self._runtime()
        entered,resume,delivered=threading.Event(),threading.Event(),threading.Event()
        class Delivery:
            def run_once(self):
                if entered.is_set():delivered.set()
                return {'state':'retry'}
        runtime._viewer_transfer_worker=Delivery()
        original=SourceReleaseFilesystem._rename_owned
        def pause(instance,*args):
            entered.set()
            if not resume.wait(8):raise RuntimeError('test runtime barrier timed out')
            return original(instance,*args)
        with patch.object(SourceReleaseFilesystem,'_rename_owned',autospec=True,side_effect=pause):
            runtime.start()
            try:
                self.assertTrue(entered.wait(4))
                source_thread=runtime._source_release_thread
                delivery_thread=runtime._viewer_transfer_thread
                timer_thread=runtime._source_timer_thread
                self.assertIsNot(source_thread,delivery_thread)
                self.assertTrue(delivered.wait(3),'Viewer delivery stalled behind cleanup I/O')
                with self.assertRaises(RuntimeConfigurationError):runtime.close()
                self.assertFalse(server.stop.is_set())
                self.assertFalse(runtime._viewer_transfer_stop.is_set())
                self.assertTrue(source_thread.is_alive())
            finally:
                resume.set();runtime.close()
            self.assertFalse(source_thread.is_alive());self.assertFalse(delivery_thread.is_alive())
            self.assertFalse(timer_thread.is_alive())
            self.assertTrue(server.closed)
            self.assertTrue(runtime._source_readers._closed)
            self.assertEqual(runtime._source_readers._handles,{})

    def test_disabled_runtime_starts_no_cleanup_thread(self):
        self.assertEqual(self.post().status,201)
        runtime,server=self._runtime(enabled=False)
        runtime.start()
        try:
            self.assertIsNone(runtime._source_release_thread)
            self.assertIsNone(runtime._source_timer_thread)
        finally:runtime.close()
        self.assertTrue(server.closed)

    def test_disabled_role_settings_are_metadata_only_even_with_saved_qualification(self):
        from processing.source_release_filesystem import SourceReleaseFilesystem
        config=copy.deepcopy(self.config);config['enabled']=False
        with patch.object(SourceReleaseFilesystem,'check_qualification',side_effect=AssertionError('disabled role must not access NAS')):
            settings=ReleaseSettings(config,viewer_bearers={VIEWER_BEARER:'staging'},spool=None)
        self.assertFalse(settings.enabled)


@unittest.skipUnless(sys.platform.startswith('linux'),'synthetic V2 TreeDar binding')
class TreeDarSourceReleaseTests(unittest.TestCase):
    """Three-byte transport/receipt fixture, not LiDAR geometry qualification."""
    sign=SourceReleaseBackendTests.sign
    post=SourceReleaseBackendTests.post
    lookup=SourceReleaseBackendTests.lookup
    ready_for_physical_phase=SourceReleaseBackendTests.ready_for_physical_phase
    finish=SourceReleaseBackendTests.finish
    tearDown=SourceReleaseBackendTests.tearDown

    def setUp(self):
        from tests import test_reference_source_intake as source_fixture
        original_setup=source_fixture.ReferenceSourceIntakeTests.setUp
        original_validate=source_fixture.ReferenceSourceIntakeTests.validate
        original_control=source_fixture.ReferenceControlAPI
        def setup(case):
            original_setup(case)
            image=case.payload['sourceManifest']['objects'][0]
            image['name']='synthetic-transport.las'
            cloud=case.payload['manifest']['objects'][0]
            cloud.update(name=image['name'],kind='lidar',mediaType='application/vnd.las')
            case.payload['manifest']['inputCounts'].update(images=0,lidarObjects=1)
            case.payload['pipeline']['odm']['requested']=False
            case.payload['pipeline']['treedar'].update(requested=True,inputObjectIds=[cloud['id']],
                provenanceReceiptId='synthetic-original-lidar')
            case.rehash()
        def validate(case,*args,**kwargs):
            kwargs['available_engines']=('odm','treedar')
            return original_validate(case,*args,**kwargs)
        def control(*args,**kwargs):
            kwargs['treedar_available']=True
            return original_control(*args,**kwargs)
        with patch.object(source_fixture.ReferenceSourceIntakeTests,'setUp',setup),\
                patch.object(source_fixture.ReferenceSourceIntakeTests,'validate',validate),\
                patch.object(source_fixture,'ReferenceControlAPI',side_effect=control):
            SourceReleaseBackendTests.setUp(self)

    def test_actual_treedar_only_v2_keeps_coordinator_id_and_releases_same_closure(self):
        job=self.store.get(self.auth['jobId'],private=True)
        self.assertFalse(job['submission']['pipeline']['odm']['requested'])
        self.assertTrue(job['submission']['pipeline']['treedar']['requested'])
        self.assertEqual(job['providerTaskId'],job['jobId'])
        self.assertEqual(job['sourceAcceptanceBinding']['providerTaskId'],job['jobId'])
        self.ready_for_physical_phase()
        state=self.finish()
        self.assertEqual(state['state'],'released',state)
        self.assertEqual({path:path.read_bytes() for path in self.originals},self.originals)

