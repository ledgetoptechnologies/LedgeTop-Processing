"""Inactive v4 storage protocol tests, not proof of live retirement authority.

The storage-only fixture deliberately lacks physical bootstrap authority. Live
producer/retained-store/emergency-budget qualification remains a separate gate.
"""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
import time

from processing import reference_indexed_fetch as j
from processing import reference_metadata_retirement as r
from processing import reference_protocol as p


class CompletedV4Fixture(unittest.TestCase):
    """Fresh low-level driver with actual stores; transport edge is synthetic.

    This is NOT a daemon bootstrap/release fixture or qualified budget profile.
    It supplies real completed metadata for independent cold business auditing.
    """
    def setUp(self):
        from tests.test_reference_driver_input_copies import InputCopyDriverTests
        from ops.reference_worker_daemon import DaemonStateLock
        from processing.scratch_policy import ScratchLedger
        from processing.worker_outbox import WorkerOutbox
        from processing.reference_failure import FailureOutbox
        from processing.reference_engine_worker import ReferenceEngineWorker
        from processing.reference_driver import ReferenceDriver
        from tests.test_reference_driver import TOKEN
        self.transport_fixture=InputCopyDriverTests();self.transport_fixture.setUp()
        self.addCleanup(self.transport_fixture.doCleanups);self.f=self.transport_fixture.f
        self.root=self.f.root/'v4-source';self.root.mkdir(mode=0o700)
        self.scratch=self.root/'scratch';self.scratch.mkdir(mode=0o700)
        self.owner=DaemonStateLock(self.root).__enter__();self.addCleanup(self.owner.__exit__,None,None,None)
        ledger=ScratchLedger(self.root/'scratch.sqlite',self.scratch,safety_floor_bytes=1024,cleanup_grace_seconds=0,input_copy_accounting=True)
        ledger.enable_input_copy_accounting()
        outbox=WorkerOutbox(self.root/'outbox.sqlite');failures=FailureOutbox(self.root/'failure-outbox.sqlite')
        worker=ReferenceEngineWorker(self.root/'worker.sqlite',ledger=ledger,outbox=outbox,engine=self.f.native,
            engine_id='engine',node_id='cluster',authority_id=self.f.queue.authority_id,coordinator_origin=self.f.gate_origin,
            gate_generation=1,worker_id='worker',engine_version='pinned-engine-1')
        self.binding=dict(stateRoot=str(self.root),stateRootIdentity=[self.root.stat().st_dev,self.root.stat().st_ino],
            scratchRoot=str(self.scratch),scratchRootIdentity=[self.scratch.stat().st_dev,self.scratch.stat().st_ino],
            inputCopyAccounting=dict(schemaVersion=4,fetchJournal=dict(schemaVersion=1,policyId=j.RETIRING_POLICY_ID)),
            indexedFetchJournal=j.compiled_profile_descriptor(j.RETIRING_POLICY_ID),
            physical=dict(nodeId='cluster',workerId='worker',workerInstanceId='instance',authorityId=self.f.queue.authority_id,engineId='engine',gateGeneration=1),
            policy=dict(safetyFloor=1024,environments=['staging']))
        if getattr(self,'complete_driver_binding',False):
            # The full maintenance wrapper independently audits the real
            # driver settings. Keep the original low-level fixtures unchanged;
            # this opt-in supplies the actual constructor values before the
            # immutable profile/binding is minted, never repairs a saved state.
            self.binding['origins']=dict(coordinatorOrigin='https://coordinator.invalid:9443',
                brokerOrigin=self.f.broker.origin,engineOrigin=worker.engine.origin,
                gateCoordinatorOrigin=worker.origin)
            self.binding['policy'].update(outputBudget=1024*1024,transferTtl=240,
                environments=['production','staging'])
        self.path=self.root/'driver.sqlite';self.context=j.initialize_fresh_profile(self.path,bootstrap_id=str(uuid.uuid4()),binding=self.binding,owner=self.owner)
        ledger.probe=j.indexed_capacity_probe(self.context,self.owner,self.scratch)
        driver=ReferenceDriver(worker,ledger,outbox,self.f.broker,'https://coordinator.invalid:9443',TOKEN,
            state_path=self.path,ssl_context=self.f.context,output_budget=1024*1024,transfer_ttl=240,safety_floor=1024,
            failure_outbox=failures,worker_instance_id='instance',input_copy_accounting=True,indexed_fetch_context=self.context,state_owner=self.owner)
        driver._request=self.f.request;driver.sender._open=self.f.sender_open
        self.f.ledger,self.f.outbox,self.f.failures,self.f.worker,self.f.driver,self.f.scratch=ledger,outbox,failures,worker,driver,self.scratch
        self.driver=driver

    def complete(self):
        job=self.f.submit();self.f.advance(job,'awaiting_acceptance')
        result=self.f.store.get(job['jobId'],private=True)['resultManifest']
        receipt=dict(schemaVersion=1,receiptId='retirement-viewer-'+str(uuid.uuid4()),storageReceiptId='retirement-storage-'+str(uuid.uuid4()),
            jobId=job['jobId'],generation=1,environment='staging',inputManifestSha256=job['manifestSha256'],pipelineDigest=job['pipelineDigest'],
            resultManifestSha256=result['resultManifestSha256'],acceptedObjectIds=[o['id'] for o in result['objects']],
            acceptedAt='2026-10-29T12:00:00.000Z',validationVersion='ltds-reference-v1')
        self.assertTrue(self.f.store.accept(job['jobId'],receipt)['fullyAccepted'])
        deadline=time.monotonic()+10
        while True:
            self.driver.step()
            with self.driver._db() as db:stage=db.execute('SELECT stage FROM driver_jobs WHERE job=?',(job['jobId'],)).fetchone()[0]
            if stage=='completed':return job
            self.assertLess(time.monotonic(),deadline);time.sleep(.03)

    def archive_for_storage_test(self,job):
        with self.driver._db() as db:
            rows=r.decode(r.snapshot(db,job['jobId']),r.MAX_ARCHIVE_BYTES)
            proof=r.terminal_proof(db,rows,state_root=self.root,binding=self.binding,eligibility=True)
            r._store_validated(db,job['jobId'],proof)
        return proof


class ArchiveStorageFixture:
    def __init__(self,path):
        self.path=Path(path);self.job=str(uuid.uuid4());self.request='storage-only-request'
        self.db=sqlite3.connect(self.path)
        self.db.execute('PRAGMA journal_mode=DELETE');self.db.execute('PRAGMA synchronous=FULL')
        for statement in j.schema_for_policy(j._RETIRING_POLICY):self.db.execute(statement)
        profile=p.canonical_json(dict(descriptor=j.compiled_profile_descriptor(j.RETIRING_POLICY_ID))).decode()
        self.db.execute('INSERT INTO indexed_profile VALUES(1,?)',(profile,))
        submission=dict(schemaVersion=1,submissionId=str(uuid.uuid4()),manifest=dict(
            id='storage-only',revision=1,totalBytes=3,objectCount=1,
            inputCounts=dict(images=1,lidarObjects=0,points=0),objects=[dict(
                id='image',name='image.jpg',byteSize=3,sha256=hashlib.sha256(b'abc').hexdigest(),kind='image',mediaType='image/jpeg')]),
            pipeline=dict(odm=dict(requested=True,options={},optionsFingerprint='a'*64),
                treedar=dict(requested=False,inputObjectIds=[],provenanceReceiptId=None),gaussian_splat=dict(requested=False)),capabilityFingerprint='b'*64)
        submission['manifest']['sha256']=p.manifest_sha256(submission['manifest'])
        checked=p.validate_submission(submission,environment='production',capability_fingerprint=submission['capabilityFingerprint'])
        claim=dict(jobId=self.job,generation=1,environment='production',nodeId='cluster',
            manifestSha256=checked['manifestSha256'],pipelineDigest=checked['pipelineDigest'],claimToken='c'*64,submission=checked)
        header={key:claim[key] for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken')}
        header['claimRequestId']=self.request
        projection=dict(claim={key:claim[key] for key in header if key in claim})
        objects=checked['manifest']['objects'];closure=p.digest(objects)
        values={
            'driver_jobs':[(1,self.job,self.request,'completed',p.canonical_json(projection).decode(),0.0)],
            'indexed_quota':[(self.job,j.JOB_CHARGE)],
            'indexed_claims':[(self.job,p.canonical_json(header).decode(),p.canonical_json(claim).decode(),p.digest(claim),1,3,closure)],
            'indexed_objects':[(self.job,0,'image',p.canonical_json(objects[0]).decode())],
            'indexed_seals':[(self.job,closure)],
            'indexed_reservations':[(self.job,p.canonical_json(dict(reservationId='storage-only-reservation')).decode())],
            'indexed_grants':[(self.job,'{}',p.digest({}))],
            'indexed_fetch':[(self.job,1,3,1,1,1.0)],
            'indexed_attempts':[(self.job,0,1,'d'*32,'confirmed')],
        }
        for table,rows in values.items():
            for row in rows:self.db.execute('INSERT INTO '+table+' VALUES('+','.join('?' for _ in row)+')',row)
        self.db.commit()
        self.proof=dict(schemaVersion=1,purpose='metadata-partition-only',jobId=self.job,claimRequestId=self.request,
            generation=1,environment='production',inputManifestSha256=claim['manifestSha256'],pipelineDigest=claim['pipelineDigest'],
            resultManifestSha256='e'*64,coordinatorReceiptSha256='f'*64,viewerReceiptsSha256='1'*64,
            profileSha256=r.digest(profile.encode()),workerClosureSha256='2'*64,scratchClosureSha256='3'*64)

    def close(self):self.db.close()


class MetadataRetirementStorageTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.f=ArchiveStorageFixture(self.root/'driver.sqlite');self.addCleanup(self.f.close)

    def test_old_descriptor_and_schema_remain_literal(self):
        self.assertEqual(p.digest(j.compiled_profile_descriptor()),'18dc06d78582538fc268ee93ed843fef0dd968c8353eafc5ac9b9c8ea454e0a5')
        self.assertIs(j.schema_for_policy(j._V2_POLICY),j.SCHEMA)
        self.assertIs(j.schema_for_policy(j._THREE_POLICY),j.SCHEMA)
        self.assertNotEqual(j.schema_for_policy(j._RETIRING_POLICY),j.SCHEMA)

    def test_one_transaction_partitions_and_keeps_driver_history_and_files(self):
        db=self.f.db;input_file=self.root/'unchanged.input';input_file.write_bytes(b'abc')
        before=input_file.stat();db.execute('BEGIN IMMEDIATE');old=r.snapshot(db,self.f.job)
        self.assertTrue(r._store_validated(db,self.f.job,self.f.proof));db.commit();db.execute('BEGIN')
        self.assertEqual(r.retired_jobs(db),(self.f.job,))
        for table in r.TABLES:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0],1 if table=='driver_jobs' else 0)
        with r.validated_partition(db) as token:
            self.assertEqual(r.partition_jobs(db,token),(self.f.job,))
            with r.logical_projection(db,job=self.f.job,partition=token) as logical:
                self.assertEqual(r.snapshot(logical,self.f.job),old)
                with self.assertRaises(sqlite3.OperationalError):logical.execute('DELETE FROM driver_jobs')
            self.assertEqual(r.normalized_history(db,partition=token)['jobs'],1)
        db.rollback();self.assertEqual(input_file.read_bytes(),b'abc')
        self.assertEqual((before.st_dev,before.st_ino),(input_file.stat().st_dev,input_file.stat().st_ino))

    def test_rollback_before_outer_commit_keeps_original_partition(self):
        db=self.f.db;db.execute('BEGIN');before=r.snapshot(db,self.f.job);db.rollback()
        db.execute('BEGIN IMMEDIATE');r._store_validated(db,self.f.job,self.f.proof);db.rollback();db.execute('BEGIN')
        self.assertEqual(r.snapshot(db,self.f.job),before);self.assertEqual(r.retired_jobs(db),())
        self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_quota').fetchone()[0],1);db.rollback()

    def test_sealed_archive_and_tombstone_are_immutable_and_hot_replay_held(self):
        db=self.f.db;db.execute('BEGIN IMMEDIATE');r._store_validated(db,self.f.job,self.f.proof);db.commit()
        for sql in ("UPDATE indexed_archives SET sha='foreign'","DELETE FROM indexed_archives",'DELETE FROM indexed_retired',"UPDATE indexed_retired SET proof='{}'"):
            with self.assertRaises(sqlite3.IntegrityError):db.execute(sql)
            db.rollback()
        with self.assertRaises(sqlite3.IntegrityError):db.execute('INSERT INTO indexed_quota VALUES(?,?)',(self.f.job,j.JOB_CHARGE))
        db.rollback();db.execute('BEGIN IMMEDIATE');self.assertFalse(r._store_validated(db,self.f.job,self.f.proof));db.rollback()

    def test_partition_token_is_not_json_or_reusable_across_transactions_with_writes(self):
        db=self.f.db;db.execute('BEGIN')
        with r.validated_partition(db) as token:self.assertEqual(r.partition_jobs(db,token),())
        with self.assertRaises(r.RetirementHeld):r.partition_jobs(db,token)
        with self.assertRaises(r.RetirementHeld):r.partition_jobs(db,{})
        db.rollback()
        with self.assertRaises(r.RetirementHeld):
            db.execute('BEGIN IMMEDIATE')
            with r.validated_partition(db) as token:
                db.execute('INSERT INTO readiness_tasks VALUES(?)',(str(uuid.uuid4()),))
                r.partition_jobs(db,token)
        db.rollback()

    def test_projection_checker_requires_factory_context_and_exact_unchanged_connections(self):
        db=self.f.db;db.execute('BEGIN IMMEDIATE');r._store_validated(db,self.f.job,self.f.proof);db.commit();db.execute('BEGIN')
        with r.validated_partition(db) as token:
            with r.logical_projection(db,job=self.f.job,partition=token) as logical:
                self.assertTrue(r.check_projection(db,logical,partition=token,job=self.f.job))
                with self.assertRaises(r.RetirementHeld):r.check_projection(db,logical,partition=token)
                with closing(sqlite3.connect(':memory:')) as foreign:
                    foreign.execute('PRAGMA query_only=ON');foreign.execute('BEGIN')
                    with self.assertRaises(r.RetirementHeld):r.check_projection(db,foreign,partition=token,job=self.f.job)
                logical.execute('PRAGMA query_only=OFF');logical.execute('UPDATE driver_jobs SET background_checked=0')
                logical.execute('PRAGMA query_only=ON')
                with self.assertRaises(r.RetirementHeld):r.check_projection(db,logical,partition=token,job=self.f.job)
            with self.assertRaises(r.RetirementHeld):r.check_projection(db,logical,partition=token,job=self.f.job)
        db.rollback()

    def test_temp_shadow_and_attached_namespaces_cannot_redirect_projection(self):
        db=self.f.db;db.execute('BEGIN')
        with r.validated_partition(db) as token:
            with r.logical_projection(db,partition=token) as logical:
                before=(logical.total_changes,logical.execute('PRAGMA main.schema_version').fetchone()[0])
                logical.execute('PRAGMA query_only=OFF')
                logical.execute("CREATE TEMP VIEW driver_jobs AS SELECT 'foreign' AS job")
                logical.execute('PRAGMA temp.schema_version=0');logical.execute('PRAGMA query_only=ON')
                self.assertEqual(before,(logical.total_changes,logical.execute('PRAGMA main.schema_version').fetchone()[0]))
                with self.assertRaisesRegex(r.RetirementHeld,'temp_shadow'):
                    r.check_projection(db,logical,partition=token)
            with r.logical_projection(db,partition=token) as logical:
                logical.execute("ATTACH ':memory:' AS extra")
                with self.assertRaisesRegex(r.RetirementHeld,'attached_database'):
                    r.check_projection(db,logical,partition=token)
            with r.logical_projection(db,partition=token) as logical:
                version=logical.execute('PRAGMA main.schema_version').fetchone()[0]
                changes=logical.total_changes
                logical.execute('PRAGMA query_only=OFF');logical.execute('CREATE TABLE foreign_schema(value TEXT)')
                logical.execute('PRAGMA main.schema_version='+str(version));logical.execute('PRAGMA query_only=ON')
                self.assertEqual((logical.total_changes,logical.execute('PRAGMA main.schema_version').fetchone()[0]),(changes,version))
                with self.assertRaisesRegex(r.RetirementHeld,'projection_context_changed'):
                    r.check_projection(db,logical,partition=token)
        db.rollback()

    def test_original_temp_shadow_is_held_before_profile_or_partition_reads(self):
        db=self.f.db;db.execute('BEGIN');db.execute("CREATE TEMP VIEW driver_jobs AS SELECT 'foreign' AS job")
        with self.assertRaisesRegex(r.RetirementHeld,'temp_shadow'):r.retired_jobs(db)
        binding=dict(inputCopyAccounting=dict(schemaVersion=4,fetchJournal=dict(schemaVersion=1,policyId=j.RETIRING_POLICY_ID)))
        with self.assertRaisesRegex(r.RetirementHeld,'temp_shadow'):
            j.validate_profile_connection(db,bootstrap_id=str(uuid.uuid4()),binding=binding,root_identity=(1,2),driver_identity=(3,4))
        db.execute('DROP VIEW temp.driver_jobs');db.rollback()

    def test_failed_or_incomplete_job_never_partitions(self):
        db=self.f.db;db.execute("UPDATE driver_jobs SET stage='awaiting_acceptance'");db.commit();db.execute('BEGIN IMMEDIATE')
        with self.assertRaises(r.RetirementHeld):r._store_validated(db,self.f.job,self.f.proof)
        db.rollback();self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_archives').fetchone()[0],0)

    @unittest.skipUnless(os.name=='posix','actual private-owner/file guards require POSIX')
    def test_missing_actual_durable_stores_cannot_be_replaced_by_saved_proof(self):
        db=self.f.db;db.execute('BEGIN IMMEDIATE');r._store_validated(db,self.f.job,self.f.proof);db.commit();db.execute('BEGIN')
        binding=dict(stateRootIdentity=[self.root.stat().st_dev,self.root.stat().st_ino],scratchRoot=str(self.root),scratchRootIdentity=[self.root.stat().st_dev,self.root.stat().st_ino])
        with self.assertRaises((r.RetirementHeld,FileNotFoundError)):
            r.validate_archived_closures(db,state_root=self.root,binding=binding)
        db.rollback()

    @unittest.skipUnless(os.name=='posix','genuine SIGKILL/SQLite rollback journal requires POSIX')
    def test_actual_kill_during_uncommitted_archive_restores_whole_hot_partition(self):
        self._kill_storage_writer(committed=False)

    @unittest.skipUnless(os.name=='posix','genuine SIGKILL/SQLite rollback journal requires POSIX')
    def test_actual_kill_after_commit_preserves_whole_archive_partition(self):
        self._kill_storage_writer(committed=True)

    def _kill_storage_writer(self,*,committed):
        import select
        db=self.f.db;db.execute('BEGIN');original=r.snapshot(db,self.f.job);db.rollback();db.close()
        before=self.f.path.read_bytes();proof_path=self.root/'proof.json';proof_path.write_text(json.dumps(self.f.proof))
        script='''import json,signal,sqlite3,sys
from processing import reference_metadata_retirement as r
db=sqlite3.connect(sys.argv[1]);db.execute('PRAGMA synchronous=FULL');db.execute('PRAGMA cache_size=1');db.execute('BEGIN IMMEDIATE')
r._store_validated(db,sys.argv[2],json.loads(open(sys.argv[3]).read()))
if sys.argv[4]=='yes':db.commit()
print('ready',flush=True)
signal.pause()
'''
        child=subprocess.Popen([sys.executable,'-c',script,str(self.f.path),self.f.job,str(proof_path),'yes' if committed else 'no'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            self.assertTrue(select.select([child.stdout],[],[],10)[0],'owned storage child did not reach checkpoint')
            self.assertEqual(child.stdout.readline(),b'ready\n')
            if not committed:
                journal=Path(str(self.f.path)+'-journal');self.assertTrue(journal.is_file())
                self.assertEqual(journal.read_bytes()[:8],bytes.fromhex('d9d505f920a163d7'),'actual pager spill required')
            child.send_signal(signal.SIGKILL);self.assertEqual(child.wait(10),-signal.SIGKILL)
        finally:
            if child.poll() is None:child.send_signal(signal.SIGKILL);child.wait(10)
            child.stdout.close();child.stderr.close()
            self.f.db=sqlite3.connect(self.f.path)
        db=self.f.db;db.execute('BEGIN')
        if committed:
            self.assertEqual(r.retired_jobs(db),(self.f.job,))
            with r.logical_projection(db,job=self.f.job) as logical:self.assertEqual(r.snapshot(logical,self.f.job),original)
        else:
            self.assertEqual(r.retired_jobs(db),());self.assertEqual(r.snapshot(db,self.f.job),original)
        db.rollback()
        if not committed:self.assertEqual(self.f.path.read_bytes(),before)


@unittest.skipUnless(os.name=='posix','actual canonical owner and retained-store audit requires POSIX')
class MetadataRetirementActualStoreTests(unittest.TestCase):
    def test_actual_completed_job_proof_and_each_rehashed_wrong_commitment(self):
        fixture=CompletedV4Fixture();fixture.setUp();self.addCleanup(fixture.doCleanups)
        job=fixture.complete()
        durable=[fixture.root/name for name in ('worker.sqlite','outbox.sqlite','scratch.sqlite')]
        before={str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in durable}
        with fixture.driver._db() as db:
            rows=r.decode(r.snapshot(db,job['jobId']),r.MAX_ARCHIVE_BYTES)
            proof=r.terminal_proof(db,rows,state_root=fixture.root,binding=fixture.binding,eligibility=True)
            for field in ('resultManifestSha256','coordinatorReceiptSha256','viewerReceiptsSha256',
                          'profileSha256','workerClosureSha256','scratchClosureSha256'):
                db.execute('SAVEPOINT invalid_retirement')
                wrong=dict(proof);wrong[field]='0'*64
                with self.assertRaises(r.RetirementHeld,msg=field):
                    r._store_validated(db,job['jobId'],wrong)
                    r.validate_archived_closures(db,state_root=fixture.root,binding=fixture.binding)
                db.execute('ROLLBACK TO invalid_retirement');db.execute('RELEASE invalid_retirement')
                self.assertEqual(r.retired_jobs(db),())
                self.assertEqual(before,{str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in durable})
            self.assertTrue(r._store_validated(db,job['jobId'],proof))
        with fixture.driver._db() as db, r.validated_partition(db) as partition:
            self.assertEqual(r.validate_archived_closures(db,state_root=fixture.root,binding=fixture.binding,partition=partition)['jobs'],1)
        self.assertEqual(len(list(fixture.scratch.glob('jobs/*/*.input'))),1)
        self.assertEqual(len(list(fixture.scratch.glob('jobs/*/all.zip'))),1)
        self.assertIsNone(fixture.driver.ledger.release_authority)


if __name__=='__main__':unittest.main()
