"""Owned local-only lifecycle fixture; no production service or NAS."""
import json
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import socket
import struct
import unittest
from unittest.mock import patch, MagicMock
from processing.crossfs_admission_candidate import CrossFsHeld
from processing.local_capacity_authority_candidate import LocalCapacityAuthority
from processing.scratch_policy import ScratchLedger
from ops.reference_worker_daemon import DaemonStateLock
from processing.local_capacity_protocol_candidate import LocalCapacityProtocol, _read_exact


@unittest.skipUnless(os.name=='posix','POSIX lock fixture required')
class LocalCapacityAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='lt-local-capacity-')).resolve()
        print('Retained synthetic root:',self.root,flush=True)
        self.journal=self.root/'journal.sqlite';self.journal.touch(mode=0o600)
        self.ledger=ScratchLedger(self.root/'ledger.sqlite',self.root,
                                  capacity_probe=lambda _:10000,safety_floor_bytes=100)
        info=self.journal.stat()
        root_info=self.ledger.root.stat(); ledger_info=self.ledger.path.stat()
        self.binding=self.root/'authority.json'
        value={'schemaVersion':1,'authorityId':'a'*32,'journal':str(self.journal),
            'journalIdentity':[info.st_dev,info.st_ino],
            'ledgers':{'local':{'root':str(self.ledger.root),'journal':str(self.ledger.path.absolute()),'floor':100,
                'identity':[root_info.st_dev,root_info.st_ino,ledger_info.st_dev,ledger_info.st_ino]}}}
        self.binding.write_text(json.dumps(value),encoding='utf-8');self.binding.chmod(0o600)
        for name,value in [('processing.local_capacity_authority_candidate.LOCAL_CAPACITY_AUTHORITY_QUALIFIED',True),
                           ('processing.crossfs_admission_candidate.CROSSFS_QUALIFIED',True),
                           ('processing.crossfs_admission_candidate.CROSSFS_CANONICAL_JOURNAL',self.journal)]:
            p=patch(name,value);p.start();self.addCleanup(p.stop)
        self.args=dict(attempt_id='attempt',job_id='job',generation=1,manifest_sha256='a'*64,
                       environment='staging',ttl_seconds=60,demands=[{'ledger':'local','bytes':300}])

    def authority(self):
        return LocalCapacityAuthority(self.root,self.binding,self.journal,{'local':self.ledger})

    def test_exclusive_owner_and_idempotent_acquire(self):
        with self.authority() as authority:
            with self.assertRaises(ValueError):
                with self.authority():self.fail('second owner admitted')
            self.assertEqual(authority.acquire(**self.args)['state'],'admitted')
            self.assertEqual(authority.acquire(**self.args)['state'],'admitted')
        with self.authority() as reopened:
            self.assertEqual(reopened.acquire(**self.args)['state'],'admitted')

    def test_changed_binding_refuses_and_releases_owned_lock(self):
        authority=self.authority()
        with self.assertRaises(CrossFsHeld):
            with authority:
                self.binding.write_text('{}',encoding='utf-8')
                authority.acquire(**self.args)
        # A fresh owner must actually acquire the same retained lock.
        with DaemonStateLock(self.root) as fresh:
            fresh.check(self.root)

    def test_replaced_ledger_journal_holds(self):
        with self.authority() as authority:
            os.rename(self.ledger.path,self.root/'retained-ledger.sqlite')
            self.ledger.path.touch(mode=0o600)
            try:
                with self.assertRaisesRegex(CrossFsHeld,'ledger identity changed'):
                    authority.acquire(**self.args)
            finally:
                # Restore the same owned inode to permit the owner lock to close.
                os.rename(self.ledger.path,self.root/'retained-replacement.sqlite')
                os.rename(self.root/'retained-ledger.sqlite',self.ledger.path)

    def test_other_process_cannot_own_same_state(self):
        child = ("import sys; from ops.reference_worker_daemon import DaemonStateLock\n"
                 "try:\n with DaemonStateLock(sys.argv[1]): pass\n"
                 "except ValueError: sys.exit(7)\n"
                 "sys.exit(2)\n")
        with self.authority():
            result=subprocess.run([sys.executable,'-c',child,str(self.root)],
                stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=8)
            self.assertEqual(result.returncode,7,'other process unexpectedly acquired owner lock')
        control=subprocess.run([sys.executable,'-c',child,str(self.root)],
            stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=8)
        self.assertEqual(control.returncode,2,'child could not acquire lock after parent released')

    def test_local_peer_protocol_fixed_plan_and_replay(self):
        with patch('processing.local_capacity_protocol_candidate.LOCAL_CAPACITY_PROTOCOL_QUALIFIED',True):
            with self.authority() as authority:
                protocol=LocalCapacityProtocol(authority,trusted_uid=os.getuid(),plans={'attempt':self.args})
                # Use owned socket pairs; protocol handles one framed request at a time.
                def call(op):
                    client,server=socket.socketpair()
                    try:
                        client.settimeout(3)
                        request=json.dumps({'op':op,'attemptId':'attempt'}).encode()
                        client.sendall(struct.pack('!I',len(request))+request)
                        protocol.serve_one(server)
                        size=struct.unpack('!I',client.recv(4))[0]
                        return json.loads(client.recv(size))
                    finally:client.close();server.close()
                self.assertEqual(call('status')['state'],'absent')
                self.assertEqual(call('reserve')['state'],'admitted')
                self.assertEqual(call('pin')['state'],'admitted')
                self.assertEqual(call('status')['state'],'admitted')

    def test_local_peer_protocol_rejects_wrong_uid_without_intent(self):
        with patch('processing.local_capacity_protocol_candidate.LOCAL_CAPACITY_PROTOCOL_QUALIFIED',True):
            with self.authority() as authority:
                protocol=LocalCapacityProtocol(authority,trusted_uid=os.getuid()+1,plans={'attempt':self.args})
                client,server=socket.socketpair()
                try:
                    with self.assertRaises(CrossFsHeld):protocol.serve_one(server)
                finally:client.close();server.close()
                with authority._gate._db() as db:
                    self.assertEqual(db.execute('select count(*) from attempts').fetchone()[0],0)

    def test_local_protocol_absolute_read_deadline(self):
        fake=MagicMock()
        fake.recv.return_value=b'x'
        with patch('processing.local_capacity_protocol_candidate.time.monotonic',side_effect=[0.5,1.1]):
            with self.assertRaisesRegex(CrossFsHeld,'deadline'):
                _read_exact(fake,2,1.0)
        self.assertEqual(fake.recv.call_count,1)

    @unittest.skipUnless(sys.platform.startswith('linux'),'Linux mount ID fixture required')
    def test_mount_identity_mismatch_holds_and_closes_owned_pin(self):
        with self.authority() as authority:
            pin=authority._mount_pins['local'][0]
            original=authority._mount_identity
            def altered(fd):
                value=original(fd)
                return value if fd==pin else (value[0],value[1],value[2]+1)
            with patch.object(authority,'_mount_identity',side_effect=altered):
                with self.assertRaisesRegex(CrossFsHeld,'mounted filesystem changed'):
                    authority.acquire(**self.args)
        with self.assertRaises(OSError):os.fstat(pin)

    @unittest.skipUnless(sys.platform.startswith('linux'),'Linux mount ID fixture required')
    def test_mount_close_error_still_releases_owner(self):
        authority=self.authority();authority.__enter__()
        pin=authority._mount_pins['local'][0]
        original_close=os.close
        def injected(fd):
            original_close(fd)
            if fd==pin:raise OSError('synthetic close failure after close')
        with patch('processing.local_capacity_authority_candidate.os.close',side_effect=injected):
            with self.assertRaisesRegex(CrossFsHeld,'descriptor cleanup failed'):
                authority.__exit__(None,None,None)
        with DaemonStateLock(self.root) as fresh:fresh.check(self.root)
        with self.assertRaises(OSError):os.fstat(pin)

    def test_managed_fetch_and_direct_worker_start_fail_closed_without_exact_permit(self):
        from processing.managed_capacity_gate_candidate import ManagedCapacityGate
        from processing.reference_driver import ReferenceDriver
        from processing.reference_engine_worker import ReferenceEngineWorker
        job={'jobId':'job','generation':1,'environment':'staging','manifestSha256':'a'*64,'pipelineDigest':'b'*64}
        with patch('processing.managed_capacity_gate_candidate.MANAGED_CAPACITY_QUALIFIED',True):
            with self.authority() as authority:
                authority.acquire(**self.args)
                with authority._gate._db() as db:
                    reservation=db.execute('select reservation from parts where attempt=?',('attempt',)).fetchone()[0]
                gate=ManagedCapacityGate(authority,b'synthetic-private-key-material-32bytes',ledger=self.ledger)
                permit=gate.issue('attempt',pipeline_digest=job['pipelineDigest'])
                driver=object.__new__(ReferenceDriver);driver._managed_capacity_gate=gate;driver.broker=MagicMock()
                with self.assertRaises(CrossFsHeld):
                    driver._fetch_capacity_managed(job,{},'grant','object','target',reservation)
                driver.broker.download_object.assert_not_called()
                with self.assertRaises(CrossFsHeld):
                    driver._fetch_capacity_managed(dict(job,generation=2),{'capacityPermit':permit},'grant','object','target',reservation)
                driver.broker.download_object.assert_not_called()
                corrupted={'fields':dict(permit['fields']),'signature':permit['signature']}
                corrupted['fields']['manifestSha256']='b'*64
                with self.assertRaises(CrossFsHeld):
                    driver._fetch_capacity_managed(job,{'capacityPermit':corrupted},'grant','object','target',reservation)
                driver.broker.download_object.assert_not_called()
                driver._fetch_capacity_managed(job,{'capacityPermit':permit},'grant','object','target',reservation)
                driver.broker.download_object.assert_called_once()
                worker=object.__new__(ReferenceEngineWorker);worker._managed_capacity_gate=gate;worker.engine=MagicMock()
                with self.assertRaises(CrossFsHeld):
                    worker.start(job,reservation_id=reservation,input_paths={})
                worker.engine.initialize.assert_not_called()
                with self.assertRaises(CrossFsHeld):
                    worker.start(job,reservation_id='other',input_paths={},capacity_permit=permit)
                worker.engine.initialize.assert_not_called()

    def test_managed_worker_start_poll_and_terminal_receipt(self):
        import uuid
        from processing.managed_capacity_gate_candidate import ManagedCapacityGate
        from processing.node_gate import NodeGate
        from processing.reference_engine_worker import ReferenceEngineWorker, WorkerHeld
        from processing.reference_protocol import validate_submission
        from processing.worker_outbox import WorkerOutbox
        from tests.test_reference_engine_worker import FakeNativeEngine
        raw=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
        submission=validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint'])
        job=dict(jobId=str(uuid.uuid4()),generation=1,environment='staging',status='running',nodeId='cluster',
            manifestSha256=submission['manifestSha256'],pipelineDigest=submission['pipelineDigest'],submission=submission)
        args=dict(self.args,attempt_id='worker-attempt',job_id=job['jobId'],manifest_sha256=job['manifestSha256'],
                  demands=[{'ledger':'local','inputBytes':3,'outputBudget':1024}])
        with patch('processing.managed_capacity_gate_candidate.MANAGED_CAPACITY_QUALIFIED',True), \
             patch('processing.node_gate.canonical_gate_path',return_value=self.root/'node-gate'):
            with self.authority() as authority:
                authority.acquire(**args)
                with authority._gate._db() as db:
                    rid=db.execute('select reservation from parts where attempt=?',('worker-attempt',)).fetchone()[0]
                directory=self.ledger.root/'jobs'/rid;directory.mkdir(parents=True,mode=0o700)
                (directory/'image.jpg').write_bytes(b'abc');self.ledger.record_allocated(rid,3)
                gate=ManagedCapacityGate(authority,b'synthetic-private-key-material-32bytes',ledger=self.ledger)
                permit=gate.issue('worker-attempt',pipeline_digest=job['pipelineDigest']);engine=FakeNativeEngine()
                outbox=WorkerOutbox(self.root/'outbox'/'outbox.sqlite')
                physical_authority=str(uuid.uuid4())
                with NodeGate('physical-engine') as node:node.provision('cluster','http://127.0.0.1:8000',physical_authority)
                kwargs=dict(ledger=self.ledger,outbox=outbox,engine=engine,engine_id='physical-engine',node_id='cluster',
                    authority_id=physical_authority,coordinator_origin='http://127.0.0.1:8000',gate_generation=1,
                    worker_id='worker',engine_version='synthetic-v1',managed_gate=gate)
                path=self.root/'worker'/'worker.sqlite';worker=ReferenceEngineWorker(path,**kwargs)
                inputs={'image-1':'jobs/'+rid+'/image.jpg'}
                with self.assertRaises(CrossFsHeld):worker.start(job,reservation_id=rid,input_paths=inputs)
                self.assertEqual(engine.calls,[])
                self.assertEqual(worker.start(job,reservation_id=rid,input_paths=inputs,capacity_permit=permit),'processing')
                with self.assertRaises(CrossFsHeld):worker.poll(job['jobId'])
                self.assertNotIn('download',engine.calls)
                outbox_id=worker.poll(job['jobId'],capacity_permit=permit)
                self.assertTrue(outbox_id);self.assertEqual(engine.calls.count('commit'),1)
                self.assertEqual(worker.poll(job['jobId']),outbox_id)  # read-only replay
                with self.assertRaises(CrossFsHeld):worker.acknowledge_delivery(job['jobId'])
                completion=outbox.get(outbox_id);payload=completion['payload'];result=payload['resultManifest']
                receipt=dict(type='durable-result-receipt',durable=True,receiptId='delivery-1',storageReceiptId='coordinator-1',
                    outboxId=outbox_id,payloadSha256=completion['digest'],acceptedObjectIds=[item['id'] for item in result['objects']],
                    **{key:payload[key] for key in ('jobId','generation','environment','inputManifestSha256','pipelineDigest','resultManifestSha256')})
                outbox.claim_next();outbox.acknowledge(outbox_id,receipt)
                wrong={'fields':dict(permit['fields']),'signature':permit['signature']};wrong['fields']['generation']=2
                with self.assertRaises(CrossFsHeld):worker.acknowledge_delivery(job['jobId'],capacity_permit=wrong)
                with NodeGate('physical-engine') as node:self.assertIsNotNone(node.snapshot()['reservation'])
                with patch.object(worker,'_phase',side_effect=OSError('synthetic lost phase write')):
                    with self.assertRaises(OSError):worker.acknowledge_delivery(job['jobId'],capacity_permit=permit)
                with NodeGate('physical-engine') as node:self.assertIsNone(node.snapshot()['reservation'])
                with self.assertRaisesRegex(CrossFsHeld,'release requires reconciliation'):
                    with gate.hold(job,permit,reservation_id=rid):pass
                worker.acknowledge_delivery(job['jobId'],capacity_permit=permit)
                worker.acknowledge_delivery(job['jobId'],capacity_permit=permit)
                with NodeGate('physical-engine') as node:self.assertIsNone(node.snapshot()['reservation'])
                # A later controlled job overwrites NodeGate's single tombstone.
                # The earlier job's per-job release evidence must still permit
                # its independently validated delayed Viewer receipt.
                second=dict(job,jobId=str(uuid.uuid4()))
                args2=dict(args,attempt_id='worker-attempt-2',job_id=second['jobId'])
                authority.acquire(**args2)
                with authority._gate._db() as db:
                    rid2=db.execute('select reservation from parts where attempt=?',('worker-attempt-2',)).fetchone()[0]
                directory2=self.ledger.root/'jobs'/rid2;directory2.mkdir(parents=True,mode=0o700)
                (directory2/'image.jpg').write_bytes(b'abc');self.ledger.record_allocated(rid2,3)
                permit2=gate.issue('worker-attempt-2',pipeline_digest=second['pipelineDigest'])
                inputs2={'image-1':'jobs/'+rid2+'/image.jpg'}
                self.assertEqual(worker.start(second,reservation_id=rid2,input_paths=inputs2,capacity_permit=permit2),'processing')
                outbox2=worker.poll(second['jobId'],capacity_permit=permit2)
                completion2=outbox.get(outbox2);payload2=completion2['payload'];result2=payload2['resultManifest']
                receipt2=dict(type='durable-result-receipt',durable=True,receiptId='delivery-2',storageReceiptId='coordinator-2',
                    outboxId=outbox2,payloadSha256=completion2['digest'],acceptedObjectIds=[item['id'] for item in result2['objects']],
                    **{key:payload2[key] for key in ('jobId','generation','environment','inputManifestSha256','pipelineDigest','resultManifestSha256')})
                outbox.claim_next();outbox.acknowledge(outbox2,receipt2)
                worker.acknowledge_delivery(second['jobId'],capacity_permit=permit2)
                with NodeGate('physical-engine') as node:
                    self.assertNotEqual(node.snapshot()['lastAcknowledgment']['job']['id'],job['jobId'])
                accepted=dict(schemaVersion=1,receiptId='viewer-acceptance',storageReceiptId='viewer-storage',
                    jobId=job['jobId'],generation=1,environment='staging',inputManifestSha256=job['manifestSha256'],
                    pipelineDigest=job['pipelineDigest'],resultManifestSha256=result['resultManifestSha256'],
                    acceptedObjectIds=['odm-archive'],acceptedAt='2026-10-27T22:00:00Z',validationVersion='ltds-reference-v1')
                with authority._gate._db() as db:
                    original=db.execute('select binding from managed_release_intents where job=?',(job['jobId'],)).fetchone()[0]
                    db.execute('update managed_release_intents set binding=? where job=?',('{}',job['jobId']))
                try:
                    with self.assertRaises(CrossFsHeld):
                        worker.apply_viewer_acceptance(job['jobId'],[accepted],capacity_permit=permit)
                finally:
                    with authority._gate._db() as db:
                        db.execute('update managed_release_intents set binding=? where job=?',(original,job['jobId']))
                with self.assertRaises(CrossFsHeld):worker.apply_viewer_acceptance(job['jobId'],[accepted])
                with self.assertRaises(ValueError):
                    worker.apply_viewer_acceptance(job['jobId'],[dict(accepted,generation=2)],capacity_permit=permit)
                outcome=worker.apply_viewer_acceptance(job['jobId'],[accepted],capacity_permit=permit)
                self.assertTrue(outcome['fullyAccepted'])
                self.assertEqual(worker.apply_viewer_acceptance(job['jobId'],[accepted],capacity_permit=permit),outcome)
                self.assertTrue((directory/'all.zip').exists())
                legacy=dict(kwargs);legacy['managed_gate']=None
                with self.assertRaisesRegex(ValueError,'legacy mode'):ReferenceEngineWorker(path,**legacy)

    @unittest.skipUnless(sys.platform.startswith('linux'),'POSIX process fixture required')
    def test_release_barrier_survives_process_death_after_gate_ack(self):
        import uuid
        from processing.managed_capacity_gate_candidate import ManagedCapacityGate
        from processing.node_gate import NodeGate, _digest
        job={'jobId':str(uuid.uuid4()),'generation':1,'environment':'staging','manifestSha256':'a'*64,'pipelineDigest':'b'*64}
        physical=str(uuid.uuid4());receipt='f'*32
        completion={'status':'succeeded','result':{'resultManifestSha256':'c'*64}}
        ack=dict(id=job['jobId'],workerId='worker',nodeId='cluster',authorityId=physical,**completion)
        release={'jobId':job['jobId'],'generation':1,'syntheticReceipt':'validated-in-fixture'}
        with patch('processing.managed_capacity_gate_candidate.MANAGED_CAPACITY_QUALIFIED',True), \
             patch('processing.node_gate.canonical_gate_path',return_value=self.root/'crash-node-gate'):
            with self.authority() as authority:
                authority.acquire(**dict(self.args,job_id=job['jobId']))
                with authority._gate._db() as db:
                    reservation=db.execute('select reservation from parts where attempt=?',('attempt',)).fetchone()[0]
                managed=ManagedCapacityGate(authority,b'synthetic-private-key-material-32bytes',ledger=self.ledger)
                permit=managed.issue('attempt',pipeline_digest=job['pipelineDigest'])
                with NodeGate('crash-engine') as node:
                    node.provision('cluster','http://127.0.0.1:8000',physical)
                    node.reserve('cluster','http://127.0.0.1:8000',physical,1,receipt)
                    node.bind(receipt,dict(id=job['jobId'],workerId='worker',nodeId='cluster',authorityId=physical))
                    node.prepare_completion(receipt,completion)
            (self.root/'crash-input.json').write_text(json.dumps(dict(job=job,permit=permit,release=release,receipt=receipt,ack=ack)))
            (self.root/'crash-input.json').chmod(0o600)
            child=subprocess.run([sys.executable,'-m','tests.managed_release_crash_child',str(self.root)],
                stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=12)
            self.assertEqual(child.returncode,23,child.stderr.decode(errors='replace'))
            with self.authority() as authority:
                managed=ManagedCapacityGate(authority,b'synthetic-private-key-material-32bytes',ledger=self.ledger)
                with self.assertRaisesRegex(CrossFsHeld,'release requires reconciliation'):
                    with managed.hold(job,permit,reservation_id=reservation):pass
                def exact_tombstone():
                    with NodeGate('crash-engine') as node:
                        state=node.snapshot();last=state.get('lastAcknowledgment')
                        expected=dict(receiptId=receipt,job=dict(id=job['jobId'],workerId='worker',nodeId='cluster',authorityId=physical),
                                      completionDigest=_digest(completion),generation=1,kind='completion')
                        return state['reservation'] is None and last==expected
                with managed.hold_terminal(job,permit,reservation_id=reservation,
                        before=exact_tombstone,after=exact_tombstone,release_binding=release):pass
                self.assertTrue(managed.release_verified(job,permit,release))

    def test_disabled_refuses_before_use(self):
        with patch('processing.local_capacity_authority_candidate.LOCAL_CAPACITY_AUTHORITY_QUALIFIED',False):
            with self.assertRaises(CrossFsHeld):self.authority()
