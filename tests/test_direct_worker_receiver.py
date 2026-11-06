"""Actual local spool and node gate; synthetic files, no HTTP or custody ACK."""
import copy
import hashlib
import io
import json
import os
import threading
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

from processing import direct_worker_scratch as scratch, direct_worker_receiver as receiver
from processing import reference_protocol as p
from processing.node_gate import NodeGate
from processing.scratch_policy import ScratchLedger, PolicyConflict
from processing.source_spool import SourceSpool, SourceSpoolError


class DirectWorkerReceiverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.root.chmod(0o700); self.now = 100
        self.worker = dict(authorityId=str(uuid.uuid4()), nodeId='cluster', workerId='worker', workerInstanceId='instance')
        self.data = b'synthetic source bytes'
        obj = dict(id='object-1', name='photo.jpg', byteSize=len(self.data),
                   sha256=hashlib.sha256(self.data).hexdigest(), kind='image', mediaType='image/jpeg')
        self.manifest = dict(id='dataset-1', revision=1, totalBytes=len(self.data), objectCount=1,
            inputCounts=dict(images=1,lidarObjects=0,points=0), objects=[obj])
        self.manifest['sha256'] = p.manifest_sha256(self.manifest)
        self.item = dict(reservationId=str(uuid.uuid4()), jobId=str(uuid.uuid4()), attemptId=str(uuid.uuid4()),
            generation=1, environment='production', manifestSha256=self.manifest['sha256'],
            inputBytes=len(self.data), outputBudget=200, ttlSeconds=10, **self.worker)
        self.ledger = self.open_ledger()
        self.observation = scratch.prepare(self.ledger,self.item,expected_worker=self.worker,worker_ready=True)
        directory = self.ledger.prepare_job_directory(self.observation['scratchReservationId'])/'direct-source'
        directory.mkdir(mode=0o700)
        # The Linux test runner supplies a real private tmpfs mounted at /tmp;
        # never mock the actual mount or filesystem durability primitives.
        mount=Path('/tmp');marker_name='.ltds-test-'+uuid.uuid4().hex
        self.mount,self.marker_name,self.directory=mount,marker_name,directory
        marker=mount/marker_name;self.marker_id='a'*32;marker.write_text(self.marker_id);marker.chmod(0o600)
        self.addCleanup(lambda:marker.unlink(missing_ok=True))
        self.spool=self.open_spool()
        self.gate_patch=patch('processing.node_gate.canonical_gate_path',return_value=self.root/'gate')
        self.gate_patch.start();self.addCleanup(self.gate_patch.stop)
        self.origin='https://127.0.0.1:8443';self.engine='fixture-engine'
        with NodeGate(self.engine) as gate:
            gate.provision('cluster',self.origin,self.worker['authorityId'])
            gate.reserve('cluster',self.origin,self.worker['authorityId'],1,self.observation['bindingSha256'][:32])
            gate.bind(self.observation['bindingSha256'][:32],dict(id=self.item['jobId'],workerId='worker',
                nodeId='cluster',authorityId=self.worker['authorityId']))

    def open_ledger(self):
        return ScratchLedger(self.root/'private'/'ledger.sqlite',self.root,
            capacity_probe=lambda _: 1000000,clock=lambda:self.now,safety_floor_bytes=100)

    def open_spool(self):
        return SourceSpool(self.directory,mount_path=self.mount,mount_id=self.marker_id,
            marker_name=self.marker_name,safety_floor_bytes=0,require_mount=True)

    def receive(self, *, data=None, item=None, manifest=None, generation=1):
        return receiver.receive(self.ledger,self.spool,manifest or self.manifest,item or self.item,
            expected_worker=self.worker,engine_id=self.engine,coordinator_origin=self.origin,
            gate_generation=generation,object_id='object-1',reader=io.BytesIO(self.data if data is None else data))

    def test_durable_local_replay_after_restart_never_claims_custody(self):
        result=self.receive();self.assertTrue(result['localObjectDurable'])
        self.assertFalse(result['sourceCustodyAccepted']);self.assertFalse(result['transferComplete'])
        self.now=1000;self.ledger=self.open_ledger();self.spool=self.open_spool()
        replay=self.receive(data=b'');self.assertTrue(replay['replayed'])
        self.assertEqual({k:v for k,v in replay.items() if k!='replayed'},
                         {k:v for k,v in result.items() if k!='replayed'})
        self.assertTrue(self.ledger.transfer_fenced(self.observation['scratchReservationId']))

    def test_truncated_bytes_hold_unknown_reader_and_scratch(self):
        with self.assertRaises(SourceSpoolError):self.receive(data=b'truncated')
        self.ledger=self.open_ledger();self.spool=self.open_spool()
        with self.assertRaisesRegex(PolicyConflict,'active or uncertain'):
            self.ledger.settle_unstarted(self.observation['scratchReservationId'],transfer_ended=True)
        with self.assertRaisesRegex(PolicyConflict,'active or uncertain'):
            self.ledger.set_engine_state(self.observation['scratchReservationId'],'terminal',terminal_attested=True)
        with self.assertRaisesRegex(PolicyConflict,'active or uncertain'):self.receive()

    def test_changed_bytes_leave_writer_pin_held(self):
        with self.assertRaises(SourceSpoolError):self.receive(data=b'x'*len(self.data))
        with self.assertRaisesRegex(PolicyConflict,'active or uncertain'):self.receive()

    def test_bad_gate_or_manifest_refuses_without_writing(self):
        with self.assertRaises(PolicyConflict):self.receive(generation=2)
        altered=copy.deepcopy(self.manifest);altered['objects'][0]['sha256']='b'*64
        with self.assertRaises(PolicyConflict):self.receive(manifest=altered)
        self.assertFalse((self.spool.root/self.item['jobId']).exists())

    def test_unfenced_or_other_scratch_binding_cannot_receive(self):
        with self.assertRaises(PolicyConflict):self.receive(item=dict(self.item,attemptId=str(uuid.uuid4())))
        self.assertFalse((self.spool.root/self.item['jobId']).exists())

    def test_failed_publication_sync_returns_no_receipt_and_pins_unknown_reader(self):
        target=self.spool.root/self.spool.storage_key(self.item['jobId'],1,'object-1')
        original=self.spool._fsync_directory
        def fail_after_publish(path):
            if target.exists():raise OSError('synthetic directory sync failure')
            return original(path)
        with patch.object(self.spool,'_fsync_directory',side_effect=fail_after_publish):
            with self.assertRaises(SourceSpoolError):self.receive()
        self.assertEqual(target.read_bytes(),self.data)
        self.ledger=self.open_ledger();self.spool=self.open_spool()
        with self.assertRaisesRegex(PolicyConflict,'active or uncertain'):self.receive(data=b'')
        with self.assertRaisesRegex(PolicyConflict,'active or uncertain'):
            self.ledger.settle_unstarted(self.observation['scratchReservationId'],transfer_ended=True)

    def test_blocked_reader_prevents_settlement_and_terminal_until_closed(self):
        entered=threading.Event();release=threading.Event();results=[]
        class BlockingReader(io.BytesIO):
            def read(self, count=-1):
                entered.set()
                if not release.wait(5):raise OSError('test reader deadline')
                return super().read(count)
        source=BlockingReader(self.data)
        def deliver():
            try:
                results.append(receiver.receive(self.ledger,self.spool,self.manifest,self.item,
                    expected_worker=self.worker,engine_id=self.engine,coordinator_origin=self.origin,
                    gate_generation=1,object_id='object-1',reader=source))
            except Exception as error:results.append(error)
        thread=threading.Thread(target=deliver);thread.start()
        try:
            self.assertTrue(entered.wait(3))
            for action in (lambda:self.ledger.settle_unstarted(self.observation['scratchReservationId'],transfer_ended=True),
                           lambda:self.ledger.set_engine_state(self.observation['scratchReservationId'],'terminal',terminal_attested=True),
                           lambda:self.ledger.set_engine_state(self.observation['scratchReservationId'],'engine_started'),
                           lambda:self.ledger.claim_engine_lease(self.observation['scratchReservationId']),
                           lambda:self.ledger.record_allocated(self.observation['scratchReservationId'],1)):
                with self.assertRaisesRegex(PolicyConflict,'active or uncertain'):action()
        finally:
            release.set();thread.join(5)
        self.assertFalse(thread.is_alive());self.assertEqual(len(results),1)
        if isinstance(results[0],Exception):raise results[0]
        self.assertTrue(source.closed);self.assertTrue(results[0]['localObjectDurable'])
        self.assertEqual(self.ledger.settle_unstarted(self.observation['scratchReservationId'],transfer_ended=True),'unstarted')

    def test_close_error_retains_pin_even_if_file_was_published(self):
        class BadClose(io.BytesIO):
            fail=True
            def close(self):
                if self.fail:
                    self.fail=False
                    raise OSError('synthetic uncertain close')
                return super().close()
        with self.assertRaises(OSError):
            receiver.receive(self.ledger,self.spool,self.manifest,self.item,expected_worker=self.worker,
                engine_id=self.engine,coordinator_origin=self.origin,gate_generation=1,
                object_id='object-1',reader=BadClose(self.data))
        self.ledger=self.open_ledger()
        with self.assertRaisesRegex(PolicyConflict,'active or uncertain'):
            self.ledger.settle_unstarted(self.observation['scratchReservationId'],transfer_ended=True)

    @unittest.skipUnless(hasattr(os,'fork'), 'POSIX process death required')
    def test_process_death_in_reader_leaves_durable_pin_after_restart(self):
        import signal
        class DeadReader(io.BytesIO):
            def read(self, count=-1):os._exit(73)
        pid=os.fork()
        if pid==0:
            try:
                receiver.receive(self.ledger,self.spool,self.manifest,self.item,expected_worker=self.worker,
                    engine_id=self.engine,coordinator_origin=self.origin,gate_generation=1,
                    object_id='object-1',reader=DeadReader(self.data))
            finally:os._exit(74)
        previous=signal.signal(signal.SIGALRM,lambda *_:(_ for _ in ()).throw(TimeoutError('child did not exit')))
        try:
            signal.alarm(5);_,status=os.waitpid(pid,0)
        finally:
            signal.alarm(0);signal.signal(signal.SIGALRM,previous)
        self.assertEqual(os.waitstatus_to_exitcode(status),73)
        self.now=10000;self.ledger=self.open_ledger();self.spool=self.open_spool()
        with self.assertRaisesRegex(PolicyConflict,'active or uncertain'):
            self.ledger.settle_unstarted(self.observation['scratchReservationId'],transfer_ended=True)
        with self.assertRaisesRegex(PolicyConflict,'active or uncertain'):self.receive()

    @unittest.skipUnless(os.environ.get('LTDS_OLD_SCRATCH_SOURCE'), 'provide exact prior scratch source')
    def test_previous_actual_ledger_refuses_after_writer_pin_retired(self):
        import importlib.util
        self.receive()
        spec=importlib.util.spec_from_file_location('previous_scratch',os.environ['LTDS_OLD_SCRATCH_SOURCE'])
        previous=importlib.util.module_from_spec(spec);spec.loader.exec_module(previous)
        with self.assertRaisesRegex(previous.PolicyConflict,'unknown accounting version'):
            previous.ScratchLedger(self.root/'private'/'ledger.sqlite',self.root,
                capacity_probe=lambda _:1000000,clock=lambda:self.now,safety_floor_bytes=100)


if __name__=='__main__':unittest.main()
