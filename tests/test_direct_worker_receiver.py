"""Actual local spool and node gate; synthetic files, no HTTP or custody ACK."""
import copy
import hashlib
import io
import json
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

    def test_truncated_or_changed_bytes_produce_no_durable_object(self):
        with self.assertRaises(SourceSpoolError):self.receive(data=b'truncated')
        with self.assertRaises(SourceSpoolError):self.receive(data=b'x'*len(self.data))
        self.assertTrue(self.receive()['localObjectDurable'])

    def test_bad_gate_or_manifest_refuses_without_writing(self):
        with self.assertRaises(PolicyConflict):self.receive(generation=2)
        altered=copy.deepcopy(self.manifest);altered['objects'][0]['sha256']='b'*64
        with self.assertRaises(PolicyConflict):self.receive(manifest=altered)
        self.assertFalse((self.spool.root/self.item['jobId']).exists())

    def test_unfenced_or_other_scratch_binding_cannot_receive(self):
        with self.assertRaises(PolicyConflict):self.receive(item=dict(self.item,attemptId=str(uuid.uuid4())))
        self.assertFalse((self.spool.root/self.item['jobId']).exists())

    def test_failed_publication_sync_returns_no_receipt_and_replay_rechecks_bytes(self):
        target=self.spool.root/self.spool.storage_key(self.item['jobId'],1,'object-1')
        original=self.spool._fsync_directory
        def fail_after_publish(path):
            if target.exists():raise OSError('synthetic directory sync failure')
            return original(path)
        with patch.object(self.spool,'_fsync_directory',side_effect=fail_after_publish):
            with self.assertRaises(SourceSpoolError):self.receive()
        self.assertEqual(target.read_bytes(),self.data)
        self.ledger=self.open_ledger();self.spool=self.open_spool()
        replay=self.receive(data=b'')
        self.assertTrue(replay['replayed']);self.assertFalse(replay['sourceCustodyAccepted'])


if __name__=='__main__':unittest.main()
