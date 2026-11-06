"""Synthetic coordinator journal tests; sender I/O is deliberately mocked."""
import io
import json
from pathlib import Path
import ssl
import threading
import unittest
import uuid
from unittest.mock import Mock

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing.direct_handoff_journal import DirectHandoff
from processing.direct_worker_receiver import _PreparedManifest
from processing.direct_worker_receipt import WorkerClosureSigner
from processing.direct_worker_scratch import scratch_identity
from processing.direct_worker_sender import DirectDeliveryUncertain, DirectWorkerSender
from processing.direct_worker_http import create_direct_worker_server
from processing.direct_worker_boundary import DirectWorkerBoundary
from processing.node_gate import NodeGate
from processing.scratch_policy import ScratchLedger
from processing.source_spool import SourceSpool
from ops.qualify_enrolled_phase_transport import configuration_material
from processing.maintenance import Conflict
from tests import test_direct_intake_reservation as reservation_fixture


class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.fixture=reservation_fixture.DirectIntakeTests();self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.addCleanup(self.fixture.temp.cleanup)
        self.addCleanup(lambda:self.fixture.queue.close())
        request=self.fixture.submit(); held=self.fixture.direct.reserve(**request)
        with self.fixture.queue._db() as db:
            manifest=json.loads(self.fixture.store._row(db,request['job_id'])['input_json'])['manifest']
        fields=('reservationId','jobId','attemptId','generation','environment','authorityId','nodeId',
                'workerId','workerInstanceId','manifestSha256','derivedManifestSha256','limitProfile')
        item={name:held[name] for name in fields};item.update(inputBytes=manifest['totalBytes'],outputBudget=100,ttlSeconds=10)
        worker={name:item[name] for name in ('authorityId','nodeId','workerId','workerInstanceId')}
        sender=DirectWorkerSender.__new__(DirectWorkerSender)
        sender._item,sender._sha,_=scratch_identity(item,expected_worker=worker)
        sender._prepared=_PreparedManifest(manifest,item,[])
        self.key=Ed25519PrivateKey.generate();sender._issuer='worker';sender._key='key';sender._public=self.key.public_key()
        self.sender=sender;self.obj=manifest['objects'][0]
        self.token='synthetic-viewer-bearer-0123456789'
        self.handoff=DirectHandoff(self.fixture.store,sender,principal='viewer',bearer=self.token)

    def observation(self):
        item=self.sender._item;obj=self.obj
        return dict(protocol='ltds-direct-local-object-v1',reservationId=item['reservationId'],
            scratchBindingSha256=self.sender._sha,jobId=item['jobId'],attemptId=item['attemptId'],
            generation=item['generation'],nodeId=item['nodeId'],objectId=obj['id'],byteSize=obj['byteSize'],
            sha256=obj['sha256'],localObjectDurable=True,sourceCustodyAccepted=False,transferComplete=False,replayed=False)

    def send(self, token=None, protected=True):
        return self.handoff.send_object(self.token if token is None else token,protected_transport=protected,
            object_id=self.obj['id'],byte_size=self.obj['byteSize'],sha256=self.obj['sha256'],reader=io.BytesIO(b'test'))

    def test_auth_denial_does_not_call_worker(self):
        self.sender.put=Mock()
        for token,protected in (('wrong',True),(self.token,False)):
            with self.assertRaises(Conflict):self.send(token,protected)
        self.sender.put.assert_not_called()

    def test_durable_intent_precedes_io_and_exact_replay_never_resends(self):
        def deliver(*_):
            with self.fixture.queue._db() as db:
                self.assertEqual(db.execute('SELECT state FROM direct_handoff_objects').fetchone()['state'],'intent')
            return self.observation()
        self.sender.put=Mock(side_effect=deliver)
        first=self.send();self.assertEqual(self.send(),first);self.assertEqual(self.sender.put.call_count,1)
        status=self.handoff.status(self.token,protected_transport=True)
        self.assertEqual((status['observedObjects'],status['uncertainObjects']),(1,0))
        self.assertFalse(status['sourceCustodyAccepted'])

    def test_lost_reply_stays_uncertain_across_reopen_and_never_auto_resends(self):
        self.sender.put=Mock(side_effect=DirectDeliveryUncertain('lost'))
        with self.assertRaises(DirectDeliveryUncertain):self.send()
        self.handoff=DirectHandoff(self.fixture.store,self.sender,principal='viewer',bearer=self.token)
        with self.assertRaisesRegex(Conflict,'uncertain'):self.send()
        self.assertEqual(self.sender.put.call_count,1)
        self.assertEqual(self.handoff.status(self.token,protected_transport=True)['uncertainObjects'],1)
        with self.assertRaisesRegex(Conflict,'incomplete or uncertain'):self.handoff.complete(self.token,protected_transport=True)

    def test_competing_transfer_cannot_send_twice(self):
        entered=threading.Event();release=threading.Event();results=[]
        def deliver(*_):
            entered.set();release.wait(3);return self.observation()
        self.sender.put=Mock(side_effect=deliver)
        def first():
            try:results.append(self.send())
            except Exception as error:results.append(error)
        thread=threading.Thread(target=first);thread.start()
        try:
            self.assertTrue(entered.wait(3))
            with self.assertRaisesRegex(Conflict,'uncertain'):self.send()
        finally:release.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(self.sender.put.call_count,1)
        self.assertIsInstance(results[0],dict)

    def test_signed_worker_closure_persists_but_never_acknowledges_viewer(self):
        self.sender.put=Mock(return_value=self.observation());self.send()
        expected=self.handoff._expected_closure()
        identity={k:expected[k] for k in ('authorityId','nodeId','workerId','workerInstanceId')}
        signer=WorkerClosureSigner(issuer_id='worker',key_id='key',identity=identity,private_key=self.key)
        signed=signer.issue(expected);self.sender.complete=Mock(return_value=signed)
        self.assertEqual(self.handoff.complete(self.token,protected_transport=True),signed)
        self.handoff=DirectHandoff(self.fixture.store,self.sender,principal='viewer',bearer=self.token)
        self.assertEqual(self.handoff.complete(self.token,protected_transport=True),signed)
        self.assertEqual(self.sender.complete.call_count,1)
        self.assertFalse(signed['closure']['sourceCustodyAccepted'])

    def test_changed_persisted_observation_never_replays(self):
        self.sender.put=Mock(return_value=self.observation());self.send()
        changed=self.observation();changed['sourceCustodyAccepted']=True
        with self.fixture.queue._db() as db:
            db.execute('UPDATE direct_handoff_objects SET response=?',(json.dumps(changed),))
        with self.assertRaisesRegex(Conflict,'observation changed'):self.send()
        self.assertEqual(self.sender.put.call_count,1)

    def test_frozen_coordinator_cannot_begin_send(self):
        from processing.coordinator_freeze import CoordinatorFrozen
        self.sender.put=Mock()
        gate=self.fixture.queue._coordinator_freeze
        gate.transition(dict(requestId=str(uuid.uuid4()),planId=str(uuid.uuid4()),action='intent',
            expectedRevision=gate.status()['revision'],startupId=self.fixture.queue.startup_id),
            owner_secret='synthetic-owner')
        with self.assertRaises(CoordinatorFrozen):self.send()
        self.sender.put.assert_not_called()


class HandoffTLSIntegrationTests(unittest.TestCase):
    """Real coordinator journal to TLS worker; no Viewer receipt or runtime."""
    def setUp(self):
        from unittest.mock import patch
        self.f=HandoffTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        root=Path(self.f.fixture.temp.name)/'worker';root.mkdir(mode=0o700)
        item=self.f.sender._item
        worker={k:item[k] for k in ('authorityId','nodeId','workerId','workerInstanceId')}
        ledger=ScratchLedger(root/'private'/'ledger.sqlite',root,capacity_probe=lambda _:1000000,
                             clock=lambda:100,safety_floor_bytes=100)
        from processing import direct_worker_scratch as scratch
        observation=scratch.prepare(ledger,item,expected_worker=worker,worker_ready=True)
        directory=ledger.prepare_job_directory(observation['scratchReservationId'])/'direct-source'
        directory.mkdir(mode=0o700)
        marker=Path('/tmp')/('.ltds-handoff-'+uuid.uuid4().hex)
        marker.write_text('a'*32);marker.chmod(0o600);self.addCleanup(lambda:marker.unlink(missing_ok=True))
        spool=SourceSpool(directory,mount_path='/tmp',mount_id='a'*32,marker_name=marker.name,safety_floor_bytes=0)
        gate_patch=patch('processing.node_gate.canonical_gate_path',return_value=root/'gate')
        gate_patch.start();self.addCleanup(gate_patch.stop)
        coordinator_origin='https://127.0.0.1:8443';engine='synthetic-engine'
        with NodeGate(engine) as gate:
            gate.provision(item['nodeId'],coordinator_origin,item['authorityId'])
            gate.reserve(item['nodeId'],coordinator_origin,item['authorityId'],1,observation['bindingSha256'][:32])
            gate.bind(observation['bindingSha256'][:32],dict(id=item['jobId'],workerId=item['workerId'],
                      nodeId=item['nodeId'],authorityId=item['authorityId']))
        files=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))['configurationFiles']
        for name in ('fixture-ca.pem','fixture-cert.pem','fixture-key.pem'):(root/name).write_bytes(files[name])
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(str(root/'fixture-cert.pem'),str(root/'fixture-key.pem'))
        signer=WorkerClosureSigner(issuer_id='worker',key_id='key',identity=worker,private_key=self.f.key)
        token='synthetic-worker-bearer-0123456789'
        boundary=DirectWorkerBoundary(ledger=ledger,spool=spool,manifest=self.f.handoff.manifest,item=item,
            derived_ids=[],expected_worker=worker,engine_id=engine,coordinator_origin=coordinator_origin,
            gate_generation=1,bearer=token,signer=signer)
        self.server=create_direct_worker_server(boundary,'127.0.0.1',0,ssl_context=context,idle_timeout=2)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.addCleanup(self.stop_server)
        sender=DirectWorkerSender(origin='https://127.0.0.1:'+str(self.server.server_port),connect_ip='127.0.0.1',
            ca_file=root/'fixture-ca.pem',bearer=token,manifest=self.f.handoff.manifest,item=item,
            derived_ids=[],expected_worker=worker,issuer_id='worker',key_id='key',worker_public_key=self.f.key.public_key(),
            idle_timeout=2,total_timeout=5)
        self.f.sender=sender
        self.f.handoff=DirectHandoff(self.f.fixture.store,sender,principal='viewer',bearer=self.f.token)

    def stop_server(self):
        self.server.shutdown();self.server.server_close();self.thread.join(3)
        self.assertFalse(self.thread.is_alive())

    def test_authenticated_journal_to_real_tls_worker_and_signed_local_closure(self):
        obj=self.f.obj;token=self.f.token;handoff=self.f.handoff
        result=handoff.send_object(token,protected_transport=True,object_id=obj['id'],
                                   byte_size=3,sha256=obj['sha256'],reader=io.BytesIO(b'abc'))
        self.assertTrue(result['localObjectDurable']);self.assertFalse(result['sourceCustodyAccepted'])
        receipt=handoff.complete(token,protected_transport=True)
        self.assertFalse(receipt['closure']['sourceCustodyAccepted'])
        self.assertFalse(receipt['closure']['processingAuthorized'])
        self.assertEqual(handoff.complete(token,protected_transport=True),receipt)

    def test_lost_real_worker_response_retains_intent_not_viewer_ack(self):
        from unittest.mock import patch
        sender=self.f.sender;original=sender._response
        def drop(*args):
            original(*args);raise DirectDeliveryUncertain('synthetic lost response')
        with patch.object(sender,'_response',side_effect=drop):
            with self.assertRaises(DirectDeliveryUncertain):
                self.f.handoff.send_object(self.f.token,protected_transport=True,object_id=self.f.obj['id'],
                    byte_size=3,sha256=self.f.obj['sha256'],reader=io.BytesIO(b'abc'))
        status=self.f.handoff.status(self.f.token,protected_transport=True)
        self.assertEqual(status['uncertainObjects'],1);self.assertFalse(status['sourceCustodyAccepted'])

    def test_lost_real_closure_response_stays_pending(self):
        from unittest.mock import patch
        self.f.handoff.send_object(self.f.token,protected_transport=True,object_id=self.f.obj['id'],
            byte_size=3,sha256=self.f.obj['sha256'],reader=io.BytesIO(b'abc'))
        original=self.f.sender._response
        def drop(*args):
            original(*args);raise DirectDeliveryUncertain('synthetic lost closure response')
        with patch.object(self.f.sender,'_response',side_effect=drop):
            with self.assertRaises(DirectDeliveryUncertain):
                self.f.handoff.complete(self.f.token,protected_transport=True)
        self.assertEqual(self.f.handoff.status(self.f.token,protected_transport=True)['state'],'closure_pending')
        with self.assertRaisesRegex(Conflict,'uncertain'):
            self.f.handoff.complete(self.f.token,protected_transport=True)


if __name__=='__main__':unittest.main()
