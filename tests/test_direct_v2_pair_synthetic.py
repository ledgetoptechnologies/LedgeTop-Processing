"""Synthetic signed admission→held reservation→worker TLS local closure.

No real Viewer or ODM engine runs, and local closure is not a Viewer ACK.
"""
import io
import base64
import copy
import sqlite3
import ssl
import tempfile
import threading
import unittest
import uuid
import hashlib
import datetime
from contextlib import closing
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from ops.qualify_enrolled_phase_transport import configuration_material
from processing.direct_v2_reservation import DirectV2Reservations
from processing.direct_worker_http import create_direct_worker_server
from processing.direct_worker_sender_v2 import DirectWorkerSenderV2
from processing.prototype import Queue
from processing.reference_source_intake import validate_source_intake
from processing.reference_store import PROFILE, ReferenceStore
from processing.maintenance import Conflict
from tests import test_direct_worker_receiver_v2 as fixture
from tests import test_reference_source_intake as source_fixture


class DirectV2PairSyntheticTests(unittest.TestCase):
    def test_signed_admission_reservation_and_tls_local_closure(self):
        self.run_pair(False)

    def test_lost_local_closure_response_remains_held(self):
        self.run_pair(True)

    def test_lost_object_observation_recovers_without_reupload(self):
        self.run_pair(False,True)

    def test_lost_retention_observation_recovers_without_false_ack(self):
        self.run_pair(False,False,True)

    def run_pair(self, lose_closure, lose_object=False, lose_retention=False):
        f=fixture.WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with closing(sqlite3.connect(root/'jobs.sqlite3')) as db, db:
                db.execute('CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY,authority_id TEXT NOT NULL)')
                db.execute('INSERT INTO coordinator_identity VALUES(1,?)',(f.worker['authorityId'],))
            queue=Queue(root,reference_enabled=True);self.addCleanup(queue.close)
            queue.allowed_profiles.add(PROFILE);store=ReferenceStore(queue,source_routing_keys=f.options['trusted_source_keys'])
            opts=f.options
            validated=validate_source_intake(f.payload,environment='staging',
                capability_fingerprint=opts['capability_fingerprint'],options_fingerprint=opts['options_fingerprint'],
                allowed_odm_options=opts['allowed_odm_options'],trusted_source_keys=opts['trusted_source_keys'],
                authority_id=opts['authority_id'])
            job,created=store.submit_source_intake(validated,capability_fingerprint=opts['capability_fingerprint'],
                options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'],
                source_spool=None,trusted_source_keys=opts['trusted_source_keys'],authority_id=opts['authority_id'],
                ingress_mode='direct-v2',authenticated_principal='viewer-staging')
            self.assertTrue(created);self.assertEqual(job['status'],'held')
            workers={node:dict(identity=dict(f.worker,nodeId=node),issuerId='worker-issuer',keyId='worker-key',
                               publicKey=f.proof_key.public_key()) for node in queue.node_ids}
            reservations=DirectV2Reservations(store,trusted_workers=workers)
            reservation_request=dict(payload=f.payload,
                authenticated_principal='viewer-staging',item=f.item,expected_worker=f.worker,
                readiness_proof=f.readiness,challenge_id=f.challenge,gate_generation=1,
                issuer_id='worker-issuer',key_id='worker-key',worker_public_key=f.proof_key.public_key(),
                trusted_source_keys=opts['trusted_source_keys'],capability_fingerprint=opts['capability_fingerprint'],
                options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'])
            material=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))['configurationFiles']
            for name in ('fixture-ca.pem','fixture-cert.pem','fixture-key.pem'):(root/name).write_bytes(material[name])
            context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2
            context.load_cert_chain(str(root/'fixture-cert.pem'),str(root/'fixture-key.pem'))
            from cryptography import x509
            from cryptography.hazmat.primitives import hashes,serialization
            from cryptography.hazmat.primitives.asymmetric import rsa
            from cryptography.x509.oid import NameOID
            now=datetime.datetime.now(datetime.timezone.utc)
            client_key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
            name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'synthetic-coordinator')])
            cert=(x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(client_key.public_key())
                .serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=1))
                .not_valid_after(now+datetime.timedelta(days=1)).add_extension(x509.BasicConstraints(ca=True,path_length=None),critical=True)
                .sign(client_key,hashes.SHA256()))
            cert_path=root/'client.pem'; key_path=root/'client.key'
            cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
            key_path.write_bytes(client_key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
            key_path.chmod(0o600)
            context.load_verify_locations(cafile=str(cert_path));context.verify_mode=ssl.CERT_REQUIRED
            from processing.direct_worker_grant_v2 import DirectWorkerGrant
            server=create_direct_worker_server(f.boundary,'127.0.0.1',0,ssl_context=context,idle_timeout=2,
                client_principals={hashlib.sha256(cert.public_bytes(serialization.Encoding.DER)).hexdigest():'coord-a'},
                reservation_grant=DirectWorkerGrant(f.boundary,coordinator_principal='coord-a'))
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            try:
                sender=DirectWorkerSenderV2(origin='https://127.0.0.1:'+str(server.server_port),
                    connect_ip='127.0.0.1',ca_file=str(root/'fixture-ca.pem'),bearer=f.bearer,
                    inventory=f.index,item=f.item,reservation_binding=f.reservation,expected_worker=f.worker,
                    issuer_id='worker-issuer',key_id='worker-key',worker_public_key=f.proof_key.public_key(),
                    idle_timeout=2,total_timeout=5,client_cert_file=str(cert_path),client_key_file=str(key_path))
                from processing.direct_v2_scheduler_bridge import DirectSchedulerBridge, WorkerHoldUncertain
                from processing.direct_v2_handoff_journal import DirectV2Handoff
                from processing.scratch_policy import ScratchLedger, PolicyConflict
                token='synthetic-viewer-handoff-token-0123456789'
                bridge=DirectSchedulerBridge(reservations,sender,principal='viewer-staging',bearer=token)
                handoff=bridge.handoff
                with self.assertRaises(Conflict):
                    bridge.reserve_and_grant('incorrect-bearer',protected_transport=True,request=reservation_request)
                with queue._db() as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_v2_reservations').fetchone()[0],0)
                with patch.object(sender,'_issuer','wrong-worker'), patch.object(reservations,'reserve') as reserve:
                    with self.assertRaises(WorkerHoldUncertain):
                        bridge.reserve_and_grant(token,protected_transport=True,request=reservation_request)
                    reserve.assert_not_called()
                from processing.direct_worker_sender import DirectDeliveryUncertain
                original_grant=sender.authorize_reservation
                def lost_grant(*args,**kwargs):
                    original_grant(*args,**kwargs)
                    raise DirectDeliveryUncertain('synthetic lost grant reply')
                with patch.object(sender,'authorize_reservation',side_effect=lost_grant):
                    with self.assertRaises(WorkerHoldUncertain):
                        bridge.reserve_and_grant(token,protected_transport=True,request=reservation_request)
                replay=bridge.resume_committed_grant(token,protected_transport=True)
                self.assertEqual(replay['nodeId'],'cluster')
                self.assertFalse(replay['sourceCustodyAccepted'])
                original_check=handoff._check;checks=[]
                def lost_postcheck(db):
                    checks.append(True)
                    if len(checks)==2:raise RuntimeError('synthetic coordinator journal failure')
                    return original_check(db)
                with patch.object(handoff,'_check',side_effect=lost_postcheck):
                    with self.assertRaises(DirectDeliveryUncertain):
                        handoff.authorize_worker(token,protected_transport=True)
                self.assertEqual(len(checks),2)
                original_operation=queue.coordinator_operation
                @contextmanager
                def fail_on_operation_exit(kind):
                    with original_operation(kind):
                        yield
                        raise RuntimeError('synthetic operation retirement failure')
                with patch.object(queue,'coordinator_operation',side_effect=fail_on_operation_exit):
                    with self.assertRaises(DirectDeliveryUncertain):
                        handoff.authorize_worker(token,protected_transport=True)
                with patch.object(sender,'_client_identity_configured',False):
                    with self.assertRaises(ValueError):
                        handoff.authorize_worker(token,protected_transport=True)
                saved=handoff.authorize_worker(token,protected_transport=True)
                self.assertEqual(saved['operationId'],f.challenge)
                self.assertFalse(saved['sourceCustodyAccepted'])
                def assert_retained_after_reopen():
                    scratch_id=f.observation['scratchReservationId']
                    reopened=ScratchLedger(f.root/'private'/'ledger.sqlite',f.root,
                        capacity_probe=lambda _:1000000,clock=lambda:1000,safety_floor_bytes=100)
                    with reopened._transaction() as db:
                        self.assertIsNotNone(db.execute('SELECT 1 FROM direct_custody_fences WHERE reservation=?',(scratch_id,)).fetchone())
                    with self.assertRaisesRegex(PolicyConflict,'custody is retained'):
                        reopened.settle_unstarted(scratch_id,transfer_ended=True)
                def assert_signed_retention_replay():
                    if lose_retention:
                        # A full journal applies backpressure; it never evicts
                        # an existing receipt or reports source custody.
                        with patch('processing.direct_v2_retention_journal.MAX_OBSERVATIONS',0):
                            with self.assertRaisesRegex(Conflict,'capacity exceeded'):
                                handoff.observe_retention(token,protected_transport=True)
                        self.assertEqual(queue.get(job['jobId'])['status'],'held')
                    if lose_retention:
                        from processing.direct_worker_sender import DirectDeliveryUncertain
                        original_observe=sender.observe_retention
                        def lost(*args,**kwargs):
                            original_observe(*args,**kwargs)
                            raise DirectDeliveryUncertain('synthetic lost retention observation')
                        with patch.object(sender,'observe_retention',side_effect=lost):
                            with self.assertRaises(DirectDeliveryUncertain):
                                handoff.observe_retention(token,protected_transport=True)
                        with queue._db() as db:
                            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_v2_retention_observations').fetchone()[0],0)
                    observed=handoff.observe_retention(token,protected_transport=True)
                    self.assertEqual((observed['protocol'],observed['schemaVersion']),('ltds-direct-worker-retention-observation-v2',2))
                    self.assertTrue(observed['retentionFenceObserved'])
                    self.assertFalse(observed['sourceCustodyAccepted'])
                    self.assertFalse(observed['transferComplete'])
                    self.assertFalse(observed['processingAuthorized'])
                    from processing.direct_worker_receipt_v2 import verify_retention, ReceiptRejected
                    with queue._db() as db:
                        saved=db.execute('SELECT closure FROM direct_v2_handoff_sessions WHERE reservation=?',(f.item['reservationId'],)).fetchone()[0]
                    import json
                    signed=json.loads(saved)
                    from processing import reference_protocol as protocol
                    old={key:value for key,value in observed.items() if key!='signature'}
                    old['protocol']='ltds-direct-worker-retention-observation-v1';old['schemaVersion']=1
                    old['retentionFenceActive']=old.pop('retentionFenceObserved')
                    old['signature']=base64.urlsafe_b64encode(f.proof_key.sign(
                        b'LTDS-DIRECT-WORKER-RETENTION-OBSERVATION-V1\0'+protocol.canonical_json(old))).decode('ascii').rstrip('=')
                    with self.assertRaises(ReceiptRejected):
                        verify_retention(old,signed_closure=signed,expected_closure=sender._expected_closure(),
                            issuer_id=sender._issuer,key_id=sender._key,public_key=sender._public)
                    for key,value in (('sourceCustodyAccepted',True),('localClosureSha256','0'*64),
                                      ('workerClosureReceiptSha256','1'*64),('signature','A'*86)):
                        changed=copy.deepcopy(observed);changed[key]=value
                        with self.subTest(field=key),self.assertRaises(ReceiptRejected):
                            verify_retention(changed,signed_closure=signed,expected_closure=sender._expected_closure(),
                                issuer_id=sender._issuer,key_id=sender._key,public_key=sender._public)
                    restarted=DirectV2Handoff(store,sender,principal='viewer-staging',bearer=token)
                    with patch.object(sender,'observe_retention',side_effect=AssertionError('must replay persisted observation')):
                        self.assertEqual(restarted.observe_retention(token,protected_transport=True),observed)
                    with self.assertRaises(Conflict):
                        restarted.observe_retention('wrong-viewer-token-012345678901234',protected_transport=True)
                    # A prior version remains untouched and is not implicitly migrated.
                    with queue._db() as db:
                        db.execute('UPDATE coordinator_identity SET direct_v2_retention_version=1 WHERE singleton=1')
                    with self.assertRaises(Conflict):
                        restarted.observe_retention(token,protected_transport=True)
                    with queue._db() as db:
                        self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_v2_retention_observations').fetchone()[0],1)
                        db.execute('UPDATE coordinator_identity SET direct_v2_retention_version=2 WHERE singleton=1')
                    # Stored evidence cannot be replaced/deleted, and removing
                    # its guard makes the coordinator fail closed on replay.
                    with queue._db() as db:
                        with self.assertRaises(sqlite3.IntegrityError):
                            db.execute("UPDATE direct_v2_retention_observations SET observation=? WHERE reservation=?",
                                       (b'{}',f.item['reservationId']))
                        with self.assertRaises(sqlite3.IntegrityError):
                            db.execute("DELETE FROM direct_v2_retention_observations WHERE reservation=?",(f.item['reservationId'],))
                        db.execute('DROP TRIGGER direct_v2_retention_no_delete')
                    with self.assertRaises(Conflict):
                        restarted.observe_retention(token,protected_transport=True)
                    from processing.direct_v2_retention_journal import TRIGGERS
                    with queue._db() as db:
                        db.execute('DELETE FROM direct_v2_retention_observations WHERE reservation=?',(f.item['reservationId'],))
                        db.execute(TRIGGERS[2])
                    with self.assertRaises(Conflict):
                        restarted.observe_retention(token,protected_transport=True)
                with self.assertRaises(Conflict):
                    handoff.complete('invalid-viewer-token-012345678901234',protected_transport=True)
                for object_id,data in ((source_fixture.GCP_ID,b''),(source_fixture.IMAGE_ID,b'abc'),
                                       ('gcp_'+f.item['attemptId'],b'1,2,3\n')):
                    if lose_object:
                        from processing.direct_worker_sender import DirectDeliveryUncertain
                        original_put=sender.put
                        def lose_after_worker_commit(*args,**kwargs):
                            original_put(*args,**kwargs)
                            raise DirectDeliveryUncertain('synthetic lost object observation')
                        with patch.object(sender,'put',side_effect=lose_after_worker_commit):
                            with self.assertRaises(DirectDeliveryUncertain):
                                handoff.send_object(token,protected_transport=True,object_id=object_id,reader=io.BytesIO(data))
                        with queue._db() as db:
                            self.assertEqual(db.execute('SELECT state FROM direct_v2_handoff_objects').fetchone()[0],'intent')
                        handoff=DirectV2Handoff(store,sender,principal='viewer-staging',bearer=token)
                        with patch.object(sender,'put',side_effect=AssertionError('must not reupload')):
                            observed=handoff.reconcile_object(token,protected_transport=True,object_id=object_id)
                        lose_object=False
                    else:
                        observed=handoff.send_object(token,protected_transport=True,object_id=object_id,reader=io.BytesIO(data))
                    self.assertTrue(observed['localObjectDurable']);self.assertFalse(observed['sourceCustodyAccepted'])
                if lose_closure:
                    from processing.direct_worker_sender import DirectDeliveryUncertain
                    original=sender.complete
                    def lose_after_worker_commit():
                        original()
                        raise DirectDeliveryUncertain('synthetic lost closure observation')
                    with patch.object(sender,'complete',side_effect=lose_after_worker_commit):
                        with self.assertRaises(DirectDeliveryUncertain):handoff.complete(token,protected_transport=True)
                    with queue._db() as db:
                        self.assertEqual(db.execute('SELECT state FROM direct_v2_handoff_sessions').fetchone()[0],'closure_pending')
                    assert_retained_after_reopen()
                    handoff=DirectV2Handoff(store,sender,principal='viewer-staging',bearer=token)
                    recovered=handoff.complete(token,protected_transport=True)
                    self.assertFalse(recovered['closure']['sourceCustodyAccepted'])
                    with queue._db() as db:
                        self.assertEqual(db.execute('SELECT state FROM direct_v2_handoff_sessions').fetchone()[0],'closure_observed')
                    self.assertEqual(queue.get(job['jobId'])['status'],'held')
                    self.assertTrue((f.spool.root.parent/'direct-v2-worker-signed-closure.json').exists())
                    assert_signed_retention_replay()
                    return
                closure=handoff.complete(token,protected_transport=True)
                assert_retained_after_reopen()
                self.assertEqual(handoff.complete(token,protected_transport=True),closure)
                self.assertEqual((closure['closure']['sourceObjectCount'],closure['closure']['derivedObjectCount']),(2,1))
                self.assertFalse(closure['closure']['sourceCustodyAccepted'])
                with queue._db() as db:
                    self.assertEqual(db.execute("SELECT state FROM direct_v2_handoff_sessions").fetchone()[0],'closure_observed')
                    self.assertEqual(db.execute("SELECT count(*) FROM direct_v2_handoff_objects WHERE state='observed'").fetchone()[0],3)
                self.assertEqual(queue.get(job['jobId'])['status'],'held')
                assert_signed_retention_replay()
            finally:
                server.shutdown();server.server_close();thread.join(3);self.assertFalse(thread.is_alive())
                queue.close()
