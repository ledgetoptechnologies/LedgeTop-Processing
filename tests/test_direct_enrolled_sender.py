"""Synthetic protected sender to a task-owned mTLS worker boundary."""
import hashlib
import io
import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
import uuid
from contextlib import closing, nullcontext
from pathlib import Path
from unittest.mock import patch
from cryptography.hazmat.primitives import serialization
from processing.direct_protected_transport import prepare_enrolled_sender
from processing.direct_v2_reservation import DirectV2Reservations
from processing.direct_worker_grant_v2 import DirectWorkerGrant
from processing.direct_worker_http import create_direct_worker_server
from tests import test_direct_protected_transport as protected_fixture
from tests import test_direct_snapshot_http as tls_fixture
from tests import test_direct_worker_receiver_v2 as worker_fixture

@unittest.skipUnless(os.environ.get('LTDS_SECURE_TEST_ROOT') and os.environ.get('LTDS_TEST_ROOT_ENROLLMENT_HANDOFF')=='1',
                     'root-supervised nonroot fixture required')
class EnrolledSenderTests(unittest.TestCase):
    def test_pinned_sender_grants_and_streams_to_actual_mtls_boundary(self):
        f=worker_fixture.WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        h=tls_fixture.SnapshotListenerTests();h.setUp();self.addCleanup(h.doCleanups)
        secure=protected_fixture.ProtectedTransportTests();secure.setUp();self.addCleanup(secure.doCleanups)
        for src,name in ((h.good,'client.pem'),(h.goodkey,'client.key'),(h.root/'fixture-ca.pem','server-ca.pem')):
            target=secure.root/name;target.write_bytes(src.read_bytes());target.chmod(0o600)
        secure.bearer.write_text(f.bearer);secure.bearer.chmod(0o600)
        server=create_direct_worker_server(f.boundary,'127.0.0.1',0,ssl_context=h.context,idle_timeout=2,
            client_principals={h.fingerprint:'coord-a'},
            reservation_grant=DirectWorkerGrant(f.boundary,coordinator_principal='coord-a'))
        thread=threading.Thread(target=server.serve_forever);thread.start()
        try:
            secure.node.update(origin='https://127.0.0.1:'+str(server.server_port),
                caFile=str(secure.root/'server-ca.pem'),clientCertFile=str(secure.root/'client.pem'),
                clientKeyFile=str(secure.root/'client.key'),issuerId='worker-issuer',keyId='worker-key',
                workerPublicKeyPem=f.proof_key.public_key().public_bytes(serialization.Encoding.PEM,
                    serialization.PublicFormat.SubjectPublicKeyInfo).decode())
            secure.config.write_text(json.dumps({'schemaVersion':6,'directWorkers':{'enabled':True,
                'nodes':{'cluster':secure.node}}}));secure.config.chmod(0o600)
            sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
            pins=dict(ca=sha(secure.root/'server-ca.pem'),certificate=sha(secure.root/'client.pem'),
                privateKey=sha(secure.root/'client.key'),bearer=sha(secure.bearer))
            registry=object.__new__(DirectV2Reservations)
            registry._workers={'cluster':(f.worker,'worker-issuer','worker-key',f.proof_key.public_key())}
            sender=prepare_enrolled_sender(str(secure.config),'cluster',expected_config_sha256=sha(secure.config),
                expected_credential_sha256=pins,reservations=registry,inventory=f.index,item=f.item,
                reservation_binding=f.reservation)
            grant=sender.authorize_reservation(f.challenge)
            self.assertFalse(grant['sourceCustodyAccepted'])
            from tests.test_reference_source_intake import GCP_ID,IMAGE_ID,ATTEMPT
            observed=sender.put(GCP_ID,io.BytesIO(b''))
            self.assertTrue(observed['localObjectDurable']);self.assertFalse(observed['sourceCustodyAccepted'])
            with self.assertRaises(ValueError):
                prepare_enrolled_sender(str(secure.config),'cluster',expected_config_sha256=sha(secure.config),
                    expected_credential_sha256=dict(pins,bearer='0'*64),reservations=registry,inventory=f.index,
                    item=f.item,reservation_binding=f.reservation)
            # Exercise the coordinator's exact durable reservation-to-handoff
            # path against the same real mTLS boundary. The preparation proof
            # is the already-created synthetic fixture, not a live prep call.
            from processing.prototype import Queue
            from processing.reference_store import ReferenceStore,PROFILE
            from processing.reference_source_intake import validate_source_intake
            from processing.direct_preparation_scheduler import DirectPreparationScheduler
            from processing.direct_preparation_journal import prepare_once
            with tempfile.TemporaryDirectory() as directory:
                with closing(sqlite3.connect(Path(directory)/'jobs.sqlite3')) as db,db:
                    db.execute('CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY,authority_id TEXT NOT NULL)')
                    db.execute('INSERT INTO coordinator_identity VALUES(1,?)',(f.worker['authorityId'],))
                queue=Queue(directory,reference_enabled=True);queue.allowed_profiles.add(PROFILE)
                try:
                    store=ReferenceStore(queue,source_routing_keys=f.options['trusted_source_keys']);opts=f.options
                    checked=validate_source_intake(f.payload,**{k:v for k,v in opts.items() if k!='authenticated_principal'})
                    store.submit_source_intake(checked,capability_fingerprint=opts['capability_fingerprint'],
                        options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'],
                        source_spool=None,trusted_source_keys=opts['trusted_source_keys'],authority_id=opts['authority_id'],
                        ingress_mode='direct-v2',authenticated_principal='viewer-staging')
                    # Each node has a separate signed registry identity. The
                    # synthetic upload endpoint is used for cluster only.
                    config=json.loads(secure.config.read_text());config['directWorkers']['nodes']['main']=secure.node
                    secure.config.write_text(json.dumps(config));secure.config.chmod(0o600)
                    transfer=dict(configPath=str(secure.config),configSha256=sha(secure.config),credentialSha256=pins)
                    enrolled={n:dict(**transfer,gateGeneration=1,outputBudget=100,ttlSeconds=10,transfer=dict(transfer)) for n in queue.node_ids}
                    workers={n:dict(identity=dict(f.worker,nodeId=n),issuerId='worker-issuer',keyId='worker-key',
                        publicKeyPem=secure.node['workerPublicKeyPem']) for n in queue.node_ids}
                    enrollment_json=json.dumps(dict(schemaVersion=1,trustedWorkers=workers,enrollment=enrolled))
                    print('TEST_ENROLLMENT '+enrollment_json,flush=True)
                    enrolled_file=Path(sys.stdin.readline().strip())
                    self.assertEqual((os.geteuid(),os.getegid(),os.getgroups()),(10001,10001,[]))
                    self.assertEqual(enrolled_file.stat().st_uid,0)
                    self.assertTrue(all(parent.stat().st_uid==0 and not parent.stat().st_mode & 0o022
                                        for parent in enrolled_file.parents))
                    with nullcontext():
                        scheduler=DirectPreparationScheduler.from_protected_file(store,str(enrolled_file))
                        with patch.object(scheduler._clients['cluster'],'prepare',return_value=f.readiness):
                            prepare_once(scheduler.reservations,scheduler._clients['cluster'],payload=f.payload,item=f.item,
                                principal='viewer-staging',challenge_id=f.challenge,gate_generation=1,
                                capability_fingerprint=opts['capability_fingerprint'],options_fingerprint=opts['options_fingerprint'],
                                allowed_odm_options=opts['allowed_odm_options'])
                        scheduler.reservations.reserve(payload=f.payload,authenticated_principal='viewer-staging',item=f.item,
                            expected_worker=f.worker,readiness_proof=f.readiness,challenge_id=f.challenge,gate_generation=1,
                            issuer_id='worker-issuer',key_id='worker-key',worker_public_key=f.proof_key.public_key(),
                            trusted_source_keys=opts['trusted_source_keys'],capability_fingerprint=opts['capability_fingerprint'],
                            options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'])
                        handoff=scheduler.open_reserved_transfer(f.item['reservationId'],allowed_odm_options=set())
                        self.assertFalse(handoff.authorize()['sourceCustodyAccepted'])
                        sent=handoff.send(GCP_ID,io.BytesIO(b''))
                        self.assertTrue(sent['localObjectDurable']);self.assertFalse(sent['sourceCustodyAccepted'])
                        from processing.maintenance import Conflict
                        with self.assertRaises(Conflict):handoff.complete()
                        wrong=io.BytesIO(b'abc')
                        with self.assertRaises(ValueError):handoff.send('unknown-object',wrong)
                        self.assertTrue(wrong.closed)
                        from processing.direct_worker_sender import DirectDeliveryUncertain
                        class BadClose(io.BytesIO):
                            def close(self):
                                super().close()
                                raise OSError('synthetic close failure')
                        failed=BadClose(b'abc')
                        with self.assertRaises(DirectDeliveryUncertain) as error:
                            handoff.send('unknown-object',failed)
                        self.assertIsInstance(error.exception.__cause__,ValueError)
                        self.assertTrue(failed.closed)
                        transport_error=DirectDeliveryUncertain('synthetic lost reply')
                        with patch.object(handoff._handoff,'send_object',side_effect=transport_error):
                            with self.assertRaises(DirectDeliveryUncertain) as error:
                                handoff.send(IMAGE_ID,BadClose(b'abc'))
                        self.assertIs(error.exception.__cause__,transport_error)
                        first=BadClose(b'abc')
                        with self.assertRaises(DirectDeliveryUncertain) as error:
                            handoff.send(IMAGE_ID,first)
                        self.assertIsInstance(error.exception.__cause__,OSError)
                        self.assertTrue(first.closed)
                        replay_bad=BadClose(b'abc')
                        with self.assertRaises(DirectDeliveryUncertain):handoff.send(IMAGE_ID,replay_bad)
                        self.assertTrue(replay_bad.closed)
                        for object_id,data in ((IMAGE_ID,b'abc'),('gcp_'+ATTEMPT,b'1,2,3\n')):
                            reader=io.BytesIO(data);observed=handoff.send(object_id,reader)
                            self.assertTrue(reader.closed)
                            self.assertTrue(observed['localObjectDurable']);self.assertFalse(observed['sourceCustodyAccepted'])
                        closure=handoff.complete()
                        self.assertFalse(closure['closure']['sourceCustodyAccepted'])
                        secure.config.write_text(secure.config.read_text()+' ')
                        with self.assertRaises(ValueError):handoff.authorize()
                        denied=io.BytesIO(b'abc')
                        with self.assertRaises(ValueError):handoff.send(IMAGE_ID,denied)
                        self.assertTrue(denied.closed)
                finally:queue.close()
        finally:
            server.shutdown();server.server_close();thread.join(5)
        self.assertFalse(thread.is_alive())
