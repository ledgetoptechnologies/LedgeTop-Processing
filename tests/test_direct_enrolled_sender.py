"""Synthetic protected sender to a task-owned mTLS worker boundary."""
import hashlib
import io
import json
import threading
import unittest
import uuid
from cryptography.hazmat.primitives import serialization
from processing.direct_protected_transport import prepare_enrolled_sender
from processing.direct_v2_reservation import DirectV2Reservations
from processing.direct_worker_grant_v2 import DirectWorkerGrant
from processing.direct_worker_http import create_direct_worker_server
from tests import test_direct_protected_transport as protected_fixture
from tests import test_direct_snapshot_http as tls_fixture
from tests import test_direct_worker_receiver_v2 as worker_fixture

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
            grant=sender.authorize_reservation(str(uuid.uuid4()))
            self.assertFalse(grant['sourceCustodyAccepted'])
            from tests.test_reference_source_intake import GCP_ID
            observed=sender.put(GCP_ID,io.BytesIO(b''))
            self.assertTrue(observed['localObjectDurable']);self.assertFalse(observed['sourceCustodyAccepted'])
            with self.assertRaises(ValueError):
                prepare_enrolled_sender(str(secure.config),'cluster',expected_config_sha256=sha(secure.config),
                    expected_credential_sha256=dict(pins,bearer='0'*64),reservations=registry,inventory=f.index,
                    item=f.item,reservation_binding=f.reservation)
        finally:
            server.shutdown();server.server_close();thread.join(5)
        self.assertFalse(thread.is_alive())
