"""Synthetic protected mTLS preparation client; no runtime enablement."""
import hashlib
import json
import ssl
import threading
import unittest
from unittest.mock import Mock
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from processing.direct_preparation_client import DirectPreparationClient
from processing.direct_snapshot_http import create_snapshot_server
from processing.direct_v2_reservation import DirectV2Reservations
from processing.direct_worker_sender import DirectDeliveryUncertain
from processing.reference_api import Response
from tests.test_direct_protected_transport import ProtectedTransportTests
from tests.test_direct_worker_receiver_v2 import WorkerV2Tests
from tests.test_direct_snapshot_http import SnapshotListenerTests

class PreparationClientTests(ProtectedTransportTests):
    def test_protected_client_to_real_mutual_tls_listener(self):
        f=WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        h=SnapshotListenerTests();h.setUp();self.addCleanup(h.doCleanups)
        for source,name in ((h.good,'client.pem'),(h.goodkey,'client.key'),(h.root/'fixture-ca.pem','server-ca.pem')):
            target=self.root/name;target.write_bytes(source.read_bytes());target.chmod(0o600)
        server_tls=h.context;fingerprint=h.fingerprint
        backend=Mock();backend._prepare_from_listener=Mock(return_value=Response(200,f.readiness))
        server=create_snapshot_server('127.0.0.1',0,enabled=True,ssl_context=server_tls,
            client_scopes={fingerprint:{'principal':'coordinator-a','environment':'staging'}},
            prepare_backend=backend)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        try:
            self.node.update(origin='https://127.0.0.1:'+str(server.server_port),caFile=str(self.root/'server-ca.pem'),
                clientCertFile=str(self.root/'client.pem'),clientKeyFile=str(self.root/'client.key'),
                issuerId='worker-issuer',keyId='worker-key',
                workerPublicKeyPem=f.proof_key.public_key().public_bytes(serialization.Encoding.PEM,
                    serialization.PublicFormat.SubjectPublicKeyInfo).decode())
            value={'schemaVersion':6,'directWorkers':{'enabled':True,'nodes':{'cluster':self.node}}}
            self.config.write_text(json.dumps(value));self.config.chmod(0o600)
            digest=hashlib.sha256(self.config.read_bytes()).hexdigest()
            pins={name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in (
                ('ca',self.root/'server-ca.pem'),('certificate',self.root/'client.pem'),
                ('privateKey',self.root/'client.key'),('bearer',self.bearer))}
            registry=object.__new__(DirectV2Reservations)
            registry._workers={'cluster':(f.worker,'worker-issuer','worker-key',f.proof_key.public_key())}
            with self.assertRaises(ValueError):
                DirectPreparationClient(config_path=str(self.config),config_sha256=digest,
                    credential_sha256=dict(pins,ca='0'*64),node='cluster',reservations=registry)
            client=DirectPreparationClient(config_path=str(self.config),config_sha256=digest,credential_sha256=pins,node='cluster',reservations=registry)
            self.assertEqual(client.prepare(source=f.payload,item=f.item,challenge_id=f.challenge,gate_generation=1),f.readiness)
            self.assertEqual(backend._prepare_from_listener.call_args.args[0],('coordinator-a','staging'))
            with self.assertRaises(DirectDeliveryUncertain):
                client.prepare(source=f.payload,item=f.item,challenge_id=f.challenge,gate_generation=2)
            registry._workers['cluster']=(f.worker,'other','worker-key',f.proof_key.public_key())
            with self.assertRaises(ValueError):
                DirectPreparationClient(config_path=str(self.config),config_sha256=digest,credential_sha256=pins,node='cluster',reservations=registry)
        finally:
            server.shutdown();server.server_close();thread.join(5)
        self.assertFalse(thread.is_alive())
