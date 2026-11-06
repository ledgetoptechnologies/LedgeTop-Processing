"""Linux-only disabled preparation; no listener or network side effect."""
import json
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from ops.qualify_enrolled_phase_transport import configuration_material
from processing.direct_protected_transport import load_disabled_transport,prepare_disabled_sender

@unittest.skipUnless(os.environ.get('LTDS_SECURE_TEST_ROOT'),'dedicated private mount required')
class ProtectedTransportTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(dir=os.environ['LTDS_SECURE_TEST_ROOT']);self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.root.chmod(0o700)
        files=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))['configurationFiles']
        for name in ('fixture-ca.pem','fixture-cert.pem','fixture-key.pem'):
            (self.root/name).write_bytes(files[name]);(self.root/name).chmod(0o600)
        self.bearer=self.root/'bearer';self.bearer.write_text('synthetic-private-bearer-'+'x'*40);self.bearer.chmod(0o600)
        key=Ed25519PrivateKey.generate().public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        self.node=dict(origin='https://worker.example:9443',connectIp='127.0.0.1',caFile=str(self.root/'fixture-ca.pem'),
            clientCertFile=str(self.root/'fixture-cert.pem'),clientKeyFile=str(self.root/'fixture-key.pem'),bearerFile=str(self.bearer),
            issuerId='worker',keyId='worker-key',workerPublicKeyPem=key)
        self.config=self.root/'config.json';self.write()
    def write(self):
        self.config.write_text(json.dumps({'schemaVersion':6,'directWorkers':{'enabled':False,'nodes':{'cluster':self.node}}}));self.config.chmod(0o600)
    def load(self):
        return load_disabled_transport(str(self.config),'cluster',expected_config_sha256=hashlib.sha256(self.config.read_bytes()).hexdigest())
    def test_loads_exact_protected_bytes_without_enabling(self):
        result=self.load()
        self.assertFalse(result);self.assertFalse(result.enabled)
        self.assertEqual((result.origin,result.connect_ip),('https://worker.example:9443','127.0.0.1'))
        self.assertNotIn('synthetic-private-bearer',repr(result))
        self.assertTrue(result._context.check_hostname)
    def test_rejects_symlink_and_writable_ancestor(self):
        target=self.root/'alias';target.symlink_to(self.bearer);self.node['bearerFile']=str(target);self.write()
        with self.assertRaises((ValueError,OSError)):self.load()
        self.node['bearerFile']=str(self.bearer);self.write();self.root.chmod(0o777)
        try:
            with self.assertRaises(ValueError):self.load()
        finally:self.root.chmod(0o700)
    def test_refuses_enabled_and_public_secret(self):
        self.bearer.chmod(0o644)
        with self.assertRaises(ValueError):self.load()

    def test_fifo_secret_is_rejected_without_waiting_for_writer(self):
        fifo=self.root/'pipe';os.mkfifo(fifo,0o600)
        self.node['bearerFile']=str(fifo);self.write()
        with self.assertRaises(ValueError):self.load()
        with self.assertRaises(ValueError):
            load_disabled_transport(str(self.config),'cluster',expected_config_sha256='0'*64)
        self.bearer.chmod(0o600)
        value=json.loads(self.config.read_text());value['directWorkers']['enabled']=True
        self.config.write_text(json.dumps(value));self.config.chmod(0o600)
        with self.assertRaises(ValueError):self.load()

    def test_prepares_sender_only_from_config_and_matching_worker_pin(self):
        from tests.test_direct_worker_receiver_v2 import WorkerV2Tests
        from processing.direct_v2_reservation import DirectV2Reservations
        f=WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        self.node['workerPublicKeyPem']=f.proof_key.public_key().public_bytes(
            serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        self.node['issuerId']='worker-issuer';self.node['keyId']='worker-key';self.write()
        registry=object.__new__(DirectV2Reservations)
        registry._workers={'cluster':(f.worker,'worker-issuer','worker-key',f.proof_key.public_key())}
        kwargs=dict(expected_config_sha256=hashlib.sha256(self.config.read_bytes()).hexdigest(),
            reservations=registry,inventory=f.index,item=f.item,reservation_binding=f.reservation)
        result=prepare_disabled_sender(str(self.config),'cluster',**kwargs)
        self.assertFalse(result);self.assertFalse(result.enabled)
        self.assertEqual(result._sender._host,'worker.example')
        with self.assertRaisesRegex(RuntimeError,'disabled sender'):
            result._sender._connect(999999999)
        from processing.direct_worker_sender_v2 import DirectWorkerSenderV2
        transport=self.load()
        with self.assertRaisesRegex(ValueError,'protected transport'):
            DirectWorkerSenderV2(origin=transport.origin,connect_ip=transport.connect_ip,ca_file=None,
                bearer=transport._bearer,inventory=f.index,item=f.item,reservation_binding=f.reservation,
                expected_worker=f.worker,issuer_id='worker-issuer',key_id='worker-key',
                worker_public_key=f.proof_key.public_key(),enrolled_transport=transport)
        self.assertNotIn('synthetic-private-bearer',repr(result))
        registry._workers['cluster']=(f.worker,'wrong','worker-key',f.proof_key.public_key())
        with self.assertRaises(ValueError):prepare_disabled_sender(str(self.config),'cluster',**kwargs)
