"""Disposable real TLS sender/worker tests; no Viewer or deployed service."""
import io
from pathlib import Path
import socket
import unittest
import uuid
from unittest.mock import patch

from ops.qualify_enrolled_phase_transport import configuration_material
from processing.direct_worker_sender import DirectDeliveryRejected, DirectDeliveryUncertain, DirectWorkerSender
from tests import test_direct_worker_http as http_fixture


class DirectWorkerSenderTests(unittest.TestCase):
    def setUp(self):
        self.fixture = http_fixture.DirectWorkerHTTPTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.origin = 'https://127.0.0.1:'+str(self.fixture.server.server_port)
        self.ca = Path(self.fixture.temp.name)/'fixture-ca.pem'
        self.sender = self.make_sender()

    def make_sender(self, **overrides):
        f=self.fixture
        options=dict(origin=self.origin,ca_file=self.ca,bearer=f.token,manifest=f.f.manifest,
            item=f.f.item,derived_ids=f.f.derived_ids,expected_worker=f.f.worker,
            issuer_id='worker',key_id='key',worker_public_key=f.signer._key.public_key(),
            idle_timeout=2,total_timeout=5)
        options.update(overrides)
        return DirectWorkerSender(**options)

    def upload(self):
        f=self.fixture.f
        return [self.sender.put('object-1',io.BytesIO(f.data)),
                self.sender.put('derived-1',io.BytesIO(f.derived_data))]

    def test_verified_tls_upload_and_worker_signature_are_not_viewer_custody(self):
        for result in self.upload():
            self.assertTrue(result['localObjectDurable'])
            self.assertFalse(result['sourceCustodyAccepted'])
        closure=self.sender.complete()
        self.assertFalse(closure['closure']['sourceCustodyAccepted'])
        self.assertFalse(closure['closure']['processingAuthorized'])

    def test_wrong_ca_or_hostname_never_publishes(self):
        root=Path(self.fixture.temp.name)
        other=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))['configurationFiles']
        wrong_ca=root/'wrong-ca.pem';wrong_ca.write_bytes(other['fixture-ca.pem'])
        with self.assertRaises(DirectDeliveryUncertain):
            self.make_sender(ca_file=wrong_ca).put('object-1',io.BytesIO(self.fixture.f.data))
        original=socket.getaddrinfo
        def resolve(host,port,*args,**kwargs):
            return original('127.0.0.1' if host=='wrong.invalid' else host,port,*args,**kwargs)
        with patch('socket.getaddrinfo',side_effect=resolve),self.assertRaises(DirectDeliveryUncertain):
            self.make_sender(origin='https://wrong.invalid:'+str(self.fixture.server.server_port)).put(
                'object-1',io.BytesIO(self.fixture.f.data))
        self.assertFalse((self.fixture.f.spool.root/self.fixture.f.item['jobId']).exists())

    def test_wrong_bearer_and_object_cap_refuse(self):
        with self.assertRaises(DirectDeliveryRejected):
            self.make_sender(bearer='wrong-but-valid-length-token-0123456789').put('object-1',io.BytesIO(self.fixture.f.data))
        with self.assertRaises(DirectDeliveryRejected):
            self.make_sender(max_object_bytes=1).put('object-1',io.BytesIO(self.fixture.f.data))
        self.assertFalse((self.fixture.f.spool.root/self.fixture.f.item['jobId']).exists())

    def test_lost_object_and_closure_response_requires_explicit_exact_replay(self):
        original=self.sender._response
        def drop(*args):
            original(*args)
            raise DirectDeliveryUncertain('synthetic lost reply')
        with patch.object(self.sender,'_response',side_effect=drop):
            with self.assertRaises(DirectDeliveryUncertain):
                self.sender.put('object-1',io.BytesIO(self.fixture.f.data))
        self.assertTrue(self.sender.put('object-1',io.BytesIO(self.fixture.f.data))['replayed'])
        self.sender.put('derived-1',io.BytesIO(self.fixture.f.derived_data))
        with patch.object(self.sender,'_response',side_effect=drop):
            with self.assertRaises(DirectDeliveryUncertain):self.sender.complete()
        self.assertFalse(self.sender.complete()['closure']['sourceCustodyAccepted'])

    def test_wrong_signing_key_rejects_completed_worker_receipt(self):
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        self.upload()
        with self.assertRaises(DirectDeliveryUncertain):
            self.make_sender(worker_public_key=Ed25519PrivateKey.generate().public_key()).complete()

    def test_oversize_response_is_uncertain_not_an_ack(self):
        with self.assertRaises(DirectDeliveryUncertain):
            self.make_sender(response_limit=1).put('object-1',io.BytesIO(self.fixture.f.data))
        # An explicit exact retry is distinguishable from new custody.
        self.assertTrue(self.sender.put('object-1',io.BytesIO(self.fixture.f.data))['replayed'])


if __name__=='__main__':unittest.main()
