"""Disposable real TLS sender/worker tests; no Viewer or deployed service."""
import io
from pathlib import Path
import socket
import ssl
import threading
import time
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
        options=dict(origin=self.origin,connect_ip='127.0.0.1',ca_file=self.ca,bearer=f.token,manifest=f.f.manifest,
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
        with self.assertRaises(DirectDeliveryUncertain):
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

    def test_post_effect_worker_errors_are_uncertain_not_rejection(self):
        original=self.fixture.boundary.put
        for error in (RuntimeError('after effect'),OSError('after effect')):
            def after_effect(*args, **kwargs):
                original(*args, **kwargs)
                raise error
            with self.subTest(error=type(error).__name__), \
                 patch.object(self.fixture.boundary,'put',side_effect=after_effect):
                with self.assertRaises(DirectDeliveryUncertain):
                    self.sender.put('object-1',io.BytesIO(self.fixture.f.data))
            self.assertTrue(self.sender.put('object-1',io.BytesIO(self.fixture.f.data))['replayed'])

    def test_unresponsive_tls_peer_cannot_exceed_transport_deadline(self):
        listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1)
        done=threading.Event()
        def stall():
            try:
                peer,_=listener.accept()
                with peer:done.wait(2)
            finally:listener.close()
        thread=threading.Thread(target=stall);thread.start()
        started=time.monotonic()
        try:
            sender=self.make_sender(origin='https://127.0.0.1:'+str(listener.getsockname()[1]),
                                    idle_timeout=2,total_timeout=0.3)
            with self.assertRaises(DirectDeliveryUncertain):
                sender.put('object-1',io.BytesIO(self.fixture.f.data))
            self.assertLess(time.monotonic()-started,1.5)
        finally:
            done.set();thread.join(3)
        self.assertFalse(thread.is_alive())

    def test_numeric_connect_address_is_mandatory(self):
        for bad in ('localhost','127.0.0.1%bad','999.2.3.4'):
            with self.subTest(address=bad),self.assertRaises(ValueError):self.make_sender(connect_ip=bad)

    def test_drip_fed_tls_response_cannot_extend_transport_deadline(self):
        root=Path(self.fixture.temp.name)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(root/'fixture-cert.pem'),str(root/'fixture-key.pem'))
        listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1)
        port=listener.getsockname()[1];done=threading.Event()
        def drip():
            try:
                peer,_=listener.accept()
                with context.wrap_socket(peer,server_side=True) as tls:
                    tls.settimeout(2);tls.recv(4096)
                    for byte in b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}':
                        if done.is_set():break
                        try:tls.sendall(bytes([byte]))
                        except OSError:break
                        time.sleep(0.08)
            finally:listener.close()
        thread=threading.Thread(target=drip);thread.start();started=time.monotonic()
        try:
            sender=self.make_sender(origin='https://127.0.0.1:'+str(port),idle_timeout=2,total_timeout=0.4)
            with self.assertRaises(DirectDeliveryUncertain):
                sender.put('object-1',io.BytesIO(self.fixture.f.data))
            self.assertLess(time.monotonic()-started,1.5)
        finally:
            done.set();thread.join(3)
        self.assertFalse(thread.is_alive())


if __name__=='__main__':unittest.main()
