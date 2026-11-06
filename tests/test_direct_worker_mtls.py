"""Disposable loopback TLS identity tests; no enrolled trust is assumed."""
import datetime
import hashlib
import http.client
import ssl
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from processing.direct_worker_http import create_direct_worker_server
from processing.direct_worker_sender import DirectWorkerSender
from processing.reference_tls import verified_client_context
from tests.test_direct_worker_http import DirectWorkerHTTPTests


class DirectMutualTLS(unittest.TestCase):
    def test_client_identity_mapping_and_sender_certificate(self):
        fixture = DirectWorkerHTTPTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        root = Path(fixture.temp.name)
        ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'fixture-client-ca')])
        now = datetime.datetime.now(datetime.timezone.utc)
        ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
              .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
              .not_valid_before(now-datetime.timedelta(minutes=1)).not_valid_after(now+datetime.timedelta(days=1))
              .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
              .sign(ca_key, hashes.SHA256()))
        (root/'client-ca.pem').write_bytes(ca.public_bytes(serialization.Encoding.PEM))
        def client(name):
            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            cert = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,name)]))
                    .issuer_name(ca_name).public_key(key.public_key()).serial_number(x509.random_serial_number())
                    .not_valid_before(now-datetime.timedelta(minutes=1)).not_valid_after(now+datetime.timedelta(days=1))
                    .sign(ca_key, hashes.SHA256()))
            certpath=root/(name+'.pem'); keypath=root/(name+'.key')
            certpath.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
            keypath.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                                  serialization.NoEncryption()))
            keypath.chmod(0o600)
            return certpath,keypath,hashlib.sha256(cert.public_bytes(serialization.Encoding.DER)).hexdigest()
        good,key,fingerprint=client('good'); other,other_key,_=client('other')
        server_tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);server_tls.minimum_version=ssl.TLSVersion.TLSv1_2
        server_tls.load_cert_chain(str(root/'fixture-cert.pem'),str(root/'fixture-key.pem'))
        server_tls.load_verify_locations(cafile=str(root/'client-ca.pem'));server_tls.verify_mode=ssl.CERT_REQUIRED
        server=create_direct_worker_server(fixture.boundary,'127.0.0.1',0,ssl_context=server_tls,
                                           client_principals={fingerprint:'coordinator-a'})
        thread=threading.Thread(target=server.serve_forever);thread.start()
        self.addCleanup(lambda: (server.shutdown(),server.server_close(),thread.join(3)))
        def request(cert=None,key=None):
            context=verified_client_context(str(root/'fixture-ca.pem'))
            if cert: context.load_cert_chain(str(cert),str(key))
            conn=http.client.HTTPSConnection('127.0.0.1',server.server_port,context=context,timeout=3)
            try:
                conn.request('POST','/direct/v1/complete',body=b'',headers=fixture.headers)
                response=conn.getresponse();response.read();return response.status
            finally:conn.close()
        with patch.object(fixture.boundary,'complete',side_effect=lambda *_a,**_k:
                          {'principal':server._local.peer_principal}) as handler:
            self.assertEqual(request(good,key),200)
            for cert,keyfile in ((None,None),(other,other_key)):
                with self.assertRaises((ssl.SSLError,OSError,http.client.HTTPException)):
                    request(cert,keyfile)
            self.assertEqual(handler.call_count,1)
        sender=DirectWorkerSender.__new__(DirectWorkerSender)
        sender._configure_transport(origin='https://127.0.0.1:'+str(server.server_port),connect_ip='127.0.0.1',
            ca_file=str(root/'fixture-ca.pem'),bearer=fixture.token,max_object_bytes=1024,response_limit=1024,
            idle_timeout=2,total_timeout=3,client_cert_file=str(good),client_key_file=str(key))
        with patch.object(fixture.boundary,'complete',return_value={'ok':True}):
            connection,guard=sender._connect(time.monotonic()+3)
            try:
                connection.request('POST','/direct/v1/complete',body=b'',headers=fixture.headers)
                response=connection.getresponse();response.read();self.assertEqual(response.status,200)
                self.assertTrue(guard.finish())
            finally: connection.close()
        with self.assertRaises(ValueError):
            sender._configure_transport(origin='https://127.0.0.1:'+str(server.server_port),connect_ip='127.0.0.1',
                ca_file=str(root/'fixture-ca.pem'),bearer=fixture.token,max_object_bytes=1024,response_limit=1024,
                idle_timeout=2,total_timeout=3,client_cert_file=str(good))

    def test_mapping_requires_mutual_tls(self):
        fixture = DirectWorkerHTTPTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2
        with self.assertRaises(ValueError):
            create_direct_worker_server(fixture.boundary,'127.0.0.1',0,ssl_context=context,client_principals={'a'*64:'coordinator'})
