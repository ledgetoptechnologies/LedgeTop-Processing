"""Synthetic mTLS snapshot listener identity tests; no source custody."""
import datetime
import hashlib
import http.client
import json
import ssl
import tempfile
import threading
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock,patch
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from ops.qualify_enrolled_phase_transport import configuration_material
from processing.direct_snapshot_http import create_snapshot_server
from processing.reference_api import Response
from processing.reference_tls import verified_client_context

class SnapshotListenerTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);self.root=Path(temp.name)
        files=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))['configurationFiles']
        for name in ('fixture-ca.pem','fixture-cert.pem','fixture-key.pem'):(self.root/name).write_bytes(files[name])
        ca_key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
        name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'synthetic-client-ca')]);now=datetime.datetime.now(datetime.timezone.utc)
        ca=(x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(ca_key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=1))
            .not_valid_after(now+datetime.timedelta(days=1)).add_extension(x509.BasicConstraints(ca=True,path_length=None),critical=True)
            .sign(ca_key,hashes.SHA256()))
        (self.root/'client-ca.pem').write_bytes(ca.public_bytes(serialization.Encoding.PEM))
        def client(label):
            key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
            cert=(x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,label)]))
                  .issuer_name(name).public_key(key.public_key()).serial_number(x509.random_serial_number())
                  .not_valid_before(now-datetime.timedelta(minutes=1)).not_valid_after(now+datetime.timedelta(days=1))
                  .sign(ca_key,hashes.SHA256()))
            certpath=self.root/(label+'.pem');keypath=self.root/(label+'.key')
            certpath.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
            keypath.write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
            return certpath,keypath,hashlib.sha256(cert.public_bytes(serialization.Encoding.DER)).hexdigest()
        self.good,self.goodkey,self.fingerprint=client('good');self.other,self.otherkey,_=client('other')
        self.context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);self.context.minimum_version=ssl.TLSVersion.TLSv1_2
        self.context.load_cert_chain(str(self.root/'fixture-cert.pem'),str(self.root/'fixture-key.pem'))
        self.context.load_verify_locations(cafile=str(self.root/'client-ca.pem'));self.context.verify_mode=ssl.CERT_REQUIRED
        self.scopes={self.fingerprint:{'principal':'viewer-a','environment':'staging'}}
        self.backend=Mock();self.backend.queue=None
        self.backend._dispatch_from_listener=Mock(return_value=Response(200,{'state':'not_custody'}))
        self.path=f'/internal/v2/reservations/{uuid.uuid4()}/source-snapshots/{uuid.uuid4()}/begin'

    def start(self,backend=True):
        server=create_snapshot_server('127.0.0.1',0,enabled=True,ssl_context=self.context,client_scopes=self.scopes,
                                      backend=self.backend if backend else None)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),thread.join(3)))
        return server

    def request(self,server,*,cert=None,key=None,method='POST',path=None,body=b'{}',headers=None):
        ctx=verified_client_context(str(self.root/'fixture-ca.pem'))
        if cert:ctx.load_cert_chain(str(cert),str(key))
        conn=http.client.HTTPSConnection('127.0.0.1',server.server_port,context=ctx,timeout=3)
        try:
            conn.request(method,path or self.path,body=body,headers=headers or {})
            response=conn.getresponse();return response.status,json.loads(response.read())
        finally:conn.close()

    def test_requires_explicit_enable_and_mutual_tls(self):
        with self.assertRaisesRegex(ValueError,'disabled'):create_snapshot_server('127.0.0.1',0)
        insecure=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        with self.assertRaisesRegex(ValueError,'mutual TLS'):
            create_snapshot_server('127.0.0.1',0,enabled=True,ssl_context=insecure,client_scopes=self.scopes)
        server=self.start()
        for cert,key in ((None,None),(self.other,self.otherkey)):
            with self.subTest(cert=cert),self.assertRaises((ssl.SSLError,OSError,http.client.HTTPException)):
                self.request(server,cert=cert,key=key)
        self.assertEqual(self.backend._dispatch_from_listener.call_count,0)

    def test_transport_scope_is_server_owned_and_client_header_is_not_trusted(self):
        server=self.start()
        self.assertEqual(self.request(server,cert=self.good,key=self.goodkey)[0],200)
        self.assertEqual(self.backend._dispatch_from_listener.call_args.args[0],('viewer-a','staging'))
        self.assertEqual(self.request(server,cert=self.good,key=self.goodkey,headers={'X-Client-Principal':'viewer-b'})[0],400)
        self.assertEqual(self.request(server,cert=self.good,key=self.goodkey,method='GET',body=b'')[0],405)
        self.assertEqual(self.backend._dispatch_from_listener.call_count,1)

    def test_missing_backend_and_route_bounds(self):
        server=self.start(backend=False)
        self.assertEqual(self.request(server,cert=self.good,key=self.goodkey)[0],503)
        page=self.path.rsplit('/',1)[0]+'/pages/64'
        self.assertEqual(self.request(server,cert=self.good,key=self.goodkey,path=page)[0],400)
        page=self.path.rsplit('/',1)[0]+'/pages/0'
        self.assertEqual(self.request(server,cert=self.good,key=self.goodkey,path=page,body=b'x'*(256*1024+1))[0],413)

    def test_drain_waits_for_active_backend_and_blocks_new_connections(self):
        started=threading.Event();release=threading.Event();outcomes=[]
        def blocked(*_args):
            started.set()
            if not release.wait(3):raise RuntimeError('test backend timeout')
            return Response(200,{'state':'not_custody'})
        self.backend._dispatch_from_listener.side_effect=blocked
        server=self.start()
        def request():
            try:outcomes.append(self.request(server,cert=self.good,key=self.goodkey)[0])
            except Exception as error:outcomes.append(error)
        thread=threading.Thread(target=request);thread.start()
        try:
            self.assertTrue(started.wait(3));self.assertFalse(server.drain(.05))
            with self.assertRaises((ssl.SSLError,OSError,http.client.HTTPException)):
                self.request(server,cert=self.good,key=self.goodkey)
            self.assertEqual(self.backend._dispatch_from_listener.call_count,1)
        finally:
            release.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(outcomes,[200]);self.assertTrue(server.drain(2))

    def test_owner_never_closes_during_active_request(self):
        from processing.direct_snapshot_service import create_snapshot_owner
        self.assertIsNone(create_snapshot_owner())
        started=threading.Event();release=threading.Event();outcomes=[]
        def blocked(*_args):
            started.set()
            if not release.wait(3):raise RuntimeError('test backend timeout')
            return Response(200,{'state':'not_custody'})
        self.backend._dispatch_from_listener.side_effect=blocked
        owner=create_snapshot_owner(enabled=True,host='127.0.0.1',port=0,ssl_context=self.context,
                                    client_scopes=self.scopes,backend=self.backend)
        self.addCleanup(lambda:(release.set(),owner.close(3)))
        server=owner._server;owner.start()
        def request():
            try:outcomes.append(self.request(server,cert=self.good,key=self.goodkey)[0])
            except Exception as error:outcomes.append(error)
        thread=threading.Thread(target=request);thread.start()
        try:
            self.assertTrue(started.wait(3));self.assertFalse(owner.close(.05));self.assertEqual(owner.status(),'draining')
        finally:
            release.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(outcomes,[200])
        self.assertTrue(owner.close(3));self.assertEqual(owner.status(),'closed')

    def test_close_exception_still_retires_activity_and_slot(self):
        server=self.start();original=server.shutdown_request
        def closed_then_error(request):
            original(request)
            raise OSError('synthetic close error after real socket closed')
        with patch.object(server,'shutdown_request',side_effect=closed_then_error):
            self.assertEqual(self.request(server,cert=self.good,key=self.goodkey)[0],200)
            self.assertTrue(server.drain(3))
            with server._activity:self.assertEqual(server._active_connections,0)
            self.assertTrue(server._slots.acquire(blocking=False));server._slots.release()

if __name__=='__main__':unittest.main()
