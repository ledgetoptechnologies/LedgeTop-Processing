import copy
import os
import unittest
import uuid
import json
import ssl
import threading
import http.client
import hashlib
import datetime

from processing.direct_worker_boundary import DirectBoundaryDenied
from processing.direct_worker_grant_v2 import DirectWorkerGrant
from processing.scratch_policy import ScratchLedger, PolicyConflict
from tests.test_direct_worker_receiver_v2 import WorkerV2Tests


@unittest.skipIf(os.name=='nt','Linux scratch fixture required')
class DirectWorkerGrantTests(unittest.TestCase):
    def test_exact_principal_binding_survives_ledger_reopen(self):
        f=WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        grant=DirectWorkerGrant(f.boundary,coordinator_principal='coord-a')
        payload=dict(operationId=str(uuid.uuid4()),item=copy.deepcopy(f.item))
        with self.assertRaises(DirectBoundaryDenied):grant.require('coord-a',f.headers)
        with self.assertRaises(DirectBoundaryDenied):grant.authorize('coord-b',f.headers,payload)
        result=grant.authorize('coord-a',f.headers,payload)
        self.assertFalse(result['sourceCustodyAccepted']);self.assertFalse(result['processingAuthorized'])
        self.assertEqual(grant.authorize('coord-a',f.headers,payload),result)
        changed=copy.deepcopy(payload);changed['item']['generation']+=1
        with self.assertRaises(DirectBoundaryDenied):grant.authorize('coord-a',f.headers,changed)
        changed=copy.deepcopy(payload);changed['operationId']=str(uuid.uuid4())
        with self.assertRaises(PolicyConflict):grant.authorize('coord-a',f.headers,changed)
        f.boundary.ledger=ScratchLedger(f.root/'private'/'ledger.sqlite',f.root,
            capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)
        grant.require('coord-a',f.headers)
        with self.assertRaises(DirectBoundaryDenied):grant.require('coord-b',f.headers)

    def test_real_mtls_grant_gates_v2_transfer(self):
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
        from ops.qualify_enrolled_phase_transport import configuration_material
        from processing.direct_worker_http import create_direct_worker_server
        from processing.reference_tls import verified_client_context
        from tests.test_reference_source_intake import IMAGE_ID

        f=WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        material=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))['configurationFiles']
        for name in ('fixture-ca.pem','fixture-cert.pem','fixture-key.pem'):(f.root/name).write_bytes(material[name])
        now=datetime.datetime.now(datetime.timezone.utc)
        key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
        name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'synthetic-coordinator')])
        cert=(x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=1))
            .not_valid_after(now+datetime.timedelta(days=1)).add_extension(x509.BasicConstraints(ca=True,path_length=None),critical=True)
            .sign(key,hashes.SHA256()))
        cert_path=f.root/'client.pem'; key_path=f.root/'client.key'
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
        fingerprint=hashlib.sha256(cert.public_bytes(serialization.Encoding.DER)).hexdigest()
        tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.minimum_version=ssl.TLSVersion.TLSv1_2
        tls.load_cert_chain(str(f.root/'fixture-cert.pem'),str(f.root/'fixture-key.pem'))
        tls.load_verify_locations(cafile=str(cert_path));tls.verify_mode=ssl.CERT_REQUIRED
        grant=DirectWorkerGrant(f.boundary,coordinator_principal='coord-a')
        server=create_direct_worker_server(f.boundary,'127.0.0.1',0,ssl_context=tls,
            client_principals={fingerprint:'coord-a'},reservation_grant=grant)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        def close():server.shutdown();server.server_close();thread.join(3)
        self.addCleanup(close)
        context=verified_client_context(str(f.root/'fixture-ca.pem'));context.load_cert_chain(str(cert_path),str(key_path))
        def request(method,path,body,headers):
            conn=http.client.HTTPSConnection('127.0.0.1',server.server_port,context=context,timeout=3)
            try:
                conn.request(method,path,body=body,headers=headers)
                response=conn.getresponse();data=json.loads(response.read());return response.status,data
            finally:conn.close()
        headers={**f.headers,'Content-SHA256':hashlib.sha256(b'abc').hexdigest()}
        self.assertEqual(request('PUT','/direct/v2/objects/'+IMAGE_ID,b'abc',headers)[0],403)
        payload=dict(operationId=str(uuid.uuid4()),item=f.item)
        status,result=request('POST','/direct/v2/reservation',json.dumps(payload).encode(),f.headers)
        self.assertEqual(status,200);self.assertFalse(result['sourceCustodyAccepted'])
        self.assertEqual(request('PUT','/direct/v2/objects/'+IMAGE_ID,b'abc',headers)[0],200)
