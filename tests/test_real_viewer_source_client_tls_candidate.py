"""Paired synthetic TLS test against exact Viewer c92 source broker."""
import json, os, queue, sqlite3, ssl, subprocess, tempfile, threading, unittest
from urllib.request import Request
from urllib.error import HTTPError,URLError
from datetime import datetime,timedelta,timezone
from pathlib import Path
from contextlib import closing
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import ExtendedKeyUsageOID,NameOID
from processing.reference_transfer import BrokerClient
from processing.source_range_transfer import SourceRangeClient,TransferError,validate_source_grant

VIEWER=os.environ.get('VIEWER_C92_ROOT')
@unittest.skipUnless(VIEWER,'VIEWER_C92_ROOT must identify Viewer c92 checkout')
class RealViewerBrokerClientTests(unittest.TestCase):
    def test_attempt_bound_actual_client_against_real_viewer_broker_with_tls(self):
        expected='c92deb5ba143fd302bdf787ba832e0ac1bd0e2f0'
        self.assertEqual(subprocess.check_output(['git','rev-parse','HEAD'],cwd=VIEWER,text=True).strip(),expected)
        for name in ('sourceRangeV2Candidate.js','referenceProtocol.js'):
            relative='apps/viewer/server/'+name
            actual=(Path(VIEWER)/relative).read_bytes()
            exact=subprocess.check_output(['git','show',expected+':'+relative],cwd=VIEWER)
            self.assertEqual(actual.replace(b'\r\n',b'\n'),exact,'Viewer module differs from pinned commit')
        with tempfile.TemporaryDirectory(prefix='paired-source-tls-') as root:
            root=Path(root);now=datetime.now(timezone.utc);ca_key=Ed25519PrivateKey.generate()
            ca_name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'Synthetic CA')])
            ca=(x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name).public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
                .not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(days=1)).add_extension(x509.BasicConstraints(ca=True,path_length=0),critical=True)
                .add_extension(x509.KeyUsage(digital_signature=True,content_commitment=False,key_encipherment=False,data_encipherment=False,key_agreement=False,key_cert_sign=True,crl_sign=True,encipher_only=False,decipher_only=False),critical=True)
                .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),critical=False).sign(ca_key,algorithm=None))
            key=Ed25519PrivateKey.generate();name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'localhost')])
            cert=(x509.CertificateBuilder().subject_name(name).issuer_name(ca_name).public_key(key.public_key()).serial_number(x509.random_serial_number())
                  .not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(hours=1)).add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost')]),critical=False)
                  .add_extension(x509.BasicConstraints(ca=False,path_length=None),critical=True).add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),critical=False)
                  .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),critical=False)
                  .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()),critical=False).sign(ca_key,algorithm=None))
            (root/'ca.pem').write_bytes(ca.public_bytes(serialization.Encoding.PEM));(root/'server.pem').write_bytes(cert.public_bytes(serialization.Encoding.PEM))
            (root/'server.key').write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
            script=Path(__file__).resolve().parent/'fixtures/viewer_source_broker_process.cjs'
            process=subprocess.Popen(['node',str(script),VIEWER,str(root)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
            def line():
                q=queue.Queue(maxsize=1);reader=threading.Thread(target=lambda:q.put(process.stdout.readline()),daemon=True);reader.start()
                try:return q.get(timeout=8)
                except queue.Empty:self.fail('synthetic broker timed out')
            try:
                f=json.loads(line());origin='https://localhost:%d'%f['port']
                context=ssl.create_default_context(cafile=str(root/'ca.pem'));broker=BrokerClient(origin,ssl_context=context)
                job={'jobId':f['binding']['jobId'],'generation':f['binding']['generation'],'environment':f['binding']['environment'],
                     'manifestSha256':f['manifest']['sha256'],'submission':{'manifest':f['manifest']},
                     'sourceViewerAttemptId':f['binding']['viewerAttemptId'],'transferRequest':f['transfer']}
                grant=validate_source_grant(f['grant'],job=job)
                self.assertEqual(grant.identity.canonical()['viewerAttemptId'],f['binding']['viewerAttemptId'])
                client=SourceRangeClient(origin,root/'client-one.sqlite',ssl_context=broker.ssl_context)
                try:
                    with client._open(Request(origin+'/synthetic-probe'),timeout=5):pass
                except HTTPError as e:self.assertEqual(e.code,404)
                self.assertEqual(client.download_object(grant=grant,object_id='photo-1',destination=root/'result-one').read_bytes(),b'synthetic-photo-123')
                # A completed local replay does not open another HTTP request.
                self.assertEqual(client.download_object(grant=grant,object_id='photo-1',destination=root/'result-one').read_bytes(),b'synthetic-photo-123')
                second=SourceRangeClient(origin,root/'client-two.sqlite',ssl_context=broker.ssl_context)
                self.assertEqual(second.download_object(grant=grant,object_id='photo-1',destination=root/'result-two').read_bytes(),b'synthetic-photo-123')
                untrusted=SourceRangeClient(origin,root/'client-untrusted.sqlite')
                with self.assertRaises(URLError) as denied:untrusted._open(Request(origin+'/synthetic-probe'),timeout=5)
                self.assertIsInstance(denied.exception.reason,ssl.SSLCertVerificationError)
                wrong_origin='https://127.0.0.1:%d'%f['port']
                wrong=SourceRangeClient(wrong_origin,root/'client-wrong-host.sqlite',ssl_context=broker.ssl_context)
                with self.assertRaises(URLError) as denied:wrong._open(Request(wrong_origin+'/synthetic-probe'),timeout=5)
                self.assertIsInstance(denied.exception.reason,ssl.SSLCertVerificationError)
                with closing(sqlite3.connect(str(root/'viewer-broker.sqlite'))) as db:
                    self.assertEqual(db.execute('SELECT requests_used,bytes_used,attempts_used FROM source_range_usage').fetchone(),(1,len(b'synthetic-photo-123'),2))
                # Explicit broker revocation denies the same otherwise-valid grant.
                process.stdin.write('revoke\n');process.stdin.flush();self.assertEqual(line().strip(),'revoked')
                third=SourceRangeClient(origin,root/'client-three.sqlite',ssl_context=broker.ssl_context)
                with self.assertRaises(TransferError):third.download_object(grant=grant,object_id='photo-1',destination=root/'result-three')
            finally:
                if process.poll() is None:
                    process.stdin.close()
                    try:process.wait(timeout=5)
                    except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
                process.stdout.close();process.stderr.close()
                self.assertIsNotNone(process.returncode)

if __name__=='__main__':unittest.main()
