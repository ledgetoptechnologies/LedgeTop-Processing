"""Real loopback TLS server, ephemeral CA; synthetic metadata only."""
import datetime,hashlib,http.client,json,ssl,tempfile,threading,unittest
from pathlib import Path
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID,ExtendedKeyUsageOID
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.viewer_batch_intent_candidate import initialize
from processing.viewer_batch_identity_candidate import Identity
from processing.viewer_batch_metadata_api_candidate import BatchMetadataAPI
from processing.reference_http import create_reference_http_server

def certificates(root):
 now=datetime.datetime.now(datetime.timezone.utc); key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
 def name(value):return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,value)])
 def ca(label):
  private=rsa.generate_private_key(public_exponent=65537,key_size=2048);subject=name(label)
  cert=(x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(private.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=1)).not_valid_after(now+datetime.timedelta(hours=1)).add_extension(x509.BasicConstraints(ca=True,path_length=0),critical=True).add_extension(x509.SubjectKeyIdentifier.from_public_key(private.public_key()),critical=False).add_extension(x509.KeyUsage(digital_signature=True,content_commitment=False,key_encipherment=False,data_encipherment=False,key_agreement=False,key_cert_sign=True,crl_sign=True,encipher_only=False,decipher_only=False),critical=True).sign(private,hashes.SHA256()))
  return private,cert
 authority,cert=ca('synthetic-ca');_,wrong=ca('wrong-ca')
 leaf=(x509.CertificateBuilder().subject_name(name('localhost')).issuer_name(cert.subject).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=1)).not_valid_after(now+datetime.timedelta(hours=1)).add_extension(x509.BasicConstraints(ca=False,path_length=None),critical=True).add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(authority.public_key()),critical=False).add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()),critical=False).add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost')]),critical=False).add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),critical=False).sign(authority,hashes.SHA256()))
 def put(filename,data):
  p=root/filename;p.write_bytes(data);return str(p)
 return (put('ca.pem',cert.public_bytes(serialization.Encoding.PEM)),put('wrong.pem',wrong.public_bytes(serialization.Encoding.PEM)),put('leaf.pem',leaf.public_bytes(serialization.Encoding.PEM)),put('leaf.key',key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())))

class TLSBatchTest(unittest.TestCase):
 def test_verified_tls_auth_and_metadata(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);ca,wrong,leaf,key=certificates(root);q=Queue(root/'q');server=None;thread=None
   try:
    store=TaskStore(q,root/'inputs');identity=Identity(store,[{'token':'a'*32,'principalId':'alice','environment':'production'}],['z'*32],q.authority_id);identity.initialize();initialize(store)
    ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.load_cert_chain(leaf,key);self.assertEqual(ctx.verify_mode,ssl.CERT_NONE)
    server=create_reference_http_server(BatchMetadataAPI(identity),ssl_context=ctx,request_timeout=3);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    task='11111111-2222-4333-8444-555555555555';cap='A'*43;prefix='/ltds/held-batches/v1/tasks/'+task;client=ssl.create_default_context(cafile=ca)
    def request(host,context,path,body,token='a'*32,duplicate=False):
     data=json.dumps(body).encode();c=http.client.HTTPSConnection(host,server.server_port,context=context,timeout=4)
     try:
      c.putrequest('POST',path);c.putheader('Authorization','Bearer '+token)
      if duplicate:c.putheader('Authorization','Bearer '+token)
      c.putheader('X-LedgeTop-Capability',cap);c.putheader('Content-Length',str(len(data)));c.endheaders(data);r=c.getresponse();status=r.status;r.read();return status
     finally:c.close()
    payload={'expectedAuthorityId':q.authority_id}
    with self.assertRaises(ssl.SSLCertVerificationError):request('localhost',ssl.create_default_context(cafile=wrong),prefix+'/reserve',payload)
    with self.assertRaises(ssl.SSLCertVerificationError):request('127.0.0.1',client,prefix+'/reserve',payload)
    self.assertEqual(request('localhost',client,prefix+'/reserve',payload,duplicate=True),400)
    self.assertEqual(request('localhost',client,prefix+'/reserve',payload,token='x'*32),401)
    self.assertEqual(request('localhost',client,prefix+'/reserve',payload),200)
    store.initialize(task,'x',[],None,'production',cap)
    body={**payload,'generation':1,'batchId':'batch','files':[{'name':'a.jpg','bytes':3,'sha256':hashlib.sha256(b'abc').hexdigest()}]}
    self.assertEqual(request('localhost',client,prefix+'/prepare',body),200)
   finally:
    if server:server.shutdown();server.server_close()
    if thread:thread.join(timeout=5);self.assertFalse(thread.is_alive())
    q.close()
