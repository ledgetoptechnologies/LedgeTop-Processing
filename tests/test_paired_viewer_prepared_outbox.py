"""Actual Viewer outbox/client paired with Processing prepared JCS API over TLS."""
import copy,hashlib,http.client,json,os,ssl,subprocess,tempfile,threading,unittest,uuid
from datetime import datetime,timedelta,timezone
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import ExtendedKeyUsageOID,NameOID
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE
from processing.reference_prepared_api_candidate import PreparedSubmissionAPI

VIEWER=os.environ.get('VIEWER_7DB_ROOT')
@unittest.skipUnless(VIEWER,'VIEWER_7DB_ROOT required for actual Viewer pair')
class PairedPreparedOutboxTests(unittest.TestCase):
 def test_real_viewer_outbox_and_processing_prepare_recovery(self):
  pin='7dbd175d1b501edc4b8414328e9d7ff2bfd2837f'
  self.assertEqual(subprocess.check_output(['git','rev-parse','HEAD'],cwd=VIEWER,text=True).strip(),pin)
  self.assertEqual(subprocess.check_output(['git','status','--porcelain'],cwd=VIEWER,text=True).strip(),'')
  root=tempfile.TemporaryDirectory(prefix='paired-prepared-');self.addCleanup(root.cleanup);directory=Path(root.name)
  now=datetime.now(timezone.utc);cakey=Ed25519PrivateKey.generate();caname=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'Synthetic CA')])
  ca=(x509.CertificateBuilder().subject_name(caname).issuer_name(caname).public_key(cakey.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(days=1)).add_extension(x509.BasicConstraints(ca=True,path_length=0),critical=True).add_extension(x509.KeyUsage(digital_signature=True,content_commitment=False,key_encipherment=False,data_encipherment=False,key_agreement=False,key_cert_sign=True,crl_sign=True,encipher_only=False,decipher_only=False),critical=True).add_extension(x509.SubjectKeyIdentifier.from_public_key(cakey.public_key()),critical=False).sign(cakey,algorithm=None))
  key=Ed25519PrivateKey.generate();name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'localhost')]);cert=(x509.CertificateBuilder().subject_name(name).issuer_name(caname).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(hours=1)).add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost')]),critical=False).add_extension(x509.BasicConstraints(ca=False,path_length=None),critical=True).add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),critical=False).add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(cakey.public_key()),critical=False).add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()),critical=False).sign(cakey,algorithm=None))
  (directory/'ca.pem').write_bytes(ca.public_bytes(serialization.Encoding.PEM));(directory/'server.pem').write_bytes(cert.public_bytes(serialization.Encoding.PEM));(directory/'server.key').write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
  fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text());raw=copy.deepcopy(fixture['submission']);raw['pipeline']['odm']['options']={'resolution':1.25}
  legacy=dict(capabilityFingerprint=fixture['capabilities']['capabilityFingerprint'],optionsFingerprint=fixture['capabilities']['engines']['odm']['optionsFingerprint'],allowedOdmOptions=['resolution'],availableEngines=['odm'])
  policy=dict(capabilityFingerprint=legacy['capabilityFingerprint'],optionsFingerprint=legacy['optionsFingerprint'],availableEngines=['odm'],optionDefinitions={'resolution':{'type':'float','domain':{'min':-100,'max':100}}})
  token='synthetic-prepared-production-token';other='synthetic-prepared-other-token-01';stage='synthetic-prepared-staging-token'
  coordinator=Queue(directory/'coordinator',reference_enabled=True);self.addCleanup(coordinator.close);store=ReferenceStore(coordinator,jcs_runtime_enabled=True)
  def make_api(pin=None):return PreparedSubmissionAPI(store,credentials=[dict(token=token,principalId='viewer-a',environment='production'),dict(token=other,principalId='viewer-b',environment='production'),dict(token=stage,principalId='viewer-a',environment='staging')],legacy_bearer_tokens=[],policies={'production':legacy,'staging':legacy},jcs_policies={'production':policy,'staging':policy},expected_authority_id=coordinator.authority_id if pin is None else pin)
  class Handler(BaseHTTPRequestHandler):
   def respond(self):
    body=self.rfile.read(int(self.headers.get('Content-Length','0'))) if self.command=='POST' else b''
    result=self.server.api.handle(self.command,self.path,self.headers,body,protected_transport=True);data=json.dumps(result.body).encode()
    self.send_response(result.status);self.send_header('Content-Length',str(len(data)));self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(data)
   do_POST=respond;do_GET=respond
   def log_message(self,*_):pass
  server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.api=make_api();server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);server_context.load_cert_chain(directory/'server.pem',directory/'server.key');server.socket=server_context.wrap_socket(server.socket,server_side=True)
  thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();origin='https://localhost:%d'%server.server_port;context=ssl.create_default_context(cafile=str(directory/'ca.pem'));authority=coordinator.authority_id
  script=Path(__file__).resolve().parent/'fixtures/paired_viewer_prepared_outbox.cjs';env=os.environ.copy();env['NODE_EXTRA_CA_CERTS']=str(directory/'ca.pem')
  def node(mode,args):
   proc=subprocess.run(['node',str(script),VIEWER,str(directory),mode],input=json.dumps(args),text=True,capture_output=True,timeout=30,env=env)
   self.assertEqual(proc.returncode,0,'Viewer synthetic harness failed: '+proc.stderr[-500:]);return json.loads(proc.stdout.strip().splitlines()[-1])
  def request(method,path,body=None,headers=None):
   req=Request(origin+path,data=None if body is None else json.dumps(body).encode(),headers=headers or {},method=method)
   try:
    with urlopen(req,context=context,timeout=5) as r:return r.status,json.load(r)
   except HTTPError as e:return e.code,json.load(e)
  try:
   info=node('create',{'origin':origin,'authority':authority,'submission':raw});self.assertEqual(info['intentState'],'prepared');envelope=info['envelope'];url='/ltds/prepared/v3/submissions';header={'Authorization':'Bearer '+token}
   self.assertEqual(request('POST',url,envelope,header)[0],400)
   self.assertEqual(request('POST',url,envelope,{**header,'X-LTDS-Expected-Coordinator-Authority':str(uuid.uuid4())})[0],503)
   wrong_pin=str(uuid.uuid4());server.api=make_api(wrong_pin)
   self.assertEqual(request('POST',url,envelope,{**header,'X-LTDS-Expected-Coordinator-Authority':wrong_pin})[0],503)
   server.api=make_api()
   conn=http.client.HTTPSConnection('localhost',server.server_port,context=context,timeout=5);payload=json.dumps(envelope).encode();conn.putrequest('POST',url);conn.putheader('Authorization','Bearer '+token);conn.putheader('X-LTDS-Expected-Coordinator-Authority',authority);conn.putheader('X-LTDS-Expected-Coordinator-Authority',authority);conn.putheader('Content-Length',str(len(payload)));conn.endheaders(payload);self.assertEqual(conn.getresponse().status,400);conn.close()
   self.assertEqual(len(coordinator.list()),0)
   args={'origin':origin,'authority':authority,'token':token,**info,'dropResponse':True}
   accepted=node('dispatch',args);self.assertEqual((accepted['state'],accepted['replayState'],accepted['posts'],accepted['gets']),('accepted','accepted',1,1))
   self.assertEqual(len(coordinator.list()),1);binding=accepted['remoteBinding'];self.assertEqual(binding['authorityId'],authority)
   headers={**header,'X-LTDS-Expected-Coordinator-Authority':authority}
   self.assertEqual(request('POST',url,envelope,headers)[0],200)
   changed=copy.deepcopy(envelope);changed['submission']['pipeline']['odm']['options']['resolution']=1.5
   self.assertEqual(request('POST',url,changed,headers)[0],409)
   recovery=url+'/'+envelope['submission']['submissionId']
   for wrong in (other,stage):self.assertEqual(request('GET',recovery,headers={'Authorization':'Bearer '+wrong,'X-LTDS-Expected-Coordinator-Authority':authority})[0],404)
   self.assertEqual(len(coordinator.list()),1)
   expired=node('lease-expired',args);self.assertEqual((expired['code'],expired['posts'],expired['gets'],expired['state']),('prepared_outbox_lease_lost',0,0,'accepted'))
   coordinator.close()
   child="""import json,sys
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
a=json.load(sys.stdin);q=Queue(a['root'],reference_enabled=True)
try:
 s=ReferenceStore(q,jcs_runtime_enabled=True)
 result=s.find_viewer_envelope_v2(a['submission'],authenticated_principal='viewer-a',authenticated_environment='production',expected_protocol='ltds-viewer-prepared-envelope-jcs-v3',expected_authority_id=a['authority'])
 print(json.dumps({'sameJob':result['jobId']==a['job'],'sameDigest':result['envelopeSha256']==a['digest'],'held':s.next_for_node('cluster') is None}))
finally:q.close()
"""
   child_result=subprocess.run([os.sys.executable,'-c',child],input=json.dumps({'root':str(directory/'coordinator'),'submission':envelope['submission']['submissionId'],'authority':authority,'job':binding['jobId'],'digest':binding['envelopeSha256']}),text=True,capture_output=True,timeout=15)
   self.assertEqual(child_result.returncode,0);self.assertEqual(json.loads(child_result.stdout),{'sameJob':True,'sameDigest':True,'held':True})
   coordinator=Queue(directory/'coordinator',reference_enabled=True);self.addCleanup(coordinator.close);self.assertEqual(coordinator.authority_id,authority)
   reopened=ReferenceStore(coordinator,jcs_runtime_enabled=True);server.api=PreparedSubmissionAPI(reopened,credentials=[dict(token=token,principalId='viewer-a',environment='production')],legacy_bearer_tokens=[],policies={'production':legacy,'staging':legacy},jcs_policies={'production':policy,'staging':policy},expected_authority_id=authority)
   status,recovered=request('GET',recovery,headers=headers);self.assertEqual(status,200);self.assertEqual(recovered['jobId'],binding['jobId']);self.assertEqual(recovered['envelopeSha256'],binding['envelopeSha256']);self.assertIsNone(reopened.next_for_node('cluster'))
  finally:server.shutdown();server.server_close();thread.join(5)
  self.assertFalse(thread.is_alive())

if __name__=='__main__':unittest.main()
