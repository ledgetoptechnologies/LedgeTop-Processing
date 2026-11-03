"""Real Viewer acceptance API TLS loopback, synthetic credentials and data only."""
import base64,datetime,ipaddress,json,os,re,ssl,subprocess,tempfile,time,unittest,uuid
from pathlib import Path
from urllib.request import build_opener,HTTPSHandler,ProxyHandler
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import ed25519,rsa
from cryptography.x509.oid import NameOID
from processing import reference_protocol as protocol
from processing.viewer_acceptance_receipt import SIGNATURE_DOMAIN
from processing.prototype import Queue
from processing.reference_store import ReferenceStore, PROFILE
from processing.reference_viewer_transfer_worker import ReferenceViewerTransferWorker
from processing.viewer_acceptance_transfer import ViewerAcceptanceTransfer
from tests.test_reference_api import ReferenceAPITests
from tests.test_viewer_acceptance_transfer import FakeResponse,FakeResultProxy,SECRET

@unittest.skipUnless(os.name == 'posix' and all(os.environ.get(k) for k in ('PINNED_VIEWER_ROOT', 'VIEWER_NODE_MODULES')),
                     'requires Docker, pinned Viewer and explicit dependencies')
class RealViewerLostCommitTests(unittest.TestCase):
 def test_actual_viewer_tls_signed_acceptance_lost_commit(self):
  root=os.environ.get('PINNED_VIEWER_ROOT');deps=os.environ.get('VIEWER_NODE_MODULES')
  if not root or not deps:self.fail('pinned Viewer root and dependencies required')
  self.assertEqual(subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'4df1a77bc4a4070558a52177275ac7fdb942ebaa')
  self.assertEqual(subprocess.check_output(['git','status','--porcelain'],cwd=root,text=True).strip(),'')
  fixture=ReferenceAPITests('runTest');fixture.setUp();container=None
  fixture.store=ReferenceStore(fixture.queue,viewer_required_environments=('staging',));fixture.api=fixture.make_api()
  try:
   job,result,_=fixture.ready()
   with tempfile.TemporaryDirectory() as tmp:
    directory=Path(tmp);key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
    subject=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'localhost')]);now=datetime.datetime.now(datetime.timezone.utc)
    cert=(x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(key.public_key()).serial_number(x509.random_serial_number())
       .not_valid_before(now-datetime.timedelta(minutes=1)).not_valid_after(now+datetime.timedelta(hours=1))
       .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost'),x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),critical=False)
       .add_extension(x509.BasicConstraints(ca=True,path_length=None),critical=True).sign(key,hashes.SHA256()))
    (directory/'server.key').write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.TraditionalOpenSSL,serialization.NoEncryption()))
    (directory/'server.crt').write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    receipt_key=ed25519.Ed25519PrivateKey.generate()
    (directory/'viewer-receipt.pem').write_bytes(receipt_key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
    expected_receipt_pem=receipt_key.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode('ascii')
    name='codex-viewer-acceptance-'+uuid.uuid4().hex
    script=Path(__file__).parent/'support'/'real_viewer_acceptance_server_candidate.cjs'
    command=['docker','create','--pull=never','--name',name,'--memory','384m','--cpus','1','--cap-drop','ALL','--security-opt','no-new-privileges',
      '--tmpfs','/tmp:rw,nosuid,nodev,size=64m','-p','127.0.0.1::8443','-v',str(Path(root).resolve())+':/viewer:ro',
      '-v',str(Path(deps).resolve())+':/deps:ro','-v',str(directory.resolve())+':/tls:ro','-v',str(script.resolve())+':/fixture.cjs:ro',
      '-e','NODE_PATH=/deps','sha256:e7add2a490bb17d97b485b8641bd88455fff7839ade0f5eba493c3508124d413','node','/fixture.cjs']
    # Never start work until create returns the exact daemon-owned ID. If the
    # CLI times out, daemon creation may still be in flight: report uncertainty.
    try:
     created=subprocess.run(command,check=True,capture_output=True,text=True,timeout=30)
    except subprocess.TimeoutExpired:
     raise RuntimeError('docker create timed out; possible late stopped container named '+name+' requires inspection') from None
    created_id=created.stdout.strip()
    if not re.fullmatch(r'[0-9a-f]{64}',created_id):
     raise RuntimeError('docker create returned unknown ID; inspect stopped container named '+name)
    container=created_id
    subprocess.run(['docker','start',container],check=True,capture_output=True,text=True,timeout=30)
    deadline=time.monotonic()+20;ready=None
    while time.monotonic()<deadline:
     logs=subprocess.run(['docker','logs',container],capture_output=True,text=True,timeout=5)
     ready=re.search(r'READY ([A-Za-z0-9+/=]+)',logs.stdout)
     if ready:break
     state=subprocess.run(['docker','inspect','-f','{{.State.Running}}',container],capture_output=True,text=True,timeout=5)
     if state.stdout.strip()!='true':self.fail('Viewer test container failed: '+logs.stderr[-1000:])
     time.sleep(.1)
    self.assertIsNotNone(ready,'Viewer test container did not start')
    port_output=subprocess.check_output(['docker','port',container,'8443/tcp'],text=True,timeout=5).strip()
    match=re.fullmatch(r'127\.0\.0\.1:(\d+)',port_output);self.assertIsNotNone(match,port_output)
    pem=base64.b64decode(ready.group(1),validate=True).decode('ascii')
    self.assertEqual(pem,expected_receipt_pem)
    receipt_keys={'synthetic-viewer-key':{'issuerId':'synthetic-viewer-issuer','publicKeyPem':expected_receipt_pem}}
    ctx=ssl.create_default_context(cafile=str(directory/'server.crt'))
    actual=build_opener(ProxyHandler({}),HTTPSHandler(context=ctx)).open
    lost=[False];remote_commit=[None];corrupt_next=[False];calls=[]
    def opener(request,timeout):
     response=actual(request,timeout=timeout);calls.append((request.get_method(),request.full_url))
     if request.get_method()=='POST' and request.full_url.endswith('/commit') and not lost[0]:
      remote_commit[0]=json.loads(response.read());response.close();lost[0]=True;raise OSError('synthetic lost committed response')
     if request.get_method()=='GET' and corrupt_next[0]:
      value=json.loads(response.read());response.close();corrupt_next[0]=False
      receipt=value['viewerReceipt'];receipt['resultManifestSha256']='0'*64
      # Sign the wrong identity with the exact synthetic Viewer key, not a random signature.
      body={k:v for k,v in receipt.items() if k!='signature'}
      signing_script="const c=require('node:crypto'),f=require('node:fs'),p=require('/viewer/apps/viewer/server/referenceProtocol');const b=JSON.parse(f.readFileSync(0,'utf8'));const k=f.readFileSync('/tls/viewer-receipt.pem');process.stdout.write(c.sign(null,Buffer.concat([Buffer.from('LTDS-VIEWER-ACCEPTED-COPY-ACK-V1\\0','ascii'),p.canonicalBytes(b)]),k).toString('base64url'));"
      signed=subprocess.run(['docker','exec','-i',container,'node','-e',signing_script],input=json.dumps(body),capture_output=True,text=True,timeout=10,check=True).stdout
      receipt['signature']=signed
      # Check signature independently before asserting the worker rejects binding.
      sigbytes=base64.urlsafe_b64decode(signed+'==')
      serialization.load_pem_public_key(pem.encode('ascii')).verify(sigbytes,SIGNATURE_DOMAIN+protocol.canonical_json(body))
      return FakeResponse(request.full_url,200,json.dumps(value,separators=(',',':')).encode())
     return response
    proxy=FakeResultProxy({'result-1':b'abc','result-2':b''})
    sender=ViewerAcceptanceTransfer('https://127.0.0.1:'+match.group(1),key_id='synthetic-service',secret=SECRET,
       trusted_receipt_keys=receipt_keys,result_proxy=proxy,ssl_context=ctx,opener=opener,chunk_bytes=2)
    now_clock=[100.0];worker=ReferenceViewerTransferWorker(fixture.store,sender,trusted_receipt_keys=receipt_keys,clock=lambda:now_clock[0])
    first=worker.run_once();self.assertEqual(first['state'],'retry_wait',first);self.assertTrue(lost[0])
    self.assertEqual(fixture.store.get(job['jobId'])['phase'],'awaiting_acceptance')
    uploaded=len([x for x in calls if '/objects/' in x[1]]);self.assertGreater(uploaded,0)
    # Release the old Queue lease and construct a new Queue on durable state.
    old_status=fixture.store.viewer_transfer_status(job['jobId'])
    old_queue=fixture.queue;old_startup=old_queue.startup_id;old_authority=old_queue.authority_id
    old_queue.close()
    fixture.queue=Queue(fixture.directory.name);fixture.queue.allowed_profiles.add(PROFILE)
    self.assertNotEqual(fixture.queue.startup_id,old_startup)
    self.assertEqual(fixture.queue.authority_id,old_authority)
    # Restart the exact Viewer container after remote commit but before retry.
    subprocess.run(['docker','stop','--time','10',container],check=True,capture_output=True,text=True,timeout=20)
    stopped=subprocess.check_output(['docker','inspect','-f','{{.State.Running}}',container],text=True,timeout=5).strip()
    self.assertEqual(stopped,'false')
    subprocess.run(['docker','start',container],check=True,capture_output=True,text=True,timeout=20)
    deadline=time.monotonic()+20; restarted=None
    while time.monotonic()<deadline:
     logs=subprocess.check_output(['docker','logs',container],text=True,timeout=5)
     found=re.findall(r'READY ([A-Za-z0-9+/=]+)',logs)
     if len(found)>=2:restarted=found[-1];break
     time.sleep(.1)
    self.assertEqual(restarted,ready.group(1),'Viewer signer changed across restart')
    restarted_port=subprocess.check_output(['docker','port',container,'8443/tcp'],text=True,timeout=5).strip()
    restarted_match=re.fullmatch(r'127\.0\.0\.1:(\d+)',restarted_port);self.assertIsNotNone(restarted_match,restarted_port)
    sender=ViewerAcceptanceTransfer('https://127.0.0.1:'+restarted_match.group(1),key_id='synthetic-service',secret=SECRET,
       trusted_receipt_keys=receipt_keys,result_proxy=proxy,ssl_context=ctx,opener=opener,chunk_bytes=2)
    reopened=ReferenceStore(fixture.queue)
    self.assertEqual(reopened.viewer_transfer_status(job['jobId']),old_status)
    self.assertEqual(reopened.get(job['jobId'])['phase'],'awaiting_acceptance')
    now_clock[0]+=2
    worker=ReferenceViewerTransferWorker(reopened,sender,trusted_receipt_keys=receipt_keys,clock=lambda:now_clock[0])
    # Corrupt only the intercepted response; actual Viewer ledger remains intact.
    corrupt_next[0]=True
    bad=worker.run_once();self.assertEqual(bad['state'],'retry_wait',bad);self.assertEqual(bad['reason'],'viewer_signed_receipt_rejected')
    self.assertFalse(corrupt_next[0]);self.assertEqual(reopened.get(job['jobId'])['phase'],'awaiting_acceptance')
    self.assertFalse(reopened.viewer_transfer_status(job['jobId'])['completed'])
    self.assertEqual(reopened.acceptance_receipts(job['jobId'])['receipts'],[])
    self.assertEqual(proxy.objects['result-1'],b'abc')
    self.assertEqual(len([x for x in calls if '/objects/' in x[1]]),uploaded)
    now_clock[0]+=5
    self.assertEqual(worker.run_once()['state'],'accepted')
    self.assertEqual(len([x for x in calls if '/objects/' in x[1]]),uploaded)
    self.assertEqual(calls[-1][0],'GET');self.assertEqual(reopened.get(job['jobId'])['phase'],'completed')
    receipts=reopened.acceptance_receipts(job['jobId'])['receipts']
    self.assertEqual(len(receipts),1)
    self.assertEqual(receipts[0]['viewerReceipt'],remote_commit[0]['viewerReceipt'])
    self.assertEqual(receipts[0]['acceptance'],remote_commit[0]['acceptance'])
    self.assertEqual(proxy.objects['result-1'],b'abc')
  finally:
   try:
    if container:
     removed=subprocess.run(['docker','rm','-f',container],capture_output=True,text=True,timeout=20)
     state=subprocess.run(['docker','inspect',container],capture_output=True,text=True,timeout=10)
     if removed.returncode!=0 or state.returncode==0 or 'no such object' not in state.stderr.lower():
      raise AssertionError('task-owned Viewer container cleanup unverified: '+container)
   finally:fixture.tearDown()

if __name__=='__main__':unittest.main()
