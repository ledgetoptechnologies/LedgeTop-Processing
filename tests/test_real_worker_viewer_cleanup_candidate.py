"""Linux-only: actual worker -> coordinator -> Viewer -> worker cleanup, synthetic photo/engine."""
import base64,copy,datetime,hashlib,ipaddress,json,os,queue,select,signal,sqlite3,ssl,subprocess,sys,tempfile,threading,unittest,uuid
from pathlib import Path
from unittest.mock import patch
from urllib.request import build_opener,HTTPSHandler,ProxyHandler
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import ed25519,rsa
from cryptography.x509.oid import NameOID
from processing.prototype import Queue
from processing.node_gate import NodeGate
from processing.reference_store import ReferenceStore
from processing.reference_protocol import validate_submission
from processing.reference_delivery import DeliveryReceiver
from processing.reference_engine_worker import ReferenceEngineWorker,WorkerHeld
from processing.reference_results import ReferenceResults,ResultAuthorization
from processing.scratch_policy import ScratchLedger
from processing.scratch_cleanup import ScratchCleanupExecutor
from processing.worker_outbox import WorkerOutbox
from processing.viewer_acceptance_transfer import ViewerAcceptanceTransfer
from processing.reference_viewer_transfer_worker import ReferenceViewerTransferWorker
from tests.test_reference_engine_worker import FakeNativeEngine

@unittest.skipUnless(os.name == 'posix' and all(os.environ.get(k) for k in ('LINUX_NODE', 'PINNED_VIEWER_ROOT', 'NODE_PATH')),
                     'requires pinned Viewer, Linux Node and explicit dependencies')
class RealWorkerViewerCleanupTests(unittest.TestCase):
 def test_same_job_lost_receipt_restart_then_exact_cleanup(self):
  node=os.environ.get('LINUX_NODE');viewer=os.environ.get('PINNED_VIEWER_ROOT');deps=os.environ.get('NODE_PATH')
  if not node or not viewer or not deps:self.fail('explicit Linux Node, pinned Viewer, JS dependencies required')
  git=os.environ.get('GIT_BIN','git')
  self.assertEqual(subprocess.check_output([git,'rev-parse','HEAD'],cwd=viewer,text=True).strip(),'4df1a77bc4a4070558a52177275ac7fdb942ebaa')
  self.assertEqual(subprocess.check_output([git,'status','--porcelain'],cwd=viewer,text=True).strip(),'')
  fixture=json.loads((Path(__file__).parents[1]/'contracts/reference-v1.contract.json').read_text())
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);q=None;children=[]
   try:
    q=Queue(root/'coordinator',reference_enabled=True);store=ReferenceStore(q)
    submitted=store.submit(validate_submission(fixture['submission'],environment='staging',capability_fingerprint=fixture['submission']['capabilityFingerprint']))[0]
    identity=dict(node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-1')
    claim=store.claim(submitted['jobId'],**identity);identity['claim_token']=claim['claimToken']
    store.reserve(submitted['jobId'],fixture['transferRequest'],node_id='cluster');store.update_phase(submitted['jobId'],'fetching');store.update_phase(submitted['jobId'],'processing')
    job=store.get(submitted['jobId'],private=True)
    gate_path=root/'gate';authority=str(uuid.uuid4())
    with patch('processing.node_gate.canonical_gate_path',return_value=gate_path):
     with NodeGate('physical-engine') as gate:gate.provision('cluster','http://127.0.0.1:8000',authority)
     scratch=root/'scratch';scratch.mkdir(mode=0o700)
     ledger=ScratchLedger(root/'ledger.sqlite',scratch,capacity_probe=lambda _:10000000,safety_floor_bytes=0,cleanup_grace_seconds=0)
     ledger.reserve(job_id=job['jobId'],generation=job['generation'],reservation_id='reservation-1',manifest_sha256=job['manifestSha256'],input_bytes=3,output_budget=100000,environment='staging',ttl_seconds=3600,worker_ready=True)
     folder=scratch/'jobs/reservation-1';folder.mkdir(parents=True,mode=0o700);(folder/'image-1.jpg').write_bytes(b'abc');ledger.record_allocated('reservation-1',3)
     outbox=WorkerOutbox(root/'outbox/outbox.sqlite');worker=ReferenceEngineWorker(root/'journal/worker.sqlite',ledger=ledger,outbox=outbox,engine=FakeNativeEngine(),engine_id='physical-engine',node_id='cluster',authority_id=authority,coordinator_origin='http://127.0.0.1:8000',gate_generation=1,worker_id='worker',engine_version='synthetic-v1')
     self.assertEqual(worker.start(job,reservation_id='reservation-1',input_paths={'image-1':'jobs/reservation-1/image-1.jpg'}),'processing')
     outbox_id=worker.poll(job['jobId']);archive=folder/'all.zip';archive_bytes=archive.read_bytes();delivery=outbox.claim_next()
     envelope={k:delivery[k] for k in ('outboxId','payloadSha256','payload')}
     receipt=DeliveryReceiver(store).receive(envelope,**identity);outbox.acknowledge(outbox_id,receipt);worker.acknowledge_delivery(job['jobId'])
     self.assertEqual(ledger.cleanup_candidates(),[]);self.assertEqual(archive.read_bytes(),archive_bytes)
     key=rsa.generate_private_key(public_exponent=65537,key_size=2048);subject=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'localhost')]);now=datetime.datetime.now(datetime.timezone.utc)
     cert=(x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=1)).not_valid_after(now+datetime.timedelta(hours=1)).add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),critical=False).add_extension(x509.BasicConstraints(ca=True,path_length=None),critical=True).sign(key,hashes.SHA256()))
     (root/'server.key').write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.TraditionalOpenSSL,serialization.NoEncryption()));(root/'server.crt').write_bytes(cert.public_bytes(serialization.Encoding.PEM))
     signer=ed25519.Ed25519PrivateKey.generate();(root/'viewer-receipt.pem').write_bytes(signer.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
     pub=signer.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode();keys={'synthetic-viewer-key':{'issuerId':'synthetic-viewer-issuer','publicKeyPem':pub}};worker.viewer_receipt_keys=keys
     def start_viewer():
      env=dict(os.environ,VIEWER_ROOT=viewer,FIXTURE_ROOT=str(root))
      child=subprocess.Popen([node,str(Path(__file__).parent/'support/linux_viewer_acceptance_candidate.cjs')],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(child)
      lines=queue.Queue();reader=threading.Thread(target=lambda:lines.put(child.stdout.readline()),daemon=True);reader.start();line=lines.get(timeout=20).strip();reader.join(timeout=2)
      self.assertFalse(reader.is_alive());parts=line.split();self.assertEqual(parts[0],'READY');self.assertEqual(base64.b64decode(parts[2]).decode(),pub)
      return child,int(parts[1])
     def stop_viewer(child):
      child.terminate()
      try:child.wait(timeout=10)
      except subprocess.TimeoutExpired:child.kill();child.wait(timeout=10)
      self.assertIsNotNone(child.poll())
     child,port=start_viewer();ctx=ssl.create_default_context(cafile=str(root/'server.crt'));actual=build_opener(ProxyHandler({}),HTTPSHandler(context=ctx)).open
     calls=[];lost=[False];call_log=root/'http-calls.log'
     def opener(request,timeout):
      response=actual(request,timeout=timeout);calls.append((request.get_method(),request.full_url))
      fd=os.open(call_log,os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
      try:os.write(fd,(request.get_method()+' '+request.full_url+'\n').encode())
      finally:os.close(fd)
      if request.get_method()=='POST' and request.full_url.endswith('/commit') and not lost[0]:response.read();response.close();lost[0]=True;raise OSError('synthetic lost committed response')
      return response
     results=ReferenceResults(worker);result=json.loads(worker._row(job['jobId'])['result'])
     authorization=ResultAuthorization(node_id='cluster',worker_id='worker',job_id=job['jobId'],generation=job['generation'],environment='staging',result_manifest_sha256=result['resultManifestSha256'],object_ids=tuple(o['id'] for o in result['objects']),share_approved=True)
     class LocalWorkerResultProxy:
      def open(self,job,manifest,object_id,*,range_header=None):
       obj=next(x for x in manifest['objects'] if x['id']==object_id)
       return results.open(job['jobId'],generation=job['generation'],object_id=object_id,result_manifest_sha256=manifest['resultManifestSha256'],authorization=authorization,object_sha256=obj['sha256'],range_header=range_header)
     def make_sender(port):return ViewerAcceptanceTransfer('https://127.0.0.1:'+str(port),key_id='synthetic-service',secret='synthetic-viewer-service-key-material',trusted_receipt_keys=keys,result_proxy=LocalWorkerResultProxy(),ssl_context=ctx,opener=opener,chunk_bytes=512)
     now_clock=[100.0];sender=make_sender(port);sender_worker=ReferenceViewerTransferWorker(store,sender,trusted_receipt_keys=keys,clock=lambda:now_clock[0])
     self.assertEqual(sender_worker.run_once()['state'],'retry_wait');self.assertTrue(lost[0]);uploads=sum('/objects/' in x for x in call_log.read_text().splitlines());self.assertGreater(uploads,0)
     self.assertEqual(ledger.cleanup_candidates(),[]);self.assertEqual(archive.read_bytes(),archive_bytes)
     # Task-owned worker process reopens retained journals while receipt is absent.
     worker_script=r'''import json,sys
from pathlib import Path
from processing.scratch_policy import ScratchLedger
from processing.worker_outbox import WorkerOutbox
from processing.reference_engine_worker import ReferenceEngineWorker
from tests.test_reference_engine_worker import FakeNativeEngine
x=json.loads(sys.stdin.readline());root=Path(x['root'])
ledger=ScratchLedger(root/'ledger.sqlite',root/'scratch',capacity_probe=lambda _:10000000,safety_floor_bytes=0,cleanup_grace_seconds=0)
outbox=WorkerOutbox(root/'outbox/outbox.sqlite')
w=ReferenceEngineWorker(root/'journal/worker.sqlite',ledger=ledger,outbox=outbox,engine=FakeNativeEngine(),engine_id='physical-engine',node_id='cluster',authority_id=x['authority'],coordinator_origin='http://127.0.0.1:8000',gate_generation=1,worker_id='worker',engine_version='synthetic-v1',viewer_receipt_keys=x['keys'])
assert w._row(x['job'])['phase']=='delivered'
assert (root/'scratch/jobs/reservation-1/all.zip').exists() and ledger.cleanup_candidates()==[]
print('ready',flush=True);sys.stdin.read(1)
'''
     worker_child=subprocess.Popen([sys.executable,'-c',worker_script],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(worker_child)
     worker_child.stdin.write(json.dumps(dict(root=str(root),authority=authority,keys=keys,job=job['jobId']))+'\n');worker_child.stdin.flush()
     lines=queue.Queue();reader=threading.Thread(target=lambda:lines.put(worker_child.stdout.readline()),daemon=True);reader.start()
     self.assertEqual(lines.get(timeout=15).strip(),'ready');worker_child.kill();worker_child.wait(timeout=10);self.assertEqual(worker_child.returncode,-signal.SIGKILL);reader.join(timeout=5);self.assertFalse(reader.is_alive())
     self.assertEqual(archive.read_bytes(),archive_bytes);self.assertEqual(ledger.cleanup_candidates(),[])
     # A fresh actual worker/ledger/outbox reconstruction consumes the receipt later.
     ledger=ScratchLedger(root/'ledger.sqlite',scratch,capacity_probe=lambda _:10000000,safety_floor_bytes=0,cleanup_grace_seconds=0)
     outbox=WorkerOutbox(root/'outbox/outbox.sqlite')
     worker=ReferenceEngineWorker(root/'journal/worker.sqlite',ledger=ledger,outbox=outbox,engine=FakeNativeEngine(),engine_id='physical-engine',node_id='cluster',authority_id=authority,coordinator_origin='http://127.0.0.1:8000',gate_generation=1,worker_id='worker',engine_version='synthetic-v1',viewer_receipt_keys=keys)
     self.assertEqual(worker._row(job['jobId'])['phase'],'delivered');self.assertEqual(archive.read_bytes(),archive_bytes)
     results=ReferenceResults(worker)
     stop_viewer(child);q.close();q=None
     child,port=start_viewer();sender=make_sender(port);now_clock[0]+=2
     # The actual coordinator process dies after accept() commits the Viewer
     # receipt, but before its transfer worker receives the return value.
     read_fd,write_fd=os.pipe();pid=os.fork()
     if pid==0:
      try:
       os.close(read_fd);cq=Queue(root/'coordinator',reference_enabled=True);cs=ReferenceStore(cq);accept=cs.accept
       def committed(*args,**kwargs):
        value=accept(*args,**kwargs);os.write(write_fd,b'committed\n');signal.pause();return value
       cs.accept=committed
       ReferenceViewerTransferWorker(cs,sender,trusted_receipt_keys=keys,clock=lambda:now_clock[0]).run_once()
      finally:os._exit(1)
     os.close(write_fd)
     try:
      self.assertTrue(select.select([read_fd],[],[],20)[0]);self.assertEqual(os.read(read_fd,10),b'committed\n')
     finally:
      try:
       try:os.kill(pid,signal.SIGKILL)
       except ProcessLookupError:pass
       _,status=os.waitpid(pid,0)
      finally:os.close(read_fd)
     self.assertTrue(os.WIFSIGNALED(status));self.assertEqual(os.WTERMSIG(status),signal.SIGKILL)
     q=Queue(root/'coordinator',reference_enabled=True);store=ReferenceStore(q)
     self.assertEqual(ReferenceViewerTransferWorker(store,sender,trusted_receipt_keys=keys,clock=lambda:now_clock[0]).run_once()['state'],'idle')
     self.assertEqual(sum('/objects/' in x for x in call_log.read_text().splitlines()),uploads)
     relayed=store.acceptance_receipts(job['jobId'])['receipts'];self.assertEqual(len(relayed),1);self.assertEqual(archive.read_bytes(),archive_bytes)
     self.assertEqual(ledger.cleanup_candidates(),[])
     wrong=copy.deepcopy(relayed);wrong[0]['viewerReceipt']['resultManifestSha256']='0'*64
     with self.assertRaises(WorkerHeld):worker.apply_viewer_acceptance(job['jobId'],wrong)
     self.assertEqual(ledger.cleanup_candidates(),[]);self.assertEqual(archive.read_bytes(),archive_bytes)
     viewer_copy=root/'viewer-state'/'managed'/relayed[0]['acceptance']['receiptId']/(result['objects'][0]['id']+'.bin')
     self.assertEqual(viewer_copy.read_bytes(),archive_bytes)
     self.assertTrue(worker.apply_viewer_acceptance(job['jobId'],relayed)['fullyAccepted'])
     candidates=ledger.cleanup_candidates();self.assertEqual(len(candidates),1);item=candidates[0]
     # Kill an actual cleanup executor immediately after its real durable intent,
     # before execute() begins file verification, rename, or unlink.
     cleanup_script=r'''import json,sys
from pathlib import Path
from processing.scratch_policy import ScratchLedger
from processing.scratch_cleanup import ScratchCleanupExecutor
x=json.loads(sys.stdin.readline());root=Path(x['root']);item=x['item']
ledger=ScratchLedger(root/'ledger.sqlite',root/'scratch',capacity_probe=lambda _:10000000,safety_floor_bytes=0,cleanup_grace_seconds=0)
original=ledger.begin_cleanup
def checkpoint(*args,**kwargs):
 intent=original(*args,**kwargs);print(intent['intent_token'],flush=True);sys.stdin.read(1);return intent
ledger.begin_cleanup=checkpoint
ScratchCleanupExecutor(ledger).execute(item['reservation'],item['id'],generation=item['generation'],sha256=item['sha'],byte_size=item['bytes'],relative_path=item['path'])
'''
     cleanup_child=subprocess.Popen([sys.executable,'-c',cleanup_script],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(cleanup_child)
     cleanup_child.stdin.write(json.dumps(dict(root=str(root),item=dict(item)))+'\n');cleanup_child.stdin.flush()
     lines=queue.Queue();reader=threading.Thread(target=lambda:lines.put(cleanup_child.stdout.readline()),daemon=True);reader.start()
     token=lines.get(timeout=15).strip();self.assertTrue(token);cleanup_child.kill();cleanup_child.wait(timeout=10);self.assertEqual(cleanup_child.returncode,-signal.SIGKILL);reader.join(timeout=5);self.assertFalse(reader.is_alive())
     self.assertEqual(archive.read_bytes(),archive_bytes);self.assertEqual((folder/'image-1.jpg').read_bytes(),b'abc');self.assertEqual(viewer_copy.read_bytes(),archive_bytes)
     ledger=ScratchLedger(root/'ledger.sqlite',scratch,capacity_probe=lambda _:10000000,safety_floor_bytes=0,cleanup_grace_seconds=0)
     self.assertEqual(ledger.cleanup_intent(item['reservation'],item['id'])['intent_token'],token)
     # Resume real executor in another child and block after quarantine rename
     # and directory sync, just before the quarantined journal update.
     rename_script=r'''import json,sys
from pathlib import Path
from processing.scratch_policy import ScratchLedger
from processing.scratch_cleanup import ScratchCleanupExecutor
x=json.loads(sys.stdin.readline());root=Path(x['root']);item=x['item'];token=x['token']
ledger=ScratchLedger(root/'ledger.sqlite',root/'scratch',capacity_probe=lambda _:10000000,safety_floor_bytes=0,cleanup_grace_seconds=0)
executor=ScratchCleanupExecutor(ledger);original=executor._persist
def checkpoint(db,directory,intent,binding,verified,phase):
 if phase=='quarantined':print('renamed',flush=True);sys.stdin.read(1)
 return original(db,directory,intent,binding,verified,phase)
executor._persist=checkpoint
executor.execute(item['reservation'],item['id'],generation=item['generation'],sha256=item['sha'],byte_size=item['bytes'],relative_path=item['path'],intent_token=token)
'''
     rename_child=subprocess.Popen([sys.executable,'-c',rename_script],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(rename_child)
     rename_child.stdin.write(json.dumps(dict(root=str(root),item=dict(item),token=token))+'\n');rename_child.stdin.flush()
     lines=queue.Queue();reader=threading.Thread(target=lambda:lines.put(rename_child.stdout.readline()),daemon=True);reader.start()
     self.assertEqual(lines.get(timeout=15).strip(),'renamed');rename_child.kill();rename_child.wait(timeout=10);self.assertEqual(rename_child.returncode,-signal.SIGKILL);reader.join(timeout=5);self.assertFalse(reader.is_alive())
     quarantined=scratch/'.cleanup-journal'/(token+'.object')
     self.assertFalse(archive.exists());self.assertEqual(quarantined.read_bytes(),archive_bytes)
     self.assertEqual((folder/'image-1.jpg').read_bytes(),b'abc');self.assertEqual(viewer_copy.read_bytes(),archive_bytes)
     with sqlite3.connect(scratch/'.cleanup-journal'/'journal.sqlite') as db:self.assertEqual(db.execute('SELECT phase FROM deletions WHERE token=?',(token,)).fetchone()[0],'verified')
     ledger=ScratchLedger(root/'ledger.sqlite',scratch,capacity_probe=lambda _:10000000,safety_floor_bytes=0,cleanup_grace_seconds=0)
     self.assertEqual(ledger.cleanup_intent(item['reservation'],item['id'])['intent_token'],token)
     # Resume and stop immediately after the real unlink, before its directory
     # fsync and final removed journal update.
     unlink_script=r'''import json,os,sys
from pathlib import Path
from processing.scratch_policy import ScratchLedger
from processing.scratch_cleanup import ScratchCleanupExecutor
x=json.loads(sys.stdin.readline());root=Path(x['root']);item=x['item'];token=x['token']
ledger=ScratchLedger(root/'ledger.sqlite',root/'scratch',capacity_probe=lambda _:10000000,safety_floor_bytes=0,cleanup_grace_seconds=0)
executor=ScratchCleanupExecutor(ledger);original=os.unlink
def checkpoint(path,*args,**kwargs):
 result=original(path,*args,**kwargs)
 if path==token+'.object':print('unlinked',flush=True);sys.stdin.read(1)
 return result
os.unlink=checkpoint
executor.execute(item['reservation'],item['id'],generation=item['generation'],sha256=item['sha'],byte_size=item['bytes'],relative_path=item['path'],intent_token=token)
'''
     unlink_child=subprocess.Popen([sys.executable,'-c',unlink_script],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(unlink_child)
     unlink_child.stdin.write(json.dumps(dict(root=str(root),item=dict(item),token=token))+'\n');unlink_child.stdin.flush()
     lines=queue.Queue();reader=threading.Thread(target=lambda:lines.put(unlink_child.stdout.readline()),daemon=True);reader.start()
     self.assertEqual(lines.get(timeout=15).strip(),'unlinked');unlink_child.kill();unlink_child.wait(timeout=10);self.assertEqual(unlink_child.returncode,-signal.SIGKILL);reader.join(timeout=5);self.assertFalse(reader.is_alive())
     self.assertFalse(archive.exists());self.assertFalse(quarantined.exists())
     self.assertEqual((folder/'image-1.jpg').read_bytes(),b'abc');self.assertEqual(viewer_copy.read_bytes(),archive_bytes)
     with sqlite3.connect(scratch/'.cleanup-journal'/'journal.sqlite') as db:self.assertEqual(db.execute('SELECT phase FROM deletions WHERE token=?',(token,)).fetchone()[0],'unlinking')
     ledger=ScratchLedger(root/'ledger.sqlite',scratch,capacity_probe=lambda _:10000000,safety_floor_bytes=0,cleanup_grace_seconds=0)
     self.assertEqual(ledger.cleanup_intent(item['reservation'],item['id'])['intent_token'],token)
     self.assertEqual(ScratchCleanupExecutor(ledger).execute(item['reservation'],item['id'],generation=item['generation'],sha256=item['sha'],byte_size=item['bytes'],relative_path=item['path'],intent_token=token)['state'],'cleaned')
     self.assertFalse(quarantined.exists())
     self.assertFalse(archive.exists());self.assertEqual((folder/'image-1.jpg').read_bytes(),b'abc');self.assertEqual(viewer_copy.read_bytes(),archive_bytes)
   finally:
    for child in children:
     if child.poll() is None:
      child.terminate()
      try:child.wait(timeout=10)
      except subprocess.TimeoutExpired:child.kill();child.wait(timeout=10)
     for stream in (child.stdin,child.stdout,child.stderr):
      if stream:stream.close()
    if q:q.close()

if __name__=='__main__':unittest.main()
