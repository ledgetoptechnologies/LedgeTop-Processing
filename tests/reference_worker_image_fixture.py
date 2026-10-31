import copy, hashlib, importlib.util, json, os, pathlib, signal, socket, ssl, subprocess, tempfile, threading, time, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.reference_control import ReferenceControlAPI
from processing.reference_http import create_reference_http_server
from processing.reference_protocol import manifest_sha256, validate_submission
from processing.node_gate import NodeGate

root=pathlib.Path(tempfile.mkdtemp(prefix='daemon-e2e-',dir='/tmp')).resolve(); root.chmod(0o700)
os.environ['XDG_STATE_HOME']=str(root/'xdg')
for n in ('state','scratch'):
 (root/n).mkdir(mode=0o700)
cert=root/'cert.pem'; key=root/'key.pem'
subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(key),'-out',str(cert),'-days','1','-subj','/CN=127.0.0.1','-addext','subjectAltName=IP:127.0.0.1'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
key.chmod(0o600)
spec=importlib.util.spec_from_file_location('synthetic_native','/fixture/native_node_fixture.py'); native=importlib.util.module_from_spec(spec); spec.loader.exec_module(native)
engine=native.NativeNodeFixture(root/'engine.sqlite',synthetic_enabled=True).start()
queue=Queue(root/'queue',reference_enabled=True); store=ReferenceStore(queue)
fixture=json.loads(pathlib.Path('/fixture/reference-v1.contract.json').read_text()); cap=fixture['capabilities']
worker_token='SYNTHETIC-WORKER-'+'w'*36; viewer_token='SYNTHETIC-VIEWER-'+'v'*36; broker_token='SYNTHETIC-BROKER-'+'b'*36
api=ReferenceControlAPI(store,viewer_bearers={viewer_token:'staging'},worker_bearers={worker_token:dict(node_id='cluster',worker_id='worker',worker_instance_id='instance')},capability_fingerprint=cap['capabilityFingerprint'],options_fingerprint=cap['engines']['odm']['optionsFingerprint'])
tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(cert,key)
coordinator=create_reference_http_server(api,'127.0.0.1',0,ssl_context=tls); threading.Thread(target=coordinator.serve_forever,daemon=True).start()
class Broker(BaseHTTPRequestHandler):
 def log_message(self,*a):pass
 def do_GET(self):
  if self.headers.get('Authorization')!='Bearer '+broker_token or not self.path.startswith('/api/processing/reference/v1/objects/'):
   self.send_error(403);return
  self.send_response(200);self.send_header('Content-Length','3');self.end_headers();self.wfile.write(b'abc')
broker=ThreadingHTTPServer(('127.0.0.1',0),Broker);broker.socket=tls.wrap_socket(broker.socket,server_side=True);threading.Thread(target=broker.serve_forever,daemon=True).start()
port=socket.socket();port.bind(('127.0.0.1',0));worker_port=port.getsockname()[1];port.close()
origin='https://127.0.0.1:'+str(coordinator.server_port)
with NodeGate('synthetic-engine') as gate:gate.provision('cluster','http://127.0.0.1:18080',queue.authority_id)
config=dict(schemaVersion=1,stateRoot=str(root/'state'),scratchRoot=str(root/'scratch'),nodeId='cluster',workerId='worker',workerInstanceId='instance',authorityId=queue.authority_id,engineId='synthetic-engine',gateGeneration=1,gateCoordinatorOrigin='http://127.0.0.1:18080',coordinatorOrigin=origin,brokerOrigin='https://127.0.0.1:'+str(broker.server_port),engineOrigin=engine.origin,engineToken='',workerBearer=worker_token,resultReadBearer='SYNTHETIC-READER-'+'r'*36,caFile=str(cert),serverCertFile=str(cert),serverKeyFile=str(key),listenHost='127.0.0.1',listenPort=worker_port,outputBudget=1048576,safetyFloor=1024,transferTtl=240,cleanupGrace=3600,pollSeconds=.2,engineVersion='synthetic-transport-only',shareResults=True)
configpath=root/'worker.json';configpath.write_text(json.dumps(config));configpath.chmod(0o600)
raw=copy.deepcopy(fixture['submission']);raw['submissionId']=str(uuid.uuid4());obj=dict(id='image-'+raw['submissionId'],name='test.jpg',byteSize=3,sha256=hashlib.sha256(b'abc').hexdigest(),kind='image',mediaType='image/jpeg');raw['manifest'].update(objects=[obj],objectCount=1,totalBytes=3,inputCounts=dict(images=1,lidarObjects=0,points=0));raw['manifest']['sha256']=manifest_sha256(raw['manifest']);job=store.submit(validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint']))[0]
process=subprocess.Popen(['python','-m','ops.reference_worker_daemon','--odm-only','--config',str(configpath)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
try:
 end=time.monotonic()+80; granted=False; restarted=False
 while time.monotonic()<end:
  if process.poll() is not None:raise RuntimeError('worker exited '+str(process.returncode))
  state=store.get(job['jobId'],private=True)
  if state['phase']=='awaiting_transfer' and not granted:
   req=state['transferRequest'];old_port=coordinator.server_port
   coordinator.shutdown();coordinator.server_close()
   assert engine.counts()=={'initialize':0,'upload':0,'commit':0,'download':0}
   store.receive_transfer_grant(job['jobId'],dict(req,jobId=job['jobId'],environment='staging',objectIds=[obj['id']],token=broker_token));granted=True
   time.sleep(2);assert process.poll() is None
   assert engine.counts()=={'initialize':0,'upload':0,'commit':0,'download':0}
   process.kill();assert process.wait(timeout=5)==-signal.SIGKILL
   assert engine.counts()=={'initialize':0,'upload':0,'commit':0,'download':0}
   coordinator=create_reference_http_server(api,'127.0.0.1',old_port,ssl_context=tls);threading.Thread(target=coordinator.serve_forever,daemon=True).start()
   process=subprocess.Popen(['python','-m','ops.reference_worker_daemon','--odm-only','--config',str(configpath)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);restarted=True
  if state['phase']=='awaiting_acceptance':
   counts=engine.counts()
   assert counts == {'initialize':1,'upload':1,'commit':1,'download':1}, counts
   files=[p for p in (root/'scratch').rglob('*') if p.is_file()]
   inputs=[p for p in files if p.read_bytes()==b'abc']
   archives=[p for p in files if p.name=='all.zip']
   assert len(inputs)==1 and len(archives)==1
   digest=hashlib.sha256(archives[0].read_bytes()).hexdigest()
   assert digest==hashlib.sha256(native.ARCHIVE).hexdigest()
   manifest=state['resultManifest']
   assert manifest is not None and len(manifest['objects'])==1
   assert manifest['objects'][0]['sha256']==digest and manifest['objects'][0]['byteSize']==len(native.ARCHIVE)
   assert restarted
   print(json.dumps({'result':'PASS','phase':state['phase'],'engineCounts':counts,'worker_restart':True,'retainedInputSha256':hashlib.sha256(b'abc').hexdigest(),'retainedArchiveSha256':digest}));break
  time.sleep(.2)
 else:raise RuntimeError('timed out in phase '+str(store.get(job['jobId'],private=True)['phase']))
finally:
 process.send_signal(signal.SIGTERM)
 try:process.wait(timeout=10)
 except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
 coordinator.shutdown();coordinator.server_close();broker.shutdown();broker.server_close();engine.close();queue.close()
