"""Owned real app.main/reference TLS shutdown rehearsal, synthetic state only."""
import json
import os
from pathlib import Path
import select
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import unittest

from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing import reference_protocol

CHILD = r'''
import sys
import processing.app as app
import processing.reference_runtime as runtime
Original=app.ThreadingHTTPServer
class OwnedNative(Original):
    def __init__(self,address,handler):
        super().__init__((address[0],0),handler)
        print('NATIVE='+str(self.server_address[1]),flush=True)
app.ThreadingHTTPServer=OwnedNative
original=runtime.create_reference_http_server
def reference(api,host,port,**kwargs):
    server=original(api,host,0,**kwargs)
    previous=server.finish_request
    def entered(request,address):
        print('REFERENCE_ENTERED',file=sys.stderr,flush=True)
        return previous(request,address)
    server.finish_request=entered
    print('REFERENCE='+str(server.server_address[1]),flush=True)
    return server
runtime.create_reference_http_server=reference
sys.exit(app.main())
'''


@unittest.skipUnless(sys.platform.startswith('linux'), 'POSIX process and select required')
class SigtermReferenceClaimOwned(unittest.TestCase):
    def test_app_main_commits_inflight_reference_claim_before_queue_unlock(self):
        root=Path(tempfile.mkdtemp(prefix='lt-reference-claim-sigterm-')).resolve()
        print('retained synthetic reference claim state:',root,flush=True)
        cert,key=root/'cert.pem',root/'key.pem'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes',
                        '-keyout',str(key),'-out',str(cert),'-days','1',
                        '-subj','/CN=localhost','-addext','subjectAltName=IP:127.0.0.1'],
                       stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,
                       timeout=15,check=True)
        key.chmod(0o600)
        api='synthetic-api-'+'a'*40
        viewer='synthetic-reference-viewer-'+'b'*40
        worker='synthetic-reference-cluster-'+'c'*40
        settings={'schemaVersion':1,'listener':{'host':'127.0.0.1','port':19443,
                  'certificate':str(cert),'privateKey':str(key),'clientCA':str(cert)},
                  'viewerBearers':{'production':viewer},'workers':{
                    'cluster':{'bearer':worker,'workerId':'worker-cluster','instanceId':'instance-cluster',
                               'resultOrigin':'https://cluster.example','resultBearer':'synthetic-result-cluster-'+'d'*40},
                    'main':{'bearer':'synthetic-reference-main-'+'e'*40,'workerId':'worker-main','instanceId':'instance-main',
                            'resultOrigin':'https://main.example','resultBearer':'synthetic-result-main-'+'f'*40}},
                  'streamBudget':{'maxConcurrent':1,'maxObjectBytes':1000000}}
        config=root/'reference.json'; config.write_text(json.dumps(settings));config.chmod(0o600)
        queue=Queue(root/'queue',node_ids=('cluster','main'),reference_enabled=True)
        try:
            store=ReferenceStore(queue)
            fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
            submission=reference_protocol.validate_submission(fixture['submission'],environment='staging',
                capability_fingerprint=fixture['capabilities']['capabilityFingerprint'])
            job,_=store.submit(submission)
            job_id=job['jobId']
        finally:queue.close()
        env={key:os.environ[key] for key in ('PATH','PYTHONDONTWRITEBYTECODE') if key in os.environ}
        env.update(PROCESSING_API_TOKEN=api,PROCESSING_PROTOTYPE_STATE_DIR=str(root/'queue'),
                   PROCESSING_PROTOTYPE_NODE_IDS='cluster,main',PROCESSING_BIND_HOST='127.0.0.1',
                   PROCESSING_REFERENCE_CONFIG=str(config),LOCAL_NODE_URL='http://127.0.0.1:1',
                   MAIN_NODE_URL='http://127.0.0.1:2')
        proc=subprocess.Popen([sys.executable,'-u','-c',CHILD],cwd=Path(__file__).resolve().parents[1],
                              env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,bufsize=0)
        connection=None
        def line(pipe,timeout):
            ready,_,_=select.select([pipe],[],[],timeout)
            if not ready:self.fail('owned reference child did not report progress')
            return pipe.readline().decode().strip()
        def assert_locked():
            other=None
            try:
                try:other=Queue(root/'queue',node_ids=('cluster','main'),reference_enabled=True)
                except RuntimeError:return
                self.fail('queue lock unexpectedly available')
            finally:
                if other is not None:other.close()
        try:
            self.assertRegex(line(proc.stdout,8),r'^NATIVE=[0-9]+$')
            deadline=time.monotonic()+8; ref=None
            while time.monotonic()<deadline:
                current=line(proc.stdout,max(.1,deadline-time.monotonic()))
                if current.startswith('REFERENCE='):
                    ref=int(current.removeprefix('REFERENCE='));break
            self.assertIsNotNone(ref)
            context=ssl.create_default_context(cafile=str(cert))
            raw=socket.create_connection(('127.0.0.1',ref),timeout=3)
            try:connection=context.wrap_socket(raw,server_hostname='127.0.0.1')
            except BaseException:raw.close();raise
            payload=json.dumps({'jobId':job_id,'claimRequestId':'synthetic-sigterm-claim'}).encode()
            headers=(f'POST /ltds/worker/v1/claim HTTP/1.1\r\nHost: localhost\r\n'
                     f'Authorization: Bearer {worker}\r\nContent-Type: application/json\r\n'
                     f'Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n').encode()
            connection.sendall(headers+payload[:1])
            self.assertEqual(line(proc.stderr,5),'REFERENCE_ENTERED')
            assert_locked()
            proc.send_signal(signal.SIGTERM)
            time.sleep(.35)
            self.assertIsNone(proc.poll(),'app.main exited with an in-flight reference request')
            assert_locked()
            connection.sendall(payload[1:])
            reply=b''
            while True:
                chunk=connection.recv(4096)
                if not chunk:break
                reply+=chunk
                self.assertLess(len(reply),65536)
            self.assertIn(b'200 OK',reply)
            out,err=proc.communicate(timeout=8)
            self.assertEqual(proc.returncode,0,err.decode(errors='replace'))
            for secret in (api,viewer,worker):self.assertNotIn(secret.encode(),out+err)
            reopened=Queue(root/'queue',node_ids=('cluster','main'),reference_enabled=True)
            try:
                recovered=reopened.get(job_id)
                self.assertEqual((recovered['status'],recovered['heldReason']),('held','coordinator_restart'))
                with reopened._db() as db:
                    row=db.execute('SELECT worker_id,instance_id FROM reference_claims WHERE job_id=? AND request_id=?',
                                   (job_id,'synthetic-sigterm-claim')).fetchone()
                    self.assertEqual(tuple(row),('worker-cluster','instance-cluster'))
            finally:reopened.close()
        finally:
            if connection is not None:connection.close()
            if proc.poll() is None:
                try:proc.kill()
                except OSError:pass
            try:proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                print('owned reference child exit unverified; PID:',proc.pid,flush=True)
                raise
