"""Synthetic-only in-container partial HTTP response and Viewer restart.

Never start this outside the reviewed network-none fixture container. Uses
only its own /owned volume, loopback listeners and exact Node children.
"""
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import uuid
from unittest.mock import patch

from processing import reference_protocol as p
from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_http import create_reference_http_server
from processing.reference_store import ReferenceStore

GOLDEN=Path('/scope/contracts/reference-v1-acceptance-sender.golden.json')
BASE=Path('/scope/contracts/reference-v1.contract.json')
TOKEN='synthetic-viewer-token-only-00001'
EXPECTED_HASH='5c81c6973a1447efa64cd27dbac68393cea0395111fa087c5bd3c451b18dca41'

def main():
    if os.environ.get('LT_PARTIAL_DROP_SYNTHETIC')!='1':raise RuntimeError('explicit synthetic opt-in required')
    if hashlib.sha256(GOLDEN.read_bytes()).hexdigest()!=EXPECTED_HASH:raise RuntimeError('golden changed')
    # In-image preflight: no owned volume writes occur before interpreter,
    # exact Node version and built-in SQLite support have been observed.
    node=subprocess.run(['node','--version'],stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=5,check=True)
    if node.stdout.strip()!=b'v24.22.2':raise RuntimeError('Node version differs')
    check=subprocess.run(['node','-e',"require('node:sqlite');process.stdout.write('sqlite-ok')"],
        stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=5,check=True)
    if check.stdout!=b'sqlite-ok':raise RuntimeError('Node SQLite unavailable')
    print('PREFLIGHT_PYTHON',sys.version.split()[0],'NODE',node.stdout.decode().strip(),'NODE_SQLITE','available',flush=True)
    fixture=json.loads(GOLDEN.read_bytes());base=json.loads(BASE.read_bytes());raw=base['submission']
    root=Path('/owned')
    if list(root.iterdir()):raise RuntimeError('owned volume not empty')
    queue=Queue(root/'coordinator',reference_enabled=True);server=proxy=server_thread=proxy_thread=None
    started_server=started_proxy=False;state={'postHashes':[],'dropped':False}
    try:
        store=ReferenceStore(queue)
        valid=p.validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint'],
            options_fingerprint=raw['pipeline']['odm']['optionsFingerprint'],allowed_odm_options=set(raw['pipeline']['odm']['options']))
        with patch('processing.reference_store.uuid.uuid4',return_value=uuid.UUID(fixture['expected']['jobId'])):job,_=store.submit(valid)
        if (job['manifestSha256'],job['pipelineDigest'])!=(fixture['expected']['inputManifestSha256'],fixture['expected']['pipelineDigest']):raise RuntimeError('submission binding differs')
        ident=dict(node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-1')
        claim=store.claim(job['jobId'],**ident);store.reserve(job['jobId'],base['transferRequest'],node_id='cluster')
        store.update_phase(job['jobId'],'fetching');store.update_phase(job['jobId'],'processing')
        store.record_worker_result(job['jobId'],fixture['resultManifest'],claim_token=claim['claimToken'],**ident)
        queue.complete(job['jobId'],{'claimToken':claim['claimToken'],'status':'succeeded','result':{}})
        store.update_phase(job['jobId'],'awaiting_acceptance')
        api=ReferenceAPI(store,bearer_tokens={TOKEN:'staging'},capability_fingerprint=raw['capabilityFingerprint'],options_fingerprint=raw['pipeline']['odm']['optionsFingerprint'])
        server=create_reference_http_server(api,request_timeout=3)
        server_thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02});server_thread.start();started_server=True
        allowed={'/ltds/v1/jobs/'+job['jobId']+'/results','/ltds/v1/jobs/'+job['jobId']+'/acceptance'}
        class Proxy(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def log_message(self,*_):pass
            def do_GET(self):self.forward()
            def do_POST(self):self.forward()
            def forward(self):
                self.connection.settimeout(3)
                if self.path not in allowed or self.headers.get('Authorization')!='Bearer '+TOKEN or self.headers.get('Transfer-Encoding'):
                    self.send_error(400);self.close_connection=True;return
                try:
                    size=int(self.headers.get('Content-Length','0'))
                    if size<0 or size>4*1024*1024:raise ValueError()
                    body=self.rfile.read(size)
                    if len(body)!=size:raise ValueError()
                    if self.command=='POST':state['postHashes'].append(hashlib.sha256(body).hexdigest())
                    conn=http.client.HTTPConnection('127.0.0.1',server.server_address[1],timeout=3)
                    try:
                        conn.request(self.command,self.path,body=body if size else None,headers={'Authorization':'Bearer '+TOKEN,'Content-Type':'application/json'})
                        response=conn.getresponse();data=response.read(65537)
                        if len(data)>65536:raise ValueError()
                        self.send_response(response.status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.send_header('Connection','close');self.end_headers()
                        if self.command=='POST' and not state['dropped']:
                            if response.status!=200 or len(data)<2:raise ValueError()
                            self.wfile.write(data[:1]);self.wfile.flush();state['dropped']=True
                            self.connection.shutdown(socket.SHUT_RDWR);self.connection.close()
                        else:self.wfile.write(data);self.wfile.flush()
                    finally:conn.close()
                except (OSError,ValueError,http.client.HTTPException):self.close_connection=True
                self.close_connection=True
        proxy=HTTPServer(('127.0.0.1',0),Proxy);proxy.timeout=3
        proxy_thread=threading.Thread(target=proxy.serve_forever,kwargs={'poll_interval':.02});proxy_thread.start();started_proxy=True
        for mode,code,marker in (('first',86,b'SYNTHETIC_PARTIAL_RESPONSE_UNCERTAIN'),('retry',0,b'SYNTHETIC_REOPEN_EXACT_REPLAY_ACKED')):
            child=subprocess.run(['node','/scope/tests/paired_partial_drop_client.cjs',mode,str(proxy.server_address[1])],
                stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=25,check=False)
            if child.returncode!=code or marker not in child.stdout:raise RuntimeError('owned Node fixture failed: '+child.stderr.decode('utf8','replace')[-500:])
            if mode=='first':
                with queue._db() as db:
                    if db.execute('SELECT COUNT(*) FROM reference_acceptances').fetchone()[0]!=1:raise RuntimeError('first acceptance not committed')
        if not state['dropped'] or len(state['postHashes'])!=2 or state['postHashes'][0]!=state['postHashes'][1]:raise RuntimeError('request replay mismatch')
        with queue._db() as db:
            if db.execute('SELECT COUNT(*) FROM reference_acceptances').fetchone()[0]!=1:raise RuntimeError('acceptance duplicated')
        print('SYNTHETIC_PARTIAL_DROP_AND_LEDGER_RESTART_PASSED',flush=True)
    finally:
        failures=[]
        try:
            for owned,thread,started in ((proxy,proxy_thread,started_proxy),(server,server_thread,started_server)):
                if owned is None:continue
                try:
                    try:
                        if started and thread.is_alive():owned.shutdown()
                    finally:owned.server_close()
                    if started:
                        thread.join(timeout=5)
                        if thread.is_alive():failures.append('listener still active')
                except Exception:failures.append('listener cleanup uncertain')
        finally:queue.close()
        if failures:raise RuntimeError('owned listener exit uncertain')

if __name__=='__main__':main()
