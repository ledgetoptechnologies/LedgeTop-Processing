"""Real loopback HTTP loss after committed reference admission, no data broker."""
import copy
import http.client
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid

from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_http import create_reference_http_server
from processing.reference_store import ReferenceStore,PROFILE

STAGING='synthetic-staging-token-0001';PRODUCTION='synthetic-production-token-0002'
FIXTURE=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())

def coordinator(root,pipe,crash):
    queue=Queue(root);queue.allowed_profiles.add(PROFILE);store=ReferenceStore(queue)
    service=ReferenceAPI(store,bearer_tokens={STAGING:'staging',PRODUCTION:'production'},
        capability_fingerprint=FIXTURE['capabilities']['capabilityFingerprint'] if crash else 'c'*64,
        options_fingerprint=FIXTURE['capabilities']['engines']['odm']['optionsFingerprint'] if crash else 'd'*64,
        allowed_odm_options={'pc-quality','projection'})
    class Fault:
        def handle(self,method,path,headers,body=b'',*,protected_transport=False):
            result=service.handle(method,path,headers,body,protected_transport=protected_transport)
            if crash and method=='POST' and path=='/ltds/v1/jobs' and result.status==201:
                os._exit(73)  # committed in the actual store, before HTTP headers
            return result
    server=create_reference_http_server(Fault(),request_timeout=2)
    pipe.send(server.server_port);pipe.close()
    server.serve_forever(poll_interval=.02)


class LostAdmissionHTTPTest(unittest.TestCase):
    def test_commit_then_process_death_replay_and_isolation(self):
        context=multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as root:
            active=[]
            def launch(crash):
                parent,child=context.Pipe();process=context.Process(target=coordinator,args=(root,child,crash))
                process.start();child.close();active.append((process,parent))
                self.assertTrue(parent.poll(10),'coordinator startup timed out')
                port=parent.recv();return process,parent,port
            def request(port,payload,token=STAGING):
                connection=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
                try:
                    connection.request('POST','/ltds/v1/jobs',body=json.dumps(payload).encode(),headers={'Authorization':'Bearer '+token})
                    response=connection.getresponse();return response.status,json.loads(response.read())
                finally:connection.close()
            try:
                first,pipe,port=launch(True)
                with self.assertRaises((http.client.RemoteDisconnected,ConnectionResetError)):
                    request(port,FIXTURE['submission'])
                first.join(10);self.assertFalse(first.is_alive());self.assertEqual(first.exitcode,73)
                pipe.close();active.remove((first,pipe))
                second,pipe,port=launch(False)
                status,job=request(port,FIXTURE['submission']);self.assertEqual(status,200)
                status,replay=request(port,FIXTURE['submission']);self.assertEqual((status,replay['jobId']),(200,job['jobId']))
                status,_=request(port,FIXTURE['submission'],PRODUCTION);self.assertNotEqual(status,200)
                cross_environment=copy.deepcopy(FIXTURE['submission'])
                cross_environment['capabilityFingerprint']='c'*64
                cross_environment['pipeline']['odm']['optionsFingerprint']='d'*64
                status,_=request(port,cross_environment,PRODUCTION);self.assertEqual(status,409)
                changed=copy.deepcopy(FIXTURE['submission']);changed['submissionId']=str(uuid.uuid4())
                status,_=request(port,changed);self.assertEqual(status,400)
                second.terminate();second.join(5);self.assertFalse(second.is_alive())
                pipe.close();active.remove((second,pipe))
                with sqlite3.connect(Path(root)/'jobs.sqlite3') as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM reference_jobs').fetchone()[0],1)
                    self.assertEqual(db.execute('SELECT count(*) FROM jobs').fetchone()[0],1)
                self.assertFalse(any(path.suffix.lower() in ('.jpg','.jpeg','.tif') for path in Path(root).rglob('*') if path.is_file()))
            finally:
                for process,pipe in active:
                    if process.is_alive():process.terminate()
                    process.join(5);self.assertFalse(process.is_alive());pipe.close()
