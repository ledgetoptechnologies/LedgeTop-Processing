"""Normal frozen coordinator boots before workers; real TLS/refused discovery.

One existing quarantine fixture, no fake readiness or worker server. The child
spy only records outbound urllib requests and calls the original transport.
Run on Linux in a network=none task container with readonly frozen source/tmpfs.
"""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import unittest
import uuid
from unittest.mock import patch

from tests import test_first_coordinator_quarantine as _quarantine


@unittest.skipUnless(sys.platform.startswith('linux'),'normal Linux authority/process fixture')
class BootstrapWorkerCatalogIndependenceTests(unittest.TestCase):
    def test_normal_frozen_boot_and_health_make_no_catalog_request_without_workers(self):
        h=_quarantine.FirstCoordinatorQuarantineTests(methodName='runTest')
        self.addCleanup(h.doCleanups);h.setUp()
        marker=h.root/'outbound-catalog-observations.jsonl'
        # Keep the worker ports reserved but explicitly NOT listening. They
        # cannot accidentally address another fixture, and real discovery gets
        # connection-refused rather than a fabricated ready/unready response.
        endpoints=set()
        for node in ('cluster','main'):
            reserved=socket.socket();self.addCleanup(reserved.close)
            reserved.bind(('127.0.0.1',0));port=reserved.getsockname()[1]
            h.bootstrap['workers'][node]['resultOrigin']='https://127.0.0.1:'+str(port)
            endpoints.add(port)
        h.write_config(h.bootstrap)
        bootstrap_rows=h.legacy_rows();bootstrap_files=h.artifact_bytes()
        script=('import sys,json,runpy,urllib.request\nfrom pathlib import Path\nfrom urllib.parse import urlsplit\n'
                'sys.dont_write_bytecode=True\nmarker=Path('+repr(str(marker))+')\n'
                'original=urllib.request.OpenerDirector.open\n'
                'def observe(self,request,*args,**kwargs):\n'
                ' url=urlsplit(request.full_url if hasattr(request,"full_url") else request)\n'
                ' with marker.open("a") as f:f.write(json.dumps(dict(host=url.hostname,port=url.port,path=url.path))+"\\n")\n'
                ' return original(self,request,*args,**kwargs)\n'
                'urllib.request.OpenerDirector.open=observe\n'
                'runpy.run_module("processing.app",run_name="__main__")\n')
        real_popen=subprocess.Popen
        def launch(argv,**kwargs):
            self.assertEqual(argv,[sys.executable,'-m','processing.app'])
            return real_popen([sys.executable,'-B','-c',script],**kwargs)
        def calls():
            return [json.loads(line) for line in marker.read_text().splitlines()] if marker.exists() else []
        with patch('tests.test_first_coordinator_quarantine.subprocess.Popen',side_effect=launch):
            h.start_child()
            self.assertEqual(calls(),[],'constructor/start/health must not contact any catalog/native HTTP endpoint')
            status,_,_=h.request('GET','/healthz');self.assertEqual(status,200)
            self.assertEqual(calls(),[])
            # Ordinary OPEN requests really attempt each configured worker and
            # fail closed; no direct native catalog or cached ready substitute.
            for method,path,payload in (('GET','/ltds/v1/options',None),('POST','/ltds/v1/jobs',{})):
                status,_,body=h.request(method,path,h.viewer,payload,tls=True)
                self.assertEqual((status,body),(503,{'error':'engine_catalog_unavailable'}))
            observed=calls();self.assertEqual(len(observed),4)
            self.assertEqual({row['port'] for row in observed},endpoints)
            self.assertTrue(all(row['host']=='127.0.0.1' and row['path']=='/ltds/worker/v1/catalog' for row in observed))
            status,_,state=h.request('GET','/v1/prototype/coordinator-freeze',h.operator)
            self.assertEqual(status,200);self.assertEqual(state['phase'],'open')
            first_startup=state['startupId'];plan=str(uuid.uuid4())
            for action in ('intent','drain','seal'):
                status,_,state=h.request('POST','/v1/prototype/coordinator-freeze',h.operator,
                    dict(requestId=str(uuid.uuid5(uuid.UUID(plan),action)),planId=plan,
                         expectedRevision=state['revision'],action=action,startupId=first_startup))
                self.assertEqual(status,200,state)
            self.assertEqual(state['phase'],'frozen')
            h.stop_child()
            self.assertEqual(h.legacy_rows(),bootstrap_rows);self.assertEqual(h.artifact_bytes(),bootstrap_files)
            before_restart=calls()
            # This is the actual first-worker activation prerequisite: same
            # schema2 bootstrap, persisted frozen gate, neither worker started.
            h.start_child()
            self.assertEqual(calls(),before_restart,'frozen constructor/start/health must not depend on workers')
            status,_,state=h.request('GET','/v1/prototype/coordinator-freeze',h.operator)
            self.assertEqual(status,200);self.assertEqual(state['phase'],'frozen')
            self.assertEqual(state['planId'],plan);self.assertNotEqual(state['startupId'],first_startup)
            for method,path in (('GET','/ltds/v1/options'),('POST','/ltds/v1/jobs')):
                status,headers,body=h.request(method,path,h.viewer,tls=True,headers_only=method=='POST')
                self.assertEqual((status,body),(503,{'error':'coordinator_maintenance'}))
                self.assertEqual(headers['Retry-After'],'2')
            self.assertEqual(calls(),before_restart,'frozen ingress must refuse before discovery/body intake')
            self.assertEqual(h.request('GET','/healthz')[0],200)
            h.stop_child()
        self.assertEqual(h.legacy_rows(),bootstrap_rows);self.assertEqual(h.artifact_bytes(),bootstrap_files)


if __name__=='__main__':unittest.main()
