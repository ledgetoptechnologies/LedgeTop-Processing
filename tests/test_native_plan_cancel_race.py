"""Existing v1 behavior only: distinct synthetic clients and one coordinator."""
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch

from processing import reference_protocol as ref
from processing.native_plan_binding import NativePlanBindings
from tests.test_native_plan_http import PlanHTTP

CLIENT = r'''
import http.client,sys
from urllib.parse import urlencode
from tests.test_native_manifest_binding import TOKEN,CAP
port,identifier,mode,body=sys.argv[1:]
if mode=='plan':
 path='/task/ledgetop/task-plan/'+identifier; data=bytes.fromhex(body); kind='application/json'
else:
 path='/task/ledgetop/cancel'; data=urlencode({'uuid':identifier}).encode();kind='application/x-www-form-urlencoded'
connection=http.client.HTTPConnection('127.0.0.1',int(port),timeout=5)
try:
 connection.request('POST',path+'?'+urlencode({'token':TOKEN}),body=data,headers={'Content-Type':kind,'X-LedgeTop-Capability':CAP})
 response=connection.getresponse();response.read();print(response.status)
finally:connection.close()
'''


class PlanCancelRace(PlanHTTP):
    def spawn_client(self, identifier, mode, body=b''):
        process=subprocess.Popen([sys.executable,'-c',CLIENT,str(self.server.server_port),identifier,mode,body.hex()],
                                 stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        def cleanup():
            if process.poll() is None:
                process.terminate()
                try:process.wait(timeout=2)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=2)
            else:process.wait(timeout=2)
        self.addCleanup(cleanup)
        return process

    def result(self, process):
        out,err=process.communicate(timeout=8)
        self.assertEqual(process.returncode,0,err[-1000:])
        return int(out.strip())

    def test_cancel_commits_before_plan_transaction(self):
        identifier,plan=self.prepare();body=ref.canonical_json(plan)
        entered=threading.Event();release=threading.Event()
        def wait_catalog():
            entered.set()
            if not release.wait(5):raise TimeoutError('fixture deadline')
            return {'ready':True}
        try:
            with patch.object(self.handler.viewer_service,'_catalog',side_effect=wait_catalog):
                plan_client=self.spawn_client(identifier,'plan',body);self.assertTrue(entered.wait(4))
                cancel_client=self.spawn_client(identifier,'cancel');self.assertEqual(self.result(cancel_client),200)
                release.set();self.assertEqual(self.result(plan_client),409)
            with self.handler.prototype_queue._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT state FROM viewer_reservations WHERE uuid=?',(identifier,)).fetchone()[0],'cancelled')
        finally:release.set()

    def test_plan_transaction_commits_before_cancel_and_remains_historical(self):
        identifier,plan=self.prepare();body=ref.canonical_json(plan);path='/task/ledgetop/task-plan/'+identifier
        inserted=threading.Event();release=threading.Event();cancel_arrived=threading.Event()
        actual_receipt=NativePlanBindings._receipt;actual_cancel=self.handler.viewer_service.cancel_reserved
        def paused_receipt(*args):
            inserted.set()
            if not release.wait(5):raise TimeoutError('fixture deadline')
            return actual_receipt(*args)
        def observed_cancel(*args):
            cancel_arrived.set();return actual_cancel(*args)
        try:
            with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}), \
                 patch.object(NativePlanBindings,'_receipt',side_effect=paused_receipt), \
                 patch.object(self.handler.viewer_service,'cancel_reserved',side_effect=observed_cancel):
                plan_client=self.spawn_client(identifier,'plan',body);self.assertTrue(inserted.wait(4))
                cancel_client=self.spawn_client(identifier,'cancel');self.assertTrue(cancel_arrived.wait(4))
                self.assertIsNone(cancel_client.poll());release.set()
                self.assertEqual(self.result(plan_client),200);self.assertEqual(self.result(cancel_client),200)
            self.assertEqual(self.request('GET',path)[0],409)
            self.stop();self.start();self.assertEqual(self.request('GET',path)[0],409)
            with self.handler.prototype_queue._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT state FROM viewer_reservations WHERE uuid=?',(identifier,)).fetchone()[0],'cancelled')
        finally:release.set()
