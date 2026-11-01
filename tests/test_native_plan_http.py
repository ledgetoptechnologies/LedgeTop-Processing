import http.client
import os
import threading
import time
import unittest
import uuid
from dataclasses import replace
from unittest.mock import patch
from urllib.parse import urlencode

from processing import config, reference_protocol as ref
from processing.app import make_handler
from processing.config import Config
from processing.native_manifest_binding import NativeManifestBindings
from processing.native_plan_binding import NativePlanBindings, plan_sha
from tests.test_native_manifest_binding import ManifestBindingHTTP, CAP, OTHER, STAGING, TOKEN
from tests.test_native_plan_binding import template


@unittest.skipUnless(os.name == 'posix','authority lease requires POSIX')
class PlanHTTP(ManifestBindingHTTP):
    def setUp(self):
        super().setUp()
        self.stop()
        self.plan_qualification=patch.object(config,'NATIVE_PLAN_BINDING_QUALIFIED',True)
        self.plan_qualification.start(); self.addCleanup(self.plan_qualification.stop)
        self.config=replace(self.config,viewer_plan_binding=True)
        self.start()

    def prepare(self):
        identifier=str(uuid.uuid4());self.reserve(identifier)
        status,manifest=self.binding(identifier);self.assertEqual(status,200)
        plan=template();plan.update(taskUuid=identifier,bindingId=manifest['bindingId'],manifestSha256=manifest['manifestSha256'])
        return identifier,plan

    def test_http_exact_replay_auth_restart_and_cancel(self):
        identifier,plan=self.prepare();path='/task/ledgetop/task-plan/'+identifier;body=ref.canonical_json(plan)
        with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):
            status,receipt=self.request('POST',path,body=body)
            self.assertEqual(status,200);self.assertEqual(self.request('POST',path,body=body),(200,receipt))
        self.assertEqual(receipt['version'],1)
        self.assertEqual((receipt['taskUuid'],receipt['environment'],receipt['generation']),(identifier,'production',1))
        self.assertEqual((receipt['bindingId'],receipt['manifestSha256']),(plan['bindingId'],plan['manifestSha256']))
        self.assertEqual(receipt['catalogDigest'],plan['expectedCatalogDigest'])
        self.assertEqual(receipt['planSha256'],plan_sha(body))
        self.assertEqual(set(receipt),{'protocol','version','taskUuid','environment','generation','bindingId','manifestSha256',
                                      'planSha256','catalogDigest','state','engineEvidence','effectiveOptions','capacityReady','dispatchReady'})
        self.assertEqual((receipt['engineEvidence'],receipt['effectiveOptions']),({'state':'unavailable'},{'state':'unavailable'}))
        self.assertFalse(receipt['capacityReady']);self.assertFalse(receipt['dispatchReady'])
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=3)
        try:
            conn.request('GET',path+'?'+urlencode({'token':TOKEN}),headers={'X-LedgeTop-Capability':CAP})
            response=conn.getresponse();self.assertEqual(response.status,200)
            self.assertEqual(response.getheader('Cache-Control'),'no-store');self.assertEqual(response.getheader('Content-Type'),'application/json')
            self.assertEqual(len(response.read()),int(response.getheader('Content-Length')))
        finally:conn.close()
        self.assertEqual(self.request('GET',path,cap=OTHER)[0],404)
        self.assertEqual(self.request('GET',path,token=STAGING)[0],404)
        self.assertEqual(self.request('POST',path,body=ref.canonical_json({**plan,'outputs':[]}))[0],409)
        self.stop();self.start()
        with patch.object(self.handler.viewer_service,'_catalog',side_effect=RuntimeError('offline')):
            self.assertEqual(self.request('GET',path),(200,receipt))
            self.assertEqual(self.request('POST',path,body=body),(200,receipt))
        status,_=self.request('POST','/task/ledgetop/cancel',kind='application/x-www-form-urlencoded',body=urlencode({'uuid':identifier}).encode())
        self.assertEqual(status,200);self.assertEqual(self.request('GET',path)[0],409)
        with self.handler.prototype_queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_tasks').fetchone()[0],0)

    def test_wrong_capability_rejected_before_body_and_nonempty_held(self):
        identifier,plan=self.prepare();path='/task/ledgetop/task-plan/'+identifier
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=3)
        try:
            conn.putrequest('POST',path+'?'+urlencode({'token':TOKEN}))
            conn.putheader('Content-Type','application/json');conn.putheader('Content-Length','10000')
            conn.putheader('X-LedgeTop-Capability',OTHER);conn.endheaders()
            response=conn.getresponse();self.assertEqual(response.status,404);response.read()
        finally:conn.close()
        plan['options']=[{'name':'fixture','value':{'type':'bool','value':True}}]
        self.assertEqual(self.request('POST',path,body=ref.canonical_json(plan))[0],409)
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],0)

    def test_http_concurrent_conflict_and_stored_authority_change(self):
        identifier,plan=self.prepare();path='/task/ledgetop/task-plan/'+identifier
        bodies=[ref.canonical_json(plan),ref.canonical_json({**plan,'outputs':[]})];results=[];barrier=threading.Barrier(2)
        def attempt(body):
            barrier.wait(3);results.append(self.request('POST',path,body=body)[0])
        with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):
            threads=[threading.Thread(target=attempt,args=(body,)) for body in bodies]
            for thread in threads:thread.start()
            for thread in threads:thread.join(5);self.assertFalse(thread.is_alive())
        self.assertEqual(sorted(results),[200,409])
        with self.handler.prototype_queue._db() as db:
            db.execute("UPDATE native_plan_bindings SET authority='changed' WHERE uuid=?",(identifier,))
        self.assertEqual(self.request('GET',path)[0],409)

    def test_disconnected_post_reconciles_by_status_without_duplicate(self):
        identifier,plan=self.prepare();path='/task/ledgetop/task-plan/'+identifier;body=ref.canonical_json(plan)
        with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):
            conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=3)
            conn.request('POST',path+'?'+urlencode({'token':TOKEN}),body=body,
                         headers={'Content-Type':'application/json','X-LedgeTop-Capability':CAP})
            conn.close()
            deadline=time.monotonic()+5
            while True:
                status,receipt=self.request('GET',path)
                if status==200:break
                if time.monotonic()>=deadline:self.fail('binding not observable after disconnect')
                time.sleep(.02)
            self.assertEqual(self.request('POST',path,body=body),(200,receipt))
        with self.handler.prototype_queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],1)

    def test_framing_and_encoding_rejected(self):
        identifier,plan=self.prepare();path='/task/ledgetop/task-plan/'+identifier;body=ref.canonical_json(plan)
        for headers in ({'Content-Type':'application/json','Content-Encoding':'gzip'},
                        {'Content-Type':'application/json','Transfer-Encoding':'chunked'}):
            self.assertIn(self.request('POST',path,body=body,extra=headers)[0],(400,415))
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=3)
        try:
            conn.putrequest('POST',path+'?'+urlencode({'token':TOKEN}));conn.putheader('Content-Type','application/json')
            conn.putheader('Content-Length',str(len(body)));conn.putheader('Content-Length',str(len(body)))
            conn.putheader('X-LedgeTop-Capability',CAP);conn.endheaders()
            response=conn.getresponse();self.assertEqual(response.status,411);response.read()
        finally:conn.close()
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],0)

    def test_http_does_not_expose_internally_bound_nonempty_plan(self):
        identifier,plan=self.prepare();path='/task/ledgetop/task-plan/'+identifier
        option={'name':'fixture','value':{'type':'bool','value':True}}
        catalog={'ready':True,'commonOptions':[{'name':'fixture','type':'bool','domain':'','value':'false'}]}
        internal=NativePlanBindings(self.handler.viewer_service,NativeManifestBindings(self.handler.viewer_service.store))
        with patch.object(self.handler.viewer_service,'_catalog',return_value=catalog):
            plan['options']=[option];plan['expectedCatalogDigest']=internal.catalog_digest(plan['options'])
            internal.bind(identifier,'production',CAP,ref.canonical_json(plan))
        self.assertEqual(self.request('GET',path)[0],409)
        self.assertEqual(self.request('POST',path,body=ref.canonical_json(plan))[0],409)
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],1)

    def test_cancellation_after_prebody_authorization_prevents_bind(self):
        from processing import viewer_http
        identifier,plan=self.prepare();path='/task/ledgetop/task-plan/'+identifier;body=ref.canonical_json(plan)
        entered=threading.Event();release=threading.Event();results=[];actual=viewer_http._length
        def wait_after_authorization(handler,maximum,**kwargs):
            if handler.path.startswith(path+'?'):
                entered.set()
                if not release.wait(3):raise TimeoutError('fixture timeout')
            return actual(handler,maximum,**kwargs)
        def post():results.append(self.request('POST',path,body=body)[0])
        thread=threading.Thread(target=post);started=False
        try:
            with patch.object(viewer_http,'_length',side_effect=wait_after_authorization), patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):
                thread.start();started=True;self.assertTrue(entered.wait(3))
                status,_=self.request('POST','/task/ledgetop/cancel',kind='application/x-www-form-urlencoded',body=urlencode({'uuid':identifier}).encode())
                self.assertEqual(status,200);release.set();thread.join(5);self.assertFalse(thread.is_alive())
            self.assertEqual(results,[409])
            with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],0)
        finally:
            release.set()
            if started:
                thread.join(5);self.assertFalse(thread.is_alive())

    def test_disabled_marker_refuses_restart(self):
        identifier,plan=self.prepare();path='/task/ledgetop/task-plan/'+identifier
        with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):
            self.assertEqual(self.request('POST',path,body=ref.canonical_json(plan))[0],200)
        self.stop()
        with self.assertRaisesRegex(RuntimeError,'native_plan_binding_configuration_required'):
            make_handler(replace(self.config,viewer_plan_binding=False))


class Qualification(unittest.TestCase):
    def test_environment_cannot_enable_even_if_qualification_patched(self):
        with patch.dict(os.environ,{'PROCESSING_API_TOKEN':'a'*32,'PROCESSING_VIEWER_PLAN_BINDING':'1'},clear=True), patch.object(config,'NATIVE_PLAN_BINDING_QUALIFIED',True):
            with self.assertRaisesRegex(ValueError,'test-only'):config.from_environment()

    def test_unqualified_direct_config_refuses_before_creating_state(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as root:
            state=Path(root)/'not-created'
            candidate=Config('a'*32,'','',1000,100,prototype_state_dir=str(state),viewer_enabled=True,
                             viewer_token=TOKEN,authority_lease_dir=str(Path(root)/'lease'),
                             viewer_manifest_binding=True,viewer_plan_binding=True)
            with patch.object(config,'NATIVE_PLAN_BINDING_QUALIFIED',False):
                with self.assertRaisesRegex(RuntimeError,'native_plan_binding_not_qualified'):make_handler(candidate)
            self.assertFalse(state.exists())
