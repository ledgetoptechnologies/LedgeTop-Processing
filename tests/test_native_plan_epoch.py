import json
import os
import sqlite3
import subprocess
import threading
import unittest
import uuid
from dataclasses import replace
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from unittest.mock import patch
from urllib.parse import urlencode

from processing import config,reference_protocol as ref
from processing.maintenance import Conflict
from processing.native_manifest_binding import NativeManifestBindings
from processing.native_plan_binding import NativePlanBindings
from processing.native_plan_epoch import NativePlanEpochs
from tests.test_native_manifest_binding import BODY,CAP,OTHER
from tests.test_native_plan_binding import template
from tests.test_native_plan_http import PlanHTTP
from tests.test_viewer_http import multipart,BOUNDARY


@unittest.skipUnless(os.name=='posix','authority lease requires POSIX')
class Epoch(PlanHTTP):
    def setUp(self):
        super().setUp();self.stop()
        self.epoch_qualification=patch.object(config,'NATIVE_PLAN_EPOCH_QUALIFIED',True)
        self.epoch_qualification.start();self.addCleanup(self.epoch_qualification.stop)
        self.config=replace(self.config,viewer_plan_epoch=True);self.start()

    def epoch(self):
        manifest=NativeManifestBindings(self.handler.viewer_service.store)
        plan=NativePlanBindings(self.handler.viewer_service,manifest)
        return NativePlanEpochs(self.handler.viewer_service,manifest,plan)

    def opened(self):
        operation=str(uuid.uuid4());identifier=str(uuid.uuid4());manifest=json.loads(BODY)
        manifest['id']='attempt-'+identifier;manifest['sha256']=ref.manifest_sha256(manifest)
        body=ref.canonical_json(manifest);epoch=self.epoch()
        epoch.open(operation,identifier,'production',CAP,manifest['id'],manifest['sha256'])
        return epoch,operation,identifier,manifest,body

    def intent(self,epoch,operation,identifier,manifest,body):
        receipt=epoch.bind_manifest(operation,identifier,'production',CAP,body)
        plan=template();plan.update(taskUuid=identifier,bindingId=receipt['bindingId'],manifestSha256=manifest['sha256'])
        data=ref.canonical_json(plan);intent=epoch.plan_intent(operation,identifier,'production',CAP,data)
        return receipt,data,intent['planSha256']

    def test_revoke_before_manifest_blocks_v1_and_replays(self):
        epoch,op,identifier,manifest,body=self.opened();request=str(uuid.uuid4())
        self.assertEqual(self.request('POST','/task/ledgetop/reserve',kind='application/x-www-form-urlencoded',body=urlencode({'uuid':identifier}).encode())[0],409)
        self.assertEqual(self.request('POST','/task/ledgetop/cancel',kind='application/x-www-form-urlencoded',body=urlencode({'uuid':identifier}).encode())[0],409)
        self.assertEqual(self.binding(identifier,body=body)[0],409)
        self.assertEqual(self.request('GET','/task/ledgetop/input-manifest/'+identifier)[0],409)
        self.assertEqual(self.request('GET','/task/ledgetop/task-plan/'+identifier)[0],409)
        legacy=multipart([('name',None,b'fixture'),('options',None,b'[]')])
        self.assertEqual(self.request('POST','/task/new/init',body=legacy,kind='multipart/form-data; boundary='+BOUNDARY,extra={'set-uuid':identifier})[0],409)
        receipt=epoch.revoke(op,identifier,'production',CAP,request,manifest['sha256'],None,None)
        self.assertEqual(receipt['state'],'closed_without_bind');self.assertIsNone(receipt['manifestReceipt']);self.assertIsNone(receipt['planIntentSha256'])
        self.assertEqual(epoch.status(op,identifier,'production',CAP),receipt)
        self.assertEqual(epoch.revoke(op,identifier,'production',CAP,request,manifest['sha256'],None,None),receipt)
        with self.assertRaises(Conflict):epoch.bind_manifest(op,identifier,'production',CAP,body)
        with self.assertRaises(Conflict):epoch.open(op,identifier,'production',CAP,manifest['id'],manifest['sha256'])
        with self.assertRaises((Conflict,KeyError)):epoch.status(op,identifier,'production',OTHER)
        self.stop();self.start();self.assertEqual(self.epoch().status(op,identifier,'production',CAP),receipt)

    def test_intent_stale_revoke_and_bound_then_closed(self):
        epoch,op,identifier,manifest,body=self.opened();manifest_receipt,plan,digest=self.intent(epoch,op,identifier,manifest,body)
        request=str(uuid.uuid4())
        with self.assertRaises(Conflict):epoch.revoke(op,identifier,'production',CAP,request,manifest['sha256'],manifest_receipt['bindingId'],None)
        with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):
            accepted=epoch.accept(op,identifier,'production',CAP,plan)
            self.assertEqual(epoch.accept(op,identifier,'production',CAP,plan),accepted)
        result=epoch.revoke(op,identifier,'production',CAP,request,manifest['sha256'],manifest_receipt['bindingId'],digest)
        self.assertEqual(result['state'],'bound_then_closed');self.assertEqual(result['manifestReceipt'],manifest_receipt)
        self.assertEqual(result['planIntentSha256'],digest);self.assertEqual(result['planReceipt'],accepted)
        self.assertEqual(epoch.revoke(op,identifier,'production',CAP,request,manifest['sha256'],manifest_receipt['bindingId'],digest),result)
        with self.assertRaises(Conflict):epoch.revoke(op,identifier,'production',CAP,request,manifest['sha256'],manifest_receipt['bindingId'],None)
        with self.assertRaises(Conflict):epoch.accept(op,identifier,'production',CAP,plan)
        with self.handler.prototype_queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_manifest_bindings').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],1)
        self.stop();self.start();self.assertEqual(self.epoch().status(op,identifier,'production',CAP),result)

    def test_revoke_between_intent_and_accept_retains_intent(self):
        epoch,op,identifier,manifest,body=self.opened();manifest_receipt,plan,digest=self.intent(epoch,op,identifier,manifest,body)
        result=epoch.revoke(op,identifier,'production',CAP,str(uuid.uuid4()),manifest['sha256'],manifest_receipt['bindingId'],digest)
        self.assertEqual(result['state'],'closed_without_bind');self.assertEqual(result['planIntentSha256'],digest)
        self.assertIsNone(result['planReceipt']);self.assertEqual(result['manifestReceipt'],manifest_receipt)
        with self.assertRaises(Conflict):epoch.accept(op,identifier,'production',CAP,plan)
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],0)

    def test_nonreuse_cross_environment_and_tamper_hold(self):
        epoch,op,identifier,manifest,body=self.opened();request=str(uuid.uuid4())
        with self.assertRaises((Conflict,KeyError)):epoch.status(op,identifier,'staging',CAP)
        with self.assertRaises((Conflict,KeyError)):epoch.open(op,str(uuid.uuid4()),'production',CAP,manifest['id'],manifest['sha256'])
        receipt=epoch.revoke(op,identifier,'production',CAP,request,manifest['sha256'],None,None)
        with self.assertRaises(Conflict):epoch.open(request,str(uuid.uuid4()),'production',CAP,manifest['id'],manifest['sha256'])
        other,otherop,otherid,othermanifest,_=self.opened()
        with self.assertRaises(Conflict):other.revoke(otherop,otherid,'production',CAP,request,othermanifest['sha256'],None,None)
        with self.handler.prototype_queue._db() as db:db.execute("UPDATE native_plan_epoch_revokes SET request_bytes=? WHERE request_id=?",(b'{}',request))
        with self.assertRaises(Conflict):epoch.status(op,identifier,'production',CAP)
        with self.handler.prototype_queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_epochs').fetchone()[0],2)
            self.assertEqual(db.execute('SELECT state FROM native_plan_epochs WHERE operation_id=?',(op,)).fetchone()[0],'closed_without_bind')

    def test_manifest_replay_uses_accepted_limit_and_detects_row_mutation(self):
        epoch,op,identifier,manifest,body=self.opened()
        first=epoch.bind_manifest(op,identifier,'production',CAP,body)
        epoch.store.max_input_bytes=1
        self.assertEqual(epoch.bind_manifest(op,identifier,'production',CAP,body),first)
        with self.handler.prototype_queue._db() as db:db.execute("UPDATE native_manifest_bindings SET manifest_id='changed' WHERE uuid=?",(identifier,))
        with self.assertRaises(Conflict):epoch.status(op,identifier,'production',CAP)

    def test_inconsistent_open_close_marker_blocks_further_mutation(self):
        epoch,op,identifier,manifest,body=self.opened()
        with self.handler.prototype_queue._db() as db:db.execute('UPDATE native_plan_epochs SET close_receipt=? WHERE operation_id=?',(b'{}',op))
        with self.assertRaises(Conflict):epoch.bind_manifest(op,identifier,'production',CAP,body)
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_manifest_bindings').fetchone()[0],0)

    def test_cross_namespace_identifier_collision_holds(self):
        epoch,op,identifier,manifest,body=self.opened()
        with self.handler.prototype_queue._db() as db:
            db.execute('INSERT INTO native_plan_epoch_revokes VALUES(?,?,?,?)',(op,str(uuid.uuid4()),b'retain',b'retain'))
        with self.assertRaisesRegex(Conflict,'identifier collision'):epoch.status(op,identifier,'production',CAP)
        with self.assertRaisesRegex(Conflict,'identifier collision'):epoch.bind_manifest(op,identifier,'production',CAP,body)
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT receipt FROM native_plan_epoch_revokes WHERE request_id=?',(op,)).fetchone()[0],b'retain')

    def test_missing_epoch_row_never_downgrades_reservation_to_v1(self):
        epoch,op,identifier,manifest,body=self.opened()
        with self.handler.prototype_queue._db() as db:db.execute('DELETE FROM native_plan_epochs WHERE operation_id=?',(op,))
        with self.assertRaises((Conflict,KeyError)):epoch.status(op,identifier,'production',CAP)
        self.assertEqual(self.request('POST','/task/ledgetop/reserve',kind='application/x-www-form-urlencoded',body=urlencode({'uuid':identifier}).encode())[0],409)
        self.assertEqual(self.request('POST','/task/ledgetop/cancel',kind='application/x-www-form-urlencoded',body=urlencode({'uuid':identifier}).encode())[0],409)
        self.assertEqual(self.binding(identifier,body=body)[0],409)
        with self.handler.prototype_queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_reservations WHERE uuid=?',(identifier,)).fetchone()[0],'reserved')
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_manifest_bindings').fetchone()[0],0)

    def test_late_task_history_collision_is_held_and_preserved(self):
        epoch,op,identifier,manifest,body=self.opened()
        with self.handler.prototype_queue._db() as db:
            db.execute("INSERT INTO viewer_incarnations VALUES (?,1,NULL,'retain','retain')",(identifier,))
        with self.assertRaisesRegex(Conflict,'history collision'):epoch.status(op,identifier,'production',CAP)
        with self.assertRaisesRegex(Conflict,'history collision'):epoch.bind_manifest(op,identifier,'production',CAP,body)
        with self.assertRaisesRegex(Conflict,'history collision'):epoch.revoke(op,identifier,'production',CAP,str(uuid.uuid4()),manifest['sha256'],None,None)
        with self.handler.prototype_queue._db() as db:
            self.assertEqual(db.execute('SELECT metadata,files FROM viewer_incarnations WHERE uuid=?',(identifier,)).fetchone()[:],('retain','retain'))

    def test_revoke_wins_while_catalog_fetch_is_blocked(self):
        epoch,op,identifier,manifest,body=self.opened();manifest_receipt,plan,digest=self.intent(epoch,op,identifier,manifest,body)
        entered=threading.Event();release=threading.Event();results=[]
        def catalog():
            entered.set()
            if not release.wait(4):raise TimeoutError('fixture deadline')
            return {'ready':True}
        def accept():
            try:epoch.accept(op,identifier,'production',CAP,plan);results.append('accepted')
            except Conflict:results.append('held')
        thread=threading.Thread(target=accept);started=False
        try:
            with patch.object(self.handler.viewer_service,'_catalog',side_effect=catalog):
                thread.start();started=True;self.assertTrue(entered.wait(3))
                result=epoch.revoke(op,identifier,'production',CAP,str(uuid.uuid4()),manifest['sha256'],manifest_receipt['bindingId'],digest)
                release.set();thread.join(5);self.assertFalse(thread.is_alive())
            self.assertEqual(result['state'],'closed_without_bind');self.assertEqual(results,['held'])
        finally:
            release.set()
            if started:thread.join(5);self.assertFalse(thread.is_alive())

    def test_manifest_wins_before_stale_revoke_and_is_retained(self):
        from processing import native_input_manifest
        epoch,op,identifier,manifest,body=self.opened();entered=threading.Event();release=threading.Event();revoke_started=threading.Event();outcomes=[]
        actual=native_input_manifest.validate
        def paused(*args,**kwargs):
            if not entered.is_set():
                entered.set()
                if not release.wait(5):raise TimeoutError('fixture deadline')
            return actual(*args,**kwargs)
        def bind():outcomes.append(('manifest',epoch.bind_manifest(op,identifier,'production',CAP,body)))
        def revoke():
            revoke_started.set()
            try:epoch.revoke(op,identifier,'production',CAP,str(uuid.uuid4()),manifest['sha256'],None,None);outcomes.append(('unexpected',None))
            except Conflict:outcomes.append(('held',None))
        first=threading.Thread(target=bind);second=threading.Thread(target=revoke);started=[]
        try:
            with patch.object(native_input_manifest,'validate',side_effect=paused):
                first.start();started.append(first);self.assertTrue(entered.wait(3))
                second.start();started.append(second);self.assertTrue(revoke_started.wait(3));release.set()
                for thread in started:thread.join(5);self.assertFalse(thread.is_alive())
            self.assertEqual(sorted(x[0] for x in outcomes),['held','manifest'])
            receipt=next(x[1] for x in outcomes if x[0]=='manifest')
            result=epoch.revoke(op,identifier,'production',CAP,str(uuid.uuid4()),manifest['sha256'],receipt['bindingId'],None)
            self.assertEqual(result['state'],'closed_without_bind');self.assertEqual(result['manifestReceipt'],receipt)
        finally:
            release.set()
            for thread in started:thread.join(5);self.assertFalse(thread.is_alive())

    def test_process_exit_after_accept_and_revoke_recovers_exact_receipts(self):
        epoch,op,identifier,manifest,body=self.opened();manifest_receipt,plan,digest=self.intent(epoch,op,identifier,manifest,body)
        request=str(uuid.uuid4());self.stop()
        code=r'''
import os,sys
from processing import config
from processing.config import Config
from processing.app import make_handler
from processing.native_manifest_binding import NativeManifestBindings
from processing.native_plan_binding import NativePlanBindings
from processing.native_plan_epoch import NativePlanEpochs
from tests.test_native_manifest_binding import TOKEN,STAGING,CAP
config.NATIVE_MANIFEST_BINDING_QUALIFIED=True;config.NATIVE_PLAN_BINDING_QUALIFIED=True;config.NATIVE_PLAN_EPOCH_QUALIFIED=True
c=Config('a'*32,'http://127.0.0.1:1','http://127.0.0.1:2',1000,20*1024**3,prototype_state_dir=sys.argv[1],
 prototype_node_ids=('cluster',),treedar_node_ids=('cluster',),viewer_enabled=True,viewer_token=TOKEN,viewer_staging_token=STAGING,
 authority_lease_dir=sys.argv[2],viewer_manifest_binding=True,viewer_plan_binding=True,viewer_plan_epoch=True)
h=make_handler(c);h.viewer_service._catalog=lambda:{'ready':True}
m=NativeManifestBindings(h.viewer_service.store);p=NativePlanBindings(h.viewer_service,m);e=NativePlanEpochs(h.viewer_service,m,p)
if sys.argv[3]=='accept':e.accept(sys.argv[4],sys.argv[5],'production',CAP,bytes.fromhex(sys.argv[6]))
else:e.revoke(sys.argv[4],sys.argv[5],'production',CAP,sys.argv[6],sys.argv[7],sys.argv[8],sys.argv[9])
os._exit(0)
'''
        def child(*args):
            result=subprocess.run([os.sys.executable,'-c',code,str(self.state),str(self.leases),*args],
                                  capture_output=True,text=True,timeout=15,check=False)
            self.assertEqual(result.returncode,0,result.stderr[-2000:])
        child('accept',op,identifier,plan.hex());self.start()
        current=self.epoch().status(op,identifier,'production',CAP)
        self.assertEqual(current['state'],'open_bound');self.assertEqual(current['planIntentSha256'],digest)
        original_plan_receipt=current['planReceipt'];self.stop()
        child('revoke',op,identifier,request,manifest['sha256'],manifest_receipt['bindingId'],digest);self.start()
        epoch=self.epoch();closed=epoch.status(op,identifier,'production',CAP)
        self.assertEqual(closed['state'],'bound_then_closed');self.assertEqual(closed['planReceipt'],original_plan_receipt)
        self.assertEqual(epoch.revoke(op,identifier,'production',CAP,request,manifest['sha256'],manifest_receipt['bindingId'],digest),closed)

    def test_separate_clients_accept_then_revoke_via_test_only_adapter(self):
        epoch,op,identifier,manifest,body=self.opened();manifest_receipt,plan,digest=self.intent(epoch,op,identifier,manifest,body)
        inserted=threading.Event();release=threading.Event();revoke_arrived=threading.Event();actual=epoch.plan._receipt
        class Adapter(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                try:
                    if self.path=='/accept':value=epoch.accept(op,identifier,'production',CAP,plan)
                    elif self.path=='/revoke':
                        revoke_arrived.set();value=epoch.revoke(op,identifier,'production',CAP,request,manifest['sha256'],manifest_receipt['bindingId'],digest)
                    else:raise ValueError('invalid fixture action')
                    payload=ref.canonical_json(value);self.send_response(200)
                except Exception:
                    payload=b'{}';self.send_response(409)
                self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
        def paused(*args):
            if not inserted.is_set():
                inserted.set()
                if not release.wait(5):raise TimeoutError('fixture deadline')
            return actual(*args)
        server=ThreadingHTTPServer(('127.0.0.1',0),Adapter);thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02})
        request=str(uuid.uuid4());children=[];thread.start()
        code="import http.client,sys;c=http.client.HTTPConnection('127.0.0.1',int(sys.argv[1]),timeout=5);c.request('POST',sys.argv[2],body=b'');r=c.getresponse();print(r.status);r.read();c.close()"
        def client(route):
            process=subprocess.Popen([os.sys.executable,'-c',code,str(server.server_port),route],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            children.append(process);return process
        try:
            with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}),patch.object(epoch.plan,'_receipt',side_effect=paused):
                first=client('/accept');self.assertTrue(inserted.wait(4))
                second=client('/revoke');self.assertTrue(revoke_arrived.wait(4));self.assertIsNone(second.poll())
                release.set()
                for process in (first,second):
                    out,err=process.communicate(timeout=8);self.assertEqual(process.returncode,0,err[-1000:]);self.assertEqual(out.strip(),'200')
            result=epoch.status(op,identifier,'production',CAP)
            self.assertEqual(result['state'],'bound_then_closed');self.assertIsNotNone(result['planReceipt'])
        finally:
            release.set()
            for process in children:
                if process.poll() is None:
                    process.terminate()
                    try:process.wait(timeout=2)
                    except subprocess.TimeoutExpired:process.kill();process.wait(timeout=2)
                else:process.wait(timeout=2)
            server.shutdown();server.server_close();thread.join(5);self.assertFalse(thread.is_alive())


class IncompleteMarker(PlanHTTP):
    def test_unqualified_direct_constructor_preserves_schema(self):
        manifest=NativeManifestBindings(self.handler.viewer_service.store)
        plan=NativePlanBindings(self.handler.viewer_service,manifest)
        with patch.object(config,'NATIVE_PLAN_EPOCH_QUALIFIED',False):
            with self.assertRaisesRegex(Conflict,'not qualified'):NativePlanEpochs(self.handler.viewer_service,manifest,plan)
        with self.handler.prototype_queue._db() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='native_plan_epochs'").fetchone())

    def test_lone_revoke_table_refuses_disabled_restart_without_alteration(self):
        with self.handler.prototype_queue._db() as db:
            db.execute('CREATE TABLE native_plan_epoch_revokes (sentinel TEXT)')
            db.execute("INSERT INTO native_plan_epoch_revokes VALUES ('retain')")
        self.stop()
        from processing.app import make_handler
        with self.assertRaisesRegex(RuntimeError,'native_plan_epoch_configuration_required'):make_handler(self.config)
        with sqlite3.connect(str(self.state/'jobs.sqlite3')) as db:
            self.assertEqual(db.execute('SELECT sentinel FROM native_plan_epoch_revokes').fetchall(),[('retain',)])
