import http.client
import hashlib
import json
import threading
import time
import unittest
import uuid
import subprocess
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode

from processing import config, reference_protocol as ref,viewer_http
from processing.native_plan_binding import NativePlanBindings
from tests.test_native_manifest_binding import BODY,CAP,OTHER,STAGING,TOKEN
from tests.test_native_plan_binding import template
from tests.test_native_plan_epoch import Epoch


class EpochHTTP(Epoch):
    def base(self,identifier,operation):
        return '/task/ledgetop/plan-epoch-v2/'+identifier+'/'+operation+'/1/1'

    def raw(self,method,path,body=b'',cap=CAP,token=TOKEN):
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=5)
        try:
            conn.request(method,path+'?'+urlencode({'token':token}),body=body,
                         headers={'Content-Type':'application/json','X-LedgeTop-Capability':cap})
            response=conn.getresponse();data=response.read()
            return response.status,data,response.getheader('Cache-Control')
        finally:conn.close()

    def setup_http(self):
        identifier=str(uuid.uuid4());operation=str(uuid.uuid4());manifest=json.loads(BODY)
        manifest['id']='attempt-'+identifier;manifest['sha256']=ref.manifest_sha256(manifest);base=self.base(identifier,operation)
        request={'schemaVersion':2,'operationKind':'plan_bind','manifestId':manifest['id'],'manifestSha256':manifest['sha256']}
        code,data,_=self.raw('POST',base+'/open',ref.canonical_json(request));self.assertEqual(code,200)
        return identifier,operation,manifest,base,json.loads(data)

    def test_http_full_history_exact_bytes_replay_restart(self):
        identifier,operation,manifest,base,opened=self.setup_http()
        self.assertEqual(set(opened),{'protocol','version','taskUuid','operationId','environment','generation','epoch','result'})
        self.assertEqual((opened['protocol'],opened['version'],opened['taskUuid'],opened['operationId']),('native-plan-epoch-v2-experiment',2,identifier,operation))
        code,data,cache=self.raw('GET',base);self.assertEqual((code,cache),(200,'no-store'))
        state=json.loads(data)['result'];self.assertEqual(state['state'],'open_unbound');self.assertIsNone(state['manifestReceipt']);self.assertFalse(state['planIntentPresent'])
        code,data,_=self.raw('POST',base+'/manifest',ref.canonical_json(manifest));self.assertEqual(code,200);manifest_receipt=json.loads(data)['result']
        plan=template();plan.update(taskUuid=identifier,bindingId=manifest_receipt['bindingId'],manifestSha256=manifest['sha256']);body=ref.canonical_json(plan)
        code,data,_=self.raw('POST',base+'/intent',body);self.assertEqual(code,200);intent=json.loads(data)['result']['planSha256']
        with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):
            code,data,_=self.raw('POST',base+'/accept',body);self.assertEqual(code,200);accepted=json.loads(data)['result']
            self.assertEqual(self.raw('POST',base+'/accept',body)[1],data)
        request={'schemaVersion':2,'requestId':str(uuid.uuid4()),'expectedManifestSha256':manifest['sha256'],
                 'expectedManifestBindingId':manifest_receipt['bindingId'],'expectedPlanSha256':intent};revoke=ref.canonical_json(request)
        code,closed,_=self.raw('POST',base+'/revoke',revoke);self.assertEqual(code,200)
        outcome=json.loads(closed)['result'];self.assertEqual(outcome['state'],'bound_then_closed');self.assertTrue(outcome['planIntentPresent'])
        self.assertEqual(outcome['manifestReceipt'],manifest_receipt);self.assertEqual(outcome['planReceipt'],accepted)
        self.assertNotIn(CAP.encode(),closed);self.assertNotIn(TOKEN.encode(),closed)
        self.assertEqual(self.raw('POST',base+'/revoke',revoke)[1],closed)
        self.assertEqual(self.raw('GET',base)[1],closed)
        self.assertEqual(self.raw('POST',base+'/accept',body)[0],409)
        self.stop();self.start();self.assertEqual(self.raw('GET',base)[1],closed)
        with self.handler.prototype_queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_epochs').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_epoch_revokes').fetchone()[0],1)

    def test_auth_before_body_closed_schema_and_framing(self):
        identifier,operation,manifest,base,_=self.setup_http()
        for path,cap,token in ((base+'/manifest',OTHER,TOKEN),(base+'/manifest',CAP,STAGING)):
            conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=3)
            try:
                conn.putrequest('POST',path+'?'+urlencode({'token':token}));conn.putheader('Content-Type','application/json')
                conn.putheader('Content-Length','10000');conn.putheader('X-LedgeTop-Capability',cap);conn.endheaders()
                response=conn.getresponse();self.assertEqual(response.status,404);response.read()
            finally:conn.close()
        self.assertEqual(self.raw('GET',base,cap=OTHER)[0],404)
        unknown=self.base(identifier,str(uuid.uuid4()));self.assertEqual(self.raw('GET',unknown)[0],404)
        self.assertEqual(self.raw('GET',base.replace('/1/1','/1/2'))[0],404)
        bad=b'{"schemaVersion":2,"schemaVersion":2,"requestId":"x"}'
        self.assertEqual(self.raw('POST',base+'/revoke',bad)[0],400)
        self.assertEqual(self.raw('POST',base+'/manifest',ref.canonical_json(manifest)+b' ')[0],409)
        with self.handler.prototype_queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_epoch_revokes').fetchone()[0],0)

    def test_open_capability_syntax_rejected_before_body(self):
        identifier=str(uuid.uuid4());operation=str(uuid.uuid4());base=self.base(identifier,operation)
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=3)
        try:
            conn.putrequest('POST',base+'/open?'+urlencode({'token':TOKEN}));conn.putheader('Content-Type','application/json')
            conn.putheader('Content-Length','10000');conn.putheader('X-LedgeTop-Capability','bad');conn.endheaders()
            response=conn.getresponse();self.assertEqual(response.status,400);response.read()
        finally:conn.close()
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_epochs').fetchone()[0],0)

    def test_manifest_trickle_times_out_without_binding(self):
        from tests.test_native_binding_deadline import trickle
        identifier,operation,manifest,base,_=self.setup_http();body=ref.canonical_json(manifest)
        trickle(self,base+'/manifest',body)
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_manifest_bindings').fetchone()[0],0)
        self.assertEqual(self.raw('POST',base+'/manifest',body)[0],200)

    def test_lost_revoke_response_and_closed_without_plan(self):
        identifier,operation,manifest,base,_=self.setup_http()
        request={'schemaVersion':2,'requestId':str(uuid.uuid4()),'expectedManifestSha256':manifest['sha256'],
                 'expectedManifestBindingId':None,'expectedPlanSha256':None};body=ref.canonical_json(request)
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=3)
        conn.request('POST',base+'/revoke?'+urlencode({'token':TOKEN}),body=body,
                     headers={'Content-Type':'application/json','X-LedgeTop-Capability':CAP});conn.close()
        deadline=time.monotonic()+5
        while True:
            code,data,_=self.raw('GET',base)
            if code==200 and json.loads(data)['result']['state']=='closed_without_bind':break
            if time.monotonic()>=deadline:self.fail('revoke not observable')
            time.sleep(.02)
        outcome=json.loads(data)['result'];self.assertIsNone(outcome['manifestReceipt']);self.assertFalse(outcome['planIntentPresent']);self.assertIsNone(outcome['planReceipt'])
        self.assertEqual(self.raw('POST',base+'/revoke',body)[1],data)
        self.assertEqual(self.raw('POST',base+'/manifest',ref.canonical_json(manifest))[0],409)

    def test_closed_without_plan_retains_manifest_and_intent_and_open_replays(self):
        identifier,operation,manifest,base,opened=self.setup_http()
        _,data,_=self.raw('POST',base+'/manifest',ref.canonical_json(manifest));manifest_receipt=json.loads(data)['result']
        plan=template();plan.update(taskUuid=identifier,bindingId=manifest_receipt['bindingId'],manifestSha256=manifest['sha256'])
        _,data,_=self.raw('POST',base+'/intent',ref.canonical_json(plan));digest=json.loads(data)['result']['planSha256']
        open_request={'schemaVersion':2,'operationKind':'plan_bind','manifestId':manifest['id'],'manifestSha256':manifest['sha256']}
        self.assertEqual(json.loads(self.raw('POST',base+'/open',ref.canonical_json(open_request))[1]),opened)
        request={'schemaVersion':2,'requestId':str(uuid.uuid4()),'expectedManifestSha256':manifest['sha256'],
                 'expectedManifestBindingId':manifest_receipt['bindingId'],'expectedPlanSha256':digest}
        code,data,_=self.raw('POST',base+'/revoke',ref.canonical_json(request));self.assertEqual(code,200)
        state=json.loads(data)['result'];self.assertEqual(state['state'],'closed_without_bind')
        self.assertEqual(state['manifestReceipt'],manifest_receipt);self.assertTrue(state['planIntentPresent'])
        self.assertEqual(state['planIntentSha256'],digest);self.assertIsNone(state['planReceipt'])
        self.assertEqual(self.raw('GET',base)[1],data)

    def test_revoke_after_prebody_auth_before_accept_holds_accept(self):
        identifier,operation,manifest,base,_=self.setup_http()
        _,data,_=self.raw('POST',base+'/manifest',ref.canonical_json(manifest));manifest_receipt=json.loads(data)['result']
        plan=template();plan.update(taskUuid=identifier,bindingId=manifest_receipt['bindingId'],manifestSha256=manifest['sha256']);body=ref.canonical_json(plan)
        _,data,_=self.raw('POST',base+'/intent',body);digest=json.loads(data)['result']['planSha256']
        entered=threading.Event();release=threading.Event();results=[];actual=viewer_http._binding_body
        def wait_after_auth(handler,length):
            if handler.path.startswith(base+'/accept?'):
                entered.set()
                if not release.wait(3):raise TimeoutError('fixture deadline')
            return actual(handler,length)
        def accept():results.append(self.raw('POST',base+'/accept',body)[0])
        thread=threading.Thread(target=accept);started=False
        try:
            with patch.object(viewer_http,'_binding_body',side_effect=wait_after_auth),patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):
                thread.start();started=True;self.assertTrue(entered.wait(3))
                request={'schemaVersion':2,'requestId':str(uuid.uuid4()),'expectedManifestSha256':manifest['sha256'],
                         'expectedManifestBindingId':manifest_receipt['bindingId'],'expectedPlanSha256':digest}
                code,data,_=self.raw('POST',base+'/revoke',ref.canonical_json(request));self.assertEqual(code,200)
                self.assertEqual(json.loads(data)['result']['state'],'closed_without_bind')
                release.set();thread.join(5);self.assertFalse(thread.is_alive())
            self.assertEqual(results,[409])
        finally:
            release.set()
            if started:thread.join(5);self.assertFalse(thread.is_alive())

    def test_accept_transaction_wins_and_stale_revoke_holds(self):
        identifier,operation,manifest,base,_=self.setup_http()
        _,data,_=self.raw('POST',base+'/manifest',ref.canonical_json(manifest));manifest_receipt=json.loads(data)['result']
        plan=template();plan.update(taskUuid=identifier,bindingId=manifest_receipt['bindingId'],manifestSha256=manifest['sha256']);body=ref.canonical_json(plan)
        _,data,_=self.raw('POST',base+'/intent',body);digest=json.loads(data)['result']['planSha256']
        request={'schemaVersion':2,'requestId':str(uuid.uuid4()),'expectedManifestSha256':manifest['sha256'],
                 'expectedManifestBindingId':manifest_receipt['bindingId'],'expectedPlanSha256':None}
        self.assertEqual(self.raw('POST',base+'/revoke',ref.canonical_json(request))[0],409)
        request['expectedPlanSha256']=digest
        entered=threading.Event();release=threading.Event();revoke_started=threading.Event();outcomes=[];actual=NativePlanBindings._receipt
        def paused(*args):
            if not entered.is_set():
                entered.set()
                if not release.wait(4):raise TimeoutError('fixture deadline')
            return actual(*args)
        def accept():outcomes.append(('accept',self.raw('POST',base+'/accept',body)[0]))
        def revoke():
            revoke_started.set();outcomes.append(('revoke',self.raw('POST',base+'/revoke',ref.canonical_json(request))[0]))
        threads=[threading.Thread(target=accept),threading.Thread(target=revoke)];started=[]
        try:
            with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}),patch.object(NativePlanBindings,'_receipt',side_effect=paused):
                threads[0].start();started.append(threads[0]);self.assertTrue(entered.wait(3))
                threads[1].start();started.append(threads[1]);self.assertTrue(revoke_started.wait(3));release.set()
                for thread in started:thread.join(5);self.assertFalse(thread.is_alive())
            self.assertEqual(sorted(outcomes),[('accept',200),('revoke',200)])
            state=json.loads(self.raw('GET',base)[1])['result'];self.assertEqual(state['state'],'bound_then_closed');self.assertIsNotNone(state['planReceipt'])
        finally:
            release.set()
            for thread in started:thread.join(5);self.assertFalse(thread.is_alive())


class Qualification(unittest.TestCase):
    def test_environment_cannot_enable_even_if_test_gate_is_patched(self):
        import os
        with patch.dict(os.environ,{'PROCESSING_API_TOKEN':'a'*32,'PROCESSING_VIEWER_PLAN_EPOCH':'1'},clear=True),patch.object(config,'NATIVE_PLAN_EPOCH_QUALIFIED',True):
            with self.assertRaisesRegex(ValueError,'test-only'):config.from_environment()


class WireVectors(unittest.TestCase):
    def test_python_and_node_canonical_unicode_boolean_and_null(self):
        vectors=json.loads((Path(__file__).parent/'fixtures/native_plan_epoch_wire_vectors.json').read_text(encoding='utf8'))['vectors']
        script=r"""const crypto=require('node:crypto');const b=Buffer.from(process.argv[1],'hex');
const c=x=>x===null?'null':Array.isArray(x)?'['+x.map(c).join(',')+']':typeof x==='object'?'{'+Object.keys(x).sort().map(k=>JSON.stringify(k)+':'+c(x[k])).join(',')+'}':JSON.stringify(x);
const out=Buffer.from(c(JSON.parse(b.toString('utf8'))));console.log(JSON.stringify({hex:out.toString('hex'),sha:crypto.createHash('sha256').update(out).digest('hex')}));"""
        for vector in vectors:
            data=vector['canonical'].encode('utf8');self.assertEqual(ref.canonical_json(ref.decode_submission(data)),data)
            self.assertEqual(hashlib.sha256(data).hexdigest(),vector['sha256'])
            try:node=subprocess.run(['node','-e',script,data.hex()],capture_output=True,text=True,timeout=10,check=True)
            except FileNotFoundError:self.skipTest('Node unavailable')
            observed=json.loads(node.stdout);self.assertEqual(observed['hex'],data.hex());self.assertEqual(observed['sha'],vector['sha256'])
