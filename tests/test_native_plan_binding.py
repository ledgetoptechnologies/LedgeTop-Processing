import copy
import json
import os
from pathlib import Path
import subprocess
import threading
import unittest
import uuid
from unittest.mock import patch

from processing import config, reference_protocol as ref
from processing.maintenance import Conflict
from processing.native_plan_binding import NativePlanBindings, catalog_sha, plan_sha, validate
from processing.native_manifest_binding import NativeManifestBindings
from tests.test_native_manifest_binding import ManifestBindingHTTP, BODY, CAP, OTHER


def template():
    return {'schemaVersion':1, 'environment':'production',
            'taskUuid':'00000000-0000-4000-8000-000000000001', 'generation':1,
            'bindingId':'a'*64, 'manifestSha256':'b'*64, 'taskName':'Test \U0001f30d',
            'options':[], 'outputs':None, 'expectedCatalogDigest':catalog_sha(b'[]')}


class ScalarVectors(unittest.TestCase):
    def test_scalars_and_canonical_bytes(self):
        for bits in ('0000000000000000','0000000000000001','3fb999999999999a','3ff0000000000000','433fffffffffffff'):
            item = template(); item['options'] = [{'name':'value','value':{'type':'number','bits':bits}}]
            self.assertEqual(validate(ref.canonical_json(item)), item)
        for bits in ('8000000000000000','7ff0000000000000','7ff8000000000000','4340000000000000'):
            item = template(); item['options'] = [{'name':'value','value':{'type':'number','bits':bits}}]
            with self.assertRaises(ValueError): validate(ref.canonical_json(item))
        for scalar in ({'type':'bool','value':True},{'type':'string','value':'\U0001f30d'},{'type':'null'}):
            item = template(); item['options'] = [{'name':'value','value':scalar}]
            self.assertEqual(validate(ref.canonical_json(item)), item)
        item = template(); body = ref.canonical_json(item)
        self.assertNotEqual(plan_sha(body), plan_sha(ref.canonical_json({**item, 'outputs':[]})))
        for raw in (b'\xef\xbb\xbf'+body, body+b' ', body.replace(b'"schemaVersion":1',b'"schemaVersion":1,"schemaVersion":1'),
                    body.replace(b'"schemaVersion":1',b'"schemaVersion":1.0')):
            with self.assertRaises(ValueError): validate(raw)
        item['taskName']='\ud800'
        with self.assertRaises(ValueError): ref.canonical_json(item)

    def test_node_binary64_and_canonical_digest(self):
        # Node checks independently encoded bits and the exact digest input.
        vectors=json.loads((Path(__file__).parent/'fixtures/native_plan_vectors.json').read_text(encoding='utf8'))['vectors']
        script = r"""const crypto=require('node:crypto');const supplied=Buffer.from(process.argv[1],'hex');
const canonical=x=>x===null?'null':Array.isArray(x)?'['+x.map(canonical).join(',')+']':typeof x==='object'?'{'+Object.keys(x).sort().map(k=>JSON.stringify(k)+':'+canonical(x[k])).join(',')+'}':JSON.stringify(x);
const body=Buffer.from(canonical(JSON.parse(supplied.toString('utf8'))));
const nums=[0,Number.MIN_VALUE,0.1,1,Number.MAX_SAFE_INTEGER];
const bits=nums.map(x=>{let b=Buffer.alloc(8);b.writeDoubleBE(x);return b.toString('hex')});
console.log(JSON.stringify({bits,canonical:body.toString('hex'),digest:crypto.createHash('sha256').update('ltds-viewer-task-plan-v1\0').update(body).digest('hex')}));"""
        for vector in vectors:
            body=vector['canonical'].encode('utf8'); validate(body)
            try: result = subprocess.run(['node','-e',script,body.hex()],capture_output=True,text=True,timeout=10,check=True)
            except FileNotFoundError: self.skipTest('Node unavailable')
            observed=json.loads(result.stdout)
            self.assertEqual(observed['bits'],['0000000000000000','0000000000000001','3fb999999999999a','3ff0000000000000','433fffffffffffff'])
            self.assertEqual(observed['canonical'],body.hex())
            self.assertEqual(observed['digest'],vector['planSha256'])
            self.assertEqual(plan_sha(body),vector['planSha256'])


@unittest.skipUnless(os.name == 'posix','authority lease requires POSIX')
class Binding(ManifestBindingHTTP):
    def construct(self):
        return NativePlanBindings(self.handler.viewer_service,NativeManifestBindings(self.handler.viewer_service.store))

    def plan_for(self, identifier):
        self.reserve(identifier)
        item=json.loads(BODY); item['id']='attempt-'+identifier; item['sha256']=ref.manifest_sha256(item)
        status, manifest=self.binding(identifier,body=ref.canonical_json(item)); self.assertEqual(status,200)
        plan=template(); plan.update(taskUuid=identifier,bindingId=manifest['bindingId'],manifestSha256=manifest['manifestSha256'])
        return plan

    def test_binding_replay_status_race_and_catalog_outage(self):
        identifier=str(uuid.uuid4()); plan=self.plan_for(identifier)
        with patch.object(config,'NATIVE_PLAN_BINDING_QUALIFIED',True):
            binding=self.construct()
            body=ref.canonical_json(plan)
            with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):
                results=[]
                threads=[threading.Thread(target=lambda:results.append(binding.bind(identifier,'production',CAP,body))) for _ in range(4)]
                for thread in threads: thread.start()
                for thread in threads: thread.join(5); self.assertFalse(thread.is_alive())
                self.assertEqual(len(results),4); self.assertTrue(all(result==results[0] for result in results))
            with patch.object(self.handler.viewer_service,'_catalog',side_effect=RuntimeError('offline')):
                self.assertEqual(binding.bind(identifier,'production',CAP,body),results[0])
                self.assertEqual(binding.status(identifier,'production',CAP),results[0])
            with self.assertRaises((Conflict,KeyError)): binding.status(identifier,'production',OTHER)
            with self.assertRaises((Conflict,KeyError)): binding.status(identifier,'staging',CAP)
            changed={**plan,'outputs':[]}
            with self.assertRaises(Conflict): binding.bind(identifier,'production',CAP,ref.canonical_json(changed))
            with self.handler.prototype_queue._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],1)
            self.assertFalse(results[0]['capacityReady']); self.assertFalse(results[0]['dispatchReady'])
            self.assertEqual(results[0]['engineEvidence'],{'state':'unavailable'})

    def test_fresh_offline_conflict_limits_and_restart(self):
        identifier=str(uuid.uuid4()); plan=self.plan_for(identifier); body=ref.canonical_json(plan)
        with self.assertRaisesRegex(Conflict,'not qualified'): self.construct()
        with self.handler.prototype_queue._db() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='native_plan_bindings'").fetchone())
        with patch.object(config,'NATIVE_PLAN_BINDING_QUALIFIED',True):
            binding=self.construct()
            with patch.object(self.handler.viewer_service,'_catalog',side_effect=RuntimeError('offline')):
                with self.assertRaisesRegex(RuntimeError,'offline'): binding.bind(identifier,'production',CAP,body)
            changed={**plan,'outputs':[]}; barrier=threading.Barrier(2); outcomes=[]
            def attempt(value):
                try:
                    barrier.wait(3); outcomes.append(('ok',binding.bind(identifier,'production',CAP,ref.canonical_json(value))))
                except Conflict: outcomes.append(('hold',None))
            with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):
                threads=[threading.Thread(target=attempt,args=(value,)) for value in (plan,changed)]
                for thread in threads: thread.start()
                for thread in threads: thread.join(5); self.assertFalse(thread.is_alive())
                self.assertEqual(sorted(x[0] for x in outcomes),['hold','ok'])
                expected=next(x[1] for x in outcomes if x[0]=='ok')
            self.stop();self.start();binding=self.construct()
            with patch.object(self.handler.viewer_service,'_catalog',side_effect=RuntimeError('offline')):
                self.assertEqual(binding.status(identifier,'production',CAP),expected)
            second=str(uuid.uuid4()); second_plan=self.plan_for(second)
            with patch('processing.native_plan_binding.MAX_RECORDS',1), patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):
                with self.assertRaisesRegex(Conflict,'history limit'): binding.bind(second,'production',CAP,ref.canonical_json(second_plan))
            self.assertEqual(binding.status(identifier,'production',CAP),expected)
            with self.handler.prototype_queue._db() as db:
                db.execute("UPDATE native_plan_bindings SET catalog_sha=? WHERE uuid=?",('0'*64,identifier))
            with self.assertRaises(Conflict): binding.status(identifier,'production',CAP)

    def test_unknown_option_profile_and_stale_manifest_hold(self):
        identifier=str(uuid.uuid4()); plan=self.plan_for(identifier)
        with patch.object(config,'NATIVE_PLAN_BINDING_QUALIFIED',True):
            binding=self.construct()
            with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):
                unsupported={**plan,'options':[{'name':'crop','value':{'type':'number','bits':'3ff0000000000000'}}]}
                with self.assertRaisesRegex(Conflict,'not qualified'): binding.bind(identifier,'production',CAP,ref.canonical_json(unsupported))
                with self.assertRaisesRegex(Conflict,'manifest mismatch'): binding.bind(identifier,'production',CAP,ref.canonical_json({**plan,'manifestSha256':'0'*64}))
            with self.handler.prototype_queue._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],0)

    def test_process_exit_after_commit_keeps_binding(self):
        identifier=str(uuid.uuid4()); plan=self.plan_for(identifier); body=ref.canonical_json(plan)
        self.stop()
        code = r'''
import os,sys
from processing import config
from processing.config import Config
from processing.app import make_handler
from processing.native_manifest_binding import NativeManifestBindings
from processing.native_plan_binding import NativePlanBindings
from tests.test_native_manifest_binding import TOKEN,STAGING,CAP
config.NATIVE_MANIFEST_BINDING_QUALIFIED=True; config.NATIVE_PLAN_BINDING_QUALIFIED=True
c=Config('a'*32,'http://127.0.0.1:1','http://127.0.0.1:2',1000,20*1024**3,
 prototype_state_dir=sys.argv[1],prototype_node_ids=('cluster',),treedar_node_ids=('cluster',),
 viewer_enabled=True,viewer_token=TOKEN,viewer_staging_token=STAGING,authority_lease_dir=sys.argv[2],viewer_manifest_binding=True)
h=make_handler(c); h.viewer_service._catalog=lambda:{'ready':True}
b=NativePlanBindings(h.viewer_service,NativeManifestBindings(h.viewer_service.store))
b.bind(sys.argv[3],'production',CAP,bytes.fromhex(sys.argv[4]))
os._exit(0)
'''
        child=subprocess.run([os.sys.executable,'-c',code,str(self.state),str(self.leases),identifier,body.hex()],
                             capture_output=True,text=True,timeout=15,check=False)
        self.assertEqual(child.returncode,0,child.stderr[-2000:])
        self.start()
        with patch.object(config,'NATIVE_PLAN_BINDING_QUALIFIED',True):
            binding=self.construct()
            with patch.object(self.handler.viewer_service,'_catalog',side_effect=RuntimeError('offline')):
                receipt=binding.status(identifier,'production',CAP)
                self.assertEqual(binding.bind(identifier,'production',CAP,body),receipt)
            with self.handler.prototype_queue._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],1)

