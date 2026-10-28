"""Real coordinator HTTP/SQLite with synthetic Docker and ingress transports."""
import json
import os
import unittest
from unittest.mock import patch

from ops import node_maintenance as maintenance
from ops.node_ingress import GateError,save
from processing.container_update import UpdateError,Uncertain
from processing.node_gate import NodeGate
from processing.odx_smoke_worker import SmokeError
from tests import test_container_update as update_fixture


@unittest.skipIf(os.name=='nt','managed host locking requires Linux')
class ManagedFinalizationTests(unittest.TestCase):
    def setUp(self):
        self.fixture=update_fixture.MaintenanceUpdateTests(methodName='runTest')
        self.fixture.setUp();self.addCleanup(self.fixture.tearDown)
        self.root,self.receipt=self.fixture.root,self.fixture.work
        self.receipt.chmod(0o700)
        self.api,self.operator,self.adapter=self.fixture.api,self.fixture.operator,self.fixture.adapter
        self.fixture.plan()
        gate_patch=patch('processing.node_gate.canonical_gate_path',return_value=self.root/'host-gate')
        gate_patch.start();self.addCleanup(gate_patch.stop)
        with NodeGate('synthetic-engine') as gate:gate.provision('cluster',self.api.origin,self.fixture.queue.authority_id)
        save(self.root/'maintenance.json',dict(nodeId='cluster',project='owned',service='node',expectedEngine='odm',
            uploadLayout='var-www-tmp-v1',processLayout='node-var-www-v1'))
        save(self.root/'compose.json',{'services':{'node':{'image':update_fixture.OLD,'mem_limit':'4g'}}})
        self.ingress_root=self.root/'ingress';self.ingress_root.mkdir(mode=0o700)
        owner=self
        class Ingress:
            root=owner.ingress_root
            config=dict(backendPort=25001,backendName='owned-backend',engineId='synthetic-engine',backendEngine='odm',
                backendToken='synthetic-private-backend-token',proxyName='owned-proxy',proxyImage='sha256:'+'c'*64,listenPort=25002)
            opens=0
            fail_open=False
            unknown_open=False
            def docker(self,args):return json.dumps([owner.adapter.inspect()])
            def inspect(self):return {'Id':'owned-proxy'}
            def workers(self,item):return {2}
            def backend(self,expected):
                if not owner.adapter.serving(expected):raise GateError('synthetic unhealthy backend')
                return dict(engine='odm',engineVersion='pinned',version='2.4.0')
            def http(self,port):
                state=json.loads((self.root/'state.json').read_text())
                return (200,self.backend(update_fixture.NEW)) if state['state']=='open' else (503,None)
            def change(self,closed,expected=None,seconds=60):
                if not closed:
                    owner.assertEqual(owner.fixture.queue.get_node('cluster')['state'],'updating')
                    owner.assertEqual(json.loads((owner.root/'compose.json').read_text())['services']['node']['image'],update_fixture.NEW)
                    self.opens+=1
                state=dict(state='closed' if closed else 'opening' if self.unknown_open else 'open',
                    containerId='owned-proxy',oldWorkers=[2] if not closed and self.unknown_open else [],
                    expectedBackendImage=expected)
                save(self.root/'state.json',state)
                if not closed and (self.fail_open or self.unknown_open):raise GateError('synthetic lost ingress response')
                return dict(state=state['state'],oldWorkersDrained=True,containerId='owned-proxy')
        self.ingress=Ingress()
        inspect=self.adapter.inspect
        self.adapter.inspect=lambda:dict(inspect(),Id='owned-backend')
        self.adapter.serving=lambda image:self.adapter.current==image and not (self.adapter.bad_health and image==update_fixture.NEW)
        def compose(args,receipt):
            self.assertEqual(args.node_origin,'http://127.0.0.1:25001')
            self.assertEqual(args.expected_engine,'odm')
            self.assertEqual(args.upload_layout,'var-www-tmp-v1')
            self.assertEqual(args.process_layout,'node-var-www-v1')
            return self.adapter
        ingress=patch.object(maintenance,'Ingress',return_value=self.ingress);ingress.start();self.addCleanup(ingress.stop)
        adapter=patch.object(maintenance,'ComposeNode',side_effect=compose);adapter.start();self.addCleanup(adapter.stop)

    def run_update(self):return maintenance.run(self.root,self.api,self.operator,self.receipt,0)

    def test_capacity_stays_closed_through_pin_health_and_open_then_exact_finish(self):
        result=self.run_update()
        self.assertEqual(result['status'],'succeeded')
        self.assertFalse(result['composePinUpdateRequired'])
        self.assertEqual(self.fixture.queue.get_node('cluster')['state'],'active')
        self.assertEqual(self.adapter.calls,[update_fixture.NEW])
        self.assertEqual(json.loads((self.root/'compose.json').read_text())['services']['node']['mem_limit'],'4g')
        private=json.loads((self.receipt/'update.json').read_text())
        self.assertNotIn(private['updateToken'],json.dumps(result))
        self.assertNotIn(self.ingress.config['backendToken'],json.dumps(result))
        self.assertEqual(self.run_update(),result)
        self.assertEqual(self.ingress.opens,1)

    def test_lost_checkpoint_after_durable_pin_only_resumes_finalization(self):
        original=maintenance.save
        def lost(path,value,*args,**kwargs):
            original(path,value,*args,**kwargs)
            if path==self.root/'compose.json':raise OSError('synthetic lost pin response')
        with patch.object(maintenance,'save',side_effect=lost):
            with self.assertRaises(OSError):self.run_update()
        self.assertEqual(self.fixture.queue.get_node('cluster')['state'],'updating')
        self.assertEqual(self.ingress.opens,0)
        self.assertEqual(self.run_update()['status'],'succeeded')
        self.assertEqual(self.adapter.calls,[update_fixture.NEW])

    def test_lost_open_response_reconciles_without_reloading_ingress_or_docker(self):
        self.ingress.fail_open=True
        with self.assertRaises(GateError):self.run_update()
        self.assertEqual(self.fixture.queue.get_node('cluster')['state'],'updating')
        self.ingress.fail_open=False
        self.assertEqual(self.run_update()['status'],'succeeded')
        self.assertEqual(self.ingress.opens,1)
        self.assertEqual(self.adapter.calls,[update_fixture.NEW])

    def test_unknown_open_remains_updating_and_never_replays_reload(self):
        self.ingress.unknown_open=True
        for _ in range(2):
            with self.assertRaises(GateError):self.run_update()
        self.assertEqual(self.fixture.queue.get_node('cluster')['state'],'updating')
        self.assertEqual(self.ingress.opens,1)
        self.assertEqual(self.adapter.calls,[update_fixture.NEW])

    def test_lost_finish_ack_is_verified_and_final_receipt_loss_allows_new_work(self):
        original=self.operator.json
        def lost(method,route,payload=None):
            value=original(method,route,payload)
            if route.endswith('/finish'):raise SmokeError('synthetic lost finish ACK')
            return value
        save_original=maintenance.save
        def checkpoint(path,value,*args,**kwargs):
            if path==self.receipt/'managed-update.json' and value.get('stage')=='completed':
                raise OSError('synthetic receipt write failed after finish')
            return save_original(path,value,*args,**kwargs)
        with patch.object(self.operator,'json',side_effect=lost),patch.object(maintenance,'save',side_effect=checkpoint):
            with self.assertRaises(OSError):self.run_update()
        self.assertEqual(self.fixture.queue.get_node('cluster')['state'],'active')
        self.fixture.queue.submit(dict(requestId='work-after-finish',environment='production',profile='treedar-synthetic-v1'))
        self.assertIsNotNone(self.fixture.queue.claim({'workerId':'new-work'}))
        with patch.object(self.adapter,'healthy',side_effect=AssertionError('finished receipt must not require idle again')):
            self.assertEqual(self.run_update()['status'],'succeeded')
        self.assertEqual(self.adapter.calls,[update_fixture.NEW])
        self.assertEqual(self.ingress.opens,1)

    def test_unknown_replacement_remains_inspection_only(self):
        self.adapter.uncertain=True
        with self.assertRaises(Uncertain):self.run_update()
        with self.assertRaises(UpdateError):self.run_update()
        self.assertEqual(self.fixture.queue.get_node('cluster')['state'],'updating')
        self.assertEqual(self.adapter.calls,[update_fixture.NEW])
        self.assertEqual(self.ingress.opens,0)

    def test_changed_authoritative_settings_are_not_overwritten_or_activated(self):
        original=maintenance.save
        def lost(path,value,*args,**kwargs):
            original(path,value,*args,**kwargs)
            if path==self.root/'compose.json':raise OSError('synthetic lost pin response')
        with patch.object(maintenance,'save',side_effect=lost):
            with self.assertRaises(OSError):self.run_update()
        source=json.loads((self.root/'compose.json').read_text())
        source['services']['node']['mem_limit']='8g';save(self.root/'compose.json',source)
        with self.assertRaises(GateError):self.run_update()
        self.assertEqual(self.fixture.queue.get_node('cluster')['state'],'updating')
        self.assertEqual(self.ingress.opens,0)
        self.assertEqual(self.adapter.calls,[update_fixture.NEW])
        self.assertEqual(json.loads((self.root/'compose.json').read_text())['services']['node']['mem_limit'],'8g')

    def test_pin_changed_during_open_is_rechecked_before_finish_and_never_rewritten(self):
        original=self.ingress.change
        def changed(closed,**kwargs):
            result=original(closed,**kwargs)
            if not closed:
                source=json.loads((self.root/'compose.json').read_text())
                source['services']['node']['image']=update_fixture.OLD
                save(self.root/'compose.json',source)
            return result
        with patch.object(self.ingress,'change',side_effect=changed):
            with self.assertRaises(GateError):self.run_update()
        with self.assertRaises(GateError):self.run_update()
        self.assertEqual(self.fixture.queue.get_node('cluster')['state'],'updating')
        self.assertEqual(self.adapter.calls,[update_fixture.NEW])
        self.assertEqual(self.ingress.opens,1)
        self.assertEqual(json.loads((self.root/'compose.json').read_text())['services']['node']['image'],update_fixture.OLD)

    def test_known_failed_candidate_retains_failed_gate_closed_ingress_and_original_pin(self):
        self.adapter.bad_health=True
        with patch('processing.container_update.wait_healthy',side_effect=lambda adapter,image,seconds:adapter.healthy(image)):
            result=self.run_update()
        self.assertEqual(result['status'],'failed')
        self.assertEqual(self.fixture.queue.get_node('cluster')['state'],'failed')
        self.assertEqual(self.adapter.calls,[update_fixture.NEW,update_fixture.OLD])
        self.assertEqual(self.ingress.opens,0)
        self.assertEqual(json.loads((self.root/'compose.json').read_text())['services']['node']['image'],update_fixture.OLD)
