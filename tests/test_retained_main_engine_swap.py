import copy
from contextlib import redirect_stdout
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

from ops import retained_main_engine_swap as swap
from ops.retained_main_engine_swap import (
    BASE, TARGET, FLAG, Held, Runtime, apply, candidate_config, canonical,
    command_with_retention, digest, no_main_claims, verify_owned_receipt,
)
from processing.maintenance import Conflict
from processing.prototype import Queue, VIEWER_PROFILE
from processing.viewer_tasks import TaskStore

class FakeRuntime:
    def __init__(self, fail=None):
        self.fail = fail
        self.calls = []
        self.closed = False
        self.current = dict(id='main', state='active', revision=0, planId=None,
                            targetImage=None, result=None)
        self.prepared = dict(requestId=str(uuid.uuid4()), authorityId=str(uuid.uuid4()),
                             node=copy.deepcopy(self.current), command=None,
                             engine={'tasks': {str(uuid.uuid4()): 40}})
        self.operator = SimpleNamespace(json=self.operator_json)

    def step(self, name):
        self.calls.append(name)
        if self.fail == name:
            raise Held('synthetic uncertain operation')

    def authority(self):
        return self.prepared['authorityId']

    def node(self):
        return copy.deepcopy(self.current)

    def operator_json(self, method, route, payload):
        assert '/nodes/main/' in route
        action = route.rsplit('/', 1)[-1]
        self.step(action)
        if action == 'schedule-owned':
            self.current.update(state='draining', revision=1, planId=str(uuid.uuid4()), targetImage=TARGET)
            receipt = dict(payload, nodeId='main', revision=1, state='draining', planId=self.current['planId'],
                           requestSha256=digest(canonical({'nodeId': 'main', 'request': payload})))
            receipt['receiptSha256'] = digest(canonical(receipt))
            return {'receipt': receipt}
        if action == 'begin':
            self.current.update(state='updating', revision=2)
            return {'node': self.node(), 'updateToken': 'synthetic-token'}
        if action == 'finish':
            self.current.update(state='active', revision=3, result=payload['result'])
            return {'node': self.node()}
        raise AssertionError('unexpected API mutation')

    def revalidate(self, _):
        self.step('revalidate')

    def engine_idle(self, *_):
        self.step('engine_idle')

    def claims(self):
        return [{'nodeId': 'cluster', 'status': 'running'}]

    def change_ingress(self, _, *, closed):
        self.step('close' if closed else 'open')
        self.closed = closed

    def stop_and_backup(self, *_):
        self.step('stop_and_backup')

    def recreate_and_verify(self, *_):
        self.step('recreate_and_verify')

    def persist_pin(self, _):
        self.step('persist_pin')

    def hold_closed(self, *_):
        self.calls.append('hold_closed')
        self.closed = True
        if self.current['state'] == 'updating':
            self.current['state'] = 'held'


class MainRetainedSwapTests(unittest.TestCase):
    def config(self):
        return {'name':swap.PROJECT,'services':{'ingress':{'image':'existing-ingress'},'nodeodx':{
            'image':BASE,'command':None,'entrypoint':None,'user':'0:0','restart':'unless-stopped',
            'pull_policy':'never','ports':[{'target':3000,'published':'13004','host_ip':'127.0.0.1'}],
            'volumes':[{'type':'bind','source':str(swap.DATA),'target':'/var/www/data'}],
            'deploy':{'resources':{'reservations':{'devices':[{'driver':'nvidia','count':'all','capabilities':['gpu']}]}}}}},
            'networks':{'default':{'name':'ledgetop-main-processing_default'}}}

    def test_only_actual_null_command_adds_bare_flag_without_photo_or_resource_limit(self):
        self.assertEqual(command_with_retention(None),[FLAG])
        for value in ([],[FLAG],['--max_images','1000'],''):
            with self.assertRaises(Held):command_with_retention(value)
        source=self.config();before=copy.deepcopy(source);out=candidate_config(source,None)
        self.assertEqual(source,before)
        expected=copy.deepcopy(source['services']['nodeodx']);expected.update(image=TARGET,command=[FLAG],pull_policy='never')
        self.assertEqual(out['services'],{'nodeodx':expected})
        self.assertEqual(out['networks'],source['networks'])
        self.assertNotIn('mem_limit',out['services']['nodeodx']);self.assertNotIn('cpus',out['services']['nodeodx'])

    def test_compose_shape_hooks_scaling_env_and_nonnull_command_are_rejected(self):
        for field,value in [('build',{}),('post_start',[{}]),('pre_stop',[{}]),('env_file',['other.env']),('environment',{'TOKEN':'other'}),('scale',2),('command',[])]:
            c=self.config();c['services']['nodeodx'][field]=value
            if field=='build':c['services']['nodeodx'][field]={'context':'.'}
            with self.assertRaises(Held,msg=field):candidate_config(c,None)
        c=self.config();c['services']['unexpected']={}
        with self.assertRaises(Held):candidate_config(c,None)
        c=self.config();c['volumes']={}
        with self.assertRaises(Held):candidate_config(c,None)

    def test_only_main_queued_running_or_held_claims_block(self):
        no_main_claims([{'nodeId':'cluster','status':'running'}])
        no_main_claims([{'nodeId':'main','status':'succeeded'}])
        for status in ['queued','running','held']:
            with self.assertRaises(Held):no_main_claims([{'nodeId':'main','status':status}])

    def test_owned_receipt_is_cryptographically_bound_to_main_not_cluster(self):
        r=FakeRuntime();payload=dict(schemaVersion=1,requestId=r.prepared['requestId'],authorityId=r.authority(),expectedRevision=0,drainAt='now',notBefore='now',afterJobId=None,targetImage=TARGET)
        receipt=r.operator_json('POST','/v1/prototype/nodes/main/schedule-owned',payload)['receipt']
        verify_owned_receipt(receipt,payload)
        receipt['nodeId']='cluster';receipt['requestSha256']=digest(canonical({'nodeId':'cluster','request':payload}));receipt['receiptSha256']=digest(canonical({k:v for k,v in receipt.items() if k!='receiptSha256'}))
        with self.assertRaises(Held):verify_owned_receipt(receipt,payload)

    def test_explicit_new_empty_environment_and_fixture_label_are_exact_not_generic_exceptions(self):
        before={'com.docker.compose.project':'main'};env=Path('/private/operation/empty.env')
        after={**before,swap.ENVIRONMENT_LABEL:str(env),swap.FIXTURE_LABEL:swap.FIXTURE_VALUE,'com.ledgetop.retention.commit':swap.PATCH_COMMIT}
        self.assertTrue(swap.recreation_labels_equivalent(before,after,'old',env))
        for field,value in [(swap.ENVIRONMENT_LABEL,'/other/empty.env'),(swap.FIXTURE_LABEL,'other'),('unexpected','extra')]:
            changed={**after,field:value};self.assertFalse(swap.recreation_labels_equivalent(before,changed,'old',env))
        self.assertFalse(swap.recreation_labels_equivalent({**before,swap.ENVIRONMENT_LABEL:'old.env'},after,'old',env))
        swap.verify_no_old_environment(None)
        with self.assertRaises(Held):swap.verify_no_old_environment('/invented/old.env')

    def test_export_budget_uses_fixed_32gib_not_docker_size_and_preserves_reserve(self):
        minimum=swap.BASE_EXPORT_BUDGET+123+456+swap.RESERVE
        self.assertEqual(swap.require_backup_headroom(minimum+1,123,456),minimum)
        with self.assertRaises(Held):swap.require_backup_headroom(minimum,123,456)
        for value in [-1,True,'1']:
            with self.assertRaises(Held):swap.require_backup_headroom(2**50,value,456)

    def test_busy_engine_or_claim_holds_before_schedule_or_journal(self):
        for mode in ['engine','claim']:
            r=FakeRuntime(fail='engine_idle' if mode=='engine' else None);states=[]
            if mode=='claim':r.claims=lambda:[{'nodeId':'main','status':'running'}]
            with tempfile.TemporaryDirectory() as root:
                with self.assertRaises(Held):apply(r,Path(root),r.prepared,lambda s:states.append(copy.deepcopy(s)))
            self.assertEqual(states,[]);self.assertNotIn('schedule-owned',r.calls);self.assertNotIn('close',r.calls)

    def test_success_keeps_owned_order_and_no_rollback(self):
        r=FakeRuntime();states=[]
        with tempfile.TemporaryDirectory() as root:
            result=apply(r,Path(root),r.prepared,lambda s:states.append(copy.deepcopy(s)))
        self.assertEqual(result['image'],TARGET);self.assertEqual(states[-1]['stage'],'completed')
        self.assertLess(r.calls.index('stop_and_backup'),r.calls.index('recreate_and_verify'))
        self.assertLess(r.calls.index('persist_pin'),r.calls.index('open'));self.assertLess(r.calls.index('open'),r.calls.index('finish'))
        self.assertNotIn('hold_closed',r.calls);self.assertFalse(r.current['result']['automaticRollback'])

    def test_every_uncertain_mutation_holds_without_replay_or_unpatched_boot(self):
        for fail in ['schedule-owned','revalidate','begin','close','stop_and_backup','recreate_and_verify','persist_pin','open','finish']:
            r=FakeRuntime(fail=fail);states=[]
            with tempfile.TemporaryDirectory() as root:
                with self.assertRaises(Held):apply(r,Path(root),r.prepared,lambda s:states.append(copy.deepcopy(s)))
            self.assertEqual(states[-1]['stage'],'held_for_inspection',fail)
            self.assertIn('hold_closed',r.calls,fail);self.assertLessEqual(r.calls.count('recreate_and_verify'),1)
            self.assertFalse(any(c in ('rollback','replace_old','start_old') for c in r.calls))

    def test_existing_operation_and_stale_authority_refuse_all_scheduling(self):
        for mode in ['existing','authority']:
            r=FakeRuntime();states=[]
            with tempfile.TemporaryDirectory() as root:
                if mode=='existing':(Path(root)/'operation.json').write_text('{}')
                else:r.authority=lambda:'changed'
                with self.assertRaises(Held):apply(r,Path(root),r.prepared,lambda s:states.append(s))
            self.assertEqual(states,[]);self.assertNotIn('schedule-owned',r.calls)

    def test_unowned_failure_cannot_close_another_plan(self):
        r=Runtime.__new__(Runtime);r.operator=SimpleNamespace(json=Mock(side_effect=Held('not owned')));r.change_ingress=Mock()
        result=r.hold_closed({'requestId':str(uuid.uuid4())},{})
        self.assertEqual(result['reason'],'own_current_plan_not_proven_no_ingress_mutation');r.change_ingress.assert_not_called()

    def failure_runtime(self, *, state='updating', claims=None):
        r=Runtime.__new__(Runtime);request,authority,plan=(str(uuid.uuid4()) for _ in range(3))
        current=dict(state=state,planId=plan,targetImage=TARGET,revision=2 if state=='updating' else 1)
        owned=dict(requestId=request,authorityId=authority,planId=plan,targetImage=TARGET,revision=1)
        def api(method,route,payload=None):
            if method=='GET':return {'receipt':owned}
            self.assertEqual(route,'/v1/prototype/nodes/main/hold')
            self.assertEqual(payload['expectedRevision'],2)
            current.update(state='held',revision=3)
            return {'node':dict(current)}
        r.operator=SimpleNamespace(json=Mock(side_effect=api));r.authority=Mock(return_value=authority)
        r.node=Mock(side_effect=lambda:dict(current));r.claims=Mock(return_value=claims or [])
        r.change_ingress=Mock()
        return r,dict(requestId=request,authorityId=authority),current

    def test_owned_draining_busy_failure_keeps_ingress_open(self):
        r,p,current=self.failure_runtime(state='draining',claims=[{'nodeId':'main','status':'running'}])
        result=r.hold_closed(p,{})
        self.assertEqual(result['reason'],'updating_idle_reservation_not_proven_no_ingress_mutation')
        r.change_ingress.assert_not_called()
        self.assertFalse(any(call.args[0]=='POST' for call in r.operator.json.call_args_list))
        self.assertEqual(current['state'],'draining')

    def test_owned_updating_idle_failure_closes_and_holds_exact_reservation(self):
        r,p,current=self.failure_runtime()
        result=r.hold_closed(p,{})
        r.change_ingress.assert_called_once_with(p,closed=True,recover_closed_intent=True)
        self.assertTrue(result['ingressClosedVerified']);self.assertTrue(result['nodeHeldVerified'])
        self.assertEqual(current['state'],'held')

    def test_claim_appearing_before_final_failure_recheck_blocks_ingress_change(self):
        r,p,current=self.failure_runtime()
        r.claims.side_effect=[[],[{'nodeId':'main','status':'running'}]]
        result=r.hold_closed(p,{})
        r.change_ingress.assert_not_called()
        self.assertFalse(result['ingressClosedVerified'])
        self.assertEqual(current['state'],'updating')

    def test_schedule_precheck_claim_race_uses_real_failure_handler_without_closing(self):
        class ClaimRace(FakeRuntime):
            def __init__(inner):
                super().__init__();inner.busy=False;inner.receipt=None
            def claims(inner):
                return [{'nodeId':'main','status':'running'}] if inner.busy else []
            def operator_json(inner,method,route,payload=None):
                if method=='GET' and '/schedule-owned/' in route:
                    return {'receipt':inner.receipt}
                result=super().operator_json(method,route,payload)
                if route.endswith('/schedule-owned'):
                    inner.receipt=result['receipt'];inner.busy=True
                return result
            def change_ingress(inner,prepared,*,closed,recover_closed_intent=False):
                return super().change_ingress(prepared,closed=closed)
            def hold_closed(inner,prepared,state):
                return Runtime.hold_closed(inner,prepared,state)
        r=ClaimRace();states=[]
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(Held):apply(r,Path(root),r.prepared,lambda state:states.append(dict(state)))
        self.assertTrue(r.busy);self.assertEqual(r.current['state'],'draining')
        self.assertFalse(r.closed);self.assertNotIn('close',r.calls)
        self.assertNotIn('stop_and_backup',r.calls);self.assertNotIn('recreate_and_verify',r.calls)

    def snapshot_container(self):
        labels={'com.docker.compose.project':swap.PROJECT,'com.docker.compose.service':swap.SERVICE,
                'com.docker.compose.project.working_dir':str(swap.COMPOSE_ROOT),
                'com.docker.compose.project.config_files':str(swap.COMPOSE)+','+str(swap.OVERRIDE)}
        container={'Id':swap.ENGINE,'Image':BASE,'State':{'Running':True},
            'Config':{'Entrypoint':['/usr/bin/node','/var/www/index.js'],'User':'0:0','Cmd':None,'Labels':labels},
            'HostConfig':{'RestartPolicy':{'Name':'unless-stopped'},'Memory':0,'NanoCpus':0,'ReadonlyRootfs':False,
                'DeviceRequests':[{'Driver':'nvidia','Count':-1,'DeviceIDs':None,'Capabilities':[['gpu']],'Options':None}]},
            'NetworkSettings':{'Ports':{'3000/tcp':[{'HostIp':'127.0.0.1','HostPort':'13004'}]},'Networks':{'ledgetop-main-processing_default':{}}},
            'Mounts':[{'Type':'bind','Destination':'/var/www/data','RW':True,'Source':str(swap.DATA)}]}
        return container

    def test_busy_snapshot_does_not_walk_data_or_inspect_target_image(self):
        container=self.snapshot_container()
        r=Runtime.__new__(Runtime);r.physical_host=Mock();r.inspect=Mock(return_value=container);r.ingress_inspect=Mock();r.claims=Mock(return_value=[{'nodeId':'main','status':'running'}]);r.docker=Mock();r.engine_idle=Mock()
        with tempfile.TemporaryDirectory() as root:
            nginx=Path(root)/'nginx.conf';nginx.write_bytes(b'listen 3004; '+swap.PASS)
            with patch.object(swap,'source_record',return_value={}),patch.object(swap,'regular'),patch.object(swap,'NGINX',nginx),patch.object(swap,'root_identity',side_effect=AssertionError('busy data walk')),patch.object(swap,'snapshot_tree',side_effect=AssertionError('busy hash')):
                with self.assertRaisesRegex(Held,'main owns unfinished claim'):r.snapshot()
        r.docker.assert_not_called();r.engine_idle.assert_not_called()

    def test_empty_gpu_options_object_is_not_the_actual_fixed_main_profile(self):
        container=self.snapshot_container();container['HostConfig']['DeviceRequests'][0]['Options']={}
        r=Runtime.__new__(Runtime);r.physical_host=Mock();r.inspect=Mock(return_value=container);r.claims=Mock();r.docker=Mock()
        with tempfile.TemporaryDirectory() as root:
            nginx=Path(root)/'nginx.conf';nginx.write_bytes(b'listen 3004; '+swap.PASS)
            with patch.object(swap,'source_record',return_value={}),patch.object(swap,'regular'),patch.object(swap,'NGINX',nginx):
                with self.assertRaisesRegex(Held,'main resource/GPU profile changed'):r.snapshot()
        r.claims.assert_not_called();r.docker.assert_not_called()

    def test_task_index_reserialization_preserves_semantics_separately_but_not_file_metadata(self):
        before={'tasks.json':{'type':'file','mode':0o600,'uid':0,'gid':0,'bytes':1,'sha256':'a'}}
        after=copy.deepcopy(before);after['tasks.json'].update(bytes=2,sha256='b')
        swap.verify_main_retained(before,after)
        after['tasks.json']['mode']=0o644
        with self.assertRaises(Held):swap.verify_main_retained(before,after)

    def test_engine_active_queue_refuses_before_task_or_filesystem_inspection(self):
        r=Runtime.__new__(Runtime);client=Mock();client.json.return_value={'engine':'odx','maxImages':None,'taskQueueCount':1};r.engine_client=Mock(return_value=client);r.docker=Mock()
        with self.assertRaises(Held):r.engine_idle(swap.ENGINE,None)
        self.assertEqual(client.json.call_count,1);r.docker.assert_not_called()

    def test_processing_child_refuses_even_when_terminal_api_claims_idle(self):
        r=Runtime.__new__(Runtime);r.engine_state=Mock(return_value={'tasks':{}});r.docker=Mock(return_value='PID ARGS\n1 node /var/www/index.js\n2 python running-heavy-task\n')
        with self.assertRaises(Held):r.engine_idle(swap.ENGINE,None)
        self.assertEqual(r.docker.call_count,1)

    def test_default_cli_is_read_only_and_redacts_private_preparation(self):
        r=Mock();r.snapshot.return_value={'engine':{'tasks':{}},'private':'do-not-print'}
        with patch('sys.argv',['swap','--credentials','private.env']),patch.object(swap,'Runtime',return_value=r),redirect_stdout(io.StringIO()) as out:
            self.assertEqual(swap.main(),0)
        self.assertEqual(json.loads(out.getvalue())['mode'],'inspect');self.assertNotIn('do-not-print',out.getvalue());self.assertEqual(r.method_calls,[('snapshot',(),{})])

    @unittest.skipUnless(os.name=='posix','POSIX numeric ownership and directory fsync')
    def test_atomic_forward_pin_write_preserves_metadata_under_private_umask(self):
        with tempfile.TemporaryDirectory() as root:
            p=Path(root)/'compose.json';p.write_text('{}');p.chmod(0o600);before=p.stat();old=os.umask(0o077)
            try:swap.atomic_preserved_json(p,{'image':TARGET,'command':[FLAG]})
            finally:os.umask(old)
            after=p.stat();self.assertEqual((after.st_uid,after.st_gid,stat.S_IMODE(after.st_mode)),(before.st_uid,before.st_gid,0o600));self.assertEqual(json.loads(p.read_text())['command'],[FLAG])

    @unittest.skipUnless(os.name=='posix' and os.geteuid()==0,'real installed UID1000 forward-pin metadata')
    def test_both_forward_pins_change_only_image_command_with_original_metadata(self):
        with tempfile.TemporaryDirectory() as root:
            rollback=Path(root)/'rollback.json';override=Path(root)/'override.json';role=Path(root)/'role.json'
            values=[{'services':{'nodeodx':{'image':'old-rollback','command':None}}},
                    {'services':{'nodeodx':{'image':'old-current'}}},
                    {'services':{'nodeodx':{'image':'old-bootstrap','command':None},'ingress':{'image':'unchanged'}}}]
            for p,value in zip([rollback,override,role],values):p.write_text(json.dumps(value));p.chmod(0o600);os.chown(p,1000,1000)
            original=rollback.read_bytes();prepared={'sourceFiles':{str(p):swap.source_record(p) for p in [rollback,override,role]}}
            r=Runtime.__new__(Runtime)
            with patch.object(swap,'COMPOSE',rollback),patch.object(swap,'OVERRIDE',override),patch.object(swap,'ROLE_COMPOSE',role):r.persist_pin(prepared)
            self.assertEqual(rollback.read_bytes(),original)
            for p,value in [(override,values[1]),(role,values[2])]:
                expected=copy.deepcopy(value);expected['services']['nodeodx'].update(image=TARGET,command=[FLAG]);self.assertEqual(json.loads(p.read_text()),expected)
                self.assertEqual((p.stat().st_uid,p.stat().st_gid,stat.S_IMODE(p.stat().st_mode)),(1000,1000,0o600))
            self.assertEqual(sorted(p.name for p in Path(root).iterdir()),['override.json','role.json','rollback.json'])

    @unittest.skipUnless(os.name=='posix' and os.geteuid()==0,'root-private main operation env qualification')
    def test_new_environment_must_be_root_private_empty_regular_and_unaliased(self):
        with tempfile.TemporaryDirectory() as root:
            p=swap.prepare_empty_environment(Path(root));self.assertEqual(swap.verified_empty_environment(Path(root)),p)
            p.write_text('UNEXPECTED=value')
            with self.assertRaises(Held):swap.verified_empty_environment(Path(root))


class RealMainQueueFenceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.now=datetime(2026,11,11,12,tzinfo=timezone.utc)
        self.queue=Queue(Path(self.temp.name)/'queue',node_ids=('cluster','main'),viewer_enabled=True,clock=lambda:self.now)
    def tearDown(self):self.queue.close();self.temp.cleanup()
    def schedule(self):
        node=self.queue.get_node('main');stamp=self.now.isoformat().replace('+00:00','Z')
        request=dict(schemaVersion=1,requestId=str(uuid.uuid4()),authorityId=self.queue.authority_id,expectedRevision=node['revision'],drainAt=stamp,notBefore=stamp,afterJobId=None,targetImage=TARGET)
        receipt=self.queue.schedule_node_owned('main',request);verify_owned_receipt(receipt,request);return receipt
    def claim(self,node):return self.queue.claim({'workerId':node+'-worker','nodeId':node,'profiles':[VIEWER_PROFILE]},eligible_node_ids=[node])
    def begin(self):
        n=self.queue.get_node('main');return self.queue.begin_node_update('main',{'expectedRevision':n['revision'],'planId':n['planId']})
    def test_real_main_drain_and_updating_fence_preserve_waiting_work_and_cluster(self):
        job=self.queue.submit({'requestId':'waiting','profile':VIEWER_PROFILE,'environment':'production'})[0]
        self.schedule();self.assertIsNone(self.claim('main'));self.assertEqual(self.begin()['node']['state'],'updating');self.assertIsNone(self.claim('main'))
        self.assertEqual(self.queue.get(job['id'])['status'],'queued');self.assertEqual(self.claim('cluster')['id'],job['id'])
    def test_real_main_claim_blocks_update_before_viewer_engine_assignment(self):
        store=TaskStore(self.queue,Path(self.temp.name)/'inputs');identifier=str(uuid.uuid4());store.initialize(identifier,'Synthetic',[],None,'production');body=b'synthetic';store.upload(identifier,'photo.jpg',io.BytesIO(body),len(body));store.commit(identifier)
        claim=self.claim('main');self.assertIsNotNone(claim);self.assertEqual(claim['nodeId'],'main');self.assertIsNone(store.task(identifier)['node_id']);self.assertIsNone(store.task(identifier)['engine_uuid']);self.schedule()
        with self.assertRaises(Conflict):self.begin()
        self.queue.complete(claim['id'],{'claimToken':claim['claimToken'],'status':'succeeded','result':{}})
        self.assertEqual(self.begin()['node']['state'],'updating')


if __name__=='__main__':unittest.main()
