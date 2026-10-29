"""Actual existing updater/HTTP/SQLite, with Docker+SSH fixture boundaries only."""
from contextlib import ExitStack,redirect_stdout,redirect_stderr,nullcontext
import copy
from dataclasses import asdict,replace
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch,Mock
import uuid
from types import SimpleNamespace

from ops import update_reference_coordinator as m
from ops.incremental_coordinator_binding import IncrementalBinding,IncrementalHeld,volume_census,digest
from processing.app import make_handler
from processing.config import Config
from processing.odx_smoke_worker import Client
from processing.reference_store import ReferenceStore
from processing.reference_terminal import TerminalService
from processing.reference_delivery import DeliveryReceiver
from processing.reference_failure import FailureReceiver
from processing.viewer_tasks import TaskStore
from tests import test_reference_coordinator_update as f
from tests.test_service import start_server,TOKEN
from tests import test_coordinator_update_controller as controller_fixture
from ops.coordinator_gate_bridge import LocalGate,BridgeGateClient,incremental_helper_digest
from processing.node_gate import NodeGate
from ops.native_node_fixture import NativeNodeFixture


TARGET='sha256:e96d957f3eff3f6a33fa673fa9f8ce211d82969c3724d16ba24721f6b376f42b'
PREDECESSOR_IMAGE='sha256:649f53f2b5dc32cd9eb229c42b6bbd7b479249fba5a8d65c48d8afc4f157b249'


@unittest.skipUnless(os.name=='posix','Linux owned updater fixture')
class IncrementalUpdateTests(unittest.TestCase):
    make_transport=f.ReferenceCoordinatorUpdateTests.make_transport

    def setUp(self):
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.tmp=self.stack.enter_context(tempfile.TemporaryDirectory());self.root=Path(self.tmp);self.root.chmod(0o700);self.volume=self.root/'volume'
        self.handler=make_handler(Config(TOKEN,'','',1000,20000,prototype_state_dir=str(self.volume),prototype_node_ids=('cluster','main'),prototype_operator_token=f.OPERATOR))
        self.queue=self.handler.prototype_queue;self.stack.callback(self.queue.close);self.queue.allowed_profiles.add('reference-pipeline-v1')
        store=ReferenceStore(self.queue);TerminalService(store);DeliveryReceiver(store);FailureReceiver(store);TaskStore(self.queue,self.volume/'viewer-tasks');(self.volume/'viewer-spool').mkdir(mode=0o700)
        with self.queue._db() as db:
            db.execute('UPDATE coordinator_identity SET authority_id=?',(m.AUTHORITY,))
            for index in range(12):db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,'staging','treedar-synthetic-v1',?)",(str(uuid.uuid4()),'synthetic-'+str(index),'failed' if index<2 else 'succeeded'))
        self.queue.authority_id=m.AUTHORITY
        for node in ('cluster','main'):
            value=self.queue.schedule_node_owned(node,dict(schemaVersion=1,requestId=str(uuid.uuid4()),authorityId=m.AUTHORITY,expectedRevision=0,targetImage=f.MAIN,drainAt='2026-01-01T00:00:00Z',notBefore='2026-01-01T00:00:00Z',afterJobId=None))
            self.queue.cancel_node(node,dict(expectedRevision=value['revision'],planId=value['planId']))
        base=volume_census(self.volume,authority=m.AUTHORITY,uid=os.geteuid())
        app=dict(PROCESSING_API_TOKEN=TOKEN,PROCESSING_PROTOTYPE_OPERATOR_TOKEN=f.OPERATOR,PROCESSING_REFERENCE_CONFIG=m.legacy.CONFIG_PATH)
        self.incremental_profile=IncrementalBinding('1'*64,PREDECESSOR_IMAGE,'671fd2a6bde40088bdfad91ccf67eab45ee3d8d7','6bbd468e1177746cb695e43195e75355b0681f23',m.AUTHORITY,m.digest(app),'a'*64,'5'*64,f.MAIN,'f'*64,'sha256:'+'f'*64,base['tables']['jobs']['sha256'],digest(base['ownedRows']),digest(base['schema']))
        self.prior_rows=base['ownedRows']
        self.server=start_server(self.handler);self.stack.callback(self.server.server_close);self.stack.callback(self.server.shutdown)
        origin='http://127.0.0.1:'+str(self.server.server_port);self.api,self.operator=Client(origin,TOKEN),Client(origin,f.OPERATOR)
        self.gates=f.Gates();self.gates.incremental_binding=self.incremental_profile;self.transport=self.make_transport()
        self.operation=self.root/'operation';self.operation.mkdir(mode=0o700)
        self.updater=m.ReferenceCoordinatorUpdate(self.operation,gate_factory=self.gates,transport=self.transport,cluster_probe=lambda _:None,incremental_binding=self.incremental_profile,helper_source_commit='d'*40)
        self.stack.enter_context(patch.object(m,'copy_backup',side_effect=self.backup))
        self.stack.enter_context(patch.object(m,'owned_api_probe',return_value={'operatorLookupVerified':True}))
        self.state=self.updater.prepare(TARGET,self.incremental_profile.candidate_source,f.MAIN)
        # Real release predicate, actual local HTTP/SQLite, synthetic Docker only.

    def backup(self,transport,root,state):
        target=root/'copied-volume';shutil.copytree(self.volume,target)
        before=volume_census(self.volume,authority=m.AUTHORITY,uid=os.geteuid());after=volume_census(target,authority=m.AUTHORITY,uid=os.geteuid());self.assertEqual(before,after)
        return dict(fullCensusSha256=m.digest(before),treeSha256=m.digest(before['files']),sqliteQuickCheck='ok')

    def assert_history(self):
        current=volume_census(self.volume,authority=m.AUTHORITY,uid=os.geteuid())
        self.assertEqual([row for row in current['ownedRows'] if row['request_id'] in {r['request_id'] for r in self.prior_rows}],self.prior_rows)
        self.assertEqual(len(current['ownedRows']),4)
        self.assertEqual({self.queue.get_node(node)['state'] for node in ('cluster','main')},{'active'})

    def test_actual_existing_prepare_activate_preserves_old_rows_and_distinct_helper_source(self):
        self.assertEqual(self.state['schemaVersion'],3);self.assertEqual(self.state['helperSourceCommit'],'d'*40)
        self.assertEqual(self.updater.activate(self.state)['phase'],'completed');self.assert_history()
        self.assertFalse(self.state['databaseRestored'])
        self.assertEqual(self.transport.inspected(m.NAME)['Id'],f.NEW_ID)
        self.assertEqual(sum(args[0]=='create' for args in self.transport.calls),1)

    def test_lost_owned_post_ack_looks_up_same_receipt_no_repost_or_new_nonce(self):
        original=self.operator.json;posts=[];lookups=[]
        def lost(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if method=='POST' and route.endswith('/schedule-owned'):
                posts.append(copy.deepcopy(payload))
                if len(posts)==1:raise OSError('synthetic lost owned schedule ack')
            if method=='GET' and '/schedule-owned/' in route:lookups.append(route)
            return value
        with patch.object(self.operator,'json',side_effect=lost):self.assertEqual(self.updater.activate(self.state)['phase'],'completed')
        self.assertEqual(len(posts),2);self.assertEqual(len(lookups),1);self.assertTrue(lookups[0].endswith(posts[0]['requestId']));self.assert_history()

    def test_lost_lookup_holds_then_explicit_receipt_reconcile_continues_no_post_replay(self):
        original=self.operator.json;posts=[]
        def lost(method,route,payload=None,**kwargs):
            if method=='GET' and '/schedule-owned/' in route:raise OSError('synthetic lookup outage')
            value=original(method,route,payload,**kwargs)
            if method=='POST' and route.endswith('/schedule-owned'):posts.append(payload);raise OSError('synthetic lost post ack')
            return value
        with patch.object(self.operator,'json',side_effect=lost):
            with self.assertRaises(OSError):self.updater.activate(self.state)
        self.assertEqual(self.transport.calls,[]);self.assertIn('pendingPlan',self.state)
        state=self.updater.load();self.assertEqual(self.updater.reconcile_schedule(state)['phase'],'prepared')
        self.state=state
        self.assertEqual(self.updater.activate(state)['phase'],'completed');self.assert_history();self.assertEqual(len(posts),1)

    def test_unknown_uncommitted_schedule_never_reposts_or_adopts_target_match(self):
        original=self.operator.json;posts=[]
        def unavailable(method,route,payload=None,**kwargs):
            if method=='POST' and route.endswith('/schedule-owned'):posts.append(payload);raise OSError('synthetic unknown before send')
            return original(method,route,payload,**kwargs)
        with patch.object(self.operator,'json',side_effect=unavailable):
            with self.assertRaises(Exception):self.updater.activate(self.state)
        with self.assertRaises(Exception):self.updater.reconcile_schedule(self.state)
        with self.assertRaises(m.Stopped):self.updater.activate(self.state)
        self.assertEqual(len(posts),1);self.assertEqual(self.transport.calls,[])

    def test_lost_create_then_real_existing_rollback_preserves_all_owned_history(self):
        self.transport.loss='create'
        with self.assertRaises(OSError):self.updater.activate(self.state)
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back');self.assert_history()
        self.assertTrue(self.transport.containers[self.incremental_profile.predecessor_container]['State']['Running'])
        self.assertEqual(sum(args[0]=='create' for args in self.transport.calls),1)

    def test_activation_release_false_refuses_before_any_schedule_or_docker_mutation(self):
        with patch.object(m,'INCREMENTAL_ACTIVATION_REVIEWED',False):
            with self.assertRaises(m.Stopped):self.updater.activate(self.state)
        self.assertEqual(self.transport.calls,[]);self.assertEqual(self.state['plans'],{})

    def test_known_pair_release_predicate_and_cli_arguments_are_exact(self):
        self.assertTrue(m.incremental_release_reviewed(self.incremental_profile,TARGET,self.incremental_profile.candidate_source))
        for field,value in (('predecessor_source','0'*40),('predecessor_image','sha256:'+'0'*64),('candidate_source','0'*40)):
            with self.subTest(field=field):
                profile=replace(self.incremental_profile,**{field:value})
                self.assertFalse(m.incremental_release_reviewed(profile,TARGET,profile.candidate_source))
        self.assertFalse(m.incremental_release_reviewed(self.incremental_profile,'sha256:'+'0'*64,self.incremental_profile.candidate_source))
        self.assertFalse(m.incremental_release_reviewed(self.incremental_profile,TARGET,'0'*40))
        with patch.object(m,'INCREMENTAL_ACTIVATION_REVIEWED',False):
            self.assertFalse(m.incremental_release_reviewed(self.incremental_profile,TARGET,self.incremental_profile.candidate_source))

    def test_actual_cli_uses_same_pair_predicate_before_loading_or_executing_update(self):
        config=self.root/'gate-config.json';config.write_text('{}')
        # Only the CLI account/lock and constructed executor are fixtures. The
        # real argument parser, profile constructor and release predicate run.
        for action in ('activate','rollback'):
            for field in ('known','predecessor_source','predecessor_image','candidate_source','candidate_image'):
                with self.subTest(action=action,field=field):
                    profile=self.incremental_profile
                    if field in ('predecessor_source','candidate_source'):profile=replace(profile,**{field:'0'*40})
                    if field=='predecessor_image':profile=replace(profile,predecessor_image='sha256:'+'0'*64)
                    image='sha256:'+'0'*64 if field=='candidate_image' else TARGET
                    executor=Mock();executor.load.return_value=self.state
                    executor.activate.return_value=self.state;executor.rollback.return_value=self.state
                    argv=[action,'--image',image,'--source-commit',profile.candidate_source,'--receipt-dir',str(self.operation),'--verified-main-image',f.MAIN,'--gate-config',str(config),'--incremental-profile',json.dumps(asdict(profile)),'--helper-source-commit','d'*40]
                    host=SimpleNamespace(operation_lock=nullcontext,private_file=lambda p:Path(p))
                    with patch.object(m,'legacy',host),patch.object(m.os,'geteuid',return_value=1000),patch('pwd.getpwuid',return_value=SimpleNamespace(pw_name='bkoltz')),patch.object(m.socket,'gethostname',return_value='cluster'),patch.object(m,'GateFactory',return_value=self.gates),patch.object(m,'ReferenceCoordinatorUpdate',return_value=executor),redirect_stderr(io.StringIO()):
                        result=m.main(argv)
                    self.assertEqual(result,0 if field=='known' else 2)
                    if field=='known':getattr(executor,action).assert_called_once_with(self.state)
                    else:
                        executor.load.assert_not_called();executor.activate.assert_not_called();executor.rollback.assert_not_called()
        self.assertEqual(self.transport.calls,[]);self.assertEqual(self.state['plans'],{})

    def test_each_unqualified_pair_refuses_direct_activation_and_rollback_before_effects(self):
        original=self.incremental_profile
        cases=(('predecessor_source','0'*40),('predecessor_image','sha256:'+'0'*64),
               ('candidate_source','0'*40),('candidate_image','sha256:'+'0'*64))
        for index,(field,value) in enumerate(cases):
            with self.subTest(field=field):
                self.incremental_profile=replace(original,**{field:value}) if field!='candidate_image' else original
                target=value if field=='candidate_image' else TARGET
                self.gates.incremental_binding=self.incremental_profile
                transport=self.make_transport();operation=self.root/('wrong-'+str(index));operation.mkdir(mode=0o700)
                updater=m.ReferenceCoordinatorUpdate(operation,gate_factory=self.gates,transport=transport,cluster_probe=lambda _:None,incremental_binding=self.incremental_profile,helper_source_commit='d'*40)
                if field in ('predecessor_source','candidate_source'):
                    # Exact source-pair baseline qualification now rejects these
                    # before a prepared checkpoint can confer any further state.
                    with self.assertRaisesRegex(IncrementalHeld,'installed_baseline_changed'):
                        updater.prepare(target,self.incremental_profile.candidate_source,f.MAIN)
                    self.assertEqual(transport.calls,[])
                    self.assertEqual(len(volume_census(self.volume,authority=m.AUTHORITY,uid=os.geteuid())['ownedRows']),2)
                    continue
                state=updater.prepare(target,self.incremental_profile.candidate_source,f.MAIN)
                # Both real entry points validate the real prepared checkpoint;
                # no validate/guard mocks substitute for the release boundary.
                with self.assertRaisesRegex(m.Stopped,'incremental_application_migration_review_required'):updater.activate(state)
                with self.assertRaisesRegex(m.Stopped,'incremental_application_migration_review_required'):updater.rollback(state)
                self.assertEqual(transport.calls,[]);self.assertEqual(state['plans'],{})
                self.assertEqual(len(volume_census(self.volume,authority=m.AUTHORITY,uid=os.geteuid())['ownedRows']),2)
        self.incremental_profile=original;self.gates.incremental_binding=original

    def test_foreign_volume_owner_after_full_pending_intent_blocks_before_owned_post(self):
        save=self.updater.save;request=self.operator.json;posts=[]
        def changed(state,phase=None,**fields):
            save(state,phase,**fields)
            if phase=='schedule_pending':self.transport.containers['0'*64]=self.transport.container('0'*64,TARGET,'foreign-volume-owner',True)
        def observed(method,route,payload=None,**kwargs):
            if method=='POST' and route.endswith('/schedule-owned'):posts.append(payload)
            return request(method,route,payload,**kwargs)
        with patch.object(self.updater,'save',side_effect=changed),patch.object(self.operator,'json',side_effect=observed):
            with self.assertRaisesRegex(m.Stopped,'incremental_volume_owner_changed'):self.updater.activate(self.state)
        self.assertEqual(posts,[]);self.assertEqual(self.transport.calls,[])
        self.assertIn('pendingPlan',self.state)

    def test_lost_start_rollback_retains_prior_and_operation_receipts_without_recreating(self):
        self.transport.loss='start'
        with self.assertRaises(OSError):self.updater.activate(self.state)
        self.assertTrue(self.transport.containers[f.NEW_ID]['State']['Running'])
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back');self.assert_history()
        self.assertEqual(sum(args[0]=='create' for args in self.transport.calls),1)

    @unittest.skipUnless(getattr(os,'geteuid',lambda:None)()==10001,'actual production UID backup script')
    def test_actual_serialized_backup_program_preserves_existing_owned_rows(self):
        for node in ('cluster','main'):
            self.queue.schedule_node_owned(node,dict(schemaVersion=1,requestId=str(uuid.uuid4()),authorityId=m.AUTHORITY,expectedRevision=self.queue.get_node(node)['revision'],targetImage=dict(main=self.incremental_profile.main_image,cluster=self.incremental_profile.cluster_image)[node],drainAt='2026-01-01T00:00:00Z',notBefore='2026-01-01T00:00:00Z',afterJobId=None))
        target=self.root/'serialized-copy';target.mkdir(mode=0o700)
        source=repr(str(self.volume));destination=repr(str(target))
        program=m.backup_script(incremental_profile=True).replace("volume_census('/source',authority=",'volume_census('+source+',authority=').replace("Path('/source'),Path('/backup')",'Path('+source+'),Path('+destination+')')
        stream=io.StringIO()
        with redirect_stdout(stream):exec(compile(program,'<actual-incremental-backup>','exec'),{'__name__':'__main__'})
        report=json.loads(stream.getvalue());before=volume_census(self.volume,authority=m.AUTHORITY);after=volume_census(target,authority=m.AUTHORITY)
        self.assertEqual(report['fullCensusSha256'],digest(before));self.assertEqual(dict(before,rootMode=0o700),after)
        self.assertEqual([row for row in after['ownedRows'] if row['request_id'] in {old['request_id'] for old in self.prior_rows}],self.prior_rows)
        self.assertEqual(len(after['ownedRows']),4)

    def test_actual_cluster_local_gate_proof_rechecks_bound_current_backend(self):
        config=controller_fixture.configuration()
        for node in ('cluster','main'):config[node]['helperSha256']=incremental_helper_digest()
        fixture=NativeNodeFixture(self.root/'gate-node.sqlite',port=13001,engine_version='3.6.2',synthetic_enabled=True).start();self.stack.callback(fixture.close)
        profile=self.incremental_profile;calls=[]
        class Transport:
            def environment(_):return {'LOCAL_NODE_TOKEN':'synthetic-gate-backend-token'}
            def inspected(_identifier,name):
                calls.append(('inspect',name))
                return dict(Id=profile.cluster_container,Name='/node-odx-gpu',Image=profile.cluster_image,State=dict(Running=True),Config=dict(Labels={'com.docker.compose.project':'ledgetop-cluster-node-private','com.docker.compose.service':'nodeodm'}),NetworkSettings=dict(Ports={'3000/tcp':[dict(HostIp='127.0.0.1',HostPort='13001')]}))
            def docker(_,*args,**kwargs):
                calls.append(args)
                if args[0]=='info':return config['cluster']['engineId']+'\n'
                if args[0]=='exec':return ''
                if args[0]=='top':return 'PID COMMAND\n1 node /var/www/index.js\n'
                raise AssertionError(args)
        transport=Transport();factory=m.GateFactory(config,operation_id=self.state['operation'],transport=transport,incremental_binding=profile)
        class CaptureSet:
            def __init__(_,local,remote):_.local=local;_.remote=remote
        factory.classes=(LocalGate,BridgeGateClient,CaptureSet)
        gate_path=self.root/'actual-cluster-gate'
        with patch('processing.node_gate.canonical_gate_path',return_value=gate_path),patch('ops.coordinator_gate_bridge.canonical_gate_path',return_value=gate_path):
            with NodeGate(config['cluster']['engineId']) as gate:gate.provision('cluster','http://127.0.0.1:18080',m.AUTHORITY)
            gates=factory()
            with gates.local:
                gates.local.check();self.assertGreaterEqual(sum(call[0]=='inspect' for call in calls),2)
                for call in calls:
                    if call[0] in ('exec','top'):self.assertEqual(call[1],profile.cluster_container)
                with patch.object(transport,'inspected',return_value=dict(Id='0'*64,Name='/node-odx-gpu',Image=profile.cluster_image,State=dict(Running=True),Config=dict(Labels={}),NetworkSettings=dict(Ports={}))):
                    with self.assertRaises(Exception):gates.local.check()


@unittest.skipUnless(os.name=='posix','Linux owned updater fixture')
class NodeODM3IncrementalUpdateTests(unittest.TestCase):
    """Real HTTP/SQLite transition with the four-row surgical baseline."""
    make_transport=IncrementalUpdateTests.make_transport
    backup=IncrementalUpdateTests.backup
    target='sha256:a04271d08115481d5dd3a8a171b9b609567b823c7d5ea8af34b56b8915db5837'

    def setUp(self):
        IncrementalUpdateTests.setUp(self)
        for node in ('cluster','main'):
            current=self.queue.get_node(node)
            row=self.queue.schedule_node_owned(node,dict(schemaVersion=1,requestId=str(uuid.uuid4()),authorityId=m.AUTHORITY,expectedRevision=current['revision'],targetImage=f.MAIN,drainAt='2026-01-01T00:00:00Z',notBefore='2026-01-01T00:00:00Z',afterJobId=None))
            self.queue.cancel_node(node,dict(expectedRevision=row['revision'],planId=row['planId']))
        baseline=volume_census(self.volume,authority=m.AUTHORITY,uid=os.geteuid())
        self.prior_rows=baseline['ownedRows'];self.assertEqual(len(self.prior_rows),4)
        self.incremental_profile=replace(self.incremental_profile,predecessor_image=TARGET,predecessor_source='6bbd468e1177746cb695e43195e75355b0681f23',candidate_source='d738edb209972a1723fa5af374578951b2a8448c',owned_history_sha256=digest(self.prior_rows))
        self.gates.incremental_binding=self.incremental_profile
        self.transport=self.make_transport();self.operation=self.root/'surgical-operation';self.operation.mkdir(mode=0o700)
        self.updater=m.ReferenceCoordinatorUpdate(self.operation,gate_factory=self.gates,transport=self.transport,cluster_probe=lambda _:None,incremental_binding=self.incremental_profile,helper_source_commit='d'*40)
        self.state=self.updater.prepare(self.target,self.incremental_profile.candidate_source,f.MAIN)

    def assert_history(self):
        current=volume_census(self.volume,authority=m.AUTHORITY,uid=os.geteuid())
        prior_ids={row['request_id'] for row in self.prior_rows}
        self.assertEqual([row for row in current['ownedRows'] if row['request_id'] in prior_ids],self.prior_rows)
        self.assertEqual(len(current['ownedRows']),6)
        self.assertEqual(current['tables']['jobs']['sha256'],self.state['baseline']['tables']['jobs']['sha256'])
        self.assertEqual({self.queue.get_node(node)['state'] for node in ('cluster','main')},{'active'})

    def test_surgical_actual_prepare_activate_preserves_four_rows_and_adds_only_two(self):
        self.assertTrue(m.incremental_release_reviewed(self.incremental_profile,self.target,self.incremental_profile.candidate_source))
        self.assertEqual(self.updater.activate(self.state)['phase'],'completed');self.assert_history()
        self.assertFalse(self.state['databaseRestored']);self.assertEqual(sum(args[0]=='create' for args in self.transport.calls),1)

    def test_surgical_lost_start_explicit_rollback_preserves_all_six_rows(self):
        self.transport.loss='start'
        with self.assertRaises(OSError):self.updater.activate(self.state)
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back');self.assert_history()
        self.assertEqual(sum(args[0]=='create' for args in self.transport.calls),1)

    def test_surgical_release_refuses_any_other_image_or_source_before_effects(self):
        for predecessor,candidate,source in ((replace(self.incremental_profile,predecessor_image='sha256:'+'0'*64),self.target,self.incremental_profile.candidate_source),(self.incremental_profile,'sha256:'+'0'*64,self.incremental_profile.candidate_source),(self.incremental_profile,self.target,'0'*40)):
            self.assertFalse(m.incremental_release_reviewed(predecessor,candidate,source))
        changed=copy.deepcopy(self.state);changed['image']='sha256:'+'0'*64
        for action in (self.updater.activate,self.updater.rollback):
            with self.assertRaisesRegex(m.Stopped,'incremental_application_migration_review_required'):action(changed)
        self.assertEqual(self.transport.calls,[])


if __name__=='__main__':unittest.main()
