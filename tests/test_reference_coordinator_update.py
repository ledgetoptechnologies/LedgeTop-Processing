"""Temporary real SQLite/HTTP, synthetic Docker/SSH boundaries; no host changes."""
from contextlib import redirect_stdout
import copy
import io
import http.client
import json
import os
from pathlib import Path
import shutil
import socket
import socketserver
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from ops import update_reference_coordinator as module
from processing.app import make_handler
from processing.config import Config
from processing.odx_smoke_worker import Client
from processing.reference_store import ReferenceStore
from processing.reference_terminal import TerminalService
from processing.viewer_tasks import TaskStore
from tests.test_service import TOKEN,start_server

TARGET='sha256:'+'b'*64
SOURCE='c'*40
NEW_ID='d'*64
MAIN='sha256:'+'e'*64
OPERATOR='synthetic-coordinator-update-operator'


class Gates:
    binding={'cluster':{'generation':1},'main':{'generation':1}}
    def __init__(self):self.live=False;self.dead=False;self.checks=0
    def __call__(self):return self
    def __enter__(self):self.live=True;self.check();return self
    def __exit__(self,*args):self.live=False
    def check(self):
        self.checks+=1
        if not self.live or self.dead:raise module.Stopped('synthetic_gate_lost')


@unittest.skipUnless(os.name=='posix','Linux ownership and offline helper contract')
class ReferenceCoordinatorUpdateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700)
        self.volume=self.root/'volume'
        self.handler=make_handler(Config(TOKEN,'','',1000,20000,prototype_state_dir=str(self.volume),
            prototype_node_ids=('cluster','main'),prototype_operator_token=OPERATOR))
        self.queue=self.handler.prototype_queue
        self.addCleanup(self.queue.close)
        self.queue.allowed_profiles.add('reference-pipeline-v1')
        self.store=ReferenceStore(self.queue)
        self.terminal=TerminalService(self.store)
        TaskStore(self.queue,self.volume/'viewer-tasks');(self.volume/'viewer-spool').mkdir(mode=0o700)
        with self.queue._db() as db:
            db.execute('UPDATE coordinator_identity SET authority_id=?',(module.AUTHORITY,))
            db.execute('DROP TABLE owned_maintenance_schedules')
            for index in range(12):
                db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,'staging','treedar-synthetic-v1',?)",
                    (str(__import__('uuid').uuid4()),'synthetic-'+str(index),'failed' if index<2 else 'succeeded'))
        self.queue.authority_id=module.AUTHORITY
        self.server=start_server(self.handler);self.addCleanup(self.server.server_close);self.addCleanup(self.server.shutdown)
        origin='http://127.0.0.1:'+str(self.server.server_port)
        self.api,self.operator=Client(origin,TOKEN),Client(origin,OPERATOR)
        self.gates=Gates();self.transport=self.make_transport()
        self.operation=self.root/'operation';self.operation.mkdir(mode=0o700)
        self.updater=module.ReferenceCoordinatorUpdate(self.operation,gate_factory=self.gates,transport=self.transport,cluster_probe=lambda state:None)
        self.backup_patch=patch.object(module,'copy_backup',side_effect=self.backup)
        self.backup_patch.start();self.addCleanup(self.backup_patch.stop)
        self.probe=patch.object(module,'owned_api_probe',return_value={'operatorLookupVerified':True})
        self.probe.start();self.addCleanup(self.probe.stop)
        self.state=self.updater.prepare(TARGET,SOURCE,MAIN)

    def make_transport(self):
        owner=self
        profile=getattr(owner,'incremental_profile',None)
        old_id=profile.predecessor_container if profile else module.OLD_ID
        old_image=profile.predecessor_image if profile else module.OLD_IMAGE
        old_source=profile.predecessor_source if profile else module.OLD_SOURCE
        class Transport:
            save=staticmethod(module.legacy.save);private_file=staticmethod(module.legacy.private_file)
            def __init__(self):
                self.calls=[];self.loss=None;self.hook=None
                self.application={'PROCESSING_API_TOKEN':TOKEN,'PROCESSING_PROTOTYPE_OPERATOR_TOKEN':OPERATOR,
                    'PROCESSING_REFERENCE_CONFIG':module.legacy.CONFIG_PATH}
                self.old=self.container(old_id,old_image,module.NAME,True)
                self.containers={old_id:self.old,
                    '6'*64:self.container('6'*64,'sha256:'+'6'*64,'historical-native-untouched',False)}
            def image(self,image):
                version=old_source if image==old_image else profile.candidate_source if profile else SOURCE
                defaults=['PATH=/old/default'] if image==old_image else ['PATH=/new/default']
                return dict(Id=image,Config=dict(Env=defaults,Labels={'org.opencontainers.image.revision':version},
                    User='10001:10001',WorkingDir='/app',Cmd=['python','-m','processing.app'],Entrypoint=None,Healthcheck={'Test':['CMD','health']}))
            def container(self,identifier,image,name,running):
                config=self.image(image)['Config'].copy()
                defaults=dict(item.split('=',1) for item in config['Env'])
                config['Env']=[key+'='+value for key,value in dict(defaults,**self.application).items()]
                config['Labels']={}
                return dict(Id=identifier,Image=image,Name='/'+name,Config=config,
                    State={'Running':running,'Health':{'Status':'healthy'}},
                    HostConfig=dict(Init=True,NetworkMode='bridge',ReadonlyRootfs=True,Privileged=False,PidsLimit=64,NanoCpus=500000000,
                        Memory=268435456,MemorySwap=536870912,IpcMode='private',ShmSize=67108864,AutoRemove=False,
                        CapDrop=['ALL'],CapAdd=None,SecurityOpt=['no-new-privileges'],RestartPolicy={'Name':'unless-stopped'},
                        LogConfig={'Type':'json-file','Config':{}},PortBindings=module.legacy.REFERENCE_PORTS),
                    Mounts=[{'Type':'volume','Name':module.VOLUME,'Destination':'/prototype-state','RW':True},
                        {'Type':'bind','Source':str(module.legacy.TLS_MOUNT),'Destination':'/run/processing-reference','RW':False}])
            def facts(self,item,**kwargs):return module.legacy.facts(item,**kwargs)
            def environment(self):return {key:value for key,value in self.application.items() if key!='PROCESSING_REFERENCE_CONFIG'}
            def inspected(self,identifier):
                if identifier=='node-odx-gpu':return {'Id':'f'*64,'Image':'sha256:'+'f'*64,'State':{'Running':True}}
                matches=[item for key,item in self.containers.items() if key.startswith(identifier) or item['Name']=='/'+identifier]
                if len(matches)!=1:raise module.Stopped('synthetic_identity_unavailable')
                return copy.deepcopy(matches[0])
            def local_clients(self):return self.environment(),owner.api,owner.operator
            def sole_owner(self,identifier):
                if [item['Id'] for item in self.containers.values() if item['State']['Running']]!=[identifier]:raise module.Stopped('synthetic_other_owner')
            def activity(self,api):
                assert api.json('GET','/v1/prototype/authority')['authorityId']==module.AUTHORITY
                jobs=api.json('GET','/v1/prototype/jobs')['jobs']
                assert all(job['status'] in ('succeeded','failed') for job in jobs)
                return dict(count=len(jobs),sha256=module.digest(sorted(jobs,key=lambda row:row['id'])))
            def candidate_loader(self,*args):pass
            def health(self,*args,**kwargs):pass
            def verify_reference(self):return {'tlsVerified':True,'nativeProductionCredentialsRejected':True}
            def docker(self,*args,**kwargs):
                action=args[0]
                if action=='ps':
                    if any(str(value).startswith('volume=') for value in args):return '\n'.join(item['Id'] for item in self.containers.values() if item['State']['Running'])
                    if any(str(value).startswith('label=') for value in args):return '\n'.join(item['Id'] for item in self.containers.values() if item['Config']['Labels'].get(module.LABEL)==owner.state['operation'])
                    return ''
                if action in ('run','exec'):
                    if args[-1]==module.TLS_CENSUS:
                        files=[{'path':'runtime.json','uid':10001,'mode':0o600,'sha256':'a'*64}]
                        return json.dumps(dict(files=files)) if profile else json.dumps(files)
                    if action=='run':
                        assert '--network' in args and args[args.index('--network')+1]=='none'
                        assert 'type=volume,src='+module.VOLUME+',dst=/source,readonly' in args
                    if profile:
                        program=args[-1].replace(repr('/source' if action=='run' else '/prototype-state'),repr(str(owner.volume)))
                        stream=io.StringIO()
                        with redirect_stdout(stream):exec(compile(program,'<serialized-incremental-census>','exec'),{'__name__':'__main__'})
                        return stream.getvalue()
                    return json.dumps(module.volume_census(owner.volume,uid=os.geteuid(),offline=action=='run'))
                assert owner.gates.live and not owner.gates.dead
                self.calls.append(args)
                if self.hook:self.hook(action)
                if action=='create':
                    item=self.container(NEW_ID,TARGET,module.NAME,False)
                    item['Config']['Labels'][module.LABEL]=owner.state['operation'];self.containers[NEW_ID]=item
                    result=NEW_ID
                elif action=='rename':self.containers[args[1]]['Name']='/'+args[2];result=''
                elif action in ('start','stop'):
                    self.containers[args[-1]]['State']['Running']=action=='start';result=args[-1]
                    if action=='start' and args[-1]==NEW_ID:
                        with owner.queue._db() as db:
                            db.execute('''CREATE TABLE IF NOT EXISTS owned_maintenance_schedules (
                                request_id TEXT PRIMARY KEY,node_id TEXT NOT NULL,authority_id TEXT NOT NULL,
                                request_sha256 TEXT NOT NULL,request_json TEXT NOT NULL,receipt_json TEXT NOT NULL)''')
                else:raise AssertionError(args)
                if self.loss==action:self.loss=None;raise OSError('synthetic lost mutation response')
                return result
        return Transport()

    def backup(self,transport,root,state):
        destination=root/'copied-volume';shutil.copytree(self.volume,destination)
        before=module.volume_census(self.volume,uid=os.geteuid(),offline=True)
        after=module.volume_census(destination,uid=os.geteuid(),offline=True)
        self.assertEqual(before,after)
        return dict(fullCensusSha256=module.digest(before),treeSha256=module.digest(before['files']),sqliteQuickCheck='ok')

    def activate(self):return self.updater.activate(self.state)

    def test_full_reference_recipe_application_defaults_and_tls_ports_preserved(self):
        self.assertEqual(self.activate()['phase'],'completed')
        current=self.transport.inspected(module.NAME)
        self.assertEqual(current['Id'],NEW_ID)
        self.assertEqual(current['HostConfig']['PortBindings'],module.legacy.REFERENCE_PORTS)
        self.assertEqual(current['Mounts'],self.transport.old['Mounts'])
        self.assertIn('PATH=/new/default',current['Config']['Env'])
        self.assertEqual(sum(call[0]=='create' for call in self.transport.calls),1)
        self.assertFalse(any('6'*64 in call for call in self.transport.calls))
        self.assertEqual({self.queue.get_node(node)['state'] for node in ('cluster','main')}, {'active'})
        self.assertFalse(self.state['databaseRestored'])

    def test_lost_legacy_schedule_ack_holds_no_adoption_or_repost(self):
        original=self.operator.json;posts=[]
        def lose(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/schedule'):posts.append(payload);raise OSError('lost schedule ACK')
            return value
        with patch.object(self.operator,'json',side_effect=lose):
            with self.assertRaises(OSError):self.activate()
            with self.assertRaises(module.Stopped):self.activate()
            with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertEqual(len(posts),1);self.assertIn('pendingPlan',self.state)
        self.assertEqual(self.transport.calls,[])
        self.assertTrue(self.transport.old['State']['Running'])

    def test_lost_create_response_recovers_exact_label_never_recreates(self):
        self.transport.loss='create'
        with self.assertRaises(OSError):self.activate()
        self.assertEqual(self.updater.candidate(self.state)['Id'],NEW_ID)
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        self.assertTrue(self.transport.containers[module.OLD_ID]['State']['Running'])
        self.assertEqual(sum(call[0]=='create' for call in self.transport.calls),1)

    def test_lost_old_stop_response_is_read_back_on_explicit_rollback_without_second_stop(self):
        self.transport.loss='stop'
        with self.assertRaises(OSError):self.activate()
        self.assertFalse(self.transport.old['State']['Running'])
        with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        self.assertEqual([call[0] for call in self.transport.calls],['stop','start'])

    def test_lost_old_rename_response_never_repeats_rename_or_creates_candidate(self):
        self.transport.loss='rename'
        with self.assertRaises(OSError):self.activate()
        self.assertEqual(self.transport.old['Name'],'/'+self.state['rollbackName'])
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        self.assertEqual([call[0] for call in self.transport.calls],['stop','rename','rename','start'])

    def test_gate_loss_before_stop_leaves_original_running_and_no_following_mutation(self):
        original=self.updater.save
        def lost(state,phase=None,**kwargs):
            original(state,phase,**kwargs)
            if phase=='stop_old_pending':self.gates.dead=True
        with patch.object(self.updater,'save',side_effect=lost):
            with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(self.transport.calls,[])
        self.assertTrue(self.transport.old['State']['Running'])

    def test_private_content_admitted_during_old_stop_blocks_before_backup_or_create(self):
        def late(action):
            if action=='stop':
                with self.queue._db() as db:db.execute("UPDATE jobs SET claim_hash='new-private-content' WHERE seq=1")
        self.transport.hook=late
        with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual([call[0] for call in self.transport.calls],['stop'])
        self.assertFalse((self.operation/'copied-volume').exists())

    def fail_after_start(self):
        self.transport.loss='start'
        with self.assertRaises(OSError):self.activate()
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])

    def test_compatible_empty_owned_table_allows_exact_reference_tls_rollback(self):
        self.fail_after_start()
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        restored=self.transport.inspected(module.NAME)
        self.assertEqual((restored['Id'],restored['Image']),(module.OLD_ID,module.OLD_IMAGE))
        self.assertEqual(restored['HostConfig']['PortBindings'],module.legacy.REFERENCE_PORTS)

    def test_receipt_admitted_while_new_stops_refuses_old_and_restarts_only_new(self):
        self.fail_after_start()
        def late(action):
            if action=='stop':
                with self.queue._db() as db:db.execute("INSERT INTO owned_maintenance_schedules VALUES('new','main','a','b','{}','{}')")
        self.transport.hook=late
        calls=len(self.transport.calls)
        with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])
        self.assertFalse(self.transport.containers[module.OLD_ID]['State']['Running'])
        self.assertEqual([row[0] for row in self.transport.calls[calls:]],['stop','start'])
        self.assertTrue(self.state['downgradeRefused'])

    def test_partial_reopen_unknown_second_ack_cannot_rollback_or_cancel_peer(self):
        original=self.operator.json;cancels=[]
        def lose(method,route,payload=None,**kwargs):
            if route.endswith('/cancel'):
                cancels.append(route)
                if len(cancels)==2:raise OSError('second reopen unknown before send')
            return original(method,route,payload,**kwargs)
        with patch.object(self.operator,'json',side_effect=lose):
            with self.assertRaises(module.Stopped):self.activate()
        before=len(self.transport.calls)
        with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertEqual(len(self.transport.calls),before)
        self.assertTrue(self.state['reopeningStarted'])
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])

    def test_gate_dies_between_reopens_no_second_cancel_or_old_restart(self):
        original=self.operator.json;cancels=[]
        def lose(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/cancel'):cancels.append(route);self.gates.dead=True
            return value
        with patch.object(self.operator,'json',side_effect=lose):
            with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(cancels,['/v1/prototype/nodes/cluster/cancel'])
        self.assertEqual(self.queue.get_node('main')['state'],'draining')
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])

    def test_unavailable_stopped_census_restarts_exact_candidate_never_old(self):
        self.fail_after_start();original=self.updater.census
        def unavailable(state,**kwargs):
            if kwargs.get('offline'):raise OSError('synthetic offline read unavailable')
            return original(state,**kwargs)
        before=len(self.transport.calls)
        with patch.object(self.updater,'census',side_effect=unavailable):
            with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertEqual([call[0] for call in self.transport.calls[before:]],['stop','start'])
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])
        self.assertFalse(self.transport.containers[module.OLD_ID]['State']['Running'])

    def test_online_new_owned_receipt_refuses_before_any_candidate_stop(self):
        self.fail_after_start()
        with self.queue._db() as db:db.execute("INSERT INTO owned_maintenance_schedules VALUES('new','main','a','b','{}','{}')")
        before=len(self.transport.calls)
        with self.assertRaises(AssertionError):self.updater.rollback(self.state)
        self.assertEqual(len(self.transport.calls),before)
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])

    def terminal_receipt_row(self):
        # Populate the actual service-owned schema. Even an isolated receipt
        # row is forbidden by this updater's empty-reference baseline policy.
        with self.queue._db() as db:
            db.execute('INSERT INTO reference_terminal_acceptances VALUES(?,?,?,?,?,?)',
                ('terminal-receipt','storage-receipt','synthetic-job',1,'a'*64,'{}'))

    def test_real_terminal_service_empty_schema_is_hashed_and_preserved(self):
        baseline=module.volume_census(self.volume,uid=os.geteuid())
        self.assertEqual(baseline['tables']['reference_terminal_acceptances']['count'],0)
        names=[row[1] for row in baseline['schema'] if row[2]=='reference_terminal_acceptances']
        self.assertEqual(len(names),4)  # Table plus three actual UNIQUE indexes.
        self.assertIn('reference_terminal_acceptances',names)
        self.assertEqual(self.activate()['phase'],'completed')

    def test_terminal_receipt_refuses_fresh_prepare_before_any_drain(self):
        self.terminal_receipt_row()
        fresh=self.root/'fresh-operation';fresh.mkdir(mode=0o700)
        updater=module.ReferenceCoordinatorUpdate(fresh,gate_factory=self.gates,transport=self.transport,cluster_probe=lambda state:None)
        with self.assertRaises(AssertionError):updater.prepare(TARGET,SOURCE,MAIN)
        checkpoint=json.loads((fresh/'checkpoint.json').read_text())
        self.assertEqual((checkpoint['phase'],checkpoint['plans']),('preparing',{}))
        self.assertEqual(self.transport.calls,[])
        self.assertTrue(self.transport.old['State']['Running'])

    def test_terminal_receipt_refuses_downgrade_before_any_candidate_stop(self):
        self.fail_after_start();self.terminal_receipt_row()
        before=len(self.transport.calls)
        with self.assertRaises(AssertionError):self.updater.rollback(self.state)
        self.assertEqual(len(self.transport.calls),before)
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])
        self.assertFalse(self.transport.containers[module.OLD_ID]['State']['Running'])

    def test_foreign_candidate_and_legacy_checkpoint_refused_without_mutation(self):
        self.fail_after_start();self.transport.containers[NEW_ID]['Config']['Labels'][module.LABEL]='foreign'
        before=len(self.transport.calls)
        with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertEqual(len(self.transport.calls),before)
        changed=dict(self.state,schemaVersion=1)
        with self.assertRaises(module.Stopped):self.updater.rollback(changed)

    def test_census_all_private_rows_schema_and_files_are_bound(self):
        baseline=module.volume_census(self.volume,uid=os.geteuid())
        with self.queue._db() as db:db.execute("UPDATE jobs SET claim_hash='changed' WHERE seq=1")
        with self.assertRaises(module.Stopped):module.compatible(module.volume_census(self.volume,uid=os.geteuid()),baseline)
        with self.queue._db() as db:db.execute('CREATE TABLE unknown_progress(value TEXT)')
        with self.assertRaises(AssertionError):module.volume_census(self.volume,uid=os.geteuid())

    def test_actual_copy_script_hashes_copied_full_database_and_retains_all_bytes(self):
        destination=self.root/'actual-backup';destination.mkdir(mode=0o700)
        code=module.backup_script().replace("Path('/source')",'Path('+repr(str(self.volume))+')').replace("Path('/backup')",'Path('+repr(str(destination))+')')
        code=code.replace('10001',str(os.geteuid())).replace('uid=10001','uid='+str(os.geteuid()))
        output=io.StringIO()
        with redirect_stdout(output):exec(compile(code,'<synthetic-offline-backup>','exec'),{})
        proof=json.loads(output.getvalue())
        self.assertEqual(proof['fullCensusSha256'],module.digest(module.volume_census(self.volume,uid=os.geteuid(),offline=True)))
        self.assertEqual((destination/'jobs.sqlite3').read_bytes(),(self.volume/'jobs.sqlite3').read_bytes())

    def test_actual_authenticated_owned_get_requires_exact_new_missing_receipt_body(self):
        self.probe.stop()
        with self.queue._db() as db:
            db.execute(next(row[3] for row in module.owned_schema() if row[0]=='table'))
        actual=http.client.HTTPConnection
        def connection(host,port,**kwargs):
            self.assertEqual((host,port),('127.0.0.1',18080))
            return actual('127.0.0.1',self.server.server_port,**kwargs)
        operator=Client('http://127.0.0.1:18080',OPERATOR)
        with patch.object(module.http.client,'HTTPConnection',side_effect=connection):
            self.assertEqual(module.owned_api_probe(operator),{'operatorLookupVerified':True})
            with self.assertRaises(module.Stopped):module.owned_api_probe(Client(operator.origin,TOKEN))
            reply=self.handler._reply
            def old_reply(handler,status,value):
                if '/schedule-owned/' in handler.path:value={'error':'not_found'}
                reply(handler,status,value)
            with patch.object(self.handler,'_reply',old_reply):
                with self.assertRaises(module.Stopped):module.owned_api_probe(operator)

    def test_unowned_private_node_column_change_during_drain_is_not_normalized_away(self):
        original=self.operator.json
        def tamper(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/main/schedule'):
                with self.queue._db() as db:db.execute("UPDATE nodes SET finish_status='foreign' WHERE id='cluster'")
            return value
        with patch.object(self.operator,'json',side_effect=tamper):
            with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(self.transport.calls,[])

    def test_gate_loss_after_candidate_health_cannot_reopen_nodes(self):
        def health(*args,**kwargs):self.gates.dead=True
        with patch.object(self.transport,'health',side_effect=health):
            with self.assertRaises(module.Stopped):self.activate()
        self.assertFalse(self.state.get('reopeningStarted',False))
        self.assertEqual({self.queue.get_node(node)['state'] for node in ('cluster','main')},{'draining'})
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])

    def test_lost_cancel_ack_reconciles_exact_revision_without_another_cancel(self):
        original=self.operator.json;cancels=[]
        def lose(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/cancel'):
                cancels.append(route);raise OSError('synthetic ACK loss after exact cancel')
            return value
        with patch.object(self.operator,'json',side_effect=lose):self.assertEqual(self.activate()['phase'],'completed')
        self.assertEqual(len(cancels),2)
        self.assertEqual(len(set(cancels)),2)

    def test_gate_loss_during_final_cancel_never_marks_completed(self):
        original=self.operator.json
        def lose(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/main/cancel'):self.gates.dead=True
            return value
        with patch.object(self.operator,'json',side_effect=lose):
            with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(self.state['phase'],'needs_inspection')
        self.assertTrue(self.state['reopeningStarted'])
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])

    def test_new_admission_after_first_reopen_is_retained_and_does_not_block_second_own_reopen(self):
        original=self.operator.json
        def admit(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/cluster/cancel'):
                self.queue.submit(dict(requestId='admitted-after-reopen',environment='staging',profile='treedar-synthetic-v1'))
            return value
        with patch.object(self.operator,'json',side_effect=admit):self.assertEqual(self.activate()['phase'],'completed')
        self.assertEqual(self.queue.get_node('main')['state'],'active')
        self.assertEqual(len(self.queue.list()),13)
        self.assertEqual(self.queue.list()[-1]['status'],'queued')
        calls=len(self.transport.calls)
        with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertEqual(len(self.transport.calls),calls)

    def test_lost_old_start_ack_retry_keeps_restored_old_sole_owner(self):
        self.fail_after_start()
        self.transport.loss='start'
        with self.assertRaises(OSError):self.updater.rollback(self.state)
        self.assertEqual(self.state['phase'],'rollback_start_old_pending')
        self.assertTrue(self.transport.containers[module.OLD_ID]['State']['Running'])
        before=len(self.transport.calls)
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        self.assertEqual(len(self.transport.calls),before)
        self.assertFalse(self.transport.containers[NEW_ID]['State']['Running'])

    def test_foreign_volume_owner_prevents_retention_start(self):
        self.fail_after_start()
        self.transport.containers[NEW_ID]['State']['Running']=False
        foreign=self.transport.containers['6'*64];foreign['State']['Running']=True
        calls=len(self.transport.calls)
        with self.gates:
            with self.assertRaises(module.Stopped):self.updater.retain(self.state,self.gates,self.transport.inspected(NEW_ID))
        self.assertEqual(len(self.transport.calls),calls)

    def test_holder_loss_during_slow_read_is_rechecked_before_any_schedule(self):
        original=self.transport.docker
        def lose(*args,**kwargs):
            value=original(*args,**kwargs)
            if args[-1]==module.TLS_CENSUS and self.gates.live:self.gates.dead=True
            return value
        with patch.object(self.transport,'docker',side_effect=lose):
            with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(self.transport.calls,[])
        self.assertEqual(self.state['plans'],{})

    def test_live_factory_binds_exact_bridge_helper_without_exporting_session_nonce(self):
        from ops.coordinator_gate_bridge import helper_digest
        def binding(node):return dict(engineId='fixed-'+node,nodeId=node,authorityId=module.AUTHORITY,
            coordinatorOrigin='http://127.0.0.1:18080',generation=1,helperSha256=helper_digest())
        config=dict(schemaVersion=1,main=binding('main'),cluster=binding('cluster'),sessionNonce='a'*64,timeoutSeconds=10)
        factory=module.GateFactory(config,operation_id=self.state['operation'],transport=self.transport)
        self.assertEqual(factory.binding,{'main':config['main'],'cluster':config['cluster']})
        self.assertNotIn(config['sessionNonce'],json.dumps(factory.binding))
        config['main']['helperSha256']='b'*64
        with self.assertRaises(module.Stopped):module.GateFactory(config,operation_id=self.state['operation'])

    def test_cluster_probe_actual_node_http_and_fixed_upload_process_census(self):
        from ops.native_node_fixture import NativeNodeFixture
        fixture=NativeNodeFixture(self.root/'native-fixture.sqlite',synthetic_enabled=True)
        fixture.start();self.addCleanup(fixture.close)
        actual_connection=socket.create_connection
        def connection(address,**kwargs):
            self.assertEqual(address,('127.0.0.1',13001))
            return actual_connection(('127.0.0.1',int(fixture.origin.rsplit(':',1)[1])),**kwargs)
        container=dict(Id='f'*64,Name='/node-odx-gpu',Image=self.state['engineImages']['cluster'],State={'Running':True},
            Config={'Labels':{'com.docker.compose.project':'ledgetop-cluster-node-private','com.docker.compose.service':'nodeodm'}},
            NetworkSettings={'Ports':{'3000/tcp':[{'HostIp':'127.0.0.1','HostPort':'13001'}]}})
        def docker(*args,**kwargs):
            if args[0]=='top':return 'PID COMMAND\n123 /usr/bin/node /var/www/index.js\n'
            if args[2:4]==('/bin/ls','-A'):return '.gitignore\n'
            if args[2]=='/usr/bin/stat':return 'regular file 1\n'
            if args[2]=='/bin/cat':return '*'
            raise AssertionError(args)
        with patch.object(module.socket,'create_connection',side_effect=connection), \
                patch.object(self.transport,'environment',return_value={'LOCAL_NODE_TOKEN':'private-existing-local-token'}), \
                patch.object(self.transport,'inspected',return_value=container),patch.object(self.transport,'docker',side_effect=docker):
            self.updater.cluster_idle(self.state)
            with fixture._db() as db:
                db.execute("INSERT INTO tasks(uuid,name,options,committed,status) VALUES('00000000-0000-4000-8000-000000000001','synthetic','[]',1,40)")
            # Zero queue count alone cannot hide a retained completed task.
            with self.assertRaises(module.Stopped):self.updater.cluster_idle(self.state)


class ClusterReadDeadlineTests(unittest.TestCase):
    def trickle(self,mode):
        stopped=threading.Event();finished=threading.Event();requests=[]
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                try:
                    request=b''
                    while b'\r\n\r\n' not in request:request+=self.request.recv(4096)
                    requests.append(request)
                    if mode=='headers':prefix=b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Trickle: '
                    else:prefix=b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 128\r\n\r\n'
                    self.request.sendall(prefix)
                    for _ in range(128):
                        if stopped.wait(.02):break
                        self.request.sendall(b' ')
                except (OSError,ConnectionError):pass
                finally:finished.set()
        server=socketserver.ThreadingTCPServer(('127.0.0.1',0),Handler)
        server.daemon_threads=False;server.block_on_close=True
        thread=threading.Thread(target=server.serve_forever);thread.start()
        actual_connection=socket.create_connection;connections=[]
        def connect(address,**kwargs):
            self.assertEqual(address,('127.0.0.1',13001))
            value=actual_connection(server.server_address,**kwargs);connections.append(value);return value
        try:
            start=time.monotonic()
            with patch.object(module.socket,'create_connection',side_effect=connect):
                with self.assertRaises(module.Stopped) as caught:
                    module.cluster_http_json('/info','private+?&%token',start+.15)
            self.assertLess(time.monotonic()-start,1)
            self.assertNotIn('private',str(caught.exception))
            self.assertEqual([item.fileno() for item in connections],[-1])
            self.assertIn(b'/info?token=private%2B%3F%26%25token HTTP/1.1',requests[0])
        finally:
            stopped.set();server.shutdown();server.server_close();thread.join(2)
        self.assertTrue(finished.is_set());self.assertFalse(thread.is_alive())

    def test_actual_trickled_headers_obey_total_deadline_and_close_socket(self):self.trickle('headers')
    def test_actual_trickled_body_obeys_total_deadline_and_close_socket(self):self.trickle('body')
