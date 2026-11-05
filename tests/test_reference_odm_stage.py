"""Real NativeEngine HTTP plus actual parent Queue/scratch/gate; no reconstruction."""
from contextlib import closing
import copy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from ops.native_node_fixture import NativeNodeFixture,ARCHIVE
from ops.reference_worker_daemon import DaemonStateLock
from processing import reference_protocol as p
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.scratch_policy import ScratchLedger
from processing.reference_pipeline import PipelineJournal,PipelinePolicy,PipelineHeld
from processing.reference_odm_stage import NativeODMStage,NativeRuntimePolicy,NativeRuntimeProbe,NativeStageHeld
from processing.viewer_service import NativeEngine
from tests.test_reference_pipeline import ActualGuard,SyntheticStage


class DockerProbeTransport:
    """Only local runtime inspection is simulated; engine traffic is actual HTTP."""
    def __init__(self,policy):self.policy=policy;self.image=policy.image_id;self.container='c'*64;self.calls=[]
    def __call__(self,command,*,timeout,max_bytes):
        self.calls.append(command);args=command[3:];policy=self.policy
        if args==['info','--format','{{.ID}}']:body=policy.engine_id
        else:
            assert args[:2]==['container','inspect'] and args[-1]==policy.backend_name
            body=json.dumps(dict(id=self.container,name='/'+policy.backend_name,image=self.image,running=True,
                labels={'com.docker.compose.project':policy.compose_project,'com.docker.compose.service':policy.compose_service,
                    'com.docker.compose.project.config_files':policy.compose_file},
                ports={str(policy.container_port)+'/tcp':[dict(HostIp='127.0.0.1',HostPort=policy.engine_origin.rsplit(':',1)[1])]}))
        return subprocess.CompletedProcess(command,0,body.encode(),b'')


class OriginalInputs:
    def __init__(self,host):self.host=host;self.binding=host.journal.adapter_binding('odm')
    def resolve(self,intent):return {o['id']:self.host.input_paths[o['id']] for o in intent['stage']['inputObjects']}


@unittest.skipUnless(os.name=='posix','native intermediate host ownership requires Linux')
class NativeODMStageTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup);self.root=Path(temporary.name);self.root.chmod(0o700)
        self.owner=DaemonStateLock(self.root).__enter__();self.addCleanup(self.owner.__exit__,None,None,None)
        self.fixture=NativeNodeFixture(self.root/'native-http.sqlite',engine_version='3.8.5',synthetic_enabled=True).start();self.addCleanup(self.fixture.close)
        self.native=NativeEngine(self.fixture.origin,'synthetic-backend-token-not-persisted')
        self.docker_socket=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);self.docker_socket.bind(str(self.root/'docker.sock'));self.addCleanup(self.docker_socket.close)
        compose=dict(services=dict(nodeodm=dict(image='sha256:'+'a'*64,container_name='native-node')),networks={})
        self.compose=self.root/'compose.json';self.compose.write_text(json.dumps(compose));self.compose.chmod(0o600)
        base=copy.deepcopy(compose);base['services']['nodeodm'].pop('image')
        self.runtime=NativeRuntimePolicy('/usr/bin/docker','unix://'+str(self.root/'docker.sock'),'physical-main',str(self.compose),p.digest(base),'native-project','nodeodm','native-node','sha256:'+'a'*64,self.native.origin,'odm','3.8.5')
        self.docker=DockerProbeTransport(self.runtime)
        self.queue=Queue(self.root/'queue',node_ids=('cluster','main'),treedar_node_ids=('main',),reference_enabled=True);self.addCleanup(self.queue.close)
        self.store=ReferenceStore(self.queue)
        raw=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
        raw['manifest']['objects'].append(dict(id='original-lidar',name='original.las',byteSize=3,sha256=hashlib.sha256(b'abc').hexdigest(),kind='lidar',mediaType='application/octet-stream'))
        raw['manifest']['objectCount']=len(raw['manifest']['objects']);raw['manifest']['totalBytes']+=3
        raw['manifest']['inputCounts']['lidarObjects']=1;raw['manifest']['inputCounts']['points']=10;raw['manifest']['sha256']=p.manifest_sha256(raw['manifest'])
        raw['pipeline']['treedar']=dict(requested=True,inputObjectIds=['original-lidar'],provenanceReceiptId='original-source')
        checked=p.validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint'],available_engines=('odm','treedar'),allowed_odm_options=set(raw['pipeline']['odm']['options']))
        public,_=self.store.submit(checked)
        self.claim=self.store.claim(public['jobId'],node_id='main',worker_id='main-worker',worker_instance_id='instance-one',claim_request_id='parent-claim')
        self.policy=PipelinePolicy('main-worker','instance-one',self.queue.authority_id,'physical-main','http://127.0.0.1:18080',1,'3.8.5','0.0.1',self.runtime.sha256,'2'*64,'3'*64,100,100)
        self.gates=patch('processing.node_gate.canonical_gate_path',return_value=self.root/'gate');self.gates.start();self.addCleanup(self.gates.stop)
        with NodeGate(self.policy.engine_id) as gate:
            gate.provision('main',self.policy.coordinator_origin,self.policy.authority_id);gate.reserve('main',self.policy.coordinator_origin,self.policy.authority_id,1,'a'*32)
            gate.bind('a'*32,dict(id=self.claim['jobId'],workerId=self.policy.worker_id,nodeId='main',authorityId=self.policy.authority_id))
        scratch=self.root/'scratch';scratch.mkdir(mode=0o700)
        self.ledger=ScratchLedger(self.root/'scratch.sqlite',scratch,capacity_probe=lambda _:1000000,safety_floor_bytes=0)
        self.ledger.reserve(job_id=self.claim['jobId'],generation=1,reservation_id='parent-reservation',manifest_sha256=self.claim['manifestSha256'],input_bytes=checked['manifest']['totalBytes'],output_budget=200,environment='staging',ttl_seconds=600,worker_ready=True)
        self.input_paths={}
        directory=scratch/'jobs'/'parent-reservation';directory.mkdir(parents=True,mode=0o700);directory.parent.chmod(0o700)
        for obj in checked['manifest']['objects']:
            relative='jobs/parent-reservation/'+obj['id']+'.input';path=scratch/relative
            with path.open('wb') as file:file.write(b'abc');file.flush();os.fsync(file.fileno())
            path.chmod(0o600);self.input_paths[obj['id']]=relative
        for path in (directory,directory.parent,scratch,self.root,self.root.parent):NativeODMStage._sync(path)
        self.ledger.record_allocated('parent-reservation',checked['manifest']['totalBytes'])
        self.journal=PipelineJournal(self.root/'pipeline.sqlite',self.policy,verify_provenance=lambda _:False)
        self.guard=ActualGuard(self)
        self.plan=self.journal.bind_parent(self.claim,claim_request_id='parent-claim',reservation_id='parent-reservation',gate_receipt_id='a'*32,guard=self.guard)
        self.inputs=OriginalInputs(self);self.stage=self.open()

    def open(self,**changes):
        values=dict(journal=self.journal,ledger=self.ledger,engine=self.native,parent_guard=self.guard,state_owner=self.owner,input_resolver=self.inputs,runtime_probe=NativeRuntimeProbe(self.runtime,runner=self.docker));values.update(changes)
        return NativeODMStage(self.root/'native-stage.sqlite',self.runtime,**values)

    def advance(self):return self.journal.advance(self.claim['jobId'],'odm',self.stage,self.guard)
    def intent(self):
        with closing(sqlite3.connect(self.journal.path.as_uri()+'?mode=ro',uri=True)) as db:return json.loads(db.execute("SELECT intent FROM pipeline_stages WHERE stage='odm'").fetchone()[0])
    def reconcile(self):return self.journal.reconcile(self.claim['jobId'],'odm',self.stage,self.guard)
    def reservation(self):
        with self.ledger._transaction() as db:return dict(self.ledger._reservation(db,'parent-reservation'))
    def counts(self):return self.fixture.counts()

    def test_archive_growth_preserves_prior_control_allocation_across_restart(self):
        control=self.ledger.root/'jobs/parent-reservation/retained-control.bin'
        control.write_bytes(b'control-retained');control.chmod(0o600)
        baseline=self.reservation()['allocated']+control.stat().st_size
        self.ledger.record_allocated('parent-reservation',baseline)
        self.advance();self.stage=self.open()
        self.stage.tick(self.claim['jobId']);self.reconcile()
        self.assertEqual(self.reservation()['allocated'],baseline+len(ARCHIVE))
        self.stage=self.open();self.stage.tick(self.claim['jobId'])
        self.assertEqual(self.reservation()['allocated'],baseline+len(ARCHIVE))
        self.assertEqual(control.read_bytes(),b'control-retained')
        self.assertEqual(self.counts()['initialize'],1)

    def test_invalid_checkpointed_allocation_baseline_holds_without_engine_action(self):
        self.advance();before=self.counts()
        with closing(sqlite3.connect(self.stage.path)) as db:
            db.execute('UPDATE native_stage_executions SET allocation_base=0');db.commit()
        with self.assertRaisesRegex(NativeStageHeld,'baseline'):
            self.stage.tick(self.claim['jobId'])
        self.assertEqual(self.counts(),before)

    def test_real_native_http_has_one_attempt_and_retains_whole_parent_fence_and_reservation(self):
        self.assertEqual(self.advance()['state'],'executing');self.assertIsNone(self.stage.lookup(self.intent()))
        proof=self.stage.tick(self.claim['jobId']);self.assertEqual(proof['objects'][0]['role'],'odm_archive')
        self.assertEqual(self.reconcile()['state'],'waiting_treedar')
        self.assertEqual({k:self.counts()[k] for k in ('initialize','upload','commit','download')},{k:1 for k in ('initialize','upload','commit','download')})
        state=self.reservation();self.assertEqual(state['state'],'engine_started')
        self.assertEqual(state['allocated'],self.plan['parentBinding']['inputBytes']+len(ARCHIVE));self.assertGreater(state['remaining'],100)
        with self.ledger._transaction() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM results').fetchone()[0],0)
        with NodeGate(self.policy.engine_id) as gate:
            held=gate.snapshot()['reservation'];self.assertEqual(held['receiptId'],'a'*32);self.assertIsNone(held['completionDigest'])
        self.assertEqual(self.queue.get(self.claim['jobId'])['status'],'running')
        self.assertTrue(all((self.ledger.root/path).read_bytes()==b'abc' for path in self.input_paths.values()))
        self.assertNotIn(self.native.token.encode(),(self.root/'native-stage.sqlite').read_bytes())
        with self.assertRaises(PipelineHeld):self.journal.final_result(self.claim['jobId'])

    def test_lost_initialize_and_upload_replies_never_replay_after_restart(self):
        # Separate complete fixtures keep each mutation's failed attempt distinct.
        for target in ('init','upload'):
            with self.subTest(target=target):
                if target=='upload':
                    other=NativeODMStageTests();other.setUp()
                else:other=self
                dispatch=other.fixture._dispatch
                def lose(method,path,headers,body):
                    result=dispatch(method,path,headers,body)
                    return (503,{'error':'synthetic lost acknowledgement'},False) if method=='POST' and ('/task/new/'+target in path) else result
                other.fixture._dispatch=lose
                try:
                    with self.assertRaises(PipelineHeld):other.advance()
                    other.stage=other.open()
                    with self.assertRaises(NativeStageHeld):other.stage.start(other.intent())
                    with self.assertRaises(NativeStageHeld):other.stage.tick(other.claim['jobId'])
                    self.assertIsNone(other.stage.lookup(other.intent()))
                    self.assertEqual(other.counts()['initialize'],1);self.assertEqual(other.counts()['commit'],0)
                    if target=='upload':self.assertEqual(other.counts()['upload'],1)
                finally:
                    if other is not self:other.doCleanups()

    def test_lost_commit_reply_reconciles_gets_without_recommitting(self):
        dispatch=self.fixture._dispatch
        def lose(method,path,headers,body):
            result=dispatch(method,path,headers,body)
            return (503,{'error':'lost successful commit'},False) if method=='POST' and '/task/new/commit/' in path else result
        self.fixture._dispatch=lose
        with self.assertRaises(PipelineHeld):self.advance()
        self.stage=self.open();self.stage.tick(self.claim['jobId']);self.assertEqual(self.reconcile()['state'],'waiting_treedar')
        self.assertEqual(self.counts()['commit'],1)

    def test_partial_get_restart_preserves_cache_on_source_error_then_retries_only_get(self):
        self.advance();dispatch=self.fixture._dispatch
        def partial(method,path,headers,body):
            result=dispatch(method,path,headers,body)
            return (200,b'PKbad',True) if path.endswith('/download/all.zip') else result
        self.fixture._dispatch=partial
        with self.assertRaises(NativeStageHeld):self.stage.tick(self.claim['jobId'])
        part=self.ledger.root/'jobs/parent-reservation/odm-stage/.archive.part';self.assertEqual(part.read_bytes(),b'PKbad')
        before=self.reservation()['allocated'];self.stage=self.open()
        def unavailable(method,path,headers,body):
            result=dispatch(method,path,headers,body)
            return (503,{'error':'temporarily unavailable'},False) if path.endswith('/download/all.zip') else result
        self.fixture._dispatch=unavailable
        with self.assertRaises(NativeStageHeld):self.stage.tick(self.claim['jobId'])
        self.assertEqual(part.read_bytes(),b'PKbad');self.assertEqual(self.reservation()['allocated'],before)
        self.fixture._dispatch=dispatch;self.stage.tick(self.claim['jobId']);self.reconcile()
        self.assertEqual(self.counts()['initialize'],1);self.assertEqual(self.counts()['commit'],1);self.assertEqual(self.counts()['download'],3)
        self.assertEqual(self.reservation()['allocated'],self.plan['parentBinding']['inputBytes']+len(ARCHIVE))

    def test_lookup_has_no_engine_probe_guard_or_sql_writes(self):
        self.advance();self.stage.tick(self.claim['jobId']);intent=self.intent()
        before=(self.stage.path.read_bytes(),self.ledger.path.read_bytes(),self.journal.path.read_bytes())
        with (patch.object(self.native,'read',side_effect=AssertionError('no engine reads')),patch.object(self.stage.probe,'check',side_effect=AssertionError('no Docker')),
                patch.object(self.guard,'check',side_effect=AssertionError('no guard transport')),patch.object(self.stage,'_commit',side_effect=AssertionError('no SQL commits'))):
            self.assertEqual(self.stage.lookup(intent)['outcome'],'succeeded')
        self.assertEqual(before,(self.stage.path.read_bytes(),self.ledger.path.read_bytes(),self.journal.path.read_bytes()))

    def test_lookup_refuses_wal_before_sqlite_can_create_reader_sidecars(self):
        self.advance();self.stage.tick(self.claim['jobId']);intent=self.intent()
        with closing(sqlite3.connect(self.stage.path)) as db:self.assertEqual(db.execute('PRAGMA journal_mode=WAL').fetchone()[0],'wal')
        before=self.stage.path.read_bytes();names=set(self.root.iterdir())
        with self.assertRaisesRegex(NativeStageHeld,'rollback_native_stage_journal_required'):self.stage.lookup(intent)
        self.assertEqual(before,self.stage.path.read_bytes());self.assertEqual(names,set(self.root.iterdir()))

    def test_same_version_different_image_or_container_refuses_fresh_proof_verification(self):
        self.advance();self.stage.tick(self.claim['jobId']);intent=self.intent();proof=self.stage.lookup(intent)
        self.docker.image='sha256:'+'b'*64
        with self.assertRaises(NativeStageHeld):self.stage.verify(intent,proof)
        self.assertEqual(self.stage.lookup(intent),proof)
        self.docker.image=self.runtime.image_id;self.docker.container='d'*64
        with self.assertRaises(NativeStageHeld):self.stage.verify(intent,proof)

    def test_corrupt_output_or_sealed_proof_cannot_enter_pipeline_closure(self):
        self.advance();self.stage.tick(self.claim['jobId']);intent=self.intent();proof=self.stage.lookup(intent)
        path=self.ledger.root/self.stage.artifact_paths(intent)['odm-archive'];path.write_bytes(b'corrupt')
        with self.assertRaises(Exception):self.stage.verify(intent,proof)
        with closing(sqlite3.connect(self.stage.path)) as db,db:
            changed=copy.deepcopy(proof);changed['objects'][0]['sha256']='f'*64
            db.execute('UPDATE native_stage_executions SET proof=?',(json.dumps(changed),))
        with self.assertRaises(PipelineHeld):self.stage.lookup(intent)

    def test_terminal_failure_is_stage_evidence_not_parent_completion(self):
        self.advance()
        with self.fixture._db() as db:db.execute('UPDATE tasks SET status=30')
        proof=self.stage.tick(self.claim['jobId']);self.assertEqual(proof['outcome'],'failed');self.assertEqual(proof['objects'],[])
        self.assertEqual(self.reconcile()['state'],'failed');self.assertEqual(self.reservation()['state'],'engine_started')
        self.assertEqual(self.queue.get(self.claim['jobId'])['status'],'running')

    def test_coordinator_restart_hold_allows_terminal_readback_without_new_mutations(self):
        self.advance()
        with self.queue._db() as db:db.execute("UPDATE jobs SET status='held',held_reason='coordinator_restart' WHERE id=?",(self.claim['jobId'],))
        self.stage=self.open();self.assertEqual(self.stage.tick(self.claim['jobId'])['outcome'],'succeeded')
        self.assertEqual(self.reconcile()['state'],'waiting_treedar')
        self.assertEqual(self.queue.get(self.claim['jobId'])['status'],'held')
        with self.assertRaises(NativeStageHeld):self.stage.start(self.intent())
        self.assertEqual(self.counts()['initialize'],1);self.assertEqual(self.counts()['commit'],1)

    def test_missing_directory_and_runtime_policy_mismatch_refuse_before_new_files(self):
        missing=self.root/'not-created'/'native.sqlite'
        with self.assertRaises(NativeStageHeld):NativeODMStage(missing,self.runtime,journal=self.journal,ledger=self.ledger,engine=self.native,parent_guard=self.guard,state_owner=self.owner,input_resolver=self.inputs)
        self.assertFalse(missing.parent.exists())
        with self.assertRaises(NativeStageHeld):
            NativeODMStage(self.root/'foreign-policy.sqlite',replace(self.runtime,image_id='sha256:'+'b'*64),journal=self.journal,ledger=self.ledger,engine=self.native,parent_guard=self.guard,state_owner=self.owner,input_resolver=self.inputs)
        self.assertFalse((self.root/'foreign-policy.sqlite').exists())

    def test_original_lidar_or_unbound_input_paths_cannot_be_uploaded_as_images(self):
        self.inputs.resolve=lambda intent:{'original-lidar':self.input_paths['original-lidar']}
        with self.assertRaises(PipelineHeld):self.advance()
        self.assertEqual(self.counts()['initialize'],0)
        self.assertIsNone(self.stage.lookup(self.intent()))

    def test_removed_current_option_is_refused_before_any_engine_mutation(self):
        dispatch=self.fixture._dispatch
        self.fixture._dispatch=lambda method,path,headers,body:(200,[],False) if path=='/options' else dispatch(method,path,headers,body)
        with self.assertRaises(PipelineHeld):self.advance()
        self.assertEqual(self.counts()['initialize'],0)
        self.assertIsNone(self.stage.lookup(self.intent()))

    def test_digit_prefixed_output_option_uses_current_native_boolean_definition(self):
        dispatch = self.fixture._dispatch
        def catalog(method, path, headers, body):
            if method == 'GET' and path == '/options':
                return 200, [dict(name='3d-tiles', type='bool', value='false')], False
            return dispatch(method, path, headers, body)
        self.fixture._dispatch = catalog
        intent = {'stage': {'options': {'options': {'3d-tiles': True}}}}
        self.stage._options(intent)
        intent['stage']['options']['options']['3d-tiles'] = 'true'
        with self.assertRaises(NativeStageHeld):
            self.stage._options(intent)
        self.assertEqual(self.counts()['initialize'], 0)

    def test_fresh_enum_text_and_domains_are_checked_before_native_mutation(self):
        from tests.test_reference_option_policy import CATALOG
        dispatch=self.fixture._dispatch
        catalog=copy.deepcopy(CATALOG)
        self.fixture._dispatch=lambda method,path,headers,body:(200,catalog,False) if path=='/options' else dispatch(method,path,headers,body)
        values={'pc-quality':'high','feature-quality':'ultra','label':'Survey A','resolution':0.5,'count':2,'3d-tiles':True}
        intent={'stage':{'options':{'options':values}}}
        self.stage._options(intent)
        for changes in ({'pc-quality':'unknown'},{'resolution':0},{'count':True},{'label':'/private'}):
            with self.subTest(changes=changes),self.assertRaises(NativeStageHeld):
                self.stage._options({'stage':{'options':{'options':{**values,**changes}}}})
        catalog[0]['domain']=['low']
        with self.assertRaises(NativeStageHeld):self.stage._options(intent)
        catalog[0]=dict(name='pc-quality',type='string',help='External script command')
        with self.assertRaises(NativeStageHeld):self.stage._options(intent)
        self.assertEqual(self.counts()['initialize'],0)
        self.assertEqual(self.counts()['commit'],0)

    def test_active_backend_queue_is_refused_before_initialization(self):
        dispatch=self.fixture._dispatch
        def active(method,path,headers,body):
            status,value,binary=dispatch(method,path,headers,body)
            if path=='/info':value=dict(value,taskQueueCount=1)
            return status,value,binary
        self.fixture._dispatch=active
        with self.assertRaises(PipelineHeld):self.advance()
        self.assertEqual(self.counts()['initialize'],0)
        self.assertIsNone(self.stage.lookup(self.intent()))

    def test_guard_drift_during_input_resolution_aborts_before_http_mutations(self):
        resolve=self.inputs.resolve
        def drift(intent):
            paths=resolve(intent);self.guard.bad_return=True;return paths
        self.inputs.resolve=drift
        with self.assertRaises(PipelineHeld):self.advance()
        self.assertEqual(self.counts()['initialize'],0)
        with closing(sqlite3.connect(self.stage.path)) as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_stage_executions').fetchone()[0],0)

    def test_owner_loss_during_input_resolution_aborts_before_http_mutations(self):
        resolve=self.inputs.resolve
        def drift(intent):
            paths=resolve(intent);self.owner.__exit__(None,None,None);return paths
        self.inputs.resolve=drift
        with self.assertRaises(PipelineHeld):self.advance()
        self.assertEqual(self.counts()['initialize'],0)

    def test_same_image_container_drift_during_input_resolution_is_not_adopted(self):
        resolve=self.inputs.resolve
        def drift(intent):
            paths=resolve(intent);self.docker.container='d'*64;return paths
        self.inputs.resolve=drift
        with self.assertRaises(PipelineHeld):self.advance()
        self.assertEqual(self.counts()['initialize'],0)
        with closing(sqlite3.connect(self.stage.path)) as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_stage_executions').fetchone()[0],0)

    def test_same_version_image_drift_during_input_resolution_aborts_before_http(self):
        resolve=self.inputs.resolve
        def drift(intent):
            paths=resolve(intent);self.docker.image='sha256:'+'b'*64;return paths
        self.inputs.resolve=drift
        with self.assertRaises(PipelineHeld):self.advance()
        self.assertEqual(self.counts()['initialize'],0)

    def test_process_identity_drift_during_input_resolution_aborts_before_http(self):
        resolve=self.inputs.resolve
        def drift(intent):
            paths=resolve(intent);self.stage._pid+=1;return paths
        self.inputs.resolve=drift
        with self.assertRaises(PipelineHeld):self.advance()
        self.assertEqual(self.counts()['initialize'],0)

    def test_mutation_client_origin_cannot_drift_behind_unchanged_engine_origin(self):
        self.native.client.origin='http://127.0.0.1:1'
        with self.assertRaises(PipelineHeld):self.advance()
        self.assertEqual(self.counts()['initialize'],0)
        with closing(sqlite3.connect(self.stage.path)) as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_stage_executions').fetchone()[0],0)

    def test_lost_pre_mutation_commit_ack_never_issues_initialization(self):
        commit=self.stage._commit
        def lose(db):
            pending=db.execute("SELECT 1 FROM native_stage_executions WHERE phase='initialize_pending'").fetchone()
            commit(db)
            if pending:raise OSError('lost durable intent acknowledgement')
        with patch.object(self.stage,'_commit',side_effect=lose):
            with self.assertRaises(PipelineHeld):self.advance()
        self.stage=self.open()
        with self.assertRaises(NativeStageHeld):self.stage.tick(self.claim['jobId'])
        self.assertEqual(self.counts()['initialize'],0);self.assertIsNone(self.stage.lookup(self.intent()))

    def test_lost_sealed_proof_commit_ack_recovers_lookup_without_new_requests(self):
        self.advance();commit=self.stage._commit
        def lose(db):
            ready=db.execute("SELECT 1 FROM native_stage_executions WHERE phase='terminal_proof'").fetchone()
            commit(db)
            if ready:raise OSError('lost durable proof acknowledgement')
        with patch.object(self.stage,'_commit',side_effect=lose):
            with self.assertRaises(NativeStageHeld):self.stage.tick(self.claim['jobId'])
        self.stage=self.open();before=self.counts()
        self.assertEqual(self.stage.lookup(self.intent())['outcome'],'succeeded');self.assertEqual(self.counts(),before)
        self.assertEqual(self.reconcile()['state'],'waiting_treedar');self.assertEqual(self.counts()['download'],1)

    def test_uncertain_or_foreign_archive_bytes_are_never_relabelled_or_removed(self):
        self.advance();part=self.ledger.root/'jobs/parent-reservation/odm-stage/.archive.part';part.write_bytes(b'retain-me')
        with self.assertRaises(NativeStageHeld):self.stage.tick(self.claim['jobId'])
        self.assertEqual(part.read_bytes(),b'retain-me');self.assertEqual(self.counts()['download'],0)
        self.assertIsNone(self.stage.lookup(self.intent()))

    def test_duplicate_stage_owner_and_forked_handles_cannot_execute(self):
        with self.stage._operation():
            with self.assertRaises(NativeStageHeld):self.open()
        child=os.fork()
        if child==0:
            try:
                try:self.stage.lookup({'parentBinding':{'jobId':self.claim['jobId']}})
                except NativeStageHeld:os._exit(0)
                os._exit(3)
            except BaseException:os._exit(4)
        _,status=os.waitpid(child,0);self.assertEqual(os.waitstatus_to_exitcode(status),0)
        self.assertEqual(self.counts()['initialize'],0)

    def test_foreign_sqlite_refused_without_overwriting_existing_state(self):
        path=self.root/'foreign.sqlite'
        with closing(sqlite3.connect(path)) as db,db:db.execute('CREATE TABLE retained(value TEXT)');db.execute("INSERT INTO retained VALUES('keep')")
        path.chmod(0o600);before=path.read_bytes()
        with self.assertRaises(NativeStageHeld):
            NativeODMStage(path,self.runtime,journal=self.journal,ledger=self.ledger,engine=self.native,parent_guard=self.guard,state_owner=self.owner,input_resolver=self.inputs)
        self.assertEqual(before,path.read_bytes())

    def test_failed_state_owner_or_receipt_cannot_mutate_native_engine(self):
        self.owner.__exit__(None,None,None)
        with self.assertRaises(Exception):self.advance()
        self.assertEqual(self.counts()['initialize'],0)
