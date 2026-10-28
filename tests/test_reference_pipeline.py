"""Synthetic stage callbacks; real parent Queue, scratch, fence and SQLite.

No real engine, NAS acquisition, LiDAR geometry or production capability is
exercised. Fake terminal evidence tests only the intermediate metadata contract.
"""
from contextlib import closing
from dataclasses import replace
import copy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from processing import reference_pipeline as module,reference_protocol as p
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.scratch_policy import ScratchLedger


class SyntheticStage:
    def __init__(self,journal,stage,root):
        self.journal,self.stage=journal,stage;self.binding=journal.adapter_binding(stage)
        self.path=root/('synthetic-'+stage+'.sqlite');self.lose=False;self.pending=False;self.invalid=None;self.verification_error=False
        self.lookups=0;self.after_start=None
        with closing(sqlite3.connect(self.path)) as db,db:
            db.execute('CREATE TABLE IF NOT EXISTS actions(attempt TEXT PRIMARY KEY,intent TEXT NOT NULL,proof TEXT)')
        self.path.chmod(0o600)

    def proof(self,intent,*,outcome='succeeded',objects=None):
        parent,stage=intent['parentBinding'],intent['stage']
        if objects is None:objects=[dict(id=self.stage+'-report',role='report',byteSize=3,sha256=hashlib.sha256(b'abc').hexdigest())] if outcome=='succeeded' else []
        return module.seal_stage_proof(dict(schemaVersion=1,planSha256=intent['planSha256'],intentSha256=intent['intentSha256'],
            jobId=parent['jobId'],generation=parent['generation'],environment=parent['environment'],inputManifestSha256=parent['manifestSha256'],
            pipelineDigest=parent['pipelineDigest'],stageId=self.stage,engineVersion=stage['engineVersion'],runtimeSha256=stage['runtimeSha256'],
            inputObjectIds=[obj['id'] for obj in stage['inputObjects']],engineTerminal=True,outcome=outcome,objects=objects,
            errorCode=None if outcome=='succeeded' else 'engine_'+outcome))

    def start(self,intent):
        # A separate read-only connection sees FULL-committed intent before the
        # synthetic backend records its one mutation attempt.
        with closing(sqlite3.connect(self.journal.path.as_uri()+'?mode=ro',uri=True)) as db:
            row=db.execute('SELECT state,intent FROM pipeline_stages WHERE job=? AND stage=?',
                (intent['parentBinding']['jobId'],self.stage)).fetchone()
        assert row==('issued',module.encoded(intent))
        proof=None if self.pending else self.proof(intent)
        with closing(sqlite3.connect(self.path)) as db,db:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('INSERT INTO actions VALUES(?,?,?)',(intent['attemptId'],module.encoded(intent),module.encoded(proof) if proof else None))
        if self.after_start:self.after_start()
        if self.lose:raise OSError('synthetic lost engine reply')
        return self.invalid(intent,proof) if self.invalid else proof

    def lookup(self,intent):
        self.lookups+=1
        with closing(sqlite3.connect(self.path)) as db:
            row=db.execute('SELECT intent,proof FROM actions WHERE attempt=?',(intent['attemptId'],)).fetchone()
        if row is None:return None
        assert row[0]==module.encoded(intent)
        return json.loads(row[1]) if row[1] else None

    def verify(self,intent,proof):
        if self.verification_error or self.lookup(intent)!=proof:raise ValueError('synthetic verification unavailable')

    def complete(self,outcome='succeeded',*,objects=None):
        with closing(sqlite3.connect(self.path)) as db,db:
            row=db.execute('SELECT attempt,intent FROM actions').fetchone();intent=json.loads(row[1])
            db.execute('UPDATE actions SET proof=? WHERE attempt=?',(module.encoded(self.proof(intent,outcome=outcome,objects=objects)),row[0]))

    def calls(self):
        with closing(sqlite3.connect(self.path)) as db:return db.execute('SELECT COUNT(*) FROM actions').fetchone()[0]


class ActualGuard:
    def __init__(self,host):self.host=host;self.binding=host.journal.guard_binding();self.bad_return=False
    def check(self,parent,*,mutation):
        host=self.host
        with host.queue._db() as db:
            row=db.execute('SELECT * FROM jobs WHERE id=?',(parent['jobId'],)).fetchone()
            claim=db.execute('SELECT * FROM reference_claims WHERE job_id=?',(parent['jobId'],)).fetchone()
            allowed=row['status']=='running' or not mutation and row['status']=='held' and row['held_reason']=='coordinator_restart'
            assert allowed and row['claim_hash']==parent['claimTokenSha256'] and row['node_id']=='main' and row['worker_id']==parent['workerId']
            assert claim['request_id']==parent['claimRequestId'] and claim['instance_id']==parent['workerInstanceId']
        with host.ledger._transaction() as db:
            reservation=host.ledger._reservation(db,parent['reservationId']);settings=json.loads(reservation['settings'])
            assert (reservation['job'],reservation['generation'],reservation['environment'])==(parent['jobId'],parent['generation'],parent['environment'])
            assert settings['manifest_sha256']==parent['manifestSha256'] and settings['input_bytes']==parent['inputBytes'] and settings['output_budget']==parent['outputBudget']
            assert reservation['state'] in ('reserved','engine_started')
        with NodeGate(host.policy.engine_id) as gate:
            state=gate.snapshot();held=state['reservation']
            assert state['generation']==parent['gateGeneration'] and state['authorityId']==parent['authorityId'] and state['coordinatorOrigin']==parent['coordinatorOrigin']
            assert held['receiptId']==parent['gateReceiptId'] and held['completionDigest'] is None
            assert held['job']==dict(id=parent['jobId'],workerId=parent['workerId'],nodeId='main',authorityId=parent['authorityId'])
        return True if self.bad_return else None


@unittest.skipUnless(os.name=='posix','main-host journal/fence ownership requires Linux')
class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700)
        self.queue=Queue(self.root/'queue',node_ids=('cluster','main'),treedar_node_ids=('main',),reference_enabled=True)
        self.addCleanup(self.queue.close);self.store=ReferenceStore(self.queue)
        raw=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
        raw['manifest']['objects'].append(dict(id='original-lidar',name='original.las',byteSize=3,sha256=hashlib.sha256(b'abc').hexdigest(),kind='lidar',mediaType='application/octet-stream'))
        raw['manifest']['objectCount']=len(raw['manifest']['objects']);raw['manifest']['totalBytes']+=3
        raw['manifest']['inputCounts']['lidarObjects']=1;raw['manifest']['inputCounts']['points']=10
        raw['manifest']['sha256']=p.manifest_sha256(raw['manifest'])
        raw['pipeline']['treedar']=dict(requested=True,inputObjectIds=['original-lidar'],provenanceReceiptId='source-receipt')
        checked=p.validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint'],available_engines=('odm','treedar'),allowed_odm_options=set(raw['pipeline']['odm']['options']))
        public,_=self.store.submit(checked)
        self.claim=self.store.claim(public['jobId'],node_id='main',worker_id='main-worker',worker_instance_id='instance-one',claim_request_id='pipeline-claim')
        self.policy=module.PipelinePolicy(worker_id='main-worker',worker_instance_id='instance-one',authority_id=self.queue.authority_id,
            engine_id='physical-main',coordinator_origin='http://127.0.0.1:18080',gate_generation=1,odm_version='3.8.5',treedar_version='0.0.1',
            odm_runtime_sha256='1'*64,treedar_runtime_sha256='2'*64,provenance_policy_sha256='3'*64,odm_output_budget=100,treedar_output_budget=100)
        self.gates=patch('processing.node_gate.canonical_gate_path',return_value=self.root/'gate');self.gates.start();self.addCleanup(self.gates.stop)
        with NodeGate(self.policy.engine_id) as gate:
            gate.provision('main',self.policy.coordinator_origin,self.policy.authority_id)
            gate.reserve('main',self.policy.coordinator_origin,self.policy.authority_id,1,'a'*32)
            gate.bind('a'*32,dict(id=self.claim['jobId'],workerId=self.policy.worker_id,nodeId='main',authorityId=self.policy.authority_id))
        scratch=self.root/'scratch';scratch.mkdir(mode=0o700)
        self.ledger=ScratchLedger(self.root/'scratch.sqlite',scratch,capacity_probe=lambda _:1000000,safety_floor_bytes=0)
        self.ledger.reserve(job_id=self.claim['jobId'],generation=1,reservation_id='parent-reservation',manifest_sha256=self.claim['manifestSha256'],
            input_bytes=checked['manifest']['totalBytes'],output_budget=200,environment='staging',ttl_seconds=600,worker_ready=True)
        self.receipt=dict(schemaVersion=1,receiptId='source-receipt',jobId=self.claim['jobId'],generation=1,environment='staging',
            objectId='original-lidar',objectSha256=hashlib.sha256(b'abc').hexdigest(),byteSize=3,sourceKind='lidar',verticalUnits='metre',verticalDatum='navd88')
        self.provenance_current=True
        self.path=self.root/'pipeline.sqlite';self.journal=self.open()
        self.guard=ActualGuard(self)
        self.plan=self.bind()
        self.odm=SyntheticStage(self.journal,'odm',self.root);self.tree=SyntheticStage(self.journal,'treedar',self.root)

    def open(self,policy=None):
        return module.PipelineJournal(self.path,policy or self.policy,verify_provenance=lambda receipt:self.provenance_current and receipt==self.receipt)

    def bind(self,claim=None,**kwargs):
        return self.journal.bind_parent(claim or self.claim,claim_request_id=kwargs.get('claim_request_id','pipeline-claim'),
            reservation_id=kwargs.get('reservation_id','parent-reservation'),gate_receipt_id=kwargs.get('gate_receipt_id','a'*32),guard=self.guard)

    def advance(self,stage):
        return self.journal.advance(self.claim['jobId'],stage,self.odm if stage=='odm' else self.tree,self.guard,
            provenance=self.receipt if stage=='treedar' else None)

    def test_one_parent_original_input_selection_and_one_final_existing_wire_closure(self):
        self.assertEqual([stage['stageId'] for stage in self.plan['stages']],['odm','treedar'])
        self.assertEqual([obj['id'] for obj in self.plan['stages'][1]['inputObjects']],['original-lidar'])
        self.assertTrue(all(obj['kind']=='image' for obj in self.plan['stages'][0]['inputObjects']))
        self.assertEqual(self.plan['parentBinding']['manifestSha256'],self.claim['manifestSha256'])
        self.assertNotIn(self.claim['claimToken'].encode(),self.path.read_bytes())
        with self.assertRaises(module.PipelineHeld):self.advance('treedar')
        self.assertEqual(self.advance('odm')['state'],'waiting_treedar')
        with self.assertRaises(module.PipelineHeld):self.journal.final_result(self.claim['jobId'])
        self.assertEqual(self.advance('treedar')['state'],'ready_final')
        final=self.journal.final_result(self.claim['jobId'])
        self.assertEqual(final['engineVersions'],{'odm':'3.8.5','treedar':'0.0.1'})
        self.assertEqual([obj['id'] for obj in final['objects']],['odm-report','treedar-report'])
        self.assertEqual(final['pipelineDigest'],self.claim['pipelineDigest'])
        p.validate_result_manifest(final,job_id=self.claim['jobId'],generation=1,environment='staging',pipeline_digest=self.claim['pipelineDigest'])
        # Internal closure is not an external completion or cleanup permission.
        self.guard.check(self.plan['parentBinding'],mutation=True)
        self.assertEqual(self.queue.get(self.claim['jobId'])['status'],'running')

    def test_duplicate_parent_and_completed_stage_do_not_replay_callbacks(self):
        self.assertEqual(self.bind(),self.plan)
        self.advance('odm');self.advance('odm');self.advance('treedar');self.advance('treedar')
        self.assertEqual((self.odm.calls(),self.tree.calls()),(1,1))
        result=self.journal.final_result(self.claim['jobId']);result['objects'][0]['sha256']='0'*64
        self.assertNotEqual(self.journal.final_result(self.claim['jobId']),result)

    def test_lost_start_reply_restart_uses_lookup_only_same_exact_attempt(self):
        self.odm.lose=True
        with self.assertRaises(module.PipelineHeld):self.advance('odm')
        self.journal=self.open();self.odm.journal=self.journal
        with self.assertRaises(module.PipelineHeld):self.advance('odm')
        self.assertEqual(self.journal.reconcile(self.claim['jobId'],'odm',self.odm,self.guard)['state'],'waiting_treedar')
        self.assertEqual(self.odm.calls(),1)
        self.advance('treedar');self.assertEqual(self.tree.calls(),1)

    def test_pending_and_missing_lookup_never_authorize_relaunch_or_next_stage(self):
        self.odm.pending=True;self.assertEqual(self.advance('odm')['state'],'executing')
        for _ in range(2):self.assertEqual(self.journal.reconcile(self.claim['jobId'],'odm',self.odm,self.guard)['state'],'executing')
        with self.assertRaises(module.PipelineHeld):self.advance('odm')
        with self.assertRaises(module.PipelineHeld):self.advance('treedar')
        self.assertEqual((self.odm.calls(),self.tree.calls()),(1,0))
        self.odm.complete();self.journal.reconcile(self.claim['jobId'],'odm',self.odm,self.guard)
        self.assertEqual(self.advance('treedar')['state'],'ready_final')

    def test_lost_intent_commit_ack_can_hold_before_callback_without_replay(self):
        commit=self.journal._commit
        def lost(db):commit(db);raise OSError('synthetic lost intent commit ACK')
        # _read uses read transactions too; fail only once the intent exists.
        def conditional(db):
            issued=db.execute("SELECT 1 FROM pipeline_stages WHERE state='issued'").fetchone()
            if issued:lost(db)
            else:commit(db)
        with patch.object(self.journal,'_commit',side_effect=conditional):
            with self.assertRaises(OSError):self.advance('odm')
        self.assertEqual(self.odm.calls(),0);self.journal=self.open()
        with self.assertRaises(module.PipelineHeld):self.advance('odm')
        self.assertEqual(self.journal.reconcile(self.claim['jobId'],'odm',self.odm,self.guard)['state'],'executing')
        self.assertEqual(self.odm.calls(),0)

    def test_lost_stage_proof_commit_ack_is_atomic_and_does_not_repeat_engine(self):
        commit=self.journal._commit
        def lost(db):
            complete=db.execute("SELECT 1 FROM pipeline_stages WHERE state='succeeded'").fetchone()
            commit(db)
            if complete:raise OSError('synthetic lost proof commit ACK')
        with patch.object(self.journal,'_commit',side_effect=lost):
            with self.assertRaises(OSError):self.advance('odm')
        self.journal=self.open();self.assertEqual(self.journal.status(self.claim['jobId'])['state'],'waiting_treedar')
        self.advance('odm');self.assertEqual(self.odm.calls(),1)

    def test_stage_failure_stops_sequence_and_does_not_fabricate_partial_result(self):
        self.odm.pending=True;self.advance('odm');self.odm.complete('failed')
        self.assertEqual(self.journal.reconcile(self.claim['jobId'],'odm',self.odm,self.guard)['state'],'failed')
        with self.assertRaises(module.PipelineHeld):self.advance('treedar')
        with self.assertRaises(module.PipelineHeld):self.journal.final_result(self.claim['jobId'])
        failure=self.journal.terminal_failure(self.claim['jobId'])
        self.assertEqual((failure['terminalState'],failure['pipelineDigest']),('failed',self.claim['pipelineDigest']))
        self.assertEqual(self.tree.calls(),0);self.guard.check(self.plan['parentBinding'],mutation=True)

    def test_second_failure_retains_first_proof_but_never_offers_partial_success(self):
        self.advance('odm');self.tree.pending=True;self.advance('treedar');self.tree.complete('cancelled')
        self.assertEqual(self.journal.reconcile(self.claim['jobId'],'treedar',self.tree,self.guard)['state'],'cancelled')
        with self.assertRaises(module.PipelineHeld):self.journal.final_result(self.claim['jobId'])
        self.assertEqual(self.journal.status(self.claim['jobId'])['stages']['odm'],'succeeded')
        self.assertEqual(self.journal.terminal_failure(self.claim['jobId'])['terminalState'],'cancelled')

    def test_revoked_synthetic_or_relabelled_provenance_refuses_before_tree_intent(self):
        self.advance('odm')
        self.provenance_current=False
        with self.assertRaises(module.PipelineHeld):self.advance('treedar')
        self.provenance_current=True
        for key,value in (('sourceKind','synthetic_lidar'),('objectId','odm-report'),('objectSha256','f'*64),('receiptId','other-receipt')):
            receipt=dict(self.receipt,**{key:value})
            with self.subTest(key=key),self.assertRaises(module.PipelineHeld):
                self.journal.advance(self.claim['jobId'],'treedar',self.tree,self.guard,provenance=receipt)
        self.assertEqual(self.tree.calls(),0)
        self.assertEqual(self.journal.status(self.claim['jobId'])['stages']['treedar'],'new')

    def test_bad_guard_boolean_or_lost_fence_cannot_authorize_stage_mutation(self):
        self.guard.bad_return=True
        with self.assertRaises(module.PipelineHeld):self.advance('odm')
        self.guard.bad_return=False
        with self.assertRaises(module.PipelineHeld):self.journal.advance(self.claim['jobId'],'odm',self.odm,True)
        self.assertEqual(self.odm.calls(),0)
        self.odm.after_start=lambda:setattr(self.guard,'bad_return',True)
        with self.assertRaises(module.PipelineHeld):self.advance('odm')
        self.assertEqual(self.odm.calls(),1)
        self.guard.bad_return=False;self.journal.reconcile(self.claim['jobId'],'odm',self.odm,self.guard)
        self.assertEqual(self.odm.calls(),1)

    def test_namespace_parent_runtime_terminal_and_budget_proof_mismatches_are_not_sealed(self):
        def wrong(intent,proof):
            value=dict(proof);value.pop('proofSha256');value['objects']=[dict(id='treedar-foreign',role='report',byteSize=3,sha256='a'*64)]
            return module.seal_stage_proof(value)
        self.odm.invalid=wrong
        with self.assertRaises(module.PipelineHeld):self.advance('odm')
        self.assertEqual(self.journal.status(self.claim['jobId'])['stages']['odm'],'issued')
        self.journal.reconcile(self.claim['jobId'],'odm',self.odm,self.guard)
        self.tree.pending=True;self.advance('treedar')
        with closing(sqlite3.connect(self.tree.path)) as db:
            intent=json.loads(db.execute('SELECT intent FROM actions').fetchone()[0])
        proof=self.tree.proof(intent)
        for key,value in (('generation',2),('environment','production'),('pipelineDigest','f'*64),('engineVersion','other'),('engineTerminal',False),('inputObjectIds',['odm-report'])):
            changed=dict(proof);changed.pop('proofSha256');changed[key]=value
            with self.subTest(key=key),self.assertRaises(module.PipelineHeld):module.validate_stage_proof(module.seal_stage_proof(changed),intent=intent,policy=self.policy)
        oversized=self.tree.proof(intent,objects=[dict(id='treedar-too-big',role='report',byteSize=101,sha256='a'*64)])
        with self.assertRaises(module.PipelineHeld):module.validate_stage_proof(oversized,intent=intent,policy=self.policy)

    def test_parent_and_runtime_policy_bindings_cannot_change_on_restart(self):
        for changed in (dict(self.claim,claimToken='z'*43),dict(self.claim,generation=2),dict(self.claim,environment='production')):
            with self.assertRaises((module.PipelineHeld,p.ProtocolError)):self.bind(changed)
        for changed in (replace(self.policy,odm_version='new-version'),replace(self.policy,treedar_output_budget=101),replace(self.policy,provenance_policy_sha256='f'*64)):
            with self.assertRaises(module.PipelineHeld):self.open(changed)
        with self.assertRaises(module.PipelineHeld):self.bind(gate_receipt_id='b'*32)
        self.assertEqual((self.odm.calls(),self.tree.calls()),(0,0))

    def test_stage_proof_identity_is_json_type_exact(self):
        self.odm.pending=True;self.advance('odm')
        with closing(sqlite3.connect(self.odm.path)) as db:
            intent=json.loads(db.execute('SELECT intent FROM actions').fetchone()[0])
        for generation in (True,1.0):
            proof=self.odm.proof(intent);proof.pop('proofSha256');proof['generation']=generation
            with self.subTest(generation=generation),self.assertRaises(module.PipelineHeld):
                module.validate_stage_proof(module.seal_stage_proof(proof),intent=intent,policy=self.policy)
        proof=self.odm.proof(intent);proof.pop('proofSha256');proof['schemaVersion']=True
        with self.assertRaises(module.PipelineHeld):
            module.validate_stage_proof(module.seal_stage_proof(proof),intent=intent,policy=self.policy)

    def test_each_stage_reserves_parent_object_count_and_utf8_metadata_space(self):
        self.odm.pending=True;self.advance('odm')
        with closing(sqlite3.connect(self.odm.path)) as db:
            intent=json.loads(db.execute('SELECT intent FROM actions').fetchone()[0])
        def objects(count,role='report'):
            return [dict(id='odm-'+str(index).zfill(6)+'x'*115,role=role,byteSize=0,sha256='a'*64) for index in range(count)]
        count_overflow=self.odm.proof(intent,objects=objects(intent['stage']['outputObjectLimit']+1))
        with self.assertRaises(module.PipelineHeld):
            module.validate_stage_proof(count_overflow,intent=intent,policy=self.policy)
        metadata_overflow=self.odm.proof(intent,objects=objects(4000,'\U0001f333'*80))
        self.assertGreater(len(p.canonical_json(metadata_overflow['objects']))-2,intent['stage']['outputMetadataBudget'])
        self.assertLess(len(p.canonical_json(metadata_overflow)),p.MAX_BODY_BYTES)
        with self.assertRaises(module.PipelineHeld):
            module.validate_stage_proof(metadata_overflow,intent=intent,policy=self.policy)
        accepted=self.odm.proof(intent,objects=objects(1000,'\U0001f333'*80))
        self.assertEqual(module.validate_stage_proof(accepted,intent=intent,policy=self.policy),accepted)

    def test_two_maximum_count_stage_closures_fit_one_existing_wire_manifest(self):
        count=p.MAX_OBJECTS//2
        for stage,adapter in (('odm',self.odm),('treedar',self.tree)):
            adapter.pending=True;self.advance(stage)
            objects=[dict(id=stage+'-'+str(index).zfill(6)+'x'*70,role='\U0001f333'*40,byteSize=0,sha256='a'*64) for index in range(count)]
            adapter.complete(objects=objects)
            self.journal.reconcile(self.claim['jobId'],stage,adapter,self.guard)
        final=self.journal.final_result(self.claim['jobId'])
        self.assertEqual(len(final['objects']),p.MAX_OBJECTS)
        self.assertLessEqual(len(p.canonical_json(final)),p.MAX_BODY_BYTES)
        p.validate_result_manifest(final,job_id=self.claim['jobId'],generation=1,environment='staging',pipeline_digest=self.claim['pipelineDigest'])
        self.assertEqual((self.odm.calls(),self.tree.calls()),(1,1))

    def test_journal_refuses_foreign_sqlite_without_modifying_its_bytes(self):
        path=self.root/'foreign.sqlite'
        with closing(sqlite3.connect(path)) as db,db:
            db.execute('CREATE TABLE foreign_progress(value TEXT)');db.execute("INSERT INTO foreign_progress VALUES('retained')")
        path.chmod(0o600);before=path.read_bytes()
        with self.assertRaisesRegex(module.PipelineHeld,'dedicated_pipeline_journal_required'):
            module.PipelineJournal(path,self.policy,verify_provenance=lambda _:True)
        self.assertEqual(path.read_bytes(),before)
        with closing(sqlite3.connect(path)) as db:
            self.assertEqual(db.execute('SELECT value FROM foreign_progress').fetchall(),[('retained',)])
            self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(),[('foreign_progress',)])

    def test_symlink_path_refused_before_creating_outside_directory(self):
        outside=self.root/'outside';outside.mkdir(mode=0o700)
        alias=self.root/'alias';alias.symlink_to(outside,target_is_directory=True)
        with self.assertRaises(module.PipelineHeld):
            module.PipelineJournal(alias/'not-created'/'journal.sqlite',self.policy,verify_provenance=lambda _:True)
        self.assertFalse((outside/'not-created').exists())

    def test_missing_nested_state_root_is_refused_before_any_file_or_directory_creation(self):
        before=set(self.root.iterdir());path=self.root/'missing'/'nested'/'journal.sqlite'
        with patch.object(module.PipelineJournal,'_create',side_effect=AssertionError('must not create files')) as create:
            with self.assertRaisesRegex(module.PipelineHeld,'preexisting_private_stage_root_required'):
                module.PipelineJournal(path,self.policy,verify_provenance=lambda _:True)
            create.assert_not_called()
        self.assertEqual(set(self.root.iterdir()),before)
        self.assertFalse((self.root/'missing').exists())

    def test_preexisting_private_root_and_parent_are_synced_before_constructor_returns(self):
        actual_fsync=os.fsync;observed=[]
        def record(fd):
            info=os.fstat(fd);observed.append((info.st_dev,info.st_ino));return actual_fsync(fd)
        with patch.object(module.os,'fsync',side_effect=record):
            module.PipelineJournal(self.root/'second.sqlite',self.policy,verify_provenance=lambda _:True)
        for directory in (self.root,self.root.parent):
            info=directory.stat();self.assertIn((info.st_dev,info.st_ino),observed)

    def test_private_file_identity_and_real_exclusive_lease_are_required(self):
        with self.journal._operation():
            with self.assertRaises(module.PipelineBusy):self.open()
            with self.assertRaises(module.PipelineBusy):self.journal.status(self.claim['jobId'])
        self.path.chmod(0o644)
        with self.assertRaises(module.PipelineHeld):self.journal.status(self.claim['jobId'])
        self.path.chmod(0o600)
        alias=self.root/'hardlink';os.link(self.path,alias)
        with self.assertRaises(module.PipelineHeld):self.journal.status(self.claim['jobId'])
        alias.unlink()
        self.assertEqual(self.journal.status(self.claim['jobId'])['state'],'bound')

    def test_forked_process_cannot_use_inherited_owner_or_compete_with_live_lease(self):
        with self.journal._operation():
            child=os.fork()
            if child==0:
                try:
                    try:self.journal.status(self.claim['jobId'])
                    except module.PipelineHeld:pass
                    else:os._exit(3)
                    try:self.open()
                    except module.PipelineBusy:pass
                    else:os._exit(4)
                    os._exit(0)
                except BaseException:os._exit(5)
            _,status=os.waitpid(child,0)
            self.assertEqual(os.waitstatus_to_exitcode(status),0)
        self.assertEqual((self.odm.calls(),self.tree.calls()),(0,0))

    def test_readonly_held_restart_reconciliation_never_permits_a_new_stage(self):
        self.odm.lose=True
        with self.assertRaises(module.PipelineHeld):self.advance('odm')
        with self.queue._db() as db:db.execute("UPDATE jobs SET status='held',held_reason='coordinator_restart' WHERE id=?",(self.claim['jobId'],))
        self.journal=self.open()
        self.assertEqual(self.journal.reconcile(self.claim['jobId'],'odm',self.odm,self.guard)['state'],'waiting_treedar')
        with self.assertRaises(module.PipelineHeld):self.advance('treedar')
        self.assertEqual((self.odm.calls(),self.tree.calls()),(1,0))

    def test_completed_proof_corruption_is_not_hidden_by_a_state_label(self):
        self.advance('odm')
        with closing(sqlite3.connect(self.path)) as db,db:
            row=db.execute("SELECT proof FROM pipeline_stages WHERE stage='odm'").fetchone();proof=json.loads(row[0]);proof['objects'][0]['sha256']='f'*64
            db.execute("UPDATE pipeline_stages SET proof=? WHERE stage='odm'",(module.encoded(proof),))
        with self.assertRaises(module.PipelineHeld):self.journal.status(self.claim['jobId'])

    def test_another_global_job_cannot_be_claimed_while_stage_pipeline_owns_parent(self):
        other,_=self.queue.submit(dict(requestId='another-job',environment='staging',profile='treedar-synthetic-v1'))
        # The foundation never completes the parent, including at ready_final.
        self.advance('odm');self.advance('treedar')
        self.assertIsNone(self.queue.claim(dict(workerId='cluster-worker',nodeId='cluster',profiles=['treedar-synthetic-v1'])))
        self.assertEqual(self.queue.get(other['id'])['status'],'queued')
