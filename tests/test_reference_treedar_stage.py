"""Synthetic Docker lifecycle; real prior ODM HTTP and actual parent ownership."""
from contextlib import closing
from dataclasses import replace
import copy
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import unittest
from unittest.mock import patch

from processing import reference_treedar_stage as m,reference_protocol as p
from processing.node_gate import NodeGate
from processing.reference_pipeline import PipelineJournal,PipelineHeld
from processing.reference_odm_stage import NativeODMStage,NativeRuntimeProbe
from processing.treedar_engine import TreeDarConfig,SOURCE_FILES,source_digest,TreeDarEngine
from tests import test_reference_odm_stage as odm
from tests.test_reference_pipeline import ActualGuard


class FakeDocker:
    def __init__(self,policy):self.policy=policy;self.calls=[];self.container=None;self.fail=None;self.drift=None;self.before=None
    def __call__(self,command,*,timeout):
        args=command[3:];self.calls.append(args);action=args[0]
        if self.before:self.before(action)
        c=self.policy.config
        if action=='info':body=c.engine_id
        elif action=='image':body=c.image_id
        elif action=='create':
            mounts=[]
            for i,value in enumerate(args):
                if value=='--mount':
                    fields=args[i+1].split(',');item=dict(s.split('=',1) for s in fields if '=' in s)
                    mounts.append(dict(Type='bind',Source=item['source'],Destination=item['target'],RW='readonly' not in fields))
            image_env=dict(value.split('=',1) for value in self.policy.image_environment)
            image_env.update(dict(args[i+1].split('=',1) for i,value in enumerate(args) if value=='--env'))
            env=[key+'='+value for key,value in image_env.items()]
            labels=dict(self.policy.image_labels)
            labels[args[args.index('--label')+1].split('=',1)[0]]=args[args.index('--label')+1].split('=',1)[1]
            self.container=dict(Id='d'*64,Name='/'+args[args.index('--name')+1],Image=c.image_id,
                Config=dict(Image=c.image_id,User=f'{c.uid}:{c.gid}',Entrypoint=['/usr/local/bin/python'],Cmd=['-B','-m','processing.treedar_engine','--inside'],
                    Labels=labels,WorkingDir='/',Env=env),
                HostConfig=dict(NetworkMode='none',ReadonlyRootfs=True,Privileged=False,CapDrop=['ALL'],CapAdd=None,SecurityOpt=['no-new-privileges'],
                    LogConfig={'Type':'none'},Devices=[],DeviceRequests=None,Binds=None,PidMode='',IpcMode='private',PortBindings={},
                    Tmpfs={'/tmp':'rw,nosuid,nodev,mode=1777'},RestartPolicy={'Name':'no'},AutoRemove=False,Memory=0,NanoCpus=0,CpuQuota=0,PidsLimit=None),
                Mounts=mounts,State=dict(Status='created',Running=False,ExitCode=0))
            body=self.container['Id']
        elif action=='start':self.container['State']=dict(Status='running',Running=True,ExitCode=0);body=self.container['Id']
        elif action=='inspect':
            if self.container is None:raise OSError('synthetic no container')
            value=copy.deepcopy(self.container)
            if self.drift:self.drift(value)
            body=json.dumps([value])
        else:raise AssertionError('unexpected Docker mutation '+action)
        if action==self.fail:raise subprocess.TimeoutExpired('synthetic lost reply',timeout)
        return subprocess.CompletedProcess(command,0,body,'')


class Inputs:
    def __init__(self,host):self.host=host;self.binding=host.journal.adapter_binding('treedar');self.before=None
    def resolve(self,intent):
        if self.before:self.before()
        return {obj['id']:self.host.input_paths[obj['id']] for obj in intent['stage']['inputObjects']}


class Provenance:
    def __init__(self,host,receipt):self.binding=host.journal.adapter_binding('treedar');self.receipt=receipt;self.valid=True;self.calls=0
    def read(self,intent):self.calls+=1;return copy.deepcopy(self.receipt) if self.valid else None


class Capture:
    def __init__(self,journal):self.binding=journal.adapter_binding('treedar');self.intent=None
    def start(self,intent):self.intent=intent;return None
    def lookup(self,intent):return None
    def verify(self,intent,proof):raise AssertionError('no fabricated v1 proof')


@unittest.skipUnless(os.name=='posix','Linux Docker ownership boundary')
class TreeStageTests(unittest.TestCase):
    def setUp(self):
        self.host=odm.NativeODMStageTests();self.host.setUp();self.addCleanup(self.host.doCleanups);h=self.host
        source=h.root/'source';(source/'processing').mkdir(parents=True,mode=0o700);source.chmod(0o700)
        for name in SOURCE_FILES:
            target=source/'processing'/name;shutil.copyfile(Path(__file__).resolve().parents[1]/'processing'/name,target);target.chmod(0o600)
        config=TreeDarConfig('/usr/bin/docker',str(h.root/'docker.sock'),'sha256:'+'b'*64,h.policy.engine_id,h.policy.authority_id,
            h.policy.coordinator_origin,1,h.policy.worker_id,str(source),source_digest(source),uid=os.geteuid(),gid=os.getegid())
        self.policy=m.TreeStagePolicy(config,max_raw_bytes=10000,max_files=getattr(self,'raw_file_limit',64),image_environment=getattr(self,'image_environment',()),image_labels=getattr(self,'image_labels',()))
        h.policy=replace(h.policy,treedar_runtime_sha256=self.policy.sha256,treedar_output_budget=50000)
        with closing(sqlite3.connect(h.ledger.path)) as db,db:
            row=db.execute('SELECT settings FROM reservations').fetchone();settings=json.loads(row[0]);settings['output_budget']=50100
            db.execute('UPDATE reservations SET settings=?,remaining=remaining+49900',(json.dumps(settings),))
        self.receipt=dict(schemaVersion=1,receiptId='original-source',jobId=h.claim['jobId'],generation=1,environment='staging',objectId='original-lidar',
            objectSha256=h.claim['submission']['manifest']['objects'][-1]['sha256'],byteSize=3,sourceKind='lidar',verticalUnits='metre',verticalDatum='navd88')
        self.authenticated=True
        h.journal=PipelineJournal(h.root/'tree-pipeline.sqlite',h.policy,verify_provenance=lambda receipt:self.authenticated and receipt==self.receipt)
        h.guard=ActualGuard(h);h.plan=h.journal.bind_parent(h.claim,claim_request_id='parent-claim',reservation_id='parent-reservation',gate_receipt_id='a'*32,guard=h.guard)
        h.inputs=odm.OriginalInputs(h)
        h.stage=NativeODMStage(h.root/'tree-prior.sqlite',h.runtime,journal=h.journal,ledger=h.ledger,engine=h.native,parent_guard=h.guard,state_owner=h.owner,
            input_resolver=h.inputs,runtime_probe=NativeRuntimeProbe(h.runtime,runner=h.docker))
        h.advance();h.stage.tick(h.claim['jobId']);h.reconcile()
        self.capture=Capture(h.journal);h.journal.advance(h.claim['jobId'],'treedar',self.capture,h.guard,provenance=self.receipt);self.intent=self.capture.intent
        self.inputs=Inputs(h);self.provenance=Provenance(h,self.receipt);self.docker=FakeDocker(self.policy);self.stage=self.open()

    def open(self):
        h=self.host
        return m.TreeDarNativeStage(h.root/'tree-native.sqlite',self.policy,journal=h.journal,ledger=h.ledger,parent_guard=h.guard,state_owner=h.owner,
            input_resolver=self.inputs,provenance_reader=self.provenance,prior_odm=h.stage,runner=self.docker)

    def begin(self):return self.stage.begin(self.intent)
    def actions(self):return [args[0] for args in self.docker.calls if args[0] in ('create','start','stop','rm')]
    def finish(self,exit_code=0):
        self.docker.container['State']=dict(Status='exited',Running=False,ExitCode=exit_code)
        output=Path(next(x['Source'] for x in self.docker.container['Mounts'] if x['Destination']=='/output'));run=output/'run';run.mkdir(mode=0o700)
        (run/'trees.csv').write_text('synthetic,native\n');(run/'trees.csv').chmod(0o600)
        (output/'proof.json').write_text('{}');(output/'proof.json').chmod(0o600)

    def test_real_prior_odm_and_one_tree_attempt_preserve_parent_everywhere(self):
        h=self.host;before=h.reservation()['allocated'];self.assertEqual(self.begin()['phase'],'processing');self.finish();evidence=self.stage.tick(h.claim['jobId'])
        self.assertEqual(evidence['nativeStatus'],'exited_unvalidated');self.assertEqual(evidence['adapterIntegration'],'held_pending_outer_v2')
        self.assertEqual(evidence,self.stage.read_evidence(self.intent));self.assertEqual(self.actions(),['create','start'])
        self.assertGreater(h.reservation()['allocated'],before);self.assertEqual(h.reservation()['state'],'engine_started')
        with h.ledger._transaction() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM results').fetchone()[0],0)
        with NodeGate(h.policy.engine_id) as gate:
            receipt=gate.snapshot()['reservation'];self.assertEqual(receipt['receiptId'],'a'*32);self.assertIsNone(receipt['completionDigest'])
        with self.assertRaises(PipelineHeld):h.journal.final_result(h.claim['jobId'])
        self.assertFalse(hasattr(self.stage,'start'));self.assertFalse(hasattr(self.stage,'lookup'));self.assertFalse(hasattr(self.stage,'verify'))
        task=json.loads(Path(next(x['Source'] for x in self.docker.container['Mounts'] if x['Destination']=='/control/task.json')).read_text())
        self.assertEqual(task['job'],{k:self.intent['parentBinding'][k] for k in ('jobId','generation','environment','manifestSha256','pipelineDigest')})
        self.assertEqual(task['inputObject']['id'],'original-lidar');self.assertFalse(task['allowSynthetic'])

    def test_lost_create_reply_only_inspects_and_never_starts_discovered_container(self):
        self.docker.fail='create'
        with self.assertRaises(m.TreeStageHeld):self.begin()
        self.stage=self.open();self.assertEqual(self.stage.tick(self.host.claim['jobId'])['observed'],'created')
        with self.assertRaises(m.TreeStageHeld):self.begin()
        self.assertEqual(self.actions(),['create'])

    def test_lost_start_reply_reconciles_running_without_replay(self):
        self.docker.fail='start'
        with self.assertRaises(m.TreeStageHeld):self.begin()
        self.stage=self.open();self.assertEqual(self.stage.tick(self.host.claim['jobId'])['observed'],'running')
        self.finish();self.stage.tick(self.host.claim['jobId'])
        with self.assertRaises(m.TreeStageHeld):self.begin()
        self.assertEqual(self.actions(),['create','start'])

    def test_actual_prior_artifact_corruption_blocks_before_docker_mutation(self):
        intent=self.host.intent();paths=self.host.stage.artifact_paths(intent);(self.host.ledger.root/paths['odm-archive']).write_bytes(b'corrupt')
        with self.assertRaises(Exception):self.begin()
        self.assertEqual(self.actions(),[])

    def test_unissued_or_rebound_intent_never_authorizes(self):
        for key,value in [('attemptId','00000000-0000-4000-8000-000000000000'),('precedingProofSha256','0'*64)]:
            wrong=dict(self.intent,**{key:value})
            with self.subTest(key=key),self.assertRaises(Exception):self.stage.begin(wrong)
        self.assertEqual(self.actions(),[])

    def test_provenance_and_parent_drift_after_input_hashing_block_mutation(self):
        self.inputs.before=lambda:setattr(self.provenance,'valid',False)
        with self.assertRaises(Exception):self.begin()
        self.assertEqual(self.actions(),[])

    def test_fresh_reader_equality_is_not_authentication(self):
        self.authenticated=False
        with self.assertRaises(Exception):self.begin()
        self.assertEqual(self.actions(),[])

    def test_read_evidence_is_sql_only_and_never_advances_or_probes(self):
        self.begin();self.finish();proof=self.stage.tick(self.host.claim['jobId']);paths=[self.stage.path,Path(self.host.ledger.path),self.host.journal.path]
        before=[path.read_bytes() for path in paths]
        with patch.object(self.stage,'_docker',side_effect=AssertionError('no Docker')),patch.object(self.host.ledger,'_transaction',side_effect=AssertionError('no ledger write')),patch.object(self.stage,'_db',wraps=self.stage._db) as database:
            self.assertEqual(self.stage.read_evidence(self.intent),proof);self.assertTrue(all(c.kwargs.get('readonly') is True for c in database.call_args_list))
        self.assertEqual(before,[path.read_bytes() for path in paths])

    def test_configuration_inspection_does_not_trust_binding_label(self):
        self.begin()
        changes=[lambda x:x.update(Image='sha256:'+'c'*64),lambda x:x['Config'].update(User='0:0'),lambda x:x['Config'].update(Cmd=['evil']),
            lambda x:x['HostConfig'].update(NetworkMode='host'),lambda x:x['HostConfig'].update(Privileged=True),lambda x:x['Mounts'][0].update(RW=True),
            lambda x:x['HostConfig'].update(RestartPolicy={'Name':'always'}),lambda x:x['Config'].update(Env=['PYTHONPATH=/evil'])]
        for change in changes:
            self.docker.drift=change
            with self.subTest(change=change),self.assertRaises(m.TreeStageHeld):self.stage.tick(self.host.claim['jobId'])
        self.assertEqual(self.actions(),['create','start'])

    def test_native_failure_retains_all_files_and_never_completes(self):
        self.begin();self.finish(1);evidence=self.stage.tick(self.host.claim['jobId']);self.assertEqual(evidence['exitCode'],1)
        self.assertEqual(self.host.reservation()['state'],'engine_started');self.assertEqual(self.actions(),['create','start'])

    def test_existing_bootstrap_pins_and_state_owner_drift_hold(self):
        target=Path(self.policy.config.source_root)/'processing'/'treedar_engine.py';target.write_text('changed')
        with self.assertRaises(m.TreeStageHeld):self.begin()
        self.assertEqual(self.actions(),[])
        with patch.object(self.stage,'_pid',-1),self.assertRaises(m.TreeStageHeld):self.stage.read_evidence(self.intent)

    def test_native_budget_failure_retains_no_cleanup(self):
        self.begin();self.finish();output=Path(next(x['Source'] for x in self.docker.container['Mounts'] if x['Destination']=='/output'))
        (output/'run'/'huge').write_bytes(b'x'*10001)
        with self.assertRaises(Exception):self.stage.tick(self.host.claim['jobId'])
        self.assertTrue((output/'run'/'huge').exists());self.assertEqual(self.host.reservation()['state'],'engine_started')

    def test_running_files_are_charged_monotonically_without_finalizing(self):
        self.begin();before=self.host.reservation()['allocated'];output=Path(next(x['Source'] for x in self.docker.container['Mounts'] if x['Destination']=='/output'))
        run=output/'run';run.mkdir(mode=0o755);path=run/'growing.tif';path.write_bytes(b'x'*1000);path.chmod(0o644)
        self.assertEqual(self.stage.tick(self.host.claim['jobId'])['observed'],'running');high=self.host.reservation()['allocated'];self.assertGreater(high,before)
        path.write_bytes(b'x');self.stage.tick(self.host.claim['jobId']);self.assertEqual(self.host.reservation()['allocated'],high)
        self.assertIsNone(self.stage.read_evidence(self.intent));self.assertEqual(self.actions(),['create','start'])

    def test_standalone_lifecycle_methods_are_never_called(self):
        with patch.object(TreeDarEngine,'start',side_effect=AssertionError('no standalone start')),patch.object(TreeDarEngine,'poll',side_effect=AssertionError('no standalone poll')),patch.object(TreeDarEngine,'_collect',side_effect=AssertionError('no legacy collection')),patch.object(TreeDarEngine,'_settle',side_effect=AssertionError('no final settlement')):
            self.begin();self.finish();self.stage.tick(self.host.claim['jobId'])

    def test_runtime_and_owner_changes_during_resolver_hold_before_create(self):
        self.inputs.before=lambda:setattr(self.host.guard,'bad_return',True)
        with self.assertRaises(Exception):self.begin()
        self.assertEqual(self.actions(),[])

    def test_fresh_guard_runs_outside_physical_gate(self):
        self.begin();self.finish();self.stage.tick(self.host.claim['jobId'])
        # ActualGuard uses NodeGate internally. Successful calls above prove no
        # attempt called it from inside the controller's already-held gate.
        self.assertGreaterEqual(self.provenance.calls,3)

    def test_sealed_evidence_corruption_and_changed_native_files_hold(self):
        self.begin();self.finish();evidence=self.stage.tick(self.host.claim['jobId'])
        row=self.stage._row(self.host.claim['jobId']);prep=json.loads(row['preparation']);(Path(prep['output'])/'run'/'trees.csv').write_text('changed')
        with self.assertRaises(Exception):self.stage.tick(self.host.claim['jobId'])
        self.assertEqual(self.stage.read_evidence(self.intent),evidence)
        with closing(sqlite3.connect(self.stage.path)) as db,db:db.execute("UPDATE tree_stage_attempts SET evidence='{}'")
        with self.assertRaises(Exception):self.stage.read_evidence(self.intent)

    def test_provenance_lost_after_create_retains_created_container_without_start(self):
        self.docker.before=lambda action:setattr(self.provenance,'valid',False) if action=='create' else None
        with self.assertRaises(Exception):self.begin()
        self.assertEqual(self.actions(),['create']);self.assertEqual(self.stage.tick(self.host.claim['jobId'])['observed'],'created')

    def test_owner_lease_released_during_resolver_prevents_any_create(self):
        self.inputs.before=lambda:self.host.owner.__exit__(None,None,None)
        with self.assertRaises(Exception):self.begin()
        self.assertEqual(self.actions(),[])

    def test_exact_raw_file_bound_plus_separate_native_proof_seals_and_reads(self):
        for limit in (64,16):
            host=self
            if limit!=64:
                host=TreeStageTests();host.raw_file_limit=limit;host.setUp();self.addCleanup(host.doCleanups)
            with self.subTest(raw_files=limit):
                host.begin();host.finish();output=Path(next(x['Source'] for x in host.docker.container['Mounts'] if x['Destination']=='/output'))
                (output/'run'/'trees.csv').unlink()
                for index in range(limit):
                    path=output/'run'/('native-'+str(index)+'.csv');path.write_text('x');path.chmod(0o600)
                proof=host.stage.tick(host.host.claim['jobId'])
                self.assertEqual(len(proof['objects']),limit+1);self.assertEqual(proof,host.stage.read_evidence(host.intent))
                host.stage=host.open();self.assertEqual(proof,host.stage.read_evidence(host.intent));self.assertEqual(proof,host.stage.tick(host.host.claim['jobId']))

    def test_over_raw_file_bound_cannot_seal_even_without_native_proof(self):
        self.begin();self.finish();output=Path(next(x['Source'] for x in self.docker.container['Mounts'] if x['Destination']=='/output'))
        (output/'proof.json').unlink();(output/'run'/'trees.csv').unlink()
        for index in range(self.policy.max_files+1):
            path=output/'run'/('native-'+str(index)+'.csv');path.write_text('x');path.chmod(0o600)
        with self.assertRaises(m.TreeStageHeld):self.stage.tick(self.host.claim['jobId'])
        self.assertIsNone(self.stage.read_evidence(self.intent));self.assertTrue((output/'run'/'native-64.csv').exists())

    def test_invalid_exit_observations_cannot_publish_unreadable_evidence(self):
        self.begin();self.finish()
        for invalid in (True,False,-1,256,1.0):
            self.docker.container['State']['ExitCode']=invalid
            with self.subTest(exit_code=invalid),self.assertRaises(m.TreeStageHeld):self.stage.tick(self.host.claim['jobId'])
            self.assertIsNone(self.stage.read_evidence(self.intent));self.assertIsNone(self.stage._row(self.host.claim['jobId'])['evidence'])
        self.docker.container['State']['ExitCode']=255
        evidence=self.stage.tick(self.host.claim['jobId']);self.assertEqual(evidence['exitCode'],255)
        self.assertEqual(evidence,self.stage.read_evidence(self.intent))

    def test_entire_closed_evidence_validator_runs_before_terminal_publication(self):
        self.begin();self.finish()
        original=self.stage._sealed
        def reject(row,intent):
            self.assertIsNone(self.stage._row(self.host.claim['jobId'])['evidence'])
            self.assertEqual(json.loads(row['evidence'])['protocol'],'ltds-treedar-native-execution-v1')
            raise m.TreeStageHeld('synthetic shared validation refusal')
        with patch.object(self.stage,'_sealed',side_effect=reject),self.assertRaises(m.TreeStageHeld):self.stage.tick(self.host.claim['jobId'])
        self.assertIsNone(self.stage.read_evidence(self.intent))
        evidence=self.stage.tick(self.host.claim['jobId']);self.assertEqual(original(self.stage._row(self.host.claim['jobId']),self.intent),evidence)

    def test_native_proof_has_its_own_byte_cap_independent_of_raw_cap(self):
        self.begin();self.finish();output=Path(next(x['Source'] for x in self.docker.container['Mounts'] if x['Destination']=='/output'))
        proof=output/'proof.json';proof.write_text(json.dumps({'native':'x'*20000}));proof.chmod(0o600)
        self.assertGreater(proof.stat().st_size,self.policy.max_raw_bytes);self.assertLess(proof.stat().st_size,self.policy.native_proof_budget)
        evidence=self.stage.tick(self.host.claim['jobId']);self.assertEqual(evidence,self.stage.read_evidence(self.intent))
        self.assertEqual(evidence,self.open().tick(self.host.claim['jobId']))

    def test_proof_over_its_own_bound_never_seals(self):
        self.begin();self.finish();output=Path(next(x['Source'] for x in self.docker.container['Mounts'] if x['Destination']=='/output'))
        (output/'proof.json').write_bytes(b'x'*(self.policy.native_proof_budget+1))
        with self.assertRaises(m.TreeStageHeld):self.stage.tick(self.host.claim['jobId'])
        self.assertIsNone(self.stage.read_evidence(self.intent));self.assertTrue((output/'proof.json').exists())

    def test_shared_object_validator_bounds_raw_byte_sum_as_well_as_proof(self):
        sha='a'*64
        with self.assertRaises(m.TreeStageHeld):self.stage._objects([dict(relativePath='run/a',byteSize=6000,sha256=sha),dict(relativePath='run/b',byteSize=6000,sha256=sha)])
        with self.assertRaises(m.TreeStageHeld):self.stage._objects([dict(relativePath='proof.json',byteSize=self.policy.native_proof_budget+1,sha256=sha)])

    def test_real_like_base_image_environment_merges_fixed_overrides_by_key(self):
        host=TreeStageTests();host.image_environment=('PATH=/usr/local/bin:/usr/bin','PYTHONDONTWRITEBYTECODE=0','PYTHONPATH=/image','IMAGE_FLAVOR=synthetic')
        host.setUp();self.addCleanup(host.doCleanups);host.begin()
        actual=host.docker.container['Config']['Env'];self.assertEqual(len(actual),4)
        self.assertEqual(m.environment(actual),dict(PATH='/usr/local/bin:/usr/bin',PYTHONDONTWRITEBYTECODE='1',PYTHONPATH='/worker',IMAGE_FLAVOR='synthetic'))
        host.finish();evidence=host.stage.tick(host.host.claim['jobId']);self.assertEqual(evidence,host.stage.read_evidence(host.intent))
        host.docker.container['Config']['Env'].append('PYTHONPATH=/worker')
        with self.assertRaises(m.TreeStageHeld):host.stage.tick(host.host.claim['jobId'])

    def test_invalid_or_duplicate_base_environment_keys_are_refused(self):
        for values in (('PATH=/bin','PATH=/usr/bin'),('=bad',),('BAD-KEY=value',),('9BAD=value',),('NOVALUE',),('BAD=one\nBAD2=two',)):
            with self.subTest(values=values),self.assertRaises(m.TreeStageHeld):replace(self.policy,image_environment=values)

    def test_real_like_oci_labels_are_inherited_and_stage_label_overrides_by_key(self):
        host=TreeStageTests();host.image_labels=(('org.opencontainers.image.revision','a'*40),('org.opencontainers.image.source','https://example.invalid/synthetic'),('ltds.treedar.stage','base-image-value'))
        host.setUp();self.addCleanup(host.doCleanups);host.begin()
        labels=host.docker.container['Config']['Labels'];self.assertEqual(labels['org.opencontainers.image.revision'],'a'*40)
        self.assertEqual(labels['org.opencontainers.image.source'],'https://example.invalid/synthetic');self.assertNotEqual(labels['ltds.treedar.stage'],'base-image-value')
        host.finish();proof=host.stage.tick(host.host.claim['jobId']);self.assertEqual(proof,host.stage.read_evidence(host.intent))
        for key,value in (('unconfigured.extra','bad'),('org.opencontainers.image.revision','b'*40),('ltds.treedar.stage','0'*64)):
            original=dict(labels);labels[key]=value
            with self.subTest(key=key),self.assertRaises(m.TreeStageHeld):host.stage.tick(host.host.claim['jobId'])
            labels.clear();labels.update(original)

    def test_base_image_labels_are_immutable_closed_unique_and_validated(self):
        for labels in ({'key':'value'},(('key','one'),('key','two')),(('bad key','value'),),(('key','bad\nvalue'),),(('key',5),),(['key','value'],)):
            with self.subTest(labels=labels),self.assertRaises(m.TreeStageHeld):replace(self.policy,image_labels=labels)

