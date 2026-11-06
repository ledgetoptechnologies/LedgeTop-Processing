"""Production journal/driver with a synthetic CLI boundary, not live cutover.

Kernel/TLS/archive implementations have separate actual component suites. This
model checks phase ordering, real dispatch/readback code and recovery decisions.
"""
import ast
import copy
from datetime import datetime,timezone,timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

from ops import enrolled_coordinator_driver as d
from ops import enrolled_coordinator_cutover as j
from ops import enrolled_coordinator_transport as t
from ops import install_reference_coordinator as installer
from tests.test_enrolled_coordinator_deployment import bundle


def packet(source,name='request'):
    for item in ast.parse(source).body:
        if isinstance(item,ast.Assign) and any(isinstance(target,ast.Name) and target.id==name for target in item.targets):
            if isinstance(item.value,ast.Call):return json.loads(ast.literal_eval(item.value.args[0]))
    raise AssertionError('controlled helper packet missing')


class ModelDocker:
    def __init__(self,plan,value):
        self.plan,self.bundle=plan,value;self.calls=[];self.boxes={};self.gates={};self.receipts={};self.sequence=0
        self.late=False;self.hot=False;self.inject_late=False;self.lose=None;self.workers_changed=False
        self.env={name:'synthetic-'+name+'-'+'x'*40 for name in
                  ('PROCESSING_PROTOTYPE_OPERATOR_TOKEN','PROCESSING_VIEWER_TOKEN','PROCESSING_VIEWER_STAGING_TOKEN')}
        self.original=self.box(j.PREDECESSOR_ID,journal_name(),plan['predecessor']['imageId'],{},running=True)
        self.original['HostConfig']['RestartPolicy']=copy.deepcopy(plan['predecessor']['restartPolicy'])
        self.boxes[j.PREDECESSOR_ID]=self.original

    def box(self,identity,name,image,labels,*,running=False):
        return dict(Id=identity,Name='/'+name,Image=image,Config=dict(Env=[k+'='+v for k,v in self.env.items()],Labels=labels),
            HostConfig=dict(RestartPolicy={'Name':'no','MaximumRetryCount':0}),
            Mounts=[dict(Type='volume',Name=self.bundle['predecessor']['stateVolume'],Source='/volume/original',Destination='/prototype-state',RW=True)],
            State=dict(Running=running,Pid=123 if running else 0,StartedAt='2026-11-05T00:00:00Z' if running else '0001-01-01T00:00:00Z',
                       FinishedAt='0001-01-01T00:00:00Z',ExitCode=0,OOMKilled=False,Error=''))

    def inspect(self,identity):return copy.deepcopy(self.boxes[identity])
    def matching(self,filters):
        result=[]
        for identity,box in self.boxes.items():
            if all(box['Config']['Labels'].get(f[6:].split('=',1)[0])==f[6:].split('=',1)[1] for f in filters):result.append(identity)
        return result
    def command(self,*args,**kwargs):
        self.calls.append(('command',args))
        if args[:2]==('container','create'):
            self.sequence+=1;identity=f'{self.sequence:064x}';name=args[args.index('--name')+1]
            labels={args[n+1].split('=',1)[0]:args[n+1].split('=',1)[1] for n,x in enumerate(args[:-1]) if x=='--label'}
            self.boxes[identity]=self.box(identity,name,self.plan['candidate']['imageId'],labels)
            return (identity+'\n').encode()
        raise AssertionError('unexpected command')
    def restart_policy(self,identity,policy):
        self.calls.append(('policy',identity));self.boxes[identity]['HostConfig']['RestartPolicy']=copy.deepcopy(policy)
    def stop(self,identity):
        self.calls.append(('stop',identity));self.boxes[identity]['State'].update(Running=False,Pid=0,FinishedAt='stopped',ExitCode=0)
        if identity==j.PREDECESSOR_ID and self.inject_late:self.late=True
        if self.lose=='stop':self.lose=None;raise OSError('synthetic reply lost after effect')
    def start(self,identity):
        self.calls.append(('start',identity));self.sequence+=1
        self.boxes[identity]['State'].update(Running=True,Pid=100+self.sequence,StartedAt=datetime.now(timezone.utc).isoformat())
        role=self.boxes[identity]['Config']['Labels'].get(t.ROLE_LABEL)
        if role in ('bootstrap','final'):
            gate=dict(authorityId=self.plan['authorityId'],startupId=str(uuid.uuid4()),revision=0 if role=='bootstrap' else 3,
                planId=None if role=='bootstrap' else self.plan['operationId'],phase='open' if role=='bootstrap' else 'frozen',
                operations=0,reservedControlBytes=0 if role=='bootstrap' else 4096)
            self.gates[identity]=gate
        if self.lose=='start':self.lose=None;raise OSError('synthetic start reply lost after effect')
    def rename(self,identity,name):self.calls.append(('rename',identity));self.boxes[identity]['Name']='/'+name


def journal_name():return j.NAME


class ModelBoundary:
    def __init__(self,driver,physical):self.driver,self.physical=driver,physical
    def host(self):pass
    def container(self,identity,role,creation):
        item=self.driver.docker.inspect(identity)
        t.require(item['Config']['Labels'][t.INTENT_LABEL]==creation,'model_creation_changed')
        t.require(item['Image']==self.driver.plan['candidate']['imageId'],'model_image_changed')
        return item
    def writers(self,target):
        rows=[]
        for identity,item in self.driver.docker.boxes.items():
            if identity==target:continue
            t.require(item['State']['Running'] is False,'model_live_competing_writer')
            rows.append(dict(containerId=identity,running=False,pid=0,finishedAt=item['State']['FinishedAt']))
        return rows
    def gate(self,identity):return dict(snapshot=copy.deepcopy(self.driver.docker.gates[identity]),ownerSha256=self.driver.owner)
    def fresh(self,identity,source,**kwargs):
        if b'SELECT * FROM coordinator_freeze_receipts' in source:
            request=packet(source);return copy.deepcopy(self.driver.docker.receipts.get(request['requestId'],{'found':False}))
        if b'HTTPConnection' in source:return dict(status=200,body={'status':'ok','mode':'routing_probe'})
        return copy.deepcopy(self.physical)
    def __call__(self,role,context):
        item=self.driver.docker.inspect(context['containerId']);t.require(item['State']['Running'],'model_owner_not_running')
        writers=self.writers(item['Id']);physical=copy.deepcopy(self.physical);held=custody=gate=None
        if role=='holder':held=dict(settings=self.driver.settings,pid=1,startTime='1',mainIdentity=[1],leaseIdentity=[2])
        else:
            gate=self.gate(item['Id']);t.require(gate['snapshot']['phase']=='frozen','model_gate_open')
            physical.update(sourceBinding={'marker':'synthetic'},sourceBindingSha256='b'*64)
            custody=dict(protocol='ltds-runtime-source-custody-v1',status='verified',epoch='e'*64,bindingSha256='b'*64)
        return dict(schemaVersion=1,role=role,container=dict(id=item['Id'],imageId=item['Image'],pid=item['State']['Pid'],
            startedAt=item['State']['StartedAt']),holder=held,physical=physical,custody=custody,writers=writers,gate=gate)


class ModelCustody:
    def __init__(self,driver):self.driver=driver;self.records={};self.artifacts=self
    def reference(self,kind,intent,body):
        name=kind+'-'+intent['intentId'];self.records[name]={'body':body}
        return dict(name=name,sha256=t.digest(body),bytes=1,kind=kind)
    def load(self,reference,kind):return copy.deepcopy(self.records[reference['name']])
    def capture(self,intent,**kwargs):
        t.require(not self.driver.docker.late,'model_late_old_job_not_closed')
        boundary=self.driver.boundary('holder',kwargs['context'])
        return self.reference('baseline',intent,dict(boundaryAfter=boundary,baseline={'original':True}))
    def backup(self,baseline,intent,**kwargs):return self.reference('backup',intent,dict(baseline=baseline))
    def inspect_backup(self,baseline,intent,**kwargs):
        name='backup-'+intent['intentId'];t.require(name in self.records,'model_missing_backup')
        return dict(name=name,sha256=t.digest(self.records[name]['body']),bytes=1,kind='backup')
    def projection(self,baseline,intent,**kwargs):
        role='final' if kwargs['phase']=='final-frozen' else 'holder'
        self.driver.boundary(role,kwargs['context'])
        return self.reference('projection',intent,dict(phase=kwargs['phase'],binding=kwargs['binding']))


class ModelDriver(d.ConcreteCutoverDriver):
    def _private(self):return {}
    def _metadata(self):
        t.require(not self.docker.late,'model_changed_pre_stop_metadata');return {'closed':True}
    def _custody(self):return self.model_custody
    def _old_schema_only(self,holder):
        t.require(not self.docker.hot,'model_hot_journal_not_repaired');return {'schemaUnchanged':True,'lateAcceptedRetained':self.docker.late}
    def _workers(self):return [{'node':'model','version':'changed' if self.docker.workers_changed else 'reviewed'}]


@unittest.skipUnless(os.name=='posix','private Linux journal and transport artifacts')
class ConcreteDriverTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name);self.root.chmod(0o700)
        self.operation=self.root/'operation';self.operation.mkdir(mode=0o700)
        value,plan=bundle();value['recipes']['final']['name']=j.NAME
        plan['candidate']['finalRecipeSha256']=t.digest(value['recipes']['final'])
        plan['bindings']['deploymentSha256']=t.digest(value)
        self.docker=ModelDocker(plan,value)
        plan['bindings']['predecessorRecipeSha256']=t.digest(t.immutable_container_recipe(self.docker.original))
        plan['bindings']['predecessorEnvironmentSha256']=t.digest(t.container_environment(self.docker.original))
        physical=dict(state={'original':True},authorityLease={'original':True},operationBinding={'original':True})
        plan['bindings']['stateSha256']=t.digest(physical['state']);plan['bindings']['authorityLeaseSha256']=t.digest(physical['authorityLease'])
        self.driver=ModelDriver(plan,value,self.operation,docker=self.docker);self.driver.boundary=ModelBoundary(self.driver,physical)
        self.driver.model_custody=ModelCustody(self.driver)
        self.driver.owner=hashlib.sha256(self.docker.env['PROCESSING_PROTOTYPE_OPERATOR_TOKEN'].encode()).hexdigest()
        def tracked(identity,source,**kwargs):
            request=packet(source)['payload'];gate=self.docker.gates[identity]
            t.require(gate['revision']==request['expectedRevision'],'model_gate_stale')
            gate.update(revision=gate['revision']+1,planId=request['planId'],phase={'intent':'intent','drain':'draining','seal':'frozen','resume':'open'}[request['action']])
            receipt={k:gate[k] for k in ('authorityId','startupId','planId','revision','phase')};receipt['requestId']=request['requestId']
            self.docker.receipts[request['requestId']]=dict(found=True,receipt=receipt,ownerSha256=self.driver.owner)
            return dict(result={'status':200,'body':receipt},process={'retired':True})
        self.driver.tracked=tracked
        self.environment=patch.object(d,'environment_file',return_value=self.docker.env);self.environment.start();self.addCleanup(self.environment.stop)
        self.lock=patch.object(installer,'LOCK_PATH',self.root/'coordinator-install.lock');self.lock.start();self.addCleanup(self.lock.stop)
        self.controller=j.FirstEnrolledCutover(self.operation,plan,self.driver);self.controller.prepare()

    def advance_until(self,phase):
        for _ in range(30):
            state=self.controller.journal.read()
            if state['phase']==phase:return state
            self.controller.advance()
        self.fail('model phase not reached')

    def test_full_production_journal_driver_sequence_opens_only_after_projection_and_worker_proof(self):
        result=self.advance_until('complete')
        self.assertTrue(result['admissionOpened']);self.assertEqual(len(result['events']),22)
        self.assertEqual(result['events'][19]['action'],'verify-final-readiness')
        self.assertEqual(result['events'][20]['action'],'resume-final-gate')
        with self.assertRaises(j.CutoverHeld):self.controller.begin_rollback()

    def test_lost_stop_reply_is_inspected_without_second_stop(self):
        self.controller.advance();self.docker.lose='stop'
        with self.assertRaises(j.CutoverHeld):self.controller.advance()
        count=sum(call[0]=='stop' for call in self.docker.calls)
        self.controller.inspect()
        self.assertEqual(sum(call[0]=='stop' for call in self.docker.calls),count)

    def test_late_old_submission_preserved_and_original_only_rollback_never_restores_database(self):
        self.docker.inject_late=True;self.advance_until('original-leases-held')
        with self.assertRaises(t.TransportHeld):self.controller.advance()
        self.controller.reconcile_not_applied(disposition='rollback');self.controller.begin_rollback()
        state=self.advance_until('rolled-back-rebaseline-required')
        self.assertEqual(state['rollbackScope'],'original-only');self.assertTrue(self.docker.late)
        self.assertFalse(any(e['action']=='start-bootstrap' for e in state['events']))
        self.assertTrue(self.docker.boxes[j.PREDECESSOR_ID]['State']['Running'])
        self.assertFalse(any('restore' in str(call) and call[0]=='command' for call in self.docker.calls))

    def test_hot_old_journal_is_not_cleared_or_accepted_for_original_only_rollback(self):
        self.advance_until('original-leases-held');self.controller.begin_rollback();self.docker.hot=True
        with self.assertRaises(t.TransportHeld):self.advance_until('rolled-back-rebaseline-required')
        self.assertTrue(self.docker.hot);self.assertFalse(self.docker.boxes[j.PREDECESSOR_ID]['State']['Running'])

    def test_workers_changed_after_readiness_keep_final_gate_frozen(self):
        self.advance_until('final-ready');self.docker.workers_changed=True
        with self.assertRaises(t.TransportHeld):self.controller.advance()
        self.assertEqual(self.docker.gates[self.driver._find('final')['Id']]['phase'],'frozen')

    def test_declared_final_rollback_preserves_source_provenance_and_retains_candidate(self):
        self.advance_until('final-ready');started=self.driver._prior(self.controller.journal.read(),'start-final')
        self.controller.begin_rollback();state=self.advance_until('rolled-back-rebaseline-required')
        self.assertEqual(state['rollbackScope'],'declared-v2')
        proof=self.driver._prior(state,'verify-rollback-projection')
        self.assertEqual(proof['binding']['sourceReaderBinding'],started['boundary']['physical']['sourceBinding'])
        self.assertEqual(proof['binding']['sourceCustody'],started['boundary']['custody'])
        final=self.driver._find('final')
        self.assertFalse(final['State']['Running']);self.assertIn('-candidate-retained-',final['Name'])
        self.assertTrue(self.docker.original['State']['Running'])

    def test_unknown_start_blocks_rollback_until_exact_running_start_is_inspected(self):
        self.advance_until('final-created');self.docker.lose='start'
        with self.assertRaises(j.CutoverHeld):self.controller.advance()
        with self.assertRaises(j.CutoverHeld):self.controller.begin_rollback()
        count=sum(call[0]=='start' for call in self.docker.calls)
        state=self.controller.inspect();self.assertEqual(state['phase'],'final-frozen')
        self.assertEqual(sum(call[0]=='start' for call in self.docker.calls),count)

    def test_start_health_cannot_accept_restarted_or_stopped_incarnation(self):
        for field,value in (('Pid',999999),('StartedAt','changed'),('Running',False)):
            with self.subTest(field=field):
                item=copy.deepcopy(self.docker.original)
                item['State'].update(Running=True,Pid=123,StartedAt='original')
                after=copy.deepcopy(item);after['State'][field]=value
                with patch.object(self.driver.docker,'inspect',return_value=after):
                    with self.assertRaisesRegex(t.TransportHeld,'startup_incarnation_changed'):
                        self.driver._health(item,initial=True)

    def test_only_initial_predecessor_start_uses_bounded_health_without_new_start(self):
        before=list(self.docker.calls)
        for action in ('start-exact-predecessor','verify-predecessor-readiness','restore-predecessor-restart'):
            with patch.object(self.driver,'_health') as health:
                self.driver._observe(action,{}, {})
                health.assert_called_once_with(self.docker.original,initial=action=='start-exact-predecessor')
        self.assertEqual(self.docker.calls,before)

    def test_delayed_recreated_controller_uses_actual_start_not_current_inspection_clock(self):
        self.advance_until('final-created');self.docker.lose='start'
        with self.assertRaises(j.CutoverHeld):self.controller.advance()
        pending=self.controller.journal.read();token=self.driver._window('start-final',pending['pending'],pending)
        path=self.operation/self.driver.proofs.name(token);before=path.read_bytes()
        class Later(datetime):
            @classmethod
            def now(cls,tz=None):return datetime.now(tz)+timedelta(minutes=5)
        self.controller=j.FirstEnrolledCutover(self.operation,self.driver.plan,self.driver)
        count=sum(call[0]=='start' for call in self.docker.calls)
        with patch.object(d,'datetime',Later):state=self.controller.inspect()
        self.assertEqual(state['phase'],'final-frozen');self.assertEqual(path.read_bytes(),before)
        self.assertEqual(sum(call[0]=='start' for call in self.docker.calls),count)

    def test_unknown_resume_is_read_back_without_repost_and_never_allows_rollback(self):
        self.advance_until('final-ready');original=self.driver.tracked;count=0
        def lost(*args,**kwargs):
            nonlocal count
            count+=1;original(*args,**kwargs);raise OSError('synthetic accepted resume response lost')
        self.driver.tracked=lost
        with self.assertRaises(j.CutoverHeld):self.controller.advance()
        with self.assertRaises(j.CutoverHeld):self.controller.begin_rollback()
        # Inspection uses authenticated exact existing SQL receipt, not POST.
        self.driver.tracked=lambda *_a,**_k:(_ for _ in ()).throw(AssertionError('must not repost'))
        state=self.controller.inspect();self.assertTrue(state['admissionOpened']);self.assertEqual(count,1)
        with self.assertRaises(j.CutoverHeld):self.controller.begin_rollback()


if __name__=='__main__':unittest.main()
