"""Portable synthetic activation boundaries; no systemd, SSH or host effects."""
import copy
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
import unittest
import uuid
from unittest.mock import patch

from ops import first_worker_activation as a
from ops.enrolled_coordinator_driver import ConcreteCutoverDriver
from ops.enrolled_coordinator_cutover import FirstEnrolledCutover
from ops.enrolled_coordinator_transport import canonical,digest,TransportHeld
from ops import historical_pilot_closure as h
from ops import first_worker_activation_service as service


def fixture():
    plan={'operationId':str(uuid.uuid4())}
    expected=dict(operationId=plan['operationId'],cutoverPlanSha256=digest(plan),nodeId='main',
        workerSha256='a'*64,manifestSha256='b'*64,installReceiptSha256='c'*64,
        ingressPolicySha256='e'*64,nativeInventorySha256='f'*64,
        bootstrapStartupId=str(uuid.uuid4()),ownerSha256='d'*64)
    return plan,expected


class MemoryActivation(a.FirstWorkerActivation):
    """Journal sequencing only; actual fsync/private modes are not claimed."""
    def __init__(self,plan,expected,boundary,store):
        self.binding=a.binding(expected);self.boundary=boundary;self.store=store
        self.driver=SimpleNamespace(plan=plan,global_lock=lambda plan:nullcontext())
        self.journal=SimpleNamespace(locked=lambda:nullcontext())
    def _save(self,state):self.store['value']=copy.deepcopy(state)
    def _load(self):return copy.deepcopy(self.store['value'])
    def _evidence(self,current):return self.boundary.observe(self.binding,current),'0'*64


class ActivationSequencingTests(unittest.TestCase):
    def setUp(self):
        self.plan,self.expected=fixture();self.calls=[];self.failure=None
        self.store={'value':dict(schemaVersion=1,protocol=a.PROTOCOL,binding=self.expected,
            phase='prepared',pending=None,events=[],receipt=None)}
        test=self
        class Boundary:
            def guard(self,expected,current,*,inspection):
                test.calls.append(('guard',current['action'],inspection));return True
            def execute(self,expected,current):
                test.assertEqual(test.store['value']['pending'],current)
                test.calls.append(('execute',current['action'],current['requestId']))
                if test.failure:raise test.failure
            def observe(self,expected,current):
                test.calls.append(('observe',current['action']));return {'syntheticEvidenceFor':current['requestId']}
            def verify(self,*args):return True
        self.boundary=Boundary();self.agent=MemoryActivation(self.plan,self.expected,self.boundary,self.store)

    def test_exact_reload_then_start_and_terminal_receipt(self):
        self.assertEqual(self.agent.advance()['phase'],'reloaded')
        done=self.agent.advance();self.assertEqual(done['phase'],'ready');self.assertEqual(done['receipt'],a.receipt(done))
        self.agent.advance();self.assertEqual([x[1] for x in self.calls if x[0]=='execute'],['reload','start'])

    def test_lost_start_reply_survives_new_controller_and_is_inspection_only(self):
        self.agent.advance();self.failure=TransportHeld('synthetic_lost_start_reply')
        with self.assertRaises(TransportHeld):self.agent.advance()
        self.assertEqual(self.store['value']['phase'],'start-pending')
        restarted=MemoryActivation(self.plan,self.expected,self.boundary,self.store)
        with self.assertRaisesRegex(TransportHeld,'inspection_only'):restarted.advance()
        self.assertEqual(restarted.inspect()['phase'],'ready')
        self.assertEqual([x[1] for x in self.calls if x[0]=='execute'],['reload','start'])

    def test_process_death_after_intent_does_not_repeat_reload(self):
        self.failure=SystemExit(42)
        with self.assertRaises(SystemExit):self.agent.advance()
        restarted=MemoryActivation(self.plan,self.expected,self.boundary,self.store)
        self.assertEqual(restarted.inspect()['phase'],'reloaded')
        self.assertEqual(sum(x[0]=='execute' for x in self.calls),1)

    def test_failed_guard_has_no_start_intent_or_effect(self):
        self.boundary.guard=lambda *args,**kwargs:False
        with self.assertRaisesRegex(TransportHeld,'guard_required'):self.agent.advance()
        self.assertEqual(self.store['value']['phase'],'prepared');self.assertFalse(self.calls)

    def test_unretired_body_remains_pending(self):
        self.boundary.observe=lambda *args:(_ for _ in ()).throw(TransportHeld('body_not_retired'))
        with self.assertRaisesRegex(TransportHeld,'not_retired'):self.agent.advance()
        with self.assertRaisesRegex(TransportHeld,'inspection_only'):self.agent.advance()
        self.assertEqual(self.store['value']['phase'],'reload-pending')


class ConcreteDriverExclusionTests(unittest.TestCase):
    def setUp(self):
        self.driver=ConcreteCutoverDriver.__new__(ConcreteCutoverDriver)
        self.plan,self.expected=fixture();self.driver.plan=self.plan;self.driver.plan_sha=digest(self.plan)
        self.worker=dict(nodeId='main',settings={'sourceRoot':'/existing-reviewed-worker'})
        self.driver.bundle={'workers':[self.worker]};self.driver.root=Path('/synthetic')
        self.driver.boundary=SimpleNamespace(host=lambda:self.fail('host effects must not precede exclusion'))

    def test_actual_driver_guard_blocks_pending_before_host_effects(self):
        for action in ('stop-bootstrap','stop-exact-candidates','resume-final-gate'):
            with self.subTest(action=action),patch.object(a,'existing_activation',return_value={'phase':'start-pending','pending':{}}):
                current=dict(operationId=self.plan['operationId'],planSha256=self.driver.plan_sha,action=action)
                with self.assertRaisesRegex(TransportHeld,'blocks_coordinator'):self.driver.guard(action,current,{},inspection=False)

    def test_corrupt_or_lost_registered_journal_blocks_progress(self):
        with patch.object(a,'existing_activation',side_effect=TransportHeld('registered_activation_journal_missing')):
            with self.assertRaisesRegex(TransportHeld,'journal_missing'):self.driver.guard_activation_rollback()

    def test_rollback_hook_precedes_direction_change(self):
        main=FirstEnrolledCutover.__new__(FirstEnrolledCutover)
        state=dict(direction='forward',admissionOpened=False,pending=None,proofs={'stop-predecessor':{},'start-bootstrap':{}})
        main._locked=lambda:nullcontext();main._load=lambda:state
        main.driver=self.driver;writes=[];main.journal=SimpleNamespace(write=writes.append)
        with patch.object(a,'existing_activation',return_value={'phase':'start-pending','pending':{}}):
            with self.assertRaisesRegex(TransportHeld,'blocks_coordinator'):main.begin_rollback()
        self.assertEqual(state['direction'],'forward');self.assertFalse(writes)

    def test_generic_driver_rollback_contract_is_preserved(self):
        main=FirstEnrolledCutover.__new__(FirstEnrolledCutover)
        state=dict(direction='forward',admissionOpened=False,pending=None,proofs={})
        main._locked=lambda:nullcontext();main._load=lambda:state;main.driver=SimpleNamespace()
        writes=[];main.journal=SimpleNamespace(write=writes.append)
        self.assertEqual(main.begin_rollback()['direction'],'rollback');self.assertEqual(len(writes),1)

    def test_absent_journal_never_authorizes_reserved_first_install(self):
        self.worker['settings']['sourceRoot']='/opt/ledgetop-processing-reference/'+str(uuid.uuid4())+'/source'
        with patch.object(a,'registered_binding',return_value=None),patch.object(a.os.path,'lexists',return_value=False):
            with self.assertRaisesRegex(TransportHeld,'receipt_required'):a.guard_coordinator_progress(self.driver,'stop-bootstrap')

    def test_existing_worker_without_activation_keeps_read_only_compatibility(self):
        with patch.object(a,'registered_binding',return_value=None),patch.object(a.os.path,'lexists',return_value=False):
            self.assertIsNone(a.activation_readiness_binding(self.driver,self.worker,{},{}))


class ClosedProofTests(unittest.TestCase):
    def test_missing_actual_pilot_validator_is_held_not_an_approval_flag(self):
        boundary=a.ControllerActivationBoundary.__new__(a.ControllerActivationBoundary);boundary.pilot_closure=None
        with self.assertRaisesRegex(TransportHeld,'retirement_unproved'):boundary._pilot({})

    def test_unknown_binding_fields_are_refused(self):
        _,expected=fixture();expected['pilotClosed']=True
        with self.assertRaisesRegex(TransportHeld,'closed_worker_activation'):a.binding(expected)

    def test_bootstrap_physical_source_checks_both_original_kernel_flocks(self):
        source=a.bootstrap_physical_source(str(uuid.uuid4()),'/run/original-lease',1).decode()
        self.assertIn("('main',str(lock),key(ls))",source);self.assertIn("('external',str(lease),key(es))",source)
        self.assertIn("['FLOCK','ADVISORY','WRITE']",source);compile(source,'bootstrap-physical','exec')


class HistoricalClosureTests(unittest.TestCase):
    def setUp(self):
        self.authority=str(uuid.uuid4());self.predecessor='a'*64
        self.opaque=dict(files=24,directories=4,bytes=6378,manifestSha256='b'*64)
        self.value=h.historical_receipt(self.authority,self.predecessor,self.opaque)
    def validate(self,value):
        return h.validate_historical_receipt(canonical(value),authority=self.authority,
            predecessor=self.predecessor,opaque=self.opaque)
    def test_exact_five_invocations_bind_three_operations_and_failed_owner_exit(self):
        result=self.validate(self.value)
        self.assertEqual(len(result['invocations']),5);self.assertEqual(set(x['operation'] for x in result['invocations']),set(h.OPS))
        self.assertEqual(result['invocations'][-1]['exitCode'],1)
        self.assertEqual(result['scope'],'historical-local-producer-termination')
    def test_later_pr66_source_cannot_replace_original_invocation(self):
        self.value['invocations'][2]['sourceSha256']='2214f5f04b35645c5067c935ef8d5dbe8a212061378709ad94f0379a01a809de'
        with self.assertRaisesRegex(TransportHeld,'binding_changed'):self.validate(self.value)
    def test_missing_terminal_or_fake_approval_is_refused(self):
        self.value['approved']=True
        with self.assertRaisesRegex(TransportHeld,'binding_changed'):self.validate(self.value)
        del self.value['approved'];self.value['invocations'].pop()
        with self.assertRaisesRegex(TransportHeld,'binding_changed'):self.validate(self.value)
    def test_other_retained_manifest_or_source_hash_is_refused(self):
        self.value['opaquePilotJournals']=dict(self.opaque,manifestSha256='c'*64)
        with self.assertRaisesRegex(TransportHeld,'binding_changed'):self.validate(self.value)
    def test_private_opaque_reader_is_bounded_and_matches_census_domain_separators(self):
        source=h.opaque_pilot_source(b'x'*32).decode();compile(source,'opaque-pilot-reader','exec')
        self.assertIn("b'path\\0'",source);self.assertIn("b'file\\0'",source)
        self.assertNotIn('sqlite',source);self.assertIn('O_NOFOLLOW',source)


class RetainedNativeTests(unittest.TestCase):
    def setUp(self):
        self.identifier=str(uuid.uuid4());self.other=str(uuid.uuid4())
        self.info=dict(uuid=self.identifier,status={'code':40},imagesCount=12)
        self.manifest=dict(gate={'nodeId':'cluster','authorityId':str(uuid.uuid4())},
            host={'dockerEngineId':'engine'},engine={'containerId':'a'*64,'imageId':'sha256:'+'b'*64,'version':'3.8.5'})
        self.inventory=dict(schemaVersion=1,protocol='ltds-retained-native-quarantine-v1',nodeId='cluster',
            authorityId=self.manifest['gate']['authorityId'],engineId='engine',containerId='a'*64,
            imageId='sha256:'+'b'*64,tasks=[dict(uuid=self.identifier,statusCode=40,infoSha256=digest(self.info))])
        self.lists=0;self.reads=[]
        def read(path,**kwargs):
            self.reads.append(path)
            if path=='/info':return {'engineVersion':'3.8.5','taskQueueCount':0}
            if path=='/task/list':self.lists+=1;return [{'uuid':self.identifier}]
            return copy.deepcopy(self.info)
        self.client=SimpleNamespace(read=read)
    def test_declared_completed_task_is_preserved_and_observed_twice(self):
        result=service.retained_native_census(self.client,self.manifest,self.inventory)
        self.assertEqual(result['taskCount'],1);self.assertEqual(self.lists,2)
        self.assertEqual(self.reads.count('/task/'+self.identifier+'/info'),2)
        self.assertTrue(all(path.startswith(('/info','/task/list','/task/'+self.identifier+'/info')) for path in self.reads))
    def test_running_or_unknown_task_refuses(self):
        for code in (10,20,99,None,True):
            with self.subTest(code=code):
                self.info['status']['code']=code
                with self.assertRaisesRegex(TransportHeld,'active_unknown'):service.retained_native_census(self.client,self.manifest,self.inventory)
    def test_changed_task_set_refuses(self):
        old=self.client.read
        def read(path,**kwargs):
            if path=='/task/list' and self.lists:self.lists+=1;return [{'uuid':self.other}]
            return old(path,**kwargs)
        self.client.read=read
        with self.assertRaisesRegex(TransportHeld,'task_set_differs'):service.retained_native_census(self.client,self.manifest,self.inventory)
    def test_undeclared_terminal_task_is_not_silently_adopted(self):
        self.inventory['tasks']=[]
        with self.assertRaisesRegex(TransportHeld,'task_set_differs'):service.retained_native_census(self.client,self.manifest,self.inventory)
    def test_changed_terminal_metadata_refuses(self):
        self.info['imagesCount']=13
        with self.assertRaisesRegex(TransportHeld,'metadata_changed'):service.retained_native_census(self.client,self.manifest,self.inventory)


if __name__=='__main__':unittest.main()
