"""Journal/fault semantics with synthetic boundaries; no Docker/host actions."""
from contextlib import contextmanager
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid

from ops import enrolled_coordinator_cutover as c


def plan():
    return dict(schemaVersion=1,mode=c.MODE,operationId=str(uuid.uuid4()),authorityId=c.AUTHORITY,
        predecessor=dict(containerId=c.PREDECESSOR_ID,imageId=c.PREDECESSOR_IMAGE,
            sourceCommit=c.PREDECESSOR_SOURCE,restartPolicy=dict(Name='unless-stopped',MaximumRetryCount=0)),
        candidate=dict(imageId='sha256:'+'a'*64,sourceCommit='b'*40,
            **{key:'c'*64 for key in ('bootstrapRecipeSha256','finalRecipeSha256','bootstrapConfigSha256',
                                     'finalConfigSha256','declaredTransformSha256')}),
        bindings={key:'d'*64 for key in ('stateSha256','authorityLeaseSha256','predecessorRecipeSha256',
            'predecessorEnvironmentSha256','censusKeySha256','reviewedCountsSha256','deploymentSha256')},
        gates={key:'e'*64 for key in ('nasQualificationSha256','pilotClosureSha256','legacyDeliverySha256','workerQualificationSha256')})


class Boundary:
    """Models effects independently; verify refuses missing/foreign effects."""
    def __init__(self):
        self.controller=None;self.calls=[];self.effects=set();self.loss=None;self.after=False
        self.wrong=set();self.before_effect_journals=[];self.observed=[];self.guard_ready=True;self.locked=False

    @contextmanager
    def global_lock(self,value):
        if self.locked:raise AssertionError('concurrent update owner')
        self.locked=True
        try:yield
        finally:self.locked=False

    def preflight(self,value):
        return dict(deploymentSha256=value['bindings']['deploymentSha256'],predecessorVerified=True,readOnly=True)

    def guard(self,action,intent,state,*,inspection):
        return self.guard_ready

    def execute(self,action,intent,state):
        if not self.locked:raise AssertionError('host-wide update ownership absent')
        stored=self.controller.journal.read()
        if stored['pending']!=intent:raise AssertionError('mutation preceded durable exact intent')
        self.before_effect_journals.append(copy.deepcopy(stored))
        self.calls.append((action,intent['intentId']))
        if self.loss==action and not self.after:raise OSError('synthetic no response before effect')
        self.effects.add(action)
        if self.loss==action:raise OSError('synthetic effect committed then response lost')

    def observe(self,action,intent,state):
        self.observed.append(action)
        return dict(intentId=intent['intentId'],bindingSha256=c.digest(intent),
                    factsSha256=c.digest(dict(action=action,exists=action in self.effects)))

    def verify(self,action,observation,intent,state):
        return action not in self.wrong and (action in c.READ_ACTIONS or action in c.ROLLBACK_READS or action in self.effects)


@unittest.skipUnless(os.name=='posix','private fsynced flock journal')
class EnrolledCutoverJournalTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.root.chmod(0o700)
        self.plan=plan();self.driver=Boundary()
        self.actor=c.FirstEnrolledCutover(self.root,self.plan,self.driver);self.driver.controller=self.actor
        self.actor.prepare()

    def until(self,action):
        while True:
            state=self.actor.journal.read()
            if c.STEPS[state['index']][0]==action:return state
            self.actor.advance()

    def test_all_phases_write_intent_before_effect_and_finish_once(self):
        for action,phase in c.STEPS:
            state=self.actor.advance();self.assertEqual(state['phase'],phase)
        self.assertEqual(state['phase'],'complete');self.assertTrue(state['admissionOpened'])
        self.assertEqual(len(self.driver.calls),len(c.STEPS)-len(c.READ_ACTIONS))
        self.assertEqual(self.actor.advance(),state)
        self.assertEqual(len(self.driver.calls),len(set(self.driver.calls)))
        self.assertFalse(any('restore' in action and 'database' in action for action,_ in self.driver.calls))

    def test_lost_create_response_is_inspected_without_repeating_mutation(self):
        self.until('create-bootstrap');self.driver.loss='create-bootstrap';self.driver.after=True
        with self.assertRaisesRegex(c.CutoverHeld,'mutation_outcome_uncertain'):self.actor.advance()
        pending=copy.deepcopy(self.actor.journal.read()['pending']);calls=list(self.driver.calls)
        with self.assertRaisesRegex(c.CutoverHeld,'inspection_only'):self.actor.advance()
        self.assertEqual(self.driver.calls,calls)
        recovered=self.actor.inspect()
        self.assertEqual(recovered['phase'],'bootstrap-created');self.assertIsNone(recovered['pending'])
        self.assertEqual(self.driver.calls,calls)
        self.assertEqual(c.digest(pending),recovered['events'][-1]['intentSha256'])

    def test_unapplied_unknown_mutation_stays_held_with_no_replay(self):
        self.driver.loss='disable-predecessor-restart'
        with self.assertRaises(c.CutoverHeld):self.actor.advance()
        for _ in range(3):
            with self.assertRaisesRegex(c.CutoverHeld,'not_verified'):self.actor.inspect()
        self.assertEqual(len(self.driver.calls),1)
        self.assertEqual(self.actor.journal.read()['phase'],'prepared')
        with self.assertRaisesRegex(c.CutoverHeld,'closed_known'):self.actor.begin_rollback()

    def test_new_process_reopens_exact_pending_intent_without_reissue(self):
        self.until('start-final');self.driver.loss='start-final';self.driver.after=True
        with self.assertRaises(c.CutoverHeld):self.actor.advance()
        pending=self.actor.journal.read()['pending'];calls=list(self.driver.calls)
        restarted=c.FirstEnrolledCutover(self.root,self.plan,self.driver);self.driver.controller=restarted
        state=restarted.inspect()
        self.assertEqual(state['phase'],'final-frozen')
        self.assertEqual(state['events'][-1]['intentSha256'],c.digest(pending))
        self.assertEqual(self.driver.calls,calls)

    def test_lost_resume_ack_never_allows_rollback_or_duplicate_resume(self):
        self.until('resume-final-gate');self.driver.loss='resume-final-gate';self.driver.after=True
        with self.assertRaises(c.CutoverHeld):self.actor.advance()
        with self.assertRaises(c.CutoverHeld):self.actor.begin_rollback()
        before=list(self.driver.calls)
        state=self.actor.inspect()
        self.assertEqual(state['phase'],'admission-open');self.assertTrue(state['admissionOpened'])
        self.assertEqual(before,self.driver.calls)
        with self.assertRaises(c.CutoverHeld):self.actor.begin_rollback()

    def test_qualifications_block_before_disabling_original_restart(self):
        changed=copy.deepcopy(self.plan);changed['gates']['pilotClosureSha256']=None
        other=Path(self.temp.name)/'other';other.mkdir(mode=0o700)
        driver=Boundary();actor=c.FirstEnrolledCutover(other,changed,driver);driver.controller=actor;actor.prepare()
        with self.assertRaisesRegex(c.CutoverHeld,'prerequisites_unqualified'):actor.advance()
        self.assertEqual(driver.calls,[]);self.assertIsNone(actor.journal.read()['pending'])

    def test_plan_mutation_cannot_resume_an_operation(self):
        altered=copy.deepcopy(self.plan);altered['candidate']['finalConfigSha256']='f'*64
        actor=c.FirstEnrolledCutover(self.root,altered,self.driver)
        with self.assertRaisesRegex(c.CutoverHeld,'journal_binding'):actor.advance()
        self.assertEqual(self.driver.calls,[])

    def test_fresh_boundary_failure_holds_without_new_intent_or_effect(self):
        self.driver.guard_ready=False
        with self.assertRaisesRegex(c.CutoverHeld,'current_cutover_boundary'):self.actor.advance()
        self.assertEqual(self.driver.calls,[])
        self.assertIsNone(self.actor.journal.read()['pending'])

    def test_each_freeze_rpc_is_independently_journaled_and_not_replayed(self):
        for action in ('intent-bootstrap-gate','drain-bootstrap-gate','seal-bootstrap-gate'):
            with self.subTest(action=action):
                # Each case starts from an independent exact journal owner.
                with tempfile.TemporaryDirectory() as directory:
                    Path(directory).chmod(0o700);driver=Boundary()
                    actor=c.FirstEnrolledCutover(Path(directory),self.plan,driver);driver.controller=actor;actor.prepare()
                    while c.STEPS[actor.journal.read()['index']][0]!=action:actor.advance()
                    driver.loss=action;driver.after=True
                    with self.assertRaises(c.CutoverHeld):actor.advance()
                    self.assertEqual(actor.journal.read()['pending']['action'],action)
                    count=len(driver.calls)
                    with self.assertRaises(c.CutoverHeld):actor.advance()
                    actor.inspect();self.assertEqual(len(driver.calls),count)

    def test_holder_create_precedes_start_and_rollback_can_create_missing_holder(self):
        actions=[action for action,_ in c.STEPS]
        self.assertLess(actions.index('create-original-lease-holder'),actions.index('acquire-original-leases'))
        self.actor.advance();self.actor.advance();self.actor.begin_rollback()
        self.actor.advance();self.actor.advance()
        self.assertEqual(self.actor.journal.read()['phase'],'rollback-lease-holder-created')
        self.assertEqual(self.actor.journal.read()['pending'],None)

    def test_bad_effect_or_declared_transform_stays_pending(self):
        self.until('verify-frozen-transform');self.driver.wrong.add('verify-frozen-transform')
        calls=list(self.driver.calls)
        with self.assertRaisesRegex(c.CutoverHeld,'not_verified'):self.actor.advance()
        self.assertEqual(self.driver.calls,calls)
        self.assertEqual(self.actor.journal.read()['phase'],'frozen-original-leases-held')
        self.assertEqual(self.actor.journal.read()['pending']['action'],'verify-frozen-transform')

    def test_rollback_has_explicit_closed_custody_phase_and_never_restores_sqlite(self):
        self.until('start-final')
        self.actor.begin_rollback()
        for action,phase in c.ROLLBACK_STEPS:self.assertEqual(self.actor.advance()['phase'],phase)
        actions=[action for action,_ in self.driver.calls]
        self.assertIn('stop-exact-candidates',actions)
        self.assertIn('acquire-rollback-original-leases',actions)
        self.assertIn('verify-rollback-projection',self.driver.observed)
        self.assertLess(actions.index('stop-exact-candidates'),actions.index('start-exact-predecessor'))
        self.assertEqual(self.actor.journal.read()['phase'],'rolled-back-rebaseline-required')
        self.assertFalse(any('database' in action or 'sqlite' in action for action in actions))

    def test_early_rollback_restores_restart_without_leasing_live_original(self):
        self.actor.advance()  # Only restart disabled; predecessor still running.
        self.actor.begin_rollback()
        self.assertEqual(self.actor.journal.read()['rollbackScope'],'restart-only')
        for _ in c.ROLLBACK_RESTART_ONLY:self.actor.advance()
        rollback=[event['action'] for event in self.actor.journal.read()['events'] if event['direction']=='rollback']
        self.assertEqual(rollback,['verify-predecessor-readiness','restore-predecessor-restart'])
        self.assertNotIn('acquire-rollback-original-leases',[action for action,_ in self.driver.calls])

    def test_stopped_original_before_candidate_uses_original_schema_rollback(self):
        self.actor.advance();self.actor.advance();self.actor.begin_rollback()
        self.assertEqual(self.actor.journal.read()['rollbackScope'],'original-only')
        for _ in c.ROLLBACK_ORIGINAL_ONLY:self.actor.advance()
        self.assertIn('verify-original-before-candidate',self.driver.observed)
        self.assertNotIn('verify-rollback-projection',self.driver.observed)

    def test_rollback_refuses_after_admission_even_before_restart_policy_step(self):
        self.until('resume-final-gate');self.actor.advance()
        with self.assertRaisesRegex(c.CutoverHeld,'closed_known'):self.actor.begin_rollback()
        self.assertTrue(self.actor.journal.read()['admissionOpened'])

    def test_journal_index_phase_or_open_flag_cannot_skip_history(self):
        for mutation in (dict(index=len(c.STEPS),phase='complete'),dict(admissionOpened=True)):
            original=self.actor.journal.read();bad=copy.deepcopy(original);bad.update(mutation);self.actor.journal.write(bad)
            with self.assertRaises(c.CutoverHeld):self.actor.advance()
            self.actor.journal.write(original)
        self.assertEqual(self.driver.calls,[])

    def test_foreign_or_changed_pending_intent_is_rejected(self):
        self.driver.loss='disable-predecessor-restart'
        with self.assertRaises(c.CutoverHeld):self.actor.advance()
        state=self.actor.journal.read();state['pending']['intentId']=str(uuid.uuid4());self.actor.journal.write(state)
        with self.assertRaisesRegex(c.CutoverHeld,'pending_intent_changed'):self.actor.inspect()
        self.assertEqual(len(self.driver.calls),1)

    def test_rollback_transform_rejection_never_restarts_predecessor(self):
        self.until('create-final');self.actor.begin_rollback()
        while self.actor._steps(self.actor.journal.read())[self.actor.journal.read()['index']][0]!='verify-rollback-projection':
            self.actor.advance()
        self.driver.wrong.add('verify-rollback-projection')
        with self.assertRaises(c.CutoverHeld):self.actor.advance()
        self.assertNotIn('start-exact-predecessor',[action for action,_ in self.driver.calls])

    def test_history_and_proof_tampering_refused(self):
        self.actor.advance();state=self.actor.journal.read();state['events'][0]['observation']['factsSha256']='f'*64
        self.actor.journal.write(state)
        with self.assertRaisesRegex(c.CutoverHeld,'history_projection'):self.actor.advance()

    def test_closed_predecessor_and_target_plan_contract(self):
        for field,value in (('containerId','0'*64),('sourceCommit','0'*40),('imageId','sha256:'+'0'*64)):
            changed=copy.deepcopy(self.plan);changed['predecessor'][field]=value
            with self.assertRaises(c.CutoverHeld):c.validate_plan(changed)
        changed=copy.deepcopy(self.plan);changed['candidate']['imageId']=c.PREDECESSOR_IMAGE
        with self.assertRaises(c.CutoverHeld):c.validate_plan(changed)

    def test_private_regular_journal_and_original_operation_lock_required(self):
        self.actor.journal.path.chmod(0o644)
        with self.assertRaisesRegex(c.CutoverHeld,'private_bounded'):self.actor.advance()


if __name__=='__main__':unittest.main()
