"""Qualification-only ordering/failure retention; actual Docker is opt-in."""
import copy
from datetime import datetime,timezone
from pathlib import Path
import tempfile
import os
import unittest
import uuid
from unittest.mock import Mock,patch

from ops.qualify_enrolled_phase_transport import post_restart_stopped_writer_audit
from ops.enrolled_coordinator_transport import TransportHeld,require
from ops.enrolled_coordinator_custody_ops import PrivateArtifacts,CustodyHeld


class PostRestartAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.original=dict(Id='original',Image='legacy',State=dict(Running=True,Pid=100,StartedAt='2026-11-05T00:00:00Z',ExitCode=0,OOMKilled=False))
        self.holder=dict(Id='holder',Image='candidate',State=dict(Running=False,Pid=0,StartedAt='2026-11-04T00:00:00Z',ExitCode=0,OOMKilled=False))
        self.calls=[];self.driver=Mock();self.driver.plan={'operationId':str(uuid.uuid4())};self.driver.plan_sha='a'*64
        self.driver._original.side_effect=lambda:copy.deepcopy(self.original)
        self.driver._find.side_effect=lambda role:copy.deepcopy(self.holder)
        def stopped(item):require(not item['State']['Running'] and item['State']['Pid']==0 and item['State']['ExitCode']==0 and not item['State']['OOMKilled'],'graceful_stop_required')
        self.driver._stopped.side_effect=stopped
        self.receipts={'start-exact-predecessor':dict(containerId='original',startedAt=self.original['State']['StartedAt']),
                       'verify-predecessor-readiness':{},'capture-full-custody':{'reference':{'name':'baseline'}}}
        self.driver._prior.side_effect=lambda state,action:self.receipts[action]
        def stop(identity):
            self.calls.append(('stop',identity));box=self.original if identity=='original' else self.holder
            box['State'].update(Running=False,Pid=0)
        def start(identity):
            self.calls.append(('start',identity));self.holder['State'].update(Running=True,Pid=200,StartedAt=datetime.now(timezone.utc).isoformat())
        self.driver.docker.stop.side_effect=stop;self.driver.docker.start.side_effect=start
        self.driver._context.return_value={'containerId':'holder'};self.driver.boundary.return_value={'fresh':'physical+socket+kernel'}
        self.driver._binding.return_value={'immutable':'binding'}
        self.driver._custody.return_value.projection.side_effect=self.projection
        self.state=dict(phase='rolled-back-rebaseline-required',admissionOpened=False,pending=None)

    def projection(self,*args,**kwargs):
        self.calls.append(('projection','rollback-final'))
        self.assertFalse(self.original['State']['Running']);self.assertTrue(self.holder['State']['Running'])
        self.assertEqual(kwargs['phase'],'rollback-final');self.assertEqual(kwargs['binding'],{'immutable':'binding'})
        return {'name':'strict-original-projection'}

    def run_audit(self):
        # The Windows unit test may not have O_DIRECTORY; durability itself is
        # exercised by the Linux actual harness and existing private_write tests.
        with patch('ops.qualify_enrolled_phase_transport.private_write',side_effect=lambda path,raw:path.write_bytes(raw)):
            return post_restart_stopped_writer_audit(self.driver,self.state,self.root)

    def test_after_real_start_stop_acquire_project_release_without_second_original_start(self):
        result=self.run_audit()
        self.assertEqual(self.calls,[('stop','original'),('start','holder'),('projection','rollback-final'),('stop','holder')])
        self.assertTrue(result['originalRestartObserved']);self.assertFalse(result['liveSQLiteAudit'])
        self.assertFalse(result['continuousAvailabilityQualified']);self.assertTrue(result['holderReleased'])
        self.assertTrue((self.root/'post-restart-audit-intent.json').exists());self.assertTrue((self.root/'post-restart-audit-result.json').exists())

    def test_projection_failure_retains_held_original_and_live_holder_without_release_or_repair(self):
        self.driver._custody.return_value.projection.side_effect=RuntimeError('changed legacy row')
        with self.assertRaisesRegex(RuntimeError,'changed legacy row'):self.run_audit()
        self.assertEqual(self.calls,[('stop','original'),('start','holder')])
        self.assertFalse(self.original['State']['Running']);self.assertTrue(self.holder['State']['Running'])
        self.assertFalse((self.root/'post-restart-audit-result.json').exists())

    def test_unknown_holder_start_is_not_retried_or_released(self):
        def lost(identity):self.calls.append(('start',identity));raise OSError('unknown start')
        self.driver.docker.start.side_effect=lost
        with self.assertRaisesRegex(OSError,'unknown start'):self.run_audit()
        self.assertEqual(self.calls,[('stop','original'),('start','holder')]);self.driver._custody.return_value.projection.assert_not_called()

    def test_rollback_not_terminal_or_admitted_never_mutates(self):
        for bad in ({'phase':'final-ready'},{'admissionOpened':True},{'pending':{'action':'unknown'}}):
            with self.subTest(bad=bad):
                self.state.update(bad)
                with self.assertRaisesRegex(TransportHeld,'completed_unadmitted_rollback_required'):self.run_audit()
                self.assertEqual(self.calls,[])
                self.state=dict(phase='rolled-back-rebaseline-required',admissionOpened=False,pending=None)

    def test_changed_original_start_receipt_refused_before_stop(self):
        self.receipts['start-exact-predecessor']['startedAt']='other'
        with self.assertRaisesRegex(TransportHeld,'exact_restarted_original_required'):self.run_audit()
        self.assertEqual(self.calls,[])

    def test_old_holder_start_proof_cannot_substitute_for_fresh_incarnation(self):
        def stale(identity):self.calls.append(('start',identity));self.holder['State'].update(Running=True,Pid=200)
        self.driver.docker.start.side_effect=stale
        with self.assertRaisesRegex(TransportHeld,'fresh_original_holder_incarnation_required'):self.run_audit()
        self.driver.boundary.assert_not_called();self.assertEqual(self.calls,[('stop','original'),('start','holder')])

    @unittest.skipUnless(hasattr(os,'geteuid') and hasattr(os,'O_DIRECTORY'),'actual Linux private artifacts')
    def test_actual_private_artifact_contract_retains_bound_audit_receipt(self):
        self.root.chmod(0o700)
        artifacts=PrivateArtifacts(self.root,self.driver.plan['operationId'],self.driver.plan_sha)
        self.driver._custody.return_value.artifacts=artifacts
        def retain(reference,intent,**kwargs):
            self.projection(reference,intent,**kwargs)
            proof=artifacts.retain('projection',intent,None,{'syntheticBoundaryOnly':True})
            document=artifacts.load(proof,'projection')
            self.assertEqual(document['operationId'],self.driver.plan['operationId'])
            self.assertEqual(document['planSha256'],self.driver.plan_sha)
            return proof
        self.driver._custody.return_value.projection.side_effect=retain
        result=self.run_audit();self.assertEqual(result['projectionReference']['kind'],'projection')
        self.assertTrue(result['holderReleased'])

    @unittest.skipUnless(hasattr(os,'geteuid'),'actual Linux private artifacts')
    def test_actual_artifact_wrong_operation_or_plan_refused_before_any_mutation(self):
        self.root.chmod(0o700)
        for operation,plan in ((str(uuid.uuid4()),self.driver.plan_sha),(self.driver.plan['operationId'],'b'*64)):
            with self.subTest(operation=operation,plan=plan):
                self.driver._custody.return_value.artifacts=PrivateArtifacts(self.root,operation,plan)
                with self.assertRaisesRegex(CustodyHeld,'original_intent_artifact_required'):self.run_audit()
                self.assertEqual(self.calls,[])
                self.assertFalse((self.root/'post-restart-audit-intent.json').exists())


if __name__=='__main__':unittest.main()
