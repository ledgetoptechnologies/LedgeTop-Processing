from datetime import timedelta
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
import uuid

from processing.update_service import UpdateService,ServiceError,ServiceHeld
from processing.update_supervisor import UpdateSupervisor,SupervisorError,SupervisorHeld
from tests import test_update_supervisor as fixture
from tests import test_update_registry as registry_fixture
from tests import test_update_pipeline as pipeline_fixture


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.authority=str(uuid.uuid4());self.now=100
        self.supervisor,self.backend,self.docker=fixture.components(self.root,self.authority)
        self.supervisor.__enter__();self.addCleanup(lambda:self.supervisor.close() if self.supervisor._lease is not None else None)
        self.idle=True;self.scheduling=True;self.admitted=True;self.releases=[];self.admissions=[]
        self.service=self.open()

    def admit(self,candidate,registry):
        self.admissions.append(candidate['candidateId']);return self.admitted

    def release(self,candidate,registry):
        registry.inspect_staged(candidate)
        self.releases.append(candidate['candidateId']);return False

    def open(self,**kwargs):
        settings=dict(config_id='fixture-v1',code_binding_sha256='c'*64,capacity_admitter=self.admit,
            capacity_releaser=self.release,idle_probe=lambda status:self.idle,can_schedule=lambda status:self.scheduling,
            discovery_interval_seconds=21600,error_cooldown_seconds=300,max_transitions=8,clock=lambda:self.now)
        settings.update(kwargs)
        return UpdateService(self.root/'service.sqlite',supervisor=self.supervisor,**settings)

    def restart(self):
        self.supervisor.close();self.supervisor,self.backend,_=fixture.components(self.root,self.authority)
        self.supervisor.registry.runner=self.docker;self.supervisor.__enter__();self.service=self.open()

    def counts(self):return tuple(self.backend.count(kind) for kind in ('qualification','schedule','run'))

    def test_one_tick_completes_one_cycle_then_restart_is_cheap_throttled_noop(self):
        result=self.service.tick()
        self.assertEqual(result['status'],'completed');self.assertEqual(result['transitions'],5)
        self.assertEqual(self.counts(),(1,1,1));self.assertEqual(len(self.releases),1)
        self.assertEqual(self.supervisor._current_state(),(registry_fixture.CONFIG,1))
        with self.service._db() as db:self.assertEqual(db.execute('PRAGMA synchronous').fetchone()[0],2)
        self.assertIsNone(self.supervisor.pending())
        before=list(self.docker.calls);self.restart();self.now=160
        self.assertEqual(self.service.tick()['status'],'not_due')
        self.assertEqual(self.docker.calls,before);self.assertEqual(self.counts(),(1,1,1))
        self.assertEqual(len(self.releases),1)

    def test_existing_completed_supervisor_adoption_is_not_reset_to_policy_baseline(self):
        identifier=self.supervisor.discover()['operationId']
        while self.supervisor.status(identifier)['state']!='completed':self.supervisor.advance(identifier)
        self.assertEqual(self.supervisor.policy.current_image_id,fixture.OLD)
        self.assertEqual(self.supervisor._current(),registry_fixture.CONFIG)
        self.assertEqual(self.service.tick()['status'],'no_change')
        self.assertEqual(self.counts(),(1,1,1))
        self.assertEqual(self.supervisor._current_state(),(registry_fixture.CONFIG,1))

    def test_busy_wait_preserves_discovered_without_staging_intent_or_capacity(self):
        self.idle=False;result=self.service.tick()
        self.assertEqual(result['status'],'waiting_idle');self.assertEqual(result['state'],'discovered')
        self.assertEqual(self.admissions,[]);self.assertEqual(self.releases,[]);self.assertEqual(self.counts(),(0,0,0))
        self.assertFalse(any(args[0]=='pull' for args in self.docker.calls))
        identifier=result['operationId'];self.restart();self.idle=True
        self.assertEqual(self.service.tick()['operationId'],identifier)
        self.assertEqual(self.counts(),(1,1,1))

    def test_low_capacity_wait_restart_then_admit_stage_without_poisoned_intent(self):
        self.admitted=False;result=self.service.tick()
        self.assertEqual((result['status'],result['state']),('waiting_capacity','discovered'))
        self.assertFalse(any(args[0]=='pull' for args in self.docker.calls))
        self.assertEqual(self.supervisor.pending()['state'],'discovered')
        with self.supervisor.registry._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM registry_staging').fetchone()[0],0)
        self.restart();self.admitted=True
        self.assertEqual(self.service.tick()['status'],'completed')
        self.assertEqual(len([args for args in self.docker.calls if args[0]=='pull']),1)

    def test_busy_after_definite_stage_releases_capacity_but_never_qualifies(self):
        states=[]
        def idle(status):
            states.append(status['state']);return status['state']=='discovered'
        self.service=self.open(idle_probe=idle)
        result=self.service.tick()
        self.assertEqual((result['status'],result['state']),('waiting_idle','staged'))
        self.assertEqual(states,['discovered','staged']);self.assertEqual(self.counts(),(0,0,0))
        self.assertEqual(len(self.releases),1)
        self.assertEqual(self.open(idle_probe=lambda status:False).tick()['status'],'waiting_idle')
        self.assertEqual(len(self.releases),1)

    def test_foreign_plan_wait_precedes_schedule_intent_and_future_window_wait_is_normal(self):
        self.scheduling=False;result=self.service.tick()
        self.assertEqual((result['status'],result['state']),('waiting_schedule','qualified'))
        self.assertEqual(self.counts(),(1,0,0))
        row=self.supervisor._row(result['operationId']);self.assertIsNone(row['schedule_request'])
        self.scheduling=True;self.backend.faults['readiness']=False
        result=self.service.tick()
        self.assertEqual((result['status'],result['state']),('waiting_run','scheduled'))
        self.assertEqual(self.counts(),(1,1,0));self.assertIsNone(self.supervisor._row(result['operationId'])['run_request'])
        self.assertEqual(self.service.tick()['status'],'waiting_run');self.assertEqual(self.counts(),(1,1,0))
        self.backend.faults['readiness']=True
        self.assertEqual(self.service.tick()['status'],'completed');self.assertEqual(self.counts(),(1,1,1))

    def test_unknown_qualification_reconciles_only_and_stops_before_schedule(self):
        self.backend.faults['qualification']='after'
        result=self.service.tick();self.assertEqual((result['status'],result['state']),('held','qualification_intent'))
        self.assertEqual(self.counts(),(1,0,0))
        self.restart();result=self.service.tick()
        self.assertEqual((result['status'],result['state']),('reconciled','qualified'))
        self.assertEqual(self.counts(),(1,0,0))
        self.assertEqual(self.service.tick()['status'],'completed');self.assertEqual(self.counts(),(1,1,1))

    def test_unknown_schedule_and_run_never_repeat_mutating_adapter(self):
        self.backend.faults['schedule']='after'
        result=self.service.tick();self.assertEqual(result['state'],'schedule_intent')
        self.restart();self.assertEqual(self.service.tick()['state'],'scheduled')
        self.assertEqual(self.counts(),(1,1,0))
        self.backend.faults['run']='after';self.assertEqual(self.service.tick()['state'],'run_intent')
        self.restart();self.assertEqual(self.service.tick()['status'],'completed')
        self.assertEqual(self.counts(),(1,1,1));self.assertEqual(self.supervisor._current(),registry_fixture.CONFIG)

    def test_unknown_staging_does_not_release_capacity_or_retry_pull(self):
        self.docker.error=TimeoutError('do-not-print')
        result=self.service.tick();self.assertEqual((result['status'],result['state']),('held','staging_intent'))
        self.assertEqual(self.releases,[])
        self.restart();self.docker.error=None
        result=self.service.tick();self.assertEqual((result['status'],result['state']),('reconciled','staged'))
        self.assertEqual(len(self.releases),1);self.assertEqual(self.counts(),(0,0,0))
        self.assertEqual(len([args for args in self.docker.calls if args[0]=='pull']),1)

    def test_permanently_unknown_qualification_blocks_new_discovery_even_if_tag_changes(self):
        self.backend.faults['qualification']='before';self.assertEqual(self.service.tick()['state'],'qualification_intent')
        before=list(self.docker.calls);self.restart();self.now+=100000
        self.docker.selected=registry_fixture.entry(config='sha256:'+'f'*64);self.docker.first=[self.docker.selected]
        for _ in range(2):self.assertEqual(self.service.tick()['status'],'held')
        self.assertEqual(self.docker.calls,before);self.assertEqual(self.counts(),(1,0,0))

    def test_discovery_error_cooldown_restart_and_clock_rollback_do_not_spin(self):
        with patch.object(self.supervisor,'discover',side_effect=OSError('do-not-print')) as discover:
            result=self.service.tick();self.assertEqual(result['status'],'discovery_cooldown')
            self.assertEqual(result['nextDiscoveryAt'],400)
            self.now=90;self.assertEqual(self.service.tick()['status'],'discovery_cooldown')
            self.assertEqual(discover.call_count,1)
        self.restart();self.now=399;self.assertEqual(self.service.tick()['status'],'discovery_cooldown')
        self.assertEqual(self.docker.calls,[])
        self.now=400;self.assertEqual(self.service.tick()['status'],'completed')

    def test_committed_discovery_intent_survives_process_gap_and_respects_cooldown(self):
        original=self.supervisor.discover
        with patch.object(self.supervisor,'discover',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):self.service.tick()
        self.restart();self.now=200
        with patch.object(self.supervisor,'discover',side_effect=AssertionError('too soon')):
            self.assertEqual(self.service.tick()['status'],'discovery_cooldown')
        self.now=400;self.assertEqual(self.service.tick()['status'],'completed')

    def test_lost_discovery_checkpoint_preserves_pending_and_does_not_rescan(self):
        with patch.object(self.service,'_finish_discovery',side_effect=OSError('checkpoint unknown')):
            with self.assertRaises(ServiceHeld):self.service.tick()
        pending=self.supervisor.pending();self.assertEqual(pending['state'],'discovered')
        self.restart();before=len([args for args in self.docker.calls if args[0]=='manifest'])
        self.assertEqual(self.service.tick()['status'],'completed')
        self.assertEqual(len([args for args in self.docker.calls if args[0]=='manifest']),before)
        self.assertEqual(self.service.tick()['status'],'not_due')

    def test_forward_clock_jump_permits_only_one_scan_and_backward_clock_retains_highwater(self):
        self.assertEqual(self.service.tick()['status'],'completed')
        self.now=10**7;self.assertEqual(self.service.tick()['status'],'no_change')
        before=list(self.docker.calls)
        self.now=100;self.restart()
        self.assertEqual(self.service.tick()['status'],'not_due');self.assertEqual(self.docker.calls,before)

    def test_transition_budget_is_bounded_and_completion_does_not_start_next_cycle(self):
        with self.assertRaises(ServiceHeld):self.open(max_transitions=1)
        limited=UpdateService(self.root/'limited-service.sqlite',supervisor=self.supervisor,config_id='one-step-fixture',
            code_binding_sha256='c'*64,capacity_admitter=self.admit,capacity_releaser=self.release,
            idle_probe=lambda status:True,can_schedule=lambda status:True,max_transitions=1,clock=lambda:self.now)
        results=[limited.tick() for _ in range(5)]
        self.assertEqual([result['state'] for result in results],['discovered','staged','qualified','scheduled','completed'])
        self.assertTrue(all(result['transitions']==1 for result in results))
        self.assertEqual(self.counts(),(1,1,1));self.assertEqual(len(self.releases),1)
        self.assertEqual(limited.tick()['status'],'not_due')

    def test_capacity_release_exception_can_only_retry_idempotent_metadata(self):
        called=[]
        def release(candidate,registry):
            registry.inspect_staged(candidate);called.append(candidate['candidateId'])
            if len(called)==1:raise OSError('lost metadata release response')
            return True
        service=self.open(capacity_releaser=release)
        result=service.tick();self.assertEqual((result['status'],result['state']),('capacity_release_held','staged'))
        self.assertEqual(self.counts(),(0,0,0))
        self.assertEqual(service.tick()['status'],'completed')
        self.assertEqual(len(called),2);self.assertEqual(len([args for args in self.docker.calls if args[0]=='pull']),1)

    def test_pending_validates_exact_nonterminal_and_refuses_ambiguous_rows(self):
        self.assertIsNone(self.supervisor.pending());self.idle=False;result=self.service.tick()
        self.assertEqual(self.supervisor.pending(),self.supervisor.status(result['operationId']))
        with self.supervisor._db() as db:
            db.execute('DROP INDEX one_open_update')
            db.execute("INSERT INTO supervisor_operations(id,node,target,state,candidate,base_image,generation) SELECT ?,node,?,'discovered',candidate,base_image,generation FROM supervisor_operations LIMIT 1",('a'*64,'b'*64))
        with self.assertRaises(SupervisorHeld):self.supervisor.pending()

    def test_service_binding_reentrancy_fork_and_supervisor_lease_are_preserved(self):
        with self.assertRaises(ServiceHeld):self.open(code_binding_sha256='d'*64)
        def reenter(status):
            with self.assertRaises(ServiceHeld):self.service.tick()
            other,_,_=fixture.components(self.root,self.authority)
            with self.assertRaises(SupervisorError):other.__enter__()
            return False
        self.service.idle_probe=reenter
        self.assertEqual(self.service.tick()['status'],'waiting_idle')
        self.supervisor.close()
        with self.assertRaises(SupervisorError):self.service.tick()

    @unittest.skipUnless(os.name=='posix','actual fork PID guard')
    def test_forked_service_refuses_before_inherited_mutex_and_cannot_unlock_owner(self):
        reader,writer=os.pipe()
        with self.service._mutex:
            pid=os.fork()
            if pid==0:
                os.close(reader)
                try:
                    try:self.service.tick();os.write(writer,b'bad')
                    except ServiceHeld:os.write(writer,b'held')
                finally:os._exit(0)
        os.close(writer);self.assertEqual(os.read(reader,10),b'held');os.close(reader);os.waitpid(pid,0)
        self.supervisor.check()

    def test_repeated_a_b_c_a_b_cycles_use_adopted_current_generation(self):
        first=self.service.tick();self.assertEqual(first['status'],'completed')
        for config in ('sha256:'+'c'*64,fixture.OLD,registry_fixture.CONFIG):
            self.now+=21600
            self.docker.selected=registry_fixture.entry(config=config);self.docker.first=[self.docker.selected];self.docker.tags=0
            self.docker.image.update(Id=config,RepoDigests=[registry_fixture.REPO+'@'+self.docker.selected['Descriptor']['digest']])
            result=self.service.tick();self.assertEqual(result['status'],'completed')
        self.assertNotEqual(first['operationId'],result['operationId'])
        self.assertEqual(self.counts(),(4,4,4));self.assertEqual(self.supervisor._current_state(),(registry_fixture.CONFIG,4))


@unittest.skipUnless(sys.platform.startswith('linux'),'actual private managed HTTP pipeline fixture requires Linux')
class HTTPPipelineServiceTests(unittest.TestCase):
    def setUp(self):
        self.fixture=pipeline_fixture.UpdatePipelineTests(methodName='runTest')
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups);self.now=100
        self.release_calls=[]
        def release(candidate,registry):
            registry.inspect_staged(candidate);self.release_calls.append(candidate['candidateId']);return False
        self.service=UpdateService(self.fixture.root/'service.sqlite',supervisor=self.fixture.supervisor,
            config_id='real-http-pipeline',code_binding_sha256='e'*64,capacity_admitter=lambda candidate,registry:True,
            capacity_releaser=release,idle_probe=lambda status:True,can_schedule=self.can_schedule,clock=lambda:self.now)

    def can_schedule(self,status):
        return self.fixture.host.api.json('GET','/v1/prototype/nodes/cluster')['node']['state']=='active'

    def test_actual_registry_qualifier_owned_schedule_http_and_managed_finalize_one_tick(self):
        result=self.service.tick()
        self.assertEqual(result['status'],'completed')
        self.assertEqual(len(self.release_calls),1)
        self.assertEqual(self.fixture.host.host.adapter.calls,[pipeline_fixture.engine_fixture.NEW])
        self.assertEqual(self.fixture.host.host.ingress.opens,1)
        self.assertEqual(self.fixture.host.host.fixture.queue.get_node('cluster')['state'],'active')
        self.assertEqual(self.service.tick()['status'],'not_due')
        self.assertEqual(self.fixture.host.host.adapter.calls,[pipeline_fixture.engine_fixture.NEW])

    def test_actual_foreign_manual_plan_is_waited_not_canceled_adopted_or_replaced(self):
        identifier=self.fixture.staged();self.fixture.supervisor.advance(identifier)
        node=self.fixture.host.host.fixture.queue.get_node('cluster')
        future=(self.fixture.host.now+timedelta(hours=1)).isoformat()
        reply=self.fixture.host.operator.json('POST','/v1/prototype/nodes/cluster/schedule',dict(expectedRevision=node['revision'],
            drainAt=future,notBefore=future,afterJobId=None,targetImage=pipeline_fixture.engine_fixture.NEW))
        before=reply['node']
        result=self.service.tick();self.assertEqual((result['status'],result['state']),('waiting_schedule','qualified'))
        self.assertEqual(self.fixture.host.host.fixture.queue.get_node('cluster'),before)
        with self.fixture.host.adapter._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM schedule_intents').fetchone()[0],0)
        self.assertEqual(self.fixture.host.host.adapter.calls,[])


if __name__=='__main__':unittest.main()
