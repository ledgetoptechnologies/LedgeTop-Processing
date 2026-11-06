"""Original pre-reservation claims recover without replacing their ownership.

Actual driver/control/recovery/Store/ledger/Gate; synthetic native engine only.
The Linux child/TLS suite is a separate qualification layer.
"""
import copy
import json
import threading
from dataclasses import replace
from unittest.mock import patch
from uuid import uuid4

from processing.node_gate import NodeGate
from processing.reference_execution import ExecutionRouter
from processing.prototype import Queue, REFERENCE_PROFILE
from processing.reference_runtime import ReferenceRuntime
from tests import test_reference_runtime as runtime_fixture
from tests.test_reference_source_grant_renewal import WORKER_TOKEN
from tests.test_reference_source_processing_recovery import ProcessingHarness, GATE_ORIGIN


class PreReservationRecoveryTests(ProcessingHarness):
    def unreserved(self):
        self.down.add('reservation')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.down.clear()
        self.assertEqual(self.row()[0], 'reservation_pending')
        self.assertIsNone(self.store.get(self.job_id, private=True)['transferRequest'])
        self.restart_coordinator()
        return copy.deepcopy(self.row()[1]['reservation'])

    def pending(self):
        reservation = self.unreserved()
        self.down.add('recover')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.down.clear()
        return reservation, copy.deepcopy(self.row()[1]['sourceRecovery']['pending']['request'])

    def post(self, request):
        return self.driver._request('POST', '/jobs/'+self.job_id+'/recover', request,
            claim_token=self.row()[1]['claim']['claimToken'])

    def assert_not_changed(self, request):
        before = self.rows()
        self.assertEqual(self.post(request)[0], 409)
        self.assertEqual(self.rows(), before)
        self.assertEqual(self.status()[0], 'held')
        self.assertEqual(self.transport.starts(), 0)

    def test_never_committed_reservation_recovers_same_intent_and_starts_once(self):
        reservation = self.unreserved()
        before = self.rows()['reference_claims']
        self.step('reservation_pending')
        receipt, = self.receipts()
        self.assertEqual((receipt['schemaVersion'],receipt['executionPhase'],receipt['resumedExecutionPhase']),
            (4,'queued','awaiting_transfer'))
        self.assertEqual(self.store.get(self.job_id,private=True)['transferRequest'],reservation)
        self.assertEqual(self.rows()['reference_claims'],before)
        self.run_to_engine()
        self.assertEqual(self.row()[1]['reservation'],reservation)


    def test_committed_reservation_lost_reply_uses_existing_schema_three(self):
        self.lose_reply.add('reservation')
        self.assertEqual(self.driver.step()['state'],'retry_wait')
        reservation = copy.deepcopy(self.row()[1]['reservation'])
        self.restart_coordinator()
        self.step('reservation_pending')
        self.assertEqual(self.receipts()[0]['schemaVersion'],3)
        self.run_to_engine()
        self.assertEqual(self.row()[1]['reservation'],reservation)

    def test_lost_initial_claim_reply_recovers_original_token_and_starts_once(self):
        self.lost_claim()

    def test_lost_claim_reply_with_actual_router_defers_new_route_until_running(self):
        self.lost_claim(routed=True)

    def lost_claim(self, *, routed=False):
        # Return fixture to an unclaimed admission. Every subsequent claim is
        # through the real ordinary control route; lose its committed response.
        with self.queue._db() as db:
            db.execute('DELETE FROM reference_claims WHERE job_id=?',(self.job_id,))
            db.execute("UPDATE jobs SET status='queued',worker_id=NULL,node_id=NULL,claim_hash=NULL WHERE id=?",(self.job_id,))
            db.execute('UPDATE reference_jobs SET node_id=NULL WHERE job_id=?',(self.job_id,))
        data={}
        if routed:
            claim=self.row()[1]['claim']
            data['execution']={key:claim[key] for key in ('jobId','generation','environment','manifestSha256','pipelineDigest')}
            data['execution']['kind']='odm'
            self.driver.execution_router=ExecutionRouter(self.root/'routes.sqlite',self.driver.worker,
                failure_outbox=self.driver.failure_outbox)
        with self.driver._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS readiness_tasks(uuid TEXT PRIMARY KEY)')
            db.execute("UPDATE driver_jobs SET stage='claim_pending',data=? WHERE job=?",(json.dumps(data),self.job_id))
        self.driver._refresh_options()
        self.lose_reply.add('claim')
        self.assertEqual(self.driver.step()['state'],'retry_wait')
        before = self.rows()['reference_claims']
        self.assertEqual(len(before),1)
        self.restart_coordinator()
        self.new_worker()
        # This fixture's attach_worker intentionally empties options; normal
        # startup discovers them before admitting/replaying a claim.
        self.driver._refresh_options()
        if routed:
            self.driver.execution_router=ExecutionRouter(self.root/'routes.sqlite',self.driver.worker,
                failure_outbox=self.driver.failure_outbox)
        self.step('claimed')
        if routed:
            with self.driver.execution_router._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM execution_routes').fetchone()[0],0)
        self.run_to_engine()
        self.assertEqual(self.rows()['reference_claims'],before)
        self.assertEqual(self.receipts()[0]['schemaVersion'],4)

    def test_recovery_reply_loss_replays_one_receipt_after_worker_death(self):
        reservation = self.unreserved()
        self.lose_reply.add('recover')
        self.assertEqual(self.driver.step()['state'],'retry_wait')
        request = self.row()[1]['sourceRecovery']['pending']['request']
        self.new_worker()
        self.run_to_engine()
        self.assertEqual([r['requestId'] for r in self.receipts()],[request['requestId']])
        self.assertEqual(self.row()[1]['reservation'],reservation)

    def test_second_restart_after_committed_recovery_uses_exact_recorded_transfer(self):
        reservation = self.unreserved()
        self.lose_reply.add('recover')
        self.assertEqual(self.driver.step()['state'],'retry_wait')
        self.restart_coordinator()
        self.new_worker()
        self.run_to_engine()
        self.assertEqual([r['schemaVersion'] for r in self.receipts()],[4,3])
        self.assertEqual(self.row()[1]['reservation'],reservation)

    def test_historical_receipt_does_not_clear_later_operator_hold(self):
        _reservation,request = self.pending()
        self.assertEqual(self.post(request)[0],200)
        self.execute("UPDATE jobs SET status='held',held_reason='operator_hold' WHERE id=?",self.job_id)
        before = self.rows()
        self.assertEqual(self.post(request)[0],200)
        self.assertEqual(self.rows(),before)

    def test_stale_epoch_revision_and_changed_transfer_are_refused(self):
        _reservation,request = self.pending()
        for field,value in (('coordinatorEpoch',str(uuid4())),('expectedNodeRevision',request['expectedNodeRevision']+1)):
            with self.subTest(field=field):
                self.assert_not_changed(dict(request,**{field:value}))
        changed = copy.deepcopy(request)
        changed['transferRequest']['objectIds']=[]
        from processing.reference_protocol import digest
        changed['transferRequestSha256']=digest(changed['transferRequest'])
        self.assert_not_changed(changed)

    def test_quota_refusal_rolls_back_transfer_status_and_receipt_together(self):
        _reservation,request = self.pending()
        with self.queue._db() as db:
            self.store.max_metadata_bytes=self.store._metadata_used_bytes(db)
        self.assert_not_changed(request)
        self.assertIsNone(self.store.get(self.job_id,private=True)['transferRequest'])

    def test_operator_hold_and_maintenance_change_refuse_pending_transition(self):
        _reservation,request = self.pending()
        self.execute("UPDATE jobs SET held_reason='operator_hold' WHERE id=?",self.job_id)
        self.assert_not_changed(request)
        self.execute("UPDATE jobs SET held_reason='coordinator_restart' WHERE id=?",self.job_id)
        self.execute("UPDATE nodes SET state='updating',revision=revision+1 WHERE id='cluster'")
        self.assert_not_changed(request)

    def test_local_execution_row_or_native_task_never_adopted(self):
        self.unreserved()
        self.transport.tasks[self.job_id]=20
        result=self.driver.step()
        self.assertEqual(result['state'],'held',result)
        self.assertEqual(self.receipts(),[])
        self.assertEqual(self.transport.starts(),0)

    def test_source_custody_change_refuses_without_reservation_effect(self):
        _reservation,request=self.pending()
        self.execute('UPDATE reference_source_object_receipts SET byte_size=byte_size+1 WHERE job_id=?',self.job_id)
        self.assert_not_changed(request)

    def test_exact_replay_does_not_spend_source_or_scratch_counters(self):
        reservation,request=self.pending()
        before=self.fenced(reservation)
        first=self.post(request)
        self.assertEqual(first[0],200)
        after=self.rows()
        self.assertEqual(self.post(request),first)
        self.assertEqual(self.rows(),after)
        self.assertEqual(self.fenced(reservation),before)
        self.assertEqual(self.transport.starts(),0)

    def test_two_concurrent_exact_recoveries_create_one_receipt_and_reservation(self):
        reservation,request=self.pending()
        barrier=threading.Barrier(2)
        replies=[]
        failures=[]
        def recover():
            try:
                barrier.wait(timeout=5)
                replies.append(self.post(copy.deepcopy(request)))
            except BaseException as error:
                failures.append(error)
        threads=[threading.Thread(target=recover) for _ in range(2)]
        for thread in threads:thread.start()
        for thread in threads:thread.join(timeout=10)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(failures,[])
        self.assertEqual(len(replies),2)
        self.assertEqual(replies[0],replies[1])
        self.assertEqual(replies[0][0],200)
        self.assertEqual(len(self.receipts()),1)
        self.assertEqual(self.store.get(self.job_id,private=True)['transferRequest'],reservation)
        self.run_to_engine()

    def test_occupied_gate_refuses_before_any_recovery_post(self):
        self.unreserved()
        with NodeGate('engine') as gate:
            gate.reserve('cluster',GATE_ORIGIN,self.queue.authority_id,1,'b'*32)
        sent=len(self.requests)
        self.assertEqual(self.driver.step(),{'state':'held','reason':'source_recovery_gate_occupied'})
        self.assertNotIn(('POST','recover'),self.requests[sent:])

    def test_local_allocation_and_lease_cannot_be_reset_by_recovery(self):
        self.unreserved()
        with self.ledger._transaction() as db:
            db.execute('UPDATE reservations SET allocated=1')
        self.assertEqual(self.driver.step()['state'],'held')
        self.assertEqual(self.receipts(),[])
        self.assertEqual(self.transport.starts(),0)

    def test_engine_outage_retains_intent_without_spending_an_attempt(self):
        reservation=self.unreserved()
        self.transport.down=True
        for _ in range(3):self.assertEqual(self.driver.step()['state'],'retry_wait')
        self.assertNotIn('sourceRecovery',self.row()[1])
        self.assertEqual(self.receipts(),[])
        self.assertEqual(self.row()[1]['reservation'],reservation)
        self.transport.down=False
        self.run_to_engine()

    def test_changed_hardware_capacity_or_busy_snapshot_refuses_new_reservation(self):
        _reservation,request=self.pending()
        capacities=copy.deepcopy(self.store._capacity_snapshot)
        for change in ({'ready':False},{'maxInputBytes':1},{'taskQueueCount':1}):
            with self.subTest(change=change):
                current=copy.deepcopy(capacities)
                current['cluster'].update(change)
                self.store.update_fleet_snapshot(current)
                self.assert_not_changed(request)
        self.store.update_fleet_snapshot(capacities)
        self.assertEqual(self.post(request)[0],200)

    def test_other_physical_node_occupant_refuses_recovery(self):
        _reservation,request=self.pending()
        self.execute("""INSERT INTO jobs(id,request_id,environment,profile,status,node_id)
            SELECT 'other-job','other-request',environment,profile,'running',node_id FROM jobs WHERE id=?""",self.job_id)
        self.assert_not_changed(request)

    def test_expired_local_lease_is_not_replaced(self):
        reservation=self.unreserved()
        from tests.test_reference_source_fetch_recovery import stamp
        self.now[0]=stamp(reservation['expiresAt'])+1
        self.assertEqual(self.driver.step()['state'],'held')
        self.assertEqual(self.receipts(),[])
        self.assertEqual(self.row()[1]['reservation'],reservation)


class RuntimePreReservationDiscoveryTests(ProcessingHarness):
    """Fresh real runtime Store cannot use its optimistic constructor defaults."""
    def runtime_after_restart(self, *, processing=False):
        if processing:
            self.to_execution_pending()
        else:
            self.down.add('reservation')
            self.assertEqual(self.driver.step()['state'],'retry_wait')
            self.down.clear()
        self.queue.close()
        self.queue=Queue(self.directory.name)
        self.queue.allowed_profiles.add(REFERENCE_PROFILE)
        fixture=runtime_fixture.ReferenceRuntimeTests('runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.value=fixture.recovery_config(self.queue.authority_id)
        fixture.write_config()
        settings=fixture.settings()
        workers=copy.deepcopy(settings.workers)
        workers['cluster'].update(bearer=WORKER_TOKEN,workerId='worker',instanceId='instance')
        settings=replace(settings,workers=workers,source_spool=self.spool,
            source_attestation_keys=self.case.trusted_keys,viewer_receipt_keys=self.viewer_receipt_keys)
        definitions=[dict(name='numeric-probe',type='float',value=1,help='Numeric setting.')]
        self.catalog=dict(ready=True,commonOptions=definitions,nodes=[dict(role=node,available=True,
            capabilitiesSha256='a'*64,engine='odm',engineVersion='pinned-engine-1',options=definitions)
            for node in ('cluster','main')])
        self.capacities={node:dict(ready=True,maxImages=None,maxInputBytes=None,taskQueueCount=0)
            for node in ('cluster','main')}
        self.discovery_calls=[]
        def discover():
            self.discovery_calls.append('catalog')
            if self.catalog is None:raise OSError('synthetic catalog outage')
            return copy.deepcopy(self.catalog)
        def capacities():
            self.discovery_calls.append('capacity')
            return copy.deepcopy(self.capacities)
        first,second=fixture.tls_patches()
        with first,second:
            runtime=ReferenceRuntime(self.queue,replace(fixture.config,prototype_state_dir=self.directory.name),settings,
                discover=discover,discover_capacities=capacities)
        self.addCleanup(runtime.close)
        self.runtime=runtime
        self.store=runtime.store
        return runtime

    def request(self, method='GET', body=None, *, token=None):
        headers={'Authorization':'Bearer '+WORKER_TOKEN,
            'X-Claim-Token':self.row()[1]['claim']['claimToken'] if token is None else token}
        suffix='recovery-state' if method=='GET' else 'recover'
        from processing.reference_protocol import canonical_json
        return self.runtime.api.handle(method,'/ltds/worker/v1/jobs/'+self.job_id+'/'+suffix,
            headers,b'' if body is None else canonical_json(body),protected_transport=True)

    def test_fresh_runtime_refreshes_changed_and_unavailable_catalog_before_schema_four(self):
        self.runtime_after_restart()
        self.assertTrue(self.store._capacity_snapshot['cluster']['ready'])
        valid=copy.deepcopy(self.catalog)
        self.catalog=None
        self.assertEqual(self.request().status,503)
        self.assertEqual(self.status(),('held','coordinator_restart'))
        self.catalog=dict(ready=True,commonOptions=[],nodes=[dict(role=node,available=False,options=[])
            for node in ('cluster','main')])
        self.assertFalse(self.request().body['eligible'])
        self.assertTrue(self.discovery_calls)
        self.assertIsNone(self.store.get(self.job_id,private=True)['transferRequest'])
        self.catalog=valid
        self.assertTrue(self.request().body['eligible'])

    def test_post_refreshes_again_instead_of_using_a_prior_valid_get(self):
        self.runtime_after_restart()
        self.control=self.runtime.api
        self.down.add('recover')
        self.assertEqual(self.driver.step()['state'],'retry_wait')
        self.down.clear()
        request=self.row()[1]['sourceRecovery']['pending']['request']
        self.capacities['cluster']['taskQueueCount']=1
        self.assertEqual(self.request('POST',request).status,409)
        self.assertIsNone(self.store.get(self.job_id,private=True)['transferRequest'])
        self.capacities['cluster']['taskQueueCount']=0
        self.assertEqual(self.request('POST',request).status,200)
        self.assertEqual(len(self.receipts()),1)

    def test_combined_private_catalog_snapshot_is_used_once_per_recovery_turn(self):
        self.runtime_after_restart()
        calls=[]
        def paired():
            calls.append('paired')
            return copy.deepcopy(self.catalog),copy.deepcopy(self.capacities)
        def separate():
            raise AssertionError('paired private catalog must not be split')
        # Root's integrated private-catalog runtime has this existing seam;
        # this regression intentionally requires its recovery-hook adaptation.
        self.runtime.api.discover_fleet=paired
        self.runtime.api.discover=separate
        self.runtime.api.discover_capacities=separate
        self.assertTrue(self.request().body['eligible'])
        self.assertEqual(calls,['paired'])
        self.control=self.runtime.api
        self.down.add('recover')
        self.assertEqual(self.driver.step()['state'],'retry_wait')
        self.down.clear()
        request=self.row()[1]['sourceRecovery']['pending']['request']
        previous=len(calls)
        self.assertEqual(self.request('POST',request).status,200)
        self.assertEqual(len(calls),previous+1)

    def test_invalid_claim_token_probes_no_catalog(self):
        self.runtime_after_restart()
        self.assertIn(self.request(token='not-the-claim').status,(401,403,409))
        self.assertEqual(self.discovery_calls,[])

    def test_processing_recovery_does_not_depend_on_catalog(self):
        self.runtime_after_restart(processing=True)
        self.catalog=None
        def paired():
            raise AssertionError('started recovery must not discover fleet')
        self.runtime.api.discover_fleet=paired
        response=self.request()
        self.assertEqual(response.status,200)
        self.assertEqual(response.body['schemaVersion'],3)
        self.assertTrue(response.body['eligible'])
        self.assertEqual(self.discovery_calls,[])
