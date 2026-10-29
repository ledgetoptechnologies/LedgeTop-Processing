"""Live canonical RPC leases: actual TLS/NativeHTTP and read-only health SQL."""
import json
import os
import ssl
import threading
import unittest
from unittest.mock import patch

from ops import reference_worker_daemon as daemon
from processing.reference_api import Response
from processing.reference_http import create_reference_http_server
from processing.reference_driver import DriverHeld
from tests import test_reference_option_evidence as evidence

@unittest.skipUnless(os.name=='posix','Linux canonical owner/private TLS fixture')
class ReadinessWindowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):evidence.OptionEvidenceClientTests.setUpClass.__func__(cls)
    def setUp(self):
        self.fixture=evidence.OptionEvidenceClientTests();self.fixture.cert,self.fixture.key=self.cert,self.key
        self.addCleanup(self.fixture.doCleanups);self.fixture.setUp()
        f=self.fixture;self.host=f.host;self.block=None;self.entered=threading.Event();self.release=threading.Event();self.claims=[];self.lose_claim=False
        self.addCleanup(self.release.set)
        owner=self
        class API:
            def handle(self,method,path,headers,body=b'',**kw):
                if path.endswith('/claim'):owner.claims.append(bytes(body))
                if path.endswith(owner.block or 'NOT-A-ROUTE'):
                    owner.entered.set();owner.release.wait(3)
                result=f.host.api.handle(method,path,headers,body,**kw)
                if path.endswith('/claim') and owner.lose_claim and result.status==200:
                    owner.lose_claim=False;return Response(503,{'error':'synthetic_lost_claim_ack'})
                return result
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(self.cert,self.key)
        self.coordinator=create_reference_http_server(API(),ssl_context=context)
        self.coordinator_thread=threading.Thread(target=self.coordinator.serve_forever,kwargs={'poll_interval':.01});self.coordinator_thread.start()
        def stop():
            self.release.set();self.coordinator.shutdown();self.coordinator.server_close();self.coordinator_thread.join(3)
        self.addCleanup(stop)
        config=evidence.private_config(f.host.root,self.cert,self.key,f.host.worker.engine,f.host.queue.authority_id,
            coordinator='https://127.0.0.1:'+str(self.coordinator.server_port),gate='engine',gate_origin=f.host.gate_origin,
            worker='worker',instance='instance',bearer=evidence.TOKEN)
        path=f.host.root/'readiness-factory.json';path.write_text(json.dumps(config));path.chmod(0o600)
        self.driver,self.server=daemon.build_worker(daemon.load_config(str(path)));self.addCleanup(self.server.server_close)
    def threaded_step(self,path):
        self.block=path;self.entered.clear();self.release.clear();answers=[]
        thread=threading.Thread(target=lambda:answers.append(self.driver.step()));thread.start()
        def stop():self.release.set();thread.join(4);self.assertFalse(thread.is_alive())
        self.addCleanup(stop);self.assertTrue(self.entered.wait(3))
        return thread,answers
    def finish(self,thread):
        self.release.set();thread.join(4);self.assertFalse(thread.is_alive())
        self.assertIsNone(self.driver._readiness_rpc);self.assertIsNone(self.driver._active_step)
        self.assertFalse(self.driver._step_guard.locked());self.assertFalse(self.driver._lock.locked())
    def test_live_discovery_window_reads_real_empty_health_but_blocks_second_step_and_mutation(self):
        thread,answers=self.threaded_step('/next')
        self.assertEqual(self.driver.step(),{'state':'busy'})
        before=self.driver.path.read_bytes()
        self.assertEqual(daemon._work_health(self.driver),dict(workState='empty',retainedJobs=0,pendingJobs=0,awaitingAcceptance=0))
        self.assertEqual(before,self.driver.path.read_bytes())
        lease=self.driver._active_step
        with self.driver._lock,self.assertRaises(DriverHeld):lease.check(self.driver)
        self.finish(thread);self.assertEqual(answers,[{'state':'idle'}])
    def test_preclaim_window_excludes_only_exact_empty_bookkeeping_then_unknown_replays_original(self):
        job=self.host.submit(contents=(b'abc',))
        self.assertEqual(self.driver.step()['state'],'claim_pending')
        thread,answers=self.threaded_step('/claim')
        self.assertEqual(daemon._work_health(self.driver),dict(workState='idle',retainedJobs=1,pendingJobs=0,awaitingAcceptance=0))
        self.assertEqual(self.driver.step(),{'state':'busy'})
        self.lose_claim=True;self.finish(thread)
        self.assertEqual(answers[0]['state'],'retry_wait')
        self.assertEqual(daemon._work_health(self.driver)['workState'],'pending_work')
        self.block=None;result=self.driver.step();self.assertEqual(result['state'],'claimed')
        self.assertEqual(len(self.claims),2);self.assertEqual(self.claims[0],self.claims[1])
        with self.host.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claims').fetchone()[0],1)
        self.assertEqual(self.fixture.native_fixture.counts().get('initialize',0),0)
    def test_pending_reserved_capacity_prevents_idle_even_inside_typed_discovery_window(self):
        thread,answers=self.threaded_step('/next')
        self.driver.ledger.reserve(job_id='synthetic-orphan',generation=1,reservation_id='synthetic-orphan-reservation',manifest_sha256='a'*64,
            input_bytes=3,output_budget=100,environment='staging',ttl_seconds=60,worker_ready=True)
        with self.assertRaisesRegex(ValueError,'owned work'):daemon._work_health(self.driver)
        self.finish(thread)
    def test_arbitrary_lock_without_live_rpc_is_still_busy(self):
        with self.driver._lock:
            self.assertEqual(daemon._work_health(self.driver),dict(workState='busy',retainedJobs=None,pendingJobs=None,awaitingAcceptance=None))

    def test_actor_mutation_guard_is_denied_even_while_real_health_observer_owns_lock(self):
        at_open=threading.Event();health_holds_lock=threading.Event();checked=threading.Event();guard=[];answers=[]
        opener=self.driver._open;sql=daemon._health_sql
        def observed_open(request,*args,**kwargs):
            if request.full_url.endswith('/next'):
                at_open.set()
                if not health_holds_lock.wait(3):raise RuntimeError('health observation deadline')
                try:self.driver._active_step.check(self.driver);guard.append('PERMITTED')
                except DriverHeld:guard.append('denied')
                finally:checked.set()
            return opener(request,*args,**kwargs)
        def observed_sql(*args,**kwargs):
            value=sql(*args,**kwargs);health_holds_lock.set()
            if not checked.wait(3):raise RuntimeError('actor check deadline')
            return value
        with patch.object(self.driver,'_open',observed_open),patch.object(daemon,'_health_sql',observed_sql):
            thread=threading.Thread(target=lambda:answers.append(self.driver.step()));thread.start()
            try:
                self.assertTrue(at_open.wait(3))
                self.assertEqual(daemon._work_health(self.driver)['workState'],'empty')
            finally:health_holds_lock.set();thread.join(4)
        self.assertFalse(thread.is_alive());self.assertEqual(guard,['denied']);self.assertEqual(answers,[{'state':'idle'}])

    def test_preclaim_data_with_reservation_is_never_classified_as_idle(self):
        self.host.submit(contents=(b'abc',));self.assertEqual(self.driver.step()['state'],'claim_pending')
        with self.driver._db() as db:db.execute('UPDATE driver_jobs SET data=?',(json.dumps({'reservation':{'synthetic':True}}),))
        thread,_=self.threaded_step('/claim')
        self.assertEqual(daemon._work_health(self.driver)['workState'],'pending_work')
        self.finish(thread)
