"""Owned HELD recovery: synthetic SQLite and loopback HTTP only."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import tempfile
import unittest
import uuid
from urllib.error import HTTPError

from processing.prototype import Conflict, PROFILE, Queue
from tests import test_prototype_http as http_fixture
from tests.test_service import TOKEN

IMAGE = 'sha256:' + 'a' * 64


def owned_held(queue):
    now = datetime.now(timezone.utc).isoformat()
    original = str(uuid.uuid4())
    schedule = queue.schedule_node_owned('cluster', dict(schemaVersion=1, requestId=original,
        authorityId=queue.authority_id, expectedRevision=queue.get_node('cluster')['revision'],
        drainAt=now, notBefore=now, afterJobId=None, targetImage=IMAGE))
    begun = queue.begin_node_update('cluster', dict(expectedRevision=schedule['revision'], planId=schedule['planId']))
    held = queue.hold_node('cluster', dict(expectedRevision=begun['node']['revision'],
        planId=schedule['planId'], note='inspected test stop'))
    payload = dict(schemaVersion=1, requestId=str(uuid.uuid4()), originalOperationId=original,
        authorityId=queue.authority_id, expectedRevision=held['revision'], planId=held['planId'],
        targetImage=IMAGE, workerStopped=True, ingressClosed=True, note='stopped and closed inspected')
    return begun, payload


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.queue = Queue(self.root.name, node_ids=('cluster', 'main'))
        self.old, self.payload = owned_held(self.queue)

    def tearDown(self):
        self.queue.close()
        self.root.cleanup()

    def recover(self, **changes):
        return self.queue.recover_node_update('cluster', dict(self.payload, **changes))

    def test_lost_ack_exact_receipt_and_no_active_window(self):
        receipt = self.recover()
        self.assertEqual(receipt, self.recover())
        self.assertEqual(receipt, self.queue.owned_recovery_receipt('cluster', self.payload['requestId']))
        self.assertTrue(receipt['tokenValid'])
        self.assertNotEqual(receipt['updateToken'], self.old['updateToken'])
        self.assertEqual(self.queue.get_node('cluster')['state'], 'updating')
        self.queue.submit(dict(requestId='test', environment='staging', profile=PROFILE))
        self.assertIsNone(self.queue.claim(dict(workerId='worker', nodeId='cluster')))
        self.assertNotIn(receipt['updateToken'], json.dumps(self.queue.list_nodes()))
        with self.assertRaises(Conflict):
            self.queue.finish_node_update('cluster', dict(planId=self.payload['planId'],
                updateToken=self.old['updateToken'], status='succeeded', result=dict(healthy=True)))

    def test_acknowledgments_must_be_exact_true(self):
        for field in ('workerStopped', 'ingressClosed'):
            for value in (False, 1, 'true', None):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    self.recover(**{field: value})
        self.assertEqual(self.queue.get_node('cluster')['state'], 'held')

    def test_cas_plan_authority_target_and_original_operation(self):
        for change in (dict(expectedRevision=self.payload['expectedRevision']-1),
                       dict(planId=str(uuid.uuid4())), dict(authorityId=str(uuid.uuid4())),
                       dict(targetImage='sha256:'+'b'*64), dict(originalOperationId=str(uuid.uuid4()))):
            with self.subTest(change=change), self.assertRaises(Conflict):
                self.recover(**change)
        self.assertEqual(self.queue.get_node('cluster')['state'], 'held')

    def test_owned_operation_cannot_link_other_plan(self):
        now=datetime.now(timezone.utc).isoformat()
        other=self.queue.schedule_node_owned('main', dict(schemaVersion=1, requestId=str(uuid.uuid4()),
            authorityId=self.queue.authority_id, expectedRevision=0, drainAt=now, notBefore=now,
            afterJobId=None, targetImage=IMAGE))
        with self.assertRaises(Conflict):
            self.recover(originalOperationId=other['requestId'])

    def test_assigned_queued_running_and_held_jobs_refuse(self):
        job,_=self.queue.submit(dict(requestId='assigned', environment='staging', profile=PROFILE))
        for status in ('queued', 'running', 'held'):
            with self.queue._db() as db:
                db.execute('UPDATE jobs SET node_id=?,status=? WHERE id=?', ('cluster',status,job['id']))
            with self.subTest(status=status), self.assertRaises(Conflict):
                self.recover()

    def test_recovery_race_only_one_new_reservation(self):
        def run(request):
            try:
                return self.queue.recover_node_update('cluster', request)
            except Conflict:
                return None
        with ThreadPoolExecutor(max_workers=2) as executor:
            results=list(executor.map(run, (self.payload, dict(self.payload, requestId=str(uuid.uuid4())))))
        self.assertEqual(sum(result is not None for result in results), 1)
        self.assertEqual(self.queue.get_node('cluster')['revision'],self.payload['expectedRevision']+1)

    def test_concurrent_claim_never_observes_active_node(self):
        self.queue.submit(dict(requestId='waiting', environment='staging', profile=PROFILE))
        with ThreadPoolExecutor(max_workers=2) as executor:
            recovery=executor.submit(self.recover)
            claim=executor.submit(self.queue.claim,dict(workerId='racing',nodeId='cluster'))
            self.assertIsNone(claim.result())
            self.assertEqual(recovery.result()['state'],'updating')
        self.assertEqual(self.queue.list()[0]['status'],'queued')

    def test_restart_holds_and_does_not_replay_secret_reservation(self):
        receipt=self.recover()
        self.queue.close()
        self.queue=Queue(self.root.name,node_ids=('cluster','main'))
        self.assertEqual(self.queue.get_node('cluster')['state'],'held')
        readback=self.queue.owned_recovery_receipt('cluster',receipt['requestId'])
        self.assertFalse(readback['tokenValid'])
        self.assertNotIn('updateToken',readback)

    def test_hold_and_finish_invalidate_receipt_without_reissue(self):
        receipt=self.recover()
        self.queue.hold_node('cluster', dict(expectedRevision=receipt['revision'],planId=receipt['planId'],note='uncertain'))
        stale=self.recover()
        self.assertFalse(stale['tokenValid'])
        self.assertNotIn('updateToken',stale)
        with self.assertRaises(Conflict):
            self.queue.finish_node_update('cluster',dict(planId=receipt['planId'],updateToken=receipt['updateToken'],
                status='succeeded',result=dict(healthy=True)))
        fresh=self.recover(requestId=str(uuid.uuid4()),expectedRevision=receipt['revision']+1)
        self.assertNotEqual(fresh['updateToken'], receipt['updateToken'])
        self.queue.finish_node_update('cluster',dict(planId=fresh['planId'],updateToken=fresh['updateToken'],
            status='succeeded',result=dict(healthy=True)))
        self.assertFalse(self.queue.owned_recovery_receipt('cluster',fresh['requestId'])['tokenValid'])

    def test_request_uuid_rebinding_and_nonheld_states(self):
        receipt=self.recover()
        with self.assertRaises(Conflict):
            self.recover(note='changed')
        for state in ('active','draining','scheduled','failed','updating'):
            with self.queue._db() as db:
                db.execute('UPDATE nodes SET state=? WHERE id=?',(state,'cluster'))
            with self.subTest(state=state), self.assertRaises(Conflict):
                self.recover(requestId=str(uuid.uuid4()), expectedRevision=receipt['revision'])


class RecoveryHTTPTests(unittest.TestCase):
    setUp = http_fixture.PrototypeHTTPTests.setUp
    tearDown = http_fixture.PrototypeHTTPTests.tearDown
    request = http_fixture.PrototypeHTTPTests.request

    def test_operator_only_post_and_secret_receipt_get(self):
        _, payload=owned_held(self.handler.prototype_queue)
        path='/v1/prototype/nodes/cluster/recover-update'
        for token in (TOKEN, 'wrong'):
            with self.assertRaises(HTTPError) as caught:
                self.request(path,payload,token=token)
            self.assertEqual(caught.exception.code,401)
        _,response=self.request(path,payload,token=http_fixture.OPERATOR_TOKEN)
        receipt=response['receipt']
        for token in (TOKEN,'wrong'):
            with self.assertRaises(HTTPError) as caught:
                self.request(path+'/'+payload['requestId'],token=token)
            self.assertEqual(caught.exception.code,401)
        _,readback=self.request(path+'/'+payload['requestId'],token=http_fixture.OPERATOR_TOKEN)
        self.assertEqual(readback['receipt'],receipt)
        _,node=self.request('/v1/prototype/nodes/cluster')
        self.assertNotIn(receipt['updateToken'],json.dumps(node))
