"""Real queue CAS for early drain; no host/container actions."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
import hashlib
import tempfile
import unittest
import uuid

from processing.prototype import Queue,PROFILE
from processing.maintenance import Conflict,canonical

IMAGE='sha256:'+'b'*64


class PendingQualificationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.queue=Queue(self.temp.name,node_ids=('small','gpu-third'))
        self.addCleanup(self.queue.close)

    def request(self,**changes):
        node=self.queue.get_node('gpu-third')
        value=dict(schemaVersion=2,requestId=str(uuid.uuid4()),authorityId=self.queue.authority_id,
            expectedRevision=node['revision'],drainAt='2026-01-01T00:00:00Z',notBefore='2026-01-01T00:00:00Z',
            afterJobId=None,targetImage=IMAGE,targetEngineVersion=None,qualificationPending=True)
        value.update(changes);return value

    def prepare(self,**changes):
        request=self.request(**changes)
        receipt=self.queue.schedule_node_owned('gpu-third',request)
        return request,receipt

    def binding(self,request,receipt,**changes):
        current=self.queue.get_node('gpu-third')
        value=dict(schemaVersion=3,requestId=str(uuid.uuid4()),authorityId=self.queue.authority_id,
            expectedRevision=current['revision'],planId=receipt['planId'],drainRequestId=request['requestId'],
            drainRequestSha256=receipt['requestSha256'],targetImage=IMAGE,targetEngineVersion='3.8.5',
            qualificationSha256='e'*64)
        value.update(changes);return value

    def begin(self):
        node=self.queue.get_node('gpu-third')
        return self.queue.begin_node_update('gpu-third',dict(planId=node['planId'],expectedRevision=node['revision']))

    def test_same_owned_plan_binds_qualified_version_without_replacing_boundary(self):
        request,receipt=self.prepare()
        with self.assertRaisesRegex(Conflict,'awaits qualified'):self.begin()
        bound=self.queue.schedule_node_owned('gpu-third',self.binding(request,receipt))
        self.assertEqual(bound['planId'],receipt['planId'])
        for key in ('targetImage','drainAt','notBefore','afterJobId'):self.assertEqual(bound[key],receipt[key])
        self.assertEqual(bound['targetEngineVersion'],'3.8.5')
        self.assertEqual(self.begin()['node']['state'],'updating')

    def test_exact_after_job_drains_before_qualification_and_preserves_running_claim(self):
        self.queue.submit(dict(requestId='occupy-small',profile=PROFILE,environment='staging'))
        self.queue.claim(dict(workerId='small-worker',nodeId='small'))
        job=self.queue.submit(dict(requestId='before-update',profile=PROFILE,environment='staging'))[0]
        claim=self.queue.claim(dict(workerId='worker',nodeId='gpu-third'))
        self.assertEqual(claim['id'],job['id'])
        request,receipt=self.prepare(afterJobId=job['id'])
        self.assertEqual(self.queue.get(job['id'])['status'],'running')
        self.assertEqual(receipt['state'],'draining')
        self.queue.submit(dict(requestId='later',profile=PROFILE,environment='staging'))
        self.assertIsNone(self.queue.claim(dict(workerId='later-worker',nodeId='gpu-third')))
        self.queue.complete(job['id'],dict(claimToken=claim['claimToken'],status='succeeded',result={}))
        self.queue.schedule_node_owned('gpu-third',self.binding(request,receipt))
        self.assertEqual(self.begin()['node']['state'],'updating')

    def test_no_legacy_null_version_plan_gains_qualification_binding_authority(self):
        request=self.request();request.pop('qualificationPending');request['schemaVersion']=1
        receipt=self.queue.schedule_node_owned('gpu-third',request)
        with self.assertRaises((Conflict,ValueError)):
            self.queue.schedule_node_owned('gpu-third',self.binding(request,receipt))
        # Historical semantics stay unchanged; existing execution/version gates
        # still refuse unsafe automated use of this unbound legacy plan.
        self.assertEqual(self.begin()['node']['state'],'updating')

    def test_changed_revision_target_owner_or_drain_hash_refuses(self):
        request,receipt=self.prepare()
        for changes in (dict(expectedRevision=0),dict(targetImage='sha256:'+'c'*64),
                        dict(authorityId=str(uuid.uuid4())),dict(drainRequestSha256='f'*64)):
            with self.subTest(changes=changes),self.assertRaises(Conflict):
                self.queue.schedule_node_owned('gpu-third',self.binding(request,receipt,**changes))
        self.assertIsNone(self.queue.get_node('gpu-third')['targetEngineVersion'])

    def test_cancelled_plan_is_never_adopted_by_late_binding(self):
        request,receipt=self.prepare()
        wire=self.binding(request,receipt)
        node=self.queue.get_node('gpu-third')
        self.queue.cancel_node('gpu-third',dict(planId=node['planId'],expectedRevision=node['revision']))
        with self.assertRaises(Conflict):self.queue.schedule_node_owned('gpu-third',wire)
        self.assertEqual(self.queue.get_node('gpu-third')['state'],'active')

    def test_bind_reply_replay_is_immutable_after_later_cancel(self):
        request,receipt=self.prepare();wire=self.binding(request,receipt)
        bound=self.queue.schedule_node_owned('gpu-third',wire)
        node=self.queue.get_node('gpu-third')
        self.queue.cancel_node('gpu-third',dict(planId=node['planId'],expectedRevision=node['revision']))
        self.assertEqual(self.queue.schedule_node_owned('gpu-third',wire),bound)
        self.assertEqual(self.queue.get_node('gpu-third')['state'],'active')
        changed=dict(wire,targetEngineVersion='9.0')
        with self.assertRaises(Conflict):self.queue.schedule_node_owned('gpu-third',changed)

    def test_concurrent_bindings_allow_one_cas_and_exact_replay(self):
        request,receipt=self.prepare();first=self.binding(request,receipt);second=self.binding(request,receipt)
        def apply(wire):
            try:return self.queue.schedule_node_owned('gpu-third',wire)
            except Conflict:return None
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(apply,(first,second)))
        self.assertEqual(sum(result is not None for result in results),1)
        accepted=first if results[0] is not None else second
        self.assertEqual(self.queue.schedule_node_owned('gpu-third',accepted),next(r for r in results if r is not None))

    def test_old_receipt_hash_and_readonly_lookup_remain_exact(self):
        request=self.request();request.pop('qualificationPending');request['schemaVersion']=1
        request['targetEngineVersion']='3.8.5'
        receipt=self.queue.schedule_node_owned('gpu-third',request)
        expected=hashlib.sha256(canonical({k:v for k,v in receipt.items() if k!='receiptSha256'}).encode()).hexdigest()
        self.assertEqual(receipt['receiptSha256'],expected)
        self.assertNotIn('qualificationPending',receipt)
        self.assertEqual(self.queue.owned_schedule_receipt('gpu-third',request['requestId']),receipt)

    def test_boundary_mutation_without_revision_change_still_refuses_bind(self):
        request,receipt=self.prepare()
        wire=self.binding(request,receipt)
        with self.queue._db() as db:
            db.execute("UPDATE nodes SET drain_at='2026-02-01T00:00:00+00:00' WHERE id='gpu-third'")
        with self.assertRaisesRegex(Conflict,'original qualification drain changed'):
            self.queue.schedule_node_owned('gpu-third',wire)

    def test_finished_requested_job_cannot_be_substituted_with_a_new_job(self):
        self.queue.submit(dict(requestId='occupy-small',profile=PROFILE,environment='staging'))
        self.queue.claim(dict(workerId='small-worker',nodeId='small'))
        job=self.queue.submit(dict(requestId='exact-original',profile=PROFILE,environment='staging'))[0]
        claim=self.queue.claim(dict(workerId='original',nodeId='gpu-third'))
        wire=self.request(afterJobId=job['id'])
        self.queue.complete(job['id'],dict(claimToken=claim['claimToken'],status='succeeded',result={}))
        self.queue.submit(dict(requestId='next',profile=PROFILE,environment='staging'))
        other=self.queue.claim(dict(workerId='other',nodeId='gpu-third'))
        self.assertNotEqual(other['id'],job['id'])
        with self.assertRaisesRegex(Conflict,'not currently running'):
            self.queue.schedule_node_owned('gpu-third',wire)
        self.assertEqual(self.queue.get_node('gpu-third')['state'],'active')

    def test_pending_wire_is_closed_and_never_implicitly_qualifies(self):
        for change in (dict(qualificationPending=False),dict(targetEngineVersion='3.8.5'),dict(schemaVersion=True)):
            with self.subTest(change=change),self.assertRaises(ValueError):
                self.queue.schedule_node_owned('gpu-third',self.request(**change))
