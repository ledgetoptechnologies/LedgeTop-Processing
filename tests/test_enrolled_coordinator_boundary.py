"""Fresh boundary composition; fake Docker is not installed custody evidence."""
import copy
import hashlib
import unittest
import uuid
from unittest.mock import patch

from ops import enrolled_coordinator_boundary as b
from ops import enrolled_coordinator_transport as t
from tests.test_enrolled_coordinator_deployment import bundle


class BoundaryTests(unittest.TestCase):
    def test_cached_prior_observer_is_not_a_fresh_guard(self):
        value,plan=bundle()
        def stale(identity,source,**kwargs):
            return dict(protocol='ltds-cutover-exec-v1',operationId=plan['operationId'],requestId=str(uuid.uuid4()),
                        containerId=identity,sourceSha256=hashlib.sha256(source).hexdigest(),bodySucceeded=True,
                        process={'retired':True},result={})
        probe=b.BoundaryProbe(plan,value,None,stale)
        with self.assertRaises(t.TransportHeld):probe.fresh('a'*64,b'print("{}")')

    def test_writer_census_rejects_running_parent_bind_alias(self):
        value,plan=bundle();original=dict(Id=plan['predecessor']['containerId'],
            Mounts=[dict(Destination='/prototype-state',Name=value['predecessor']['stateVolume'],Source='/volumes/original')],
            State=dict(Running=False,Pid=0,FinishedAt='known'))
        alias=dict(Id='b'*64,Mounts=[dict(Source='/volumes')],State=dict(Running=True,Pid=123,FinishedAt=''))
        class Docker:
            def inspect(self,key):return original if key==original['Id'] else alias
            def matching(self,filters):return [original['Id'],alias['Id']]
        probe=b.BoundaryProbe(plan,value,Docker(),None)
        with self.assertRaises(t.TransportHeld):probe.writers('a'*64)
        alias['State']=dict(Running=False,Pid=0,FinishedAt='stopped')
        self.assertEqual(len(probe.writers('a'*64)),2)

    def test_holder_boundary_joins_actual_descriptor_identity_with_original_physical_pins(self):
        value,plan=bundle();physical=dict(state={'synthetic':'original'},authorityLease={'synthetic':'external'},
            operationBinding={'authorityLease':{'fileIdentity':[2,3,4,5,6,7]}})
        physical['state']['main']=[1,2,3,4,5,6]
        plan['bindings']['stateSha256']=t.digest(physical['state'])
        plan['bindings']['authorityLeaseSha256']=t.digest(physical['authorityLease'])
        state=dict(Running=True,Pid=123,StartedAt='same-incarnation')
        class Docker:
            def inspect(self,key):return dict(State=state)
        probe=b.BoundaryProbe(plan,value,Docker(),None)
        held=dict(settings=probe.settings,pid=1,mainIdentity=physical['state']['main'],
                  leaseIdentity=physical['operationBinding']['authorityLease']['fileIdentity'])
        context=dict(containerId='a'*64,creationIntentId=str(uuid.uuid4()),gate=None)
        writers=[dict(containerId=plan['predecessor']['containerId'],running=False,pid=0,finishedAt='done')]
        with patch.object(probe,'host'),patch.object(probe,'container',return_value=dict(Image=plan['candidate']['imageId'],State=state)),\
             patch.object(probe,'writers',return_value=writers),patch.object(probe,'fresh',side_effect=[held,physical]):
            result=probe('holder',context)
        self.assertEqual(set(result),{'schemaVersion','role','container','holder','physical','custody','writers','gate'})
        bad=copy.deepcopy(held);bad['mainIdentity']=[9]*6
        with patch.object(probe,'host'),patch.object(probe,'container',return_value=dict(Image=plan['candidate']['imageId'],State=state)),\
             patch.object(probe,'writers',return_value=writers),patch.object(probe,'fresh',side_effect=[bad,physical]):
            with self.assertRaises(t.TransportHeld):probe('holder',context)

    def test_final_frozen_label_without_matching_plan_cannot_proceed(self):
        value,plan=bundle();probe=b.BoundaryProbe(plan,value,None,None)
        state=dict(Running=True,Pid=123,StartedAt='same')
        context=dict(containerId='a'*64,creationIntentId=str(uuid.uuid4()),
                     gate=dict(startupId=str(uuid.uuid4()),ownerSha256='1'*64))
        snapshot=dict(phase='frozen',revision=3,operations=0,planId=str(uuid.uuid4()),startupId=context['gate']['startupId'])
        with patch.object(probe,'host'),patch.object(probe,'container',return_value={'State':state}),\
             patch.object(probe,'writers',return_value=[]),patch.object(probe,'gate',return_value=dict(snapshot=snapshot,ownerSha256='1'*64)):
            with self.assertRaises(t.TransportHeld):probe('final',context)


if __name__=='__main__':unittest.main()
