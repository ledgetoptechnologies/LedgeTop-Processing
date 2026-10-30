"""Quarantined updater profile: no real services or Docker mutations."""
import copy
import json
import unittest
from unittest.mock import patch

from ops import update_reference_coordinator as u


class Transport:
    def __init__(self,item): self.item=item;self.calls=[];self.health_failures=0;self.change_after_health=False
    def inspected(self,identifier,**kwargs): self.calls.append(('inspect',identifier));return copy.deepcopy(self.item)
    def facts(self,item,reference=False):
        assert reference and item['HostConfig']['NetworkMode']=='bridge'
        assert item['HostConfig']['RestartPolicy']['Name']=='unless-stopped'
    def docker(self,*args,**kwargs):
        self.calls.append(args)
        if self.health_failures:
            self.health_failures-=1
            if self.change_after_health:self.item['HostConfig']['NetworkMode']='bridge'
            raise OSError('not yet listening')
        return json.dumps({'status':'ok','viewerEnabled':True,'syntheticPrototypeEnabled':True})


class QuarantineUpdaterTests(unittest.TestCase):
    def setUp(self):
        self.state={'image':'sha256:'+'a'*64,'operation':'b'*32,'newContainerId':'c'*64,
                    'sourceCommit':u.QUARANTINED_A75_SOURCE,'applicationEnvironmentSha256':'d'*64}
        self.item={'Id':self.state['newContainerId'],'Name':'/'+u.NAME,'Image':self.state['image'],
                   'Config':{'Labels':{u.LABEL:self.state['operation']}},'State':{'Running':True},
                   'HostConfig':{'NetworkMode':'none','PortBindings':None,'RestartPolicy':{'Name':'no','MaximumRetryCount':0}},
                   'NetworkSettings':{'Ports':{},'Networks':{'none':{}}}}
        self.obj=object.__new__(u.ReferenceCoordinatorUpdate);self.obj.t=Transport(self.item)

    def test_create_has_no_ports_no_network_and_no_restart(self):
        args=self.obj.quarantine_create_args(self.state)
        self.assertEqual(args[args.index('--network')+1],'none')
        self.assertEqual(args[args.index('--restart')+1],'no')
        self.assertNotIn('-p',args)
        self.assertEqual(args[-1],self.state['image'])

    def test_verification_probes_internal_health_and_rejects_isolation_changes(self):
        with patch.object(u,'application_environment',return_value=self.state['applicationEnvironmentSha256']):
            self.assertEqual(self.obj.verify_quarantined_candidate(self.state)['internalHealth'],'ok')
            cases=[('NetworkMode','bridge'),('PortBindings',{'8080/tcp':[{'HostPort':'8080'}]}),
                   ('RestartPolicy',{'Name':'unless-stopped','MaximumRetryCount':0})]
            for key,value in cases:
                with self.subTest(key=key):
                    original=self.obj.t.item['HostConfig'][key];self.obj.t.item['HostConfig'][key]=value
                    with self.assertRaises(u.Stopped):self.obj.verify_quarantined_candidate(self.state)
                    self.obj.t.item['HostConfig'][key]=original
            self.obj.t.item['NetworkSettings']['Networks']['extra']={}
            with self.assertRaises(u.Stopped):self.obj.verify_quarantined_candidate(self.state)

    def test_a75_is_hard_disabled_before_transport_or_mutation(self):
        self.obj.validate=lambda state:None
        for binding in (None,object()):
            self.obj.incremental_binding=binding
            with self.assertRaisesRegex(u.Stopped,'a75_quarantine_activation_not_qualified'):
                self.obj.activate(self.state)
        with patch.object(u,'QUARANTINED_A75_REVIEWED',True):
            self.obj.incremental_binding=None
            with self.assertRaisesRegex(u.Stopped,'a75_incremental_profile_required'):
                self.obj.activate(self.state)
        self.assertEqual(self.obj.t.calls,[])

    def test_health_retries_once_and_checks_identity_again(self):
        self.obj.t.health_failures=1
        with patch.object(u,'application_environment',return_value=self.state['applicationEnvironmentSha256']):
            self.assertEqual(self.obj.verify_quarantined_candidate(self.state)['internalHealth'],'ok')
        self.assertEqual(sum(call[0]=='exec' for call in self.obj.t.calls),2)
        self.assertEqual(sum(call[0]=='inspect' for call in self.obj.t.calls),3)

    def test_identity_change_during_health_retry_fails_closed(self):
        self.obj.t.health_failures=1;self.obj.t.change_after_health=True
        with patch.object(u,'application_environment',return_value=self.state['applicationEnvironmentSha256']):
            with self.assertRaisesRegex(u.Stopped,'quarantine_candidate_not_isolated'):
                self.obj.verify_quarantined_candidate(self.state)
        self.assertEqual(sum(call[0]=='exec' for call in self.obj.t.calls),1)

    def test_health_deadline_fails_without_mutations(self):
        self.obj.t.health_failures=100
        clock=iter((0,1,2,11))
        with patch.object(u,'application_environment',return_value=self.state['applicationEnvironmentSha256']), \
             patch.object(u.time,'monotonic',side_effect=lambda:next(clock)), patch.object(u.time,'sleep'):
            with self.assertRaisesRegex(u.Stopped,'quarantine_health_unavailable'):
                self.obj.verify_quarantined_candidate(self.state)
        self.assertTrue(all(call[0] in ('inspect','exec') for call in self.obj.t.calls))

    def test_a75_no_rollback_retain_or_reopen_until_explicitly_qualified(self):
        with self.assertRaisesRegex(u.Stopped,'a75_rollback_not_qualified'):self.obj.rollback(self.state)
        with self.assertRaisesRegex(u.Stopped,'a75_rollback_not_qualified'):self.obj.finish_rollback(self.state,None)
        with self.assertRaisesRegex(u.Stopped,'a75_retention_not_qualified'):self.obj.retain(self.state,None,None)
        with self.assertRaisesRegex(u.Stopped,'a75_reopen_not_qualified'):self.obj.reopen(self.state,None,None,None)
        self.assertEqual(self.obj.t.calls,[])

    def test_create_rejects_modified_template(self):
        with patch.object(self.obj,'create_args',return_value=('create','--network','host','--restart','unless-stopped')):
            with self.assertRaisesRegex(u.Stopped,'quarantine_create_template_changed'):
                self.obj.quarantine_create_args(self.state)


if __name__=='__main__':unittest.main()
