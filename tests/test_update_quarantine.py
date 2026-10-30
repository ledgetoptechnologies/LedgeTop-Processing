"""Quarantined updater profile: no real services or Docker mutations."""
import copy
import json
import unittest
from unittest.mock import patch

from ops import update_reference_coordinator as u


class Transport:
    def __init__(self,item): self.item=item;self.calls=[]
    def inspected(self,identifier): self.calls.append(('inspect',identifier));return copy.deepcopy(self.item)
    def facts(self,item,reference=False):
        assert reference and item['HostConfig']['NetworkMode']=='bridge'
        assert item['HostConfig']['RestartPolicy']['Name']=='unless-stopped'
    def docker(self,*args,**kwargs):
        self.calls.append(args)
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
        self.obj.validate=lambda state:None;self.obj.incremental_binding=object()
        with self.assertRaisesRegex(u.Stopped,'a75_quarantine_activation_not_qualified'):
            self.obj.activate(self.state)
        self.assertEqual(self.obj.t.calls,[])


if __name__=='__main__':unittest.main()
