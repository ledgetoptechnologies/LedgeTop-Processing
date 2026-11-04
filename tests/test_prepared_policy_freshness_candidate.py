import copy
from types import SimpleNamespace
import unittest

from processing.options import catalog
from processing.prepared_policy_freshness_candidate import PreparedPolicyHeld, inspect_policy_held
from processing.reference_runtime import discovered_policy
from tests.test_options import Engine


class PreparedPolicyFreshnessTests(unittest.TestCase):
    def setUp(self):
        self.local=Engine();self.main=Engine();self.addCleanup(self.local.close);self.addCleanup(self.main.close)
        self.local.options=[{'name':'resolution','type':'float','domain':{'min':1,'max':100},'value':10}]
        self.main.options=copy.deepcopy(self.local.options)
        self.nodes=[('local',self.local.origin,self.local.token),('main',self.main.origin,self.main.token)]
        self.config=SimpleNamespace(local_max_photos=1000,local_max_input_bytes=20*1024**3,node_inventory=())
        capability,options,_=discovered_policy(catalog(self.nodes),self.config)
        self.policy=dict(capabilityFingerprint=capability,optionsFingerprint=options,
            availableEngines=['odm'],optionDefinitions={'resolution':{'type':'float','domain':{'min':1,'max':100}}})

    def check(self,policy=None):
        return inspect_policy_held(nodes=self.nodes,config=self.config,configured_policy=policy or self.policy)

    def test_current_fresh_policy_only_returns_held(self):
        self.assertEqual(self.check()['state'],'held')
        self.assertGreaterEqual(len(self.local.requests),4)
        self.assertGreaterEqual(len(self.main.requests),4)

    def test_removed_option_or_changed_node_fails_closed(self):
        self.main.options=[{'name':'different','type':'bool','domain':None,'value':False}]
        with self.assertRaises(PreparedPolicyHeld):self.check()
        self.main.options=copy.deepcopy(self.local.options)
        self.main.info['engineVersion']='changed-node-version'
        with self.assertRaises(PreparedPolicyHeld):self.check()

    def test_offline_fleet_fails_closed(self):
        self.main.mode='unavailable'
        with self.assertRaises(PreparedPolicyHeld):self.check()

    def test_policy_domain_and_type_must_match_explicit_engine_domain(self):
        for change in ({'type':'float','domain':{'min':0,'max':100}},
                       {'type':'int','domain':{'min':1,'max':100}}):
            policy=copy.deepcopy(self.policy);policy['optionDefinitions']['resolution']=change
            with self.subTest(change=change),self.assertRaises(PreparedPolicyHeld):self.check(policy)
        self.local.options[0].pop('domain');self.main.options[0].pop('domain')
        # Even a matching new capability cannot infer an absent domain.
        capability,options,_=discovered_policy(catalog(self.nodes),self.config)
        policy=dict(self.policy,capabilityFingerprint=capability,optionsFingerprint=options)
        with self.assertRaises(PreparedPolicyHeld):self.check(policy)

    def test_unknown_configured_option_is_refused_even_with_current_fingerprint(self):
        policy=copy.deepcopy(self.policy);policy['optionDefinitions']['unknown']={'type':'bool','domain':None}
        with self.assertRaises(PreparedPolicyHeld):self.check(policy)


if __name__=='__main__':unittest.main()
