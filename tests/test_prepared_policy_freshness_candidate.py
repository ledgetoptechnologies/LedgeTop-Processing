import copy
from types import SimpleNamespace
import unittest

from processing.options import catalog
from processing.prepared_policy_freshness_candidate import PreparedPolicyHeld, inspect_policy_held, _numeric_domain_supported
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


class NumericDomainGrammarTests(unittest.TestCase):
    def test_observed_integer_domains_and_strict_subsets(self):
        cases=[('integer',None,True),('positive integer',{'min':1,'max':100},True),
               ('positive integer',{'min':0,'max':100},False),
               ('integer: 1 <= x <= 14',{'min':1,'max':14},True),
               ('integer: 1 <= x <= 14',{'min':1,'max':15},False),
               ('integer: 1 <= x <= 14',[1,14],True),
               ('integer: 1 <= x <= 14',[0,14],False),
               ('integer: 1 <= x <= 14',None,False),
               ('integer: 1 <= x <= 15',{'min':1,'max':14},False)]
        for text,domain,expected in cases:
            with self.subTest(engine=text,configured=domain):
                self.assertEqual(_numeric_domain_supported({'type':'int','domain':text},{'type':'int','domain':domain}),expected)

    def test_positive_float_rejects_zero_and_unknown_grammar(self):
        for text in ('positive float','float > 0.0'):
            for minimum,expected in ((0,False),(0.01,True),(-1,False)):
                with self.subTest(engine=text,minimum=minimum):
                    self.assertEqual(_numeric_domain_supported({'type':'float','domain':text},
                        {'type':'float','domain':{'min':minimum,'max':10}}),expected)
        self.assertFalse(_numeric_domain_supported({'type':'float','domain':'float: unknown'},
            {'type':'float','domain':{'min':1,'max':10}}))
        self.assertFalse(_numeric_domain_supported({'type':'int','domain':'integer'},
            {'type':'float','domain':None}))

    def test_float_positive_integer_accepts_only_explicit_whole_values(self):
        engine={'type':'float','domain':'positive integer'}
        cases=[([1,2.0,14],True),([1,2.5],False),([0,1],False),([-1],False),([],False),
               ({'min':1,'max':1},True),({'min':1.5,'max':1.5},False),
               ({'min':0,'max':0},False),({'min':1,'max':2},False),(None,False)]
        for domain,expected in cases:
            with self.subTest(domain=domain):
                self.assertEqual(_numeric_domain_supported(engine,{'type':'float','domain':domain}),expected)
        self.assertFalse(_numeric_domain_supported({'type':'float','domain':'positive integers'},
            {'type':'float','domain':[1]}))

    def test_float_integer_domain_passes_real_shaped_common_catalog(self):
        fixture=PreparedPolicyFreshnessTests('test_current_fresh_policy_only_returns_held')
        fixture.setUp();self.addCleanup(fixture.doCleanups)
        for engine in (fixture.local,fixture.main):
            engine.options=[{'name':'engine-flag','type':'float','domain':'positive integer','value':'2'}]
        capability,options,_=discovered_policy(catalog(fixture.nodes),fixture.config)
        policy=dict(capabilityFingerprint=capability,optionsFingerprint=options,availableEngines=['odm'],
            optionDefinitions={'engine-flag':{'type':'float','domain':[1,2,3]}})
        self.assertEqual(fixture.check(policy)['state'],'held')
        policy['optionDefinitions']['engine-flag']['domain']={'min':1,'max':3}
        with self.assertRaises(PreparedPolicyHeld):fixture.check(policy)

    def test_boolean_and_string_text_are_not_guessed(self):
        for kind,domain in (('bool','boolean'),('string','string')):
            self.assertFalse(_numeric_domain_supported({'type':kind,'domain':domain},
                {'type':kind,'domain':None}))

    def test_real_shaped_domain_runs_through_actual_catalog(self):
        fixture=PreparedPolicyFreshnessTests('test_current_fresh_policy_only_returns_held')
        fixture.setUp();self.addCleanup(fixture.doCleanups)
        for engine in (fixture.local,fixture.main):
            engine.options=[{'name':'mesh-depth','type':'int','domain':'integer: 1 <= x <= 14','value':'11'}]
        capability,options,_=discovered_policy(catalog(fixture.nodes),fixture.config)
        policy=dict(capabilityFingerprint=capability,optionsFingerprint=options,availableEngines=['odm'],
                    optionDefinitions={'mesh-depth':{'type':'int','domain':{'min':1,'max':14}}})
        self.assertEqual(fixture.check(policy)['state'],'held')


if __name__=='__main__':unittest.main()
