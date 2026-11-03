import unittest

from processing.effective_option_plan import plan_effective_options


def opt(name, kind, value, domain):
    return {'name': name, 'type': kind, 'value': value, 'domain': domain}


class EffectiveOptionPlanTests(unittest.TestCase):
    def setUp(self):
        self.small = [opt('matcher-type', 'enum', 'flann', ['bow', 'bruteforce', 'flann']),
                      opt('max-concurrency', 'int', '8', 'positive integer'),
                      opt('pc-quality', 'enum', 'medium', ['ultra', 'high', 'medium', 'low', 'lowest']),
                      opt('gltf', 'bool', 'false', ''),
                      opt('cameras', 'string', '', 'json')]
        self.large = [opt('matcher-type', 'enum', 'auto', ['auto', 'bow', 'bruteforce', 'flann', 'hamming']),
                      opt('max-concurrency', 'int', '32', 'positive integer'),
                      opt('pc-quality', 'enum', 'medium', ['ultra', 'high', 'medium', 'low', 'lowest']),
                      opt('gltf', 'bool', 'false', ''),
                      opt('cameras', 'string', '', 'json')]
        self.request = [{'name':'pc-quality','value':'low'}, {'name':'gltf','value':True}]

    def test_reviewed_differences_propose_only_the_semantic_pin(self):
        plan = plan_effective_options(self.small, self.large, self.request,
                                      {'matcher-type':'pin-default', 'max-concurrency':'node-local'})
        self.assertEqual(plan['state'], 'proposal')
        self.assertEqual(plan['pins'], [{'name':'matcher-type','value':'flann'}])
        self.assertFalse(plan['authorizesDispatch'])

    def test_unreviewed_or_missing_definitions_never_fall_through(self):
        self.assertEqual(plan_effective_options(self.small,self.large,self.request,{})['state'], 'held')
        main = self.large + [opt('report-units','enum','m',['m','ft'])]
        plan = plan_effective_options(self.small,main,self.request,
                                      {'matcher-type':'pin-default','max-concurrency':'node-local'})
        self.assertEqual(plan['state'], 'held')
        self.assertIn('report-units:missing_definition',plan['reasons'])
        self.assertEqual(plan['pins'], [])

    def test_source_default_must_be_valid_on_target(self):
        self.large[0]['domain']=['auto','bow']
        self.assertEqual(plan_effective_options(self.small,self.large,self.request,
                         {'matcher-type':'pin-default','max-concurrency':'node-local'})['state'], 'held')

    def test_special_or_malformed_requests_hold(self):
        for request in ([{'name':'cameras','value':'/path/file'}],
                        [{'name':'gltf','value':'true'}],
                        [{'name':'pc-quality','value':'unknown'}],
                        [{'name':'pc-quality','value':'low'},{'name':'pc-quality','value':'high'}]):
            with self.subTest(request=request):
                self.assertEqual(plan_effective_options(self.small,self.large,request,
                                 {'matcher-type':'pin-default','max-concurrency':'node-local'})['state'], 'held')

    def test_descriptive_numeric_domains_and_unknown_domain_fail_closed(self):
        source=self.small+[opt('mesh-octree-depth','int','11','integer: 1 <= x <= 14')]
        target=self.large+[opt('mesh-octree-depth','int','11','integer: 1 <= x <= 14')]
        policy={'matcher-type':'pin-default','max-concurrency':'node-local'}
        self.assertEqual(plan_effective_options(source,target,self.request,policy)['state'], 'proposal')
        target[-1]['domain']='uninterpretable numeric range'
        self.assertEqual(plan_effective_options(source,target,self.request,policy)['state'], 'held')

    def test_catalog_types_defaults_and_policy_are_never_implicitly_trusted(self):
        policy={'matcher-type':'pin-default','max-concurrency':'node-local'}
        for changed in (opt('new-engine-option','future','x',''),
                        opt('negative-default','int','-1','positive integer'),
                        opt('ambiguous-default','float','NaN','float')):
            with self.subTest(changed=changed):
                self.assertEqual(plan_effective_options(self.small+[changed],self.large+[changed],self.request,policy)['state'], 'held')
        self.assertEqual(plan_effective_options(self.small,self.large,self.request,
                         {'matcher-type':'node-local','max-concurrency':'node-local'})['state'], 'held')


if __name__ == '__main__':
    unittest.main()
