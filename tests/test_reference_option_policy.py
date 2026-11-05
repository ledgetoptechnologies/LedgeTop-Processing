"""Shared policy agrees with coordinator advertisement and exact worker values."""
from types import SimpleNamespace
import unittest

from processing.reference_option_policy import safe_option_definitions, validate_option_values
from processing.reference_runtime import discovered_policy, validate_discovered_option_values


CATALOG = [
    dict(name='pc-quality',type='enum',domain=['low','medium','high']),
    dict(name='feature-quality',type='string',domain='["normal","ultra"]'),
    dict(name='label',type='string',domain='A short survey label'),
    dict(name='resolution',type='double',domain='0 < x <= 5'),
    dict(name='count',type='integer',domain={'min':1,'max':3}),
    dict(name='3d-tiles',type='boolean'),
]


class SharedOptionPolicyTests(unittest.TestCase):
    def test_runtime_and_worker_agree_on_fresh_enum_string_alias_and_domains(self):
        snapshot=dict(ready=True,commonOptions=CATALOG,nodes=[dict(role='cluster',available=True)])
        _,_,allowed=discovered_policy(snapshot,SimpleNamespace(local_max_photos=1000,local_max_input_bytes=1024))
        definitions=safe_option_definitions(CATALOG)
        self.assertEqual(allowed,frozenset(definitions))
        values={'pc-quality':'high','feature-quality':'ultra','label':'Survey A','resolution':0.5,'count':3,'3d-tiles':True}
        validate_option_values(values,definitions)
        validate_discovered_option_values({'pipeline':{'odm':{'options':values}}},snapshot,allowed)
        for name,value in [('pc-quality','other'),('feature-quality',True),('resolution',0),('resolution',6),
                ('count',True),('count',1.5),('count',4),('3d-tiles','true'),('label','x'*4001),
                ('label','https://example.com'),('label','/etc/passwd'),('label',r'C:\data'),('label','../data')]:
            bad={**values,name:value}
            with self.subTest(name=name,value=str(value)[:30]):
                with self.assertRaises(ValueError):validate_option_values(bad,definitions)
                with self.assertRaises(ValueError):validate_discovered_option_values({'pipeline':{'odm':{'options':bad}}},snapshot,allowed)

    def test_unsafe_definitions_removed_and_domain_changes_are_effective(self):
        unsafe=[dict(name='access-token',type='string'),dict(name='target',type='string',help='File path'),
            dict(name='gcp',type='string'),dict(name='choice',type='enum',domain=['safe','/private']),
            dict(name='network',type='enum',domain=['https://example.com']),dict(name='bad',type='int',domain={'min':4,'max':1})]
        self.assertEqual(safe_option_definitions(unsafe),{})
        old=safe_option_definitions([dict(name='pc-quality',type='enum',domain=['high'])])
        validate_option_values({'pc-quality':'high'},old)
        for catalog in ([],[dict(name='pc-quality',type='enum',domain=['low'])],
                [dict(name='pc-quality',type='string',help='External script command')]):
            with self.assertRaises(ValueError):
                validate_option_values({'pc-quality':'high'},safe_option_definitions(catalog))

    def test_huge_integer_domain_bounds_fail_closed_without_overflow(self):
        catalog=[dict(name='count',type='integer',domain={'max':10**400})]
        self.assertEqual(safe_option_definitions(catalog),{})
        snapshot=dict(ready=True,commonOptions=catalog,nodes=[dict(role='cluster',available=True)])
        _,_,allowed=discovered_policy(snapshot,SimpleNamespace(local_max_photos=1000,local_max_input_bytes=1024))
        self.assertEqual(allowed,frozenset())
        with self.assertRaises(ValueError):
            validate_option_values({'count':1},safe_option_definitions(catalog))
