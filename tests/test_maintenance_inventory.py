import time
import unittest
from unittest.mock import patch
from ops import node_maintenance as m

class MaintenanceInventoryTests(unittest.TestCase):
    def test_outstanding_and_terminal_jobs(self):
        self.assertTrue(m.node_has_outstanding_work({'jobs':[{'status':'running','nodeId':'cluster'}]},'cluster'))
        self.assertFalse(m.node_has_outstanding_work({'jobs':[{'status':'running','nodeId':'main'},
            {'status':'queued','nodeId':None},{'status':'succeeded','nodeId':None}]},'cluster'))
    def test_incomplete_or_unknown_fails_closed_before_gate(self):
        class API:
            def __init__(self,response):self.response=response
            def json(self,*args):return self.response
        for response in ({}, {'jobs':None}, {'jobs':[{'status':'running'}]},
                         {'jobs':[{'status':'held','nodeId':None}]}, {'jobs':[{'status':'new-status','nodeId':'cluster'}]},
                         {'jobs':[None]}):
            with self.subTest(response=response),patch.object(m,'NodeGate',side_effect=AssertionError('must not acquire')):
                with self.assertRaises(m.GateError):
                    with m.idle_host_gate(API(response),'cluster','engine',time.monotonic()+1):pass
