"""Synthetic hashing vectors only; neither claim nor signature verified."""
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from processing import reference_protocol as p,reference_v2_event_transition_candidate as t

class TransitionTests(unittest.TestCase):
    def setUp(self):
        self.fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v2-transition-experiment.json').read_text())
        self.body=json.loads(self.fixture['canonicalBody']);self.flag=patch.object(t,'QUALIFIED',True);self.flag.start();self.addCleanup(self.flag.stop)
    def test_deterministic_vector(self):
        self.assertEqual(t.calculate(self.fixture['previousDigest'],self.fixture['canonicalBody'].encode()),self.fixture['nextDigest'])
        self.assertNotEqual(t.calculate('1'*64,self.fixture['canonicalBody'].encode()),self.fixture['nextDigest'])
    def test_invalid_bounds_and_noncanonical_hold(self):
        for change in (lambda b:b.update(previousRevision=p.MAX_SAFE_INTEGER,nextRevision=p.MAX_SAFE_INTEGER),lambda b:b.update(nextRevision=2),lambda b:b.update(chargedBytes=2),lambda b:b.update(objectLength=0,chargedBytes=0),lambda b:b.update(generation=True),lambda b:b.update(range={'kind':'partial'}),lambda b:b.update(requestId='bad')):
            value=copy.deepcopy(self.body);change(value)
            with self.subTest(value=value),self.assertRaises(t.TransitionHeld):t.calculate('0'*64,p.canonical_json(value))
        with self.assertRaises(t.TransitionHeld):t.calculate('0'*64,self.fixture['canonicalBody'].encode()+b' ')
        with self.assertRaises(t.TransitionHeld):t.calculate('A'*64,self.fixture['canonicalBody'].encode())
    def test_disabled(self):
        with patch.object(t,'QUALIFIED',False),self.assertRaises(t.TransitionHeld):t.calculate('0'*64,self.fixture['canonicalBody'].encode())
