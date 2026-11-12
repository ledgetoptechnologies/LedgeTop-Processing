"""Isolated partial-forward-write proof; no Docker/SSH or production paths."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ops import retained_main_engine_swap as S
from tests.test_retained_main_engine_swap import FakeRuntime

def record(path):
    raw = path.read_bytes()
    return {'value': json.loads(raw), 'sha256': hashlib.sha256(raw).hexdigest()}

class DualPinFailure(unittest.TestCase):
    def test_second_forward_write_failure_holds_before_reopen(self):
        for fail_after_replacement in (False, True):
            with self.subTest(fail_after_replacement=fail_after_replacement), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                historical, override, bootstrap = [root/n for n in ('rollback.json', 'override.json', 'bootstrap.json')]
                historical.write_text(json.dumps({'services': {'nodeodx': {'image': S.BASE}}}))
                override.write_text(json.dumps({'services': {'nodeodx': {'image': S.BASE}}}))
                bootstrap.write_text(json.dumps({'services': {'nodeodx': {'image': 'historical-bootstrap', 'command': None}}}))
                original = {p: p.read_bytes() for p in (historical, override, bootstrap)}
                r = FakeRuntime()
                r.prepared['sourceFiles'] = {str(p): record(p) for p in original}
                writes, states = [], []
                def write(path, value):
                    writes.append(path)
                    if path == bootstrap and not fail_after_replacement:
                        raise S.Held('synthetic second-write failure before replacement')
                    path.write_bytes(S.canonical(value))
                    if path == bootstrap:
                        raise S.Held('synthetic second-write failure after replacement')
                def actual_persist(prepared):
                    r.calls.append('actual_persist_pin')
                    return S.Runtime.persist_pin(r, prepared)
                r.persist_pin = actual_persist
                with patch.object(S, 'COMPOSE', historical), patch.object(S, 'OVERRIDE', override), patch.object(S, 'ROLE_COMPOSE', bootstrap), patch.object(S, 'source_record', record), patch.object(S, 'atomic_preserved_json', write):
                    with self.assertRaisesRegex(S.Held, 'second-write failure'):
                        S.apply(r, root, r.prepared, lambda state: states.append(copy.deepcopy(state)))
                self.assertEqual(writes, [override, bootstrap])
                self.assertEqual(historical.read_bytes(), original[historical])
                self.assertEqual(json.loads(override.read_text())['services']['nodeodx'], {'image': S.TARGET, 'command': [S.FLAG]})
                if fail_after_replacement:
                    self.assertEqual(json.loads(bootstrap.read_text())['services']['nodeodx'], {'image': S.TARGET, 'command': [S.FLAG]})
                else:
                    self.assertEqual(bootstrap.read_bytes(), original[bootstrap])
                self.assertEqual(states[-1]['stage'], 'held_for_inspection')
                self.assertTrue(r.closed)
                self.assertEqual(r.current['state'], 'held')
                self.assertEqual(r.calls.count('recreate_and_verify'), 1)
                self.assertEqual(r.calls.count('actual_persist_pin'), 1)
                self.assertNotIn('open', r.calls)
                self.assertNotIn('finish', r.calls)
                self.assertFalse(any(x in r.calls for x in ('rollback', 'replace_old', 'start_old')))

if __name__ == '__main__':
    unittest.main()
