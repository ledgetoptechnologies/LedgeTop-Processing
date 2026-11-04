"""Characterize why maintenance drain alone is not an exclusive test lease."""
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from processing.node_gate import NodeGate
from processing.prototype import PROFILE, Queue


class RehearsalDrainBoundary(unittest.TestCase):
    def test_cancel_can_reopen_while_physical_gate_is_still_reserved(self):
        with tempfile.TemporaryDirectory() as root:
            queue = Queue(str(Path(root) / 'queue'), node_ids=('cluster', 'main'))
            try:
                now = datetime.now(timezone.utc).isoformat()
                plan = queue.schedule_node('main', {'expectedRevision': 0, 'drainAt': now,
                    'notBefore': now, 'afterJobId': None, 'targetImage': 'sha256:' + 'a' * 64})
                self.assertEqual(plan['state'], 'draining')
                queue.submit({'requestId': 'queued', 'profile': PROFILE, 'environment': 'production'})
                with patch('processing.node_gate.canonical_gate_path', return_value=Path(root) / 'gate'):
                    with NodeGate('synthetic-engine') as gate:
                        origin = 'http://127.0.0.1:18080'
                        gate.provision('main', origin, queue.authority_id)
                        gate.reserve('main', origin, queue.authority_id, 1, 'a' * 32)
                        self.assertIsNone(queue.claim({'workerId': 'worker', 'nodeId': 'main'}))
                        queue.cancel_node('main', {'expectedRevision': plan['revision'],
                                                  'planId': plan['planId']})
                        # Queue.claim has no host-gate argument: cancel can admit
                        # new work even while that physical gate is reserved.
                        claimed = queue.claim({'workerId': 'worker', 'nodeId': 'main'})
                        self.assertEqual(claimed['nodeId'], 'main')
                        self.assertIsNotNone(gate.snapshot()['reservation'])
            finally:
                queue.close()

    def test_restart_preserves_drain_and_gate_but_cancel_ignores_gate(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            queue = Queue(str(path / 'queue'), node_ids=('cluster', 'main'))
            authority = queue.authority_id
            now = datetime.now(timezone.utc).isoformat()
            plan = queue.schedule_node('main', {'expectedRevision': 0, 'drainAt': now,
                'notBefore': now, 'afterJobId': None, 'targetImage': 'sha256:' + 'a' * 64})
            with patch('processing.node_gate.canonical_gate_path', return_value=path / 'gate'):
                origin = 'http://127.0.0.1:18080'
                with NodeGate('synthetic-engine') as gate:
                    gate.provision('main', origin, authority)
                    gate.reserve('main', origin, authority, 1, 'a' * 32)
                queue.close()
                queue = Queue(str(path / 'queue'), node_ids=('cluster', 'main'))
                try:
                    self.assertEqual(queue.get_node('main')['state'], 'draining')
                    with NodeGate('synthetic-engine') as gate:
                        self.assertIsNotNone(gate.snapshot()['reservation'])
                    queue.submit({'requestId': 'after-restart', 'profile': PROFILE,
                                  'environment': 'production'})
                    self.assertIsNone(queue.claim({'workerId': 'worker', 'nodeId': 'main'}))
                    queue.cancel_node('main', {'expectedRevision': plan['revision'],
                                              'planId': plan['planId']})
                    self.assertEqual(queue.claim({'workerId': 'worker', 'nodeId': 'main'})['nodeId'], 'main')
                finally:
                    queue.close()

    def test_begin_update_does_not_check_physical_gate_reservation(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            queue = Queue(str(path / 'queue'), node_ids=('cluster', 'main'))
            try:
                now = datetime.now(timezone.utc).isoformat()
                plan = queue.schedule_node('main', {'expectedRevision': 0, 'drainAt': now,
                    'notBefore': now, 'afterJobId': None, 'targetImage': 'sha256:' + 'a' * 64})
                with patch('processing.node_gate.canonical_gate_path', return_value=path / 'gate'):
                    with NodeGate('synthetic-engine') as gate:
                        origin = 'http://127.0.0.1:18080'
                        gate.provision('main', origin, queue.authority_id)
                        gate.reserve('main', origin, queue.authority_id, 1, 'a' * 32)
                        result = queue.begin_node_update('main', {'expectedRevision': plan['revision'],
                                                                  'planId': plan['planId']})
                        self.assertEqual(result['node']['state'], 'updating')
                        self.assertIsNotNone(gate.snapshot()['reservation'])
            finally:
                queue.close()


if __name__ == '__main__':
    unittest.main()
