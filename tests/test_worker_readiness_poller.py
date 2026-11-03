import io
import json
import ssl
import unittest
import tempfile
from pathlib import Path
from email.message import Message

from processing.node_readiness import NodeReadiness
from processing.worker_readiness_poller import WorkerReadinessPoller
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.reference_protocol import validate_submission


class Reply(io.BytesIO):
    status = 200
    def __init__(self, value):
        super().__init__(json.dumps(value).encode())
        self.headers = Message()
        self.headers['Content-Type'] = 'application/json'


class WorkerReadinessPollerTests(unittest.TestCase):
    def test_bound_identity_and_failed_poll_invalidates_previous_report(self):
        cache = NodeReadiness(('small',))
        observation = dict(ready=True, taskQueueCount=0, maxImages=1000,
                           availableScratchBytes=4000, outputBudgetBytes=1000)
        answer = [dict(nodeId='small', workerId='worker', observation=observation)]
        calls = []
        def opener(request, timeout):
            calls.append((request.full_url, timeout))
            return Reply(answer[0])
        poller = WorkerReadinessPoller({'small': dict(origin='https://worker.invalid:444',
            bearer='synthetic-read-token-0001', workerId='worker')}, cache,
            ssl_context=ssl.create_default_context(), opener=opener)
        poller.poll_once()
        self.assertEqual(cache.get('small'), observation)
        self.assertEqual(calls, [('https://worker.invalid:444/ltds/worker/v1/readiness', 3)])
        answer[0]['workerId'] = 'different'
        poller.poll_once()
        self.assertIsNone(cache.get('small'))
        poller.close()

    def test_tls_verification_is_mandatory(self):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        with self.assertRaises(ValueError):
            WorkerReadinessPoller({'small': dict(origin='https://worker.invalid',
                bearer='synthetic-read-token-0001', workerId='worker')},
                NodeReadiness(('small',)), ssl_context=context)

    def test_polled_capacity_drives_held_store_and_restart_drops_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            queue = Queue(directory, reference_enabled=True, node_ids=('cluster', 'main'),
                          treedar_node_ids=('main',), per_node_slots=True)
            try:
                inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                             dict(id='main', maxPhotos=None, capabilities=['odm'])]
                cache = NodeReadiness(('cluster', 'main'))
                store = ReferenceStore(queue, node_inventory=inventory, node_readiness=cache.get)
                raw = json.loads((Path(__file__).parents[1] / 'contracts/reference-v1.contract.json').read_text())['submission']
                job = store.submit(validate_submission(raw, environment='staging',
                    capability_fingerprint=raw['capabilityFingerprint']))[0]
                workers = {n: dict(origin='https://' + n + '.invalid', bearer='synthetic-token-00001',
                                   workerId=n + '-worker') for n in ('cluster', 'main')}
                def opener(request, timeout):
                    node = 'cluster' if 'cluster.invalid' in request.full_url else 'main'
                    return Reply(dict(nodeId=node, workerId=node + '-worker', observation=dict(
                        ready=True, taskQueueCount=0, maxImages=None,
                        availableScratchBytes=2 if node == 'cluster' else 100,
                        outputBudgetBytes=0)))
                poller = WorkerReadinessPoller(workers, cache, ssl_context=ssl.create_default_context(), opener=opener)
                self.assertIsNone(store.next_for_node('main'))
                poller.poll_once()
                self.assertEqual(store.next_for_node('main')['jobId'], job['jobId'])
                self.assertIsNone(store.next_for_node('cluster'))
                poller.close()
                fresh = NodeReadiness(('cluster', 'main'))
                restarted = ReferenceStore(queue, node_inventory=inventory, node_readiness=fresh.get)
                self.assertIsNone(restarted.next_for_node('main'))
            finally:
                queue.close()


if __name__ == '__main__':
    unittest.main()
