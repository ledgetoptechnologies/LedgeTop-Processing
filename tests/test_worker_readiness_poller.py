import io
import json
import ssl
import subprocess
import threading
import unittest
import tempfile
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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
        poller.close()
        self.assertIsNone(cache.get('small'))
        poller = WorkerReadinessPoller({'small': dict(origin='https://worker.invalid:444',
            bearer='synthetic-read-token-0001', workerId='worker')}, cache,
            ssl_context=ssl.create_default_context(), opener=opener)
        poller.poll_once()
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

    def test_worker_and_cache_node_sets_must_match(self):
        with self.assertRaises(ValueError):
            WorkerReadinessPoller({'small': dict(origin='https://worker.invalid',
                bearer='synthetic-read-token-0001', workerId='worker')},
                NodeReadiness(('different',)), ssl_context=ssl.create_default_context())

    def test_unexpected_poller_failure_invalidates_observations(self):
        cache = NodeReadiness(('small',))
        poller = WorkerReadinessPoller({'small': dict(origin='https://worker.invalid',
            bearer='synthetic-read-token-0001', workerId='worker')}, cache,
            ssl_context=ssl.create_default_context())
        ticket = cache.begin('small')
        cache.publish('small', dict(ready=True, taskQueueCount=0, maxImages=None,
            availableScratchBytes=1000, outputBudgetBytes=0), ticket=ticket)
        poller.poll_once = lambda: (_ for _ in ()).throw(RuntimeError('synthetic failure'))
        poller.start(); poller._thread.join(timeout=5)
        self.assertFalse(poller.healthy())
        self.assertIsNone(cache.get('small'))
        poller.close()

    def test_real_tls_listener_requires_trusted_certificate_and_bearer(self):
        with tempfile.TemporaryDirectory() as directory:
            cert, key = Path(directory) / 'cert.pem', Path(directory) / 'key.pem'
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=IP:127.0.0.1',
                '-keyout', str(key), '-out', str(cert)], check=True, capture_output=True, timeout=15)
            observation = dict(ready=True, taskQueueCount=0, maxImages=1000,
                               availableScratchBytes=4000, outputBudgetBytes=1000)
            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *_):
                    pass
                def do_GET(self):
                    if self.headers.get('Authorization') != 'Bearer synthetic-read-token-0001':
                        self.send_response(401); self.end_headers(); return
                    payload = json.dumps(dict(nodeId='small', workerId='worker', observation=observation)).encode()
                    self.send_response(200); self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(payload))); self.end_headers(); self.wfile.write(payload)
            server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.load_cert_chain(cert, key)
            server.socket = server_context.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05})
            thread.start()
            try:
                cache = NodeReadiness(('small',))
                origin = 'https://127.0.0.1:' + str(server.server_port)
                workers = {'small': dict(origin=origin, bearer='synthetic-read-token-0001', workerId='worker')}
                poller = WorkerReadinessPoller(workers, cache, ssl_context=ssl.create_default_context(cafile=str(cert)))
                poller.poll_once(); self.assertEqual(cache.get('small'), observation)
                wrong = WorkerReadinessPoller({'small': {**workers['small'], 'bearer': 'synthetic-wrong-token-001'}},
                    cache, ssl_context=ssl.create_default_context(cafile=str(cert)))
                wrong.poll_once(); self.assertIsNone(cache.get('small'))
                untrusted = WorkerReadinessPoller(workers, cache, ssl_context=ssl.create_default_context())
                untrusted.poll_once(); self.assertIsNone(cache.get('small'))
                for item in (poller, wrong, untrusted): item.close()
            finally:
                server.shutdown(); server.server_close(); thread.join(timeout=5)
                self.assertFalse(thread.is_alive())

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

    def test_failed_probe_leaves_node_unknown_until_its_ttl(self):
        now = [100.0]
        cache = NodeReadiness(('small',), ttl_seconds=30, clock=lambda: now[0])
        workers = {'small': dict(origin='https://small.invalid', bearer='synthetic-token-00001', workerId='small-worker')}
        observation = dict(ready=True, taskQueueCount=0, maxImages=None, availableScratchBytes=100, outputBudgetBytes=0)
        answers = [Reply(dict(nodeId='small', workerId='small-worker', observation=observation))]
        def opener(request, timeout):
            if answers:
                return answers.pop()
            raise OSError('synthetic failure')
        poller = WorkerReadinessPoller(workers, cache, ssl_context=ssl.create_default_context(), opener=opener)
        poller.poll_once()
        self.assertTrue(cache.status('small')['ready'])
        now[0] = 129
        poller.poll_once()
        self.assertIsNone(cache.status('small'))
        now[0] = 130
        poller.poll_once()
        self.assertEqual(cache.status('small'), {'ready': False, 'reason': 'readiness_expired'})


if __name__ == '__main__':
    unittest.main()
