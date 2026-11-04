"""Bounded authenticated readiness polling; never run inside a queue transaction."""
import ssl
import threading
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

from . import reference_protocol as protocol
from .node_readiness import NodeReadiness
from .reference_result_proxy import _NoRedirects, _origin


class WorkerReadinessPoller:
    def __init__(self, workers, cache, *, ssl_context, opener=None, timeout=3, interval=5):
        if (type(workers) is not dict or not 1 <= len(workers) <= 8
                or not isinstance(cache, NodeReadiness) or not isinstance(ssl_context, ssl.SSLContext)
                or not ssl_context.check_hostname or ssl_context.verify_mode != ssl.CERT_REQUIRED
                or type(timeout) not in (int, float) or not 0 < timeout <= 5
                or type(interval) not in (int, float) or not 1 <= interval <= 30
                or (opener is not None and not callable(opener))):
            raise ValueError('invalid readiness poller configuration')
        if set(workers) != cache.node_ids:
            raise ValueError('readiness cache must cover exactly the configured workers')
        self._workers = {}
        for node, worker in workers.items():
            protocol._slug(node, 'node')
            if type(worker) is not dict or set(worker) != {'origin', 'bearer', 'workerId'}:
                raise ValueError('invalid readiness worker binding')
            protocol._slug(worker['workerId'], 'worker')
            token = worker['bearer']
            if type(token) is not str or not 16 <= len(token) <= 4096 or not token.isascii() or any(ord(c) < 33 or ord(c) > 126 for c in token):
                raise ValueError('invalid readiness credential')
            self._workers[node] = (_origin(worker['origin']), token, worker['workerId'])
        self._cache, self._timeout, self._interval = cache, timeout, interval
        self._open = opener or build_opener(ProxyHandler({}), _NoRedirects(), HTTPSHandler(context=ssl_context)).open
        self._stop = threading.Event()
        self._thread = None

    def healthy(self):
        return self._thread is not None and self._thread.is_alive() and not self._stop.is_set()

    def poll_once(self):
        for node, (origin, token, worker_id) in self._workers.items():
            if self._stop.is_set():
                return
            ticket = self._cache.begin(node)
            if self._stop.is_set():
                self._cache.invalidate(node)
                return
            try:
                request = Request(origin + '/ltds/worker/v1/readiness',
                    headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/json'})
                with self._open(request, timeout=self._timeout) as response:
                    if response.status != 200 or response.headers.get_content_type() != 'application/json':
                        raise ValueError('unexpected response')
                    raw = response.read(4097)
                    if len(raw) > 4096:
                        raise ValueError('oversize response')
                body = protocol.decode_submission(raw)
                protocol._closed(body, ('nodeId', 'workerId', 'observation'), 'readiness response')
                if body['nodeId'] != node or body['workerId'] != worker_id:
                    raise ValueError('worker identity mismatch')
                self._cache.publish(node, body['observation'], ticket=ticket)
            except Exception:
                # begin() already invalidated the prior observation, so the
                # node is unknown, not busy, until its TTL lapses. A newer poll,
                # if any, owns its own ticket and must remain untouched.
                continue

    def start(self):
        if self._thread is not None:
            raise ValueError('readiness poller already started')
        def run():
            try:
                while not self._stop.is_set():
                    self.poll_once()
                    self._stop.wait(self._interval)
            except Exception:
                # Do not log exception text: a transport may include a token.
                pass
            finally:
                for node in self._workers:
                    self._cache.invalidate(node)
        self._thread = threading.Thread(target=run, name='reference-worker-readiness', daemon=False)
        self._thread.start()

    def close(self):
        self._stop.set()
        for node in self._workers:
            self._cache.invalidate(node)
        if self._thread is not None:
            self._thread.join()
            self._thread = None
        for node in self._workers:
            self._cache.invalidate(node)
