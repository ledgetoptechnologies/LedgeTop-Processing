"""Short-lived, process-local observations for held inventory scheduling.

Only a trusted caller that has already authenticated and checked a node may
publish an observation. This cache is not a node attestation or reservation.
"""
import math
import threading
import time


class NodeReadiness:
    def __init__(self, node_ids, *, ttl_seconds=10, clock=time.monotonic):
        ids = tuple(node_ids)
        if (not ids or len(ids) > 8 or len(set(ids)) != len(ids)
                or any(type(n) is not str or not n for n in ids)
                or type(ttl_seconds) not in (int, float) or isinstance(ttl_seconds, bool)
                or not math.isfinite(ttl_seconds) or not 0 < ttl_seconds <= 60
                or not callable(clock)):
            raise ValueError('invalid readiness cache configuration')
        self._nodes, self._ttl, self._clock = frozenset(ids), ttl_seconds, clock
        self._observations = {}
        self._epochs = {node: 0 for node in ids}
        self._pending = {}
        self._lock = threading.Lock()

    @property
    def node_ids(self):
        return self._nodes

    def begin(self, node_id):
        """Invalidate prior readiness and return a ticket for one local probe."""
        if node_id not in self._nodes:
            raise ValueError('unknown node')
        now = self._clock()
        if type(now) not in (int, float) or not math.isfinite(now):
            self.invalidate(node_id)
            raise ValueError('invalid readiness clock')
        with self._lock:
            self._epochs[node_id] += 1
            self._pending[node_id] = (self._epochs[node_id], now)
            self._observations.pop(node_id, None)
            return self._epochs[node_id]

    def publish(self, node_id, report, *, ticket):
        if node_id not in self._nodes:
            raise ValueError('unknown node')
        with self._lock:
            pending = self._pending.get(node_id)
            if type(ticket) is not int or pending is None or pending[0] != ticket or ticket < 1:
                raise ValueError('stale readiness probe')
        if (type(report) is not dict or set(report) != {'ready', 'taskQueueCount', 'maxImages',
                                                      'availableScratchBytes', 'outputBudgetBytes'}
                or type(report['ready']) is not bool or type(report['taskQueueCount']) is not int
                or report['taskQueueCount'] < 0 or (report['maxImages'] is not None
                    and (type(report['maxImages']) is not int or report['maxImages'] < 1))
                or any(type(report[k]) is not int or report[k] < 0
                       for k in ('availableScratchBytes', 'outputBudgetBytes'))):
            self._invalidate_ticket(node_id, ticket)
            raise ValueError('invalid readiness observation')
        now = self._clock()
        if type(now) not in (int, float) or not math.isfinite(now):
            self._invalidate_ticket(node_id, ticket)
            raise ValueError('invalid readiness clock')
        with self._lock:
            pending = self._pending.get(node_id)
            if pending is None or pending[0] != ticket:
                raise ValueError('stale readiness probe')
            self._pending.pop(node_id)
            if now < pending[1] or now - pending[1] >= self._ttl:
                raise ValueError('expired readiness probe')
            self._observations[node_id] = (pending[1], dict(report))

    def _invalidate_ticket(self, node_id, ticket):
        with self._lock:
            if self._epochs[node_id] == ticket:
                self._observations.pop(node_id, None)
                self._pending.pop(node_id, None)

    def invalidate(self, node_id):
        if node_id not in self._nodes:
            raise ValueError('unknown node')
        with self._lock:
            self._epochs[node_id] += 1
            self._pending.pop(node_id, None)
            self._observations.pop(node_id, None)

    def get(self, node_id):
        if node_id not in self._nodes:
            raise ValueError('unknown node')
        now = self._clock()
        with self._lock:
            record = self._observations.get(node_id)
            if (record is None or type(now) not in (int, float) or not math.isfinite(now)
                    or now < record[0] or now - record[0] >= self._ttl):
                return None
            return dict(record[1])
