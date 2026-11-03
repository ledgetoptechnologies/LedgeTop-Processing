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
        self._lock = threading.Lock()

    def publish(self, node_id, report):
        if node_id not in self._nodes:
            raise ValueError('unknown node')
        if (type(report) is not dict or set(report) != {'ready', 'taskQueueCount', 'maxImages',
                                                      'availableScratchBytes', 'outputBudgetBytes'}
                or type(report['ready']) is not bool or type(report['taskQueueCount']) is not int
                or report['taskQueueCount'] < 0 or (report['maxImages'] is not None
                    and (type(report['maxImages']) is not int or report['maxImages'] < 1))
                or any(type(report[k]) is not int or report[k] < 0
                       for k in ('availableScratchBytes', 'outputBudgetBytes'))):
            self.invalidate(node_id)
            raise ValueError('invalid readiness observation')
        now = self._clock()
        if type(now) not in (int, float) or not math.isfinite(now):
            self.invalidate(node_id)
            raise ValueError('invalid readiness clock')
        with self._lock:
            self._observations[node_id] = (now, dict(report))

    def invalidate(self, node_id):
        if node_id not in self._nodes:
            raise ValueError('unknown node')
        with self._lock:
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
