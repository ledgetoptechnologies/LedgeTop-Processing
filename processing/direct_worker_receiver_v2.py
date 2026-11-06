"""Unwired V2 worker receive boundary. No Viewer custody acknowledgement."""
import hmac
import json
import re
import threading

from . import reference_protocol as p
from .direct_v2_inventory import VerifiedWorkerInventory
from .direct_worker_boundary import DirectBoundaryDenied
from .direct_worker_receiver import check_scope
from .direct_worker_scratch_v2 import identity, require_fenced, settings
from .node_gate import NodeGate
from .scratch_policy import PolicyConflict


class DirectWorkerBoundaryV2:
    """The future TLS listener must supply actual transport and reserved state."""
    def __init__(self, *, ledger, spool, inventory, item, reservation_binding,
                 expected_worker, engine_id, coordinator_origin, gate_generation, bearer, readiness_pin=None):
        if (not isinstance(inventory, VerifiedWorkerInventory) or type(bearer) is not str
                or not 32 <= len(bearer) <= 4096 or not bearer.isascii()
                or not all(33 <= ord(c) <= 126 for c in bearer)):
            raise DirectBoundaryDenied('invalid V2 worker configuration')
        if readiness_pin is not None and (type(readiness_pin) is not str or
                                          not re.fullmatch('[a-f0-9]{32}', readiness_pin)):
            raise DirectBoundaryDenied('invalid V2 readiness pin')
        item, _, _ = identity(item, expected_worker=expected_worker)
        saved = json.loads(inventory._wire)
        if (p.canonical_json(reservation_binding) != inventory._reservation_wire
                or any(item[key] != reservation_binding[key] for key in reservation_binding)
                or any(item[key] != saved[key] for key in ('sourceManifestSha256',
                    'derivedManifestSha256', 'pipelineManifestSha256', 'inventorySha256'))
                or item['inputBytes'] != sum(obj['byteSize'] for key in ('sourceManifest', 'derivedManifest')
                    for obj in saved[key]['objects'])
                or item['objectCount'] != sum(len(saved[key]['objects'])
                    for key in ('sourceManifest', 'derivedManifest'))):
            raise DirectBoundaryDenied('V2 inventory and scratch binding differ')
        self.ledger, self.spool, self.inventory = ledger, spool, inventory
        self.item, self.reservation, self.worker = item, dict(reservation_binding), dict(expected_worker)
        self.engine_id, self.origin, self.generation, self._bearer = engine_id, coordinator_origin, gate_generation, bearer
        self._readiness_pin = readiness_pin
        self._writer_lock = threading.Lock()

    def put(self, headers, *, protected_transport, object_id, content_length, content_sha256, reader):
        from .direct_worker_boundary import DirectWorkerBoundary
        supplied = DirectWorkerBoundary._header(headers, 'Authorization')
        authority = DirectWorkerBoundary._header(headers, 'X-Coordinator-Authority')
        if (protected_transport is not True or type(supplied) is not str or not supplied.startswith('Bearer ')
                or not supplied[7:].isascii() or not hmac.compare_digest(supplied[7:], self._bearer)
                or authority != self.item['authorityId']):
            raise DirectBoundaryDenied('private coordinator authorization required')
        obj = self.inventory.object(object_id, reservation_binding=self.reservation)
        if (type(content_length) is not int or content_length != obj['byteSize']
                or content_sha256 != obj['sha256']):
            raise DirectBoundaryDenied('V2 object metadata mismatch')
        if not callable(getattr(reader, 'close', None)) or getattr(reader, 'closed', None) is not False:
            raise ValueError('owned open source reader required')
        item, digest, scratch_id = require_fenced(self.ledger, self.item, expected_worker=self.worker)
        with self._writer_lock, NodeGate(self.engine_id) as gate:
            check_scope(self.ledger, self.spool, item, digest, scratch_id, gate,
                        coordinator_origin=self.origin, gate_generation=self.generation)
            # Persist a schema-2 node hold before opening a writer pin. A
            # completed object must not free this node between source objects.
            gate._arm_direct_hold(digest[:32], digest)
            if self._readiness_pin is None:
                pin = self.ledger.begin_direct_writer(scratch_id, settings(item, scratch_id))
            else:
                pin = self.ledger._handover_direct_writer(scratch_id, settings(item, scratch_id), self._readiness_pin)
                # If the subsequent operation fails, the new pin remains held.
                self._readiness_pin = None
            receipt, replay = self.spool.receive_object(job_id=item['jobId'], generation=item['generation'],
                object_id=object_id, byte_size=obj['byteSize'], sha256=obj['sha256'], reader=reader)
            reader.close()
            if getattr(reader, 'closed', None) is not True:
                raise PolicyConflict('V2 source reader retirement uncertain')
            require_fenced(self.ledger, item, expected_worker=self.worker)
            self.ledger._retire_direct_writer(scratch_id, pin)
        return dict(protocol='ltds-direct-local-object-v2', reservationId=item['reservationId'],
            scratchBindingSha256=digest, objectId=object_id, byteSize=obj['byteSize'], sha256=obj['sha256'],
            localObjectDurable=True, sourceCustodyAccepted=False, transferComplete=False, replayed=replay)
