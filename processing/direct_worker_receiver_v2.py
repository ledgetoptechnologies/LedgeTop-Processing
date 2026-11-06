"""Unwired V2 worker receive boundary. No Viewer custody acknowledgement."""
import hmac
import copy
import hashlib
import json
import threading
import time

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
                 expected_worker, engine_id, coordinator_origin, gate_generation, bearer,
                 readiness_proof=None, challenge_id=None, issuer_id=None, key_id=None,
                 public_key=None, closure_expected_binding=None, closure_verification=None, worker_signer=None,
                 resume_observation_only=False):
        if (not isinstance(inventory, VerifiedWorkerInventory) or type(bearer) is not str
                or not 32 <= len(bearer) <= 4096 or not bearer.isascii()
                or not all(33 <= ord(c) <= 126 for c in bearer)):
            raise DirectBoundaryDenied('invalid V2 worker configuration')
        # Construction is internal and remains unwired. Every fresh boundary
        # requires the exact durable readiness pin; restart without a qualified
        # handover record fails closed rather than inferring continuation.
        if type(resume_observation_only) is not bool:raise DirectBoundaryDenied('explicit V2 resume mode required')
        item, scratch_sha, scratch_id = identity(item, expected_worker=expected_worker)
        from .direct_worker_fence_proof_v2 import verify as verify_readiness, FenceProofRejected
        try:
            checked_mono = time.monotonic()
            checked_at = time.time()
            # Historical proof is evidence of the already-consumed handover,
            # never fresh write authorization for a resumed worker.
            proof_now = readiness_proof['body']['issuedAt'] if resume_observation_only else int(checked_at)
            verified = verify_readiness(readiness_proof, expected_item=item, expected_worker=expected_worker,
                challenge_id=challenge_id, gate_generation=gate_generation, issuer_id=issuer_id,
                key_id=key_id, public_key=public_key, now=proof_now)
        except (ValueError, TypeError, KeyError, FenceProofRejected):
            raise DirectBoundaryDenied('verified V2 readiness proof required') from None
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
        if resume_observation_only:
            require_fenced(ledger,item,expected_worker=expected_worker)
            with NodeGate(engine_id) as gate:
                check_scope(ledger,spool,item,scratch_sha,scratch_id,gate,
                            coordinator_origin=coordinator_origin,gate_generation=gate_generation)
                state=gate.snapshot()
                expected_hold=dict(receiptId=scratch_sha[:32],generation=gate_generation,
                                   scratchBindingSha256=scratch_sha)
                if state.get('schema')!=2 or state.get('directHold')!=expected_hold:
                    raise DirectBoundaryDenied('V2 retained node hold required')
                with ledger._transaction() as db:
                    used=db.execute('SELECT reservation,proof_sha256 FROM direct_v2_readiness_uses WHERE challenge=?',
                                    (challenge_id,)).fetchone()
                    expected_sha=hashlib.sha256(p.canonical_json(readiness_proof)).hexdigest()
                    if (used is None or used['reservation']!=scratch_id or used['proof_sha256']!=expected_sha
                            or db.execute('SELECT 1 FROM direct_writer_pins WHERE reservation=?',(scratch_id,)).fetchone()):
                        raise DirectBoundaryDenied('V2 retained handover requires reconciliation')
        self.ledger, self.spool, self.inventory = ledger, spool, inventory
        self.item, self.reservation, self.worker = item, dict(reservation_binding), dict(expected_worker)
        self.engine_id, self.origin, self.generation, self._bearer = engine_id, coordinator_origin, gate_generation, bearer
        self._readiness_pin = None if resume_observation_only else verified['pinId']
        self._observation_only=resume_observation_only
        self._readiness_proof = copy.deepcopy(readiness_proof)
        self._proof_deadline = checked_mono + max(0, verified['expiresAt'] - checked_at)
        self._proof_binding = dict(challenge_id=challenge_id, issuer_id=issuer_id,
                                   key_id=key_id, public_key=public_key)
        self._writer_lock = threading.Lock()
        self._closure_binding=copy.deepcopy(closure_expected_binding)
        self._closure_verification=dict(closure_verification) if type(closure_verification) is dict else None
        self._closure_signer=worker_signer

    def put(self, headers, *, protected_transport, object_id, content_length, content_sha256, reader):
        from .direct_worker_boundary import DirectWorkerBoundary
        supplied = DirectWorkerBoundary._header(headers, 'Authorization')
        authority = DirectWorkerBoundary._header(headers, 'X-Coordinator-Authority')
        if (protected_transport is not True or type(supplied) is not str or not supplied.startswith('Bearer ')
                or not supplied[7:].isascii() or not hmac.compare_digest(supplied[7:], self._bearer)
                or authority != self.item['authorityId']):
            raise DirectBoundaryDenied('private coordinator authorization required')
        if self._observation_only:raise DirectBoundaryDenied('V2 resumed boundary does not authorize writes')
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
                pin = self.ledger.begin_direct_writer(scratch_id, settings(item, scratch_id), require_unclosed=True)
            else:
                from .direct_worker_fence_proof_v2 import verify as verify_readiness
                def check_readiness():
                    if time.monotonic() >= self._proof_deadline:
                        raise DirectBoundaryDenied('V2 readiness proof expired')
                    observed = verify_readiness(self._readiness_proof, expected_item=item,
                        expected_worker=self.worker, gate_generation=self.generation,
                        now=int(time.time()), **self._proof_binding)
                    if observed['pinId'] != self._readiness_pin:
                        raise DirectBoundaryDenied('V2 readiness pin changed')
                    return True
                check_readiness()
                pin = self.ledger._handover_v2_writer(scratch_id, settings(item, scratch_id), self._readiness_pin,
                    challenge_id=self._proof_binding['challenge_id'],
                    proof_sha256=hashlib.sha256(p.canonical_json(self._readiness_proof)).hexdigest(),
                    readiness_guard=check_readiness)
                # A blocked transaction can finish too late. Never read or
                # release its replacement pin when outcome freshness is lost.
                try:
                    check_readiness()
                except Exception as error:
                    raise PolicyConflict('V2 readiness outcome after handover uncertain') from error
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

    def complete(self, headers, *, protected_transport):
        from .direct_worker_boundary import DirectWorkerBoundary
        supplied=DirectWorkerBoundary._header(headers,'Authorization')
        if (protected_transport is not True or type(supplied) is not str or not supplied.startswith('Bearer ')
                or not supplied[7:].isascii() or not hmac.compare_digest(supplied[7:],self._bearer)
                or DirectWorkerBoundary._header(headers,'X-Coordinator-Authority')!=self.item['authorityId']):
            raise DirectBoundaryDenied('private coordinator authorization required')
        if (self._closure_binding is None or self._closure_verification is None or self._closure_signer is None):
            raise DirectBoundaryDenied('V2 closure signing not configured')
        from .direct_worker_closure_v2 import verify_closure
        with self._writer_lock:
            if self._readiness_pin is not None:
                raise DirectBoundaryDenied('V2 initial handover required')
            return verify_closure(self.ledger,self.spool,self.inventory,self.item,
                expected_binding=self._closure_binding,reservation_binding=self.reservation,
                verification=self._closure_verification,expected_worker=self.worker,engine_id=self.engine_id,
                coordinator_origin=self.origin,gate_generation=self.generation,worker_signer=self._closure_signer)

    def retention(self, headers, *, protected_transport):
        """Observe a persisted local fence, never acknowledge Viewer custody."""
        signed=self.complete(headers,protected_transport=protected_transport)
        _,digest,scratch_id=require_fenced(self.ledger,self.item,expected_worker=self.worker)
        with self.ledger._transaction() as db:
            row=db.execute('SELECT binding_sha,closure_sha FROM direct_custody_fences WHERE reservation=?',(scratch_id,)).fetchone()
            if row is None or row['binding_sha']!=digest:
                raise PolicyConflict('V2 retention fence unavailable')
            closure_sha=row['closure_sha']
        return self._closure_signer.retention_observation(signed,binding_sha=digest,closure_sha=closure_sha)

    def retention_challenge(self, headers, *, protected_transport, challenge):
        """Challenge binds a newly observed local fence; never grants custody."""
        observation=self.retention(headers,protected_transport=protected_transport)
        return self._closure_signer.challenged_retention(observation,challenge=challenge)

    def observe(self, headers, *, protected_transport, object_id):
        """Observe an exact durable object; absence or active writers stays uncertain."""
        from .direct_worker_boundary import DirectWorkerBoundary
        supplied=DirectWorkerBoundary._header(headers,'Authorization')
        if (protected_transport is not True or type(supplied) is not str or not supplied.startswith('Bearer ')
                or not supplied[7:].isascii() or not hmac.compare_digest(supplied[7:],self._bearer)
                or DirectWorkerBoundary._header(headers,'X-Coordinator-Authority')!=self.item['authorityId']):
            raise DirectBoundaryDenied('private coordinator authorization required')
        obj=self.inventory.object(object_id,reservation_binding=self.reservation)
        with self._writer_lock, NodeGate(self.engine_id) as gate:
            if self._readiness_pin is not None:raise PolicyConflict('V2 handover not established')
            item,digest,scratch_id=require_fenced(self.ledger,self.item,expected_worker=self.worker)
            check_scope(self.ledger,self.spool,item,digest,scratch_id,gate,
                        coordinator_origin=self.origin,gate_generation=self.generation)
            gate._arm_direct_hold(digest[:32],digest)
            path=self.spool.root/self.spool.storage_key(item['jobId'],item['generation'],object_id)
            if not path.exists():raise PolicyConflict('V2 object outcome uncertain')
            pin=self.ledger.begin_direct_writer(scratch_id,settings(item,scratch_id))
            self.spool.verify_object(job_id=item['jobId'],generation=item['generation'],object_id=object_id,
                                     byte_size=obj['byteSize'],sha256=obj['sha256'])
            # A content hash alone does not establish durable namespace
            # publication. Re-sync the exact parent and then recheck both
            # mount identity and the final file before reporting durability.
            self.spool._fsync_directory(path.parent)
            self.spool._assert_mount()
            self.spool.verify_object(job_id=item['jobId'],generation=item['generation'],object_id=object_id,
                                     byte_size=obj['byteSize'],sha256=obj['sha256'])
            require_fenced(self.ledger,item,expected_worker=self.worker)
            self.ledger._retire_direct_writer(scratch_id,pin)
        return dict(protocol='ltds-direct-local-object-v2',reservationId=item['reservationId'],
            scratchBindingSha256=digest,objectId=object_id,byteSize=obj['byteSize'],sha256=obj['sha256'],
            localObjectDurable=True,sourceCustodyAccepted=False,transferComplete=False,replayed=True)
