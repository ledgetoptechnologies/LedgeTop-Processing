"""Unwired coordinator reservation-to-worker-grant bridge.

Only a trusted local coordinator may construct this adapter with protected
worker pins, authenticated source principal and its scoped handoff credential.
The worker must already have issued the exact readiness proof. This does not
launch a listener, clear a worker hold, or authorize source custody.
"""
from . import reference_protocol as p
from .direct_v2_handoff_journal import DirectV2Handoff
from .direct_v2_reservation import DirectV2Reservations
from .direct_worker_sender_v2 import DirectWorkerSenderV2
from cryptography.hazmat.primitives import serialization


class WorkerHoldUncertain(RuntimeError):
    """Worker may have a durable hold; exact reconciliation is required."""


class DirectSchedulerBridge:
    def __init__(self, reservations, sender, *, principal, bearer):
        # There is no enabled, protected endpoint/CA/credential factory or
        # authenticated caller context yet. An arbitrary externally supplied
        # sender could transmit its bearer to an unrelated endpoint. Do not
        # make this bridge constructible by a runtime caller until those
        # dependencies are implemented and independently qualified.
        raise RuntimeError('direct scheduler bridge is not available')

    def _validate_sender(self):
        pin=self.reservations._workers.get(self.sender._item['nodeId'])
        if (pin is None or not self.sender._client_identity_configured
                or any(self.sender._item[key]!=pin[0][key] for key in ('authorityId','nodeId','workerId','workerInstanceId'))
                or (self.sender._issuer,self.sender._key)!=(pin[1],pin[2])
                or self.sender._public.public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)
                !=pin[3].public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)):
            raise ValueError('sender trust differs from enrolled worker')

    def reserve_and_grant(self, token, *, protected_transport, request):
        """Commit the scheduler-selected node, then grant that exact worker.

        A failure after authenticated invocation is conservatively uncertain:
        even scheduler rejection can leave the already-prepared worker held.
        The caller must never clear or reassign it on that basis.
        """
        self.handoff._auth(token,protected_transport)
        if type(request) is not dict:raise ValueError('exact reservation request required')
        try:
            if (request.get('authenticated_principal')!=self.principal
                    or p.canonical_json(request['item'])!=p.canonical_json(self.sender._item)):
                raise ValueError('reservation identity changed')
            self._validate_sender()
            reserved=self.reservations.reserve(**request)
            granted=self.handoff.authorize_worker(token,protected_transport=True)
            return {'reservationId':reserved['reservationId'],'nodeId':reserved['nodeId'],
                    'operationId':granted['operationId'],'state':'worker_granted_not_custody',
                    'sourceCustodyAccepted':False,'processingAuthorized':False}
        except Exception:
            raise WorkerHoldUncertain('worker hold requires exact reconciliation') from None

    def resume_committed_grant(self,token,*,protected_transport):
        """Revalidate durable reservation and replay exact worker grant.

        This does not infer new readiness from an expired proof and never
        retries unknown input bytes or releases a worker hold.
        """
        self.handoff._auth(token,protected_transport)
        try:
            self._validate_sender()
            granted=self.handoff.authorize_worker(token,protected_transport=True)
            return {'reservationId':self.sender._item['reservationId'],'nodeId':self.sender._item['nodeId'],
                    'operationId':granted['operationId'],'state':'worker_granted_not_custody',
                    'sourceCustodyAccepted':False,'processingAuthorized':False}
        except Exception:
            raise WorkerHoldUncertain('worker hold requires exact reconciliation') from None
