"""Protected mTLS preparation candidate; no admission or custody authority.

The caller must derive config digest and worker registry from independently
protected enrollment. This module is not wired to runtime startup.
"""
import ipaddress
import time
from urllib.parse import urlsplit
from cryptography.hazmat.primitives import serialization

from . import reference_protocol as p
from .direct_protected_transport import load_enrolled_transport
from .direct_v2_reservation import DirectV2Reservations
from .direct_worker_fence_proof_v2 import verify
from .direct_worker_sender import DirectWorkerSender, DirectDeliveryUncertain


class DirectPreparationClient(DirectWorkerSender):
    def __init__(self, *, config_path, config_sha256, credential_sha256, node, reservations):
        if not isinstance(reservations,DirectV2Reservations):
            raise ValueError('protected worker registry required')
        enrolled=load_enrolled_transport(config_path,node,expected_config_sha256=config_sha256,
                                          expected_credential_sha256=credential_sha256)
        pin=reservations._workers.get(node)
        if (pin is None or (enrolled.issuer_id,enrolled.key_id)!=(pin[1],pin[2])
                or enrolled.worker_public_key.public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)
                !=pin[3].public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)):
            raise ValueError('protected worker registry changed')
        parsed=urlsplit(enrolled.origin)
        self._host,self._port=parsed.hostname,parsed.port or 443
        self._address=ipaddress.ip_address(enrolled.connect_ip)
        self._context=enrolled._context
        self._worker,self._issuer,self._key,self._public=dict(pin[0]),pin[1],pin[2],pin[3]
        self._idle,self._total,self._response_limit=10,120,65536

    def prepare(self, *, source, item, challenge_id, gate_generation):
        """Request exact signed readiness. Any transport failure is uncertain."""
        p._uuid(challenge_id,'challenge')
        raw=p.canonical_json(dict(source=source,item=item,challengeId=challenge_id))
        if len(raw)>16*1024*1024:raise ValueError('preparation request too large')
        deadline=time.monotonic()+self._total
        connection=guard=None
        try:
            connection,guard=self._connect(deadline)
            connection.putrequest('POST','/internal/v2/worker/prepare')
            connection.putheader('Content-Length',str(len(raw)))
            connection.putheader('Content-Type','application/json')
            connection.endheaders()
            for offset in range(0,len(raw),65536):
                self._timeout(connection,deadline);connection.send(raw[offset:offset+65536])
            proof=self._response(connection,deadline)
            verify(proof,expected_item=item,expected_worker=self._worker,challenge_id=challenge_id,
                   gate_generation=gate_generation,issuer_id=self._issuer,key_id=self._key,
                   public_key=self._public,now=int(time.time()))
            if not guard.finish():raise DirectDeliveryUncertain('worker preparation outcome uncertain')
            return proof
        except Exception:
            raise DirectDeliveryUncertain('worker preparation outcome uncertain') from None
        finally:
            if guard is not None:guard.finish()
            if connection is not None:connection.close()
