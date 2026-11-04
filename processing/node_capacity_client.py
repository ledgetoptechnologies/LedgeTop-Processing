"""Inactive coordinator client for a node-local scratch reservation API.

The caller must persist the exact request (especially reservationId) before
calling reserve(). Any transport or response uncertainty raises
CapacityReservationHeld; retry the same request unchanged or keep dispatch held.
This client does not attest physicalHostId: deployment must bind that identity
to the actual node and protect the shared receipt key.
"""
import hashlib
import hmac
import json
import re
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler, ProxyHandler

from .node_capacity_api import MAX_BODY, canonical


class CapacityReservationHeld(RuntimeError):
    """Reservation outcome cannot be safely confirmed; do not dispatch."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


class NodeCapacityClient:
    def __init__(self, *, origin, bearer_token, receipt_key, timeout_seconds=3,
                 transport=None):
        parsed=urlsplit(origin) if isinstance(origin,str) else None
        if (parsed is None or parsed.scheme!='https' or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('','/')):
            raise ValueError('fixed HTTPS node origin required')
        if not isinstance(bearer_token,str) or not 32<=len(bearer_token)<=4096:
            raise ValueError('strong bearer token required')
        if not isinstance(receipt_key,bytes) or len(receipt_key)<32:
            raise ValueError('strong receipt key required')
        if type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=15:
            raise ValueError('bounded timeout required')
        self.origin=f'https://{parsed.netloc}'
        self.token=bearer_token
        self.key=receipt_key
        self.timeout=timeout_seconds
        self.transport=transport

    def _post(self, request):
        body=canonical(request).encode('utf-8')
        if len(body)>MAX_BODY:raise CapacityReservationHeld('reservation request exceeds protocol bound')
        headers={'Authorization':'Bearer '+self.token,'Content-Type':'application/json','Accept':'application/json'}
        try:
            if self.transport is not None:
                status,raw=self.transport(self.origin+'/capacity/v1/reserve',headers,body,self.timeout)
            else:
                req=Request(self.origin+'/capacity/v1/reserve',data=body,headers=headers,method='POST')
                with build_opener(ProxyHandler({}),_NoRedirect).open(req,timeout=self.timeout) as response:
                    status=response.status
                    raw=response.read(MAX_BODY+1)
            if len(raw)>MAX_BODY:raise CapacityReservationHeld('reservation response exceeds protocol bound')
            if status!=200:raise CapacityReservationHeld('node did not acknowledge reservation')
            value=json.loads(raw.decode('utf-8'),object_pairs_hook=self._unique)
            if type(value) is not dict or not self._verify(value,request):
                raise CapacityReservationHeld('reservation receipt identity or signature mismatch')
            return value
        except CapacityReservationHeld:raise
        except (HTTPError,URLError,OSError,TimeoutError,ValueError,UnicodeError,TypeError):
            raise CapacityReservationHeld('reservation outcome uncertain; retry exact request id') from None

    @staticmethod
    def _unique(pairs):
        result={}
        for key,value in pairs:
            if key in result:raise ValueError('duplicate JSON key')
            result[key]=value
        return result

    def _verify(self, receipt, request):
        expected={'schemaVersion','protocol','nodeId','physicalHostId','reservationId','jobId',
                  'generation','environment','manifestSha256','inputBytes','outputBudgetBytes',
                  'state','receiptSha256','signature'}
        if set(receipt)!=expected:return False
        if receipt['schemaVersion']!=1 or receipt['protocol']!='ltds-node-scratch-reservation-v1' or receipt['state']!='reserved':return False
        fields=('nodeId','physicalHostId','reservationId','jobId','generation','environment',
                'manifestSha256','inputBytes','outputBudgetBytes')
        if any(receipt.get(k)!=request.get(k) for k in fields):return False
        unsigned={k:v for k,v in receipt.items() if k not in ('signature','receiptSha256')}
        digest=hashlib.sha256(canonical(unsigned).encode()).hexdigest()
        if not isinstance(receipt['receiptSha256'],str) or not hmac.compare_digest(digest,receipt['receiptSha256']):return False
        signed=dict(unsigned,receiptSha256=receipt['receiptSha256'])
        signature=hmac.new(self.key,canonical(signed).encode(),hashlib.sha256).hexdigest()
        return isinstance(receipt['signature'],str) and hmac.compare_digest(signature,receipt['signature'])

    def reserve(self, *, node_id, physical_host_id, job_id, generation, reservation_id,
                environment, manifest_sha256, input_bytes, output_budget_bytes, ttl_seconds):
        """Request a reservation. Persist and reuse these exact fields on retry."""
        request={'nodeId':node_id,'physicalHostId':physical_host_id,'jobId':job_id,
                 'generation':generation,'reservationId':reservation_id,'environment':environment,
                 'manifestSha256':manifest_sha256,'inputBytes':input_bytes,
                 'outputBudgetBytes':output_budget_bytes,'ttlSeconds':ttl_seconds}
        return self._post(request)
