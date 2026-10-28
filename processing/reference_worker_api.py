"""Private worker metadata boundary; never mount under Viewer credentials.

The listener establishes protected_transport from its actual socket. Bearer
configuration fixes worker identity; requests never choose a node/environment.
No filesystem paths are part of the worker wire protocol.
"""
import hmac
import re
from types import MappingProxyType

from . import reference_protocol as protocol
from .maintenance import Conflict
from .reference_api import Response
from .reference_transfer import validate_grant


PREFIX = "/ltds/worker/v1"


class ReferenceWorkerAPI:
    def __init__(self, store, receiver, *, bearer_workers):
        if type(bearer_workers) is not dict or not bearer_workers:
            raise ValueError("explicit private worker bearer configuration required")
        workers = {}
        for token, identity in bearer_workers.items():
            if (type(token) is not str or not 16 <= len(token) <= 4096
                    or not all(0x21 <= ord(char) <= 0x7e for char in token)):
                raise ValueError("invalid worker bearer configuration")
            protocol._closed(identity, ("node_id", "worker_id", "worker_instance_id"), "worker identity")
            store.queue._known_node(identity["node_id"])
            store.queue.slug(identity["worker_id"])
            protocol._slug(identity["worker_instance_id"], "worker instance ID")
            workers[token] = MappingProxyType(dict(identity))
        self.store, self.receiver = store, receiver
        self._workers = MappingProxyType(workers)

    @staticmethod
    def _header(headers, name):
        if not hasattr(headers, "items"):
            return None
        values = [value for key, value in headers.items()
                  if type(key) is str and key.lower() == name.lower()]
        return values[0] if len(values) == 1 and type(values[0]) is str else None

    def _authenticate(self, headers):
        authorization = self._header(headers, "Authorization")
        if authorization is None or not authorization.startswith("Bearer "):
            return None
        token = authorization[7:]
        if not token.isascii() or not 16 <= len(token) <= 4096:
            return None
        identity = None
        for configured, worker in self._workers.items():
            if hmac.compare_digest(token, configured):
                identity = worker
        return identity

    @staticmethod
    def _error(status, code):
        return Response(status, {"error": code})

    def _owned_job(self, job_id, identity, claim_token):
        job = self.store.get(job_id, private=True)
        with self.store.queue._db() as db:
            claim = db.execute("SELECT * FROM reference_claims WHERE job_id=?", (job_id,)).fetchone()
        if (not claim or (claim["node_id"], claim["worker_id"], claim["instance_id"]) !=
                (identity["node_id"], identity["worker_id"], identity["worker_instance_id"])
                or type(claim_token) is not str or not claim_token.isascii()
                or not hmac.compare_digest(claim["token"], claim_token)):
            raise Conflict("worker grant ownership mismatch")
        return job

    def _grant(self, job_id, identity, claim_token):
        job = self._owned_job(job_id, identity, claim_token)
        if job["status"] != "running" or job["phase"] != "awaiting_transfer":
            raise Conflict("worker cannot fetch in this execution state")
        grant = self.store.private_transfer_grant(job_id)
        if grant is None:
            raise Conflict("transfer grant not ready")
        try:
            validate_grant(grant, job=job)
        except ValueError:
            raise Conflict("persisted transfer grant is unavailable") from None
        return grant

    def handle(self, method, path, headers, body=b"", *, protected_transport=False):
        if protected_transport is not True:
            return self._error(503, "protected_transport_required")
        identity = self._authenticate(headers)
        if identity is None:
            return self._error(401, "unauthorized")
        try:
            if method == 'GET' and path == PREFIX + '/next':
                return Response(200, {'job': self.store.next_for_node(identity['node_id'])})
            if method == "POST" and path == PREFIX + "/claim":
                payload = protocol.decode_submission(body)
                protocol._closed(payload, ("jobId", "claimRequestId"), "claim")
                protocol._slug(payload["jobId"], "job ID")
                protocol._slug(payload["claimRequestId"], "claim request ID")
                claim = self.store.claim(payload["jobId"], **identity,
                                         claim_request_id=payload["claimRequestId"])
                if claim is None:
                    return self._error(409, "claim_unavailable")
                return Response(200, claim)
            if method == "POST" and path == PREFIX + "/results":
                payload = protocol.decode_submission(body)
                protocol._closed(payload, ("claimRequestId", "claimToken", "delivery"), "worker results")
                protocol._slug(payload["claimRequestId"], "claim request ID")
                token = payload["claimToken"]
                if (type(token) is not str or not 1 <= len(token) <= 128 or not token.isascii()):
                    raise ValueError("invalid claim token")
                receipt = self.receiver.receive(payload["delivery"], **identity,
                    claim_request_id=payload["claimRequestId"], claim_token=token)
                return Response(200, receipt)
            action = re.fullmatch(re.escape(PREFIX) + r'/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})/(reservation|phase)', path) if type(path) is str else None
            if method == 'POST' and action:
                job_id, kind = action.groups()
                payload = protocol.decode_submission(body)
                field = 'transferRequest' if kind == 'reservation' else 'phase'
                protocol._closed(payload, ('claimToken', field), 'worker control')
                job = self._owned_job(job_id, identity, payload['claimToken'])
                if job['status'] != 'running':
                    raise Conflict('worker execution is not running')
                if kind == 'reservation':
                    return Response(200, self.store.reserve(job_id, payload[field], node_id=identity['node_id']))
                if payload[field] not in ('fetching', 'processing'):
                    raise ValueError('worker may only advance transfer and processing phases')
                return Response(200, self.store.update_phase(job_id, payload[field]))
            match = re.fullmatch(re.escape(PREFIX) + r"/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})/(transfer-grant|acceptance)", path) if type(path) is str else None
            if method == "GET" and match:
                claim_token = self._header(headers, "X-Claim-Token")
                if claim_token is None or not 1 <= len(claim_token) <= 128:
                    return self._error(401, "claim_authentication_required")
                if match[2] == 'acceptance':
                    self._owned_job(match[1], identity, claim_token)
                    return Response(200, self.store.acceptance_receipts(match[1]))
                return Response(200, self._grant(match[1], identity, claim_token))
            return self._error(404, "not_found")
        except Conflict:
            return self._error(409, "worker_conflict")
        except (ValueError, TypeError):
            return self._error(400, "invalid_request")
        except Exception:
            return self._error(500, "internal_error")
