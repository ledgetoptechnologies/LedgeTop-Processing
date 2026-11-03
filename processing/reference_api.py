"""Opt-in HTTP metadata adapter, independent of listener/framework details.

The trusted listener must supply protected_transport=True only for its own TLS
or loopback connection. Forwarded headers never establish this trust. This
module performs no transfer, execution, token issuance or network requests.
"""
from dataclasses import dataclass, field
import hmac
import re
from types import MappingProxyType

from .maintenance import Conflict
from . import reference_protocol as protocol


@dataclass(frozen=True)
class Response:
    status: int
    body: dict
    headers: dict = field(default_factory=lambda: {
        "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff"})


class ReferenceAPI:
    def __init__(self, store, *, bearer_tokens, capability_fingerprint,
                 options_fingerprint, allowed_odm_options=(), treedar_available=False,
                 result_proxy=None, viewer_receipt_keys=None):
        protocol._hash(capability_fingerprint, "capability fingerprint")
        protocol._hash(options_fingerprint, "options fingerprint")
        if type(bearer_tokens) is not dict or not bearer_tokens:
            raise ValueError("explicit environment-scoped bearer configuration required")
        for token, environment in bearer_tokens.items():
            if (type(token) is not str or len(token) < 16 or len(token) > 4096
                    or not all(0x21 <= ord(char) <= 0x7e for char in token)):
                raise ValueError("invalid bearer configuration")
            protocol._environment(environment)
        if type(treedar_available) is not bool:
            raise ValueError("explicit TreeDar availability boolean required")
        options = frozenset(allowed_odm_options)
        if any(type(name) is not str or not re.fullmatch(r"[a-z][a-z0-9-]{0,79}", name) for name in options):
            raise ValueError("invalid ODM option allowlist")
        self.store = store
        self._bearers = MappingProxyType(dict(bearer_tokens))
        self.capability_fingerprint = capability_fingerprint
        self.options_fingerprint = options_fingerprint
        self.allowed_odm_options = options
        self.treedar_available = treedar_available
        self.result_proxy = result_proxy
        from .viewer_acceptance_receipt import validate_trusted_viewer_receipt_keys
        try:
            self.viewer_receipt_keys = validate_trusted_viewer_receipt_keys(
                {} if viewer_receipt_keys is None else viewer_receipt_keys)
        except ValueError:
            raise ValueError("invalid pinned Viewer receipt keys") from None
        from .reference_terminal import TerminalService
        self.terminal = TerminalService(store)

    def _authenticate(self, headers):
        if not hasattr(headers, "items"):
            return None
        entries = [value for name, value in headers.items()
                   if type(name) is str and name.lower() == "authorization"]
        if len(entries) != 1 or type(entries[0]) is not str or not entries[0].startswith("Bearer "):
            return None
        token = entries[0][7:]
        # ASCII bounds also ensure compare_digest cannot raise on hostile input.
        if not 16 <= len(token) <= 4096 or not token.isascii():
            return None
        environment = None
        for configured, bound_environment in self._bearers.items():
            if hmac.compare_digest(token, configured):
                environment = bound_environment
        return environment

    def _capabilities(self, environment):
        return {"protocol": "ltds-reference-v1", "schemaVersion": 1,
                "environment": environment, "capabilityFingerprint": self.capability_fingerprint,
                "limits": {"metadataBytes": protocol.MAX_BODY_BYTES, "objects": protocol.MAX_OBJECTS,
                           "integer": protocol.MAX_SAFE_INTEGER},
                "engines": {"odm": {"available": True, "optionsFingerprint": self.options_fingerprint},
                            "treedar": {"available": self.treedar_available,
                                        "admissionVersion": protocol.TREEDAR_ADMISSION_VERSION},
                            "gaussian_splat": {"available": False, "enabled": False}}}

    @staticmethod
    def _error(status, code):
        return Response(status, {"error": code})

    def _job(self, job_id, environment, *, private=False):
        try:
            job = self.store.get_legacy_job(job_id, environment, private=private)
        except Conflict:
            return None
        if job is None:
            return None
        return job

    def handle(self, method, path, headers, body=b"", *, protected_transport=False):
        """Return JSON-ready metadata, with no forwarding-header assumptions.

        The caller must preserve this status/headers and serialize body as JSON.
        This adapter deliberately has no claims, grants or worker-facing routes.
        """
        if protected_transport is not True:
            return self._error(503, "protected_transport_required")
        environment = self._authenticate(headers)
        if environment is None:
            return self._error(401, "unauthorized")
        if type(method) is not str or type(path) is not str:
            return self._error(404, "not_found")
        if method == "GET" and path == protocol.PREFIX + "/capabilities":
            return Response(200, self._capabilities(environment))
        try:
            binary = re.fullmatch(re.escape(protocol.PREFIX) + r'/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})/results/([A-Za-z0-9][A-Za-z0-9_-]{0,127})', path)
            if method == 'GET' and binary:
                job = self._job(binary[1], environment, private=True)
                if job is None:
                    return self._error(404, 'not_found')
                if self.result_proxy is None or job['resultManifest'] is None:
                    return self._error(409, 'result_not_ready')
                if binary[2] not in {obj['id'] for obj in job['resultManifest']['objects']}:
                    return self._error(404, 'not_found')
                range_values = [value for name, value in headers.items()
                                if type(name) is str and name.lower() == 'range']
                if len(range_values) > 1:
                    return self._error(400, 'duplicate_header')
                range_header = range_values[0] if range_values else None
                if range_header is not None and type(range_header) is not str:
                    return self._error(400, 'invalid_request')
                public = self._job(binary[1], environment)
                stream = self.result_proxy.open(public, job['resultManifest'], binary[2],
                                                range_header=range_header)
                from .reference_binary import BinaryResponse
                return BinaryResponse(stream, status=stream.status, headers=stream.headers)
            lookup = re.fullmatch(re.escape(protocol.PREFIX) + r'/jobs/by-submission/([0-9a-f-]{36})', path)
            if method == 'GET' and lookup:
                job = self.store.find_submission(lookup[1], environment=environment)
                return Response(200, job) if job else self._error(404, 'not_found')
            if method == "POST" and path == protocol.PREFIX + "/jobs":
                payload = protocol.decode_submission(body)
                validated = protocol.validate_submission(payload, environment=environment,
                    capability_fingerprint=self.capability_fingerprint,
                    options_fingerprint=self.options_fingerprint,
                    allowed_odm_options=self.allowed_odm_options,
                    available_engines=("odm", "treedar") if self.treedar_available else ("odm",))
                job, created = self.store.submit(validated)
                return Response(201 if created else 200, job)
            match = re.fullmatch(re.escape(protocol.PREFIX) + r"/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})(/results|/acceptance|/transfer-grants|/terminal-outcome|/terminal-acceptance)?", path)
            if not match:
                return self._error(404, "not_found")
            job_id, suffix = match.groups()
            # Scope authorization before parsing action bodies or returning any
            # details about existing jobs or their execution/acceptance state.
            job = self._job(job_id, environment)
            if job is None:
                return self._error(404, "not_found")
            if method == "GET" and suffix is None:
                return Response(200, job)
            if method == 'GET' and suffix == '/terminal-outcome':
                return Response(200, self.terminal.outcome(job_id, environment))
            if method == 'POST' and suffix == '/terminal-acceptance':
                receipt = protocol.decode_submission(body)
                return Response(200, self.terminal.accept(job_id, receipt, environment))
            if method == "GET" and suffix == "/results":
                private = self._job(job_id, environment, private=True)
                if private["resultManifest"] is None:
                    return self._error(409, "result_not_ready")
                return Response(200, private["resultManifest"])
            if method == "POST" and suffix == "/acceptance":
                payload = protocol.decode_submission(body)
                viewer_receipt = None
                if type(payload) is dict and set(payload) == {"acceptance", "viewerReceipt"}:
                    receipt, viewer_receipt = payload["acceptance"], payload["viewerReceipt"]
                    if not self.viewer_receipt_keys:
                        raise Conflict("signed Viewer receipt verification is not configured")
                else:
                    receipt = payload
                return Response(200, self.store.accept(job_id, receipt, viewer_receipt=viewer_receipt,
                    trusted_viewer_receipt_keys=self.viewer_receipt_keys))
            if method == 'POST' and suffix == '/transfer-grants':
                grant = protocol.decode_submission(body)
                return Response(200, self.store.receive_transfer_grant(job_id, grant))
            return self._error(405, "method_not_allowed")
        except (protocol.ProtocolError, ValueError, TypeError):
            return self._error(400, "invalid_request")
        except Conflict:
            return self._error(409, "reference_conflict")
        except Exception:
            # Framework/listener must not turn internal exceptions into a body
            # containing paths, credentials or private worker metadata.
            return self._error(500, "internal_error")
