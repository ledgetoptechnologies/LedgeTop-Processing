"""Opt-in HTTP metadata adapter, independent of listener/framework details.

The trusted listener must supply protected_transport=True only for its own TLS
or loopback connection. Forwarded headers never establish this trust. This
module performs no transfer, execution, token issuance or network requests.
"""
from dataclasses import dataclass, field
import hmac
from urllib.parse import parse_qs, urlsplit
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
                 result_proxy=None, viewer_receipt_keys=None, source_spool=None,
                 source_attestation_keys=None, authority_id=None):
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
        if any(not protocol.valid_odm_option_name(name) for name in options):
            raise ValueError("invalid ODM option allowlist")
        self.store = store
        self._bearers = MappingProxyType(dict(bearer_tokens))
        self.capability_fingerprint = capability_fingerprint
        self.options_fingerprint = options_fingerprint
        self.allowed_odm_options = options
        self.treedar_available = treedar_available
        self.result_proxy = result_proxy
        self.source_spool = source_spool
        self.source_attestation_keys = source_attestation_keys
        self.authority_id = authority_id
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

    def _capabilities(self, environment, *, source_intake=False):
        result={"protocol": "ltds-reference-v1", "schemaVersion": 1,
                "environment": environment, "capabilityFingerprint": self.capability_fingerprint,
                "limits": {"metadataBytes": protocol.MAX_BODY_BYTES, "objects": protocol.MAX_OBJECTS,
                           "integer": protocol.MAX_SAFE_INTEGER},
                "engines": {"odm": {"available": True, "optionsFingerprint": self.options_fingerprint},
                            "treedar": {"available": self.treedar_available,
                                        "admissionVersion": protocol.TREEDAR_ADMISSION_VERSION},
                            "gaussian_splat": {"available": False, "enabled": False}}}
        if not source_intake:
            return result
        result['viewerAcceptance']={'enabled': bool(self.viewer_receipt_keys)}
        if self.source_spool is not None and self.source_attestation_keys is not None and self.authority_id:
            from .reference_limits import V2_SOURCE_INTAKE_BYTES
            result['sourceIntake']={**self.store.source_capacity(self.source_spool),
                                    'maxMetadataBytes': V2_SOURCE_INTAKE_BYTES, 'enabled': True}
        else:
            result['sourceIntake']={'enabled': False}
        return result

    @staticmethod
    def _error(status, code):
        return Response(status, {"error": code})

    def _job(self, job_id, environment, *, private=False):
        try:
            job = self.store.get(job_id, private=private)
        except Conflict:
            return None
        if job["environment"] != environment:
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
        if method == "GET" and path in (protocol.PREFIX + "/capabilities", "/ltds/v2/capabilities"):
            return Response(200, self._capabilities(environment, source_intake=path == "/ltds/v2/capabilities"))
        try:
            from . import reference_logs as logs
            log_request = logs.request(path, protocol.PREFIX)
            if log_request is not None:
                if method != 'GET' or body not in (b'', ''):
                    return self._error(400, 'invalid_log_request')
                job_id, generation, after, limit = log_request
                job = self._job(job_id, environment)
                if job is None:
                    return self._error(404, 'not_found')
                if generation != job['generation']:
                    return self._error(409, 'log_binding_mismatch')
                if job['nodeId'] is None and job['status'] == 'queued':
                    return Response(200, logs.page(job, [], after))
                if self.result_proxy is None:
                    return self._error(503, 'execution_logs_unavailable')
                try:
                    return Response(200, self.result_proxy.logs(job, after=after, limit=limit))
                except Exception:
                    return self._error(503, 'execution_logs_unavailable')
            if method == "POST" and path == "/ltds/v2/jobs":
                if (self.source_spool is None or self.source_attestation_keys is None
                        or not self.authority_id):
                    return self._error(503, "source_intake_unavailable")
                from .reference_source_intake import validate_source_intake
                from .reference_limits import V2_SOURCE_INTAKE_BYTES
                validated=validate_source_intake(protocol.decode_submission(
                    body, max_bytes=V2_SOURCE_INTAKE_BYTES),
                    environment=environment,capability_fingerprint=self.capability_fingerprint,
                    options_fingerprint=self.options_fingerprint,allowed_odm_options=self.allowed_odm_options,
                    available_engines=("odm","treedar") if self.treedar_available else ("odm",),
                    trusted_source_keys=self.source_attestation_keys,authority_id=self.authority_id)
                job,created=self.store.submit_source_intake(validated,
                    capability_fingerprint=self.capability_fingerprint,
                    options_fingerprint=self.options_fingerprint,allowed_odm_options=self.allowed_odm_options,
                    source_spool=self.source_spool,
                    trusted_source_keys=self.source_attestation_keys,authority_id=self.authority_id,
                    available_engines=("odm","treedar") if self.treedar_available else ("odm",))
                return Response(201 if created else 200,job)
            source_job=re.fullmatch(r"/ltds/v2/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})(/source/complete)?",path)
            if source_job:
                job_id,suffix=source_job.groups();job=self._job(job_id,environment)
                if job is None:return self._error(404,"not_found")
                if method=="GET" and suffix is None:return Response(200,job)
                if method=="POST" and suffix=="/source/complete":
                    if (self.source_spool is None or self.source_attestation_keys is None
                            or not self.authority_id):return self._error(503,"source_intake_unavailable")
                    completion=protocol.decode_submission(body)
                    protocol._closed(completion,("generation","sourceManifestSha256","derivedManifestSha256","pipelineManifestSha256"),"source completion")
                    result,created=self.store.complete_source_intake(job_id,
                        generation=completion["generation"],source_manifest_sha256=completion["sourceManifestSha256"],
                        derived_manifest_sha256=completion["derivedManifestSha256"],
                        pipeline_manifest_sha256=completion["pipelineManifestSha256"],source_spool=self.source_spool)
                    return Response(201 if created else 200,result)
                return self._error(405,"method_not_allowed")
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

    @staticmethod
    def _header(headers,name):
        values=[value for key,value in headers.items() if type(key) is str and key.lower()==name.lower()]
        if len(values)!=1 or type(values[0]) is not str:raise ValueError("invalid source transfer header")
        return values[0]

    def stream_source_object(self,path,headers,reader,content_length,*,protected_transport=False):
        if protected_transport is not True:return self._error(503,"protected_transport_required")
        environment=self._authenticate(headers)
        if environment is None:return self._error(401,"unauthorized")
        if (self.source_spool is None or self.source_attestation_keys is None
                or not self.authority_id):return self._error(503,"source_intake_unavailable")
        try:
            parsed=urlsplit(path);query=parse_qs(parsed.query,keep_blank_values=True,strict_parsing=True)
        except ValueError:return self._error(400,"invalid_request")
        match=re.fullmatch(r"/ltds/v2/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})/source/([A-Za-z0-9][A-Za-z0-9_-]{0,127})",parsed.path)
        if not match:return self._error(404,"not_found")
        try:
            if set(query)!={"generation"} or len(query["generation"])!=1:return self._error(400,"invalid_request")
            job_id,object_id=match.groups()
            if self._job(job_id,environment) is None:return self._error(404,"not_found")
            generation_text=query["generation"][0];digest=self._header(headers,"Content-SHA256")
            if not re.fullmatch(r"[1-9][0-9]{0,8}",generation_text) or type(content_length) is not int or content_length<0:return self._error(400,"invalid_request")
            expected=self.store.source_object_expectation(job_id,generation=int(generation_text),object_id=object_id)
            if expected["environment"]!=environment:return self._error(404,"not_found")
            if content_length!=expected["byteSize"] or digest!=expected["sha256"]:return self._error(409,"source_manifest_mismatch")
            if expected["byteSize"]>self.source_spool.max_object_bytes:return self._error(413,"source_object_too_large")
            receipt,replay=self.source_spool.receive_object(job_id=job_id,generation=expected["generation"],
                object_id=object_id,byte_size=expected["byteSize"],sha256=expected["sha256"],reader=reader,
                reserved_bytes=expected["otherReservedBytes"])
            created=self.store.record_source_object_receipt(job_id,receipt,source_spool=self.source_spool)
            return Response(201 if created else 200,{"jobId":job_id,"generation":expected["generation"],
                "objectId":object_id,"byteSize":expected["byteSize"],"sha256":expected["sha256"],
                "objectKind":expected["objectKind"],"received":True,"replayed":replay or not created})
        except Conflict:return self._error(409,"reference_conflict")
        except (ValueError,TypeError):return self._error(400,"invalid_request")
        except Exception as error:
            from .source_spool import SourceSpoolError
            if isinstance(error,SourceSpoolError):
                status=413 if error.code=="source_object_too_large" else 507 if error.code=="spool_capacity_unavailable" else 503 if error.code in ("spool_mount_unavailable","spool_mount_changed") else 409 if error.code in ("source_object_conflict","source_object_receipt_mismatch") else 400
                return self._error(status,error.code)
            return self._error(500,"internal_error")
