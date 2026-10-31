"""Fixed-origin worker result streaming; no spool copy, file deletion, or ACK.

HTTP consumers must abort their response if read/chunks raises, and must close
the stream on disconnect. Bytes are provisional until the whole object verifies;
the last chunk is withheld until its exact size, EOF and SHA256 are established.
The worker endpoint owns the upstream read pin until that response closes.
"""
import hashlib
import ssl
from types import MappingProxyType
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, Request, build_opener

from . import reference_protocol as protocol

CHUNK_BYTES = 64 * 1024
RESULT_PREFIX = "/ltds/worker/v1/jobs/"


class ResultProxyError(ValueError):
    """Secret-free configuration, identity, admission or stream failure."""


def _origin(value):
    if type(value) is not str:
        raise ResultProxyError("configured worker HTTPS origin required")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ResultProxyError("invalid worker origin") from None
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.path not in ("", "/")
            or parsed.query or parsed.fragment or "\\" in value
            or any(ord(c) <= 32 for c in value) or port == 0):
        raise ResultProxyError("configured worker HTTPS origin required")
    return value.rstrip("/")


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ResultProxyError("worker result redirects are forbidden")


class ResultProxyStream:
    """Single-consumer response DTO. Always use as a context manager or close()."""
    status = 200

    def __init__(self, response, lease, *, byte_size, sha256, manifest_sha256, content_type):
        self._response, self._lease = response, lease
        self.byte_size, self.sha256 = byte_size, sha256
        self.headers = MappingProxyType({"Content-Type": content_type,
            "Content-Length": str(byte_size), "X-Result-Sha256": sha256,
            "X-Result-Manifest-Sha256": manifest_sha256,
            "Cache-Control": "no-store"})
        self._digest, self._count = hashlib.sha256(), 0
        self.verified, self.closed = False, False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        if self.closed:
            return
        self.closed = True
        failed = False
        try:
            self._response.close()
        except Exception:
            failed = True
        try:
            self._lease.__exit__(None, None, None)
        except Exception:
            failed = True
        if failed:
            raise ResultProxyError("worker result stream close failed") from None

    def _fail(self):
        try:
            self.close()
        except Exception:
            pass
        raise ResultProxyError("worker result stream integrity failed") from None

    def read(self, size=CHUNK_BYTES):
        """Read at most one bounded chunk; never return an unverified last chunk."""
        if type(size) is not int or size < 1:
            raise ResultProxyError("positive bounded stream read required")
        if self.verified:
            self.close()
            return b""
        if self.closed:
            raise ResultProxyError("worker result stream is closed")
        try:
            remaining = self.byte_size - self._count
            chunk = self._response.read(min(size, CHUNK_BYTES, remaining + 1))
            if type(chunk) is not bytes or len(chunk) > min(size, CHUNK_BYTES, remaining + 1):
                self._fail()
            self._count += len(chunk)
            if self._count > self.byte_size or (not chunk and self._count != self.byte_size):
                self._fail()
            self._digest.update(chunk)
            if self._count == self.byte_size:
                extra = self._response.read(1)
                if extra != b"" or self._digest.hexdigest() != self.sha256:
                    self._fail()
                self.verified = True
            return chunk
        except Exception:
            self._fail()

    def chunks(self):
        try:
            while chunk := self.read():
                yield chunk
        finally:
            self.close()

    def __iter__(self):
        return self.chunks()


class ResultProxy:
    def __init__(self, worker_origins, *, worker_bearers, admit_read,
                 ssl_context=None, opener=None, timeout=30):
        """admit_read(binding) must return an entered-lifetime budget context.

        The callback receives exact byteSize and bufferBytes before any request;
        it admits bounded streaming capacity, never a full archive disk copy.
        A caller-supplied opener is a trusted synthetic-test boundary.
        """
        if (type(worker_origins) is not dict or type(worker_bearers) is not dict
                or not worker_origins or set(worker_origins) != set(worker_bearers)):
            raise ResultProxyError("explicit worker origins and separate read credentials required")
        origins, tokens = {}, {}
        for node, origin in worker_origins.items():
            protocol._slug(node, "worker node ID")
            origins[node] = _origin(origin)
            token = worker_bearers[node]
            if (type(token) is not str or not 16 <= len(token) <= 4096
                    or any(ord(c) < 33 or ord(c) > 126 for c in token)):
                raise ResultProxyError("invalid worker read credential")
            tokens[node] = token
        if ssl_context is not None and not isinstance(ssl_context, ssl.SSLContext):
            raise ResultProxyError("invalid worker TLS context")
        context = ssl_context or ssl.create_default_context()
        if not context.check_hostname or context.verify_mode != ssl.CERT_REQUIRED:
            raise ResultProxyError("worker TLS verification required")
        if type(timeout) not in (int, float) or not 0 < timeout <= 300:
            raise ResultProxyError("invalid result timeout")
        if not callable(admit_read) or (opener is not None and not callable(opener)):
            raise ResultProxyError("result admission and opener must be callable")
        self._origins, self._tokens = MappingProxyType(origins), MappingProxyType(tokens)
        self._admit_read = admit_read
        self._open = opener or build_opener(_NoRedirects(), HTTPSHandler(context=context)).open
        self.timeout = timeout

    def open(self, job, result_manifest, object_id):
        """Open one exact sealed result from its configured assigned worker node."""
        lease, entered, response = None, False, None
        try:
            protocol._slug(job["jobId"], "job ID")
            protocol._integer(job["generation"], "generation")
            protocol._slug(object_id, "result object ID")
            manifest = protocol.validate_result_manifest(result_manifest, job_id=job["jobId"],
                generation=job["generation"], environment=job["environment"], pipeline_digest=job["pipelineDigest"])
            if manifest["resultManifestSha256"] != job["resultManifestSha256"]:
                raise ResultProxyError("result manifest is not the job sealed result")
            node = job["nodeId"]
            if type(node) is not str or node not in self._origins:
                raise ResultProxyError("job worker has no configured result origin")
            obj = next((obj for obj in manifest["objects"] if obj["id"] == object_id), None)
            if obj is None:
                raise ResultProxyError("object is outside sealed result")
            binding = dict(nodeId=node, jobId=job["jobId"], generation=job["generation"],
                environment=job["environment"], resultManifestSha256=manifest["resultManifestSha256"],
                objectId=obj["id"], byteSize=obj["byteSize"], bufferBytes=CHUNK_BYTES)
            lease = self._admit_read(binding)
            lease.__enter__()
            entered = True
            url = self._origins[node] + RESULT_PREFIX + job["jobId"] + "/results/" + obj["id"]
            request = Request(url, method="GET", headers={
                "Authorization": "Bearer " + self._tokens[node], "Accept-Encoding": "identity",
                "X-Result-Generation": str(job["generation"]), "X-Result-Sha256": obj["sha256"],
                "X-Result-Manifest-Sha256": manifest["resultManifestSha256"]})
            try:
                response = self._open(request, timeout=self.timeout)
            except HTTPError as error:
                error.close()
                raise ResultProxyError("worker rejected result read") from None
            if response.status != 200 or response.geturl() != url:
                raise ResultProxyError("unexpected worker result response")
            if (response.headers.get("X-Result-Sha256") != obj["sha256"]
                    or response.headers.get("X-Result-Manifest-Sha256") != manifest["resultManifestSha256"]):
                raise ResultProxyError("worker result response identity mismatch")
            length = response.headers.get("Content-Length")
            if length is not None and (not length.isascii() or not length.isdecimal() or int(length) != obj["byteSize"]):
                raise ResultProxyError("worker result length mismatch")
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                raise ResultProxyError("encoded worker results are forbidden")
            content_type = "application/zip" if obj["role"] == "odm_archive" else "application/octet-stream"
            if response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != content_type:
                raise ResultProxyError("worker result content type mismatch")
            return ResultProxyStream(response, lease, byte_size=obj["byteSize"], sha256=obj["sha256"],
                manifest_sha256=manifest["resultManifestSha256"], content_type=content_type)
        except Exception:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass
            if entered:
                try:
                    lease.__exit__(None, None, None)
                except Exception:
                    pass
            raise ResultProxyError("worker result read unavailable") from None
