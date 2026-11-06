"""Bounded resumable push of sealed result objects into Viewer acceptance storage.

This is only the transport leg. The caller must persist the exact acceptance
envelope before invoking it and retain source objects until the returned
Viewer-signed receipt is durably recorded by the coordinator.
"""
from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import hmac
import json
import re
import secrets
import ssl
from urllib.error import HTTPError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener

from . import reference_protocol as protocol
from .viewer_acceptance_receipt import verify_viewer_acceptance_receipt
from .viewer_result_manifest import derive_viewer_result_manifest, validate_viewer_acceptance

PATH = "/api/v1/reference/v1/acceptances"
PATH_V2 = "/api/v1/reference/v2/acceptances"
MAX_JSON_BYTES = protocol.MAX_BODY_BYTES
DEFAULT_CHUNK_BYTES = 4 * 1024 * 1024
MAX_CHUNK_BYTES = 16 * 1024 * 1024
MAX_OBJECTS = protocol.MAX_OBJECTS
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_HASH = re.compile(r"[a-f0-9]{64}\Z")


class ViewerTransferUnavailable(ValueError):
    """A safe, retryable or operator-visible transfer failure code."""


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ViewerTransferUnavailable("viewer_redirect_rejected")


def _origin(value):
    if type(value) is not str:
        raise ValueError("configured Viewer HTTPS origin required")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ValueError("invalid Viewer origin") from None
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.path not in ("", "/")
            or parsed.query or parsed.fragment or "\\" in value
            or any(ord(c) <= 32 for c in value) or port == 0):
        raise ValueError("configured Viewer HTTPS origin required")
    return value.rstrip("/")


def _credential(value, label, maximum):
    if (type(value) is not str or not 1 <= len(value) <= maximum
            or any(ord(c) < 33 or ord(c) > 126 for c in value)):
        raise ValueError("invalid configured " + label)
    return value


def _decode_json(body):
    if type(body) is not bytes or len(body) > MAX_JSON_BYTES:
        raise ViewerTransferUnavailable("viewer_response_limit")
    try:
        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise ValueError("duplicate key")
                result[key] = value
            return result
        def nonfinite(_):
            raise ValueError("non-finite number")
        value = json.loads(body.decode("utf-8"), object_pairs_hook=pairs, parse_constant=nonfinite)
        protocol.canonical_json(value)
    except (ValueError, UnicodeError, RecursionError):
        raise ViewerTransferUnavailable("viewer_invalid_json") from None
    if type(value) is not dict:
        raise ViewerTransferUnavailable("viewer_invalid_json")
    return value


def _service_auth_headers(secret, key_id, method, path, body, *, timestamp, nonce):
    body_hash = hashlib.sha256(body).hexdigest()
    canonical = "\n".join(("ltds-viewer-service-v1", method.upper(), path,
                           str(timestamp), nonce, body_hash)).encode("utf-8")
    signature = base64.urlsafe_b64encode(
        hmac.new(secret, canonical, hashlib.sha256).digest()).decode("ascii").rstrip("=")
    return {
        "X-LTDS-Key-Id": key_id,
        "X-LTDS-Timestamp": str(timestamp),
        "X-LTDS-Nonce": nonce,
        "X-LTDS-Content-SHA256": body_hash,
        "X-LTDS-Signature": signature,
    }


class ViewerAcceptanceTransfer:
    """Upload one immutable job result using the Viewer service-auth protocol.

    ``result_proxy`` must be the coordinator's fixed-origin ResultProxy. It
    opens only exact manifest objects and enforces the existing aggregate
    stream budget. No result bytes are spooled on coordinator storage.
    """

    def __init__(self, origin, *, key_id, secret, trusted_receipt_keys,
                 result_proxy, ssl_context=None, opener=None, chunk_bytes=DEFAULT_CHUNK_BYTES,
                 timeout=30):
        self.origin = _origin(origin)
        self.key_id = _credential(key_id, "Viewer service key ID", 128)
        self.secret = _credential(secret, "Viewer service key", 4096).encode("ascii")
        if not callable(getattr(result_proxy, "open", None)):
            raise ValueError("configured exact result proxy required")
        if ssl_context is not None and not isinstance(ssl_context, ssl.SSLContext):
            raise ValueError("invalid Viewer TLS context")
        context = ssl_context or ssl.create_default_context()
        if not context.check_hostname or context.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError("Viewer TLS verification required")
        if type(chunk_bytes) is not int or not 1 <= chunk_bytes <= MAX_CHUNK_BYTES:
            raise ValueError("invalid Viewer chunk size")
        if type(timeout) not in (int, float) or not 0 < timeout <= 300:
            raise ValueError("invalid Viewer transfer timeout")
        if opener is not None and not callable(opener):
            raise ValueError("invalid Viewer transport")
        from .viewer_acceptance_receipt import validate_trusted_viewer_receipt_keys
        self.trusted_receipt_keys = validate_trusted_viewer_receipt_keys(trusted_receipt_keys)
        if not self.trusted_receipt_keys:
            raise ValueError("Viewer receipt verification keys are required")
        self.result_proxy = result_proxy
        self.chunk_bytes, self.timeout = chunk_bytes, timeout
        self._open = opener or build_opener(ProxyHandler({}), _NoRedirects(), HTTPSHandler(context=context)).open

    def _headers(self, method, path, body):
        timestamp = str(int(datetime.now(timezone.utc).timestamp()))
        nonce = secrets.token_urlsafe(24)
        return _service_auth_headers(self.secret, self.key_id, method, path, body,
            timestamp=timestamp, nonce=nonce) | {"Accept-Encoding": "identity", "Accept": "application/json"}

    def _request(self, method, path, body=b"", *, content_type="application/json", allowed=(200,)):
        if type(path) is not str or not any(path.startswith(prefix + "/") for prefix in (PATH, PATH_V2)) or "#" in path:
            raise ViewerTransferUnavailable("viewer_path_invalid")
        headers = self._headers(method, path, body)
        headers["Content-Type"] = content_type
        if method in ("PUT", "POST"):
            headers["Content-Length"] = str(len(body))
        url = self.origin + path
        request = Request(url, data=body if method in ("PUT", "POST") else None,
                          method=method, headers=headers)
        try:
            response = self._open(request, timeout=self.timeout)
        except HTTPError as error:
            status = error.code
            error.close()
            if status not in allowed:
                raise ViewerTransferUnavailable("viewer_http_" + str(status)) from None
            return status, {}
        except Exception:
            raise ViewerTransferUnavailable("viewer_transport_unavailable") from None
        try:
            if response.status not in allowed or response.geturl() != url:
                raise ViewerTransferUnavailable("viewer_unexpected_response")
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                raise ViewerTransferUnavailable("viewer_encoded_response_rejected")
            if response.status == 204:
                return response.status, {}
            if response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json":
                raise ViewerTransferUnavailable("viewer_json_required")
            length = response.headers.get("Content-Length")
            if length is not None and (not length.isascii() or not length.isdecimal()
                                       or int(length) > MAX_JSON_BYTES):
                raise ViewerTransferUnavailable("viewer_response_limit")
            data = response.read(MAX_JSON_BYTES + 1)
            if len(data) > MAX_JSON_BYTES or (length is not None and len(data) != int(length)):
                raise ViewerTransferUnavailable("viewer_response_limit")
            return response.status, _decode_json(data)
        finally:
            response.close()

    @staticmethod
    def _transfer_identity(value, job, acceptance, result_manifest):
        if type(value) is not dict or value.get("receiptId") != acceptance["receiptId"]:
            raise ViewerTransferUnavailable("viewer_transfer_identity_mismatch")
        expected = {
            "jobId": job["jobId"], "generation": job["generation"],
            "environment": job["environment"],
            "inputManifestSha256": job["manifestSha256"],
            "resultManifestSha256": result_manifest["resultManifestSha256"],
        }
        if any(value.get(key) != expected_value for key, expected_value in expected.items()):
            raise ViewerTransferUnavailable("viewer_transfer_identity_mismatch")
        ids = [item["id"] for item in result_manifest["objects"]]
        objects = value.get("objects")
        if type(objects) is not list or len(objects) != len(ids):
            raise ViewerTransferUnavailable("viewer_transfer_object_mismatch")
        by_id = {item.get("objectId"): item for item in objects if type(item) is dict}
        if set(by_id) != set(ids):
            raise ViewerTransferUnavailable("viewer_transfer_object_mismatch")
        for obj in result_manifest["objects"]:
            state = by_id[obj["id"]]
            received = state.get("receivedBytes")
            transfer_state = state.get("state")
            if (type(received) is not int or received < 0 or received > obj["byteSize"]
                    or state.get("expectedBytes") != obj["byteSize"]
                    or state.get("expectedSha256") != obj["sha256"]
                    or transfer_state not in ("pending", "receiving", "complete")
                    or (transfer_state == "pending" and received != 0)
                    or (transfer_state == "receiving" and not 0 < received < obj["byteSize"])
                    or (transfer_state == "complete" and received != obj["byteSize"])):
                raise ViewerTransferUnavailable("viewer_transfer_object_mismatch")
        return by_id

    def _accepted(self, body, job, acceptance, result_manifest):
        receipt = body.get("acceptance")
        signed = body.get("viewerReceipt")
        checked = validate_viewer_acceptance(receipt, result_manifest=result_manifest,
                                               input_manifest_sha256=job["manifestSha256"])
        if (checked != acceptance or body.get("state") != "accepted"
                or body.get("receiptId") != acceptance["receiptId"]
                or body.get("fullyAccepted") is not True
                or set(body.get("acceptedObjectIds", [])) != set(acceptance["acceptedObjectIds"])):
            raise ViewerTransferUnavailable("viewer_acceptance_conflict")
        expected = {key: job[key] if key not in ("inputManifestSha256",) else job["manifestSha256"]
                    for key in ("jobId", "generation", "environment", "inputManifestSha256",
                                "pipelineDigest", "resultManifestSha256")}
        expected["resultManifestSha256"] = result_manifest["resultManifestSha256"]
        try:
            verify_viewer_acceptance_receipt(signed, trusted_keys=self.trusted_receipt_keys,
                expected=expected, result_manifest=result_manifest,
                accepted_object_ids=acceptance["acceptedObjectIds"])
        except (TypeError, ValueError):
            raise ViewerTransferUnavailable("viewer_signed_receipt_rejected") from None
        return {"acceptance": checked, "viewerReceipt": signed}

    def transfer(self, job, acceptance, *, viewer_result_manifest=None):
        """Resume or commit one transfer; return a verified receipt pair only.

        ``job`` must be the private durable coordinator record. ``acceptance``
        must be a stable, coordinator-persisted request reused on every retry.
        """
        required = ("jobId", "generation", "environment", "manifestSha256", "pipelineDigest",
                    "nodeId", "submission", "resultManifest", "resultManifestSha256")
        if type(job) is not dict or any(key not in job for key in required):
            raise ViewerTransferUnavailable("viewer_job_binding_unavailable")
        if job["environment"] not in ("production", "staging"):
            raise ViewerTransferUnavailable("viewer_job_binding_unavailable")
        submission, source_result_manifest = job["submission"], job["resultManifest"]
        # Source-bound jobs are accepted through Viewer's pull/copy-ACK path.
        # This legacy sender verifies V1 receipts and cannot establish their
        # source/attempt binding, so never contact Viewer for such a job.
        if ("sourceIntakeVersion" in job
                or type(submission) is dict and "sourceIntakeVersion" in submission):
            raise ViewerTransferUnavailable("viewer_source_bound_pull_required")
        if (type(submission) is not dict or type(submission.get("manifest")) is not dict
                or submission["manifest"].get("sha256") != job["manifestSha256"]
                or protocol.manifest_sha256(submission["manifest"]) != job["manifestSha256"]):
            raise ViewerTransferUnavailable("viewer_job_manifest_unavailable")
        try:
            source_result_manifest = protocol.validate_result_manifest(source_result_manifest, job_id=job["jobId"],
                generation=job["generation"], environment=job["environment"],
                pipeline_digest=job["pipelineDigest"])
        except (TypeError, ValueError):
            raise ViewerTransferUnavailable("viewer_result_manifest_invalid") from None
        try:
            result_manifest = derive_viewer_result_manifest(source_result_manifest,
                job_id=job["jobId"], generation=job["generation"], environment=job["environment"],
                pipeline_digest=job["pipelineDigest"])
        except (TypeError, ValueError):
            raise ViewerTransferUnavailable("viewer_result_manifest_invalid") from None
        if viewer_result_manifest is not None and viewer_result_manifest != result_manifest:
            raise ViewerTransferUnavailable("viewer_result_manifest_changed")
        if type(acceptance) is not dict:
            raise ViewerTransferUnavailable("viewer_acceptance_invalid")
        try:
            acceptance = validate_viewer_acceptance(acceptance, result_manifest=result_manifest,
                input_manifest_sha256=job["manifestSha256"])
        except (TypeError, ValueError):
            raise ViewerTransferUnavailable("viewer_acceptance_invalid") from None
        expected_ids = [item["id"] for item in result_manifest["objects"]]
        if (acceptance["acceptedObjectIds"] != expected_ids if result_manifest["schemaVersion"] == 2
                else set(acceptance["acceptedObjectIds"]) != set(expected_ids)):
            raise ViewerTransferUnavailable("viewer_acceptance_closure_incomplete")
        route = PATH_V2 if result_manifest["schemaVersion"] == 2 else PATH
        receipt_path = route + "/" + quote(acceptance["receiptId"], safe="")
        status, current = self._request("GET", receipt_path, allowed=(200, 202, 404))
        if status == 200 and current.get("state") == "accepted":
            return self._accepted(current, job, acceptance, result_manifest)
        expected = {
            "jobId": job["jobId"], "generation": job["generation"],
            "environment": job["environment"],
            "inputManifestSha256": job["manifestSha256"],
            "pipelineDigest": job["pipelineDigest"],
            "resultManifestSha256": result_manifest["resultManifestSha256"],
            "objectIds": expected_ids,
        }
        if status == 404:
            body = protocol.canonical_json({"inputManifest": submission["manifest"],
                "expected": expected, "resultManifest": result_manifest, "acceptance": acceptance})
            _status, current = self._request("PUT", receipt_path, body, allowed=(200, 202))
        by_id = self._transfer_identity(current, job, acceptance, result_manifest)
        for obj in result_manifest["objects"]:
            offset = by_id[obj["id"]]["receivedBytes"]
            if obj["byteSize"] == 0 and by_id[obj["id"]]["state"] != "complete":
                with self.result_proxy.open(job, source_result_manifest, obj["id"]) as source:
                    if source.read(1):
                        raise ViewerTransferUnavailable("worker_result_range_overrun")
                path = receipt_path + "/objects/" + quote(obj["id"], safe="") + "?" + urlencode({"offset": 0})
                _status, written = self._request("PUT", path, b"",
                    content_type="application/octet-stream", allowed=(200, 201))
                if written.get("receivedBytes") != 0 or written.get("state") != "object_complete":
                    raise ViewerTransferUnavailable("viewer_chunk_ack_mismatch")
                continue
            while offset < obj["byteSize"]:
                count = min(self.chunk_bytes, obj["byteSize"] - offset)
                range_header = f"bytes={offset}-{offset + count - 1}"
                with self.result_proxy.open(job, source_result_manifest, obj["id"],
                                            range_header=range_header) as source:
                    pieces, actual = [], 0
                    while actual < count:
                        piece = source.read(min(64 * 1024, count - actual))
                        if not piece:
                            raise ViewerTransferUnavailable("worker_result_range_truncated")
                        pieces.append(piece); actual += len(piece)
                    if source.read(1):
                        raise ViewerTransferUnavailable("worker_result_range_overrun")
                chunk = b"".join(pieces)
                path = receipt_path + "/objects/" + quote(obj["id"], safe="") + "?" + urlencode({"offset": offset})
                _status, written = self._request("PUT", path, chunk,
                    content_type="application/octet-stream", allowed=(200, 201))
                new_offset = written.get("receivedBytes")
                if (type(new_offset) is not int or new_offset != offset + len(chunk)
                        or written.get("state") not in ("receiving", "object_complete")):
                    raise ViewerTransferUnavailable("viewer_chunk_ack_mismatch")
                offset = new_offset
        _status, committed = self._request("POST", receipt_path + "/commit", b"", allowed=(200, 201))
        return self._accepted(committed, job, acceptance, result_manifest)
