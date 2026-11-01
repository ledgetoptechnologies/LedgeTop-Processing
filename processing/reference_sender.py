"""Private HTTPS transport for WorkerOutbox metadata; no engine or file actions.

The configured bearer identifies node/worker/instance. A trusted binding provider
returns the original queue claim for the six immutable result binding fields.
Neither credentials nor underlying transport exception text enter results/logs.
"""
import json
import ssl
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener

from . import reference_protocol as protocol
from .worker_outbox import BINDING, OutboxConflict
from .reference_transfer import _TransferWatchdog, _DeadlineHTTPSHandler

RESULTS_PATH = "/ltds/worker/v1/results"
MAX_RECEIPT_BYTES = 4 * 1024 * 1024
READ_BYTES = 64 * 1024


class SenderError(ValueError):
    """Secret-free transport or receipt error."""


def _credential(value, *, maximum):
    if (type(value) is not str or not 1 <= len(value) <= maximum
            or any(ord(c) < 33 or ord(c) > 126 for c in value)):
        raise SenderError("invalid configured credential")
    return value


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise SenderError("coordinator redirects are forbidden")


def decode_receipt(body):
    """Decode bounded finite UTF-8 JSON, rejecting duplicate keys at any depth."""
    if not isinstance(body, bytes) or len(body) > MAX_RECEIPT_BYTES:
        raise SenderError("invalid receipt body size")
    try:
        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise SenderError("duplicate receipt key")
                result[key] = value
            return result
        def nonfinite(_):
            raise SenderError("nonfinite receipt number")
        result = json.loads(body.decode("utf-8"), object_pairs_hook=pairs,
                            parse_constant=nonfinite)
        # JSON exponent overflow (1e999) does not invoke parse_constant.
        protocol.canonical_json(result)
        if type(result) is not dict:
            raise SenderError("receipt must be an object")
        return result
    except (ValueError, UnicodeError, RecursionError):
        raise SenderError("invalid durable receipt JSON") from None


class ReferenceSender:
    def __init__(self, outbox, origin, *, bearer_token, claim_binding_provider,
                 ssl_context=None, opener=None, timeout=30, lease_seconds=60, total_timeout=30):
        if type(origin) is not str:
            raise ValueError("configured HTTPS coordinator origin required")
        parsed = urlsplit(origin)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.path not in ("", "/")
                or parsed.query or parsed.fragment or "\\" in origin
                or any(ord(c) <= 32 for c in origin)):
            raise ValueError("configured HTTPS coordinator origin required")
        try:
            parsed.port
        except ValueError:
            raise ValueError("invalid coordinator port") from None
        if ssl_context is not None and not isinstance(ssl_context, ssl.SSLContext):
            raise ValueError("invalid TLS context")
        context = ssl_context or ssl.create_default_context()
        if not context.check_hostname or context.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError("coordinator TLS verification required")
        if type(timeout) not in (int, float) or not 0 < timeout <= 300:
            raise ValueError("invalid sender timeout")
        if type(total_timeout) not in (int, float) or not 0 < total_timeout <= 300:
            raise ValueError("invalid sender total timeout")
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 3600:
            raise ValueError("invalid sender lease")
        if not callable(claim_binding_provider) or (opener is not None and not callable(opener)):
            raise ValueError("callable sender providers required")
        self.outbox = outbox
        self._url = origin.rstrip("/") + RESULTS_PATH
        self._bearer = _credential(bearer_token, maximum=4096)
        self._binding_provider = claim_binding_provider
        self._open = opener or build_opener(ProxyHandler({}), _NoRedirects(), HTTPSHandler(context=context)).open
        self._default_open = self._open
        self._custom_opener, self._ssl_context = opener is not None, context
        self.total_timeout = total_timeout
        self.timeout, self.lease_seconds = timeout, lease_seconds

    def _send(self, delivery):
        binding = {key: delivery["payload"][key] for key in BINDING}
        claim = self._binding_provider(binding)
        protocol._closed(claim, ("claimRequestId", "claimToken"), "worker claim")
        protocol._slug(claim["claimRequestId"], "claim request ID")
        _credential(claim["claimToken"], maximum=128)
        envelope = {key: delivery[key] for key in ("outboxId", "payloadSha256", "payload")}
        body = protocol.canonical_json(dict(claimRequestId=claim["claimRequestId"],
            claimToken=claim["claimToken"], delivery=envelope))
        request = Request(self._url, data=body, method="POST", headers={
            "Authorization": "Bearer " + self._bearer,
            "Content-Type": "application/json", "Accept": "application/json",
            "Accept-Encoding": "identity"})
        watchdog = _TransferWatchdog(self.total_timeout)
        try:
            receipt = self._receive(request, watchdog)
        finally:
            watchdog.close()
        # A complete, verified receipt remains valid even if the deadline races
        # with acknowledgement. An ambiguous POST is retried with the same ID.
        self.outbox.acknowledge(delivery["outboxId"], receipt)

    def _receive(self, request, watchdog):
        try:
            custom_opener = self._custom_opener or self._open is not self._default_open
            opener = self._open if custom_opener else build_opener(
                ProxyHandler({}), _NoRedirects(),
                _DeadlineHTTPSHandler(watchdog, context=self._ssl_context)).open
            response = opener(request, timeout=min(self.timeout, watchdog.remaining()))
        except HTTPError as error:
            error.close()
            raise SenderError("coordinator rejected delivery") from None
        try:
            watchdog.response(response)
            watchdog.remaining()
            if response.status != 200 or response.geturl() != self._url:
                raise SenderError("unexpected coordinator response")
            content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            if content_type != "application/json":
                raise SenderError("coordinator receipt must be JSON")
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                raise SenderError("encoded receipts are forbidden")
            expected = response.headers.get("Content-Length")
            if expected is not None:
                if not expected.isascii() or not expected.isdecimal() or int(expected) > MAX_RECEIPT_BYTES:
                    raise SenderError("invalid receipt length")
                expected = int(expected)
            chunks, count = [], 0
            read = response.read if custom_opener else getattr(response, "read1", response.read)
            while True:
                watchdog.remaining()
                chunk = read(min(READ_BYTES, MAX_RECEIPT_BYTES + 1 - count))
                if not chunk:
                    break
                if type(chunk) is not bytes:
                    raise SenderError("invalid receipt bytes")
                count += len(chunk)
                if count > MAX_RECEIPT_BYTES:
                    raise SenderError("receipt exceeds byte limit")
                chunks.append(chunk)
            if expected is not None and count != expected:
                raise SenderError("truncated receipt")
            return decode_receipt(b"".join(chunks))
        finally:
            response.close()

    def send_next(self):
        """Attempt one due immutable completion, returning only secret-free state.

        Lost/invalid responses back off. If this attempt's lease has expired or a
        newer sender owns it, leave that durable lease/retry state untouched.
        """
        delivery = self.outbox.claim_next(lease_seconds=self.lease_seconds)
        if delivery is None:
            return {"state": "idle"}
        outbox_id = delivery["outboxId"]
        try:
            self._send(delivery)
        except Exception:
            try:
                due = self.outbox.failed_attempt(outbox_id, delivery["claimToken"])
            except OutboxConflict:
                return {"state": "retry_wait", "outboxId": outbox_id}
            return {"state": "retry_scheduled", "outboxId": outbox_id, "nextAttempt": due}
        return {"state": "acknowledged", "outboxId": outbox_id}
