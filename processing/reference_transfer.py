"""Private reference transfer primitives; no HTTP routes or runtime activation.

The caller supplies a trusted persisted private job envelope and a private local
destination directory. Grants and Authorization headers must never be logged.
"""
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import http.client
import math
import os
from pathlib import Path
import secrets
import socket
import ssl
import stat
import time
import threading
from urllib.parse import urlsplit
from urllib.error import HTTPError, URLError
from urllib.request import HTTPSHandler, HTTPRedirectHandler, Request, build_opener

from . import reference_protocol as p

OBJECT_PREFIX = "/api/processing/reference/v1/objects/"
CHUNK_BYTES = 1024 * 1024
# Matches the current broker's default response ceiling; lower configuration
# must be coordinated with that broker. This is not a job request budget.
MAX_RANGE_BYTES = 16 * 1024 * 1024
MAX_RANGES_PER_OBJECT = 10_000


class TransferError(ValueError):
    """Bounded, secret-free transfer failure."""


class TransferResponseUnavailable(TransferError):
    """No response received; broker budget and local cleanup remain uncertain."""


def _now(clock):
    value = clock()
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise TransferError("invalid transfer clock")
    return value


@dataclass(frozen=True)
class TransferObject:
    id: str
    byte_size: int
    sha256: str


@dataclass(frozen=True)
class ValidatedGrant:
    job_id: str
    reservation_id: str
    generation: int
    environment: str
    expires: float
    objects: tuple
    token: str = field(repr=False)


def validate_grant(grant, *, job, clock=time.time):
    """Bind an ephemeral grant to ReferenceStore.get(private=True) metadata."""
    p._closed(grant, ("reservationId", "generation", "expiresAt", "manifestSha256",
        "byteBudget", "nonce", "jobId", "environment", "objectIds", "token"), "transfer grant")
    request = job["transferRequest"]
    if not isinstance(request, dict):
        raise TransferError("job has no persisted transfer reservation")
    for key in ("reservationId", "nonce", "jobId"):
        p._slug(grant[key], key)
    p._integer(grant["generation"], "generation")
    p._integer(grant["byteBudget"], "byteBudget")
    p._hash(grant["manifestSha256"], "manifestSha256")
    for key in ("reservationId", "generation", "expiresAt", "manifestSha256", "byteBudget", "nonce"):
        if grant[key] != request[key]:
            raise TransferError("grant reservation identity mismatch")
    if (grant["jobId"], grant["generation"], grant["environment"], grant["manifestSha256"]) != (
            job["jobId"], job["generation"], job["environment"], job["manifestSha256"]):
        raise TransferError("grant job identity mismatch")
    p._environment(grant["environment"])
    manifest = job["submission"]["manifest"]
    if (manifest["sha256"] != grant["manifestSha256"]
            or p.manifest_sha256(manifest) != manifest["sha256"]
            or not manifest['totalBytes'] <= grant['byteBudget'] <=
                min(p.MAX_SAFE_INTEGER, 3 * manifest['totalBytes'])):
        raise TransferError("persisted input manifest mismatch")
    objects = []
    for obj in manifest["objects"]:
        p._slug(obj["id"], "object ID")
        p._integer(obj["byteSize"], "object bytes")
        p._hash(obj["sha256"], "object hash")
        objects.append(TransferObject(obj["id"], obj["byteSize"], obj["sha256"]))
    ids = grant["objectIds"]
    if (type(ids) is not list or any(type(item) is not str for item in ids)
            or len(set(ids)) != len(ids) or set(ids) != {obj.id for obj in objects}
            or len(objects) != len({obj.id for obj in objects})
            or sum(obj.byte_size for obj in objects) != manifest['totalBytes']):
        raise TransferError("grant object closure mismatch")
    p._string(grant["expiresAt"], "grant expiry", 40)
    try:
        parsed = datetime.fromisoformat(grant["expiresAt"].replace("Z", "+00:00"))
        if parsed.utcoffset() is None:
            raise ValueError()
        expiry = parsed.timestamp()
    except (ValueError, OverflowError, OSError):
        raise TransferError("invalid grant expiry") from None
    if expiry <= _now(clock):
        raise TransferError("transfer grant expired")
    token = grant["token"]
    if (type(token) is not str or not 1 <= len(token) <= 4096
            or any(ord(c) < 33 or ord(c) > 126 for c in token)):
        raise TransferError("invalid transfer token")
    return ValidatedGrant(grant["jobId"], grant["reservationId"], grant["generation"],
                          grant["environment"], expiry, tuple(objects), token)


class _RefuseRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise TransferError("broker redirects are forbidden")


class _TransferWatchdog:
    """Interrupt active sockets at one monotonic object-transfer deadline.

    DNS resolution and filesystem syscalls cannot be interrupted by this
    watchdog; an expired deadline is checked before any result is published.
    """
    def __init__(self, seconds):
        self.deadline = time.monotonic() + seconds
        self._lock = threading.Lock()
        self._connection = None
        self._socket = None
        self._expired = False
        self._timer = threading.Timer(seconds, self._expire)
        self._timer.daemon = True
        self._timer.start()

    def _expire(self):
        with self._lock:
            self._expired = True
            sockets = (self._socket, getattr(self._connection, 'sock', None))
            for sock in sockets:
                if sock is not None:
                    try: sock.shutdown(socket.SHUT_RDWR)
                    except OSError: pass

    def connection(self, connection):
        with self._lock:
            if self._expired: raise TransferResponseUnavailable('transfer deadline expired')
            self._connection = connection
            self._socket = None

    def response(self, response):
        sock = getattr(getattr(getattr(response, 'fp', None), 'raw', None), '_sock', None)
        with self._lock:
            if self._expired: raise TransferResponseUnavailable('transfer deadline expired')
            self._socket = sock

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if self._expired or remaining <= 0: raise TransferResponseUnavailable('transfer deadline expired')
        return remaining

    def expired(self):
        return self._expired or time.monotonic() >= self.deadline

    def close(self):
        self._timer.cancel()
        self._timer.join()
        with self._lock:
            self._connection = self._socket = None


class _DeadlineHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host, *, watchdog, **kwargs):
        super().__init__(host, **kwargs)
        self._watchdog = watchdog
        watchdog.connection(self)

    def connect(self):
        # HTTPConnection assigns the raw socket before proxy tunnelling. The
        # watchdog can interrupt the tunnel and TLS handshake as well as HTTP.
        http.client.HTTPConnection.connect(self)
        self._watchdog.connection(self)  # DNS/connect may have exceeded the budget.
        server_hostname = self._tunnel_host or self.host
        self.sock = self._context.wrap_socket(self.sock, server_hostname=server_hostname,
                                              do_handshake_on_connect=False)
        self._watchdog.connection(self)
        self.sock.settimeout(min(self.timeout, self._watchdog.remaining()))
        self.sock.do_handshake()


class _DeadlineHTTPSHandler(HTTPSHandler):
    def __init__(self, watchdog, *, context):
        super().__init__(context=context)
        self._watchdog = watchdog

    def https_open(self, request):
        watchdog = self._watchdog
        def connection(host, **kwargs):
            return _DeadlineHTTPSConnection(host, watchdog=watchdog, **kwargs)
        response = self.do_open(connection, request, context=self._context)
        try:
            watchdog.response(response)
            return response
        except BaseException:
            response.close()
            raise


class BrokerClient:
    def __init__(self, origin, *, ssl_context=None, opener=None, clock=time.time, timeout=30,
                 max_response_bytes=MAX_RANGE_BYTES, enable_ranges=False, total_timeout=3600):
        parsed = urlsplit(origin)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.path not in ("", "/")
                or parsed.query or parsed.fragment or "\\" in origin
                or any(ord(c) <= 32 for c in origin)):
            raise ValueError("broker must be a configured HTTPS origin")
        try:
            parsed.port
        except ValueError:
            raise ValueError("invalid broker port") from None
        if ssl_context is not None and not isinstance(ssl_context, ssl.SSLContext):
            raise ValueError("invalid TLS context")
        context = ssl_context or ssl.create_default_context()
        if not context.check_hostname or context.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError("broker TLS verification is required")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 300:
            raise ValueError("invalid transfer timeout")
        if type(max_response_bytes) is not int or not 1 <= max_response_bytes <= MAX_RANGE_BYTES:
            raise ValueError('bounded broker response size required')
        if type(enable_ranges) is not bool:
            raise ValueError('explicit range setting required')
        if type(total_timeout) not in (int, float) or not math.isfinite(total_timeout) or not 0 < total_timeout <= 86400:
            raise ValueError('invalid total transfer timeout')
        self.origin = origin.rstrip("/")
        self.clock, self.timeout, self.total_timeout = clock, timeout, total_timeout
        self.max_response_bytes = max_response_bytes
        self.enable_ranges = enable_ranges
        self._custom_opener = opener is not None
        self._open = opener or build_opener(_RefuseRedirects(), HTTPSHandler(context=context)).open
        self._ssl_context = context

    def download_object(self, grant, object_id, destination):
        """Publish one verified object without replacing any existing filesystem entry.

        Injection of an opener is a trusted testing boundary. Production always
        uses the redirect-refusing HTTPS opener constructed above.
        """
        if not isinstance(grant, ValidatedGrant):
            raise TransferError("validated transfer grant required")
        obj = next((obj for obj in grant.objects if obj.id == object_id), None)
        if obj is None:
            raise TransferError("object is outside grant")
        if grant.expires <= _now(self.clock):
            raise TransferError("transfer grant expired")
        if self.enable_ranges and (obj.byte_size + self.max_response_bytes - 1) // self.max_response_bytes > MAX_RANGES_PER_OBJECT:
            raise TransferError('object range count exceeds bound')
        target = Path(destination).absolute()
        p._basename(target.name)
        parent = target.parent
        if (not parent.is_dir() or any(path.is_symlink() for path in (parent, *parent.parents))
                or parent.resolve() != parent):
            raise TransferError("private destination directory required")
        info = parent.stat()
        if os.name != "nt" and (info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077):
            raise TransferError("private destination directory required")
        if os.path.lexists(target):
            raise TransferError("destination already exists")
        part = parent / (".reference-" + secrets.token_hex(16) + ".part")
        descriptor = None
        owned_part = False
        watchdog = _TransferWatchdog(self.total_timeout)
        try:
            descriptor = os.open(part, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
            owned_part = True
            digest, count = hashlib.sha256(), 0
            ranged = self.enable_ranges and obj.byte_size > self.max_response_bytes
            with os.fdopen(descriptor, "wb") as output:
                descriptor = None
                while count < obj.byte_size or (obj.byte_size == 0 and count == 0):
                    watchdog.remaining()
                    if grant.expires <= _now(self.clock):
                        raise TransferError("transfer grant expired")
                    start = count
                    length_expected = min(self.max_response_bytes, obj.byte_size - start) if ranged else obj.byte_size
                    end = start + length_expected - 1
                    headers = {"Authorization": "Bearer " + grant.token, "Accept-Encoding": "identity"}
                    if ranged: headers['Range'] = f'bytes={start}-{end}'
                    request = Request(self.origin + OBJECT_PREFIX + obj.id, headers=headers, method="GET")
                    try:
                        open_request = self._open if self._custom_opener else build_opener(_RefuseRedirects(), _DeadlineHTTPSHandler(watchdog, context=self._ssl_context)).open
                        response = open_request(request, timeout=min(self.timeout, watchdog.remaining()))
                    except HTTPError as error:
                        error.close()
                        raise TransferError("broker request rejected") from None
                    except ssl.SSLError as error:
                        if isinstance(error, ssl.SSLCertVerificationError):
                            raise TransferError("broker TLS validation failed") from None
                        if watchdog.expired(): raise TransferResponseUnavailable("transfer deadline expired") from None
                        raise TransferError("broker TLS validation failed") from None
                    except URLError as error:
                        if isinstance(error.reason, ssl.SSLCertVerificationError):
                            raise TransferError("broker TLS validation failed") from None
                        if watchdog.expired(): raise TransferResponseUnavailable("transfer deadline expired") from None
                        if isinstance(error.reason, ssl.SSLError):
                            raise TransferError("broker TLS validation failed") from None
                        raise TransferResponseUnavailable("broker response unavailable") from None
                    except OSError:
                        raise TransferResponseUnavailable("broker response unavailable") from None
                    try:
                        watchdog.response(response)
                        watchdog.remaining()
                        if response.status != (206 if ranged else 200) or response.geturl() != request.full_url:
                            raise TransferError("unexpected broker response")
                        if ranged and response.headers.get('Content-Range') != f'bytes {start}-{end}/{obj.byte_size}':
                            raise TransferError('broker content range mismatch')
                        length = response.headers.get("Content-Length")
                        if (ranged and length is None) or (length is not None and (not length.isascii() or not length.isdecimal() or int(length) != length_expected)):
                            raise TransferError("broker object length mismatch")
                        encoding = response.headers.get("Content-Encoding", "identity")
                        if encoding.lower() != "identity":
                            raise TransferError("encoded broker objects are forbidden")
                        part_count = 0
                        read = response.read if self._custom_opener else getattr(response, 'read1', response.read)
                        while True:
                            remaining = watchdog.remaining()
                            if grant.expires <= _now(self.clock):
                                raise TransferError("transfer grant expired")
                            sock = getattr(getattr(getattr(response, 'fp', None), 'raw', None), '_sock', None)
                            if sock is not None: sock.settimeout(min(self.timeout, remaining))
                            try:
                                chunk = read(min(CHUNK_BYTES, length_expected - part_count + 1))
                            except ssl.SSLCertVerificationError:
                                raise TransferError("broker TLS validation failed") from None
                            except (OSError, http.client.HTTPException):
                                raise TransferResponseUnavailable("broker response unavailable") from None
                            if chunk and (not isinstance(chunk, bytes) or part_count + len(chunk) > length_expected):
                                raise TransferError("broker object exceeds declared size")
                            watchdog.remaining()
                            if not chunk:
                                break
                            part_count += len(chunk)
                            count += len(chunk)
                            digest.update(chunk)
                            output.write(chunk)
                        if part_count != length_expected:
                            raise TransferError("broker object length mismatch")
                    finally:
                        response.close()
                    if obj.byte_size == 0: break
                if count != obj.byte_size or digest.hexdigest() != obj.sha256:
                    raise TransferError("broker object integrity mismatch")
                output.flush()
                os.fsync(output.fileno())
            current = parent.stat()
            if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino) or parent.resolve() != parent:
                raise TransferError("destination directory changed")
            watchdog.remaining()
            if grant.expires <= _now(self.clock):
                raise TransferError("transfer grant expired")
            # Same-directory hard link atomically fails if target exists, including
            # a dangling symlink. os.replace would overwrite a racing consumer.
            os.link(part, target, follow_symlinks=False)
            part.unlink()
            if os.name != "nt":
                directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            return target
        except TransferResponseUnavailable:
            raise
        except Exception:
            # Do not expose exception strings from HTTP stacks or injected openers:
            # they can embed request headers and credentials.
            raise TransferError("reference object transfer failed") from None
        finally:
            watchdog.close()
            if descriptor is not None:
                os.close(descriptor)
            if owned_part:
                try:
                    part.unlink(missing_ok=True)
                except OSError:
                    pass
