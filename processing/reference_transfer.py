"""Private reference transfer primitives; no HTTP routes or runtime activation.

The caller supplies a trusted persisted private job envelope and a private local
destination directory. Grants and Authorization headers must never be logged.
"""
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import math
import os
from pathlib import Path
import secrets
import ssl
import stat
import time
from urllib.parse import urlsplit
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, HTTPRedirectHandler, Request, build_opener

from . import reference_protocol as p

OBJECT_PREFIX = "/api/processing/reference/v1/objects/"
CHUNK_BYTES = 1024 * 1024


class TransferError(ValueError):
    """Bounded, secret-free transfer failure."""


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


def _sync_directory(parent):
    if os.name == "nt":
        return
    directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


class BrokerClient:
    def __init__(self, origin, *, ssl_context=None, opener=None, clock=time.time, timeout=30):
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
        self.origin = origin.rstrip("/")
        self.clock, self.timeout = clock, timeout
        self._open = opener or build_opener(_RefuseRedirects(), HTTPSHandler(context=context)).open

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
        published_identity = None
        try:
            descriptor = os.open(part, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
            owned_part = True
            request = Request(self.origin + OBJECT_PREFIX + obj.id,
                headers={"Authorization": "Bearer " + grant.token, "Accept-Encoding": "identity"}, method="GET")
            try:
                response = self._open(request, timeout=self.timeout)
            except HTTPError as error:
                error.close()
                raise TransferError("broker request rejected") from None
            try:
                if response.status != 200 or response.geturl() != request.full_url:
                    raise TransferError("unexpected broker response")
                length = response.headers.get("Content-Length")
                if length is not None and (not length.isascii() or not length.isdecimal() or int(length) != obj.byte_size):
                    raise TransferError("broker object length mismatch")
                encoding = response.headers.get("Content-Encoding", "identity")
                if encoding.lower() != "identity":
                    raise TransferError("encoded broker objects are forbidden")
                digest, count = hashlib.sha256(), 0
                with os.fdopen(descriptor, "wb") as output:
                    descriptor = None
                    while True:
                        if grant.expires <= _now(self.clock):
                            raise TransferError("transfer grant expired")
                        chunk = response.read(min(CHUNK_BYTES, obj.byte_size - count + 1))
                        if not chunk:
                            break
                        if not isinstance(chunk, bytes) or count + len(chunk) > obj.byte_size:
                            raise TransferError("broker object exceeds declared size")
                        count += len(chunk)
                        digest.update(chunk)
                        output.write(chunk)
                    if count != obj.byte_size or digest.hexdigest() != obj.sha256:
                        raise TransferError("broker object integrity mismatch")
                    output.flush()
                    os.fsync(output.fileno())
            finally:
                response.close()
            current = parent.stat()
            if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino) or parent.resolve() != parent:
                raise TransferError("destination directory changed")
            if grant.expires <= _now(self.clock):
                raise TransferError("transfer grant expired")
            # Same-directory hard link atomically fails if target exists, including
            # a dangling symlink. os.replace would overwrite a racing consumer.
            part_info = part.stat(follow_symlinks=False)
            os.link(part, target, follow_symlinks=False)
            published_identity = (part_info.st_dev, part_info.st_ino)
            part.unlink()
            _sync_directory(parent)
            return target
        except Exception:
            # If any step after publication fails (including directory fsync),
            # roll back only the exact inode this call linked. Never remove a
            # replacement installed by another process after publication.
            if published_identity is not None:
                try:
                    current = target.stat(follow_symlinks=False)
                    if (current.st_dev, current.st_ino) == published_identity:
                        target.unlink()
                except OSError:
                    pass
            # Do not expose exception strings from HTTP stacks or injected openers:
            # they can embed request headers and credentials.
            raise TransferError("reference object transfer failed") from None
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if owned_part:
                try:
                    part.unlink(missing_ok=True)
                except OSError:
                    pass
