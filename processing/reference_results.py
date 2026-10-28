"""Pinned, verified local result streams; no HTTP auth or cleanup execution.

Only a trusted authenticated adapter may construct ResultAuthorization after an
explicit sharing decision. Viewer acceptance is deliberately not a read gate:
the Viewer must first download a result before it can acknowledge storage.
"""
from dataclasses import dataclass
import hashlib
import json
import os
import re
import secrets
import stat

from . import reference_protocol as protocol

CHUNK_BYTES = 64 * 1024


class ResultUnavailable(ValueError):
    status = 409
    code = "result_unavailable"

    def __init__(self):
        super().__init__(self.code)


class ResultDenied(ResultUnavailable):
    status = 403
    code = "result_not_authorized"


class ResultRangeError(ResultUnavailable):
    status = 416
    code = "range_not_satisfiable"

    def __init__(self, size):
        super().__init__()
        self.headers = {"Content-Range": "bytes */" + str(size)}


@dataclass(frozen=True)
class ResultAuthorization:
    node_id: str
    worker_id: str
    job_id: str
    generation: int
    environment: str
    result_manifest_sha256: str
    object_ids: tuple
    share_approved: bool = False

    def __post_init__(self):
        for value in (self.node_id, self.worker_id, self.job_id):
            protocol._slug(value, "result authorization identity")
        protocol._integer(self.generation, "result authorization generation")
        protocol._environment(self.environment)
        protocol._hash(self.result_manifest_sha256, "result authorization manifest")
        if (type(self.object_ids) is not tuple or len(self.object_ids) > protocol.MAX_OBJECTS
                or type(self.share_approved) is not bool):
            raise ValueError("invalid result authorization scope")
        for value in self.object_ids:
            protocol._slug(value, "authorized result object")
        if len(set(self.object_ids)) != len(self.object_ids):
            raise ValueError("duplicate authorized result object")


def _fingerprint(value):
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns,
            value.st_ctime_ns, value.st_nlink)


def _range(header, size):
    if header is None:
        return 0, size, 200
    match = re.fullmatch(r"bytes=([0-9]{0,16})-([0-9]{0,16})", header) if type(header) is str else None
    if not match or not size or not any(match.groups()):
        raise ResultRangeError(size)
    first, last = match.groups()
    if not first:
        suffix = int(last)
        if not suffix:
            raise ResultRangeError(size)
        start, end = max(0, size - suffix), size - 1
    else:
        start, end = int(first), int(last) if last else size - 1
        if start >= size or end < start:
            raise ResultRangeError(size)
        end = min(end, size - 1)
    return start, end - start + 1, 206


class ResultStream:
    """Path-free response stream. Always close it, including socket aborts.

    Each read returns at most CHUNK_BYTES, even read(-1). The last bytes remain
    pinned until EOF is consumed or close() is called by the response owner.
    A process crash leaves a conservative durable pin for reconciliation.
    """
    def __init__(self, descriptor, *, length, fingerprint, headers, status, release):
        self._descriptor = descriptor
        self._remaining = length
        self._fingerprint = fingerprint
        self._release = release
        self.headers, self.status = dict(headers), status

    @property
    def closed(self):
        return self._descriptor is None

    def close(self):
        if self._descriptor is None:
            return
        descriptor, self._descriptor = self._descriptor, None
        try:
            os.close(descriptor)
        except OSError:
            # Unknown descriptor outcome must never release a live consumer pin.
            raise ResultUnavailable() from None
        try:
            self._release()
        except Exception:
            # A failed release retains the durable pin, safely blocking cleanup.
            raise ResultUnavailable() from None

    def read(self, size=-1):
        if self.closed:
            return b""
        if type(size) is not int or size < -1:
            raise ValueError("invalid stream read size")
        if size == 0:
            return b""
        if not self._remaining:
            self.close()
            return b""
        try:
            if _fingerprint(os.fstat(self._descriptor)) != self._fingerprint:
                raise ResultUnavailable()
            chunk = os.read(self._descriptor, min(self._remaining, CHUNK_BYTES,
                            CHUNK_BYTES if size == -1 else size))
            if not chunk or _fingerprint(os.fstat(self._descriptor)) != self._fingerprint:
                raise ResultUnavailable()
            self._remaining -= len(chunk)
            return chunk
        except Exception:
            self.close()
            raise ResultUnavailable() from None

    def chunks(self):
        try:
            while chunk := self.read(CHUNK_BYTES):
                yield chunk
        finally:
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class ReferenceResults:
    def __init__(self, worker):
        self.worker = worker
        self.ledger, self.outbox = worker.ledger, worker.outbox

    def _load(self, job_id):
        protocol._slug(job_id, "job ID")
        row = self.worker._row(job_id)
        if not row["result"] or not row["outbox_id"]:
            raise ResultUnavailable()
        identity = json.loads(row["identity"])
        job, reservation = identity["job"], identity["reservation"]
        if job["jobId"] != job_id or job["nodeId"] != self.worker.node_id:
            raise ResultUnavailable()
        result = protocol.validate_result_manifest(json.loads(row["result"]), job_id=job_id,
            generation=job["generation"], environment=job["environment"], pipeline_digest=job["pipelineDigest"])
        completion = self.outbox.get(row["outbox_id"])
        payload, local = completion["payload"], completion["local_metadata"]
        keys = ("jobId", "generation", "environment", "inputManifestSha256", "pipelineDigest", "resultManifestSha256")
        if (protocol.digest(payload) != completion["digest"]
                or protocol.digest({key: payload[key] for key in keys}) != row["outbox_id"]
                or payload["terminalState"] != "succeeded"
                or protocol.canonical_json(payload["resultManifest"]) != protocol.canonical_json(result)
                or tuple(payload[key] for key in keys) != (job_id, job["generation"], job["environment"],
                    job["manifestSha256"], job["pipelineDigest"], result["resultManifestSha256"])
                or local["ownedPrefix"] != "jobs/" + reservation
                or set(local["localObjects"]) != {obj["id"] for obj in result["objects"]}):
            raise ResultUnavailable()
        with self.ledger._transaction() as db:
            registered = db.execute("SELECT identity FROM results WHERE reservation=?", (reservation,)).fetchone()
            if registered is None:
                raise ResultUnavailable()
            registered = json.loads(registered[0])
            if tuple(registered[key] for key in ("job_id", "generation", "manifest_sha256", "pipeline_sha256", "result_manifest_sha256")) != (
                    job_id, job["generation"], job["manifestSha256"], job["pipelineDigest"], result["resultManifestSha256"]):
                raise ResultUnavailable()
        return job, reservation, result, local

    def metadata(self, job_id):
        """Private adapter input for sharing policy; contains no paths/tokens."""
        try:
            job, _reservation, result, _local = self._load(job_id)
            return {"jobId": job_id, "generation": job["generation"], "environment": job["environment"],
                    "resultManifestSha256": result["resultManifestSha256"],
                    "objects": [dict(obj) for obj in result["objects"]]}
        except Exception:
            raise ResultUnavailable() from None

    def open(self, job_id, *, generation, object_id, result_manifest_sha256,
             authorization, object_sha256=None, range_header=None):
        descriptor, pin, reservation = None, None, None
        try:
            job, reservation, result, local = self._load(job_id)
            expected = (self.worker.node_id, self.worker.worker_id, job_id, job["generation"],
                        job["environment"], result["resultManifestSha256"])
            if (type(authorization) is not ResultAuthorization or authorization.share_approved is not True
                    or (authorization.node_id, authorization.worker_id, authorization.job_id, authorization.generation,
                        authorization.environment, authorization.result_manifest_sha256) != expected
                    or type(authorization.object_ids) is not tuple or object_id not in authorization.object_ids):
                raise ResultDenied()
            if type(generation) is not int or generation != job["generation"] or result_manifest_sha256 != result["resultManifestSha256"]:
                raise ResultUnavailable()
            obj = next((obj for obj in result["objects"] if obj["id"] == object_id), None)
            if obj is None or (object_sha256 is not None and object_sha256 != obj["sha256"]):
                raise ResultUnavailable()
            start, length, status = _range(range_header, obj["byteSize"])
            relative = local["localObjects"][object_id]
            self.ledger._owned_path(reservation, relative)
            with self.ledger._transaction() as db:
                stored = db.execute("SELECT * FROM objects WHERE reservation=? AND id=?", (reservation, object_id)).fetchone()
                if stored is None or (stored["path"], stored["sha"], stored["bytes"]) != (relative, obj["sha256"], obj["byteSize"]):
                    raise ResultUnavailable()
            pin = "result-read-" + secrets.token_hex(24)
            self.ledger.set_pin(reservation, object_id, pin, kind="read", state="active")
            # Recheck after pin admission; cleanup can no longer acquire a fence.
            self.ledger._owned_path(reservation, relative)
            path = self.ledger.root / relative
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
            observed, current = os.fstat(descriptor), path.lstat()
            if (not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1
                    or observed.st_size != obj["byteSize"]
                    or (observed.st_dev, observed.st_ino) != (current.st_dev, current.st_ino)):
                raise ResultUnavailable()
            sha = hashlib.sha256()
            count = 0
            while chunk := os.read(descriptor, min(CHUNK_BYTES, obj["byteSize"] - count + 1)):
                count += len(chunk)
                if count > obj["byteSize"]:
                    raise ResultUnavailable()
                sha.update(chunk)
            if (count != obj["byteSize"] or sha.hexdigest() != obj["sha256"]
                    or _fingerprint(os.fstat(descriptor)) != _fingerprint(observed)):
                raise ResultUnavailable()
            os.lseek(descriptor, start, os.SEEK_SET)
            headers = {"Content-Type": "application/zip" if obj["role"] == "odm_archive" else "application/octet-stream",
                       "Content-Length": str(length), "Accept-Ranges": "bytes", "Cache-Control": "no-store",
                       "X-Result-Sha256": obj["sha256"], "X-Result-Manifest-Sha256": result["resultManifestSha256"]}
            if status == 206:
                headers["Content-Range"] = "bytes " + str(start) + "-" + str(start + length - 1) + "/" + str(obj["byteSize"])
            release = lambda reservation=reservation, object_id=object_id, pin=pin: self.ledger.set_pin(
                reservation, object_id, pin, kind="read", state="released")
            stream = ResultStream(descriptor, length=length, fingerprint=_fingerprint(observed),
                                  headers=headers, status=status, release=release)
            descriptor, pin = None, None
            return stream
        except Exception as error:
            closed = descriptor is None
            if descriptor is not None:
                try:
                    os.close(descriptor)
                    closed = True
                except OSError:
                    pass
            if pin is not None and closed:
                try:
                    self.ledger.set_pin(reservation, object_id, pin, kind="read", state="released")
                except Exception:
                    pass
            if isinstance(error, (ResultDenied, ResultRangeError)):
                raise
            raise ResultUnavailable() from None
