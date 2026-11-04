"""Inactive node-local capacity API candidate backed by ScratchLedger.

This module is deliberately not mounted by app.py. A future node agent must
provide authenticated transport and bind the configured identities to the
actual host/engine before mounting it.
"""
import hashlib
import hmac
import json
import re
import shutil

from .scratch_policy import PolicyConflict, ScratchLedger

MAX_BODY = 16 * 1024
MAX_BYTES = 2**53 - 1
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_SHA = re.compile(r"[a-f0-9]{64}\Z")


def _int(value, name, minimum=0):
    if type(value) is not int or not minimum <= value <= MAX_BYTES:
        raise ValueError(f"invalid {name}")
    return value


def _id(value, name):
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise ValueError(f"invalid {name}")
    return value


def _sha(value, name):
    if not isinstance(value, str) or not _SHA.fullmatch(value):
        raise ValueError(f"invalid {name}")
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


class NodeCapacityAPI:
    """Bounded, token-authenticated API logic; caller supplies transport."""

    def __init__(self, *, ledger: ScratchLedger, node_id: str,
                 physical_host_id: str, bearer_token: str, receipt_key: bytes):
        self.ledger = ledger
        self.node_id = _id(node_id, "node id")
        self.physical_host_id = _id(physical_host_id, "physical host id")
        if not isinstance(bearer_token, str) or len(bearer_token) < 32 or len(bearer_token) > 4096:
            raise ValueError("strong bearer token required")
        if not isinstance(receipt_key, bytes) or len(receipt_key) < 32:
            raise ValueError("strong receipt key required")
        self._token = bearer_token.encode("ascii", "strict")
        self._receipt_key = receipt_key

    def _authenticate(self, authorization):
        expected = b"Bearer " + self._token
        supplied = authorization.encode("utf-8") if isinstance(authorization, str) else b""
        if not hmac.compare_digest(supplied, expected):
            raise PermissionError("unauthorized")

    def _receipt(self, payload):
        body = dict(schemaVersion=1, protocol="ltds-node-scratch-reservation-v1",
                    nodeId=self.node_id, physicalHostId=self.physical_host_id, **payload)
        body["receiptSha256"] = hashlib.sha256(canonical(body).encode()).hexdigest()
        body["signature"] = hmac.new(self._receipt_key, canonical(body).encode(), hashlib.sha256).hexdigest()
        return body

    def verify_receipt(self, receipt, *, expected_job=None, expected_generation=None,
                       expected_manifest=None):
        if type(receipt) is not dict or set(receipt) != {
                "schemaVersion", "protocol", "nodeId", "physicalHostId", "reservationId",
                "jobId", "generation", "environment", "manifestSha256", "inputBytes",
                "outputBudgetBytes", "state", "receiptSha256", "signature"}:
            return False
        unsigned = dict(receipt)
        signature = unsigned.pop("signature")
        digest = unsigned.pop("receiptSha256")
        actual_digest = hashlib.sha256(canonical(unsigned).encode()).hexdigest()
        signed = dict(unsigned, receiptSha256=digest)
        actual_signature = hmac.new(self._receipt_key, canonical(signed).encode(), hashlib.sha256).hexdigest()
        if not (isinstance(digest, str) and hmac.compare_digest(digest, actual_digest)
                and isinstance(signature, str) and hmac.compare_digest(signature, actual_signature)):
            return False
        if receipt["nodeId"] != self.node_id or receipt["physicalHostId"] != self.physical_host_id:
            return False
        if expected_job is not None and receipt["jobId"] != expected_job:return False
        if expected_generation is not None and receipt["generation"] != expected_generation:return False
        if expected_manifest is not None and receipt["manifestSha256"] != expected_manifest:return False
        return True

    def handle(self, method, path, authorization, body=b""):
        """Return (status, bounded JSON object); no socket/server is installed."""
        self._authenticate(authorization)
        if not isinstance(body, bytes) or len(body) > MAX_BODY:
            return 413, {"error": "body_too_large"}
        if method == "GET" and path == "/capacity/v1/probe":
            usage = shutil.disk_usage(self.ledger.root)
            total, free = usage.total, usage.free
            return 200, dict(schemaVersion=1, nodeId=self.node_id,
                             physicalHostId=self.physical_host_id,
                             availableScratchBytes=free, totalScratchBytes=total,
                             safetyFloorBytes=self.ledger.floor)
        if method != "POST" or path not in (
                "/capacity/v1/reserve", "/capacity/v1/allocated", "/capacity/v1/engine-state"):
            return 404, {"error": "not_found"}
        try:
            value = json.loads(body.decode("utf-8"), object_pairs_hook=_object)
            if type(value) is not dict:
                raise ValueError("object required")
            if path.endswith("/reserve"):
                keys = {"nodeId", "physicalHostId", "jobId", "generation", "reservationId",
                        "environment", "manifestSha256", "inputBytes", "outputBudgetBytes", "ttlSeconds"}
                if set(value) != keys:raise ValueError("exact reservation request required")
                self._identity(value)
                row = self.ledger.reserve(job_id=value["jobId"], generation=value["generation"],
                    reservation_id=value["reservationId"], manifest_sha256=value["manifestSha256"],
                    input_bytes=value["inputBytes"], output_budget=value["outputBudgetBytes"],
                    environment=value["environment"], ttl_seconds=value["ttlSeconds"], worker_ready=True)
                receipt = self._receipt(dict(reservationId=value["reservationId"], jobId=value["jobId"],
                    generation=value["generation"], environment=value["environment"],
                    manifestSha256=value["manifestSha256"], inputBytes=value["inputBytes"],
                    outputBudgetBytes=value["outputBudgetBytes"], state=row["state"]))
                return 200, receipt
            if path.endswith("/allocated"):
                if set(value) != {"nodeId", "physicalHostId", "reservationId", "allocatedBytes"}:
                    raise ValueError("exact allocation request required")
                self._node(value)
                self.ledger.record_allocated(_id(value["reservationId"], "reservation id"),
                                             _int(value["allocatedBytes"], "allocated bytes"))
                return 200, {"schemaVersion": 1, "recorded": True}
            if set(value) != {"nodeId", "physicalHostId", "reservationId", "state", "terminalAttested"}:
                raise ValueError("exact engine-state request required")
            self._node(value)
            if type(value["terminalAttested"]) is not bool:raise ValueError("boolean attestation required")
            self.ledger.set_engine_state(_id(value["reservationId"], "reservation id"), value["state"],
                                         terminal_attested=value["terminalAttested"])
            return 200, {"schemaVersion": 1, "state": value["state"]}
        except PermissionError:
            raise
        except (ValueError, TypeError, KeyError, PolicyConflict, OSError, json.JSONDecodeError):
            return 409, {"error": "capacity_request_rejected"}

    def _node(self, value):
        if value.get("nodeId") != self.node_id or value.get("physicalHostId") != self.physical_host_id:
            raise ValueError("node identity mismatch")

    def _identity(self, value):
        self._node(value)
        _id(value["jobId"], "job id");_id(value["reservationId"], "reservation id")
        _int(value["generation"], "generation", 1)
        _sha(value["manifestSha256"], "manifest digest")
        _int(value["inputBytes"], "input bytes");_int(value["outputBudgetBytes"], "output budget", 1)
        _int(value["ttlSeconds"], "TTL", 1)
        if value["environment"] not in ("production", "staging"):
            raise ValueError("invalid environment")
