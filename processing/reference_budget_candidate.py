"""Pure arithmetic oracle for the proposed Viewer range-budget vectors.

This is a review candidate only. It is deliberately not used by grant
validation, the driver, or any runtime admission path until a closed wire
policy is agreed across Processing and Viewer.
"""
from __future__ import annotations

MAX_SAFE_INTEGER = 2**53 - 1


def plan(policy, objects):
    """Return proposed request/byte budgets, or None when the proposal rejects.

    ``objects`` is a list of ``{"bytes": positive int, "count": positive int}``
    groups from the shared synthetic proposal fixture. Arithmetic uses integer
    ceiling division only; booleans and unsafe JS integers are rejected.
    """
    required = {
        "chunkBytes", "maxObjectBytes", "maxObjects", "maxManifestBytes",
        "maxBaseRequests", "maxRequests", "headroomNumerator",
        "headroomDenominator",
    }
    if type(policy) is not dict or set(policy) != required or type(objects) is not list:
        return None
    if any(type(value) is not int or not 1 <= value <= MAX_SAFE_INTEGER
           for value in policy.values()):
        return None
    chunk = policy["chunkBytes"]
    if policy["headroomNumerator"] < policy["headroomDenominator"]:
        return None

    object_count = total_bytes = base_requests = 0
    for group in objects:
        if (type(group) is not dict or set(group) != {"bytes", "count"}
                or type(group["bytes"]) is not int or type(group["count"]) is not int
                or not 1 <= group["bytes"] <= MAX_SAFE_INTEGER
                or not 1 <= group["count"] <= MAX_SAFE_INTEGER):
            return None
        size, count = group["bytes"], group["count"]
        if size > policy["maxObjectBytes"]:
            return None
        object_count += count
        total_bytes += size * count
        base_requests += ((size + chunk - 1) // chunk) * count
        if (object_count > policy["maxObjects"] or total_bytes > MAX_SAFE_INTEGER
                or base_requests > policy["maxBaseRequests"]):
            return None

    if not object_count or total_bytes > policy["maxManifestBytes"]:
        return None
    numerator, denominator = policy["headroomNumerator"], policy["headroomDenominator"]
    request_budget = (base_requests * numerator + denominator - 1) // denominator
    byte_budget = (total_bytes * numerator + denominator - 1) // denominator
    if (request_budget > policy["maxRequests"] or request_budget > MAX_SAFE_INTEGER
            or byte_budget > MAX_SAFE_INTEGER):
        return None
    return {
        "objectCount": object_count,
        "totalBytes": total_bytes,
        "baseRequests": base_requests,
        "requestBudget": request_budget,
        "byteBudget": byte_budget,
    }


def replay_recovery(vectors):
    """Replay the proposal's synthetic crash sequence by immutable identity.

    This state machine is only an arithmetic/contract oracle. Its in-memory
    dictionary is not evidence of durable broker accounting or worker resume.
    """
    ledgers = {}
    outcomes = []
    for event in vectors:
        if type(event) is not dict or event.get("action") not in {"send", "verify", "restart"}:
            return None
        identity, range_key = event.get("identity"), event.get("range")
        if type(identity) is not str or type(range_key) is not str:
            return None
        try:
            start, end = (int(part) for part in range_key.split("-", 1))
        except (TypeError, ValueError):
            return None
        if start < 0 or end < start or end >= MAX_SAFE_INTEGER:
            return None
        state = ledgers.setdefault(identity, {"ranges": {}, "requests": 0, "bytes": 0})
        ledger = state["ranges"]
        status = ledger.get(range_key)
        action = event["action"]
        if action == "send":
            result = ("verified" if status == "verified" else
                      ("charged" if status is None else "hold"))
            if result == "charged":
                ledger[range_key] = "charged"
                state["requests"] += 1
                state["bytes"] += end - start + 1
        elif action == "verify":
            result = "verified" if status in {"charged", "verified"} else "hold"
            if status == "charged":
                ledger[range_key] = "verified"
        else:
            if status == "charged":
                ledger[range_key] = "uncertain"
            result = "verified" if ledger.get(range_key) == "verified" else "hold"

        verified = []
        for item, item_status in ledger.items():
            if item_status == "verified":
                lo, hi = (int(part) for part in item.split("-", 1))
                verified.append((lo, hi + 1))
        offset = 0
        for lo, hi in sorted(verified):
            if lo != offset:
                break
            offset = hi
        outcomes.append({"result": result, "offset": offset,
                         "requests": state["requests"], "bytes": state["bytes"]})
    return outcomes
