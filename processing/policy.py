"""Pure small-node-first selection, shared by probes and dispatch preflight.

An otherwise small job waits for local while it is busy, unavailable or draining.
Only an explicit ``allow_main_fallback=True`` permits it to consume main. Jobs
above the configured or freshly advertised local photo ceiling require main and never overflow to local.
Input-byte admission and disk capacity remain separate concerns. The input-byte
arguments remain for caller compatibility and do not decide node preference.
This helper reserves nothing: callers must atomically gate and recheck admission
before dispatch. Missing queue/state fields retain legacy fixture compatibility;
when supplied, queue counts must be zero and maintenance state must be active.
"""


def plan(photo_count: int, input_bytes: int, local: dict, main: dict,
         local_max_photos: int, local_max_input_bytes: int, *,
         allow_main_fallback: bool = False) -> dict:
    def accepts(node: dict) -> bool:
        if not isinstance(node, dict) or node.get("ready") is not True:
            return False
        queued = node.get("taskQueueCount", 0)
        if type(queued) is not int or queued != 0:
            return False
        if any(node.get(key, "active") != "active" for key in ("state", "maintenanceState")):
            return False
        limit = node.get("maxImages")
        return limit is None or (type(limit) is int and limit > 0 and photo_count <= limit)

    # NodeODM advertises a hard maximum for new tasks. A healthy probe may
    # report a stricter ceiling than configuration; a failed/untrusted probe
    # must never turn a small job into opportunistic main-node fallback.
    queue_count = local.get("taskQueueCount") if isinstance(local, dict) else None
    credible = isinstance(local, dict) and local.get("ready") is True and type(queue_count) is int and queue_count >= 0
    limit = local.get("maxImages") if credible else None
    ceiling = min(local_max_photos, limit) if type(limit) is int and limit > 0 else local_max_photos
    local_size_ok = photo_count <= ceiling
    if local_size_ok:
        if accepts(local):
            return {"decision": "local", "reason": "local_within_limits"}
        if allow_main_fallback is True and accepts(main):
            return {"decision": "main", "reason": "explicit_local_fallback"}
        return {"decision": "wait", "reason": "local_unavailable"}
    if accepts(main):
        return {"decision": "main", "reason": "exceeds_local_limits"}
    return {"decision": "wait", "reason": "main_unavailable"}
