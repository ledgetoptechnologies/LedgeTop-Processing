"""Pure small-node-first selection, shared by probes and dispatch preflight.

An otherwise small job waits for local while it is busy, unavailable or draining.
Only an explicit ``allow_main_fallback=True`` permits it to consume main. Jobs
above the local photo ceiling always require main and never overflow to local.
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

    local_size_ok = photo_count <= local_max_photos
    if local_size_ok:
        if accepts(local):
            return {"decision": "local", "reason": "local_within_limits"}
        if allow_main_fallback is True and accepts(main):
            return {"decision": "main", "reason": "explicit_local_fallback"}
        return {"decision": "wait", "reason": "local_unavailable"}
    if accepts(main):
        return {"decision": "main", "reason": "exceeds_local_limits"}
    return {"decision": "wait", "reason": "main_unavailable"}
