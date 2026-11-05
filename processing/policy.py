"""Pure ordered-node selection, shared by probes and dispatch preflight.

This helper reserves nothing: callers must atomically gate and recheck admission
before dispatch. Missing queue/state fields retain legacy fixture compatibility;
when supplied, queue counts must be zero and maintenance state must be active.
"""


def plan_ordered_nodes(photo_count: int, input_bytes: int, nodes: list[dict], *,
                       required_profile: str | None = None,
                       required_features: tuple[str, ...] = ()) -> dict:
    """Choose the first available node in configured order that can run a job.

    ``maxImages`` may be populated directly from each engine's live ``/info``
    response. ``maxInputBytes`` is an optional per-node policy ceiling. A
    missing maximum means that dimension is unbounded. This is only a routing
    decision; the caller still has to atomically claim the job and recheck the
    node's maintenance and execution-slot state.
    """
    if type(photo_count) is not int or photo_count < 0 or type(input_bytes) is not int or input_bytes < 0:
        raise ValueError("job size must use non-negative integers")
    if (type(nodes) not in (list, tuple) or not 1 <= len(nodes) <= 8
            or any(type(node) is not dict or type(node.get("id")) is not str
                   or not node["id"] or not node["id"].isascii()
                   or any(ord(char) <= 32 for char in node["id"]) for node in nodes)
            or len({node["id"] for node in nodes}) != len(nodes)):
        raise ValueError("an ordered fleet of unique node IDs is required")
    if (required_profile is not None and (type(required_profile) is not str or not required_profile)
            or type(required_features) not in (list, tuple)
            or any(type(feature) is not str or not feature for feature in required_features)
            or len(set(required_features)) != len(required_features)):
        raise ValueError("invalid job requirements")

    saw_healthy = saw_capacity = saw_idle = False
    for node in nodes:
        if node.get("ready") is not True:
            continue
        state = node.get("state", "active")
        maintenance = node.get("maintenanceState", "active")
        if state != "active" or maintenance != "active":
            continue
        saw_healthy = True
        queued = node.get("taskQueueCount", 0)
        if type(queued) is not int or queued != 0:
            continue
        saw_idle = True
        max_images = node.get("maxImages")
        max_bytes = node.get("maxInputBytes")
        if ((max_images is not None and (type(max_images) is not int or max_images < 1))
                or (max_bytes is not None and (type(max_bytes) is not int or max_bytes < 1))):
            continue
        if max_images is not None and photo_count > max_images:
            continue
        if max_bytes is not None and input_bytes > max_bytes:
            continue
        saw_capacity = True
        profiles = node.get("allowedProfiles")
        if (required_profile is not None and profiles is not None
                and (type(profiles) is not list or required_profile not in profiles)):
            continue
        features = node.get("features")
        if (required_features and (type(features) is not list
                or any(feature not in features for feature in required_features))):
            continue
        return {"decision": "node", "nodeId": node["id"], "reason": "first_available_eligible_node"}

    if not saw_healthy:
        reason = "no_healthy_node"
    elif not saw_idle:
        reason = "nodes_busy"
    elif not saw_capacity:
        reason = "no_node_capacity"
    else:
        reason = "no_node_with_required_capabilities"
    return {"decision": "wait", "reason": reason}


def plan(photo_count: int, input_bytes: int, local: dict, main: dict,
         local_max_photos: int, local_max_input_bytes: int, *,
         allow_main_fallback: bool = False) -> dict:
    local_size_ok = photo_count <= local_max_photos and input_bytes <= local_max_input_bytes
    if local_size_ok:
        if type(local) is not dict:
            return {"decision": "wait", "reason": "local_unavailable"}
        candidates = [
            {"id": "local", **local, "maxImages": _minimum_limit(local.get("maxImages"), local_max_photos),
             "maxInputBytes": local_max_input_bytes},
        ]
        if allow_main_fallback is True:
            if type(main) is dict:
                candidates.append({"id": "main", **main})
        result = plan_ordered_nodes(photo_count, input_bytes, candidates)
        if result["decision"] == "node":
            return {"decision": "local" if result["nodeId"] == "local" else "main",
                    "reason": "local_within_limits" if result["nodeId"] == "local"
                    else "explicit_local_fallback"}
        return {"decision": "wait", "reason": "local_unavailable"}
    if type(main) is not dict:
        return {"decision": "wait", "reason": "main_unavailable"}
    result = plan_ordered_nodes(photo_count, input_bytes, [{"id": "main", **main}])
    return ({"decision": "main", "reason": "exceeds_local_limits"}
            if result["decision"] == "node" else {"decision": "wait", "reason": "main_unavailable"})


def _minimum_limit(engine_limit, configured_limit):
    if engine_limit is None:
        return configured_limit
    if type(engine_limit) is not int or engine_limit < 1:
        return engine_limit
    return min(engine_limit, configured_limit)
