"""Pure, non-dispatching routing policy for the first coordinator slice."""


def plan(photo_count: int, input_bytes: int, local: dict, main: dict,
         local_max_photos: int, local_max_input_bytes: int) -> dict:
    def accepts(node: dict) -> bool:
        limit = node.get("maxImages")
        return bool(node.get("ready")) and (limit is None or photo_count <= limit)

    local_size_ok = photo_count <= local_max_photos and input_bytes <= local_max_input_bytes
    if local_size_ok and accepts(local):
        return {"decision": "local", "reason": "local_within_limits"}
    if accepts(main):
        return {"decision": "main", "reason": "local_unavailable" if local_size_ok else "exceeds_local_limits"}
    return {
        "decision": "wait",
        "reason": "main_unavailable" if not local_size_ok else "no_eligible_node",
    }
