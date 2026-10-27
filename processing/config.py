"""Strict startup configuration for the private routing prototype."""

from dataclasses import dataclass, field
import os
import re
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Config:
    api_token: str
    local_node_url: str
    main_node_url: str
    local_max_photos: int
    local_max_input_bytes: int
    bind_host: str = "127.0.0.1"
    port: int = 8080
    prototype_state_dir: str = ""
    prototype_operator_token: str = ""
    photo_smoke_enabled: bool = False
    prototype_node_ids: tuple[str, ...] = ("cluster",)
    prototype_observer_token: str = ""
    local_node_token: str = field(default="", repr=False)
    main_node_token: str = field(default="", repr=False)


def _node_origin(value: str) -> str:
    if not value:
        return ""
    parsed = urlsplit(value)
    if (parsed.scheme not in ("http", "https") or not parsed.hostname
            or parsed.username or parsed.password or parsed.path not in ("", "/")
            or parsed.query or parsed.fragment or parsed.port == 0):
        raise ValueError("node URL must be an exact HTTP(S) origin")
    return value.rstrip("/")


def _positive_int(value: str, name: str, maximum: int) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not 1 <= number <= maximum:
        raise ValueError(f"{name} is outside its allowed range")
    return number


def from_environment() -> Config:
    token = os.environ.get("PROCESSING_API_TOKEN", "")
    if len(token) < 32 or any(ord(char) < 33 or ord(char) > 126 for char in token):
        raise ValueError("PROCESSING_API_TOKEN must contain at least 32 printable characters")
    operator = os.environ.get("PROCESSING_PROTOTYPE_OPERATOR_TOKEN", "")
    observer = os.environ.get("PROCESSING_PROTOTYPE_OBSERVER_TOKEN", "")
    node_tokens = tuple(os.environ.get(name, '') for name in ('LOCAL_NODE_TOKEN', 'MAIN_NODE_TOKEN'))
    if any(len(value) > 4096 or any(ord(c) < 33 or ord(c) > 126 for c in value) for value in node_tokens):
        raise ValueError('node tokens must contain at most 4096 non-space printable ASCII characters')
    if observer and (len(observer) < 32 or observer in (token, operator)
                     or any(ord(char) < 33 or ord(char) > 126 for char in observer)):
        raise ValueError("prototype observer token must be a separate printable credential of at least 32 characters")
    node_ids = tuple(os.environ.get("PROCESSING_PROTOTYPE_NODE_IDS", "cluster").split(","))
    if (not 1 <= len(node_ids) <= 8 or 'cluster' not in node_ids or len(set(node_ids)) != len(node_ids)
            or any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", node) for node in node_ids)):
        raise ValueError("PROCESSING_PROTOTYPE_NODE_IDS must list 1 to 8 unique ASCII node IDs")
    photo_smoke = os.environ.get("PROCESSING_PHOTO_SMOKE_ENABLED", "0")
    if photo_smoke not in ("0", "1"):
        raise ValueError("PROCESSING_PHOTO_SMOKE_ENABLED must be 0 or 1")
    if photo_smoke == "1" and not os.environ.get("PROCESSING_PROTOTYPE_STATE_DIR"):
        raise ValueError("photo smoke requires a dedicated prototype state directory")
    if operator and (len(operator) < 32 or operator == token
                     or any(ord(char) < 33 or ord(char) > 126 for char in operator)):
        raise ValueError("prototype operator token must be a separate printable credential of at least 32 characters")
    return Config(
        api_token=token,
        local_node_url=_node_origin(os.environ.get("LOCAL_NODE_URL", "")),
        main_node_url=_node_origin(os.environ.get("MAIN_NODE_URL", "")),
        local_max_photos=_positive_int(os.environ.get("LOCAL_MAX_PHOTOS", "800"), "LOCAL_MAX_PHOTOS", 1_000_000),
        local_max_input_bytes=_positive_int(os.environ.get("LOCAL_MAX_INPUT_BYTES", "21474836480"), "LOCAL_MAX_INPUT_BYTES", 10**15),
        bind_host=os.environ.get("PROCESSING_BIND_HOST", "127.0.0.1"),
        port=_positive_int(os.environ.get("PROCESSING_PORT", "8080"), "PROCESSING_PORT", 65535),
        prototype_state_dir=os.environ.get("PROCESSING_PROTOTYPE_STATE_DIR", ""),
        prototype_operator_token=operator,
        photo_smoke_enabled=photo_smoke == "1",
        prototype_node_ids=node_ids,
        prototype_observer_token=observer,
        local_node_token=node_tokens[0],
        main_node_token=node_tokens[1],
    )
