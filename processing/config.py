"""Strict startup configuration for the private routing prototype."""

from dataclasses import dataclass, field
import os
import re
from urllib.parse import urlsplit


@dataclass(frozen=True, repr=False)
class ProcessingNode:
    """One ordered physical engine endpoint used by the Viewer scheduler."""

    node_id: str
    origin: str
    token: str = field(default='', repr=False)
    max_photos: int | None = None
    max_input_bytes: int | None = None


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
    treedar_node_ids: tuple[str, ...] | None = None
    viewer_enabled: bool = False
    viewer_token: str = field(default='', repr=False)
    viewer_staging_token: str = field(default='', repr=False)
    viewer_tls_port: int | None = None
    viewer_max_input_bytes: int = 20 * 1024 ** 3
    viewer_max_archive_bytes: int = 100 * 1024 ** 3
    viewer_min_free_bytes: int = 20 * 1024 ** 3
    reference_config_path: str = field(default='', repr=False)
    processing_nodes: tuple[ProcessingNode, ...] = field(default=(), repr=False)


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


def _optional_positive_int(value: str, name: str, maximum: int) -> int | None:
    return None if value == '' else _positive_int(value, name, maximum)


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
    # This list is also the Viewer scheduler's preference order. Existing
    # deployments keep using PROCESSING_PROTOTYPE_NODE_IDS; the clearer alias
    # lets a future fleet add/remove nodes without changing dispatch code.
    node_ids = tuple(os.environ.get("PROCESSING_NODE_ORDER",
                                   os.environ.get("PROCESSING_PROTOTYPE_NODE_IDS", "cluster")).split(","))
    if (not 1 <= len(node_ids) <= 8 or 'cluster' not in node_ids or len(set(node_ids)) != len(node_ids)
            or any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", node) for node in node_ids)):
        raise ValueError("PROCESSING_PROTOTYPE_NODE_IDS must list 1 to 8 unique ASCII node IDs")
    treedar_node_ids = tuple(os.environ.get("PROCESSING_TREEDAR_NODE_IDS", ",".join(node_ids)).split(","))
    if (not treedar_node_ids or len(set(treedar_node_ids)) != len(treedar_node_ids)
            or any(node not in node_ids for node in treedar_node_ids)):
        raise ValueError("PROCESSING_TREEDAR_NODE_IDS must list unique configured node IDs")
    photo_smoke = os.environ.get("PROCESSING_PHOTO_SMOKE_ENABLED", "0")
    if photo_smoke not in ("0", "1"):
        raise ValueError("PROCESSING_PHOTO_SMOKE_ENABLED must be 0 or 1")
    if photo_smoke == "1" and not os.environ.get("PROCESSING_PROTOTYPE_STATE_DIR"):
        raise ValueError("photo smoke requires a dedicated prototype state directory")
    viewer = os.environ.get('PROCESSING_VIEWER_ENABLED', '0')
    if viewer not in ('0', '1'):
        raise ValueError('PROCESSING_VIEWER_ENABLED must be 0 or 1')
    viewer_token = os.environ.get('PROCESSING_VIEWER_TOKEN', '')
    viewer_staging = os.environ.get('PROCESSING_VIEWER_STAGING_TOKEN', '')
    viewer_tls_port = _optional_positive_int(os.environ.get('PROCESSING_VIEWER_TLS_PORT', ''),
                                            'PROCESSING_VIEWER_TLS_PORT', 65535)
    if viewer_tls_port is not None and (viewer != '1' or not os.environ.get('PROCESSING_REFERENCE_CONFIG')):
        raise ValueError('Viewer TLS requires the enabled Viewer facade and configured reference TLS')
    if viewer == '1':
        if not os.environ.get('PROCESSING_PROTOTYPE_STATE_DIR') or not os.environ.get('LOCAL_NODE_URL'):
            raise ValueError('Viewer requires persistent state and the ordered cluster node')
        credentials = (token, operator, observer)
        for value in (viewer_token, viewer_staging):
            if value and (len(value) < 32 or value in credentials
                          or any(ord(c) < 33 or ord(c) > 126 for c in value)):
                raise ValueError('Viewer credentials must be separate printable secrets')
        if not viewer_token or (viewer_staging and viewer_staging == viewer_token):
            raise ValueError('separate Viewer production credential required')
    processing_nodes = ()
    if viewer == '1':
        processing_nodes_list = []
        prefixes = set()
        for node_id in node_ids:
            if node_id == 'cluster':
                origin, node_token = _node_origin(os.environ.get('LOCAL_NODE_URL', '')), node_tokens[0]
                max_photos, max_bytes = (
                    _positive_int(os.environ.get('LOCAL_MAX_PHOTOS', '1000'), 'LOCAL_MAX_PHOTOS', 1_000_000),
                    _positive_int(os.environ.get('LOCAL_MAX_INPUT_BYTES', '21474836480'),
                                  'LOCAL_MAX_INPUT_BYTES', 10**15))
            elif node_id == 'main':
                origin, node_token = _node_origin(os.environ.get('MAIN_NODE_URL', '')), node_tokens[1]
                # The main machine has no application policy cap. The engine's
                # live maxImages value, when present, still applies.
                max_photos, max_bytes = None, None
            else:
                suffix = re.sub(r'[-.]', '_', node_id).upper()
                if suffix in prefixes:
                    raise ValueError('processing node IDs collide as environment variable names')
                prefixes.add(suffix)
                prefix = 'PROCESSING_NODE_' + suffix
                origin = _node_origin(os.environ.get(prefix + '_URL', ''))
                node_token = os.environ.get(prefix + '_TOKEN', '')
                if len(node_token) > 4096 or any(ord(c) < 33 or ord(c) > 126 for c in node_token):
                    raise ValueError('processing node token must contain at most 4096 printable characters')
                max_photos = _optional_positive_int(os.environ.get(prefix + '_MAX_PHOTOS', ''),
                                                    prefix + '_MAX_PHOTOS', 1_000_000)
                max_bytes = _optional_positive_int(os.environ.get(prefix + '_MAX_INPUT_BYTES', ''),
                                                   prefix + '_MAX_INPUT_BYTES', 10**15)
            if not origin:
                raise ValueError('every ordered processing node requires a configured origin')
            processing_nodes_list.append(ProcessingNode(node_id, origin, node_token, max_photos, max_bytes))
        processing_nodes = tuple(processing_nodes_list)
    if operator and (len(operator) < 32 or operator == token
                     or any(ord(char) < 33 or ord(char) > 126 for char in operator)):
        raise ValueError("prototype operator token must be a separate printable credential of at least 32 characters")
    return Config(
        api_token=token,
        local_node_url=_node_origin(os.environ.get("LOCAL_NODE_URL", "")),
        main_node_url=_node_origin(os.environ.get("MAIN_NODE_URL", "")),
        local_max_photos=_positive_int(os.environ.get("LOCAL_MAX_PHOTOS", "1000"), "LOCAL_MAX_PHOTOS", 1_000_000),
        local_max_input_bytes=_positive_int(os.environ.get("LOCAL_MAX_INPUT_BYTES", "21474836480"), "LOCAL_MAX_INPUT_BYTES", 10**15),
        bind_host=os.environ.get("PROCESSING_BIND_HOST", "127.0.0.1"),
        port=_positive_int(os.environ.get("PROCESSING_PORT", "8080"), "PROCESSING_PORT", 65535),
        prototype_state_dir=os.environ.get("PROCESSING_PROTOTYPE_STATE_DIR", ""),
        prototype_operator_token=operator,
        photo_smoke_enabled=photo_smoke == "1",
        prototype_node_ids=node_ids,
        treedar_node_ids=treedar_node_ids,
        viewer_enabled=viewer == '1', viewer_token=viewer_token,
        viewer_staging_token=viewer_staging,
        viewer_tls_port=viewer_tls_port,
        viewer_max_input_bytes=_positive_int(os.environ.get('PROCESSING_VIEWER_MAX_INPUT_BYTES', str(20 * 1024 ** 3)),
                                            'PROCESSING_VIEWER_MAX_INPUT_BYTES', 10 ** 15),
        viewer_max_archive_bytes=_positive_int(os.environ.get('PROCESSING_VIEWER_MAX_ARCHIVE_BYTES', str(100 * 1024 ** 3)),
                                              'PROCESSING_VIEWER_MAX_ARCHIVE_BYTES', 10 ** 15),
        viewer_min_free_bytes=_positive_int(os.environ.get('PROCESSING_VIEWER_MIN_FREE_BYTES', str(20 * 1024 ** 3)),
                                            'PROCESSING_VIEWER_MIN_FREE_BYTES', 10 ** 15),
        prototype_observer_token=observer,
        local_node_token=node_tokens[0],
        main_node_token=node_tokens[1],
        reference_config_path=os.environ.get('PROCESSING_REFERENCE_CONFIG', ''),
        processing_nodes=processing_nodes,
    )
