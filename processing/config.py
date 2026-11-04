"""Strict startup configuration for the private routing prototype."""

from dataclasses import dataclass, field
import ipaddress
import json
import os
import re
from pathlib import Path
from urllib.parse import urlsplit

ADMISSION_DRAIN_QUALIFIED = False


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
    viewer_max_input_bytes: int = 20 * 1024 ** 3
    viewer_max_archive_bytes: int = 100 * 1024 ** 3
    reference_config_path: str = field(default='', repr=False)
    admission_drain_enabled: bool = False
    authority_lease_dir: str = ''
    viewer_production_held: bool = False
    per_node_slots: bool = False
    node_inventory: tuple = field(default=(), repr=False)
    viewer_parallel_dispatch: bool = False
    viewer_node_lost_hold: bool = False
    viewer_log_retention: bool = False
    viewer_lost_result_adopt: bool = False


def parse_node_inventory(raw, environment):
    try:
        items = json.loads(raw)
    except (TypeError, ValueError):
        raise ValueError('invalid processing node inventory') from None
    if type(items) is not list or not 1 <= len(items) <= 8:
        raise ValueError('processing node inventory must have 1 to 8 nodes')
    result = []
    for item in items:
        if type(item) is not dict or set(item) != {'id', 'physicalHostId', 'origin', 'tokenEnv', 'maxPhotos', 'capabilities'}:
            raise ValueError('invalid processing node inventory entry')
        identifier, origin, token_key = item['id'], item['origin'], item['tokenEnv']
        maximum, capabilities = item['maxPhotos'], item['capabilities']
        physical_id = item['physicalHostId']
        if (type(identifier) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}', identifier)
                or type(physical_id) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}', physical_id)
                or type(origin) is not str or not _node_origin(origin)
                or type(token_key) is not str or not re.fullmatch(r'[A-Z][A-Z0-9_]{0,127}', token_key)
                or (maximum is not None and (type(maximum) is not int or not 1 <= maximum <= 1_000_000))
                or type(capabilities) is not list or not capabilities
                or any(type(cap) is not str or cap not in ('odm', 'treedar') for cap in capabilities)
                or len(capabilities) != len(set(capabilities))):
            raise ValueError('invalid processing node inventory entry')
        node_token = environment.get(token_key, '')
        if type(node_token) is not str or len(node_token) > 4096 or any(ord(c) < 33 or ord(c) > 126 for c in node_token):
            raise ValueError('invalid node credential')
        result.append({'id': identifier, 'physicalHostId': physical_id, 'origin': _node_origin(origin), 'token': node_token,
                       'maxPhotos': maximum, 'capabilities': tuple(capabilities)})
    if len({v['id'] for v in result}) != len(result):
        raise ValueError('duplicate processing node id')
    if len({v['physicalHostId'] for v in result}) != len(result):
        raise ValueError('duplicate processing physical host id')
    for i, left in enumerate(result):
        for right in result[i + 1:]:
            require_distinct_node_hosts(left['origin'], right['origin'])
    return tuple(result)


def require_distinct_node_hosts(local_url: str, main_url: str):
    """Reject obvious same-host aliases, not a physical identity attestation."""
    def host(origin):
        value = _node_origin(origin)
        if not value:
            raise ValueError('per-node slots require two node origins')
        name = urlsplit(value).hostname.rstrip('.').lower()
        if name == 'localhost':
            return '127.0.0.1'
        try:
            ip = ipaddress.ip_address(name)
            if getattr(ip, 'ipv4_mapped', None):
                ip = ip.ipv4_mapped
            if ip.is_loopback:
                return '127.0.0.1'
            return str(ip)
        except ValueError:
            return name
    if host(local_url) == host(main_url):
        raise ValueError('per-node slots require distinct node hosts')


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
    inventory_raw = os.environ.get('PROCESSING_NODE_INVENTORY', '')
    inventory = parse_node_inventory(inventory_raw, os.environ) if inventory_raw else ()
    legacy_state = os.environ.get('PROCESSING_PROTOTYPE_STATE_DIR', '')
    if inventory and legacy_state and (Path(legacy_state) / 'jobs.sqlite3').exists():
        legacy_host = os.environ.get('PROCESSING_INVENTORY_LEGACY_HOST_ID', '')
        mapped = [node for node in inventory if node['physicalHostId'] == legacy_host]
        legacy_limit = _positive_int(os.environ.get('LOCAL_MAX_PHOTOS', '1000'), 'LOCAL_MAX_PHOTOS', 1_000_000)
        if (not legacy_host or len(mapped) != 1 or mapped[0]['maxPhotos'] is None
                or mapped[0]['maxPhotos'] > legacy_limit):
            raise ValueError('existing state requires explicit legacy host mapping preserving the local photo limit')
    legacy_local = os.environ.get('LOCAL_NODE_URL', '')
    if inventory and legacy_local:
        legacy_limit = _positive_int(os.environ.get('LOCAL_MAX_PHOTOS', '1000'), 'LOCAL_MAX_PHOTOS', 1_000_000)
        for node in inventory:
            try:
                require_distinct_node_hosts(legacy_local, node['origin'])
            except ValueError:
                if node['maxPhotos'] is None or node['maxPhotos'] > legacy_limit:
                    raise ValueError('inventory must preserve existing local photo limit') from None
    node_ids = tuple(v['id'] for v in inventory) if inventory else tuple(os.environ.get("PROCESSING_PROTOTYPE_NODE_IDS", "cluster").split(","))
    if (not 1 <= len(node_ids) <= 8 or (not inventory and 'cluster' not in node_ids) or len(set(node_ids)) != len(node_ids)
            or any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", node) for node in node_ids)):
        raise ValueError("PROCESSING_PROTOTYPE_NODE_IDS must list 1 to 8 unique ASCII node IDs")
    treedar_node_ids = tuple(v['id'] for v in inventory if 'treedar' in v['capabilities']) if inventory else tuple(os.environ.get("PROCESSING_TREEDAR_NODE_IDS", ",".join(node_ids)).split(","))
    if ((not treedar_node_ids and not inventory) or len(set(treedar_node_ids)) != len(treedar_node_ids)
            or any(node not in node_ids for node in treedar_node_ids)):
        raise ValueError("PROCESSING_TREEDAR_NODE_IDS must list unique configured node IDs")
    photo_smoke = os.environ.get("PROCESSING_PHOTO_SMOKE_ENABLED", "0")
    if photo_smoke not in ("0", "1"):
        raise ValueError("PROCESSING_PHOTO_SMOKE_ENABLED must be 0 or 1")
    if photo_smoke == "1" and not os.environ.get("PROCESSING_PROTOTYPE_STATE_DIR"):
        raise ValueError("photo smoke requires a dedicated prototype state directory")
    viewer = os.environ.get('PROCESSING_VIEWER_ENABLED', '0')
    per_node_slots = os.environ.get('PROCESSING_PER_NODE_SLOTS', '0')
    if per_node_slots not in ('0', '1'):
        raise ValueError('PROCESSING_PER_NODE_SLOTS must be 0 or 1')
    parallel_dispatch = os.environ.get('PROCESSING_VIEWER_PARALLEL_DISPATCH', '0')
    if parallel_dispatch not in ('0', '1'):
        raise ValueError('PROCESSING_VIEWER_PARALLEL_DISPATCH must be 0 or 1')
    if parallel_dispatch == '1' and per_node_slots != '1':
        raise ValueError('parallel Viewer dispatch requires per-node slots')
    if parallel_dispatch == '1' and os.environ.get('PROCESSING_REFERENCE_CONFIG'):
        raise ValueError('parallel Viewer dispatch is not qualified with the reference runtime')
    node_lost_hold = os.environ.get('PROCESSING_VIEWER_NODE_LOST_HOLD', '0')
    if node_lost_hold not in ('0', '1'):
        raise ValueError('PROCESSING_VIEWER_NODE_LOST_HOLD must be 0 or 1')
    log_retention = os.environ.get('PROCESSING_VIEWER_LOG_RETENTION', '0')
    if log_retention not in ('0', '1'):
        raise ValueError('PROCESSING_VIEWER_LOG_RETENTION must be 0 or 1')
    lost_result_adopt = os.environ.get('PROCESSING_VIEWER_LOST_RESULT_ADOPT', '0')
    if lost_result_adopt not in ('0', '1'):
        raise ValueError('PROCESSING_VIEWER_LOST_RESULT_ADOPT must be 0 or 1')
    if lost_result_adopt == '1' and node_lost_hold != '1':
        raise ValueError('PROCESSING_VIEWER_LOST_RESULT_ADOPT requires PROCESSING_VIEWER_NODE_LOST_HOLD')
    if inventory and (per_node_slots != '1' or os.environ.get('PROCESSING_REFERENCE_CONFIG')):
        raise ValueError('inventory requires per-node slots and no reference runtime')
    lease_dir = os.environ.get('PROCESSING_AUTHORITY_LEASE_DIR', '')
    if lease_dir and (not os.path.isabs(lease_dir) or not os.environ.get('PROCESSING_PROTOTYPE_STATE_DIR')):
        raise ValueError('PROCESSING_AUTHORITY_LEASE_DIR must be an absolute path used with persistent state')
    production_admission = os.environ.get('PROCESSING_VIEWER_PRODUCTION_ADMISSION', 'open')
    if production_admission not in ('open', 'held'):
        raise ValueError('PROCESSING_VIEWER_PRODUCTION_ADMISSION must be open or held')
    admission = os.environ.get('PROCESSING_ADMISSION_DRAIN_ENABLED', '0')
    if admission not in ('0', '1') or (admission == '1' and (not os.environ.get('PROCESSING_PROTOTYPE_STATE_DIR') or not operator)):
        raise ValueError('admission drain requires persistent state and operator credential')
    if admission == '1' and ADMISSION_DRAIN_QUALIFIED is not True:
        raise ValueError('admission drain is not qualified for startup')
    if viewer not in ('0', '1'):
        raise ValueError('PROCESSING_VIEWER_ENABLED must be 0 or 1')
    viewer_token = os.environ.get('PROCESSING_VIEWER_TOKEN', '')
    viewer_staging = os.environ.get('PROCESSING_VIEWER_STAGING_TOKEN', '')
    if viewer == '1':
        if inventory and not any('odm' in n['capabilities'] for n in inventory):
            raise ValueError('Viewer inventory requires an ODM-capable node')
        if (not os.environ.get('PROCESSING_PROTOTYPE_STATE_DIR') or (not inventory and ('main' not in node_ids
                or not os.environ.get('LOCAL_NODE_URL') or not os.environ.get('MAIN_NODE_URL')))):
            raise ValueError('Viewer requires persistent state and both configured nodes')
        credentials = (token, operator, observer)
        for value in (viewer_token, viewer_staging):
            if value and (len(value) < 32 or value in credentials
                          or any(ord(c) < 33 or ord(c) > 126 for c in value)):
                raise ValueError('Viewer credentials must be separate printable secrets')
        if not viewer_token or (viewer_staging and viewer_staging == viewer_token):
            raise ValueError('separate Viewer production credential required')
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
        viewer_max_input_bytes=_positive_int(os.environ.get('PROCESSING_VIEWER_MAX_INPUT_BYTES', str(20 * 1024 ** 3)),
                                            'PROCESSING_VIEWER_MAX_INPUT_BYTES', 10 ** 15),
        viewer_max_archive_bytes=_positive_int(os.environ.get('PROCESSING_VIEWER_MAX_ARCHIVE_BYTES', str(100 * 1024 ** 3)),
                                              'PROCESSING_VIEWER_MAX_ARCHIVE_BYTES', 10 ** 15),
        prototype_observer_token=observer,
        local_node_token=node_tokens[0],
        main_node_token=node_tokens[1],
        reference_config_path=os.environ.get('PROCESSING_REFERENCE_CONFIG', ''),
        admission_drain_enabled=admission == '1',
        authority_lease_dir=lease_dir,
        viewer_production_held=production_admission == 'held',
        per_node_slots=per_node_slots == '1',
        node_inventory=inventory,
        viewer_parallel_dispatch=parallel_dispatch == '1',
        viewer_node_lost_hold=node_lost_hold == '1',
        viewer_log_retention=log_retention == '1',
        viewer_lost_result_adopt=lost_result_adopt == '1',
    )
