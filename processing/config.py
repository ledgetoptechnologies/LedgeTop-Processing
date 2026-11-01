"""Strict startup configuration for the private routing prototype."""

from dataclasses import dataclass, field
import ipaddress
import os
import re
from urllib.parse import urlsplit

ADMISSION_DRAIN_QUALIFIED = False
RECOVERY_HOLD_QUALIFIED = False
ARCHIVE_REGISTRY_QUALIFIED = False


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
    viewer_archive_autocapture: bool = False
    viewer_archive_free_floor: int = 20 * 1024 ** 3
    viewer_archive_registry: bool = False
    reference_config_path: str = field(default='', repr=False)
    admission_drain_enabled: bool = False
    admission_start_closed: bool = False
    recovery_hold_required: bool = False
    authority_lease_dir: str = ''
    viewer_production_held: bool = False


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
    lease_dir = os.environ.get('PROCESSING_AUTHORITY_LEASE_DIR', '')
    autocapture = os.environ.get('PROCESSING_VIEWER_ARCHIVE_AUTOCAPTURE', '0')
    registry = os.environ.get('PROCESSING_VIEWER_ARCHIVE_REGISTRY', '0')
    if registry not in ('0','1'):
        raise ValueError('invalid archive registry setting')
    if registry == '1' and (not ARCHIVE_REGISTRY_QUALIFIED or viewer != '1' or not lease_dir):
        raise ValueError('archive registry is not qualified for startup')
    if autocapture not in ('0', '1'):
        raise ValueError('PROCESSING_VIEWER_ARCHIVE_AUTOCAPTURE must be 0 or 1')
    if autocapture == '1':
        if viewer != '1' or not lease_dir or os.name != 'posix' or not hasattr(os, 'posix_fallocate'):
            raise ValueError('Viewer archive autocapture requires POSIX Viewer and authority lease')
        # No DNS lookup may outlive bounded child supervision.
        try:
            for name in ('LOCAL_NODE_URL', 'MAIN_NODE_URL'):
                ipaddress.ip_address(urlsplit(os.environ.get(name, '')).hostname or '')
        except ValueError:
            raise ValueError('Viewer archive autocapture requires literal node IP addresses') from None
    if lease_dir and (not os.path.isabs(lease_dir) or not os.environ.get('PROCESSING_PROTOTYPE_STATE_DIR')):
        raise ValueError('PROCESSING_AUTHORITY_LEASE_DIR must be an absolute path used with persistent state')
    production_admission = os.environ.get('PROCESSING_VIEWER_PRODUCTION_ADMISSION', 'open')
    if production_admission not in ('open', 'held'):
        raise ValueError('PROCESSING_VIEWER_PRODUCTION_ADMISSION must be open or held')
    admission = os.environ.get('PROCESSING_ADMISSION_DRAIN_ENABLED', '0')
    if admission not in ('0', '1') or (admission == '1' and (not os.environ.get('PROCESSING_PROTOTYPE_STATE_DIR') or not operator)):
        raise ValueError('admission drain requires persistent state and operator credential')
    recovery_hold=os.environ.get('PROCESSING_RECOVERY_HOLD_REQUIRED','0')
    if recovery_hold not in ('0','1') or (recovery_hold=='1' and admission!='1'):
        raise ValueError('required recovery hold requires admission drain')
    if recovery_hold=='1' and RECOVERY_HOLD_QUALIFIED is not True:
        raise ValueError('recovery hold is not qualified for startup')
    start_closed=os.environ.get('PROCESSING_ADMISSION_START_CLOSED','0')
    if start_closed not in ('0','1') or (start_closed=='1' and admission!='1'):
        raise ValueError('closed startup requires admission drain')
    if admission == '1' and ADMISSION_DRAIN_QUALIFIED is not True:
        raise ValueError('admission drain is not qualified for startup')
    if viewer not in ('0', '1'):
        raise ValueError('PROCESSING_VIEWER_ENABLED must be 0 or 1')
    viewer_token = os.environ.get('PROCESSING_VIEWER_TOKEN', '')
    viewer_staging = os.environ.get('PROCESSING_VIEWER_STAGING_TOKEN', '')
    if viewer == '1':
        if (not os.environ.get('PROCESSING_PROTOTYPE_STATE_DIR') or 'main' not in node_ids
                or not os.environ.get('LOCAL_NODE_URL') or not os.environ.get('MAIN_NODE_URL')):
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
        viewer_archive_autocapture=autocapture == '1',
        viewer_archive_registry=registry == '1',
        viewer_archive_free_floor=_positive_int(os.environ.get('PROCESSING_VIEWER_ARCHIVE_FREE_FLOOR', str(20 * 1024 ** 3)),
                                                'PROCESSING_VIEWER_ARCHIVE_FREE_FLOOR', 10 ** 15),
        prototype_observer_token=observer,
        local_node_token=node_tokens[0],
        main_node_token=node_tokens[1],
        reference_config_path=os.environ.get('PROCESSING_REFERENCE_CONFIG', ''),
        admission_drain_enabled=admission == '1',
        admission_start_closed=start_closed == '1',
        recovery_hold_required=recovery_hold == '1',
        authority_lease_dir=lease_dir,
        viewer_production_held=production_admission == 'held',
    )
