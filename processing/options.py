"""Fresh, read-only NodeODM-family option catalogs from fixed configured origins.

No application flag names are encoded here. Definitions come from the engine's
own authenticated /options response. This module does not implement a Viewer
provider, admit a job, or claim that upstream's in-process cache is refreshed.
"""

from concurrent.futures import ThreadPoolExecutor
import hashlib
from http.client import HTTPException
import json
import re
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z')
NODE_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z')
MAX_INFO = 65536
MAX_OPTIONS = 262144
MAX_COUNT = 2048


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


def _canonical(value):
    return json.dumps(value, sort_keys=True, allow_nan=False, separators=(',', ':')).encode()


def _sha(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _origin(value):
    if not isinstance(value, str) or any(ord(char) < 33 or ord(char) > 126 for char in value):
        raise ValueError('invalid configured origin')
    parsed = urlsplit(value)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username
            or parsed.password or parsed.path not in ('', '/') or parsed.query
            or parsed.fragment or parsed.port == 0):
        raise ValueError('invalid configured origin')
    return value.rstrip('/')


def _get_json(origin, route, token, limit):
    query = '?' + urlencode({'token': token}) if token else ''
    request = Request(origin + route + query, headers={'Accept': 'application/json'})
    opener = build_opener(ProxyHandler({}), _NoRedirect())
    with opener.open(request, timeout=3) as response:
        media_type = response.headers.get_content_type().lower()
        if media_type != 'application/json' and not media_type.endswith('+json'):
            raise ValueError('invalid response type')
        raw = response.read(limit + 1)
        if len(raw) > limit:
            raise ValueError('response exceeds limit')
        return json.loads(raw)


def _validate_info(info):
    if not isinstance(info, dict):
        raise ValueError('invalid node information')
    for key in ('version', 'engine', 'engineVersion'):
        value = info.get(key)
        if (not isinstance(value, str) or not 1 <= len(value) <= 120
                or any(ord(char) < 32 or ord(char) == 127 for char in value)):
            raise ValueError('invalid node information')
    return {key: info[key] for key in ('version', 'engine', 'engineVersion')}


def _validate_options(options):
    if not isinstance(options, list) or not 1 <= len(options) <= MAX_COUNT:
        raise ValueError('invalid option catalog')
    names = set()
    for option in options:
        if (not isinstance(option, dict) or not isinstance(option.get('name'), str)
                or not NAME.fullmatch(option['name']) or option['name'] in names):
            raise ValueError('invalid option definition')
        names.add(option['name'])
    # Reject non-finite values and unreasonable recursive data without removing
    # newly introduced metadata fields from an otherwise valid engine response.
    _canonical(options)
    return options


def _contains_secret(value, token):
    """Fail closed if an upstream reflects its private credential as metadata."""
    if not token:
        return False
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str) and token in item:
            return True
        if isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
    return False


def discover_node(role, origin, token=''):
    """Fetch a fresh snapshot; errors contain no URL, credential or server body."""
    if not isinstance(role, str) or not NODE_ID.fullmatch(role):
        raise ValueError('invalid configured node ID')
    unavailable = {'role': role, 'configured': bool(origin), 'available': False,
                   'options': [], 'optionsSha256': None, 'capabilitiesSha256': None,
                   'version': None, 'engine': None, 'engineVersion': None,
                   'reason': 'not_configured'}
    if not origin:
        return unavailable
    try:
        origin = _origin(origin)
        if (not isinstance(token, str) or len(token) > 4096
                or any(ord(char) < 33 or ord(char) > 126 for char in token)):
            raise ValueError('invalid configured credential')
        info = _validate_info(_get_json(origin, '/info', token, MAX_INFO))
        options = _validate_options(_get_json(origin, '/options', token, MAX_OPTIONS))
        if _contains_secret([info, options], token):
            raise ValueError('credential reflected by upstream')
        option_hash = _sha(sorted(options, key=lambda option: option['name']))
        return {**unavailable, **info, 'available': True, 'reason': None,
                'options': options, 'optionsSha256': option_hash,
                'capabilitiesSha256': _sha({**info, 'optionsSha256': option_hash})}
    except HTTPError as error:
        error.close()
        return {**unavailable, 'reason': 'discovery_failed'}
    except (URLError, OSError, TimeoutError, HTTPException):
        return {**unavailable, 'reason': 'discovery_failed'}
    except (ValueError, TypeError, RecursionError, UnicodeError):
        return {**unavailable, 'reason': 'invalid_response_or_configuration'}


def catalog(nodes):
    """Exact-definition intersection across every configured node, never a cache.

    ``nodes`` contains up to eight unique configured node IDs, each as
    (node_id, configured_origin, optional_token). Unconfigured nodes are reported
    but do not constrain the intersection. Any configured unavailable node closes
    the fleet catalog; healthy per-node snapshots remain available for inspection.
    """
    nodes = list(nodes)
    if (not 1 <= len(nodes) <= 8 or any(not isinstance(node, (tuple, list)) or len(node) != 3 for node in nodes)
            or any(not isinstance(node[0], str) or not NODE_ID.fullmatch(node[0]) for node in nodes)
            or len({node[0] for node in nodes}) != len(nodes)):
        raise ValueError('catalog requires one to eight unique node configurations')
    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        snapshots = list(executor.map(lambda node: discover_node(*node), nodes))
    configured = [node for node in snapshots if node['configured']]
    ready = bool(configured) and all(node['available'] for node in configured)
    available = [node for node in configured if node['available']]
    by_node = {node['role']: {option['name']: option for option in node['options']} for node in available}
    union = set().union(*(set(options) for options in by_node.values())) if by_node else set()
    common = set.intersection(*(set(options) for options in by_node.values())) if by_node else set()
    conflicting = sorted(name for name in common
        if len({_canonical(options[name]) for options in by_node.values()}) > 1)
    safe_names = common - set(conflicting) if ready else set()
    reference = next(iter(by_node.values()), {})
    common_options = [reference[name] for name in sorted(safe_names)]
    return {'ready': ready, 'reason': None if ready else ('node_unavailable' if configured else 'not_configured'),
            'nodes': snapshots, 'commonOptions': common_options,
            'commonOptionsSha256': _sha(common_options) if ready else None,
            'conflictingNames': conflicting,
            'unsupportedByNode': {node_id: sorted(union - set(options)) for node_id, options in by_node.items()}}
