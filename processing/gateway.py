"""Optional coordinator-to-node gateway authentication.

This header is independent of a native NodeODM token. It is never placed in a
URL. Non-loopback gateways must use HTTPS with normal certificate verification.
"""
import ipaddress
import re
from urllib.parse import urlsplit


def gateway_headers(origin, secret=''):
    if not isinstance(secret, str):
        raise ValueError('invalid gateway credential')
    if secret == '':
        return {}
    if not re.fullmatch(r'[A-Za-z0-9_-]{32,128}', secret):
        raise ValueError('invalid gateway credential')
    if not isinstance(origin, str) or any(ord(c) < 33 or ord(c) > 126 for c in origin):
        raise ValueError('invalid gateway origin')
    parsed = urlsplit(origin)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username
            or parsed.password or parsed.path not in ('', '/') or parsed.query or parsed.fragment):
        raise ValueError('invalid gateway origin')
    try:
        if parsed.port == 0:
            raise ValueError('invalid gateway origin')
    except ValueError:
        raise ValueError('invalid gateway origin') from None
    try:
        loopback = ipaddress.ip_address(parsed.hostname).is_loopback
    except ValueError:
        loopback = False
    if parsed.scheme != 'https' and not loopback:
        raise ValueError('gateway credential requires HTTPS outside loopback')
    return {'X-LedgeTop-Gateway': secret}


def node_arguments(role, origin, token, secret=''):
    """Preserve legacy three-argument calls when the feature is disabled."""
    return (role, origin, token, secret) if secret else (role, origin, token)
