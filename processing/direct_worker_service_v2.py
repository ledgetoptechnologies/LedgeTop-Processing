"""Opt-in, single-reservation V2 worker listener factory; not runtime wired.

The caller supplies trusted, locally enrolled reservation dependencies.  This
module never accepts reservation data from a remote request or starts a thread.
"""
import ipaddress
import os

from . import reference_protocol as protocol
from .reference_runtime import _file, RuntimeConfigurationError
from .direct_worker_receiver_v2 import DirectWorkerBoundaryV2
from .direct_worker_http import create_direct_worker_server


def load_listener_settings(path=None):
    """Read owner-private opt-in settings; absence means disabled without I/O."""
    if path is None:
        return None
    try:
        candidate, initial = _file(path, private=True)
        fd = os.open(candidate, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        try:
            observed = os.fstat(fd)
            if (observed.st_dev, observed.st_ino) != (initial.st_dev, initial.st_ino):
                raise ValueError('listener configuration changed')
            with os.fdopen(fd, 'rb', closefd=False) as stream:
                raw = stream.read(4097)
        finally:
            os.close(fd)
        if len(raw) > 4096:
            raise ValueError('listener configuration too large')
        value = protocol.decode_submission(raw)
        protocol._closed(value, ('schemaVersion', 'enabled', 'host', 'port'), 'V2 listener settings')
        if type(value['schemaVersion']) is not int or value['schemaVersion'] != 1 or type(value['enabled']) is not bool:
            raise ValueError('invalid listener configuration')
        # Initial integration is loopback-only until network identity and
        # installed ingress are independently qualified.
        if type(value['host']) is not str or not ipaddress.ip_address(value['host']).is_loopback:
            raise ValueError('loopback listener required')
        if type(value['port']) is not int or not 1 <= value['port'] <= 65535:
            raise ValueError('explicit listener port required')
        return value if value['enabled'] else None
    except (OSError, ValueError, TypeError, KeyError, RuntimeConfigurationError):
        raise RuntimeConfigurationError('invalid private V2 worker listener settings') from None


def create_listener(settings_path=None, *, boundary_kwargs=None, tls_context=None):
    """Construct a pinned boundary and TLS listener, or return None if disabled.

    `boundary_kwargs` and `tls_context` must come from a trusted local owner;
    neither may be populated from an HTTP request.  Caller owns server_close().
    """
    settings = load_listener_settings(settings_path)
    if settings is None:
        return None
    if type(boundary_kwargs) is not dict:
        raise RuntimeConfigurationError('trusted V2 boundary dependencies required')
    boundary = DirectWorkerBoundaryV2(**boundary_kwargs)
    return create_direct_worker_server(boundary, settings['host'], settings['port'], ssl_context=tls_context)
