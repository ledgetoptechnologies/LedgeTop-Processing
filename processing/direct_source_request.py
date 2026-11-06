"""Request-scoped Viewer identity derived from an actual TLS connection.

This is infrastructure for a future direct intake endpoint, not an activation
or a durable authorization.  The trusted listener must construct and consume
the context synchronously; forwarded headers are never transport evidence.
"""
from dataclasses import dataclass, field
import ssl
import threading


@dataclass(frozen=True, repr=False)
class DirectSourceRequest:
    environment: str
    principal: str
    _connection: object = field(repr=False, compare=False)
    _thread: int = field(repr=False, compare=False)

    def require_current(self):
        if (threading.get_ident() != self._thread
                or not isinstance(self._connection, ssl.SSLSocket)
                or self._connection.fileno() < 0
                or self._connection.cipher() is None):
            raise ValueError('source request connection unavailable')


def authenticate_source_request(viewer_api, connection, headers):
    """Use the existing protected bearer registry, not caller identity fields."""
    if (not isinstance(connection, ssl.SSLSocket) or connection.fileno() < 0
            or connection.cipher() is None):
        raise ValueError('TLS source connection required')
    environment = viewer_api._authenticate(headers)
    principal = viewer_api._source_principal(headers)
    if environment is None or principal is None:
        raise ValueError('source identity required')
    return DirectSourceRequest(environment, principal, connection, threading.get_ident())
