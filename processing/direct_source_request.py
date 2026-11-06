"""Request-scoped Viewer identity derived from an actual TLS connection.

This is infrastructure for a future direct intake endpoint, not an activation
or a durable authorization.  The trusted listener must construct and consume
the context synchronously; forwarded headers are never transport evidence.
"""
import ssl
import threading

class DirectSourceRequest:
    """Synchronous capability, revalidated against its originating registry.

    Same-process Python and the underlying socket are trusted; this is not a
    sandbox against code able to mutate interpreter objects or the registry.
    """
    __slots__ = ('_api', '_connection', '_thread', '_headers', '_environment', '_principal', '_active')

    def __init__(self, *args, **kwargs):
        raise TypeError('use authenticate_source_request')

    @property
    def environment(self):
        self.require_current()
        return self._environment

    @property
    def principal(self):
        self.require_current()
        return self._principal

    def invalidate(self):
        self._active = False
        self._headers = {}

    def require_current(self):
        if (not self._active or threading.get_ident() != self._thread
                or not isinstance(self._connection, ssl.SSLSocket)
                or self._connection.fileno() < 0
                or self._connection.cipher() is None
                or self._api._authenticate(self._headers) != self._environment
                or self._api._source_principal(self._headers) != self._principal):
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
    context = object.__new__(DirectSourceRequest)
    context._api, context._connection, context._thread = viewer_api, connection, threading.get_ident()
    context._headers = dict(headers.items())
    context._environment, context._principal, context._active = environment, principal, True
    return context
