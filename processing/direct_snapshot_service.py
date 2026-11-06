"""Opt-in private coordinator snapshot listener owner; not host wired."""
from .direct_snapshot_http import create_snapshot_server
from .direct_worker_service_v2 import ListenerOwner

def create_snapshot_owner(*,enabled=False,host=None,port=None,ssl_context=None,client_scopes=None,backend=None):
    """Only trusted local enrollment supplies TLS, scope and backend objects."""
    if enabled is False:return None
    if enabled is not True:raise ValueError('explicit snapshot enable required')
    if backend is None:raise ValueError('snapshot backend required')
    server=create_snapshot_server(host,port,enabled=True,ssl_context=ssl_context,
                                  client_scopes=client_scopes,backend=backend)
    return ListenerOwner(server)
