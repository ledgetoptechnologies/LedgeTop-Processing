"""Bounded live engine metadata over the existing private worker HTTPS boundary.

No arbitrary native route, engine credential, job submission or new listener.
Busy workers remain discoverable; this does not acquire the Driver/NodeGate lock.
"""
from concurrent.futures import ThreadPoolExecutor
import json
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener
from urllib.error import HTTPError
import uuid

from . import reference_protocol as p
from .reference_api import Response
from .options import (MAX_INFO, MAX_OPTIONS, _canonical, _contains_secret, _sha,
                      _validate_info, _validate_options, catalog_from_snapshots)
from .provider import _viewer_type, REQUIRED_VIEWER_OUTPUTS
from .reference_result_proxy import _origin, _NoRedirects

PATH = '/ltds/worker/v1/catalog'
PROTOCOL = 'ltds-worker-engine-catalog-v1'
MAX_RESPONSE = MAX_INFO + MAX_OPTIONS + 8192
IDENTITY = ('nodeId', 'workerId', 'workerInstanceId', 'authorityId', 'engineId', 'gateGeneration')


def validate_metadata(info, options):
    projected = _validate_info(info)
    if 'maxImages' not in info or 'taskQueueCount' not in info:
        raise ValueError('complete engine capacity required')
    maximum, queued = info['maxImages'], info['taskQueueCount']
    if maximum is not None: p._integer(maximum, 'maximum images')
    p._integer(queued, 'engine queue', allow_zero=True)
    options = _validate_options(options)
    if len(_canonical(info)) > MAX_INFO or len(_canonical(options)) > MAX_OPTIONS:
        raise ValueError('engine metadata exceeds bounds')
    return dict(projected, maxImages=maximum, taskQueueCount=queued), options


class WorkerCatalogAPI:
    def __init__(self, inner, worker, *, worker_instance_id):
        # Reuse the existing fixed result-reader principal and TLS socket. Never
        # expose this under Viewer/claim credentials or a public native route.
        if not callable(getattr(inner, '_authenticate', None)):
            raise ValueError('existing worker result authentication required')
        p._slug(worker_instance_id, 'worker instance')
        self.inner, self.worker, self.instance = inner, worker, worker_instance_id

    def handle(self, method, path, headers, body=b'', *, protected_transport=False):
        if path != PATH:
            return self.inner.handle(method, path, headers, body, protected_transport=protected_transport)
        if protected_transport is not True: return Response(503, {'error':'protected_transport_required'})
        reader = self.inner._authenticate(headers)
        if reader is None: return Response(401, {'error':'unauthorized'})
        if (reader['node_id'], reader['worker_id']) != (self.worker.node_id, self.worker.worker_id):
            return Response(403, {'error':'worker_identity_mismatch'})
        if method != 'POST': return Response(404, {'error':'not_found'})
        try:
            if type(body) is not bytes or len(body) > 4096: raise ValueError()
            request = p.decode_submission(body, max_bytes=4096)
            p._closed(request, ('schemaVersion','nonce'), 'catalog request')
            if type(request['schemaVersion']) is not int or request['schemaVersion'] != 1: raise ValueError()
            p._uuid(request['nonce'], 'catalog nonce')
        except (ValueError, TypeError): return Response(400, {'error':'invalid_catalog_request'})
        try:
            worker = self.worker
            worker._verify_local_engine()
            raw_info = worker.engine.read('/info', limit=MAX_INFO)
            raw_options = worker.engine.read('/options', limit=MAX_OPTIONS)
            info, options = validate_metadata(raw_info, raw_options)
            if info['engineVersion'] != worker.version: raise ValueError('engine pin changed')
            for secret in (worker.engine.token, *self.inner._readers):
                if _contains_secret([raw_info, options], secret): raise ValueError('private credential reflected')
            worker._verify_local_engine()
            value = dict(schemaVersion=1, protocol=PROTOCOL, nonce=request['nonce'],
                nodeId=worker.node_id, workerId=worker.worker_id, workerInstanceId=self.instance,
                authorityId=worker.authority_id, engineId=worker.engine_id,
                gateGeneration=worker.gate_generation, info=info, options=options)
            if len(_canonical(value)) > MAX_RESPONSE: raise ValueError('catalog exceeds bounds')
            return Response(200, value)
        except Exception:
            return Response(503, {'error':'engine_catalog_unavailable'})


class WorkerCatalogClient:
    """Fresh fixed-worker snapshots, using the existing result origin/auth/CA."""
    def __init__(self, workers, *, node_ids, authority_id, ssl_context, recovery_bindings=None, opener=None):
        if set(workers) != set(node_ids) or not 1 <= len(node_ids) <= 8:
            raise ValueError('complete worker catalog inventory required')
        self.workers = {node:dict(value, resultOrigin=_origin(value['resultOrigin'])) for node,value in workers.items()}
        self.node_ids, self.authority = tuple(node_ids), authority_id
        self.bindings = recovery_bindings or {}
        self.opener = opener or build_opener(ProxyHandler({}), HTTPSHandler(context=ssl_context), _NoRedirects())

    def _read(self, node):
        configured = self.workers[node]
        nonce = str(uuid.uuid4())
        url = configured['resultOrigin'] + PATH
        message = Request(url, data=p.canonical_json(dict(schemaVersion=1, nonce=nonce)),
            headers={'Authorization':'Bearer '+configured['resultBearer'], 'Content-Type':'application/json'}, method='POST')
        with self.opener.open(message, timeout=10) as response:
            raw = response.read(MAX_RESPONSE + 1)
            if (response.status != 200 or response.geturl() != url or len(raw) > MAX_RESPONSE or
                    response.headers.get_content_type() != 'application/json' or
                    response.headers.get('Content-Encoding') not in (None, 'identity')):
                raise ValueError('bounded worker catalog response required')
        value = p.decode_submission(raw, max_bytes=MAX_RESPONSE)
        p._closed(value, ('schemaVersion','protocol','nonce',*IDENTITY,'info','options'), 'worker catalog')
        if (type(value['schemaVersion']) is not int or value['schemaVersion'] != 1 or
                value['protocol'] != PROTOCOL or value['nonce'] != nonce or value['nodeId'] != node or
                value['workerId'] != configured['workerId'] or value['workerInstanceId'] != configured['instanceId'] or
                value['authorityId'] != self.authority):
            raise ValueError('worker catalog identity changed')
        p._slug(value['engineId'], 'engine identity')
        p._integer(value['gateGeneration'], 'gate generation')
        binding = self.bindings.get(node)
        if binding is not None and (value['engineId'],value['gateGeneration']) != (binding.engine_id,binding.gate_generation):
            raise ValueError('worker catalog physical binding changed')
        info, options = validate_metadata(value['info'], value['options'])
        if _contains_secret(value, configured['resultBearer']): raise ValueError('catalog reflected credential')
        return info, options

    def _snapshot(self, node):
        unavailable = dict(role=node, configured=True, available=False, options=[], optionsSha256=None,
            capabilitiesSha256=None, version=None, engine=None, engineVersion=None, reason='discovery_failed')
        capacity = dict(ready=False, maxImages=None, taskQueueCount=1)
        try:
            info, options = self._read(node)
            projected = _validate_info(info)
            option_hash = _sha(sorted(options, key=lambda item:item['name']))
            snapshot = dict(unavailable, **projected, available=True, reason=None, options=options,
                optionsSha256=option_hash, capabilitiesSha256=_sha(dict(projected, optionsSha256=option_hash)))
            capacity = dict(ready=_viewer_type(info) is not None and REQUIRED_VIEWER_OUTPUTS <=
                {item['name'] for item in options}, maxImages=info['maxImages'], taskQueueCount=info['taskQueueCount'])
            return snapshot, capacity
        except HTTPError as error:
            error.close()
            return unavailable, capacity
        except Exception:
            return unavailable, capacity

    def discover(self):
        with ThreadPoolExecutor(max_workers=len(self.node_ids)) as executor:
            values = list(executor.map(self._snapshot, self.node_ids))
        return catalog_from_snapshots([value[0] for value in values]), {
            node:value[1] for node,value in zip(self.node_ids,values)}
