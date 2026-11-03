"""Experimental mTLS streaming writer; no runtime activation or read proxy.

The caller must supply a certificate-verifying SSL context, a trusted fixed
endpoint, and an authenticated job-to-engine binding. Lost acknowledgments are
never retried here: the node journal and spool must be reconciled first.
"""
import hashlib
import http.client
import json
import os
from pathlib import Path

from .viewer_service import EngineUnavailable


class GuardedNativeClient:
    node_guarded = True

    def __init__(self, host, port, context, bearer, *, timeout=120):
        if not context.check_hostname or context.verify_mode == 0:
            raise ValueError('certificate and hostname verification required')
        self.host, self.port, self.context, self.bearer, self.timeout = host, port, context, bearer, timeout

    def _connection(self):
        return http.client.HTTPSConnection(self.host, self.port, context=self.context, timeout=self.timeout)

    def _json(self, route, value):
        conn = self._connection()
        try:
            body = json.dumps(value, allow_nan=False, separators=(',', ':')).encode()
            conn.request('POST', '/v1/' + route, body, {'Authorization': 'Bearer ' + self.bearer})
            response = conn.getresponse()
            result = json.loads(response.read(65537))
            if response.status != 200 or type(result) is not dict:
                raise EngineUnavailable('guard operation held')
            return result
        except (OSError, ValueError, http.client.HTTPException):
            raise EngineUnavailable('guard operation unavailable') from None
        finally:
            conn.close()

    def initialize(self, task):
        self._json('initialize', {'task': {k: task[k] for k in ('name', 'engine_uuid', 'outputs', 'options')}})

    def action(self, action, identifier):
        if action != 'commit': raise EngineUnavailable('unsupported guarded operation')
        return self._json('commit', {'task': identifier})

    def inspect_mutation(self, claim):
        """Inspect only; returned evidence never authorizes replay or resume."""
        value = self._json('inspect-mutation', {'claim': claim})
        if (set(value) != {'schema', 'claim', 'state', 'ackDigest', 'held'}
                or value['schema'] != 'ledgetop-node-mutation-proof-v1'
                or value['claim'] != claim or value['held'] is not True
                or value['state'] not in ('pending', 'done')
                or (value['state'] == 'pending' and value['ackDigest'] is not None)
                or (value['state'] == 'done' and (type(value['ackDigest']) is not str
                    or len(value['ackDigest']) != 64
                    or any(c not in '0123456789abcdef' for c in value['ackDigest'])))):
            raise EngineUnavailable('guard proof unavailable')
        return value

    def inspect_bound_mutation(self, envelope, operation):
        """Return a validated held-only observation from the authenticated peer."""
        value = self._json('inspect-bound-mutation', {'jobClaim': envelope, 'operation': operation})
        if (set(value) != {'schema', 'claim', 'operation', 'state', 'ackDigest', 'held'}
                or value['schema'] != 'ledgetop-bound-node-observation-v1'
                or value['claim'] != envelope.get('claim') or value['operation'] != operation
                or value['held'] is not True or value['state'] not in ('pending', 'done')
                or (value['state'] == 'pending' and value['ackDigest'] is not None)
                or (value['state'] == 'done' and (type(value['ackDigest']) is not str
                    or len(value['ackDigest']) != 64
                    or any(c not in '0123456789abcdef' for c in value['ackDigest'])))):
            raise EngineUnavailable('guard proof unavailable')
        return value

    def upload(self, identifier, item):
        path = Path(item['path'])
        conn = self._connection()
        try:
            # No body buffering: bytes are read and sent in bounded chunks.
            if path.is_symlink() or not path.is_file() or path.stat().st_size != item['bytes']:
                raise ValueError('input changed')
            conn.putrequest('POST', '/v1/upload-stream')
            for key, value in {'Authorization': 'Bearer ' + self.bearer, 'Content-Length': str(item['bytes']),
                               'X-Task-Id': identifier, 'X-File-Name': item['name'],
                               'X-File-Sha256': item['sha256']}.items():
                conn.putheader(key, value)
            conn.endheaders()
            digest = hashlib.sha256()
            sent = 0
            with path.open('rb') as source:
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk: break
                    sent += len(chunk)
                    if sent > item['bytes']: raise ValueError('input changed')
                    digest.update(chunk)
                    conn.send(chunk)
            if sent != item['bytes'] or digest.hexdigest() != item['sha256']:
                raise ValueError('input changed')
            response = conn.getresponse()
            value = json.loads(response.read(65537))
            if response.status != 200 or value != {'accepted': True}:
                raise EngineUnavailable('guard operation held')
        except (OSError, ValueError, http.client.HTTPException):
            raise EngineUnavailable('guard operation unavailable') from None
        finally:
            conn.close()
