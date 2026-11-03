"""Unmounted test candidate for principal-scoped prepared submissions.

No listener mounts this adapter. A future integrator must supply protected
transport, a complete legacy credential inventory, authenticated registry and
coherent server-owned capabilities; never derive policy or identity from JSON.
"""
import copy
import hmac
import re
from . import reference_protocol as p
from .maintenance import Conflict
from .reference_store import JCSRuntimeUnavailable
from .reference_api import Response

PREFIX = '/ltds/prepared/v2'


def _token(token):
    if type(token) is not str or not 16 <= len(token) <= 4096 or not all(33 <= ord(c) <= 126 for c in token):
        raise ValueError('invalid credential configuration')


class PreparedSubmissionAPI:
    def __init__(self, store, *, credentials, legacy_bearer_tokens, policies, jcs_policies=None):
        # Sequence rather than dict ensures duplicate credentials cannot be hidden.
        if type(credentials) not in (tuple, list) or not credentials or type(legacy_bearer_tokens) not in (tuple, list):
            raise ValueError('explicit credential inventories required')
        legacy = tuple(legacy_bearer_tokens)
        for token in legacy: _token(token)
        if len(set(legacy)) != len(legacy): raise ValueError('ambiguous legacy credentials')
        configured = []
        for entry in credentials:
            p._closed(entry, ('token','principalId','environment'), 'credential')
            token, principal, env = entry['token'], entry['principalId'], entry['environment']
            _token(token); p._slug(principal, 'principal'); p._environment(env)
            if any(hmac.compare_digest(token, prev[0]) for prev in configured) or any(hmac.compare_digest(token, old) for old in legacy):
                raise ValueError('ambiguous credential inventories')
            configured.append((token, principal, env))
        p._closed(policies, ('production','staging'), 'server policies')
        for policy in policies.values():
            p._closed(policy, ('capabilityFingerprint','optionsFingerprint','allowedOdmOptions','availableEngines'), 'server policy')
            p._hash(policy['capabilityFingerprint'], 'capability fingerprint')
            p._hash(policy['optionsFingerprint'], 'options fingerprint')
            opts, engines = policy['allowedOdmOptions'], policy['availableEngines']
            if (type(opts) not in (list,tuple) or any(type(x) is not str or not re.fullmatch(r'[a-z][a-z0-9-]{0,79}', x) for x in opts)
                or len(set(opts)) != len(opts) or type(engines) not in (list,tuple)
                or any(type(x) is not str or x not in ('odm','treedar') for x in engines) or len(set(engines)) != len(engines)):
                raise ValueError('invalid server policy')
        if jcs_policies is not None and not store.jcs_runtime_enabled:
            raise ValueError('JCS API requires capable store')
        if jcs_policies is not None:
            from .prepared_envelope_jcs_candidate import _validate_definition
            p._closed(jcs_policies, ('production','staging'), 'JCS server policies')
            for policy in jcs_policies.values():
                p._closed(policy, ('capabilityFingerprint','optionsFingerprint','availableEngines','optionDefinitions'), 'JCS server policy')
                p._hash(policy['capabilityFingerprint'], 'capability fingerprint')
                p._hash(policy['optionsFingerprint'], 'options fingerprint')
                engines, definitions = policy['availableEngines'], policy['optionDefinitions']
                if (type(engines) not in (list,tuple) or any(type(e) is not str or e not in ('odm','treedar') for e in engines)
                    or len(set(engines)) != len(engines) or type(definitions) is not dict):
                    raise ValueError('invalid JCS server policy')
                for name, definition in definitions.items():
                    if type(name) is not str or not re.fullmatch(r'[a-z][a-z0-9-]{0,79}',name): raise ValueError('invalid option')
                    _validate_definition(definition)
        self._jcs_policies = copy.deepcopy(jcs_policies)
        self.store = store
        self._credentials = tuple(configured)
        self._policies = copy.deepcopy(policies)

    def _authenticate(self, headers):
        if not hasattr(headers, 'items'): return None
        entries = [v for k,v in headers.items() if type(k) is str and k.lower() == 'authorization']
        if len(entries) != 1 or type(entries[0]) is not str or not entries[0].startswith('Bearer '): return None
        token = entries[0][7:]
        try: _token(token)
        except ValueError: return None
        match = None
        for expected, principal, env in self._credentials:
            if hmac.compare_digest(token, expected): match = (principal, env)
        return match

    def handle(self, method, path, headers, body=b'', *, protected_transport=False):
        if protected_transport is not True: return Response(503, {'error':'protected_transport_required'})
        identity = self._authenticate(headers)
        if identity is None: return Response(401, {'error':'unauthorized'})
        principal, environment = identity
        try:
            if method == 'POST' and path == '/ltds/prepared/v3/submissions' and self._jcs_policies is not None:
                job, created = self.store.prepare_viewer_envelope_jcs_v3(body, authenticated_principal=principal,
                    authenticated_environment=environment, trusted_policy=self._jcs_policies[environment])
                return Response(201 if created else 200, job)
            view = re.fullmatch(r'/ltds/prepared/v3/submissions/([0-9a-f-]{36})/(status|result-manifest|accepted-copies)', path) if type(path) is str else None
            if method == 'GET' and view and self._jcs_policies is not None:
                if body: raise ValueError('GET body forbidden')
                outcome=self.store.read_viewer_v3_job(view[1], authenticated_principal=principal,
                    authenticated_environment=environment, view=view[2])
                return Response(200,outcome) if outcome is not None else Response(404,{'error':'not_found'})
            activation = re.fullmatch(r'/ltds/prepared/v3/submissions/([0-9a-f-]{36})/activation', path) if type(path) is str else None
            if method == 'POST' and activation and self._jcs_policies is not None:
                request = p.decode_submission(body)
                outcome = self.store.activate_viewer_envelope_jcs_v3(activation[1], request,
                    authenticated_principal=principal, authenticated_environment=environment,
                    trusted_policy=self._jcs_policies[environment])
                return Response(200, outcome) if outcome is not None else Response(404, {'error':'not_found'})
            v3 = re.fullmatch(r'/ltds/prepared/v3/submissions/([0-9a-f-]{36})', path) if type(path) is str else None
            if method == 'GET' and v3 and self._jcs_policies is not None:
                if body: raise ValueError('GET body forbidden')
                result = self.store.find_viewer_envelope_v2(v3[1], authenticated_principal=principal,
                    authenticated_environment=environment, expected_protocol='ltds-viewer-prepared-envelope-jcs-v3')
                return Response(200, result) if result is not None else Response(404, {'error':'not_found'})
            if method == 'POST' and path == PREFIX + '/submissions':
                envelope = p.decode_submission(body)
                job, created = self.store.prepare_viewer_envelope_v2(envelope, authenticated_principal=principal,
                    authenticated_environment=environment, trusted_policy=self._policies[environment])
                return Response(201 if created else 200, job)
            match = re.fullmatch(re.escape(PREFIX) + r'/submissions/([0-9a-f-]{36})', path) if type(path) is str else None
            if method == 'GET' and match:
                if body: raise ValueError('GET body forbidden')
                result = self.store.find_viewer_envelope_v2(match[1], authenticated_principal=principal,
                    authenticated_environment=environment)
                return Response(200, result) if result is not None else Response(404, {'error':'not_found'})
            return Response(404, {'error':'not_found'})
        except JCSRuntimeUnavailable: return Response(503, {'error':'prepared_runtime_unavailable'})
        except Conflict: return Response(409, {'error':'prepared_conflict'})
        except (TypeError, ValueError): return Response(400, {'error':'invalid_request'})
        except Exception: return Response(500, {'error':'internal_error'})
