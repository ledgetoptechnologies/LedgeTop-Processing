"""Native Viewer facade service and cooperative engine dispatcher.

Only fixed configured engines are contacted. Mutation intent is durable before
HTTP; uncertain mutations hold the applicable slot instead of replaying a job.
Completed engine tasks and downloaded archives remain until operator cleanup.
"""

import http.client
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import stat
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, build_opener, ProxyHandler
import uuid
import zipfile
from urllib.parse import urlsplit

from .maintenance import Conflict
from .odx_smoke_worker import Client, SmokeError
from .options import catalog, discover_node, _NoRedirect
from .policy import plan
from .prototype import VIEWER_PROFILE
from .provider import probe_node
from .viewer_tasks import TaskStore, sync_directory
from .viewer_limits import MAX_UPLOAD_BYTES

# A silent node holds overflow this long, matching the readiness cache TTL.
UNANSWERED_NODE_SECONDS = 30


class EngineUnavailable(OSError):
    pass


class _DispatchInterrupted(Exception):
    """A cooperative stop at a boundary, with all partial work retained."""


class _PinnedLoopbackClient(Client):
    """A private mutation client with a fixed validated connection target."""
    def __init__(self, origin):
        super().__init__(origin)
        self.__target = urlsplit(self.origin)

    def request(self, *args, **kwargs):
        if '_fixed_target' in kwargs:
            raise SmokeError('fixed node target cannot be overridden')
        return super().request(*args, _fixed_target=self.__target, **kwargs)

    def fixed_origin(self):
        # Use the same private parsed target as the mutation socket.  Never
        # derive a read URL from mutable public engine or client attributes.
        return self.__target.scheme + '://' + self.__target.netloc


class NativeEngine:
    def __init__(self, origin, token=''):
        self.origin, self.token = origin, token
        self.client = Client(origin)

    def pin_guarded_local(self, expected_origin):
        """Pin a previously checked loopback origin for mutation calls only."""
        expected = urlsplit(expected_origin.rstrip('/'))
        if (self.origin != expected_origin or self.client.origin != expected_origin.rstrip('/')
                or self.client.parsed != expected
                or expected.scheme != 'http' or expected.hostname not in ('127.0.0.1', '::1')
                or expected.port is None or expected.username or expected.password
                or expected.path not in ('', '/') or expected.query or expected.fragment):
            raise ValueError('native engine target differs')
        if hasattr(self, '_guarded_client'):
            # A failed listener startup can be retried without weakening the
            # previously pinned target.
            self._mutation_client()
            return
        self._guarded_origin = expected_origin
        self._guarded_client = _PinnedLoopbackClient(expected_origin)
        self.client = self._guarded_client

    def _mutation_client(self):
        pinned = getattr(self, '_guarded_client', None)
        if pinned is not None:
            if (self.client is not pinned or self.origin != self._guarded_origin
                    or pinned.origin != self._guarded_origin.rstrip('/')
                    or pinned.parsed != urlsplit(self._guarded_origin.rstrip('/'))):
                raise SmokeError('guarded native target changed')
            # The captured client uses its pinned endpoint even if its parsed
            # attribute changes concurrently after this check.
            return pinned
        return self.client

    def route(self, route, **query):
        if self.token:
            query['token'] = self.token
        return route + ('?' + urlencode(query) if query else '')

    def read(self, route, *, missing=False, limit=4 * 1024 ** 2, **query):
        try:
            pinned = getattr(self, '_guarded_client', None)
            origin = self._mutation_client().fixed_origin() if pinned is not None else self.origin
            request = Request(origin + self.route(route, **query))
            with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=5) as response:
                if 'json' not in response.headers.get('Content-Type', ''):
                    raise ValueError()
                raw = response.read(limit + 1)
                if len(raw) > limit:
                    raise ValueError()
                value = json.loads(raw)
                if isinstance(value, dict) and 'error' in value:
                    raise ValueError()
                return value
        except HTTPError as error:
            code = error.code
            error.close()
            if missing and code == 404:
                return None
            raise EngineUnavailable('engine_read_unavailable') from None
        except (OSError, ValueError, http.client.HTTPException):
            raise EngineUnavailable('engine_read_unavailable') from None

    def initialize(self, task):
        fields = {'name': task['name'], 'options': json.dumps(task['options'])}
        if task['outputs'] is not None:
            fields['outputs'] = json.dumps(task['outputs'])
        result = self._mutation_client().json('POST', self.route('/task/new/init'), fields=fields,
                                  headers={'set-uuid': task['engine_uuid']})
        if not isinstance(result, dict) or result.get('uuid') != task['engine_uuid']:
            raise SmokeError('engine initialization identity uncertain')

    def upload(self, identifier, item):
        path = Path(item['path'])
        if path.is_symlink() or not path.is_file() or path.stat().st_size != item['bytes']:
            raise SmokeError('input file changed')
        boundary = 'processing-' + uuid.uuid4().hex
        prefix = (f'--{boundary}\r\nContent-Disposition: form-data; name="images"; '
                  f'filename="{item["name"]}"\r\nContent-Type: application/octet-stream\r\n\r\n').encode('ascii')
        raw = self._mutation_client().request('POST', self.route('/task/new/upload/' + identifier),
            headers={'Content-Type': 'multipart/form-data; boundary=' + boundary},
            file=path, prefix=prefix, suffix=f'\r\n--{boundary}--\r\n'.encode(),
            expected_hash=item['sha256'], timeout=120)
        result = json.loads(raw)
        if not isinstance(result, dict) or result.get('success') is not True or 'error' in result:
            raise SmokeError('engine upload acknowledgment uncertain')

    def action(self, action, identifier):
        if action == 'commit':
            route, fields = '/task/new/commit/' + identifier, None
            result = self._mutation_client().json('POST', self.route(route))
        else:
            route = '/task/' + action
            raw = self._mutation_client().request('POST', self.route(route), body=urlencode({'uuid': identifier}).encode(),
                                      headers={'Content-Type': 'application/x-www-form-urlencoded'})
            result = json.loads(raw)
        if (not isinstance(result, dict) or 'error' in result or result.get('success') is False
                or ('uuid' in result and result['uuid'] != identifier)):
            raise SmokeError('engine mutation acknowledgment uncertain')
        return result

    def download(self, identifier):
        pinned = getattr(self, '_guarded_client', None)
        origin = self._mutation_client().fixed_origin() if pinned is not None else self.origin
        return build_opener(ProxyHandler({}), _NoRedirect()).open(
            origin + self.route('/task/' + identifier + '/download/all.zip'), timeout=60)


class ViewerService:
    def __init__(self, queue, config, *, store=None, engines=None, probe=probe_node, guard_probe=None,
                 guarded_engine_factory=None):
        self.queue, self.config = queue, config
        self._parallel_dispatch = getattr(config, 'viewer_parallel_dispatch', False) is True
        if self._parallel_dispatch and not queue.per_node_slots:
            raise ValueError('parallel dispatch requires per-node slots')
        if self._parallel_dispatch != queue.viewer_outbound_parallel:
            raise ValueError('Viewer service and Queue outbound modes differ')
        self.store = store or TaskStore(queue, Path(config.prototype_state_dir) / 'viewer-tasks',
                                       config.viewer_max_input_bytes)
        self.inventory = getattr(config, 'node_inventory', ())
        if self.inventory and (not queue.per_node_slots or tuple(n['id'] for n in self.inventory) != queue.node_ids):
            raise ValueError('inventory requires matching ordered per-node queue')
        if (self.inventory and queue.node_photo_limits is not None
                and queue.node_photo_limits != {n['id']: n['maxPhotos'] for n in self.inventory}):
            raise ValueError('inventory photo limits differ from queue')
        self.engines = engines or ({n['id']: NativeEngine(n['origin'], n['token']) for n in self.inventory if 'odm' in n['capabilities']}
                                  if self.inventory else {'cluster': NativeEngine(config.local_node_url, config.local_node_token),
                                  'main': NativeEngine(config.main_node_url, config.main_node_token)})
        self.probe = probe
        # A guard-aware integration must supply its authenticated node-local
        # readiness reader. Merely enabling it without the reader is unsafe.
        require_guard = getattr(config, 'viewer_require_guard', False)
        if type(require_guard) is not bool:
            raise ValueError('guard requirement must be boolean')
        self._require_guard = require_guard
        if self._require_guard and not callable(guard_probe):
            raise ValueError('guard readiness reader required')
        if self._require_guard and not callable(guarded_engine_factory):
            raise ValueError('guarded task client factory required')
        if guarded_engine_factory is not None and (not self._require_guard or not callable(guarded_engine_factory)):
            raise ValueError('guarded task clients require guard readiness and a callable factory')
        self._guarded_engine_factory = guarded_engine_factory
        self._guard_probe = guard_probe
        self._unanswered = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None
        self._backoff = {}
        self._downloads = {}
        # Prototype only: absent configuration keeps the deployed synchronous
        # path. A node's in-flight transfer remains in its durable queue slot.
        self._dispatch_threads = {}

    def _guard_ready(self, node_id):
        if not self._require_guard:
            return True
        try:
            # Missing, malformed and unavailable responses all fail closed.
            return self._guard_probe(node_id) is True
        except Exception:
            return False

    def _task_engine(self, task):
        """Resolve an optional guarded client from immutable task identity.

        The caller-provided factory must recover the same signed claim after
        restart; the node verifies it against the durable binding. This opt-in
        adapter does not provision keys or establish node ingress isolation.
        """
        if self._guarded_engine_factory is None:
            if self._require_guard:
                raise EngineUnavailable('guarded task client unavailable')
            engine = self.engines[task['node_id']]
            if task.get('node_guarded') == 1 and getattr(engine, 'node_guarded', False) is not True:
                raise EngineUnavailable('guarded task client unavailable')
            return engine
        from .guarded_native_client import GuardedNativeClient
        from . import reference_protocol as protocol
        request = {key: task[key] for key in ('name', 'engine_uuid', 'outputs', 'options')}
        expected = {'viewerJobId': task['job_id'], 'viewerGeneration': task['generation'],
                    'engineTaskId': task['engine_uuid'], 'nodeId': task['node_id'],
                    'environment': task['environment'], 'requestSha256': protocol.digest(request)}
        try:
            # Pass only a detached copy. Neither task metadata nor a mutable
            # factory argument is accepted as proof of the returned binding.
            client = self._guarded_engine_factory(json.loads(json.dumps(expected)))
            claim = client.job_claim['claim']
            if (not isinstance(client, GuardedNativeClient) or client.operation_signer is None
                    or type(claim) is not dict
                    or any(type(claim.get(k)) is not type(v) or claim[k] != v for k, v in expected.items())):
                raise ValueError('guarded client mismatch')
            return client
        except Exception:
            raise EngineUnavailable('guarded task client unavailable') from None

    def _catalog(self):
        if self.inventory:
            # A partial fleet may admit a task based on the healthy nodes' exact
            # common definitions. Its saved evidence is checked again against
            # the selected node before any claim or upstream mutation.
            return catalog([(n['id'], n['origin'], n['token']) for n in self.inventory if 'odm' in n['capabilities']], inventory=True, available_only=True)
        return catalog([('local', self.config.local_node_url, self.config.local_node_token),
                        ('main', self.config.main_node_url, self.config.main_node_token)])

    def options(self):
        current = self._catalog()
        if not current['ready']:
            raise Conflict('engine catalog unavailable')
        return current['commonOptions']

    def capabilities(self):
        # This is our own 2.x-compatible facade, not an official ClusterODM build.
        snapshot = self._catalog()
        if not snapshot['ready']:
            raise Conflict('engine catalog unavailable')
        jobs = self.queue.list()
        return {'version': '2.4.0', 'engine': 'ledgetop-processing', 'engineVersion': '0.2.0',
                'ledgetopCatalogDegraded': snapshot.get('degraded', False),
                'maxImages': None, 'maxParallelTasks': sum('odm' in n['capabilities'] for n in self.inventory) if self.inventory else (len(self.queue.node_ids) if self.queue.per_node_slots else 1), 'coordinator': True,
                'ledgetopMaxInputBytes': self.store.max_input_bytes,
                'ledgetopMaxUploadRequestBytes': MAX_UPLOAD_BYTES,
                'ledgetopReservations': self.store.reservation_stats(),
                'taskQueueCount': sum(j['status'] in ('queued', 'running', 'held') for j in jobs)}

    def _selected_options(self, node_id):
        if self.inventory:
            node = next((n for n in self.inventory if n['id'] == node_id and 'odm' in n['capabilities']), None)
            if node is None:
                raise Conflict('unknown selected node')
            snapshot = discover_node(node['id'], node['origin'], node['token'], inventory=True)
            if not snapshot['available']:
                raise Conflict('selected engine options unavailable')
            return snapshot['options']
        if node_id == 'cluster':
            role, origin, token = 'local', self.config.local_node_url, self.config.local_node_token
        elif node_id == 'main':
            role, origin, token = 'main', self.config.main_node_url, self.config.main_node_token
        else:
            raise Conflict('unknown selected node')
        snapshot = discover_node(role, origin, token)
        if not snapshot['available']:
            raise Conflict('selected engine options unavailable')
        return snapshot['options']

    @staticmethod
    def _option_values(options, definitions):
        by_name = {item['name']: item for item in definitions}
        if (type(options) is not list
                or any(type(item) is not dict or set(item) != {'name', 'value'}
                       or type(item['name']) is not str or item['name'] not in by_name for item in options)
                or len({item['name'] for item in options}) != len(options)):
            raise ValueError('option unavailable on configured engines')
        for item in options:
            spec, value = by_name[item['name']], item['value']
            kind = spec.get('type')
            finite = lambda number: ((type(number) is int and abs(number) <= 9007199254740991)
                                     or (type(number) is float and math.isfinite(number)))
            valid = ((kind in ('bool', 'boolean') and type(value) is bool)
                     or (kind in ('int', 'integer') and type(value) is int and finite(value))
                     or (kind in ('float', 'double', 'number') and finite(value))
                     or (kind in ('str', 'string', 'enum') and type(value) is str and len(value) <= 4000))
            if not valid:
                raise ValueError('invalid option value')
            domain = spec.get('domain')
            if isinstance(domain, str):
                domain = domain.strip()
                if not domain:
                    domain = None
                elif domain in ('integer', 'positive integer', 'float', 'positive float',
                                'float > 0.0', 'integer: 1 <= x <= 14'):
                    if type(value) not in (int, float) or (domain in ('integer', 'positive integer', 'integer: 1 <= x <= 14') and not float(value).is_integer()):
                        raise ValueError('option outside domain')
                    if ((domain.startswith('positive ') and value <= 0)
                            or (domain == 'float > 0.0' and value <= 0)
                            or (domain == 'integer: 1 <= x <= 14' and not 1 <= value <= 14)):
                        raise ValueError('option outside domain')
                    continue
                elif domain in ('string', 'json') and kind in ('str', 'string'):
                    if domain == 'json':
                        try:
                            json.loads(value, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
                        except (ValueError, TypeError, RecursionError):
                            raise ValueError('invalid JSON option') from None
                    continue
                else:
                    try: domain = json.loads(domain)
                    except (ValueError, TypeError): raise ValueError('invalid option domain') from None
            if kind == 'enum' and (type(domain) is not list or not domain
                                   or any(type(choice) is not str for choice in domain)):
                raise ValueError('invalid enum domain')
            if domain is None:
                continue
            if type(domain) is list:
                if not any(type(value) is type(choice) and value == choice for choice in domain):
                    raise ValueError('option outside domain')
            elif type(domain) is dict:
                if (not domain or not set(domain).issubset({'min', 'max'})
                        or any(not finite(bound) for bound in domain.values())
                        or ('min' in domain and 'max' in domain and domain['min'] > domain['max'])):
                    raise ValueError('invalid option domain')
                for key in ('min', 'max'):
                    bound = domain.get(key)
                    if bound is not None:
                        if not finite(bound) or type(value) not in (int, float):
                            raise ValueError('invalid option domain')
                        if (key == 'min' and value < bound) or (key == 'max' and value > bound):
                            raise ValueError('option outside domain')
            else:
                raise ValueError('invalid option domain')

    def _check_options(self, options, *, node_id=None, evidence=None, full_evidence=None, definitions=None):
        if node_id is not None and full_evidence is not None:
            raise Conflict('full catalog policy requires explicit task reconciliation')
        # A legacy task without saved explicit-option definitions retains the
        # conservative complete-fleet check.  Existing full-catalog evidence,
        # where present, remains integrity checked by the task store, but is
        # not treated as a requirement that node-local defaults be identical.
        if node_id is not None and evidence is None and self.inventory:
            complete = catalog([(n['id'], n['origin'], n['token']) for n in self.inventory
                                if 'odm' in n['capabilities']], inventory=True)
            if not complete['ready']:
                raise Conflict('legacy engine catalog unavailable')
            self._option_values(options, complete['commonOptions'])
            definitions = self._selected_options(node_id)
        elif definitions is None:
            definitions = self._selected_options(node_id) if node_id is not None and evidence is not None else self.options()
        allowed = {item['name'] for item in definitions}
        if (type(options) is not list or any(type(item) is not dict or type(item.get('name')) is not str
                                             or item['name'] not in allowed for item in options)):
            raise ValueError('option unavailable on configured engines')
        selected = [item for item in definitions if item['name'] in {option['name'] for option in options}]
        if evidence is not None:
            source = {item['name']: item for item in evidence}
            if (set(source) != {item['name'] for item in options}
                    or any(item.get('type') != source[item['name']].get('type') for item in selected)):
                raise Conflict('selected engine option type changed')
            self._option_values(options, evidence)
        self._option_values(options, definitions)
        return selected

    def initialize(self, identifier, name='', options=None, outputs=None, environment='production', capability=None):
        if capability is not None:
            replay = self.store.initialize(identifier, name, options, outputs, environment, capability, replay_only=True)
            if replay is not None: return replay
        if self.inventory:
            snapshot = self._catalog()
            if not snapshot['ready']:
                raise Conflict('engine catalog unavailable')
            evidence = self._check_options(options or [], definitions=snapshot['commonOptions'])
        else:
            evidence = self._check_options(options or [])
        return self.store.initialize(identifier, name, options, outputs, environment, capability,
                                     option_evidence=evidence)

    def upload(self, identifier, filename, stream, length, environment=None, capability=None):
        return self.store.upload(identifier, filename, stream, length, environment, capability)

    def upload_batch(self, identifier):
        """Fence the whole multipart request, including gaps between files."""
        return self.store._task_fence(identifier, upload=True)

    def commit(self, identifier, environment=None, capability=None):
        return self.store.commit(identifier, environment, capability)

    def info(self, identifier):
        info = self.store.info(identifier)
        try:
            task = self.store.task(identifier)
        except Conflict:
            return info
        if task['node_id']:
            # The ID is configured locally, not an endpoint or credential.
            info['ledgetopNodeId'] = task['node_id']
        return info

    def reserve(self, identifier, environment, capability):
        with self._lock:return self.store.reserve(identifier,environment,capability)

    def cancel_reserved(self, identifier, environment, capability):
        with self._lock:return self.store.cancel_reservation(identifier,environment,capability)

    def authorize(self, identifier, environment, capability=None):
        self.store.authorize_reservation(identifier,environment,capability)
        if self.store.task_environment(identifier) != environment:
            raise KeyError('unknown task')

    def output(self, identifier, line=0):
        if type(line) is not int or not 0 <= line <= 2 ** 53 - 1:
            raise ValueError('nonnegative output cursor required')
        task = self.store.task(identifier)
        # A cursor must refer to exactly one stream for the lifetime of a task.
        # Before an engine is bound there are no engine lines. Coordinator
        # messages are not part of that stream, even if stored locally.
        if not task['engine_uuid']:
            return []
        if task['node_id'] not in self.engines:
            raise EngineUnavailable('engine_output_unavailable')
        try:
            result = self._task_engine(task).read('/task/' + task['engine_uuid'] + '/output', line=line)
            if not isinstance(result, list) or any(not isinstance(x, str) for x in result):
                raise EngineUnavailable('engine_output_unavailable')
            return result
        except EngineUnavailable:
            # A caller can retry the same cursor after a 503; returning local
            # lines here would silently advance it in a different stream.
            raise EngineUnavailable('engine_output_unavailable') from None

    def _archive_identity(self, task):
        return {'version': 1, 'task': task['uuid'], 'generation': task['generation'],
                'job': task['job_id'], 'node': task['node_id'], 'engine': task['engine_uuid']}

    def _recover_metadata_link(self, path):
        """Finish unlinking one task-owned temp link after interrupted publish."""
        info=path.lstat()
        if info.st_nlink==1: return
        if (path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink!=2
                or (os.name!='nt' and info.st_uid!=os.geteuid())):
            raise Conflict('archive metadata requires inspection')
        prefix='.'+path.name+'.tmp-'; matches=[]
        for candidate in path.parent.iterdir():
            if not candidate.name.startswith(prefix) or not re.fullmatch(r'[0-9a-f]{32}',candidate.name[len(prefix):]): continue
            stat_candidate=candidate.lstat()
            if (stat_candidate.st_dev,stat_candidate.st_ino)==(info.st_dev,info.st_ino): matches.append(candidate)
        if len(matches)!=1: raise Conflict('archive metadata requires inspection')
        temporary=matches[0];current=path.lstat();linked=temporary.lstat()
        if ((current.st_dev,current.st_ino,current.st_nlink)!=(info.st_dev,info.st_ino,2)
                or (linked.st_dev,linked.st_ino)!=(info.st_dev,info.st_ino)):
            raise Conflict('archive metadata identity changed')
        temporary.unlink();sync_directory(path.parent)
        current=path.lstat()
        if (current.st_dev,current.st_ino,current.st_nlink)!=(info.st_dev,info.st_ino,1):
            raise Conflict('archive metadata identity changed')

    def _read_archive_metadata(self, path):
        try:
            if path.is_symlink(): raise ValueError()
            fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
            with os.fdopen(fd, 'rb') as stream:
                info = os.fstat(stream.fileno())
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink not in (1,2) or info.st_size > 4096
                        or (os.name != 'nt' and info.st_uid != os.geteuid())):
                    raise ValueError()
                def unique(pairs):
                    result = {}
                    for key, item in pairs:
                        if key in result: raise ValueError()
                        result[key] = item
                    return result
                value = json.loads(stream.read(4097), object_pairs_hook=unique)
            return value
        except (OSError, ValueError, TypeError, KeyError):
            raise Conflict('archive metadata requires inspection') from None

    def _publish_archive_metadata(self, path, value):
        """Publish a complete, synced marker without overwriting an existing one."""
        directory=path.parent
        temporary=directory/('.'+path.name+'.tmp-'+uuid.uuid4().hex)
        fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o600)
        original=os.fstat(fd)
        try:
            with os.fdopen(fd,'wb') as marker:
                fd=None
                marker.write(json.dumps(value,sort_keys=True,separators=(',',':')).encode('ascii'))
                marker.flush();os.fsync(marker.fileno())
            current=temporary.lstat()
            if (current.st_dev,current.st_ino)!=(original.st_dev,original.st_ino):
                raise Conflict('archive metadata identity changed')
            try: os.link(temporary,path)
            except FileExistsError: raise Conflict('archive metadata already exists') from None
            published=path.lstat()
            if (published.st_dev,published.st_ino)!=(original.st_dev,original.st_ino):
                raise Conflict('archive metadata identity changed')
            sync_directory(directory)
        finally:
            if fd is not None: os.close(fd)
            try:
                current=temporary.lstat()
                if (current.st_dev,current.st_ino)==(original.st_dev,original.st_ino):
                    temporary.unlink()
                    sync_directory(directory)
            except FileNotFoundError: pass

    def _archive_receipt(self, task, path):
        """Validate a bounded private receipt, binding it to this incarnation."""
        try:
            value = self._read_archive_metadata(path)
            expected = self._archive_identity(task)
            if (type(value) is not dict or set(value) != set(expected) | {'bytes','sha256'}
                    or any(type(value[k]) is not type(v) or value[k] != v for k, v in expected.items())
                    or type(value['bytes']) is not int or not 4 <= value['bytes'] <= self.config.viewer_max_archive_bytes
                    or type(value['sha256']) is not str or not re.fullmatch(r'[a-f0-9]{64}', value['sha256'])):
                raise ValueError()
            self._recover_metadata_link(path)
            return value
        except (OSError, ValueError, TypeError, KeyError):
            raise Conflict('archive receipt requires inspection') from None

    def _verify_archive(self, path, receipt):
        stream = None
        try:
            if path.is_symlink(): raise ValueError()
            fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
            stream = os.fdopen(fd, 'rb')
            before = os.fstat(stream.fileno())
            if (not stat.S_ISREG(before.st_mode) or before.st_size != receipt['bytes']
                    or (os.name != 'nt' and before.st_uid != os.geteuid())):
                raise ValueError()
            digest = hashlib.sha256()
            while chunk := stream.read(1024 * 1024): digest.update(chunk)
            if digest.hexdigest() != receipt['sha256'] or not zipfile.is_zipfile(stream):
                raise ValueError()
            after = os.fstat(stream.fileno()); current = path.lstat()
            fields = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
            if fields(before) != fields(after): raise ValueError()
            # Windows may expose link-change time differently through a
            # handle and pathname after hard-link publication.
            if fields(after)[:4] != fields(current)[:4]: raise ValueError()
            if os.name != 'nt' and fields(after)[4] != fields(current)[4]: raise ValueError()
            stream.seek(0)
            # Only receipt-backed, fully rehashed archives carry this optional
            # HTTP identity. It is not an acknowledgement or custody proof.
            stream.ledgetop_archive_sha256 = receipt['sha256']
            return stream
        except (OSError, ValueError):
            if stream is not None: stream.close()
            raise Conflict('archive file requires inspection') from None

    def _restore_recorded_archive(self, task, path, receipt):
        """Restore only bytes matching the existing task-bound receipt."""
        directory = Path(task['directory'])
        if path != directory / 'all.zip' or directory.resolve(strict=True) != directory:
            raise Conflict('retained archive path requires inspection')
        temporary = directory / ('.all.zip.restore-' + uuid.uuid4().hex)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
        fd = os.open(temporary, flags, 0o600)
        original = os.fstat(fd); stream = None
        try:
            with os.fdopen(fd, 'wb') as output:
                fd = None
                with self._task_engine(task).download(task['engine_uuid']) as response:
                    if getattr(response, 'status', 200) != 200:
                        raise EngineUnavailable('archive_unavailable')
                    media = response.headers.get('Content-Type', '').split(';')[0].lower()
                    if media and media not in ('application/zip','application/octet-stream','application/x-zip-compressed'):
                        raise EngineUnavailable('archive_not_ready')
                    declared = response.headers.get('Content-Length')
                    if declared is not None and (not declared.isdigit() or int(declared) != receipt['bytes']):
                        raise Conflict('archive response differs from retained receipt')
                    total = 0
                    while chunk := response.read(1024 ** 2):
                        total += len(chunk)
                        if total > receipt['bytes']:
                            raise Conflict('archive response differs from retained receipt')
                        output.write(chunk)
                    if total != receipt['bytes']:
                        raise EngineUnavailable('archive_transfer_incomplete')
                output.flush(); os.fsync(output.fileno())
            stream = self._verify_archive(temporary, receipt)
            opened = os.fstat(stream.fileno()); observed = temporary.lstat()
            if (opened.st_dev,opened.st_ino)!=(original.st_dev,original.st_ino) or (observed.st_dev,observed.st_ino)!=(original.st_dev,original.st_ino):
                raise Conflict('archive restore identity changed')
            try: os.link(temporary,path)
            except FileExistsError: raise Conflict('archive restore destination changed') from None
            linked = path.lstat()
            if (linked.st_dev,linked.st_ino)!=(opened.st_dev,opened.st_ino):
                raise Conflict('archive restore identity changed')
            sync_directory(directory)
            if os.name == 'nt':
                stream.close();stream=None
                temporary.unlink()
                stream=self._verify_archive(path,receipt)
                reopened=os.fstat(stream.fileno())
                if (reopened.st_dev,reopened.st_ino)!=(opened.st_dev,opened.st_ino):
                    raise Conflict('archive restore identity changed')
            else:
                current=temporary.lstat()
                if (current.st_dev,current.st_ino)!=(original.st_dev,original.st_ino):
                    raise Conflict('archive restore identity changed')
                temporary.unlink()
            sync_directory(directory)
            result=stream;stream=None
            return result,receipt['bytes']
        finally:
            if fd is not None: os.close(fd)
            if stream is not None: stream.close()
            try:
                current=temporary.lstat()
                if (current.st_dev,current.st_ino)==(original.st_dev,original.st_ino): temporary.unlink()
            except FileNotFoundError: pass

    def download(self, identifier):
        task = self.store.task(identifier)
        self._require_downloadable(task)
        with self._lock:
            lock = self._downloads.setdefault(identifier, threading.Lock())
        with lock:
            task = self.store.task(identifier)
            # State may have changed while waiting on the per-task lock. Check
            # again before touching either local archives or the engine.
            self._require_downloadable(task)
            path = Path(task['archive_path']) if task['archive_path'] else Path(task['directory']) / 'all.zip'
            if not task['archive_path']:
                partial = path.with_suffix('.zip.partial')
                receipt_path = path.with_suffix('.zip.receipt')
                intent_path = path.with_suffix('.zip.intent')
                if receipt_path.exists() or receipt_path.is_symlink():
                    receipt = self._archive_receipt(task, receipt_path)
                    final_exists = path.exists() or path.is_symlink()
                    partial_exists = partial.exists() or partial.is_symlink()
                    stream = None
                    try:
                        if final_exists:
                            stream = self._verify_archive(path, receipt)
                            if partial_exists and (partial.is_symlink() or not os.path.samefile(path, partial)):
                                raise Conflict('competing archive requires inspection')
                        elif partial_exists:
                            stream = self._verify_archive(partial, receipt)
                            os.link(partial, path)
                        else:
                            raise Conflict('archive receipt has no file')
                        current = path.lstat(); opened = os.fstat(stream.fileno())
                        if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
                            raise Conflict('archive identity changed during publication')
                        if partial_exists:
                            if os.name == 'nt':
                                # Windows can refuse unlink while a hard-linked
                                # file is open; revalidate the same inode.
                                identity = (opened.st_dev, opened.st_ino)
                                stream.close(); stream = None
                                partial.unlink()
                                stream = self._verify_archive(path, receipt)
                                reopened = os.fstat(stream.fileno())
                                if (reopened.st_dev, reopened.st_ino) != identity:
                                    raise Conflict('archive identity changed during publication')
                            else:
                                partial.unlink()
                        sync_directory(path.parent)
                        self.store.update(identifier, archive_path=str(path))
                        return stream, receipt['bytes']
                    except BaseException:
                        if stream is not None: stream.close()
                        raise
                if path.exists() or path.is_symlink():
                    raise Conflict('archive without receipt requires inspection')
                if intent_path.exists() or intent_path.is_symlink():
                    intent = self._read_archive_metadata(intent_path); expected = self._archive_identity(task)
                    if (type(intent) is not dict or set(intent) != set(expected)
                            or any(type(intent[k]) is not type(v) or intent[k] != v for k, v in expected.items())):
                        raise Conflict('archive intent requires inspection')
                    self._recover_metadata_link(intent_path)
                else:
                    if partial.exists() or partial.is_symlink():
                        raise Conflict('partial archive without intent requires inspection')
                    self._publish_archive_metadata(intent_path,self._archive_identity(task))
                if partial.exists() or partial.is_symlink():
                    info = partial.lstat()
                    if (partial.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                            or (os.name != 'nt' and info.st_uid != os.geteuid())):
                        raise Conflict('previous archive path requires inspection')
                    # A GET is safe to repeat; the engine's original is retained.
                    # Only this exact incomplete local transfer is replaced.
                    partial.unlink()
                total = 0
                digest = hashlib.sha256()
                with self._task_engine(task).download(task['engine_uuid']) as response:
                    if getattr(response, 'status', 200) != 200:
                        raise EngineUnavailable('archive_unavailable')
                    media_type = response.headers.get('Content-Type', '').split(';')[0].lower()
                    if media_type and media_type not in ('application/zip', 'application/octet-stream',
                                                          'application/x-zip-compressed'):
                        raise EngineUnavailable('archive_not_ready')
                    declared = response.headers.get('Content-Length')
                    if declared is not None and (not declared.isdigit() or int(declared) > self.config.viewer_max_archive_bytes):
                        raise Conflict('archive exceeds configured disk allowance')
                    with partial.open('xb') as output:
                        os.chmod(partial, 0o600)
                        while chunk := response.read(1024 ** 2):
                            total += len(chunk)
                            if total > self.config.viewer_max_archive_bytes:
                                raise Conflict('archive exceeds configured disk allowance')
                            digest.update(chunk)
                            output.write(chunk)
                        if declared is not None and total != int(declared):
                            raise EngineUnavailable('archive_transfer_incomplete')
                        output.flush()
                        os.fsync(output.fileno())
                if total < 4 or not zipfile.is_zipfile(partial):
                    raise EngineUnavailable('archive_transfer_incomplete')
                receipt = {**self._archive_identity(task), 'bytes': total, 'sha256': digest.hexdigest()}
                self._publish_archive_metadata(receipt_path,receipt)
                os.link(partial, path)
                partial.unlink()
                sync_directory(path.parent)
                self.store.update(identifier, archive_path=str(path))
                return self._verify_archive(path, receipt), receipt['bytes']

            receipt_path = path.with_suffix('.zip.receipt')
            if receipt_path.exists() or receipt_path.is_symlink():
                receipt = self._archive_receipt(task, receipt_path)
                try: path.lstat()
                except FileNotFoundError:
                    return self._restore_recorded_archive(task,path,receipt)
                return self._verify_archive(path, receipt), receipt['bytes']
            if path.is_symlink() or not path.is_file():
                raise Conflict('retained archive unavailable')
            return path.open('rb'), path.stat().st_size

    @staticmethod
    def _require_downloadable(task):
        if (task['node_guarded'] == 1 and
                task['phase'] in ('init_pending', 'upload_pending', 'commit_pending')):
            raise Conflict('guarded mutation requires reconciliation')
        if not task['job'] or task['job']['status'] != 'succeeded':
            raise Conflict('task is not complete')

    def cancel(self, identifier):
        with self._lock:
            if identifier in self._dispatch_threads:
                raise Conflict('submission in progress; inspect before cancelling')
            self.store.authorize_reservation(identifier,None,None)
            cancelled = self.store.cancel_never_started(identifier)
            if cancelled is not None:
                return cancelled
            task = self.store.task(identifier)
            job = task['job']
            if not job or job['status'] in ('queued', 'failed', 'succeeded'):
                return self.store.cancel(identifier)
            if job['status'] == 'held':
                raise Conflict('uncertain execution requires inspection')
            if task['phase'] == 'cancel_pending':
                return {'success': True}
            if task['phase'] != 'processing':
                raise Conflict('submission in progress; inspect before cancelling')
            self.store.update(identifier, phase='cancel_pending')
            try:
                self._task_engine(task).action('cancel', task['engine_uuid'])
            except Exception:
                self._hold(task, 'cancel_ack_uncertain')
                raise Conflict('cancellation acknowledgment uncertain') from None
            # Keep the execution slot until the engine reports a terminal state.
            return {'success': True}

    def remove(self, identifier):
        with self._lock:
            # Deliberately retain engine results and all local bytes. Viewer has
            # no successful-ingestion ACK; remove is also its upload-reset route.
            return self.store.remove(identifier)

    def operator(self, identifier, action, request):
        """Serialize operator state changes with Viewer engine mutations."""
        with self._lock:
            if any(job_id == identifier for job_id, _ in self._dispatch_threads.values()):
                raise Conflict('submission in progress; inspect before operator action')
            return self.queue.operator(identifier, action, request)

    def _send_claimed_task(self, identifier):
        """Run one already-claimed submission; never replay a mutation on error."""
        try:
            task = self.store.task(identifier)
            engine = self._task_engine(task)
            self._check_dispatch_boundary(task)
            # Persist the guard association with the first pending phase,
            # before issuing a mutation, so recovery is independent of the
            # replacement process's engine object or runtime configuration.
            guarded = getattr(engine, 'node_guarded', False) is True
            self.store.update(identifier, phase='init_pending', node_guarded=guarded)
            if guarded:
                checkpoint = self.store.checkpoint_guarded_dispatch(identifier, 'init',
                    {'requestSha256': hashlib.sha256(json.dumps(
                        {key: task[key] for key in ('name', 'engine_uuid', 'options', 'outputs')},
                        sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()})
            engine.initialize(task)
            if guarded:
                self.store.acknowledge_guarded_dispatch(identifier, checkpoint)
            self._check_dispatch_boundary(task)
            self.store.update(identifier, phase='initialized')
            for item in task['files']:
                self._check_dispatch_boundary(task)
                self.store.update(identifier, phase='upload_pending')
                if guarded:
                    checkpoint = self.store.checkpoint_guarded_dispatch(identifier, 'upload',
                        {key: item[key] for key in ('name', 'bytes', 'sha256')})
                engine.upload(task['engine_uuid'], item)
                if guarded:
                    self.store.acknowledge_guarded_dispatch(identifier, checkpoint)
                self._check_dispatch_boundary(task)
                self.store.update(identifier, phase='uploaded')
            self._check_dispatch_boundary(task)
            self.store.update(identifier, phase='commit_pending')
            if guarded:
                checkpoint = self.store.checkpoint_guarded_dispatch(identifier, 'commit',
                    {'engine_uuid': task['engine_uuid']})
            engine.action('commit', task['engine_uuid'])
            if guarded:
                self.store.acknowledge_guarded_dispatch(identifier, checkpoint)
            self.store.update(identifier, phase='processing')
        except _DispatchInterrupted as exc:
            self._hold(self.store.task(identifier), str(exc))
        except Exception:
            current = self.store.task(identifier)
            if current['phase'] == 'commit_pending':
                if current['node_guarded'] == 1:
                    # A guarded node may have committed without a verifiable
                    # response, leaving its durable intent unresolved. Native
                    # status alone does not acknowledge that journal entry.
                    self._hold(current, 'commit_ack_uncertain')
                else:
                    self.store.update(identifier, error='commit_ack_uncertain')
            else:
                self._hold(current, 'engine_submission_uncertain')

    def _transfer_claimed_task(self, identifier, job_id):
        """Thread entry: no tick remains to hold a failure that escapes here."""
        try:
            self._send_claimed_task(identifier)
        except Exception:
            # No traceback: a chained engine error may name a credentialed route.
            logging.getLogger(__name__).error('Viewer transfer failed; inspection required')
            try:
                self.store.hold_reconciliation_error(job_id)
            except Exception:
                pass  # The next tick holds a running job no sender owns.
        finally:
            with self._lock:
                self._dispatch_threads.pop(identifier, None)

    def _check_dispatch_boundary(self, task):
        if self._stop.is_set():
            raise _DispatchInterrupted('dispatch_interrupted_for_shutdown')
        node = self.queue.get_node(task['node_id'])
        if (node['state'] not in ('active', 'scheduled') and not
                (node['state'] == 'draining' and node.get('afterJobId') == task['job_id'])):
            raise _DispatchInterrupted('dispatch_interrupted_for_maintenance')
        if self._stop.is_set():
            raise _DispatchInterrupted('dispatch_interrupted_for_shutdown')

    def _hold(self, task, reason):
        self.store.hold(task['uuid'], reason)

    def _poll(self, task):
        identifier = task['uuid']
        now = time.monotonic()
        due, failures = self._backoff.get(identifier, (0, 0))
        if now < due:
            return
        try:
            info = self._task_engine(task).read('/task/' + task['engine_uuid'] + '/info')
            if (not isinstance(info, dict) or info.get('uuid') != task['engine_uuid']
                    or not isinstance(info.get('status'), dict)
                    or type(info['status'].get('code')) is not int
                    or info['status']['code'] not in (10, 20, 30, 40, 50)):
                raise EngineUnavailable('engine_status_unavailable')
        except EngineUnavailable:
            failures += 1
            self._backoff[identifier] = (now + min(60, 2 ** min(failures, 6)), failures)
            return
        self._backoff.pop(identifier, None)
        code = info['status']['code']
        if code in (10, 20):
            # Queued10 may mean a delayed accepted commit or an uncommitted
            # upload. Keep ownership and poll; neither case permits a replay.
            progress = info.get('progress', 0)
            if type(progress) not in (int, float) or not 0 <= progress <= 100:
                self._backoff[identifier] = (now + 2, 1)
                return
            fields = {'progress': progress}
            if code == 20:
                fields['stage'] = 'running'
            else:
                # A later queued observation must not keep advertising an old
                # processing state. Keep the same claim and node reservation;
                # this is an observation, never permission to replay work.
                fields['stage'] = 'queued'
            if code == 20 and task['phase'] == 'commit_pending':
                fields.update(phase='processing', error=None)
            self.store.update(identifier, **fields)
            return
        if code == 50:
            # A native cancellation status does not prove that the engine's
            # processes have stopped. Preserve the global slot until an
            # independent physical termination check can resolve the hold.
            self._hold(task, 'cancel_termination_unverified')
            return
        status = 'succeeded' if code == 40 else 'failed'
        result = {'engineTaskId': task['engine_uuid'], 'nodeId': task['node_id'],
                  'environment': task['environment'], 'cancelled': code == 50}
        self.store.complete_engine_task(task, status, result)

    def tick(self):
        """One bounded scheduler turn; public for deterministic synthetic tests."""
        with self._lock:
            if self._stop.is_set():
                return
            # Do not claim queued work if guarded mode lost its configured
            # task client factory. A probe alone is not a mutation boundary.
            if self._require_guard and not callable(self._guarded_engine_factory):
                return
            jobs = self.queue.list()
            split_jobs = self.store.incomplete_terminal_job_ids()
            reconciliation_errors = []
            # Older coordinators could commit the job before the Viewer row.
            # Only replay a terminal result when every persisted identity agrees.
            for job in jobs:
                if job['id'] not in split_jobs:
                    continue
                try:
                    task = self.store.task_for_job(job['id'])
                    if task['phase'] == 'complete':
                        continue
                    if task['node_guarded'] == 1 and task['phase'] in ('init_pending','upload_pending','commit_pending'):
                        raise Conflict('guarded mutation requires reconciliation')
                    expected = {'engineTaskId': task['engine_uuid'], 'nodeId': task['node_id'],
                                'environment': task['environment'], 'cancelled': False}
                    if (job['status'] == 'failed' and isinstance(job['result'], dict)
                            and job['result'].get('operatorAction') == 'resolve'):
                        self.store.settle_operator_resolution(task)
                        continue
                    if (not task['engine_uuid'] or not task['node_id'] or not task['claim_token']
                            or job['result'] != expected):
                        raise Conflict('terminal Viewer identity requires inspection')
                    self.store.complete_engine_task(task, job['status'], expected)
                except Exception as exc:
                    if not self.queue.per_node_slots:
                        raise
                    reconciliation_errors.append(exc)
            for job in jobs:
                if job['profile'] != VIEWER_PROFILE or job['status'] not in ('running', 'held'):
                    continue
                if job['status'] == 'held' and job['heldReason'] != 'coordinator_restart':
                    continue
                try:
                    task = self.store.task_for_job(job['id'])
                    if task['uuid'] in self._dispatch_threads:
                        continue
                    if task['node_guarded'] == 1 and task['phase'] in ('init_pending', 'upload_pending', 'commit_pending'):
                        self._hold(task, 'guard_mutation_uncertain')
                        continue
                    if (task['phase'] in ('processing', 'commit_pending', 'cancel_pending')
                            and task['engine_uuid'] and task['node_id'] and task['claim_token']):
                        self._poll(task)
                    else:
                        self._hold(task, 'submission_interrupted')
                except Exception as exc:
                    if not self.queue.per_node_slots:
                        raise
                    reconciliation_errors.append(exc)
                    if job['status'] == 'running':
                        try:
                            current = self.queue.get(job['id'])
                            if current and current['status'] == 'running':
                                self.store.hold_reconciliation_error(job['id'])
                        except Exception as hold_error:
                            reconciliation_errors.append(hold_error)
            if reconciliation_errors:
                raise Conflict('Viewer reconciliation requires inspection') from reconciliation_errors[0]
            jobs = self.queue.list()
            if not self.queue.per_node_slots and any(j['status'] in ('running', 'held') for j in jobs):
                return
            queued = sorted((j for j in jobs if j['status'] == 'queued'),
                            key=lambda j: (j['environment'] != 'production', jobs.index(j)))
            if not queued or (not self.inventory and queued[0]['profile'] != VIEWER_PROFILE):
                return
            # Queue-owned fence: each node's answer is rechecked below and at claim.
            if queued[0]['environment'] == 'staging' and not any(
                    self.queue.staging_may_claim(node) for node in self.queue.node_ids):
                return
            first_viewer = next((job for job in queued if job['profile'] == VIEWER_PROFILE
                                 and job['environment'] == queued[0]['environment']), None)
            if first_viewer is None and not (self.inventory and any(job['profile'] == VIEWER_PROFILE for job in queued)):
                return
            task = self.store.task_for_job(first_viewer['id']) if first_viewer else None
            nodes = {}
            if self.inventory:
                occupied = {j['nodeId'] for j in jobs if j['status'] in ('running', 'held')}
                if any(node not in self.queue.node_ids for node in occupied):
                    return
                available = []
                for spec in self.inventory:
                    if 'odm' not in spec['capabilities'] or spec['id'] in occupied:
                        self._unanswered.pop(spec['id'], None)
                        continue
                    if not self._guard_ready(spec['id']):
                        continue
                    if self.queue.get_node(spec['id'])['state'] != 'active':
                        continue
                    report = self.probe(spec['id'], spec['origin'], spec['token'])
                    if not isinstance(report, dict) or 'maxImages' not in report:
                        # Silence is not busy: hold overflow past this node for
                        # a bounded time, then treat it as unavailable.
                        first = self._unanswered.setdefault(spec['id'], time.monotonic())
                        if time.monotonic() - first < UNANSWERED_NODE_SECONDS:
                            available.append((spec, None, False))
                        continue
                    self._unanswered.pop(spec['id'], None)
                    limit = report.get('maxImages')
                    if (report.get('ready') is not True or type(report.get('taskQueueCount')) is not int
                            or report['taskQueueCount'] != 0
                            or (limit is not None and (type(limit) is not int or limit < 1))):
                        continue
                    available.append((spec, limit, True))
                node_id = None
                held = False
                top_environment = queued[0]['environment']
                for candidate in queued:
                    if candidate['profile'] != VIEWER_PROFILE:
                        continue
                    staging = candidate['environment'] == 'staging'
                    try:
                        current = self.store.task_for_job(candidate['id'])
                    except Conflict:
                        # Corrupt staging stops only staging behind production.
                        if candidate['environment'] == top_environment:
                            raise
                        break
                    uncertain = False
                    for spec, limit, answered in available:
                        if all(maximum is None or current['images_count'] <= maximum for maximum in (spec['maxPhotos'], limit)):
                            # A node production might use is closed to staging,
                            # so it neither takes the job nor holds it back.
                            if staging and not self.queue.staging_may_claim(spec['id']):
                                continue
                            if not answered:
                                uncertain = True
                                continue
                            if not self.queue.viewer_skip_is_safe(candidate['id'], spec['id']):
                                continue
                            try:
                                self._check_options(current['options'], node_id=spec['id'],
                                                    evidence=current['option_evidence'], full_evidence=current['full_option_evidence'])
                            except (Conflict, ValueError):
                                # A node that returned with changed options is
                                # ineligible for this task; try the next node.
                                continue
                            held = uncertain
                            if not held:
                                task, node_id = current, spec['id']
                            break
                    if node_id is not None or held:
                        break
                if node_id is None:
                    if task is not None:
                        self.store.update(task['uuid'], phase='waiting_node', error='no_eligible_node')
                    return
            else:
                for node, role, origin, token in (
                        ('cluster', 'local', self.config.local_node_url, self.config.local_node_token),
                        ('main', 'main', self.config.main_node_url, self.config.main_node_token)):
                    nodes[node] = {**self.probe(role, origin, token),
                                   'state': self.queue.get_node(node)['state']}
                    if not self._guard_ready(node):
                        nodes[node]['ready'] = False
                occupied = {j['nodeId'] for j in jobs if j['status'] in ('running', 'held')}
                if any(node not in nodes for node in occupied):
                    return
                cluster_busy = ('cluster' in occupied or
                                (nodes['cluster'].get('ready') is True and
                                 type(nodes['cluster'].get('taskQueueCount')) is int and
                                 nodes['cluster']['taskQueueCount'] > 0))
                for node in occupied:
                    nodes[node]['taskQueueCount'] = 1
                decision = plan(task['images_count'], task['input_bytes'], nodes['cluster'], nodes['main'],
                                self.config.local_max_photos, self.config.local_max_input_bytes,
                                allow_main_fallback=self.queue.per_node_slots and cluster_busy)
                if decision['decision'] == 'wait':
                    self.store.update(task['uuid'], phase='waiting_node', error=decision['reason'])
                    return
                node_id = 'cluster' if decision['decision'] == 'local' else 'main'
            # Submission uses the fleet-wide common catalog. At dispatch,
            # refresh the selected engine specifically so an unrelated node
            # outage does not strand an already accepted job.
            self._check_options(task['options'], node_id=node_id, evidence=task['option_evidence'],
                                full_evidence=task['full_option_evidence'])
            claim_request = {'workerId': 'viewer-dispatch', 'nodeId': node_id,
                             'profiles': [VIEWER_PROFILE], 'expectedJobId': task['job_id']}
            job = (self.queue.claim(claim_request, allow_viewer_same_priority_skip=True)
                   if self.inventory else self.queue.claim(claim_request))
            if not job:
                return
            try:
                self.store.update(task['uuid'], stage='queued', phase='claimed', node_id=node_id,
                                  claim_token=job['claimToken'], engine_uuid=str(uuid.uuid4()), error=None)
                if self._parallel_dispatch:
                    if not self.queue.per_node_slots:
                        raise Conflict('parallel dispatch requires per-node slots')
                    thread = threading.Thread(target=self._transfer_claimed_task, args=(task['uuid'], job['id']),
                                              name='viewer-node-transfer', daemon=True)
                    self._dispatch_threads[task['uuid']] = (job['id'], thread)
                    try:
                        thread.start()
                    except BaseException:
                        self._dispatch_threads.pop(task['uuid'], None)
                        raise
                else:
                    self._send_claimed_task(task['uuid'])
            except Exception:
                current = self.store.task(task['uuid'])
                if current['phase'] == 'commit_pending':
                    # Exact assigned engine UUID lets us reconcile a lost commit
                    # ACK by reads. Never send commit again under uncertainty.
                    if current['node_guarded'] == 1:
                        self._hold(current, 'commit_ack_uncertain')
                    else:
                        self.store.update(task['uuid'], error='commit_ack_uncertain')
                else:
                    self._hold(current, 'engine_submission_uncertain')

    def start(self):
        if self._thread:
            raise RuntimeError('dispatcher already started')
        self._thread = threading.Thread(target=self._run, name='viewer-dispatch', daemon=True)
        self._thread.start()

    def _run(self):
        reported_error = False
        while not self._stop.is_set():
            try:
                self.tick()
                reported_error = False
            except Exception:
                # No replay of mutations. Intents/claims remain durable; a
                # restart retains their slot for reconciliation or inspection.
                if not reported_error:
                    logging.getLogger(__name__).error('Viewer scheduler iteration failed; inspection required')
                    reported_error = True
            self._stop.wait(2)

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                raise RuntimeError('Viewer scheduler did not stop; retain queue ownership')
        with self._lock:
            transfers = [thread for _, thread in self._dispatch_threads.values()]
        for thread in transfers:
            thread.join(timeout=10)
        if any(thread.is_alive() for thread in transfers):
            raise RuntimeError('active Viewer transfer did not stop; retain queue ownership')

    def wait_for_transfers(self):
        """Keep coordinator authority until every task-owned sender exits.

        No lock is held while waiting: senders need it for their final cleanup.
        A caller may terminate the process, but must not release queue authority
        in a still-running process with a sender able to mutate an engine.
        """
        while True:
            with self._lock:
                transfers = [thread for _, thread in self._dispatch_threads.values()]
            if not transfers:
                return
            for thread in transfers:
                thread.join()
