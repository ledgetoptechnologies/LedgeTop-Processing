"""Native Viewer facade service and ordered, per-node engine dispatcher.

Only configured engine endpoints are contacted. Mutation intent is durable
before HTTP; uncertain mutations hold that physical node's slot instead of
replaying a job. Completed engine tasks and downloaded archives remain until
operator cleanup.
"""
from contextlib import contextmanager

import http.client
import hashlib
import json
import os
from pathlib import Path
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, build_opener, ProxyHandler
import uuid
import zipfile

from .config import ProcessingNode
from .engine_output import read_output_prefix
from .maintenance import Conflict
from .odx_smoke_worker import Client, SmokeError
from .options import discover_node, _NoRedirect, _compatibility_signature
from .prototype import VIEWER_PROFILE
from .provider import probe_node
from .viewer_tasks import TaskStore, sync_directory


class EngineUnavailable(OSError):
    pass


class NativeEngine:
    def __init__(self, origin, token=''):
        self.origin, self.token = origin, token
        self.client = Client(origin)

    def route(self, route, **query):
        if self.token:
            query['token'] = self.token
        return route + ('?' + urlencode(query) if query else '')

    def read(self, route, *, missing=False, limit=4 * 1024 ** 2, **query):
        try:
            request = Request(self.origin + self.route(route, **query))
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

    def output(self, identifier, line=0):
        if type(line) is not int or not 0 <= line <= 2 ** 53 - 1:
            raise ValueError('invalid output cursor')
        try:
            request = Request(self.origin + self.route('/task/' + identifier + '/output', line=line))
            deadline = time.monotonic() + 5
            with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=5) as response:
                if 'json' not in response.headers.get('Content-Type', ''):
                    raise ValueError()
                return read_output_prefix(response, deadline=deadline)
        except HTTPError as error:
            error.close()
            raise EngineUnavailable('engine_output_unavailable') from None
        except (OSError, ValueError, http.client.HTTPException):
            raise EngineUnavailable('engine_output_unavailable') from None

    def initialize(self, task):
        fields = {'name': task['name'], 'options': json.dumps(task['options'])}
        if task['outputs'] is not None:
            fields['outputs'] = json.dumps(task['outputs'])
        result = self.client.json('POST', self.route('/task/new/init'), fields=fields,
                                  headers={'set-uuid': task['engine_uuid']})
        if not isinstance(result, dict) or result.get('uuid') != task['engine_uuid']:
            raise SmokeError('engine initialization identity uncertain')

    def upload(self, identifier, item):
        path = Path(item['path'])
        if path.resolve() != path or path.is_symlink() or not path.is_file() or path.stat().st_size != item['bytes']:
            raise SmokeError('input file changed')
        boundary = 'processing-' + uuid.uuid4().hex
        prefix = (f'--{boundary}\r\nContent-Disposition: form-data; name="images"; '
                  f'filename="{item["name"]}"\r\nContent-Type: application/octet-stream\r\n\r\n').encode('ascii')
        raw = self.client.request('POST', self.route('/task/new/upload/' + identifier),
            headers={'Content-Type': 'multipart/form-data; boundary=' + boundary},
            file=path, prefix=prefix, suffix=f'\r\n--{boundary}--\r\n'.encode(),
            expected_hash=item['sha256'], timeout=120)
        result = json.loads(raw)
        if not isinstance(result, dict) or result.get('success') is not True or 'error' in result:
            raise SmokeError('engine upload acknowledgment uncertain')

    def action(self, action, identifier):
        if action == 'commit':
            route, fields = '/task/new/commit/' + identifier, None
            result = self.client.json('POST', self.route(route))
        else:
            route = '/task/' + action
            raw = self.client.request('POST', self.route(route), body=urlencode({'uuid': identifier}).encode(),
                                      headers={'Content-Type': 'application/x-www-form-urlencoded'})
            result = json.loads(raw)
        if (not isinstance(result, dict) or 'error' in result or result.get('success') is False
                or ('uuid' in result and result['uuid'] != identifier)):
            raise SmokeError('engine mutation acknowledgment uncertain')
        return result

    def download(self, identifier):
        return build_opener(ProxyHandler({}), _NoRedirect()).open(
            self.origin + self.route('/task/' + identifier + '/download/all.zip'), timeout=60)


class _PinnedStream:
    def __init__(self, stream, store, token):
        self._stream, self._store, self._token = stream, store, token

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def read(self, size=-1):
        return self._stream.read(size)

    def close(self):
        if self._token is None:
            return
        # If close fails, preserve the durable pin for explicit reconciliation.
        self._stream.close()
        self._store.release_transfer(self._token)
        self._token = None


class ViewerService:
    def __init__(self, queue, config, *, store=None, engines=None, probe=probe_node):
        self.queue, self.config = queue, config
        self.store = store or TaskStore(queue, Path(config.prototype_state_dir) / 'viewer-tasks',
                                       min_free_bytes=getattr(config, 'viewer_min_free_bytes', 0))
        self.nodes = tuple(getattr(config, 'processing_nodes', ()) or (
            ProcessingNode('cluster', config.local_node_url, config.local_node_token,
                           config.local_max_photos),
            ProcessingNode('main', config.main_node_url, config.main_node_token)))
        self.node_order = tuple(node.node_id for node in self.nodes)
        if set(self.node_order) != set(queue.node_ids):
            raise ValueError('ordered processing nodes must exactly match queue nodes')
        self.engines = engines or {node.node_id: NativeEngine(node.origin, node.token)
                                   for node in self.nodes}
        self.probe = probe
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None
        self._backoff = {}
        self._downloads = {}
        # Registry lookup must never wait for the scheduler's whole upload turn.
        self._downloads_lock = threading.Lock()
        self._upload_manifests = {}
        self._upload_files_per_turn = 4
        self._upload_seconds_per_turn = 1.0

    @staticmethod
    def _probe_role(node_id):
        # Keep the established fixed endpoint roles compatible while the
        # scheduler and queue use stable configured node IDs.
        return {'cluster': 'local', 'main': 'main'}.get(node_id, node_id)

    def _catalog(self):
        nodes = []
        for node in self.nodes:
            health = self.probe(self._probe_role(node.node_id), node.origin, node.token)
            options = discover_node(node.node_id, node.origin, node.token, allow_dynamic=True)
            ready = health.get('ready') is True and options.get('available') is True
            nodes.append({'nodeId': node.node_id, 'ready': ready, 'health': health,
                          'options': options.get('options', []) if ready else []})
        available = [node for node in nodes if node['ready']]
        by_node = {node['nodeId']: {item['name']: item for item in node['options']}
                   for node in available}
        common = set.intersection(*(set(value) for value in by_node.values())) if by_node else set()
        safe = [by_node[available[0]['nodeId']][name] for name in sorted(common)
                if len({json.dumps(_compatibility_signature(by_node[node['nodeId']][name]),
                                   sort_keys=True, separators=(',', ':'), allow_nan=False)
                        for node in available}) == 1]
        # An unavailable node contributes no stale options. The remaining live
        # nodes can still accept and queue work; dispatch rechecks the selected
        # node's own fresh definitions before claiming a task.
        return {'ready': bool(available), 'reason': None if available else 'node_unavailable',
                'nodes': nodes, 'commonOptions': safe}

    def options(self):
        current = self._catalog()
        if not current['ready']:
            raise Conflict('engine catalog unavailable')
        return current['commonOptions']

    def capabilities(self):
        # This is our own 2.x-compatible facade, not an official ClusterODM build.
        self.options()
        jobs = self.queue.list()
        return {'version': '2.4.0', 'engine': 'ledgetop-processing', 'engineVersion': '0.2.0',
                'maxImages': None, 'maxParallelTasks': len(self.nodes), 'coordinator': True,
                'taskQueueCount': sum(j['status'] in ('queued', 'running', 'held') for j in jobs)}

    def _check_options(self, options, definitions=None):
        allowed = {item['name'] for item in (definitions if definitions is not None else self.options())}
        if (not isinstance(options, list) or any(not isinstance(item, dict)
                or item.get('name') not in allowed for item in options)):
            raise ValueError('option unavailable on configured engines')

    def initialize(self, identifier, name='', options=None, outputs=None, environment='production'):
        self._check_options(options or [])
        return self.store.initialize(identifier, name, options, outputs, environment)

    def upload(self, identifier, filename, stream, length, *, transfer=None):
        if transfer is not None:
            return self.store.upload(identifier, filename, stream, length, transfer=transfer)
        with self.transfer_scope(identifier, 'upload') as pin:
            return self.store.upload(identifier, filename, stream, length, transfer=pin)

    def ensure_upload_capacity(self, request_bytes):
        # Reserve both the request-body temp file and durable per-file temp copy
        # before accepting the body. The per-file write rechecks the same bound.
        return self.store.ensure_capacity(request_bytes, copies=2)

    def commit(self, identifier):
        return self.store.commit(identifier)

    def info(self, identifier):
        return self.store.info(identifier)

    def producer_binding(self, identifier, environment):
        return self.store.producer_binding(identifier, environment)

    def authorize(self, identifier, environment):
        if self.store.task(identifier)['environment'] != environment:
            raise KeyError('unknown task')

    def output(self, identifier, line=0):
        task = self.store.task(identifier)
        if task['engine_uuid'] and task['node_id'] and task['phase'] in ('processing', 'complete', 'cancel_pending'):
            try:
                engine = self.engines[task['node_id']]
                # Older injected adapters keep their read-only output contract.
                output = getattr(engine, 'output', None)
                result = output(task['engine_uuid'], line) if callable(output) else engine.read(
                    '/task/' + task['engine_uuid'] + '/output', line=line)
                if not isinstance(result, list) or any(not isinstance(x, str) for x in result):
                    raise EngineUnavailable('engine_output_unavailable')
                return result
            except EngineUnavailable:
                # Never return engine error bodies, query tokens or host paths.
                return self.store.output(identifier, line)
        return self.store.output(identifier, line)

    @contextmanager
    def transfer_scope(self, identifier, kind, *, expected_generation=None):
        token = self.store.acquire_transfer(identifier, kind, expected_generation=expected_generation)
        try:
            yield (token, self.store.transfer_generation(token))
        finally:
            self.store.release_transfer(token)

    def download(self, identifier):
        generation = self.store.task(identifier)['generation']
        try:
            token = self.store.acquire_transfer(identifier, 'download', expected_generation=generation)
        except KeyError:
            raise Conflict('task generation changed') from None
        try:
            stream, length = self._download_pinned(identifier, generation)
        except KeyError:
            self.store.release_transfer(token)
            raise Conflict('task generation changed') from None
        except BaseException:
            self.store.release_transfer(token)
            raise
        return _PinnedStream(stream, self.store, token), length

    def _download_pinned(self, identifier, generation):
        task = self.store.task(identifier)
        if task['generation'] != generation:
            raise Conflict('task generation changed')
        if not task['job'] or task['job']['status'] != 'succeeded':
            raise Conflict('task is not complete')
        identity = {field: task[field] for field in
                    ('generation', 'job_id', 'engine_uuid', 'node_id', 'directory')}
        def current_result():
            current = self.store.task(identifier)
            if {field: current[field] for field in identity} != identity:
                raise Conflict('task result identity changed')
            if not current['job'] or current['job']['status'] != 'succeeded':
                raise Conflict('task is not complete')
            return current
        with self._downloads_lock:
            lock = self._downloads.setdefault(identifier, threading.Lock())
        with lock:
            # Completion alone cannot bind a waiter across a same-UUID reset.
            task = current_result()
            path = Path(task['archive_path']) if task['archive_path'] else Path(task['directory']) / 'all.zip'
            if not task['archive_path']:
                partial = path.with_suffix('.zip.partial')
                if partial.exists():
                    if partial.is_symlink() or not partial.is_file():
                        raise Conflict('previous archive path requires inspection')
                    # A GET is safe to repeat; the engine's original is retained.
                    # Only this exact incomplete local transfer is replaced.
                    partial.unlink()
                total = 0
                archive_digest = hashlib.sha256()
                with self.engines[task['node_id']].download(task['engine_uuid']) as response:
                    if getattr(response, 'status', 200) != 200:
                        raise EngineUnavailable('archive_unavailable')
                    media_type = response.headers.get('Content-Type', '').split(';')[0].lower()
                    if media_type and media_type not in ('application/zip', 'application/octet-stream',
                                                          'application/x-zip-compressed'):
                        raise EngineUnavailable('archive_not_ready')
                    declared = response.headers.get('Content-Length')
                    if declared is not None:
                        if not declared.isdigit():
                            raise EngineUnavailable('archive_transfer_incomplete')
                        self.store.ensure_capacity(int(declared))
                    with partial.open('xb') as output:
                        os.chmod(partial, 0o600)
                        while chunk := response.read(1024 ** 2):
                            total += len(chunk)
                            self.store.ensure_capacity(len(chunk))
                            archive_digest.update(chunk)
                            output.write(chunk)
                        if declared is not None and total != int(declared):
                            raise EngineUnavailable('archive_transfer_incomplete')
                        output.flush()
                        os.fsync(output.fileno())
                if total < 4 or not zipfile.is_zipfile(partial):
                    raise EngineUnavailable('archive_transfer_incomplete')
                os.replace(partial, path)
                sync_directory(path.parent)
                current_result()
                self.store.update(identifier, expected_result=identity, archive_path=str(path),
                                  archive_sha256=archive_digest.hexdigest())
            if path.is_symlink() or not path.is_file():
                raise Conflict('retained archive unavailable')
            task = current_result()
            if not task['archive_sha256']:
                digest = hashlib.sha256()
                with path.open('rb') as archive:
                    while chunk := archive.read(1024 ** 2):
                        digest.update(chunk)
                current_result()
                self.store.update(identifier, expected_result=identity, archive_sha256=digest.hexdigest())
            current_result()
            return path.open('rb'), path.stat().st_size

    def accept_viewer_delivery(self, identifier, environment, receipt):
        return self.store.accept_viewer_delivery(identifier, environment, receipt)

    def cancel(self, identifier):
        with self._lock:
            task = self.store.task(identifier)
            job = task['job']
            if not job or job['status'] in ('queued', 'failed', 'succeeded'):
                return self.store.cancel(identifier)
            if task['phase'] == 'cancel_pending':
                return {'success': True}
            if job['status'] == 'held' and job['heldReason'] != 'coordinator_restart':
                raise Conflict('uncertain execution requires inspection')
            if task['phase'] != 'processing':
                raise Conflict('submission in progress; inspect before cancelling')
            self.store.update(identifier, phase='cancel_pending')
            try:
                self.engines[task['node_id']].action('cancel', task['engine_uuid'])
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

    def _hold(self, task, reason):
        self._upload_manifests.pop(task['uuid'], None)
        job = self.queue.get(task['job_id'])
        if job and (job['status'] == 'running' or
                    (job['status'] == 'held' and job['heldReason'] == 'coordinator_restart')):
            self.queue.operator(task['job_id'], 'hold', {'note': reason})
        self.store.update(task['uuid'], return_task=False, stage='held', phase='held', error=reason)

    def _poll(self, task):
        identifier = task['uuid']
        now = time.monotonic()
        due, failures = self._backoff.get(identifier, (0, 0))
        if now < due:
            return
        try:
            info = self.engines[task['node_id']].read('/task/' + task['engine_uuid'] + '/info')
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
        if code != 10:self._upload_manifests.pop(identifier, None)
        if code in (10, 20):
            # Queued10 may mean a delayed accepted commit or an uncommitted
            # upload. Keep ownership and poll; neither case permits a replay.
            progress = info.get('progress', 0)
            if type(progress) not in (int, float) or not 0 <= progress <= 100:
                self._backoff[identifier] = (now + 2, 1)
                return
            fields = {'progress': progress}
            if code == 20 and task['phase'] in ('commit_pending', 'awaiting_engine_start'):
                fields.update(phase='processing', error=None)
            self.store.update(identifier, return_task=False, **fields)
            return
        status = 'succeeded' if code == 40 else 'failed'
        result = {'engineTaskId': task['engine_uuid'], 'nodeId': task['node_id'],
                  'environment': task['environment'], 'cancelled': code == 50}
        self.queue.complete(task['job_id'], {'claimToken': task['claim_token'], 'status': status, 'result': result})
        self.store.update(identifier, return_task=False, stage='cancelled' if code == 50 else status,
                          phase='complete', progress=100 if code == 40 else task['progress'])

    def _node_snapshots(self, current):
        rows = current.get('nodes')
        if isinstance(rows, list):
            return {row.get('nodeId'): row for row in rows if isinstance(row, dict)}
        # Test doubles and older callers may provide only a fleet-wide option
        # catalog. Probe each configured node independently; no shared stale
        # readiness bit may hide a healthy sibling.
        result = {}
        for node in self.nodes:
            health = self.probe(self._probe_role(node.node_id), node.origin, node.token)
            ready = current.get('ready') is True and health.get('ready') is True
            result[node.node_id] = {'nodeId': node.node_id, 'ready': ready,
                'health': health, 'options': current.get('commonOptions', []) if ready else []}
        return result

    def _can_run_on(self, task, node, snapshot, active_nodes):
        if (snapshot is None or snapshot.get('ready') is not True
                or node.node_id in active_nodes or node.node_id not in self.engines):
            return False
        try:
            state = self.queue.get_node(node.node_id)['state']
        except (KeyError, ValueError):
            return False
        if state not in ('active', 'scheduled'):
            return False
        health = snapshot.get('health') or {}
        queued_count = health.get('taskQueueCount')
        if type(queued_count) is not int or queued_count != 0:
            return False
        limits = [value for value in (node.max_photos, health.get('maxImages'))
                  if type(value) is int and value > 0]
        if limits and task['images_count'] > min(limits):
            return False
        allowed = {item.get('name') for item in snapshot.get('options', []) if isinstance(item, dict)}
        requested = {item.get('name') for item in task['options'] if isinstance(item, dict)}
        return requested <= allowed

    def _start_claimed(self, task, job, node_id):
        try:
            self.store.update(task['uuid'], return_task=False, stage='running', phase='claimed', node_id=node_id,
                              claim_token=job['claimToken'], engine_uuid=str(uuid.uuid4()),
                              upload_cursor=0, error=None)
            task = self.store.task_state(task['uuid'])
            engine = self.engines[node_id]
            if self._stop.is_set():
                return False
            self.store.update(task['uuid'], return_task=False, phase='init_pending')
            if self._stop.is_set():
                return True
            engine.initialize(task)
            self.store.update(task['uuid'], return_task=False, phase='initialized', upload_cursor=0)
            return self._upload_and_commit(self.store.task_state(task['uuid']))
        except Exception:
            self._submission_failed(task, 'engine_submission_uncertain')

    def _submission_failed(self, task, reason):
        current = self.store.task_state(task['uuid'])
        if current['phase'] == 'commit_pending':
            # Reconcile the original UUID by reads; never replay an uncertain
            # commit, including one attempted on a later bounded turn.
            self.store.update(task['uuid'], return_task=False, error='commit_ack_uncertain')
        else:
            self._hold(current, reason)

    @staticmethod
    def _upload_identity(task):
        return {field: task[field] for field in
                ('generation', 'job_id', 'engine_uuid', 'node_id', 'directory')}

    def _upload_and_commit(self, task):
        with self.transfer_scope(task['uuid'], 'engine_upload', expected_generation=task['generation']):
            return self._upload_and_commit_pinned(task)

    def _upload_and_commit_pinned(self, task):
        """One bounded upload turn; only the dispatcher mutates engine state."""
        if self._stop.is_set():
            return False
        identity = self._upload_identity(task)
        cached = self._upload_manifests.get(task['uuid'])
        if cached is None:
            manifest = task if 'files' in task else self.store.task(task['uuid'])
            if self._upload_identity(manifest) != identity:
                raise Conflict('upload task identity changed')
            cached = (identity, tuple(dict(item) for item in manifest['files']))
            self._upload_manifests[task['uuid']] = cached
        if cached[0] != identity:
            raise Conflict('upload task identity changed')
        files = cached[1]
        engine = self.engines[task['node_id']]
        cursor = task['upload_cursor']
        if cursor > len(files):
            raise SmokeError('durable upload cursor exceeds input manifest')
        advanced = False
        started = time.monotonic()
        sent = 0
        while cursor < len(files):
            if (self._stop.is_set() or sent >= self._upload_files_per_turn
                    or (sent and time.monotonic() - started >= self._upload_seconds_per_turn)):
                return advanced
            self.store.update(task['uuid'], return_task=False, expected_result=identity,
                              phase='upload_pending', upload_cursor=cursor)
            if self._stop.is_set():
                # No HTTP call began, so only this process can prove the intent
                # had no side effect. Keep the acknowledged cursor resumable.
                self.store.update(task['uuid'], return_task=False, expected_result=identity,
                                  phase='uploaded', upload_cursor=cursor)
                return True
            engine.upload(task['engine_uuid'], files[cursor])
            # Even if stop arrived during HTTP, persist its ACK before yielding.
            self.store.update(task['uuid'], return_task=False, expected_result=identity,
                              phase='uploaded', upload_cursor=cursor + 1)
            cursor += 1
            sent += 1
            advanced = True
        if (self._stop.is_set()
                or (sent and time.monotonic() - started >= self._upload_seconds_per_turn)):
            return advanced
        self.store.update(task['uuid'], return_task=False, expected_result=identity, phase='commit_pending')
        if self._stop.is_set():
            # The commit client has not been called; do not leave an ambiguous
            # commit intent when this process knows no mutation was attempted.
            self.store.update(task['uuid'], return_task=False, expected_result=identity, phase='uploaded')
            return True
        engine.action('commit', task['engine_uuid'])
        self.store.update(task['uuid'], return_task=False, expected_result=identity, phase='awaiting_engine_start')
        self._upload_manifests.pop(task['uuid'], None)
        if not self._stop.is_set():
            self._poll(self.store.task_state(task['uuid']))
        return True

    def _resume_precommit(self, task):
        """Resume only restart phases whose last engine mutation is known."""
        if self._stop.is_set():
            return False
        engine = self.engines[task['node_id']]
        phase = task['phase']
        if phase == 'claimed':
            # The engine UUID was durably assigned before any engine request.
            self.store.update(task['uuid'], return_task=False, phase='init_pending')
            task = self.store.task_state(task['uuid'])
            phase = task['phase']
        if phase == 'init_pending':
            info = engine.read('/task/' + task['engine_uuid'] + '/info', missing=True)
            if info is None:
                if self._stop.is_set():return False
                engine.initialize(task)
            elif (not isinstance(info, dict) or info.get('uuid') != task['engine_uuid']
                  or not isinstance(info.get('status'), dict)
                  or info['status'].get('code') != 10):
                raise SmokeError('engine initialization recovery uncertain')
            self.store.update(task['uuid'], return_task=False, phase='initialized', upload_cursor=0)
        elif phase in ('initialized', 'uploaded'):
            info = engine.read('/task/' + task['engine_uuid'] + '/info', missing=True)
            if info is None:
                # No engine task exists, so none of the previously acknowledged
                # uploads remain. Reinitialize the same reserved UUID and replay
                # only these known-absent side effects from the retained source.
                if self._stop.is_set():return False
                engine.initialize(task)
                self.store.update(task['uuid'], return_task=False, phase='initialized', upload_cursor=0)
            elif (not isinstance(info, dict) or info.get('uuid') != task['engine_uuid']
                  or not isinstance(info.get('status'), dict)
                  or info['status'].get('code') != 10):
                raise SmokeError('engine upload recovery uncertain')
        else:
            raise SmokeError('submission phase is not safely resumable')
        return self._upload_and_commit(self.store.task_state(task['uuid']))

    def tick(self):
        """One bounded scheduler turn; public for deterministic synthetic tests."""
        progressed = False
        with self._lock:
            if self._stop.is_set():return False
            for task in self.store.list_task_states():
                if self._stop.is_set():return progressed
                job = task['job']
                if not job or job['status'] not in ('running', 'held'):
                    continue
                if job['status'] == 'held' and job['heldReason'] != 'coordinator_restart':
                    continue
                if (task['phase'] in ('init_pending', 'initialized', 'uploaded', 'claimed')
                        and task['engine_uuid'] and task['node_id'] and task['claim_token']
                        and job['status'] == 'held' and job['heldReason'] == 'coordinator_restart'):
                    due, failures = self._backoff.get(task['uuid'], (0, 0))
                    if time.monotonic() < due:
                        continue
                    try:
                        progressed = self._resume_precommit(task) or progressed
                    except EngineUnavailable:
                        failures += 1
                        self._backoff[task['uuid']] = (
                            time.monotonic() + min(60, 2 ** min(failures, 6)), failures)
                    except Exception:
                        self._submission_failed(task, 'submission_interrupted')
                elif (job['status'] == 'running' and task['phase'] in ('initialized', 'uploaded')
                        and task['engine_uuid'] and task['node_id'] and task['claim_token']):
                    try:
                        progressed = self._upload_and_commit(task) or progressed
                    except Exception:
                        self._submission_failed(task, 'submission_interrupted')
                elif (task['phase'] in ('processing', 'commit_pending', 'awaiting_engine_start', 'cancel_pending')
                        and task['engine_uuid'] and task['node_id'] and task['claim_token']):
                    self._poll(task)
                elif task['phase'] == 'upload_pending' and job['status'] == 'held' \
                        and job['heldReason'] == 'coordinator_restart':
                    # NodeODM does not expose an authoritative uploaded-file
                    # inventory; never retry this ambiguous mutation blindly.
                    self._hold(task, 'submission_interrupted')
                else:
                    self._hold(task, 'submission_interrupted')
            if self._stop.is_set():return progressed
            catalogue = self._catalog()
            snapshots = self._node_snapshots(catalogue)
            if not catalogue.get('ready'):
                return progressed
            active_nodes = set()
            for job in self.queue.list():
                if job['status'] in ('running', 'held'):
                    if job['nodeId'] not in self.node_order:
                        # An active job without a configured physical owner
                        # fences all dispatch until an operator reconciles it.
                        return progressed
                    active_nodes.add(job['nodeId'])

            for node in self.nodes:
                if self._stop.is_set():return progressed
                queue_snapshot = self.queue.dispatch_snapshot()
                jobs = queue_snapshot['jobs']
                if node.node_id in active_nodes:
                    continue
                queued = [job for job in jobs if job['status'] == 'queued']
                if not queued:
                    continue
                # Production always precedes waiting staging work. Within that
                # priority, skip jobs this node cannot run so a 5k-photo job
                # cannot pin the small node's queue ahead of an eligible job.
                production = [job for job in queued if job['environment'] == 'production']
                priority_jobs = production or [job for job in queued if job['environment'] == 'staging']
                # Only photo tasks are submitted by this adapter. Still account
                # for every queue profile above so a waiting production job
                # cannot be bypassed by a staging photo on another node.
                priority_jobs = [job for job in priority_jobs if job['profile'] == VIEWER_PROFILE]
                task_by_job = {task['job']['id']: task for task in self.store.list_task_states()
                               if task.get('job') is not None}
                ready_option_names = {
                    item.get('name')
                    for snapshot in snapshots.values() if snapshot.get('ready') is True
                    for item in snapshot.get('options', []) if isinstance(item, dict)
                }
                eligible = []
                node_options = (snapshots.get(node.node_id) or {}).get('options', [])
                for candidate in priority_jobs:
                    candidate_task = task_by_job.get(candidate['id'])
                    if candidate_task is None:
                        continue
                    requested = {item.get('name') for item in candidate_task['options']
                                 if isinstance(item, dict)}
                    if not requested <= ready_option_names:
                        # A node update can remove an option after admission.
                        # Preserve this job for retry, but do not let it stop
                        # other eligible work in the same priority tier.
                        if (candidate_task['phase'] != 'waiting_node'
                                or candidate_task.get('error') != 'options_unavailable'):
                            self.store.update(candidate_task['uuid'], phase='waiting_node',
                                              error='options_unavailable')
                        continue
                    if candidate_task.get('error') == 'options_unavailable':
                        self.store.update(candidate_task['uuid'], phase='waiting_node',
                                          error='no_eligible_node')
                    if self._can_run_on(candidate_task, node, snapshots.get(node.node_id), active_nodes):
                        eligible.append((candidate, candidate_task))
                if not eligible:
                    for candidate in priority_jobs:
                        candidate_task = task_by_job.get(candidate['id'])
                        if candidate_task is not None and candidate_task['phase'] not in ('waiting_node', 'processing'):
                            self.store.update(candidate_task['uuid'], phase='waiting_node', error='no_eligible_node')
                    continue

                candidate, task = eligible[0]
                eligible_nodes = []
                for other in self.nodes:
                    if self._can_run_on(task, other, snapshots.get(other.node_id), active_nodes):
                        eligible_nodes.append(other.node_id)
                eligible_job_ids = [job['id'] for job, _ in eligible]
                self._check_options(task['options'], node_options)
                if self._stop.is_set():return progressed
                job = self.queue.claim({'workerId': 'viewer-dispatch', 'nodeId': node.node_id,
                    'profiles': [VIEWER_PROFILE], 'expectedJobId': candidate['id']},
                    eligible_job_ids=eligible_job_ids, eligible_node_ids=eligible_nodes,
                    expected_queue_sequence=queue_snapshot['sequence'])
                if job is None:
                    # A concurrent submission or claim invalidated this
                    # snapshot; do not try a lower-priority selection this turn.
                    return progressed
                active_nodes.add(node.node_id)
                progressed = self._start_claimed(task, job, node.node_id) or progressed

        return progressed

    def start(self):
        if self._thread:
            raise RuntimeError('dispatcher already started')
        self._thread = threading.Thread(target=self._run, name='viewer-dispatch', daemon=True)
        self._thread.start()

    def _run(self):
        while not self._stop.is_set():
            progressed = False
            try:
                progressed = self.tick()
            except Exception:
                # No replay of mutations. Intents/claims remain durable; a
                # restart retains their slot for reconciliation or inspection.
                pass
            self._stop.wait(0 if progressed else 2)

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
