"""Native Viewer facade service and ordered, per-node engine dispatcher.

Only configured engine endpoints are contacted. Mutation intent is durable
before HTTP; uncertain mutations hold that physical node's slot instead of
replaying a job. Completed engine tasks and downloaded archives remain until
operator cleanup.
"""

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
        if path.is_symlink() or not path.is_file() or path.stat().st_size != item['bytes']:
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

    def upload(self, identifier, filename, stream, length):
        return self.store.upload(identifier, filename, stream, length)

    def ensure_upload_capacity(self, request_bytes):
        # Reserve both the request-body temp file and durable per-file temp copy
        # before accepting the body. The per-file write rechecks the same bound.
        return self.store.ensure_capacity(request_bytes, copies=2)

    def commit(self, identifier):
        return self.store.commit(identifier)

    def info(self, identifier):
        return self.store.info(identifier)

    def authorize(self, identifier, environment):
        if self.store.task(identifier)['environment'] != environment:
            raise KeyError('unknown task')

    def output(self, identifier, line=0):
        task = self.store.task(identifier)
        if task['engine_uuid'] and task['node_id'] and task['phase'] in ('processing', 'complete', 'cancel_pending'):
            try:
                result = self.engines[task['node_id']].read('/task/' + task['engine_uuid'] + '/output', line=line)
                if not isinstance(result, list) or any(not isinstance(x, str) for x in result):
                    raise EngineUnavailable('engine_output_unavailable')
                return result
            except EngineUnavailable:
                # Never return engine error bodies, query tokens or host paths.
                return self.store.output(identifier, line)
        return self.store.output(identifier, line)

    def download(self, identifier):
        task = self.store.task(identifier)
        if not task['job'] or task['job']['status'] != 'succeeded':
            raise Conflict('task is not complete')
        with self._lock:
            lock = self._downloads.setdefault(identifier, threading.Lock())
        with lock:
            task = self.store.task(identifier)
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
                self.store.update(identifier, archive_path=str(path), archive_sha256=archive_digest.hexdigest())
            if path.is_symlink() or not path.is_file():
                raise Conflict('retained archive unavailable')
            task = self.store.task(identifier)
            if not task['archive_sha256']:
                digest = hashlib.sha256()
                with path.open('rb') as archive:
                    while chunk := archive.read(1024 ** 2):
                        digest.update(chunk)
                self.store.update(identifier, archive_sha256=digest.hexdigest())
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
        job = self.queue.get(task['job_id'])
        if job and (job['status'] == 'running' or
                    (job['status'] == 'held' and job['heldReason'] == 'coordinator_restart')):
            self.queue.operator(task['job_id'], 'hold', {'note': reason})
        self.store.update(task['uuid'], stage='held', phase='held', error=reason)

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
            self.store.update(identifier, **fields)
            return
        status = 'succeeded' if code == 40 else 'failed'
        result = {'engineTaskId': task['engine_uuid'], 'nodeId': task['node_id'],
                  'environment': task['environment'], 'cancelled': code == 50}
        self.queue.complete(task['job_id'], {'claimToken': task['claim_token'], 'status': status, 'result': result})
        self.store.update(identifier, stage='cancelled' if code == 50 else status,
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
            self.store.update(task['uuid'], stage='running', phase='claimed', node_id=node_id,
                              claim_token=job['claimToken'], engine_uuid=str(uuid.uuid4()),
                              upload_cursor=0, error=None)
            task = self.store.task(task['uuid'])
            engine = self.engines[node_id]
            self.store.update(task['uuid'], phase='init_pending')
            engine.initialize(task)
            self.store.update(task['uuid'], phase='initialized', upload_cursor=0)
            self._upload_and_commit(self.store.task(task['uuid']))
        except Exception:
            current = self.store.task(task['uuid'])
            if current['phase'] == 'commit_pending':
                # Exact assigned engine UUID lets us reconcile a lost commit
                # ACK by reads. Never send commit again under uncertainty.
                self.store.update(task['uuid'], error='commit_ack_uncertain')
            else:
                self._hold(current, 'engine_submission_uncertain')

    def _upload_and_commit(self, task):
        engine = self.engines[task['node_id']]
        cursor = task['upload_cursor']
        if cursor > len(task['files']):
            raise SmokeError('durable upload cursor exceeds input manifest')
        for index in range(cursor, len(task['files'])):
            self.store.update(task['uuid'], phase='upload_pending', upload_cursor=index)
            engine.upload(task['engine_uuid'], task['files'][index])
            # Advance only after an acknowledged full-file upload. If the
            # process stops in upload_pending, do not guess whether the engine
            # received that file; keep the job held for inspection.
            self.store.update(task['uuid'], phase='uploaded', upload_cursor=index + 1)
        self.store.update(task['uuid'], phase='commit_pending')
        engine.action('commit', task['engine_uuid'])
        # Commit acceptance does not mean ODM has begun processing yet.
        self.store.update(task['uuid'], phase='awaiting_engine_start')
        self._poll(self.store.task(task['uuid']))

    def _resume_precommit(self, task):
        """Resume only restart phases whose last engine mutation is known."""
        engine = self.engines[task['node_id']]
        phase = task['phase']
        if phase == 'claimed':
            # The engine UUID was durably assigned before any engine request.
            self.store.update(task['uuid'], phase='init_pending')
            task = self.store.task(task['uuid'])
            phase = task['phase']
        if phase == 'init_pending':
            info = engine.read('/task/' + task['engine_uuid'] + '/info', missing=True)
            if info is None:
                engine.initialize(task)
            elif (not isinstance(info, dict) or info.get('uuid') != task['engine_uuid']
                  or not isinstance(info.get('status'), dict)
                  or info['status'].get('code') != 10):
                raise SmokeError('engine initialization recovery uncertain')
            self.store.update(task['uuid'], phase='initialized', upload_cursor=0)
        elif phase in ('initialized', 'uploaded'):
            info = engine.read('/task/' + task['engine_uuid'] + '/info', missing=True)
            if info is None:
                # No engine task exists, so none of the previously acknowledged
                # uploads remain. Reinitialize the same reserved UUID and replay
                # only these known-absent side effects from the retained source.
                engine.initialize(task)
                self.store.update(task['uuid'], phase='initialized', upload_cursor=0)
            elif (not isinstance(info, dict) or info.get('uuid') != task['engine_uuid']
                  or not isinstance(info.get('status'), dict)
                  or info['status'].get('code') != 10):
                raise SmokeError('engine upload recovery uncertain')
        else:
            raise SmokeError('submission phase is not safely resumable')
        self._upload_and_commit(self.store.task(task['uuid']))

    def tick(self):
        """One bounded scheduler turn; public for deterministic synthetic tests."""
        with self._lock:
            for task in self.store.list_tasks():
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
                        self._resume_precommit(task)
                    except EngineUnavailable:
                        failures += 1
                        self._backoff[task['uuid']] = (
                            time.monotonic() + min(60, 2 ** min(failures, 6)), failures)
                    except Exception:
                        self._hold(task, 'submission_interrupted')
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
            catalogue = self._catalog()
            snapshots = self._node_snapshots(catalogue)
            if not catalogue.get('ready'):
                return
            active_nodes = set()
            for job in self.queue.list():
                if job['status'] in ('running', 'held'):
                    if job['nodeId'] not in self.node_order:
                        # An active job without a configured physical owner
                        # fences all dispatch until an operator reconciles it.
                        return
                    active_nodes.add(job['nodeId'])

            for node in self.nodes:
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
                task_by_job = {task['job']['id']: task for task in self.store.list_tasks()
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
                job = self.queue.claim({'workerId': 'viewer-dispatch', 'nodeId': node.node_id,
                    'profiles': [VIEWER_PROFILE], 'expectedJobId': candidate['id']},
                    eligible_job_ids=eligible_job_ids, eligible_node_ids=eligible_nodes,
                    expected_queue_sequence=queue_snapshot['sequence'])
                if job is None:
                    # A concurrent submission or claim invalidated this
                    # snapshot; do not try a lower-priority selection this turn.
                    return
                active_nodes.add(node.node_id)
                self._start_claimed(task, job, node.node_id)

    def start(self):
        if self._thread:
            raise RuntimeError('dispatcher already started')
        self._thread = threading.Thread(target=self._run, name='viewer-dispatch', daemon=True)
        self._thread.start()

    def _run(self):
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                # No replay of mutations. Intents/claims remain durable; a
                # restart retains their slot for reconciliation or inspection.
                pass
            self._stop.wait(2)

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
