"""Native Viewer facade service and cooperative, single-slot engine dispatcher.

Only fixed configured engines are contacted. Mutation intent is durable before
HTTP; uncertain mutations hold the shared slot instead of replaying a job.
Completed engine tasks and downloaded archives remain until operator cleanup.
"""

import http.client
from contextlib import nullcontext
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

from .maintenance import Conflict
from .odx_smoke_worker import Client, SmokeError
from .options import catalog, _NoRedirect
from .policy import plan
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
        if (not isinstance(result, dict) or result.get('uuid') != task['engine_uuid']
                or 'error' in result or ('success' in result and result['success'] is not True)):
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
        if (not isinstance(result, dict) or 'error' in result or ('success' in result and result['success'] is not True)
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
                                       config.viewer_max_input_bytes)
        self.engines = engines or {'cluster': NativeEngine(config.local_node_url, config.local_node_token),
                                  'main': NativeEngine(config.main_node_url, config.main_node_token)}
        self.probe = probe
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None
        self._backoff = {}
        self._downloads = {}
        self.spool_admission = None

    def enable_spool_admission(self,capacity):
        from .viewer_spool_capacity import SpoolCapacity
        if (type(capacity) is not SpoolCapacity or type(self.store) is not TaskStore
                or capacity.queue is not self.queue or capacity.tasks!=self.store.root
                or self._thread is not None or self.spool_admission is not None):
            raise ValueError('shared pre-start spool admission required')
        self.spool_admission=capacity;self.store.spool_admission=capacity

    def upload_window(self,identifier,length):
        if self.spool_admission is None:return nullcontext()
        task=self.store.task(identifier)
        if task['stage']!='uploading' or task['job_id']:
            raise Conflict('task is no longer accepting uploads')
        return self.spool_admission.window('upload',identifier,task['generation'],length)

    def init_window(self,identifier,length):
        if self.spool_admission is None:return nullcontext()
        with self.queue._db() as db:
            row=db.execute('SELECT generation,removed FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone()
        generation=1 if row is None else row['generation']+(1 if row['removed'] else 0)
        return self.spool_admission.window('form',identifier,generation,length)

    def _catalog(self):
        return catalog([('local', self.config.local_node_url, self.config.local_node_token),
                        ('main', self.config.main_node_url, self.config.main_node_token)])

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
                'maxImages': None, 'maxParallelTasks': 1, 'coordinator': True,
                'taskQueueCount': sum(j['status'] in ('queued', 'running', 'held') for j in jobs)}

    def _check_options(self, options):
        allowed = {item['name'] for item in self.options()}
        if (not isinstance(options, list) or any(not isinstance(item, dict)
                or item.get('name') not in allowed for item in options)):
            raise ValueError('option unavailable on configured engines')

    def initialize(self, identifier, name='', options=None, outputs=None, environment='production'):
        self._check_options(options or [])
        return self.store.initialize(identifier, name, options, outputs, environment)

    def upload(self, identifier, filename, stream, length):
        return self.store.upload(identifier, filename, stream, length)

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
                with self.engines[task['node_id']].download(task['engine_uuid']) as response:
                    if getattr(response, 'status', 200) != 200:
                        raise EngineUnavailable('archive_unavailable')
                    media_type = response.headers.get('Content-Type', '').split(';')[0].lower()
                    if media_type and media_type not in ('application/zip', 'application/octet-stream',
                                                          'application/x-zip-compressed'):
                        raise EngineUnavailable('archive_not_ready')
                    declared = response.headers.get('Content-Length')
                    if declared is not None and (not declared.isdigit() or int(declared) > self.config.viewer_max_archive_bytes):
                        raise Conflict('archive exceeds configured disk allowance')
                    bound=int(declared) if declared is not None else self.config.viewer_max_archive_bytes
                    admission=(self.spool_admission.window('archive',identifier,task['generation'],max(1,bound))
                        if self.spool_admission is not None else nullcontext())
                    with admission, partial.open('xb') as output:
                        os.chmod(partial, 0o600)
                        while chunk := response.read(1024 ** 2):
                            total += len(chunk)
                            if total > self.config.viewer_max_archive_bytes:
                                raise Conflict('archive exceeds configured disk allowance')
                            if self.spool_admission is not None:
                                self.spool_admission.consume('archive',identifier,task['generation'],len(chunk))
                            output.write(chunk)
                        if declared is not None and total != int(declared):
                            raise EngineUnavailable('archive_transfer_incomplete')
                        output.flush()
                        os.fsync(output.fileno())
                if total < 4 or not zipfile.is_zipfile(partial):
                    raise EngineUnavailable('archive_transfer_incomplete')
                os.replace(partial, path)
                sync_directory(path.parent)
                self.store.update(identifier, archive_path=str(path))
            if path.is_symlink() or not path.is_file():
                raise Conflict('retained archive unavailable')
            return path.open('rb'), path.stat().st_size

    def cancel(self, identifier):
        with self._lock:
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

    def operator(self, identifier, action, request):
        """Serialize operator state changes with Viewer engine mutations."""
        with self._lock:
            return self.queue.operator(identifier, action, request)

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
            if code == 20 and task['phase'] == 'commit_pending':
                fields.update(phase='processing', error=None)
            self.store.update(identifier, **fields)
            return
        status = 'succeeded' if code == 40 else 'failed'
        result = {'engineTaskId': task['engine_uuid'], 'nodeId': task['node_id'],
                  'environment': task['environment'], 'cancelled': code == 50}
        self.queue.complete(task['job_id'], {'claimToken': task['claim_token'], 'status': status, 'result': result})
        self.store.update(identifier, stage='cancelled' if code == 50 else status,
                          phase='complete', progress=100 if code == 40 else task['progress'])

    def tick(self):
        """One bounded scheduler turn; public for deterministic synthetic tests."""
        with self._lock:
            tasks = self.store.list_tasks()
            for task in tasks:
                job = task['job']
                if not job or job['status'] not in ('running', 'held'):
                    continue
                if job['status'] == 'held' and job['heldReason'] != 'coordinator_restart':
                    continue
                if (task['phase'] in ('processing', 'commit_pending', 'cancel_pending')
                        and task['engine_uuid'] and task['node_id'] and task['claim_token']):
                    self._poll(task)
                else:
                    self._hold(task, 'submission_interrupted')
            jobs = self.queue.list()
            if any(j['status'] in ('running', 'held') for j in jobs):
                return
            queued = sorted((j for j in jobs if j['status'] == 'queued'),
                            key=lambda j: (j['environment'] != 'production', jobs.index(j)))
            if not queued or queued[0]['profile'] != VIEWER_PROFILE:
                return
            task = next((t for t in tasks if t['job_id'] == queued[0]['id']), None)
            if task is None:
                raise Conflict('execution metadata unavailable')
            nodes = {}
            for node, role, origin, token in (
                    ('cluster', 'local', self.config.local_node_url, self.config.local_node_token),
                    ('main', 'main', self.config.main_node_url, self.config.main_node_token)):
                nodes[node] = {**self.probe(role, origin, token),
                               'state': self.queue.get_node(node)['state']}
            decision = plan(task['images_count'], task['input_bytes'], nodes['cluster'], nodes['main'],
                            self.config.local_max_photos, self.config.local_max_input_bytes)
            if decision['decision'] == 'wait':
                self.store.update(task['uuid'], phase='waiting_node', error=decision['reason'])
                return
            node_id = 'cluster' if decision['decision'] == 'local' else 'main'
            self._check_options(task['options'])
            job = self.queue.claim({'workerId': 'viewer-dispatch', 'nodeId': node_id,
                                   'profiles': [VIEWER_PROFILE], 'expectedJobId': task['job_id']})
            if not job:
                return
            try:
                self.store.update(task['uuid'], stage='running', phase='claimed', node_id=node_id,
                                  claim_token=job['claimToken'], engine_uuid=str(uuid.uuid4()), error=None)
                task = self.store.task(task['uuid'])
                engine = self.engines[node_id]
                self.store.update(task['uuid'], phase='init_pending')
                engine.initialize(task)
                self.store.update(task['uuid'], phase='initialized')
                for item in task['files']:
                    self.store.update(task['uuid'], phase='upload_pending')
                    engine.upload(task['engine_uuid'], item)
                    self.store.update(task['uuid'], phase='uploaded')
                self.store.update(task['uuid'], phase='commit_pending')
                engine.action('commit', task['engine_uuid'])
                self.store.update(task['uuid'], phase='processing')
            except Exception:
                current = self.store.task(task['uuid'])
                if current['phase'] == 'commit_pending':
                    # Exact assigned engine UUID lets us reconcile a lost commit
                    # ACK by reads. Never send commit again under uncertainty.
                    self.store.update(task['uuid'], error='commit_ack_uncertain')
                else:
                    self._hold(current, 'engine_submission_uncertain')

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
