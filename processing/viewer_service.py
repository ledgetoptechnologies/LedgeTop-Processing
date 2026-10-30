"""Native Viewer facade service and cooperative, single-slot engine dispatcher.

Only fixed configured engines are contacted. Mutation intent is durable before
HTTP; uncertain mutations hold the shared slot instead of replaying a job.
Completed engine tasks and downloaded archives remain until operator cleanup.
"""

import http.client
import hashlib
import json
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

from .maintenance import Conflict
from .odx_smoke_worker import Client, SmokeError
from .options import catalog, discover_node, _NoRedirect
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
                                       config.viewer_max_input_bytes)
        self.engines = engines or {'cluster': NativeEngine(config.local_node_url, config.local_node_token),
                                  'main': NativeEngine(config.main_node_url, config.main_node_token)}
        self.probe = probe
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None
        self._backoff = {}
        self._downloads = {}

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
                'ledgetopReservations': self.store.reservation_stats(),
                'taskQueueCount': sum(j['status'] in ('queued', 'running', 'held') for j in jobs)}

    def _selected_options(self, node_id):
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

    def _check_options(self, options, *, node_id=None, evidence=None):
        # Legacy tasks lack admission evidence and retain the conservative
        # fleet-wide intersection check.
        definitions = self._selected_options(node_id) if node_id is not None and evidence is not None else self.options()
        allowed = {item['name'] for item in definitions}
        if (not isinstance(options, list) or any(not isinstance(item, dict)
                or item.get('name') not in allowed for item in options)):
            raise ValueError('option unavailable on configured engines')
        selected = [item for item in definitions if item['name'] in {option['name'] for option in options}]
        if evidence is not None and json.dumps(sorted(selected,key=lambda item:item['name']),sort_keys=True,separators=(',',':'),allow_nan=False) != json.dumps(evidence,sort_keys=True,separators=(',',':'),allow_nan=False):
            raise Conflict('selected engine option definitions changed')
        return selected

    def initialize(self, identifier, name='', options=None, outputs=None, environment='production', capability=None):
        evidence = self._check_options(options or [])
        return self.store.initialize(identifier, name, options, outputs, environment, capability, option_evidence=evidence)

    def upload(self, identifier, filename, stream, length, environment=None, capability=None):
        return self.store.upload(identifier, filename, stream, length, environment, capability)

    def commit(self, identifier, environment=None, capability=None):
        return self.store.commit(identifier, environment, capability)

    def info(self, identifier):
        return self.store.info(identifier)

    def reserve(self, identifier, environment, capability):
        with self._lock:return self.store.reserve(identifier,environment,capability)

    def cancel_reserved(self, identifier, environment, capability):
        with self._lock:return self.store.cancel_reservation(identifier,environment,capability)

    def authorize(self, identifier, environment, capability=None):
        self.store.authorize_reservation(identifier,environment,capability)
        if self.store.task_environment(identifier) != environment:
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

    def _archive_identity(self, task):
        return {'version': 1, 'task': task['uuid'], 'generation': task['generation'],
                'job': task['job_id'], 'node': task['node_id'], 'engine': task['engine_uuid']}

    def _read_archive_metadata(self, path):
        try:
            if path.is_symlink(): raise ValueError()
            fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
            with os.fdopen(fd, 'rb') as stream:
                info = os.fstat(stream.fileno())
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 4096
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
                with self.engines[task['node_id']].download(task['engine_uuid']) as response:
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
        if not task['job'] or task['job']['status'] != 'succeeded':
            raise Conflict('task is not complete')
        with self._lock:
            lock = self._downloads.setdefault(identifier, threading.Lock())
        with lock:
            task = self.store.task(identifier)
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
                else:
                    if partial.exists() or partial.is_symlink():
                        raise Conflict('partial archive without intent requires inspection')
                    with intent_path.open('xb') as marker:
                        os.chmod(intent_path, 0o600)
                        marker.write(json.dumps(self._archive_identity(task),sort_keys=True,separators=(',',':')).encode('ascii'))
                        marker.flush();os.fsync(marker.fileno())
                    sync_directory(path.parent)
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
                with receipt_path.open('xb') as marker:
                    os.chmod(receipt_path, 0o600)
                    marker.write(json.dumps(receipt,sort_keys=True,separators=(',',':')).encode('ascii'))
                    marker.flush();os.fsync(marker.fileno())
                sync_directory(path.parent)
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

    def cancel(self, identifier):
        with self._lock:
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
        if code == 50:
            # A native cancellation status does not prove that the engine's
            # processes have stopped. Preserve the global slot until an
            # independent physical termination check can resolve the hold.
            self._hold(task, 'cancel_termination_unverified')
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
            jobs = self.queue.list()
            for job in jobs:
                if job['profile'] != VIEWER_PROFILE or job['status'] not in ('running', 'held'):
                    continue
                if job['status'] == 'held' and job['heldReason'] != 'coordinator_restart':
                    continue
                task = self.store.task_for_job(job['id'])
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
            task = self.store.task_for_job(queued[0]['id'])
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
            # Submission uses the fleet-wide common catalog. At dispatch,
            # refresh the selected engine specifically so an unrelated node
            # outage does not strand an already accepted job.
            self._check_options(task['options'], node_id=node_id, evidence=task['option_evidence'])
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
