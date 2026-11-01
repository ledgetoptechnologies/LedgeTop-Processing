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
import subprocess
import stat
import sys
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, build_opener, ProxyHandler
import uuid
import zipfile

from .maintenance import Conflict
from . import config as runtime_config
from .odx_smoke_worker import Client, SmokeError, strict_wire_json
from .options import catalog, discover_node, _NoRedirect
from .policy import plan
from .prototype import VIEWER_PROFILE
from .provider import probe_node
from .viewer_tasks import ArchiveClaimHeld, TaskStore, sync_directory
from .viewer_limits import MAX_UPLOAD_BYTES


class ArchiveIdentityChanged(Exception):
    """Capture candidate no longer denotes the discovered task incarnation."""


class ArchiveInspectionHeld(Conflict):
    """A durable inspection hold forbids automatic or manual archive mutation."""


class EngineUnavailable(OSError):
    pass


class OptionCatalogUnavailable(Conflict):
    pass


class OptionDefinitionChanged(Conflict):
    pass


class OptionMissing(ValueError):
    pass


def _automatic_archive_length(headers, maximum):
    """Require one unambiguous fixed-length response before reserving output."""
    get_all = getattr(headers, 'get_all', None)
    if callable(get_all):
        lengths = get_all('Content-Length', [])
        transfers = get_all('Transfer-Encoding', [])
    else:
        length = headers.get('Content-Length')
        transfer = headers.get('Transfer-Encoding')
        lengths = [] if length is None else [length]
        transfers = [] if transfer is None else [transfer]
    if len(lengths) != 1 or transfers or not isinstance(lengths[0], str):
        raise Conflict('archive size unavailable for automatic capture')
    value = lengths[0].strip()
    if not value.isascii() or not value.isdecimal() or len(value) > 20:
        raise Conflict('archive size unavailable for automatic capture')
    size = int(value)
    if not 4 <= size <= maximum:
        raise Conflict('archive size unavailable for automatic capture')
    return size


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
                lengths=response.headers.get_all('Content-Length',[])
                transfers=response.headers.get_all('Transfer-Encoding',[])
                if (len(lengths)>1 or len(transfers)>1 or (lengths and transfers)
                    or (transfers and transfers[0].strip().lower()!='chunked')
                    or (not lengths and not transfers)):
                    raise ValueError()
                declared=None
                if lengths:
                    value=lengths[0].strip()
                    if not value.isascii() or not value.isdecimal():raise ValueError()
                    declared=int(value)
                    if declared>limit:raise ValueError()
                raw = response.read(limit + 1)
                if len(raw) > limit or (declared is not None and len(raw)!=declared):
                    raise ValueError()
                value = strict_wire_json(raw)
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
        result = strict_wire_json(raw)
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
            result = strict_wire_json(raw)
        if (not isinstance(result, dict) or 'error' in result or ('success' in result and result['success'] is not True)
                or ('uuid' in result and result['uuid'] != identifier)
                or (action == 'commit' and result.get('success') is not True and result.get('uuid') != identifier)):
            raise SmokeError('engine mutation acknowledgment uncertain')
        return result

    def download(self, identifier):
        return build_opener(ProxyHandler({}), _NoRedirect()).open(
            self.origin + self.route('/task/' + identifier + '/download/all.zip'), timeout=60)


class ViewerService:
    def __init__(self, queue, config, *, store=None, engines=None, probe=probe_node, guarded_engines=None):
        if guarded_engines is not None:
            from . import guarded_native_engine, engine_upload_contract
            if guarded_native_engine.QUALIFIED is not True or engine_upload_contract.QUALIFIED is not True:
                raise Conflict('guarded dispatcher not qualified')
            if engines is not None or type(guarded_engines) is not dict or set(guarded_engines)!={'cluster','main'}:
                raise Conflict('exact guarded engine mapping required')
        self._guarded_engines_enabled = guarded_engines is not None
        self.queue, self.config = queue, config
        self._archive_registry = getattr(config, 'viewer_archive_registry', False)
        if type(self._archive_registry) is not bool:
            raise ValueError('explicit archive registry setting required')
        if self._archive_registry:
            if runtime_config.ARCHIVE_REGISTRY_QUALIFIED is not True:
                raise RuntimeError('archive registry is not qualified')
            lease = getattr(queue, 'authority_lease', None)
            if (queue is None or lease is None or getattr(lease, '_descriptor', None) is None
                    or queue._lock.closed):
                raise RuntimeError('archive registry requires coordinator authority')
        if queue is not None and not self._archive_registry:
            with queue._db() as db:
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_archive_registry'").fetchone():
                    raise RuntimeError('archive registry configuration required')
        self.store = store or TaskStore(queue, Path(config.prototype_state_dir) / 'viewer-tasks',
                                       config.viewer_max_input_bytes)
        self._durable_archive_claims = getattr(config, 'viewer_durable_archive_claims', False)
        if type(self._durable_archive_claims) is not bool:
            raise ValueError('explicit durable archive claims setting required')
        if queue is not None:
            if self._durable_archive_claims:
                self.store.require_archive_claim_schema()
            else:
                with queue._db() as db:
                    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_archive_claims_v1'").fetchone():
                        raise RuntimeError('durable archive claims configuration required')
        if self._archive_registry:
            self.store.enable_archive_registry()
        if guarded_engines is not None:
            from .guarded_native_engine import GuardedNativeEngine
            self.engines={node:GuardedNativeEngine(self.store,guard) for node,guard in guarded_engines.items()}
            if (any(engine.guard.binding.node_id!=node for node,engine in self.engines.items())
                or len({engine.guard.binding.engine_id for engine in self.engines.values()})!=2):
                raise Conflict('guarded node identity differs')
        else:
            self.engines = engines or {'cluster': NativeEngine(config.local_node_url, config.local_node_token),
                                      'main': NativeEngine(config.main_node_url, config.main_node_token)}
        self.probe = probe
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._archive_wake = threading.Event()
        self._thread = None
        self._unexpected_dispatch_failure = threading.Event()
        self._backoff = {}
        self._downloads = {}
        # Do not make completed downloads wait on a different task's dispatch.
        # The per-task locks still serialize manual and automatic capture.
        self._downloads_lock = threading.Lock()
        self._archive_thread = None
        self._archive_cursor = 0
        self._archive_retry = {}
        self._archive_volatile_holds = set()
        self._archive_status_persistence_failed = False
        self._archive_child_unreaped = threading.Event()
        self._archive_child_lock = threading.Lock()
        self._archive_uncertain_process = None

    def _archive_task_lock(self, identifier):
        if isinstance(self.store, TaskStore):
            return self.store.archive_lock(identifier)
        # Private capture-child stores have no shared durable status writer.
        with self._downloads_lock:
            return self._downloads.setdefault(identifier, threading.RLock())

    def _archive_inspection_held(self, task):
        identity = (task['uuid'], task['generation'], task['job_id'])
        return task.get('archive_state') == 'inspection' or identity in self._archive_volatile_holds

    def _publish_captured_archive(self, identifier, task, path, claim):
        if self._durable_archive_claims:
            return self.store.update(identifier, expected_archive_task=task,
                                     expected_archive_claim=claim, archive_path=str(path))
        return self.store.update(identifier, expected_archive_task=task, archive_path=str(path))

    def _reserve_archive(self, output, size):
        """Reserve output bytes; the free-space floor is a point-in-time check."""
        if type(size) is not int or not 4 <= size <= self.config.viewer_max_archive_bytes:
            raise Conflict('archive size unavailable for automatic capture')
        space = os.fstatvfs(output.fileno())
        if space.f_bavail * space.f_frsize < size + self.config.viewer_archive_free_floor:
            raise Conflict('archive capacity unavailable')
        os.posix_fallocate(output.fileno(), 0, size)

    def _archive_once(self, identifier, expected_identity=None):
        """Serialize capture children and refuse after uncertain termination."""
        with self._archive_task_lock(identifier):
            with self._archive_child_lock:
                if self._archive_child_unreaped.is_set():
                    raise RuntimeError('archive capture child termination uncertain')
                return self._archive_once_locked(identifier,expected_identity=expected_identity)

    def _archive_once_locked(self, identifier, expected_identity=None):
        """Run file capture in a reaped child that inherits coordinator flocks."""
        with self._archive_task_lock(identifier):
            if self._stop.is_set():
                return
            task = self.store.task(identifier)
            if expected_identity is not None and (task['generation'], task['job_id']) != expected_identity:
                raise ArchiveIdentityChanged()
            # Discovery and capture are separate operations.  An inspection
            # hold established after the scan must still stop this attempt.
            if self._archive_inspection_held(task):
                raise ArchiveInspectionHeld('archive requires inspection')
            if not task['job'] or task['job']['status'] != 'succeeded' or task['archive_path']:
                return
            lease = getattr(self.queue, 'authority_lease', None)
            if lease is None or lease._descriptor is None or self.queue._lock.closed:
                raise RuntimeError('archive capture requires coordinator authority')
            if self._archive_registry:
                self.store.archive_registry_check(task, register=True)
            node = task['node_id']
            if node not in ('cluster', 'main'):
                raise Conflict('archive engine identity unavailable')
            origin = self.config.local_node_url if node == 'cluster' else self.config.main_node_url
            token = self.config.local_node_token if node == 'cluster' else self.config.main_node_token
            fields = ('uuid', 'generation', 'job_id', 'node_id', 'engine_uuid', 'environment', 'directory', 'archive_path')
            capture_task = {key: task[key] for key in fields}
            capture_task['job'] = {'status': 'succeeded'}
            payload = {'task': capture_task, 'origin': origin, 'token': token,
                       'maxBytes': self.config.viewer_max_archive_bytes, 'freeFloor': self.config.viewer_archive_free_floor}
            raw = json.dumps(payload, separators=(',', ':'), allow_nan=False).encode()
            if len(raw) > 16384:
                raise Conflict('archive capture metadata exceeds bound')
            claim = self.store.claim_archive(task) if self._durable_archive_claims else None
            env = {key: os.environ[key] for key in ('PATH', 'LANG', 'LC_ALL') if key in os.environ}
            process = subprocess.Popen([sys.executable, '-m', 'processing.viewer_archive_worker'],
                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                cwd=str(Path(__file__).resolve().parent.parent), env=env,
                pass_fds=(self.queue._lock.fileno(), lease._descriptor), start_new_session=True)
            failure = None
            try:
                # Do not block shutdown if the child cannot read its input.
                deadline = time.monotonic() + 6 * 60 * 60
                os.set_blocking(process.stdin.fileno(), False)
                remaining = memoryview(raw)
                while remaining and not self._stop.is_set() and time.monotonic() < deadline:
                    try:
                        sent = os.write(process.stdin.fileno(), remaining)
                        if sent <= 0: raise BrokenPipeError()
                        remaining = remaining[sent:]
                    except BlockingIOError:
                        self._stop.wait(.1)
                process.stdin.close()
                while not remaining and process.poll() is None and not self._stop.wait(.25) and time.monotonic() < deadline:
                    pass
                if process.poll() is None:
                    process.kill()
                try:
                    code = process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Keep the per-task lock until the killed child is reaped.
                    # close() may report failure after its deadline; inherited
                    # coordinator flocks also remain held by any live child.
                    process.wait()
                    raise RuntimeError('archive capture child not reaped in time') from None
                if code != 0 or self._stop.is_set():
                    raise EngineUnavailable('archive capture incomplete')
                path = Path(task['directory']) / 'all.zip'
                receipt = self._archive_receipt(task, path.with_suffix('.zip.receipt'))
                current = path.lstat()
                if (path.is_symlink() or not stat.S_ISREG(current.st_mode) or current.st_size != receipt['bytes']
                        or (os.name != 'nt' and current.st_uid != os.geteuid())):
                    raise Conflict('captured archive identity changed')
                # Bind publication to the captured incarnation in the same DB transaction.
                self._publish_captured_archive(identifier, task, path, claim)
            except Exception as exc:
                failure = exc
            finally:
                try:
                    if process.stdin and not process.stdin.closed:
                        process.stdin.close()
                except Exception as exc:
                    if failure is None:
                        failure = exc
                finally:
                    try:
                        if process.poll() is None:
                            try: process.kill()
                            except ProcessLookupError: pass
                            process.wait(timeout=5)
                    except Exception:
                        # This child may still hold inherited coordinator locks
                        # and write state. Never report a clean shutdown.
                        self._archive_uncertain_process = process
                        self._archive_child_unreaped.set()
                        raise RuntimeError('archive capture child termination uncertain') from None
            if failure is not None:
                # The child is now reaped even if writing stdin, waiting, or
                # shutdown failed. Never infer retry safety from an incomplete
                # writer or modify any partial bytes.
                uncertain = process.poll() == 2
                for name in ('all.zip.partial', '.all.zip.restore', '.all.zip.restore-pending'):
                    try:
                        (Path(task['directory']) / name).lstat()
                    except FileNotFoundError:
                        continue
                    except OSError:
                        uncertain = True
                        break
                    else:
                        uncertain = True
                        break
                if uncertain:
                    raise Conflict('partial_requires_inspection') from None
                raise failure

    def _archive_run(self):
        deferred = 0
        while not self._stop.is_set():
            try:
                # Clear before the durable scan so a completion racing with the
                # scan cannot be lost between observing no work and waiting.
                self._archive_wake.clear()
                # close() may have set both events immediately before clear().
                if self._stop.is_set():
                    break
                candidate = self.store.next_unarchived_success(self._archive_cursor)
                if candidate is None:
                    self._archive_wake.wait(2)
                    continue
                previous_cursor = self._archive_cursor
                self._archive_cursor, identifier = candidate
                wrapped = self._archive_cursor <= previous_cursor
                def defer():
                    nonlocal deferred
                    deferred += 1
                    # Skip delayed entries promptly; yield periodically and
                    # avoid a tight loop once a whole scan finds no work.
                    if wrapped:
                        self._stop.wait(.25)
                        deferred = 0
                    elif deferred >= 64:
                        self._stop.wait(.1)
                        deferred = 0
                identity = None
                prior = self._archive_retry.get(identifier)
                # Without a readable identity we cannot distinguish a replacement;
                # honor the bounded metadata-failure delay before trying again.
                if prior is not None and prior[0] is None and time.monotonic() < prior[1]:
                    defer()
                    continue
                due, failures = (prior[1], prior[2]) if prior is not None and prior[0] is None else (0, 0)
                # Keep the per-task lock through persistence of a failed capture.
                with self._archive_task_lock(identifier):
                    try:
                        task = self.store.task(identifier)
                        identity = (task['generation'], task['job_id'])
                        prior = self._archive_retry.get(identifier)
                        if prior is None or prior[0] != identity:
                            # A public UUID can be reused. Once identity is readable,
                            # discard a different incarnation's retry delay.
                            self._archive_retry.pop(identifier, None)
                            due, failures = 0, 0
                        else:
                            _, due, failures = prior
                        if time.monotonic() < due:
                            defer()
                            continue
                        deferred = 0
                        self._archive_once(identifier, expected_identity=identity)
                        self._archive_retry.pop(identifier, None)
                    except ArchiveIdentityChanged:
                        # The new incarnation has not been attempted; discover it anew.
                        self._archive_retry.pop(identifier, None)
                    except (ArchiveInspectionHeld, ArchiveClaimHeld):
                        # Do not downgrade an inspection hold to a retryable error.
                        self._archive_retry.pop(identifier, None)
                    except Exception as exc:
                        if self._archive_child_unreaped.is_set():
                            # Never launch a second writer when an earlier child
                            # may still own the coordinator's inherited locks.
                            self._archive_status_persistence_failed = True
                            return
                        if identity is None:
                            # No trustworthy task identity can be associated with this failure.
                            # Preserve bounded retry for an unavailable metadata store.
                            self._archive_status_persistence_failed = True
                            failures += 1
                            self._archive_retry[identifier] = (None, time.monotonic() + min(300, 2 ** min(failures, 8)), failures)
                        else:
                            inspection = isinstance(exc, Conflict) and str(exc) == 'partial_requires_inspection'
                            try:
                                recorded = self.store.set_archive_status(identifier, *identity,
                                    'inspection' if inspection else 'pending',
                                    'partial_requires_inspection' if inspection else 'capture_failed')
                                if not recorded:
                                    # The incarnation changed or was removed; do not
                                    # apply its retry delay to the replacement.
                                    self._archive_retry.pop(identifier, None)
                                    continue
                            except Exception:
                                self._archive_status_persistence_failed = True
                                if inspection:
                                    # Fail closed within this process if durable
                                    # hold persistence is unavailable.
                                    self._archive_volatile_holds.add((identifier, *identity))
                            failures += 1
                            self._archive_retry[identifier] = (identity, time.monotonic() + min(300, 2 ** min(failures, 8)), failures)
                self._stop.wait(.25)
            except Exception:
                self._archive_status_persistence_failed = True
                self._stop.wait(2)

    def _catalog(self):
        return catalog([('local', self.config.local_node_url, self.config.local_node_token),
                        ('main', self.config.main_node_url, self.config.main_node_token)])

    def options(self):
        current = self._catalog()
        if not current['ready']:
            raise OptionCatalogUnavailable('engine catalog unavailable')
        return current['commonOptions']

    def capabilities(self):
        # This is our own 2.x-compatible facade, not an official ClusterODM build.
        self.options()
        jobs = self.queue.list()
        return {'version': '2.4.0', 'engine': 'ledgetop-processing', 'engineVersion': '0.2.0',
                'maxImages': None, 'maxParallelTasks': 1, 'coordinator': True,
                'ledgetopArchiveHealth': self._archive_health(),
                'ledgetopMaxInputBytes': self.store.max_input_bytes,
                'ledgetopMaxUploadRequestBytes': MAX_UPLOAD_BYTES,
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
            raise OptionCatalogUnavailable('selected engine options unavailable')
        return snapshot['options']

    def _check_options(self, options, *, node_id=None, evidence=None):
        # Legacy tasks lack admission evidence and retain the conservative
        # fleet-wide intersection check.
        definitions = self._selected_options(node_id) if node_id is not None and evidence is not None else self.options()
        allowed = {item['name'] for item in definitions}
        if (not isinstance(options, list) or any(not isinstance(item, dict)
                or item.get('name') not in allowed for item in options)):
            raise OptionMissing('option unavailable on configured engines')
        selected = [item for item in definitions if item['name'] in {option['name'] for option in options}]
        if evidence is not None and json.dumps(sorted(selected,key=lambda item:item['name']),sort_keys=True,separators=(',',':'),allow_nan=False) != json.dumps(evidence,sort_keys=True,separators=(',',':'),allow_nan=False):
            raise OptionDefinitionChanged('selected engine option definitions changed')
        return selected

    def initialize(self, identifier, name='', options=None, outputs=None, environment='production', capability=None):
        if capability is not None:
            replay = self.store.initialize(identifier, name, options, outputs, environment, capability, replay_only=True)
            if replay is not None: return replay
        evidence = self._check_options(options or [])
        return self.store.initialize(identifier, name, options, outputs, environment, capability, option_evidence=evidence)

    def upload(self, identifier, filename, stream, length, environment=None, capability=None):
        return self.store.upload(identifier, filename, stream, length, environment, capability)

    def commit(self, identifier, environment=None, capability=None):
        return self.store.commit(identifier, environment, capability)

    def _archive_health(self):
        if self._archive_status_persistence_failed:
            return {'state': 'inspection', 'reason': 'status_persistence_failed'}
        if getattr(self.config, 'viewer_archive_autocapture', False):
            # No durable proof exists that a previous process persisted every
            # failure; do not clear this warning merely because we restarted.
            return {'state': 'inspection', 'reason': 'status_unverified_after_restart'}
        return {'state': 'disabled', 'reason': None}

    def info(self, identifier):
        info = self.store.info(identifier)
        info['ledgetopArchiveHealth'] = self._archive_health()
        if info['status']['code'] == 40:
            task = self.store.task(identifier)
            if self._durable_archive_claims and self.store.archive_claim_held(task):
                archive = {'state': 'claimed', 'reason': 'capture_active_or_unresolved'}
            elif task['archive_path']:
                archive = {'state': 'recorded', 'reason': None}
            elif not getattr(self.config, 'viewer_archive_autocapture', False):
                archive = {'state': 'disabled', 'reason': None}
            else:
                archive = {'state': task.get('archive_state', 'pending'), 'reason': task.get('archive_reason')}
            info['ledgetopArchive'] = archive
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

    @staticmethod
    def _archive_receipt_digest(receipt):
        return hashlib.sha256(json.dumps(receipt, sort_keys=True, separators=(',', ':'),
                                         allow_nan=False).encode('ascii')).hexdigest()

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

    def _restore_recorded_archive(self, task, path, receipt, *, automatic=False):
        """Restore only bytes matching the existing task-bound receipt."""
        directory = Path(task['directory'])
        if path != directory / 'all.zip' or directory.resolve(strict=True) != directory:
            raise Conflict('retained archive path requires inspection')
        # A single deterministic recovery slot prevents repeated crashes from
        # creating an unbounded number of archives. The per-task lock and the
        # single-coordinator fence remain necessary for safe serialization.
        temporary = directory / '.all.zip.restore-pending'
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
        stream = None
        fd = None
        try:
            try:
                temporary.lstat()
            except FileNotFoundError:
                existing = False
            else:
                existing = True
            if existing:
                # Existing data is never replaced; only a complete slot bound
                # to the exact receipt is eligible for verified publication.
                stream = self._verify_archive(temporary, receipt)
                original = os.fstat(stream.fileno())
                if original.st_nlink != 1:
                    raise Conflict('previous archive restore requires inspection')
            else:
                # Open and validate the upstream response before making an
                # artifact. Connection/HTTP/header failures can then retry
                # without erasing an ambiguous local slot. The exclusive open
                # below still refuses a racing creator after this observation.
                with self.engines[task['node_id']].download(task['engine_uuid']) as response:
                    if getattr(response, 'status', 200) != 200:
                        raise EngineUnavailable('archive_unavailable')
                    media = response.headers.get('Content-Type', '').split(';')[0].lower()
                    if media and media not in ('application/zip','application/octet-stream','application/x-zip-compressed'):
                        raise EngineUnavailable('archive_not_ready')
                    declared = response.headers.get('Content-Length')
                    if automatic and _automatic_archive_length(response.headers, self.config.viewer_max_archive_bytes) != receipt['bytes']:
                        raise Conflict('archive response differs from retained receipt')
                    if declared is not None and (not declared.isdigit() or int(declared) != receipt['bytes']):
                        raise Conflict('archive response differs from retained receipt')
                    try:
                        fd = os.open(temporary, flags, 0o600)
                    except FileExistsError:
                        raise Conflict('previous archive restore requires inspection') from None
                    original = os.fstat(fd)
                    with os.fdopen(fd, 'wb') as output:
                        fd = None
                        if automatic:
                            self._reserve_archive(output, receipt['bytes'])
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
            # A recovered file can have been written by a process that died
            # before fsync. Make its verified data durable before publication.
            # On failure retain even a slot created by this invocation.
            try:
                sync_fd = os.open(temporary, os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0)
                                  | getattr(os, 'O_NONBLOCK', 0))
                try:
                    sync_identity = os.fstat(sync_fd)
                    if (sync_identity.st_dev,sync_identity.st_ino)!=(opened.st_dev,opened.st_ino):
                        raise OSError('archive restore identity changed')
                    os.fsync(sync_fd)
                finally:
                    os.close(sync_fd)
            except OSError:
                raise Conflict('archive restore sync failed') from None
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
            # Failed or uncertain restores are retained for inspection.

    def download(self, identifier, *, automatic=False):
        if self._archive_child_unreaped.is_set():
            raise Conflict('archive capture child termination uncertain')
        task = self.store.task(identifier)
        if not task['job'] or task['job']['status'] != 'succeeded':
            raise Conflict('task is not complete')
        with self._archive_task_lock(identifier):
            if self._archive_child_unreaped.is_set():
                raise Conflict('archive capture child termination uncertain')
            task = self.store.task(identifier)
            if self._archive_inspection_held(task):
                raise ArchiveInspectionHeld('archive requires inspection')
            if self._durable_archive_claims and self.store.archive_claim_held(task):
                raise ArchiveClaimHeld('archive requires inspection')
            claim = self.store.claim_archive(task) if self._durable_archive_claims and not task['archive_path'] else None
            if self._archive_registry:
                lease=getattr(self.queue,'authority_lease',None)
                if lease is None or lease._descriptor is None or self.queue._lock.closed:
                    raise Conflict('archive registry authority unavailable')
                if not self.store.archive_registry_check(task, register=not bool(task['archive_path'])):
                    raise Conflict('historical archive format requires inspection')
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
                            # The receipt already binds the exact bytes to this
                            # completed task. If local copies disappeared before
                            # archive_path was persisted, recover only that same
                            # content from the retained engine result.
                            stream, size = self._restore_recorded_archive(task, path, receipt, automatic=automatic)
                            self._publish_captured_archive(identifier, task, path, claim)
                            return stream, size
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
                        self._publish_captured_archive(identifier, task, path, claim)
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
                    # Intent alone does not prove partial bytes or engine
                    # availability. Preserve them rather than overwriting.
                    raise Conflict('partial archive requires inspection')
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
                    automatic_size = _automatic_archive_length(response.headers, self.config.viewer_max_archive_bytes) if automatic else None
                    if declared is not None and (not declared.isdigit() or int(declared) > self.config.viewer_max_archive_bytes):
                        raise Conflict('archive exceeds configured disk allowance')
                    with partial.open('xb') as output:
                        os.chmod(partial, 0o600)
                        if automatic:
                            self._reserve_archive(output, automatic_size)
                        while chunk := response.read(1024 ** 2):
                            total += len(chunk)
                            if total > self.config.viewer_max_archive_bytes or (automatic and total > automatic_size):
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
                self._publish_captured_archive(identifier, task, path, claim)
                return self._verify_archive(path, receipt), receipt['bytes']
            receipt_path = path.with_suffix('.zip.receipt')
            if receipt_path.exists() or receipt_path.is_symlink():
                receipt = self._archive_receipt(task, receipt_path)
                try: path.lstat()
                except FileNotFoundError:
                    if not self._durable_archive_claims:
                        return self._restore_recorded_archive(task,path,receipt)
                    restore_claim = self.store.claim_archive(task, restore=True,
                        receipt_sha256=self._archive_receipt_digest(receipt))
                    stream = None
                    try:
                        stream, size = self._restore_recorded_archive(task,path,receipt)
                        stream.close()
                        current_receipt = self._archive_receipt(task, receipt_path)
                        stream = self._verify_archive(path, current_receipt)
                        self.store.finish_archive_restore(task, restore_claim, stream,
                                                          self._archive_receipt_digest(current_receipt))
                        result = stream; stream = None
                        return result, size
                    finally:
                        if stream is not None:
                            stream.close()
                return self._verify_archive(path, receipt), receipt['bytes']
            if self._archive_registry:
                raise Conflict('archive receipt requires inspection')
            # A capture intent identifies the receipt-backed format. Losing
            # its receipt must never silently downgrade it to legacy delivery.
            # If both markers disappear, legacy and new formats are ambiguous.
            try:
                path.with_suffix('.zip.intent').lstat()
            except FileNotFoundError:
                pass
            except OSError:
                raise Conflict('archive metadata requires inspection') from None
            else:
                raise Conflict('archive receipt requires inspection')
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
        if (task.get('claim_token')
                and (self._guarded_engines_enabled or self.queue.has_attested_started_transfer(task['job_id']))
                and self.queue.hold_started_transfer(task['job_id'], task['claim_token'])):
            return
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
        if code in (30, 50):
            # Neither failure nor cancellation proves all engine descendants
            # have exited. Preserve the slot until an operator confirms this.
            reason = 'engine_failure_termination_unverified' if code == 30 else 'cancel_termination_unverified'
            self._hold(task, reason)
            return
        status = 'succeeded'
        result = {'engineTaskId': task['engine_uuid'], 'nodeId': task['node_id'],
                  'environment': task['environment'], 'cancelled': code == 50}
        self.store.complete_engine_task(task, status, result)
        if status == 'succeeded':
            self._archive_wake.set()

    def tick(self):
        """One bounded scheduler turn; public for deterministic synthetic tests."""
        with self._lock:
            jobs = self.queue.list()
            split_jobs = self.store.incomplete_terminal_job_ids()
            # Older coordinators could commit the job before the Viewer row.
            # Only replay a terminal result when every persisted identity agrees.
            for job in jobs:
                if job['id'] not in split_jobs:
                    continue
                task = self.store.task_for_job(job['id'])
                if task['phase'] == 'complete':
                    continue
                expected = {'engineTaskId': task['engine_uuid'], 'nodeId': task['node_id'],
                            'environment': task['environment'], 'cancelled': False}
                if (job['status'] == 'failed' and isinstance(job['result'], dict)
                        and job['result'].get('operatorAction') == 'resolve'):
                    # The operator attested the worker stopped; finish the hold.
                    self.store.settle_operator_resolution(task)
                    continue
                if (not task['engine_uuid'] or not task['node_id'] or not task['claim_token']
                        or job['result'] != expected):
                    raise Conflict('terminal Viewer identity requires inspection')
                self.store.complete_engine_task(task, job['status'], expected)
                if job['status'] == 'succeeded':
                    self._archive_wake.set()
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
            try:
                self._check_options(task['options'], node_id=node_id, evidence=task['option_evidence'])
            except OptionCatalogUnavailable:
                self.store.update(task['uuid'], phase='waiting_node', error='option_catalog_unavailable')
                return
            except (OptionMissing, OptionDefinitionChanged):
                self.store.update(task['uuid'], phase='waiting_node', error='option_definition_changed')
                return
            if task['phase'] == 'waiting_node' and task['error'] in (
                    'option_catalog_unavailable', 'option_definition_changed'):
                self.store.update(task['uuid'], phase='queued', error=None)
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

    def dispatcher_status(self):
        """Process-local observation only; no job, exception, or credential data."""
        thread = self._thread
        if thread is None:
            state = 'not_started'
        elif thread.is_alive():
            state = 'stopping' if self._stop.is_set() else 'running'
        else:
            state = 'stopped' if self._stop.is_set() else 'not_running'
        return {'enabled': True, 'threadState': state,
                'unexpectedFailureObserved': self._unexpected_dispatch_failure.is_set()}

    def start(self):
        if self._thread:
            raise RuntimeError('dispatcher already started')
        if getattr(self.config, 'viewer_archive_autocapture', False):
            if getattr(self.queue, 'authority_lease', None) is None:
                raise RuntimeError('archive capture requires coordinator authority lease')
            self.store.enable_archive_status()
            self._archive_thread = threading.Thread(target=self._archive_run, name='viewer-archive-capture', daemon=True)
            self._archive_thread.start()
        self._thread = threading.Thread(target=self._run, name='viewer-dispatch', daemon=True)
        self._thread.start()

    def _run(self):
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                # Sticky, sanitized process-local signal. Never expose exception
                # text; no mutation replay or queue policy change is implied.
                self._unexpected_dispatch_failure.set()
            self._stop.wait(2)

    def close(self):
        self._stop.set()
        self._archive_wake.set()
        if self._thread:
            self._thread.join(timeout=10)
        if self._archive_thread:
            self._archive_thread.join(timeout=10)
            if self._archive_thread.is_alive():
                raise RuntimeError('archive capture did not stop')
        if self._archive_child_unreaped.is_set():
            raise RuntimeError('archive capture child termination uncertain')
