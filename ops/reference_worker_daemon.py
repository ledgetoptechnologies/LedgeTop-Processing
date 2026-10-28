"""Explicit native ODM reference worker daemon. No import-time activation.

Run with --config pointing to an owner-private JSON file. The physical engine
gate must already be provisioned for this exact authority/origin/generation.
No credentials, gate handover, firewall rules or engine containers are created.
"""
import argparse
import ipaddress
import os
from pathlib import Path
import signal
import ssl
import stat
import sys
import threading
from urllib.parse import urlsplit

from processing import reference_protocol as p
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_driver import ReferenceDriver
from processing.reference_engine_cleanup import EngineCacheCleanup
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_failure import FailureOutbox
from processing.reference_http import create_reference_http_server
from processing.reference_result_api import ReferenceResultAPI
from processing.reference_results import ReferenceResults
from processing.reference_transfer import BrokerClient
from processing.reference_tls import verified_client_context
from processing.scratch_cleanup import ScratchCleanupExecutor
from processing.scratch_policy import ScratchLedger
from processing.viewer_service import NativeEngine
from processing.worker_outbox import WorkerOutbox

MAX_CONFIG_BYTES = 64 * 1024
REQUIRED = frozenset(('stateRoot','scratchRoot','nodeId','workerId','workerInstanceId','authorityId',
    'engineId','gateGeneration','gateCoordinatorOrigin','coordinatorOrigin','brokerOrigin','engineOrigin',
    'engineToken','workerBearer','resultReadBearer','caFile','serverCertFile','serverKeyFile','listenHost',
    'listenPort','outputBudget','safetyFloor','transferTtl','cleanupGrace','pollSeconds','engineVersion','shareResults'))
OPTIONAL = frozenset(('allowedEnvironments','cleanupEnabled','engineCleanupEnabled','allowedOdmOptions','schemaVersion'))


class DaemonStateLock:
    """One daemon owns this state root until its driver/readers finish draining."""
    def __init__(self, state_root):
        self.root = state_root
        self._stream = None

    def __enter__(self):
        if self._stream is not None:
            raise ValueError('worker state already owned')
        root = _directory(self.root)
        path = root / 'worker-daemon.lock'
        if path.is_symlink():
            raise ValueError('invalid worker daemon lock')
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0)
                             | getattr(os, 'O_NONBLOCK', 0), 0o600)
        try:
            observed, current = os.fstat(descriptor), path.lstat()
            if (not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1
                    or (observed.st_dev, observed.st_ino) != (current.st_dev, current.st_ino)
                    or (os.name != 'nt' and observed.st_uid != os.geteuid())):
                raise ValueError('invalid worker daemon lock')
            if os.name != 'nt':
                os.fchmod(descriptor, 0o600)
            self._stream = os.fdopen(descriptor, 'r+b')
            descriptor = None
            if os.name == 'nt':
                import msvcrt
                if not self._stream.read(1):
                    self._stream.write(b'0')
                    self._stream.flush()
                    os.fsync(self._stream.fileno())
                self._stream.seek(0)
                msvcrt.locking(self._stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return self
        except Exception:
            if self._stream is not None:
                self._stream.close()
                self._stream = None
            raise ValueError('worker state already owned or unavailable') from None
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def __exit__(self, *_args):
        if self._stream is not None:
            try:
                if os.name == 'nt':
                    import msvcrt
                    self._stream.seek(0)
                    msvcrt.locking(self._stream.fileno(), msvcrt.LK_UNLCK, 1)
            finally:
                self._stream.close()
                self._stream = None


class WorkerService:
    """Join ownership-bearing threads; shutdown never kills an in-flight mutation.

    Operators must drain maintenance before stopping and allow the current
    bounded driver stage/HTTP IO to finish. No join timeout becomes permission
    to kill work or drop read pins. Abrupt process death still needs explicit
    stale-pin reconciliation after proving the old process/readers are gone.
    """
    def __init__(self, driver, server, *, poll_seconds):
        if type(poll_seconds) not in (int, float) or not 0.1 <= poll_seconds <= 60:
            raise ValueError('invalid worker service polling interval')
        self.driver, self.server, self.poll_seconds = driver, server, poll_seconds
        self.server.daemon_threads = False
        self.server.block_on_close = True
        self._lock = threading.Lock()
        self._state, self._reason = 'starting', None
        self._driver_thread = self._server_thread = None

    def _failed(self, reason, stop):
        with self._lock:
            self._state, self._reason = 'failed', reason
        stop.set()

    def health(self):
        with self._lock:
            return dict(state=self._state, reason=self._reason,
                driverAlive=self._driver_thread is not None and self._driver_thread.is_alive(),
                listenerAlive=self._server_thread is not None and self._server_thread.is_alive())

    def run(self, stop):
        if self._server_thread is not None:
            raise ValueError('worker service already started')
        def serve():
            try:
                self.server.serve_forever(poll_interval=0.2)
                if not stop.is_set():
                    self._failed('listener_thread_exited', stop)
            except BaseException:
                self._failed('listener_thread_failed', stop)

        def drive():
            backoff = self.poll_seconds
            try:
                while not stop.is_set():
                    state = self.driver.step()['state']
                    backoff = min(60, max(self.poll_seconds, backoff * 2)) if state in ('retry_wait','held','not_ready') else self.poll_seconds
                    stop.wait(backoff)
            except BaseException:
                self._failed('driver_thread_failed', stop)

        self._server_thread = threading.Thread(target=serve, name='reference-result-listener', daemon=False)
        self._driver_thread = threading.Thread(target=drive, name='reference-driver', daemon=False)
        server_started = driver_started = False
        try:
            self._server_thread.start()
            server_started = True
            self._driver_thread.start()
            driver_started = True
            with self._lock:
                if self._state != 'failed':
                    self._state = 'ready'
            stop.wait()
        except BaseException:
            self._failed('worker_service_failed', stop)
        finally:
            stop.set()
            with self._lock:
                if self._state != 'failed':
                    self._state = 'draining'
            # Stop new reads first, then join handlers so every stream's finally
            # closes its descriptor and releases only its own durable read pin.
            try:
                if server_started:
                    if self._server_thread.is_alive():
                        try:
                            self.server.shutdown()
                        except BaseException:
                            self._failed('listener_drain_failed', stop)
                    self._server_thread.join()
                try:
                    self.server.server_close()
                except BaseException:
                    self._failed('listener_drain_failed', stop)
                    # ThreadingMixIn closes the socket before joining its tracked
                    # handlers. Preserve that join even if socket close failed.
                    handlers = getattr(self.server, '_threads', None)
                    if handlers is not None:
                        handlers.join()
            finally:
                if driver_started:
                    self._driver_thread.join()
            with self._lock:
                failed = self._state == 'failed'
                if not failed:
                    self._state = 'stopped'
        return 2 if failed else 0


def _file(path, *, private=False):
    if type(path) is not str or not Path(path).is_absolute():
        raise ValueError('explicit absolute configuration file required')
    candidate = Path(path).absolute()
    if candidate.resolve() != candidate or any(part.is_symlink() for part in (candidate, *candidate.parents)):
        raise ValueError('regular configuration file required')
    info = candidate.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError('single-link regular configuration file required')
    if private and os.name != 'nt' and (info.st_uid != os.geteuid() or info.st_mode & 0o077):
        raise ValueError('owner-private configuration file required')
    return candidate, info


def load_config(path):
    """Return private settings; never include their values in an exception."""
    try:
        candidate, initial = _file(path, private=True)
        descriptor = os.open(candidate, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
        try:
            observed = os.fstat(descriptor)
            if (observed.st_dev, observed.st_ino) != (initial.st_dev, initial.st_ino):
                raise ValueError('configuration identity changed')
            with os.fdopen(descriptor, 'rb', closefd=False) as source:
                raw = source.read(MAX_CONFIG_BYTES + 1)
        finally:
            os.close(descriptor)
        if len(raw) > MAX_CONFIG_BYTES:
            raise ValueError('configuration exceeds size limit')
        config = p.decode_submission(raw)
        if type(config) is not dict or not REQUIRED <= set(config) or set(config) - REQUIRED - OPTIONAL:
            raise ValueError('configuration fields differ')
        if 'schemaVersion' in config and (type(config['schemaVersion']) is not int or config['schemaVersion'] != 1):
            raise ValueError('unsupported configuration version')
        for field in ('stateRoot','scratchRoot'):
            if type(config[field]) is not str or not Path(config[field]).is_absolute():
                raise ValueError('absolute private directories required')
        Queue.slug(config['nodeId'])
        Queue.slug(config['workerId'])
        p._slug(config['workerInstanceId'], 'worker instance')
        p._uuid(config['authorityId'], 'authority')
        p._integer(config['gateGeneration'], 'gate generation')
        for field in ('outputBudget','transferTtl'):
            p._integer(config[field], 'worker budget')
        for field in ('safetyFloor','cleanupGrace'):
            p._integer(config[field], 'worker policy', allow_zero=True)
        if (type(config['listenPort']) is not int or not 1 <= config['listenPort'] <= 65535
                or type(config['listenHost']) is not str or not config['listenHost']
                or any(ord(char) <= 32 for char in config['listenHost'])
                or type(config['pollSeconds']) not in (int, float) or not 0.1 <= config['pollSeconds'] <= 60):
            raise ValueError('invalid listener or polling configuration')
        for field in ('workerBearer','resultReadBearer'):
            value = config[field]
            if type(value) is not str or not 32 <= len(value) <= 4096 or not all(0x21 <= ord(c) <= 0x7e for c in value):
                raise ValueError('separate bounded worker credentials required')
        if config['workerBearer'] == config['resultReadBearer']:
            raise ValueError('claim and result reader credentials must differ')
        token = config['engineToken']
        if type(token) is not str or len(token) > 4096 or not all(0x21 <= ord(c) <= 0x7e for c in token):
            raise ValueError('invalid engine credential')
        version = config['engineVersion']
        if type(version) is not str or not 1 <= len(version) <= 128 or not all(0x21 <= ord(c) <= 0x7e for c in version):
            raise ValueError('explicit actual engine version pin required')
        if config['shareResults'] is not True:
            raise ValueError('explicit result sharing decision required')
        config.setdefault('cleanupEnabled', False)
        config.setdefault('engineCleanupEnabled', False)
        config.setdefault('allowedEnvironments', ['production','staging'])
        if (type(config['cleanupEnabled']) is not bool or type(config['engineCleanupEnabled']) is not bool
                or type(config['allowedEnvironments']) is not list or not config['allowedEnvironments']):
            raise ValueError('invalid worker sharing or cleanup policy')
        for environment in config['allowedEnvironments']:
            p._environment(environment)
        # Names and numeric/bool value types are discovered from /options for
        # each new claim. This compatibility key cannot install a static list.
        if config.get('allowedOdmOptions', []) != []:
            raise ValueError('static ODM allowlists are not supported')
        for field in ('caFile','serverCertFile','serverKeyFile'):
            _file(config[field], private=field == 'serverKeyFile')
        return config
    except Exception:
        raise ValueError('invalid private worker configuration') from None


def _directory(value):
    root = Path(value).absolute()
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    if root.resolve() != root or any(part.is_symlink() for part in (root, *root.parents)):
        raise ValueError('owned private worker directory required')
    info = root.stat()
    if not stat.S_ISDIR(info.st_mode) or (os.name != 'nt' and (info.st_uid != os.geteuid() or info.st_mode & 0o077)):
        raise ValueError('owned private worker directory required')
    return root


def build_worker(config):
    """Build explicit components; verify the existing gate and actual version."""
    root, scratch = _directory(config['stateRoot']), _directory(config['scratchRoot'])
    with NodeGate(config['engineId']) as gate:
        state = gate.snapshot()
        if (state['nodeId'], state['authorityId'], state['coordinatorOrigin'], state['generation']) != (
                config['nodeId'], config['authorityId'], config['gateCoordinatorOrigin'].rstrip('/'), config['gateGeneration']):
            raise ValueError('preprovisioned physical gate differs')
    client_tls = verified_client_context(config['caFile'])
    parsed = urlsplit(config['engineOrigin'])
    if (parsed.scheme not in ('http','https') or not ipaddress.ip_address(parsed.hostname).is_loopback
            or parsed.username is not None or parsed.password is not None or parsed.path not in ('','/')
            or parsed.query or parsed.fragment or parsed.port == 0):
        raise ValueError('fixed literal loopback engine origin required')
    engine = NativeEngine(config['engineOrigin'], config['engineToken'])
    info = engine.read('/info')
    if type(info) is not dict or info.get('engineVersion') != config['engineVersion']:
        raise ValueError('actual engine version differs from configured pin')
    ledger = ScratchLedger(root / 'scratch.sqlite', scratch, safety_floor_bytes=config['safetyFloor'],
                           cleanup_grace_seconds=config['cleanupGrace'])
    outbox, failures = WorkerOutbox(root / 'outbox.sqlite'), FailureOutbox(root / 'failure-outbox.sqlite')
    worker = ReferenceEngineWorker(root / 'worker.sqlite', ledger=ledger, outbox=outbox, engine=engine,
        engine_id=config['engineId'], node_id=config['nodeId'], authority_id=config['authorityId'],
        coordinator_origin=config['gateCoordinatorOrigin'], gate_generation=config['gateGeneration'],
        worker_id=config['workerId'], engine_version=config['engineVersion'])
    broker = BrokerClient(config['brokerOrigin'], ssl_context=client_tls)
    driver = ReferenceDriver(worker, ledger, outbox, broker, config['coordinatorOrigin'], config['workerBearer'],
        state_path=root / 'driver.sqlite', ssl_context=client_tls, output_budget=config['outputBudget'],
        transfer_ttl=config['transferTtl'], safety_floor=config['safetyFloor'], failure_outbox=failures,
        worker_instance_id=config['workerInstanceId'], allowed_environments=tuple(config['allowedEnvironments']),
        engine_cleanup=EngineCacheCleanup(worker, path=root / 'engine-cleanup.sqlite')
            if config['engineCleanupEnabled'] else None,
        cleanup_executor=ScratchCleanupExecutor(ledger) if config['cleanupEnabled'] else None)
    permitted = frozenset(config['allowedEnvironments'])
    result_api = ReferenceResultAPI(ReferenceResults(worker), bearer_readers={config['resultReadBearer']: dict(
        node_id=config['nodeId'], worker_id=config['workerId'], environments=tuple(permitted),
        share_policy=lambda metadata: config['shareResults'] is True and metadata['environment'] in permitted)})
    server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_tls.load_cert_chain(config['serverCertFile'], config['serverKeyFile'])
    server = create_reference_http_server(result_api, config['listenHost'], config['listenPort'], ssl_context=server_tls)
    return driver, server


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, help='owner-private JSON settings file')
    args = parser.parse_args(argv)
    server = None
    try:
        config = load_config(args.config)
        with DaemonStateLock(config['stateRoot']):
            driver, server = build_worker(config)
            stop = threading.Event()
            for name in ('SIGINT','SIGTERM'):
                if hasattr(signal, name):
                    signal.signal(getattr(signal, name), lambda *_args: stop.set())
            service = WorkerService(driver, server, poll_seconds=config['pollSeconds'])
            print('reference worker starting', flush=True)
            result = service.run(stop)
            server = None  # Service drained and closed before releasing state lock.
            if result:
                print('reference worker unavailable', file=sys.stderr, flush=True)
            return result
    except Exception:
        print('reference worker unavailable', file=sys.stderr, flush=True)
        return 2
    finally:
        if server is not None:
            server.server_close()


if __name__ == '__main__':
    raise SystemExit(main())
