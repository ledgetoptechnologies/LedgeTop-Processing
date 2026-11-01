"""Explicit reference worker daemon; ODM by default. No import-time activation.

Run with --config pointing to an owner-private JSON file. The physical engine
gate must already be provisioned for this exact authority/origin/generation.
Startup creates no credentials, gate handover, firewall rules or engine containers.
Versioned per-job routing is optional; TreeDar composition is main-host only.
"""
import argparse
import ipaddress
import os
from pathlib import Path
import re
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
from ops.private_worker_directory import private_worker_directory, sync_worker_lease

MAX_CONFIG_BYTES = 64 * 1024
REQUIRED = frozenset(('stateRoot','scratchRoot','nodeId','workerId','workerInstanceId','authorityId',
    'engineId','gateGeneration','gateCoordinatorOrigin','coordinatorOrigin','brokerOrigin','engineOrigin',
    'engineToken','workerBearer','resultReadBearer','caFile','serverCertFile','serverKeyFile','listenHost',
    'listenPort','outputBudget','safetyFloor','transferTtl','cleanupGrace','pollSeconds','engineVersion','shareResults'))
OPTIONAL = frozenset(('allowedEnvironments','cleanupEnabled','engineCleanupEnabled','allowedOdmOptions','schemaVersion',
                      'executionRouting','engineGatewaySecret'))
TREEDAR_DOCKER_EXECUTABLE = '/usr/bin/docker'
TREEDAR_DOCKER_SOCKET = '/run/docker.sock'
_STATE_OWNERS = {}


def _execution_routing(config):
    """Explicit source-only opt-in; no capabilities or issuer are provisioned.

    treedar:null enables per-job ODM routing on either node. A TreeDar object is
    main/Linux only. Journals, command, socket, UID and container policy cannot
    be supplied by a request or overridden in this configuration shape.
    """
    if 'executionRouting' not in config:
        return None
    routing = config['executionRouting']
    p._closed(routing, ('schemaVersion','mode','treedar'), 'execution routing')
    if (type(routing['schemaVersion']) is not int or routing['schemaVersion'] != 1
            or routing['mode'] != 'per-job-v1'):
        raise ValueError('unsupported execution routing version')
    tree = routing['treedar']
    if tree is not None:
        p._closed(tree, ('imageId','sourceRoot','sourceSha256','engineVersion'), 'TreeDar runtime')
        if (config['nodeId'] != 'main' or not sys.platform.startswith('linux')
                or os.geteuid() == 0 or os.getegid() == 0
                or type(tree['imageId']) is not str or not re.fullmatch(r'sha256:[a-f0-9]{64}',tree['imageId'])
                or type(tree['engineVersion']) is not str
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.+-]{0,79}',tree['engineVersion'])
                or type(tree['sourceRoot']) is not str or not Path(tree['sourceRoot']).is_absolute()
                or any(char in tree['sourceRoot'] for char in '\n\r,')):
            raise ValueError('fixed main TreeDar runtime required')
        source = Path(tree['sourceRoot'])
        if source.resolve() != source or any(part.is_symlink() for part in (source,*source.parents)):
            raise ValueError('canonical installed source required')
        p._hash(tree['sourceSha256'], 'installed source digest')
    return routing


class DaemonStateLock:
    """One daemon owns this state root until its driver/readers finish draining."""
    def __init__(self, state_root):
        self.root = Path(state_root).absolute()
        self._stream = None
        self._creator_pid = os.getpid()
        self._identity = self._root_identity = None

    def __enter__(self):
        if self._creator_pid != os.getpid():
            raise ValueError('worker state owner process changed')
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
            self._identity = (observed.st_dev, observed.st_ino)
            current_root = root.stat()
            self._root_identity = (current_root.st_dev, current_root.st_ino)
            self.check(root)
            sync_worker_lease(self._stream.fileno(), root)
            self.check(root)
            _STATE_OWNERS[str(root)] = self
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
        if self._creator_pid != os.getpid():
            raise ValueError('worker state owner process changed')
        if self._stream is not None:
            self.check(self.root)
            try:
                if os.name == 'nt':
                    import msvcrt
                    self._stream.seek(0)
                    msvcrt.locking(self._stream.fileno(), msvcrt.LK_UNLCK, 1)
            finally:
                self._stream.close()
                self._stream = None
                if _STATE_OWNERS.get(str(self.root)) is self:
                    del _STATE_OWNERS[str(self.root)]

    def check(self, root=None):
        """Validate the live capability before state migration or activation."""
        # No inherited mutex may be acquired before checking the process.
        if self._creator_pid != os.getpid() or self._stream is None:
            raise ValueError('worker state owner unavailable')
        try:
            if root is not None and Path(root).absolute() != self.root:
                raise ValueError('worker state root differs')
            if self.root.resolve() != self.root or any(part.is_symlink() for part in (self.root,*self.root.parents)):
                raise ValueError('worker state root changed')
            directory = self.root.stat()
            path = self.root/'worker-daemon.lock'
            observed,current = os.fstat(self._stream.fileno()),path.lstat()
            if (not stat.S_ISDIR(directory.st_mode)
                    or (directory.st_dev,directory.st_ino) != self._root_identity
                    or not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1
                    or not stat.S_ISREG(current.st_mode) or current.st_nlink != 1
                    or (observed.st_dev,observed.st_ino) != self._identity
                    or (current.st_dev,current.st_ino) != self._identity
                    or os.get_inheritable(self._stream.fileno())
                    or (os.name != 'nt' and (directory.st_uid != os.geteuid() or directory.st_mode & 0o077
                        or observed.st_uid != os.geteuid() or stat.S_IMODE(observed.st_mode) != 0o600))):
                raise ValueError('worker state owner identity changed')
        except Exception:
            raise ValueError('worker state owner unavailable') from None
        return self


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
                # Factory ownership is released by server_close. Finish the
                # last bounded driver mutation before allowing a new factory.
                if driver_started:
                    self._driver_thread.join()
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
        _execution_routing(config)
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
        from processing.gateway import gateway_headers
        gateway_secret = config.setdefault('engineGatewaySecret', '')
        gateway_headers(config['engineOrigin'], gateway_secret)
        from processing.config import GATEWAY_CLIENT_QUALIFIED
        if gateway_secret and GATEWAY_CLIENT_QUALIFIED is not True:
            raise ValueError('gateway client profile not qualified')
        if gateway_secret and gateway_secret in (token, config['workerBearer'], config['resultReadBearer']):
            raise ValueError('gateway credential must be separate')
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
    return private_worker_directory(value)


def build_worker(config):
    """Own state/readers until the returned server has fully drained and closed."""
    if not sys.platform.startswith('linux'):
        raise ValueError('owned result reader daemon requires Linux')
    owner=DaemonStateLock(config['stateRoot'])
    owner.__enter__()
    resources={}
    try:
        return _build_worker(config,owner,resources)
    except BaseException:
        # Stop at the first uncertain drain/release. Strong ownership registries
        # retain later leases until explicit reconciliation or process death.
        if resources.get('serverClose') is not None:resources['serverClose']()
        if resources.get('session') is not None:resources['session'].close()
        owner.__exit__(None,None,None)
        raise


def _build_worker(config,owner,resources):
    """Construct under the factory's mandatory state-root ownership capability."""
    routing = _execution_routing(config)
    root, scratch = _directory(config['stateRoot']), _directory(config['scratchRoot'])
    owner.check(root)
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
    engine = NativeEngine(config['engineOrigin'], config['engineToken'], config.get('engineGatewaySecret', ''))
    info = engine.read('/info')
    if type(info) is not dict or info.get('engineVersion') != config['engineVersion']:
        raise ValueError('actual engine version differs from configured pin')
    ledger = ScratchLedger(root / 'scratch.sqlite', scratch, safety_floor_bytes=config['safetyFloor'],
                           cleanup_grace_seconds=config['cleanupGrace'])
    from processing.reference_readers import ReaderSession
    session=ReaderSession(ledger)
    session.__enter__()
    resources['session']=session
    outbox, failures = WorkerOutbox(root / 'outbox.sqlite'), FailureOutbox(root / 'failure-outbox.sqlite')
    worker = ReferenceEngineWorker(root / 'worker.sqlite', ledger=ledger, outbox=outbox, engine=engine,
        engine_id=config['engineId'], node_id=config['nodeId'], authority_id=config['authorityId'],
        coordinator_origin=config['gateCoordinatorOrigin'], gate_generation=config['gateGeneration'],
        worker_id=config['workerId'], engine_version=config['engineVersion'])
    broker = BrokerClient(config['brokerOrigin'], ssl_context=client_tls)
    router = provenance = None
    if routing is not None:
        from processing.reference_execution import ExecutionRouter
        tree_worker = None
        if routing['treedar'] is not None:
            from processing.reference_provenance import LidarProvenanceClient
            from processing.treedar_delivery import TreeDarDelivery
            from processing.treedar_engine import TreeDarConfig, TreeDarEngine
            from processing.treedar_runtime import TreeDarRuntimeManager, runtime_descriptor
            tree = routing['treedar']
            provenance = LidarProvenanceClient(broker, state_path=root/'lidar-provenance.sqlite')
            physical=dict(nodeId=worker.node_id,workerId=worker.worker_id,engineId=worker.engine_id,
                authorityId=worker.authority_id,origin=worker.origin,gateGeneration=worker.gate_generation)
            manager=TreeDarRuntimeManager(root/'treedar-delivery.sqlite',ledger=ledger,outbox=outbox,
                failure_outbox=failures,physical_binding=physical,allow_legacy_v1=True)
            previous=manager.snapshot()
            tree_config=TreeDarConfig(docker_executable=TREEDAR_DOCKER_EXECUTABLE,
                    docker_socket=TREEDAR_DOCKER_SOCKET, image_id=tree['imageId'],
                    source_root=tree['sourceRoot'], source_sha256=tree['sourceSha256'],
                    engine_version=tree['engineVersion'], engine_id=config['engineId'],
                    authority_id=config['authorityId'], coordinator_origin=config['gateCoordinatorOrigin'],
                    gate_generation=config['gateGeneration'], worker_id=config['workerId'],
                    uid=os.geteuid(), gid=os.getegid())
            controller = TreeDarEngine(manager.controller_path(tree_config),ledger=ledger,
                config=tree_config,verify_provenance=provenance.verify)
            # Read-only local Docker/image/source census before new claims. No
            # pull, handover, container mutation or capability advertisement.
            controller._host()
            tree_worker = TreeDarDelivery(root/'treedar-delivery.sqlite', controller=controller,
                outbox=outbox,failure_outbox=failures,runtime_manager=manager)
            candidate=runtime_descriptor(controller,tree_worker.path)
            if previous is None or previous['runtime']!=candidate:
                with NodeGate(config['engineId']) as gate:
                    owner.check(root);session.check()
                    manager.activate_runtime(controller,
                        expected_epoch=previous['epochId'] if previous else None,
                        expected_revision=previous['revision'] if previous else 0,
                        owner=owner,reader_session=session,gate=gate,
                        census=lambda db,envelope:ExecutionRouter.migration_census(root/'execution-routes.sqlite',
                            driver_path=root/'driver.sqlite',physical_binding=physical,odm_worker=worker,
                            failure_outbox=failures,delivery_db=db,current_envelope=envelope))
            else:
                manager.assert_active(candidate)
        router = ExecutionRouter(root/'execution-routes.sqlite', worker, tree_worker, failure_outbox=failures)
    driver = ReferenceDriver(worker, ledger, outbox, broker, config['coordinatorOrigin'], config['workerBearer'],
        state_path=root / 'driver.sqlite', ssl_context=client_tls, output_budget=config['outputBudget'],
        transfer_ttl=config['transferTtl'], safety_floor=config['safetyFloor'], failure_outbox=failures,
        worker_instance_id=config['workerInstanceId'], allowed_environments=tuple(config['allowedEnvironments']),
        execution_router=router, provenance_client=provenance,state_owner=owner,
        engine_cleanup=EngineCacheCleanup(worker, path=root / 'engine-cleanup.sqlite')
            if config['engineCleanupEnabled'] else None,
        cleanup_executor=ScratchCleanupExecutor(ledger) if config['cleanupEnabled'] else None)
    permitted = frozenset(config['allowedEnvironments'])
    result_api = ReferenceResultAPI(ReferenceResults(router if router is not None else worker,
        reader_session=session), bearer_readers={config['resultReadBearer']: dict(
        node_id=config['nodeId'], worker_id=config['workerId'], environments=tuple(permitted),
        share_policy=lambda metadata: config['shareResults'] is True and metadata['environment'] in permitted)})
    server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_tls.load_cert_chain(config['serverCertFile'], config['serverKeyFile'])
    server = create_reference_http_server(result_api, config['listenHost'], config['listenPort'], ssl_context=server_tls)
    original_close = server.server_close
    resources['serverClose']=original_close
    server.daemon_threads = False
    server.block_on_close = True
    close_lock = threading.Lock()
    closed = False
    def close_owned_readers():
        nonlocal closed
        if os.getpid()!=owner._creator_pid:
            raise ValueError('worker state owner process changed')
        with close_lock:
            if closed:
                return
            with driver._lock:
                owner.check(root)
                # A failed join or lease close retains all later ownership.
                original_close()
                session.close()
                owner.__exit__(None,None,None)
                closed = True
    server.server_close = close_owned_readers
    return driver, server


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, help='owner-private JSON settings file')
    args = parser.parse_args(argv)
    server = None
    try:
        config = load_config(args.config)
        driver, server = build_worker(config)
        stop = threading.Event()
        for name in ('SIGINT','SIGTERM'):
            if hasattr(signal, name):
                signal.signal(getattr(signal, name), lambda *_args: stop.set())
        service = WorkerService(driver, server, poll_seconds=config['pollSeconds'])
        print('reference worker starting', flush=True)
        result = service.run(stop)
        server = None  # Factory ownership survives until successful full drain.
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
