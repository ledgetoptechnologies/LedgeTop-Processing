"""The two child processes behind test_reference_source_fetch_process_restart.

``coordinator`` serves the real Queue, store and control API over verified TLS.
``worker`` runs real ReferenceDriver.step() against it, one step per stdin line,
with the real journal, fenced ledger, range client and HTTP request path. Each is
killed with SIGKILL by the test, so only what reached disk survives. The worker
is built without __init__ because a real engine and gate are absent: a call into
either fails loudly, and nothing up to execution_pending makes one.
"""
import json
import os
from pathlib import Path
import signal
import ssl
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch
from urllib.request import HTTPSHandler, ProxyHandler, build_opener

from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_driver import ReferenceDriver, _NoRedirects
from processing.reference_http import create_reference_http_server
from processing.reference_recovery import RecoveryNodeBinding
from processing.reference_store import PROFILE, ReferenceStore
from processing.reference_tls import verified_client_context
from processing.scratch_policy import ScratchLedger
from processing.source_range_transfer import SourceRangeClient
from processing.source_spool import SourceSpool

WORKER_TOKEN = 'synthetic-worker-token-012345'
CLAIM_REQUEST = 'source-claim-process'


def coordinator(config):
    root = Path(config['root'])
    with patch('processing.source_spool.os.path.ismount', return_value=True):
        queue = Queue(str(root))
        queue.allowed_profiles.add(PROFILE)
        store = ReferenceStore(queue)
        spool = SourceSpool(root / 'spool', mount_id='a' * 64, safety_floor_bytes=0, max_object_bytes=1024, chunk_bytes=3)
        control = ReferenceControlAPI(store, viewer_bearers={'synthetic-staging-token': 'staging'},
            worker_bearers={WORKER_TOKEN: dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')},
            capability_fingerprint='a' * 64, options_fingerprint=config['optionsFingerprint'],
            allowed_odm_options={'numeric-probe'}, source_spool=spool, source_attestation_keys=config['trustedKeys'],
            authority_id=queue.authority_id, recovery_bindings={'cluster': RecoveryNodeBinding('cluster',
                'physical-cluster', queue.authority_id, 'http://127.0.0.1:18080', 1, 1000)})
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(config['cert'], config['key'])
        server = create_reference_http_server(control, port=config['port'], ssl_context=context)
        print('ready', flush=True)
        server.serve_forever()


def build_worker(config):
    root = Path(config['root'])
    origin = 'https://127.0.0.1:' + str(config['port'])
    context = verified_client_context(config['cert'])
    driver = object.__new__(ReferenceDriver)
    driver.path, driver.origin, driver._bearer = root / 'driver-journal.sqlite', origin, WORKER_TOKEN
    driver._combined_policy = driver._combined_policy_initial = driver._combined_preparation_initial = None
    driver._state_owner = driver.execution_router = driver.combined_preparation = None
    driver._active_step = driver._active_job = None
    driver._lock, driver.clock = threading.Lock(), time.time
    driver.node_id, driver.worker_id, driver.worker_instance_id = 'cluster', 'worker', 'instance'
    driver.worker = SimpleNamespace(authority_id=config['authorityId'])
    driver.ledger = ScratchLedger(root / 'ledger.sqlite', root / 'scratch', capacity_probe=lambda _: 10**9, safety_floor_bytes=0)
    driver.source_range_v2_enabled, driver.output_budget, driver.transfer_ttl = True, 10000, 300
    driver._open = build_opener(ProxyHandler({}), _NoRedirects(), HTTPSHandler(context=context)).open
    driver.source_range_client = SourceRangeClient(origin, root / 'worker-ledger.sqlite', ssl_context=context,
        renewal_origin=origin, renewal_headers=driver._source_renewal_headers)
    with driver._db() as db:
        db.execute('''CREATE TABLE IF NOT EXISTS driver_jobs (
          created INTEGER PRIMARY KEY AUTOINCREMENT, job TEXT UNIQUE NOT NULL,
          claim_request TEXT UNIQUE NOT NULL, stage TEXT NOT NULL, data TEXT NOT NULL)''')
        fresh = db.execute('SELECT 1 FROM driver_jobs').fetchone() is None
    if fresh:
        _, claim = driver._request('POST', '/claim', {'jobId': config['jobId'], 'claimRequestId': CLAIM_REQUEST})
        with driver._db() as db:
            db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,'claimed',?)",
                (config['jobId'], CLAIM_REQUEST, json.dumps({'claim': claim})))
    return driver


def die_around_recovery(driver, moment):
    """SIGKILL this process just before the recovery POST leaves or just after it returns."""
    real = driver._request

    def request(method, suffix, *args, **kwargs):
        recovering = method == 'POST' and suffix.endswith('/recover')
        if recovering and moment == 'before_post':
            os.kill(os.getpid(), signal.SIGKILL)
        reply = real(method, suffix, *args, **kwargs)
        if recovering and moment == 'after_post':
            os.kill(os.getpid(), signal.SIGKILL)
        return reply

    driver._request = request


def worker(config):
    driver = build_worker(config)
    if os.environ.get('SOURCE_FETCH_DIE'):
        die_around_recovery(driver, os.environ['SOURCE_FETCH_DIE'])
    print('ready', flush=True)
    for _ in sys.stdin:
        print(json.dumps(driver.step()), flush=True)


if __name__ == '__main__':
    {'coordinator': coordinator, 'worker': worker}[sys.argv[1]](json.loads(Path(sys.argv[2]).read_text()))
