import concurrent.futures
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from unittest.mock import patch

from processing import reference_owned_binding_candidate as candidate, reference_protocol as protocol
from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.reference_store import ReferenceStore, PROFILE


_LOST_RESPONSE_CHILD = r'''
import json,os,sys
from unittest.mock import patch
from processing.prototype import Queue
from processing.reference_store import ReferenceStore, PROFILE
from processing import reference_owned_binding_candidate as candidate
root, submission, binding, attempt, principal = sys.argv[1:]
q=Queue(root);q.allowed_profiles.add(PROFILE)
try:
    store=ReferenceStore(q)
    with patch.object(candidate,'QUALIFIED',True):
        candidate.submit_bound_candidate(store,json.loads(submission),binding_id=binding,
            attempt_id=attempt,principal_id=principal)
    # Candidate returned after Queue context committed. Simulate a process
    # dying before it can send its response; q.close does not run.
    os._exit(75)
finally:
    q.close()
'''

_HTTP_CLIENT = r'''
import json,sys,urllib.request
url,payload=sys.argv[1:]
request=urllib.request.Request(url,data=payload.encode(),method='POST')
with urllib.request.urlopen(request,timeout=10) as response:
    print(response.read().decode())
'''


def _reap_owned(child):
    """Act only on an exact Popen child. Report if it cannot be reaped."""
    if child.poll() is None:
        child.kill()
    try:
        child.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        print('owned synthetic child not reaped, pid:', child.pid, flush=True)
        raise


class OwnedBindingTests(unittest.TestCase):
    def setUp(self):
        # Retain isolated synthetic state for inspection; no cleanup of prior
        # approval-blocked resources is attempted by this fixture.
        self.root = tempfile.mkdtemp(prefix='lt-owned-binding-')
        print('retained synthetic binding root:', self.root, flush=True)
        self.queue = Queue(self.root)
        self.addCleanup(self.queue.close)
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        fixture = json.loads((Path(__file__).resolve().parents[1] /
                              'contracts/reference-v1.contract.json').read_text())
        body = fixture['submission']
        self.body = body
        self.submission = protocol.validate_submission(body, environment='staging',
            capability_fingerprint=body['capabilityFingerprint'],
            options_fingerprint=body['pipeline']['odm']['optionsFingerprint'],
            allowed_odm_options={'pc-quality', 'projection'})
        self.binding, self.attempt, self.principal = (str(uuid.uuid4()) for _ in range(3))

    def install(self):
        with patch.object(candidate, 'QUALIFIED', True):
            candidate.install_empty_candidate(self.store, synthetic_pipeline_digest=self.submission['pipelineDigest'])

    def submit(self, **overrides):
        args = dict(binding_id=self.binding, attempt_id=self.attempt, principal_id=self.principal)
        args.update(overrides)
        with patch.object(candidate, 'QUALIFIED', True):
            return candidate.submit_bound_candidate(self.store, self.submission, **args)

    def test_disabled_by_default_and_exact_replay(self):
        with self.assertRaises(Conflict):
            candidate.install_empty_candidate(self.store, synthetic_pipeline_digest=self.submission['pipelineDigest'])
        self.install()
        first, created = self.submit()
        self.assertTrue(created)
        replay, created = self.submit()
        self.assertFalse(created)
        self.assertEqual(first, replay)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_jobs').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_owned_binding_candidate').fetchone()[0], 1)

    def test_changed_attempt_binding_or_principal_holds(self):
        self.install()
        self.submit()
        for key in ('binding_id', 'attempt_id', 'principal_id'):
            with self.subTest(key=key), self.assertRaises(Conflict):
                self.submit(**{key: str(uuid.uuid4())})
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_jobs').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_owned_binding_candidate').fetchone()[0], 1)

    def test_binding_write_failure_rolls_back_new_job(self):
        self.install()
        with self.queue._db() as db:
            db.execute("CREATE TRIGGER fail_binding BEFORE INSERT ON reference_owned_binding_candidate "
                       "BEGIN SELECT RAISE(ABORT,'synthetic binding failure'); END")
        with self.assertRaises(Conflict):
            self.submit()
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_jobs').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_owned_binding_candidate').fetchone()[0], 0)

    def test_existing_v1_job_cannot_be_retroactively_bound(self):
        self.install()
        original, _ = self.store.submit(self.submission)
        with self.assertRaises(Conflict):
            self.submit()
        self.assertEqual(self.store.get(original['jobId'])['jobId'], original['jobId'])
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_owned_binding_candidate').fetchone()[0], 0)

    def test_unique_binding_cannot_be_reused_for_new_submission(self):
        self.install()
        first, _ = self.submit()
        body = dict(self.body, submissionId=str(uuid.uuid4()))
        other = protocol.validate_submission(body, environment='staging',
            capability_fingerprint=body['capabilityFingerprint'],
            options_fingerprint=body['pipeline']['odm']['optionsFingerprint'],
            allowed_odm_options={'pc-quality', 'projection'})
        with patch.object(candidate, 'QUALIFIED', True), self.assertRaises(Conflict):
            candidate.submit_bound_candidate(self.store, other, binding_id=self.binding,
                attempt_id=str(uuid.uuid4()), principal_id=self.principal)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_jobs').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT job_id FROM reference_owned_binding_candidate').fetchone()[0], first['jobId'])

    def test_same_submission_wrong_environment_holds(self):
        self.install()
        self.submit()
        other = protocol.validate_submission(self.body, environment='production',
            capability_fingerprint=self.body['capabilityFingerprint'],
            options_fingerprint=self.body['pipeline']['odm']['optionsFingerprint'],
            allowed_odm_options={'pc-quality', 'projection'})
        with patch.object(candidate, 'QUALIFIED', True), self.assertRaises(Conflict):
            candidate.submit_bound_candidate(self.store, other, binding_id=self.binding,
                attempt_id=self.attempt, principal_id=self.principal)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_jobs').fetchone()[0], 1)

    def test_concurrent_exact_replays_return_one_job(self):
        self.install()
        barrier = threading.Barrier(2, timeout=5)
        def run(_):
            barrier.wait()
            return candidate.submit_bound_candidate(self.store, self.submission,
                binding_id=self.binding, attempt_id=self.attempt, principal_id=self.principal)
        # Same Queue owner serializes its DB contexts; this is not a physical
        # multi-process writer-fence test.
        with patch.object(candidate, 'QUALIFIED', True):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(run, range(2)))
        self.assertEqual(sorted(created for _, created in results), [False, True])
        self.assertEqual(results[0][0]['jobId'], results[1][0]['jobId'])

    def test_nonempty_install_and_malformed_identity_hold(self):
        self.store.submit(self.submission)
        with patch.object(candidate, 'QUALIFIED', True), self.assertRaises(Conflict):
            candidate.install_empty_candidate(self.store, synthetic_pipeline_digest=self.submission['pipelineDigest'])
        for value in ('x', self.binding + '\n', self.binding.upper()):
            with self.subTest(value=value), self.assertRaises(ValueError):
                candidate._uuid4(value)

    def test_unowned_pipeline_is_rejected_and_profile_is_immutable(self):
        self.install()
        body = copy.deepcopy(self.body)
        body['pipeline']['odm']['optionsFingerprint'] = 'c' * 64
        other = protocol.validate_submission(body, environment='staging',
            capability_fingerprint=body['capabilityFingerprint'],
            options_fingerprint=body['pipeline']['odm']['optionsFingerprint'],
            allowed_odm_options={'pc-quality', 'projection'})
        with patch.object(candidate, 'QUALIFIED', True), self.assertRaises(Conflict):
            candidate.submit_bound_candidate(self.store, other, binding_id=self.binding,
                attempt_id=self.attempt, principal_id=self.principal)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_jobs').fetchone()[0], 0)
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('UPDATE reference_owned_profile_candidate SET pipeline_digest=?', ('c' * 64,))

    def test_process_dies_after_commit_before_response_and_replays(self):
        self.install()
        self.queue.close()
        child = subprocess.Popen([sys.executable, '-c', _LOST_RESPONSE_CHILD,
            self.root, json.dumps(self.submission), self.binding, self.attempt, self.principal],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print('owned synthetic child pid:', child.pid, flush=True)
        try:
            _, stderr = child.communicate(timeout=15)
            self.assertEqual(child.returncode, 75, stderr.decode(errors='replace'))
        finally:
            _reap_owned(child)
        self.queue = Queue(self.root)
        self.addCleanup(self.queue.close)
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        replay, created = self.submit()
        self.assertFalse(created)
        self.assertEqual(len(self.store.list()), 1)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_owned_binding_candidate').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT job_id FROM reference_owned_binding_candidate').fetchone()[0], replay['jobId'])

    def test_two_client_processes_exact_replay_at_one_owner(self):
        self.install()
        owner = self
        gate = threading.Barrier(2, timeout=8)
        class OwnedServer(ThreadingHTTPServer):
            def __init__(self, *args):
                self.owned_handlers = []
                super().__init__(*args)
            def process_request(self, request, client_address):
                thread = threading.Thread(target=self.process_request_thread,
                    args=(request, client_address), daemon=True)
                self.owned_handlers.append(thread)
                thread.start()
        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                self.connection.settimeout(5)
            def log_message(self, *_):
                pass
            def do_POST(self):
                try:
                    length = int(self.headers.get('Content-Length', '0'))
                    if not 0 < length <= 65536:
                        raise ValueError('invalid synthetic input')
                    payload = json.loads(self.rfile.read(length))
                    gate.wait()
                    job, created = candidate.submit_bound_candidate(owner.store, payload,
                        binding_id=owner.binding, attempt_id=owner.attempt, principal_id=owner.principal)
                    data = json.dumps({'jobId': job['jobId'], 'created': created}).encode()
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except Exception:
                    self.send_error(500, 'synthetic handler failed')
        server = OwnedServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        children = []
        thread.start()
        try:
            url = 'http://127.0.0.1:%d/' % server.server_port
            with patch.object(candidate, 'QUALIFIED', True):
                for _ in range(2):
                    child = subprocess.Popen([sys.executable, '-c', _HTTP_CLIENT, url, json.dumps(self.submission)],
                        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    children.append(child)
                    print('owned synthetic client pid:', child.pid, flush=True)
                results = []
                for child in children:
                    stdout, stderr = child.communicate(timeout=15)
                    self.assertEqual(child.returncode, 0, stderr.decode(errors='replace'))
                    results.append(json.loads(stdout))
            self.assertEqual(sorted(item['created'] for item in results), [False, True])
            self.assertEqual(results[0]['jobId'], results[1]['jobId'])
            self.assertEqual(len(self.store.list()), 1)
        finally:
            failures = []
            try:
                for child in children:
                    try:
                        _reap_owned(child)
                    except BaseException as error:
                        failures.append(error)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)
                if thread.is_alive():
                    failures.append(RuntimeError('owned synthetic server thread did not exit'))
                for handler in server.owned_handlers:
                    handler.join(timeout=15)
                    if handler.is_alive():
                        failures.append(RuntimeError('owned synthetic handler thread did not exit'))
            if failures:
                raise failures[0]


if __name__ == '__main__':
    unittest.main()
