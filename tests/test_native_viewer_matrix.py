"""Synthetic native-path matrix: actual Viewer worker modules -> native facade.

Opt-in: requires Node.js >= 24 and VIEWER_SOURCE_ROOT pointing at a pinned,
read-only LedgeTop-Ops tree (apps/viewer/server), on Linux (Viewer storage uses
/proc). Viewer's npm dependencies (unzipper, geotiff, ...) must resolve from an
ancestor node_modules, e.g. a read-only /node_modules mount in a container;
VIEWER_NODE_PATH is passed through as NODE_PATH for CommonJS-only setups.
Everything runs on loopback with invented bytes and synthetic engines; no
photogrammetry, NAS, installed service or client data is involved. Cases that
document a known gap assert the DESIRED behaviour under expectedFailure, so a
fix shows up as an unexpected success instead of being silently absorbed.
"""
from contextlib import ExitStack
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
import zipfile

from processing.app import make_handler
from processing.config import Config
from processing.prototype import PROFILE
from processing.synthetic_engine import SyntheticEngine, handler_for

DRIVER = Path(__file__).resolve().parent / 'native_matrix' / 'viewer_driver.cjs'
VIEWER_ROOT = os.environ.get('VIEWER_SOURCE_ROOT')
NODE = shutil.which('node')


def _node_major():
    if not NODE:
        return 0
    try:
        return int(subprocess.run([NODE, '--version'], capture_output=True, text=True, timeout=10).stdout.strip().lstrip('v').split('.')[0])
    except (OSError, ValueError, subprocess.SubprocessError):
        return 0


AVAILABLE = bool(VIEWER_ROOT and Path(VIEWER_ROOT, 'apps', 'viewer', 'server', 'processingWorker.js').is_file()
                 and _node_major() >= 24)


class OdmLayoutEngine(SyntheticEngine):
    """Synthetic engine whose archive has ODM output paths (invented bytes)."""

    def download(self, identifier):
        if self.info(identifier)['status']['code'] != 40:
            raise ValueError('fixture unfinished')
        output = io.BytesIO()
        with zipfile.ZipFile(output, 'w') as archive:
            archive.writestr('SYNTHETIC-TRANSFER-ONLY.txt', 'Invented bytes in ODM paths; not a real result.\n')
            archive.writestr('entwine_pointcloud/ept.json', json.dumps({'synthetic': True}))
            archive.writestr('odm_georeferencing/odm_georeferenced_model.laz', b'SYNTHETIC LAZ')
            archive.writestr('odm_texturing/odm_textured_model_geo.obj', 'mtllib odm_textured_model_geo.mtl\nv 0 0 0\n')
            archive.writestr('odm_texturing/odm_textured_model_geo.mtl', 'newmtl m\nmap_Kd texture.png\n')
            archive.writestr('odm_texturing/texture.png', b'\x89PNG\r\n\x1a\nSYNTHETIC')
            archive.writestr('odm_texturing/odm_textured_model_geo.glb', b'glTF SYNTHETIC')
        output.seek(0)
        return output, len(output.getvalue())


@unittest.skipUnless(AVAILABLE, 'needs Node.js >= 24 and VIEWER_SOURCE_ROOT at a pinned Viewer tree')
class NativeViewerMatrix(unittest.TestCase):
    engine_duration = .25
    engine_type = SyntheticEngine

    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory(prefix='native-matrix-')))
        node_token, self.viewer_token, self.staging_token = (secrets.token_urlsafe(32) for _ in range(3))
        self.engines = {'cluster': self.engine_type(20, self.engine_duration), 'main': self.engine_type(None, self.engine_duration)}
        origins = []
        for engine in self.engines.values():
            origins.append(self._serve(handler_for(engine, node_token)))
        config = Config(secrets.token_urlsafe(32), *origins, 20, 1024 ** 2,
            prototype_state_dir=str(self.root / 'state'), prototype_operator_token=secrets.token_urlsafe(32),
            prototype_node_ids=('cluster', 'main'), treedar_node_ids=('main',),
            local_node_token=node_token, main_node_token=node_token,
            viewer_enabled=True, viewer_token=self.viewer_token, viewer_staging_token=self.staging_token)
        handler = make_handler(config)
        self.queue, self.service = handler.prototype_queue, handler.viewer_service
        self.addCleanup(self.queue.close)
        self.addCleanup(self.service.close)
        self.endpoint = self._serve(handler)
        self.service.start()

    def _serve(self, handler):
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (server.shutdown(), server.server_close(), thread.join(5)))
        return 'http://127.0.0.1:' + str(server.server_port)

    def run_case(self, name, timeout=180, **extra):
        env = {**os.environ, 'MATRIX_CASE': name, 'MATRIX_ROOT': str(self.root), 'MATRIX_ENDPOINT': self.endpoint,
               'MATRIX_TOKEN': self.viewer_token, 'MATRIX_STAGING_TOKEN': self.staging_token, **extra}
        if os.environ.get('VIEWER_NODE_PATH'):
            env['NODE_PATH'] = os.environ['VIEWER_NODE_PATH']
        return subprocess.Popen([NODE, str(DRIVER)], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def finish(self, process, timeout=180):
        stdout, stderr = process.communicate(timeout=timeout)
        lines = [line for line in stdout.splitlines() if line.startswith('{')]
        self.assertTrue(lines, 'driver produced no result: ' + stderr[-1500:])
        record = json.loads(lines[-1])
        self.__class__.results.append(record)
        return record

    def wait_file(self, name, timeout=60):
        deadline = time.monotonic() + timeout
        while not (self.root / name).exists():
            self.assertLess(time.monotonic(), deadline, 'timed out waiting for ' + name)
            time.sleep(.025)

    def commits(self):
        return {name: engine.commits for name, engine in self.engines.items()}

    def retained_archives(self):
        with self.queue._db() as db:
            return [row['archive_path'] for row in db.execute('SELECT archive_path FROM viewer_tasks WHERE archive_path IS NOT NULL')]

    @classmethod
    def setUpClass(cls):
        cls.results = []

    @classmethod
    def tearDownClass(cls):
        target = os.environ.get('MATRIX_RESULTS')
        if target:
            with open(target, 'a', encoding='utf-8') as handle:
                for record in cls.results:
                    handle.write(json.dumps(record, sort_keys=True) + '\n')


class SubmitAndReplay(NativeViewerMatrix):
    def test_t7_lost_commit_response_resumes_without_second_engine_run(self):
        record = self.finish(self.run_case('replay'))
        self.assertTrue(record['ok'], record.get('error'))
        self.assertEqual(record['result']['lostCommitResponses'], 1)
        self.assertEqual(sum(self.commits().values()), 1)
        self.assertNotEqual(record['result']['statusAfterResume'], 'failed', record['result'])
        self.assertNotEqual(record['result']['statusAfterUpstreamCompletion'], 'failed', record['result'])

    def test_t8_interrupted_download_retries_identical_bytes(self):
        record = self.finish(self.run_case('download'))
        self.assertTrue(record['ok'], record.get('error'))
        self.assertEqual(record['result']['firstSha256'], record['result']['secondSha256'])
        self.assertTrue(all(Path(p).is_file() for p in self.retained_archives()))


class SuccessIngest(NativeViewerMatrix):
    engine_type = OdmLayoutEngine

    def test_t1_t12_submit_poll_download_and_retain(self):
        record = self.finish(self.run_case('success'))
        self.assertTrue(record['ok'], record.get('error'))
        result = record['result']
        self.assertEqual(self.commits(), {'cluster': 1, 'main': 0})  # 20 photos -> small node.
        archives = self.retained_archives()
        self.assertEqual(len(archives), 1)
        self.assertTrue(Path(archives[0]).is_file(), 'coordinator result copy was deleted')
        # Ingest commits exactly one model version and hands off to Viewer's
        # separate derivative worker; derivative conversion itself is not run.
        self.assertEqual(result['finalStatus'], 'derivatives', result)
        self.assertEqual(result['modelVersions'], 1)


class SchedulingOrder(NativeViewerMatrix):
    def test_t2_t3_t4_production_first_small_to_cluster_one_global_slot(self):
        guard = self.queue.submit({'requestId': 'matrix-slot-guard', 'environment': 'production', 'profile': PROFILE})[0]
        claim = self.queue.claim({'workerId': 'matrix-guard', 'nodeId': 'main'})
        self.assertEqual(claim['id'], guard['id'])
        process = self.run_case('priority')
        self.wait_file('priority-ready', 90)
        self.assertEqual(sum(self.commits().values()), 0, 'dispatch bypassed the held global slot')
        self.queue.complete(claim['id'], {'claimToken': claim['claimToken'], 'status': 'succeeded', 'result': {'matrixSlotGuard': True}})
        record = self.finish(process)
        self.assertTrue(record['ok'], record.get('error'))
        self.assertEqual(self.commits(), {'cluster': 2, 'main': 1})
        self.assertTrue(all(engine.maximum_running == 1 for engine in self.engines.values()))
        done = sorted((task for engine in self.engines.values() for task in engine.tasks.values() if task['code'] == 40),
                      key=lambda task: task['end'])
        self.assertEqual([task['name'] for task in done], ['production-small', 'production-large', 'staging-small'])


class Maintenance(NativeViewerMatrix):
    def test_t6_draining_small_node_blocks_dispatch_until_reopened(self):
        now = datetime.now(timezone.utc).isoformat()
        node = self.queue.schedule_node('cluster', {'expectedRevision': 0, 'drainAt': now, 'notBefore': now,
                                                    'afterJobId': None, 'targetImage': 'sha256:' + 'a' * 64})
        process = self.run_case('maintenance')
        self.wait_file('maintenance-submitted', 90)
        time.sleep(2)
        self.assertEqual(self.commits(), {'cluster': 0, 'main': 0}, 'small job overflowed or dispatched while draining')
        self.queue.cancel_node('cluster', {'expectedRevision': node['revision'], 'planId': node['planId']})
        (self.root / 'maintenance-reopened').write_text('1')
        record = self.finish(process)
        self.assertTrue(record['ok'], record.get('error'))
        self.assertEqual(record['result']['finalStatus'], 'completed')
        self.assertEqual(self.commits(), {'cluster': 1, 'main': 0})


class Cancellation(NativeViewerMatrix):
    engine_duration = 30

    def test_t5_running_cancel_is_idempotent_and_reaches_engine(self):
        record = self.finish(self.run_case('cancel'))
        self.assertTrue(record['ok'], record.get('error'))
        self.assertEqual(record['result']['viewerStatus'], 'cancelled')
        self.assertEqual(record['result']['upstreamStatus'], 'cancelled')


class CoordinatorOutage(NativeViewerMatrix):
    engine_duration = 8

    def test_t10_short_outage_within_retry_window_recovers(self):
        record = self.finish(self.run_case('outage', OUTAGE_FAILURES='2'))
        self.assertTrue(record['ok'], record.get('error'))
        self.assertNotEqual(record['result']['finalStatus'], 'failed', record['result'])

    @unittest.expectedFailure
    def test_t9_long_outage_keeps_attempt_while_upstream_runs(self):
        """Desired: an unreachable coordinator must not fail a still-running job.

        Viewer main fails the attempt after five failed reconcile claims
        (processingWorker.js processOne backoff); recorded as expected failure.
        """
        record = self.finish(self.run_case('outage', OUTAGE_FAILURES='6'))
        self.assertTrue(record['ok'], record.get('error'))
        self.assertNotEqual(record['result']['statusAfterOutage'], 'failed', record['result'])


# ----- Versioned fixed-source regressions -------------------------------------
# These never change the v1 expectations above. Each targets one exact Viewer
# source, is selected explicitly with MATRIX_VIEWER_TARGET and first proves the
# pinned module bytes actually ran.

FIXED_POLL_V1 = dict(
    target='viewer-e66e37b-poll-fix-v1',
    viewer_commit='e66e37b31165035d162109bf6a1faa2ebc47b5cd',
    processing_worker_sha256='fdb948ee45c0d61ec76dba94e42bca8ae4d8b2619b98309bba35bfab12f06752')


FIXED_POLL_7DE = dict(
    target='viewer-7de601c-poll-only-v1',
    viewer_commit='7de601c1381a791fe9c9ec7f3385a5645724b91e',
    # sha256 of the checked-out bytes (CRLF working tree, core.autocrlf=true);
    # its LF git blob digest is 5e32e0afbb7f8455879a96b1b300d472615e4c324bf4f6af105a915d6e564cdf.
    processing_worker_sha256='d08e2512d5449a9a3192c062492c2e88c0577ae41c139b6d7c0e40a2c5aadbf4')


class FixedPollRegression:
    """Plain T9 regression for one pinned Viewer source (set ``pin``)."""

    pin = None
    engine_duration = 8

    def loaded_worker_sha256(self, record):
        return {m['path']: m['sha256'] for m in record['viewerModules']['loaded']}.get(
            'apps/viewer/server/processingWorker.js')

    def check_fixed_poll(self):
        record = self.finish(self.run_case('outage', OUTAGE_FAILURES='6'))
        self.assertEqual(self.loaded_worker_sha256(record), self.pin['processing_worker_sha256'],
                         'the selected Viewer tree is not the pinned poll fix ' + self.pin['viewer_commit'])
        self.assertTrue(record['ok'], record.get('error'))
        result = record['result']
        self.assertEqual(len(result['outageSteps']), 6)
        self.assertTrue(all(step['status'] == 'queued_upstream' for step in result['outageSteps']), result)
        self.assertEqual({step['jobs']['error_code'] for step in result['outageSteps']}, {'provider_observation_unavailable'})
        self.assertEqual(result['statusAfterOutage'], 'queued_upstream')
        self.assertIn(result['finalStatus'], ('ingesting', 'derivatives', 'ready_for_review'))


@unittest.skipUnless(os.environ.get('MATRIX_VIEWER_TARGET') == FIXED_POLL_V1['target'],
                     'select with MATRIX_VIEWER_TARGET=' + FIXED_POLL_V1['target'])
class CoordinatorOutageFixedPollV1(FixedPollRegression, NativeViewerMatrix):
    """T9 against Viewer e66e37b only: a plain assertion, no expectedFailure."""

    pin = FIXED_POLL_V1

    def test_t9_fixed_v1_long_outage_keeps_attempt_and_ingests(self):
        self.check_fixed_poll()


@unittest.skipUnless(os.environ.get('MATRIX_VIEWER_TARGET') == FIXED_POLL_7DE['target'],
                     'select with MATRIX_VIEWER_TARGET=' + FIXED_POLL_7DE['target'])
class CoordinatorOutageFixedPoll7de(FixedPollRegression, NativeViewerMatrix):
    """T9 against Viewer 7de601c only (the final poll-only candidate)."""

    pin = FIXED_POLL_7DE

    def test_t9_fixed_7de_long_outage_keeps_attempt_and_ingests(self):
        self.check_fixed_poll()


FIXED_74E = dict(
    target='viewer-74e97db-warning-atomic-v1',
    viewer_commit='74e97db414df544075c71dab58215a61e2a550c3',
    # Checked-out bytes (CRLF, core.autocrlf=true) that Node loads; LF git blob
    # digests: processingWorker 7713468e..., processingRepository 26265f1c...,
    # processingOverview f87a3760....
    processing_worker_sha256='32cf4f95085e4839fed9b5f5fe21c02fbe31c4e41b1dc70fa8ac4cfb77e5459b',
    modules={
        'apps/viewer/server/processingWorker.js': '32cf4f95085e4839fed9b5f5fe21c02fbe31c4e41b1dc70fa8ac4cfb77e5459b',
        'apps/viewer/server/processingRepository.js': '212ea4fe1b363b1e977d940dd9ba32c488aaa411e77574a6b711896941a6efdb',
        'apps/viewer/server/processingOverview.js': '556a69989e406c66cb8d372ca21d8d39ab3a408d739188cb53272edf5b618b12'})


@unittest.skipUnless(os.environ.get('MATRIX_VIEWER_TARGET') == FIXED_74E['target'],
                     'select with MATRIX_VIEWER_TARGET=' + FIXED_74E['target'])
class ObservationWarningAtomicIngest74e(FixedPollRegression, NativeViewerMatrix):
    """Viewer 74e97db only: T9 survival, additive warning and atomic ingest."""

    pin = FIXED_74E

    def assert_modules(self, record):
        loaded = {m['path']: m['sha256'] for m in record['viewerModules']['loaded']}
        for path, digest in self.pin['modules'].items():
            self.assertEqual(loaded.get(path), digest, 'not the pinned 74e97db source: ' + path)

    def test_t9_74e_long_outage_keeps_attempt_and_ingests(self):
        self.check_fixed_poll()

    def test_t11w_74e_observation_warning_is_raised_and_cleared(self):
        record = self.finish(self.run_case('outage', OUTAGE_FAILURES='6'))
        self.assert_modules(record)
        self.assertTrue(record['ok'], record.get('error'))
        result = record['result']
        self.assertEqual([step['observationWarnings'] for step in result['outageSteps']], [1] * 6, result)
        self.assertEqual(result['observationWarningsAfterRecovery'], 0, result)

    def test_t13_74e_ingest_enqueue_failure_never_strands_an_ingesting_attempt(self):
        record = self.finish(self.run_case('atomic_ingest'))
        self.assert_modules(record)
        self.assertTrue(record['ok'], record.get('error'))
        result = record['result']
        self.assertFalse(result['strandedIngesting'], result)
        self.assertEqual(result['afterInjection']['status'], 'running', result)
        self.assertEqual(result['afterInjection']['reconcile']['error_code'], 'provider_observation_persistence_unavailable', result)
        self.assertEqual(result['finalStatus'], 'derivatives', result)
        self.assertEqual(result['modelVersions'], 1)


if __name__ == '__main__':
    unittest.main()
