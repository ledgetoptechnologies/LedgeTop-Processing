"""Opt-in packaged worker smoke test; uses synthetic data and no network access."""
import json
import os
from pathlib import Path
import subprocess
import unittest
import uuid


@unittest.skipUnless(os.environ.get('LT_REFERENCE_WORKER_TEST_IMAGE'),
                     'set LT_REFERENCE_WORKER_TEST_IMAGE to an immutable local image ID')
class ReferenceWorkerImageE2ETests(unittest.TestCase):
    def test_daemon_resumes_transfer_after_coordinator_outage_and_sigkill(self):
        image = os.environ['LT_REFERENCE_WORKER_TEST_IMAGE']
        revision = os.environ.get('LT_REFERENCE_WORKER_SOURCE_REVISION', '')
        self.assertRegex(image, r'^sha256:[0-9a-f]{64}$')
        self.assertRegex(revision, r'^[0-9a-f]{40}$')
        inspected = subprocess.run(['docker', 'image', 'inspect', image], check=True,
                                   capture_output=True, text=True, timeout=20)
        metadata = json.loads(inspected.stdout)
        self.assertEqual(len(metadata), 1)
        self.assertEqual(metadata[0]['Id'], image)
        self.assertEqual((metadata[0]['Config'].get('Labels') or {}).get(
            'org.opencontainers.image.revision'), revision)
        root = Path(__file__).resolve().parents[1]
        mounts = ((root/'tests/reference_worker_image_fixture.py', '/tmp/fixture.py'),
                  (root/'ops/native_node_fixture.py', '/fixture/native_node_fixture.py'),
                  (root/'contracts/reference-v1.contract.json', '/fixture/reference-v1.contract.json'))
        name = 'lt-test-worker-image-' + uuid.uuid4().hex
        command = ['docker', 'run', '--name', name, '--rm', '--network', 'none', '--read-only',
                   '--tmpfs', '/tmp:rw,noexec,nosuid,size=128m', '--cpus', '1', '--memory', '512m',
                   '--pids-limit', '80', '-e', 'PYTHONPATH=/app']
        for source, destination in mounts:
            command += ['--mount', f'type=bind,src={source},dst={destination},readonly']
        command += ['--entrypoint', 'python', image, '/tmp/fixture.py']
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
            self.assertEqual(result.returncode, 0, result.stderr[-4000:])
            self.assertEqual(json.loads(result.stdout)['result'], 'PASS')
        finally:
            # Only this test's randomly named container is eligible for removal.
            subprocess.run(['docker', 'rm', '-f', name], capture_output=True, timeout=20)
