"""Run the synthetic durable Viewer acceptance pair in a disposable container.

Set LTDS_VIEWER_SOURCE_DIR to a pinned Viewer checkout. Requires Docker, the
already-local node:24-bookworm-slim image, and Python cryptography. No image is pulled.
"""
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import time
import uuid

HERE = Path(__file__).resolve().parent
PROCESSING = HERE.parents[2]
viewer = Path(os.environ['LTDS_VIEWER_SOURCE_DIR']).resolve(strict=True)
if not (viewer / 'apps/viewer/server/referenceAcceptance.js').is_file():
    raise SystemExit('Viewer source checkout is missing the acceptance ledger')

name = 'lt-acceptance-test-' + uuid.uuid4().hex
with tempfile.TemporaryDirectory(prefix='lt-durable-acceptance-') as scratch:
    env = os.environ.copy()
    env.update(LT_TEST_DIR=scratch, LTDS_PROCESSING_REPO=str(PROCESSING))
    subprocess.run([sys.executable, str(HERE / 'make_certs.py')], env=env, check=True)
    args = ['docker', 'run', '--rm', '--pull=never', '--name', name,
            '--memory', '1g', '--cpus', '1', '-p', '127.0.0.1::44443',
            '-v', f'{viewer}:/viewer:ro', '-v', f'{HERE}:/scripts:ro',
            '-v', f'{scratch}:/harness:ro', '--tmpfs', '/data:size=64m',
            'node:24-bookworm-slim', 'node', '/scripts/server.js']
    child = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        deadline = time.monotonic() + 30
        origin = None
        while time.monotonic() < deadline:
            if child.poll() is not None:
                raise RuntimeError('test container exited before readiness: ' + child.stdout.read())
            result = subprocess.run(['docker', 'port', name, '44443/tcp'], capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip().startswith('127.0.0.1:'):
                port = int(result.stdout.strip().rsplit(':', 1)[1])
                origin = f'https://localhost:{port}'
                break
            time.sleep(.2)
        if origin is None:
            raise RuntimeError('test container port was not published')
        # The published port may precede Node's listen call; wait for READY.
        lines = queue.Queue()
        threading.Thread(target=lambda: lines.put(child.stdout.readline()), daemon=True).start()
        try:
            line = lines.get(timeout=15)
        except queue.Empty:
            raise RuntimeError('test server readiness timed out') from None
        if not line.startswith('READY '):
            raise RuntimeError('test server did not report readiness: ' + line)
        env['LT_TEST_ORIGIN'] = origin
        subprocess.run([sys.executable, str(HERE / 'run_e2e.py')], env=env, check=True, timeout=60)
    finally:
        subprocess.run(['docker', 'stop', '--time', '3', name], capture_output=True, timeout=10)
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)
