"""Opt-in synthetic test; requires Node 24 and an independently pinned Viewer export."""
import hashlib
import os
from pathlib import Path
import subprocess
import unittest

from unittest.mock import patch
from tests.test_native_plan_http import PlanHTTP

PINNED = {
    'apps/viewer/server/nativeManifestBindingCandidate.js':'6073f1cb3ae9d9dee2ea20d1a89a0f35f70203dd04a3332851831b9bc27cd832',
    'apps/viewer/server/nativeManifestCandidate.js':'3e745de2a759cd232c847cc3d4c128c49e4c87c1785e2a607aab2fd1c2a6c890',
    'apps/viewer/server/nativeBindingStrictJson.js':'8825c09a5b4c9729b42623fe0c7abce61f4591b4e06d2c80297a557f510078cd',
    'apps/viewer/server/providerCredentials.js':'2ed8f996ac8410dd904c9be2b4d6db25fc72a345f4f9e90edfefd802773ba0fe',
    'apps/viewer/server/providerAuth.js':'0e713d282ae00d975ae50ed89eaba7bd874af5954c5b20fec85aa40144d60250',
    'apps/viewer/server/nativePlanCandidate.js':'9defa4e94d57b42c305280657fa51209772bf9400a1a0c09ec56cbf2bdf38ab0',
    'apps/viewer/test/fixtures/native-input-manifest-v1-candidate.json':'e146a50b72785e45d88b9ca561d5e642957ba163089dcf5b73533669ad4192b7',
}


@unittest.skipUnless(os.environ.get('NATIVE_BINDING_CROSS_LANGUAGE') == '1', 'explicit synthetic fixture only')
class CrossLanguageBinding(PlanHTTP):
    def test_viewer_client_replays_after_discarded_reply_and_restart(self):
        viewer = Path(os.environ['PINNED_VIEWER_EXPORT']).resolve(strict=True)
        if PINNED is None: self.fail('Viewer source not pinned and reviewed')
        for name, expected in PINNED.items():
            target = viewer / name
            current = target
            while current != viewer:
                self.assertFalse(current.is_symlink())
                current = current.parent
            self.assertTrue(target.is_file())
            self.assertEqual(hashlib.sha256(target.read_bytes()).hexdigest(), expected)
        node = os.environ['NODE_BINARY']
        fixture = Path(__file__).parent / 'fixtures' / 'native_binding_cross_language.cjs'
        endpoint = 'http://127.0.0.1:' + str(self.server.server_port)

        def run(phase):
            result = subprocess.run([node, str(fixture), str(viewer), str(self.root), endpoint, phase],
                                    capture_output=True, text=True, timeout=20, check=False)
            self.assertEqual(result.returncode, 0, result.stderr[-2000:])

        # Synthetic healthy catalog: this is not evidence of an engine.
        with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):run('lose')
        # The server retains its port, so Viewer authority binding remains stable.
        port = self.server.server_port
        self.stop()
        from processing.app import make_handler
        from http.server import ThreadingHTTPServer
        import threading
        handler = make_handler(self.config)
        try:
            server = ThreadingHTTPServer(('127.0.0.1', port), handler)
        except BaseException:
            handler.viewer_service.close(); handler.prototype_queue.close(); handler.prototype_queue.authority_lease.close()
            raise
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .02})
        try:
            thread.start()
        except BaseException:
            server.server_close()
            handler.viewer_service.close(); handler.prototype_queue.close(); handler.prototype_queue.authority_lease.close()
            raise
        self.handler, self.server, self.thread = handler, server, thread
        with patch.object(self.handler.viewer_service,'_catalog',side_effect=RuntimeError('offline')):run('recover')
        with patch.object(self.handler.viewer_service,'_catalog',return_value={'ready':True}):run('nonempty')
        with self.handler.prototype_queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_manifest_bindings').fetchone()[0], 2)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_tasks').fetchone()[0], 0)

