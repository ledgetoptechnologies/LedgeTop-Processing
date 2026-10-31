"""Mock-only safety checks; never calls a Docker daemon or shared resource."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


@unittest.skipUnless(os.name=='posix','POSIX shell fixture required')
class ViewerBridgeHarnessTests(unittest.TestCase):
    def test_uncertain_creation_records_identity_and_never_removes_by_name(self):self._failure('uncertain')
    def test_changed_container_identity_is_never_stopped_or_removed(self):self._failure('identity-mismatch')
    def test_full_mode_records_uncertain_creation_without_cleanup(self):self._failure('uncertain','full')
    def test_full_mode_rejects_modified_fixture_before_docker(self):self._failure('tampered','full')
    def test_archive_failure_stops_before_docker(self):self._failure('archive-failure','full')
    def test_default_mode_is_full(self):self._failure('uncertain',None)
    def test_reconcile_mode_is_rejected_before_docker(self):
        script=Path(__file__).resolve().parents[1]/'ops'/'run_viewer_bridge_rehearsal.sh'
        with tempfile.TemporaryDirectory() as directory:
            env={**os.environ,'VIEWER_REPO':directory,'PROCESSING':directory,'BRIDGE_MODE':'reconcile'}
            result=subprocess.run(['sh',str(script)],env=env,text=True,capture_output=True,timeout=5)
            self.assertNotEqual(result.returncode,0)
            self.assertIn('reconcile mode is not qualified',result.stderr)
            self.assertFalse(list(Path(directory).iterdir()))

    def _failure(self,mode,bridge_mode='full'):
        script=Path(__file__).resolve().parents[1]/'ops'/'run_viewer_bridge_rehearsal.sh'
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);bin_dir=root/'bin';bin_dir.mkdir();log=root/'docker.log'
            (bin_dir/'git').write_text('''#!/bin/sh
shift 2
if [ "$1" = -c ]; then shift 2; fi
case "$1" in
 rev-parse) case "$2" in HEAD) echo 1111111111111111111111111111111111111111;; *) echo baaaf2439d5a641ae59c3f550f77dc7512e3b84d;; esac;;
 status|merge-base|diff|ls-files) exit 0;;
 archive) [ "$MOCK_MODE" != archive-failure ] || exit 7; printf synthetic;;
 *) exit 1;;
esac
''')
            fixture_dir=script.parents[1]/'tests'
            server_hash=hashlib.sha256((fixture_dir/'viewer_full_bridge_server.py').read_bytes()).hexdigest()
            client_hash=hashlib.sha256((fixture_dir/'viewer_full_bridge_client.cjs').read_bytes()).hexdigest()
            (bin_dir/'sha256sum').write_text('''#!/bin/sh
for item in "$@"; do
 case "$item" in
 */verify_viewer_export.py) hash=b075ad603632339d38b31fbdc1de614889546a1014aa814c8c6381e5999b34ef;;
 */viewer_full_bridge_server.py) hash="$MOCK_SERVER_HASH"; [ "$MOCK_MODE" != tampered ] || hash=bad;;
 */viewer_full_bridge_client.cjs) hash="$MOCK_CLIENT_HASH";;
 *) hash=synthetic;;
 esac
 printf '%s  %s\\n' "$hash" "$item"
done
''')
            (bin_dir/'python3').write_text('#!/bin/sh\ncase "$1" in */verify_viewer_export.py) exit 0;; esac\nexec "$MOCK_PYTHON" "$@"\n')
            (bin_dir/'tar').write_text('#!/bin/sh\nexit 0\n')
            (bin_dir/'docker').write_text('''#!/bin/sh
printf '%s\\n' "$*" >> "$MOCK_DOCKER_LOG"
if [ "$1" = image ]; then
 case "$3" in sha256:0bb883*) echo sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668;;
 sha256:aa943*) echo sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159;; *) exit 1;; esac
 exit 0
fi
if [ "$1" = run ]; then
 if [ "$MOCK_MODE" = identity-mismatch ]; then echo aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa; exit 0; fi
 exit 75
fi
if [ "$1" = inspect ]; then echo unexpected-identity; exit 0; fi
exit 1
''')
            for item in bin_dir.iterdir():item.chmod(0o755)
            try:subprocess.run([str(bin_dir/'tar')],check=True,timeout=2)
            except PermissionError:self.skipTest('temporary filesystem does not permit fixture execution')
            env={**os.environ,'PATH':str(bin_dir)+os.pathsep+os.environ['PATH'],'TMPDIR':str(root),
                 'VIEWER_REPO':str(root),'PROCESSING':str(root),'MOCK_DOCKER_LOG':str(log),'MOCK_MODE':mode,'MOCK_PYTHON':__import__('sys').executable,'MOCK_SERVER_HASH':server_hash,'MOCK_CLIENT_HASH':client_hash}
            if bridge_mode is None:env.pop('BRIDGE_MODE',None)
            else:env['BRIDGE_MODE']=bridge_mode
            result=subprocess.run(['sh',str(script)],env=env,text=True,capture_output=True,timeout=10)
            self.assertNotEqual(result.returncode,0)
            if mode=='archive-failure':
                self.assertFalse(log.exists())
                return
            if mode=='tampered':
                self.assertIn('Full server fixture hash mismatch',result.stderr)
                self.assertIn('bad  ',result.stdout)
                self.assertFalse(log.exists())
                return
            if mode=='uncertain':self.assertIn('Server creation outcome uncertain',result.stderr)
            else:self.assertIn('Container identity unavailable or changed',result.stderr)
            evidence=list(root.glob('codex-viewer-bridge-*/run-identity.json'));self.assertEqual(len(evidence),1)
            identity=json.loads(evidence[0].read_text())
            self.assertEqual(identity['bridgeMode'],bridge_mode or 'full')
            self.assertIn(identity['runId'],identity['serverName']);self.assertIn(identity['runId'],identity['clientName'])
            calls=log.read_text().splitlines()
            self.assertTrue(any(line.startswith('run ') for line in calls))
            self.assertFalse(any(line.startswith(('stop ','rm ')) for line in calls))
