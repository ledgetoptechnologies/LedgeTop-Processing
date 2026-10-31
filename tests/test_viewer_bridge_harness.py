"""Mock-only safety checks; never calls a Docker daemon or shared resource."""
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

    def _failure(self,mode):
        script=Path(__file__).resolve().parents[1]/'ops'/'run_viewer_bridge_rehearsal.sh'
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);bin_dir=root/'bin';bin_dir.mkdir();log=root/'docker.log'
            (bin_dir/'git').write_text('''#!/bin/sh
shift 2
case "$1" in
 rev-parse) case "$2" in HEAD) echo 1111111111111111111111111111111111111111;; *) echo baaaf2439d5a641ae59c3f550f77dc7512e3b84d;; esac;;
 status|merge-base|diff) exit 0;;
 archive) printf synthetic;;
 *) exit 1;;
esac
''')
            (bin_dir/'tar').write_text('#!/bin/sh\nexit 0\n')
            (bin_dir/'docker').write_text('''#!/bin/sh
printf '%s\\n' "$*" >> "$MOCK_DOCKER_LOG"
if [ "$1" = image ]; then
 case "$3" in viewer-*) echo sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668;;
 python:*) echo sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159;; *) exit 1;; esac
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
                 'VIEWER_REPO':str(root),'PROCESSING':str(root),'MOCK_DOCKER_LOG':str(log),'MOCK_MODE':mode}
            result=subprocess.run(['sh',str(script)],env=env,text=True,capture_output=True,timeout=10)
            self.assertNotEqual(result.returncode,0)
            if mode=='uncertain':self.assertIn('Server creation outcome uncertain',result.stderr)
            else:self.assertIn('Container identity unavailable or changed',result.stderr)
            evidence=list(root.glob('codex-viewer-bridge-*/run-identity.json'));self.assertEqual(len(evidence),1)
            identity=json.loads(evidence[0].read_text())
            self.assertIn(identity['runId'],identity['serverName']);self.assertIn(identity['runId'],identity['clientName'])
            calls=log.read_text().splitlines()
            self.assertTrue(any(line.startswith('run ') for line in calls))
            self.assertFalse(any(line.startswith(('stop ','rm ')) for line in calls))
