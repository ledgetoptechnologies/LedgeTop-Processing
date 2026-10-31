"""Bounded subprocess test for the disposable cross-language fixture."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from urllib.request import urlopen
import uuid

from tests.test_viewer_service import synthetic_zip


class RehearsalServerTest(unittest.TestCase):
    def test_loopback_archive_remains_available_after_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'state';root.mkdir();archive=Path(tmp)/'synthetic.zip';archive.write_bytes(synthetic_zip())
            identifier=str(uuid.uuid4())
            process=subprocess.Popen([sys.executable,'-m','tests.viewer_archive_rehearsal_server','--root',str(root),
                '--archive',str(archive),'--uuid',identifier],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True)
            try:
                ready=json.loads(process.stdout.readline());self.assertEqual(ready['uuid'],identifier)
                url='http://127.0.0.1:%d/task/%s/download/all.zip?token=%s' % (ready['port'],identifier,ready['token'])
                info=url.replace('/download/all.zip','/info')
                with urlopen(info,timeout=5) as response:self.assertEqual(json.load(response)['status']['code'],40)
                with urlopen(url,timeout=5) as response:self.assertEqual(response.read(),archive.read_bytes())
                process.stdin.write('{"command":"restart"}\n');process.stdin.flush()
                restarted=json.loads(process.stdout.readline());self.assertEqual(restarted,ready)
                with urlopen(info,timeout=5) as response:self.assertEqual(json.load(response)['status']['code'],40)
                with urlopen(url,timeout=5) as response:self.assertEqual(response.read(),archive.read_bytes())
                process.stdin.write('{"command":"stop"}\n');process.stdin.flush()
                self.assertEqual(json.loads(process.stdout.readline()),{'ok':True});self.assertEqual(process.wait(timeout=10),0)
                self.assertEqual(archive.read_bytes(),synthetic_zip())
            finally:
                if process.poll() is None:process.kill();process.wait(timeout=5)
                process.stdin.close();process.stdout.close()
