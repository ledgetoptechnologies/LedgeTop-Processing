"""Bounded subprocess test for the disposable cross-language fixture."""
import json
import hashlib
import queue
import signal
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import uuid

from tests.test_viewer_service import synthetic_zip
from tests.test_viewer_http import BOUNDARY, multipart


def read_response(process):
    result=queue.Queue(maxsize=1)
    threading.Thread(target=lambda:result.put(process.stdout.readline()),daemon=True).start()
    return json.loads(result.get(timeout=15))


def cleanup(process):
    try:
        if process.poll() is None:
            # The group leader remains unreaped until wait(), so its identity
            # cannot be recycled between this liveness check and killpg.
            if os.name=='posix':
                try:os.killpg(process.pid,signal.SIGKILL)
                except ProcessLookupError:pass
            else:process.kill()
            process.wait(timeout=5)
        elif process.returncode != 0:
            # Once the leader has been reaped, its group identity is no longer
            # safe to signal. Child ownership is uncertain; report it.
            raise RuntimeError('fixture exited abnormally; descendant cleanup unverified')
    finally:
        process.stdin.close();process.stdout.close()


class RehearsalServerTest(unittest.TestCase):
    def test_unprepared_accepts_client_submission_and_explicit_finish(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'state';root.mkdir();archive=Path(tmp)/'synthetic.zip';archive.write_bytes(synthetic_zip())
            identifier=str(uuid.uuid4());fallback=str(uuid.uuid4())
            process=subprocess.Popen([sys.executable,'-m','tests.viewer_archive_rehearsal_server','--root',str(root),
                '--archive',str(archive),'--uuid',fallback,'--unprepared'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,text=True,start_new_session=os.name=='posix')
            try:
                ready=read_response(process);base='http://127.0.0.1:%d'%ready['port'];token=ready['token']
                def request(route,body=None,headers=None):
                    return urlopen(Request(base+route+'?token='+token,data=body,headers=headers or {}),timeout=5)
                with self.assertRaises(HTTPError) as missing:request('/task/'+identifier+'/info')
                self.assertEqual(missing.exception.code,404)
                headers={'Content-Type':'multipart/form-data; boundary='+BOUNDARY}
                with request('/task/new/init',multipart([('name',None,b'Synthetic'),('options',None,b'[]')]),dict(headers,**{'set-uuid':identifier})) as response:self.assertEqual(json.load(response)['uuid'],identifier)
                with request('/task/new/upload/'+identifier,multipart([('images','photo.jpg',b'synthetic')]),headers) as response:self.assertTrue(json.load(response)['success'])
                with request('/task/new/commit/'+identifier,b'') as response:self.assertTrue(json.load(response)['success'])
                for command in ('tick','finish'):
                    process.stdin.write(json.dumps({'command':command,'uuid':identifier})+'\n');process.stdin.flush();self.assertEqual(read_response(process),{'result':'ok'})
                with request('/task/'+identifier+'/info') as response:self.assertEqual(json.load(response)['status']['code'],40)
                with request('/task/'+identifier+'/download/all.zip') as response:self.assertEqual(response.read(),archive.read_bytes())
                process.stdin.write(json.dumps({'command':'archive_path','uuid':identifier})+'\n');process.stdin.flush()
                captured=Path(read_response(process)['path']);self.assertEqual(captured.read_bytes(),archive.read_bytes())
                def evidence(task):
                    process.stdin.write(json.dumps({'command':'input_evidence','uuid':task,'filename':'photo.jpg'})+'\n');process.stdin.flush()
                    return read_response(process)
                expected={'bytes':len(b'synthetic'),'sha256':hashlib.sha256(b'synthetic').hexdigest()}
                self.assertEqual(evidence(identifier),expected);self.assertIn('error',evidence(fallback))
                process.stdin.write('{"command":"restart"}\n');process.stdin.flush();self.assertEqual(read_response(process),ready)
                self.assertEqual(evidence(identifier),expected)
                process.stdin.write('{"command":"stop"}\n');process.stdin.flush();self.assertEqual(read_response(process),{'ok':True});self.assertEqual(process.wait(timeout=10),0)
                self.assertEqual(archive.read_bytes(),synthetic_zip())
            finally:cleanup(process)

    def test_interrupted_engine_download_holds_partial_through_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'state';root.mkdir();archive=Path(tmp)/'synthetic.zip';archive.write_bytes(synthetic_zip())
            process=subprocess.Popen([sys.executable,'-m','tests.viewer_archive_rehearsal_server','--root',str(root),
                '--archive',str(archive),'--uuid',str(uuid.uuid4()),'--interrupt-first'],stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True,start_new_session=os.name=='posix')
            try:
                ready=read_response(process)
                url='http://127.0.0.1:%d/task/%s/download/all.zip?token=%s' % (ready['port'],ready['uuid'],ready['token'])
                with self.assertRaises(HTTPError) as first:urlopen(url,timeout=5)
                self.assertEqual(first.exception.code,503)
                partial=list(root.rglob('all.zip.partial'));self.assertEqual(len(partial),1)
                saved=partial[0].read_bytes();self.assertTrue(saved)
                process.stdin.write('{"command":"restart"}\n');process.stdin.flush();self.assertEqual(read_response(process),ready)
                with self.assertRaises(HTTPError) as second:urlopen(url,timeout=5)
                self.assertIn(second.exception.code,(409,503));self.assertEqual(partial[0].read_bytes(),saved)
                self.assertEqual(archive.read_bytes(),synthetic_zip())
                process.stdin.write('{"command":"stop"}\n');process.stdin.flush();self.assertEqual(read_response(process),{'ok':True})
                self.assertEqual(process.wait(timeout=10),0)
            finally:
                cleanup(process)

    def test_loopback_archive_remains_available_after_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'state';root.mkdir();archive=Path(tmp)/'synthetic.zip';archive.write_bytes(synthetic_zip())
            identifier=str(uuid.uuid4())
            process=subprocess.Popen([sys.executable,'-m','tests.viewer_archive_rehearsal_server','--root',str(root),
                '--archive',str(archive),'--uuid',identifier],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True,start_new_session=os.name=='posix')
            try:
                ready=read_response(process);self.assertEqual(ready['uuid'],identifier)
                url='http://127.0.0.1:%d/task/%s/download/all.zip?token=%s' % (ready['port'],identifier,ready['token'])
                info=url.replace('/download/all.zip','/info')
                with urlopen(info,timeout=5) as response:self.assertEqual(json.load(response)['status']['code'],40)
                with urlopen(url,timeout=5) as response:self.assertEqual(response.read(),archive.read_bytes())
                process.stdin.write('{"command":"restart"}\n');process.stdin.flush()
                restarted=read_response(process);self.assertEqual(restarted,ready)
                with urlopen(info,timeout=5) as response:self.assertEqual(json.load(response)['status']['code'],40)
                with urlopen(url,timeout=5) as response:self.assertEqual(response.read(),archive.read_bytes())
                process.stdin.write('{"command":"stop"}\n');process.stdin.flush()
                self.assertEqual(read_response(process),{'ok':True});self.assertEqual(process.wait(timeout=10),0)
                self.assertEqual(archive.read_bytes(),synthetic_zip())
            finally:
                cleanup(process)
