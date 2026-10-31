"""Bounded subprocess test for the disposable cross-language fixture."""
import json
import hashlib
import queue
import signal
import sqlite3
import stat
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import uuid

from tests.test_viewer_service import synthetic_zip
from tests.test_viewer_http import BOUNDARY, multipart
from tests.viewer_archive_rehearsal_server import Rehearsal


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
    @unittest.skipUnless(os.name=='posix','dirfd and no-follow required')
    def test_input_evidence_rejects_symlink_and_replacement(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);task=root/'viewer-tasks'/'task';task.mkdir(parents=True)
            identifier=str(uuid.uuid4());source=task/'photo.bin';source.write_bytes(b'synthetic')
            with sqlite3.connect(root/'jobs.sqlite3') as db:
                db.execute('CREATE TABLE viewer_tasks(uuid TEXT,directory TEXT)')
                db.execute('CREATE TABLE viewer_files(uuid TEXT,name TEXT,path TEXT,bytes INTEGER,sha256 TEXT)')
                db.execute('INSERT INTO viewer_tasks VALUES (?,?)',(identifier,'task'))
                db.execute('INSERT INTO viewer_files VALUES (?,?,?,?,?)',(identifier,'photo.jpg','photo.bin',9,hashlib.sha256(b'synthetic').hexdigest()))
            fixture=Rehearsal(str(root),'unused',identifier)
            source.unlink();source.symlink_to('other')
            with self.assertRaises((OSError,ValueError)):fixture.input_evidence(identifier,'photo.jpg')
            source.unlink();os.mkfifo(source)
            previous=signal.signal(signal.SIGALRM,lambda *_:(_ for _ in ()).throw(TimeoutError('FIFO read blocked')))
            try:
                signal.alarm(3)
                with self.assertRaises(ValueError):fixture.input_evidence(identifier,'photo.jpg')
            finally:
                signal.alarm(0);signal.signal(signal.SIGALRM,previous)
            source.unlink();task.rename(task.with_name('other'));task.symlink_to('other',target_is_directory=True)
            with self.assertRaises((OSError,ValueError)):fixture.input_evidence(identifier,'photo.jpg')
            task.unlink();task.with_name('other').rename(task)
            source.write_bytes(b'synthetic');replacement=task/'replacement';replacement.write_bytes(b'synthetic')
            original=os.fstat;seen=0
            def replace_on_second_file_stat(fd):
                nonlocal seen
                value=original(fd)
                if stat.S_ISREG(value.st_mode):
                    seen+=1
                    if seen==2:os.replace(replacement,source)
                return value
            with patch('tests.viewer_archive_rehearsal_server.os.fstat',side_effect=replace_on_second_file_stat):
                with self.assertRaises(ValueError):fixture.input_evidence(identifier,'photo.jpg')

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
