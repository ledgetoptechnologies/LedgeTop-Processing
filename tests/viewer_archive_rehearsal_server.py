"""Disposable loopback coordinator for cross-language Viewer archive tests.

Accepts JSON lines on stdin and writes one JSON response per line. The caller
owns the temporary work directory and supplied synthetic ZIP. No live engines
or NAS are accessed. This is a test fixture, not a deployment entry point.
"""
import argparse
from contextlib import closing
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import stat
import sys
from urllib.request import Request, urlopen
import uuid

from tests.test_viewer_http import BOUNDARY, TOKEN, multipart


def child(root, pipe, zip_path, mode, port):
    import tests.test_viewer_archive_http_recovery as fixture
    original = fixture.FakeEngine

    class FixtureEngine(original):
        def __init__(self, *args):
            super().__init__(*args)
            self.archive = Path(zip_path).read_bytes()
            self.interrupt_download_once = mode == 'interrupt'

    fixture.FakeEngine = FixtureEngine
    original_server = fixture.ThreadingHTTPServer
    fixture.ThreadingHTTPServer = lambda address, handler: original_server((address[0], port), handler)
    fixture.synthetic_coordinator(root, pipe, mode)


class Rehearsal:
    def __init__(self, root, archive, identifier):
        self.root, self.archive, self.identifier = root, archive, identifier
        self.process = self.pipe = self.port = None

    def start(self, mode='normal'):
        context = multiprocessing.get_context('spawn')
        parent, remote = context.Pipe()
        process = context.Process(target=child, args=(self.root, remote, self.archive, mode, self.port or 0))
        self.process, self.pipe = process, parent
        try:
            process.start(); remote.close()
            if not parent.poll(10): raise RuntimeError('fixture coordinator failed to start')
            _, self.port, returned = parent.recv()
            if returned != Path(self.archive).read_bytes(): raise RuntimeError('fixture archive changed')
        except BaseException:
            remote.close(); self.stop(); raise

    def command(self, name, identifier=None):
        self.pipe.send((name, self.identifier if identifier is None else identifier))
        if not self.pipe.poll(10): raise RuntimeError('fixture command timed out')
        return self.pipe.recv()

    def input_evidence(self, identifier, filename):
        """Read one synthetic stored input without disclosing a path or bytes."""
        uuid.UUID(identifier)
        if type(filename) is not str or not filename or len(filename)>255 or Path(filename).name!=filename:
            raise ValueError('bounded filename required')
        db_path=Path(self.root)/'jobs.sqlite3'
        with closing(sqlite3.connect(db_path.as_uri()+'?mode=ro',uri=True)) as db:
            rows=db.execute('SELECT t.directory,f.path,f.bytes,f.sha256 FROM viewer_tasks t JOIN viewer_files f ON f.uuid=t.uuid WHERE t.uuid=? AND f.name=?',(identifier,filename)).fetchall()
        if len(rows)!=1:raise ValueError('one matching fixture input required')
        directory,relative,size,expected=rows[0]
        if (type(directory) is not str or not directory or Path(directory).name!=directory or directory in ('.','..')
                or type(relative) is not str or not relative or Path(relative).name!=relative or relative in ('.','..')
                or type(size) is not int or not 0<=size<=16*1024*1024):
            raise ValueError('bounded fixture input required')
        flags=os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC
        root_fd=spool_fd=task_fd=descriptor=None
        try:
            root_fd=os.open(self.root,flags)
            spool_fd=os.open('viewer-tasks',flags,dir_fd=root_fd)
            task_fd=os.open(directory,flags,dir_fd=spool_fd)
            descriptor=os.open(relative,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK,dir_fd=task_fd)
            before=os.fstat(descriptor)
            named=os.stat(relative,dir_fd=task_fd,follow_symlinks=False)
            if (not stat.S_ISREG(before.st_mode) or before.st_size!=size
                    or (before.st_dev,before.st_ino)!=(named.st_dev,named.st_ino)):
                raise ValueError('fixture input changed')
            digest=hashlib.sha256()
            with os.fdopen(descriptor,'rb',closefd=False) as stream:
                remaining=size
                while remaining:
                    chunk=stream.read(min(remaining,1024*1024))
                    if not chunk:raise ValueError('fixture input truncated')
                    digest.update(chunk);remaining-=len(chunk)
                if stream.read(1):raise ValueError('fixture input grew')
            after=os.fstat(descriptor)
            named_after=os.stat(relative,dir_fd=task_fd,follow_symlinks=False)
            task_after=os.stat(directory,dir_fd=spool_fd,follow_symlinks=False)
            opened_task=os.fstat(task_fd)
            spool_after=os.stat('viewer-tasks',dir_fd=root_fd,follow_symlinks=False)
            opened_spool=os.fstat(spool_fd)
            root_after=os.lstat(self.root);opened_root=os.fstat(root_fd)
            if ((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)
                    or (after.st_dev,after.st_ino)!=(named_after.st_dev,named_after.st_ino)
                    or (opened_task.st_dev,opened_task.st_ino)!=(task_after.st_dev,task_after.st_ino)
                    or (opened_spool.st_dev,opened_spool.st_ino)!=(spool_after.st_dev,spool_after.st_ino)
                    or (opened_root.st_dev,opened_root.st_ino)!=(root_after.st_dev,root_after.st_ino)
                    or digest.hexdigest()!=expected):
                raise ValueError('fixture input changed')
            return {'bytes':size,'sha256':expected}
        finally:
            for fd in (descriptor,task_fd,spool_fd,root_fd):
                if fd is not None:os.close(fd)

    def request(self, route, body=None, headers=None):
        url = 'http://127.0.0.1:%d%s?token=%s' % (self.port, route, TOKEN)
        with urlopen(Request(url, data=body, headers=headers or {}), timeout=10) as response:
            return json.load(response)

    def prepare(self):
        headers = {'Content-Type': 'multipart/form-data; boundary=' + BOUNDARY}
        self.request('/task/new/init', multipart([('name', None, b'Synthetic survey'), ('options', None, b'[]')]),
                     dict(headers, **{'set-uuid': self.identifier}))
        self.request('/task/new/upload/' + self.identifier, multipart([('images', 'photo.jpg', b'synthetic')]), headers)
        self.request('/task/new/commit/' + self.identifier, b'')
        self.command('tick'); self.command('finish')

    def stop(self):
        if self.process is None: return
        if self.process.pid is not None and self.process.is_alive():
            try: self.pipe.send(('stop', None))
            except (BrokenPipeError,EOFError,OSError): pass
            self.process.join(5)
        if self.process.pid is not None and self.process.is_alive():
            self.process.terminate(); self.process.join(5)
        if self.process.pid is not None and self.process.is_alive():
            self.process.kill(); self.process.join(5)
        if self.process.pid is not None and self.process.is_alive():
            raise RuntimeError('fixture coordinator could not be reaped')
        if self.process.pid is not None:self.process.join(0)
        self.pipe.close(); self.process = self.pipe = None

    def state(self):
        return dict(port=self.port, token=TOKEN, uuid=self.identifier,
                    # uuid is a fallback for commands, not proof a task exists.
                    archiveSha256=hashlib.sha256(Path(self.archive).read_bytes()).hexdigest(),
                    archiveBytes=Path(self.archive).stat().st_size)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--root',required=True);parser.add_argument('--archive',required=True)
    parser.add_argument('--uuid',required=True);parser.add_argument('--interrupt-first',action='store_true')
    parser.add_argument('--unprepared',action='store_true');args=parser.parse_args()
    uuid.UUID(args.uuid)
    root=Path(args.root).resolve();archive=Path(args.archive).resolve()
    if not root.is_dir() or any(root.iterdir()) or not archive.is_file(): raise ValueError('empty fixture root and synthetic archive required')
    instance=Rehearsal(str(root),str(archive),args.uuid)
    try:
        instance.start('interrupt' if args.interrupt_first else 'normal')
        if not args.unprepared:instance.prepare()
        print(json.dumps(instance.state()),flush=True)
        for line in sys.stdin:
            try:
                request=json.loads(line);command=request['command']
                if command=='restart':
                    instance.stop();instance.start('normal');result=instance.state()
                elif command in ('tick','finish','archive_path'):
                    identifier=request.get('uuid',instance.identifier);uuid.UUID(identifier)
                    if command=='archive_path':result={'path':instance.command('path',identifier)}
                    else:result={'result':instance.command(command,identifier)}
                elif command=='input_evidence':
                    result=instance.input_evidence(request.get('uuid',instance.identifier),request.get('filename'))
                elif command=='stop':
                    instance.stop();print(json.dumps({'ok':True}),flush=True);break
                else:raise ValueError('unknown command')
                print(json.dumps(result),flush=True)
            except Exception as exc:
                print(json.dumps({'error':type(exc).__name__}),flush=True)
    finally:instance.stop()


if __name__=='__main__':main()
