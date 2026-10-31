"""Disposable loopback coordinator for cross-language Viewer archive tests.

Accepts JSON lines on stdin and writes one JSON response per line. The caller
owns the temporary work directory and supplied synthetic ZIP. No live engines
or NAS are accessed. This is a test fixture, not a deployment entry point.
"""
import argparse
import hashlib
import json
import multiprocessing
from pathlib import Path
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
                elif command=='archive_path':result={'path':instance.command('path')}
                elif command in ('tick','finish'):
                    identifier=request.get('uuid',instance.identifier);uuid.UUID(identifier)
                    result={'result':instance.command(command,identifier)}
                elif command=='stop':
                    instance.stop();print(json.dumps({'ok':True}),flush=True);break
                else:raise ValueError('unknown command')
                print(json.dumps(result),flush=True)
            except Exception as exc:
                print(json.dumps({'error':type(exc).__name__}),flush=True)
    finally:instance.stop()


if __name__=='__main__':main()
