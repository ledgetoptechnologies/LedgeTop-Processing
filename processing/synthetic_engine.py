"""Explicit opt-in HTTP engine double for transport tests, never photogrammetry.

No images are decoded or processed. Results are labelled synthetic transfer
fixtures and must never be treated as real Viewer ingestion/science validation.
"""

import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import threading
import time
import zipfile

from .maintenance import Conflict
from .viewer_http import ViewerFacade


class SyntheticEngine:
    def __init__(self, max_images=None, duration=1):
        self.max_images, self.duration = max_images, duration
        self.tasks = {}
        self.lock = threading.RLock()
        self.commits = 0
        self.maximum_running = 0

    def options(self):
        return [{'name': name, 'type': 'bool', 'value': False, 'help': 'Synthetic transfer fixture'}
                for name in ('pc-ept', 'gltf', '3d-tiles')]

    def capabilities(self):
        with self.lock:
            self._advance()
            return {'version': '2.4.0', 'engine': 'synthetic-odm', 'engineVersion': '0.0.0',
                    'maxImages': self.max_images, 'maxParallelTasks': 1,
                    'taskQueueCount': sum(t['code'] == 20 for t in self.tasks.values()),
                    'synthetic': True}

    def initialize(self, identifier, name='', options=None, outputs=None, environment='production'):
        with self.lock:
            if identifier in self.tasks:
                raise Conflict('synthetic engine init repeated')
            self.tasks[identifier] = {'name': name, 'files': {}, 'code': 10, 'end': 0}
            return {'uuid': identifier}

    def upload(self, identifier, filename, stream, length):
        with self.lock:
            task = self.tasks[identifier]
            if task['code'] != 10:
                raise Conflict('already committed')
            body = stream.read(length + 1)
            if len(body) != length:
                raise ValueError('truncated fixture')
            task['files'][filename] = {'bytes': length, 'sha256': hashlib.sha256(body).hexdigest()}
            return {'success': True}

    def upload_batch(self, identifier):
        # Keep commit from interleaving with the multipart request, just as the
        # real service's batch guard does. The lock is reentrant for upload().
        return self.lock

    def commit(self, identifier):
        with self.lock:
            self._advance()
            task = self.tasks[identifier]
            if task['code'] != 10:
                raise Conflict('commit repeated')
            if any(t['code'] == 20 for t in self.tasks.values()):
                raise Conflict('synthetic single slot busy')
            task.update(code=20, end=time.monotonic() + self.duration)
            self.commits += 1
            self.maximum_running = max(self.maximum_running, sum(t['code'] == 20 for t in self.tasks.values()))
            return {'success': True, 'uuid': identifier}

    def _advance(self):
        for task in self.tasks.values():
            if task['code'] == 20 and time.monotonic() >= task['end']:
                task['code'] = 40

    def info(self, identifier):
        with self.lock:
            self._advance()
            task = self.tasks[identifier]
            return {'uuid': identifier, 'status': {'code': task['code']},
                    'imagesCount': len(task['files']), 'progress': 100 if task['code'] == 40 else 0}

    def output(self, identifier, line=0):
        self.info(identifier)
        return ['Synthetic HTTP transfer only; no photogrammetry executed.', '', 'Fixture complete.'][line:]

    def download(self, identifier):
        if self.info(identifier)['status']['code'] != 40:
            raise Conflict('fixture unfinished')
        output = io.BytesIO()
        with zipfile.ZipFile(output, 'w') as archive:
            archive.writestr('SYNTHETIC-TRANSFER-ONLY.txt', 'Not a real ODM result or Viewer ingestion fixture.\n')
            archive.writestr('input-manifest.json', json.dumps(self.tasks[identifier]['files'], sort_keys=True))
        output.seek(0)
        return output, len(output.getvalue())

    def cancel(self, identifier):
        with self.lock:
            self.tasks[identifier]['code'] = 50
            return {'success': True}

    def remove(self, identifier):
        with self.lock:
            del self.tasks[identifier]
            return {'success': True}


def handler_for(engine, token, temp_dir=None):
    facade = ViewerFacade(engine, token, temp_dir=temp_dir)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            if not facade.handle(self):
                self.send_error(404)

        def do_POST(self):
            if not facade.handle(self):
                self.send_error(404)

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--max-images', type=int)
    parser.add_argument('--duration', type=float, default=1)
    parser.add_argument('--token-file', required=True)
    parser.add_argument('--spool', required=True)
    args = parser.parse_args()
    if os.environ.get('PROCESSING_SYNTHETIC_ENGINE') != '1':
        parser.error('explicit PROCESSING_SYNTHETIC_ENGINE=1 required')
    token = Path(args.token_file).read_text().strip()
    ThreadingHTTPServer((args.host, args.port), handler_for(
        SyntheticEngine(args.max_images, args.duration), token, args.spool)).serve_forever()


if __name__ == '__main__':
    main()
