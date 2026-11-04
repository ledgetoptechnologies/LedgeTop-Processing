"""Synthetic loopback coordinator owned by the paired test; never production."""
import json
import socket
import sys
import threading
import time
import io
import zipfile
import hashlib
from http.server import ThreadingHTTPServer
from pathlib import Path

from processing.app import CoordinatorService, make_handler
from processing.config import Config
from tests.test_viewer_service import FakeEngine, FakeFleet, OPTIONS


def main():
    root = Path(sys.argv[1])
    root.mkdir(mode=0o700)  # Must not already exist.
    config = Config(api_token='synthetic-api-' + 'a' * 32,
                    local_node_url='http://192.0.2.1:1', main_node_url='http://192.0.2.2:2',
                    local_max_photos=1000, local_max_input_bytes=1000000,
                    prototype_state_dir=str(root), prototype_node_ids=('cluster', 'main'),
                    treedar_node_ids=('main',), viewer_enabled=True, per_node_slots=True, viewer_parallel_dispatch=True,
                    viewer_token='synthetic-viewer-' + 'b' * 32)
    handler = make_handler(config)
    fleet = FakeFleet()
    handler.viewer_service.engines = {name: FakeEngine(name, fleet) for name in ('cluster', 'main')}
    if sys.argv[2] in ('result', 'result-retry'):
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, 'w') as z:
            for name, value in {
                'odm_texturing/odm_textured_model_geo.glb': 'glb',
                'odm_texturing/odm_textured_model_geo.obj': 'mtllib materials/model.mtl\nv 0 0 0\n',
                'odm_texturing/materials/model.mtl': 'newmtl surface\nmap_Kd ../textures/model.jpg\n',
                'odm_texturing/textures/model.jpg': 'texture',
                'entwine_pointcloud/ept.json': '{}',
                '3d_tiles/model/tileset.json': '{}',
            }.items():
                z.writestr(name, value)
        for engine in handler.viewer_service.engines.values():
            engine.archive = archive.getvalue()
            engine.interrupt_download_once = sys.argv[2] == 'result-retry'
    handler.viewer_service.probe = lambda role, origin, token: {'ready': True, 'maxImages': 1000, 'taskQueueCount': 0}
    handler.viewer_service.options = lambda: [dict(x) for x in OPTIONS]
    handler.viewer_service._selected_options = lambda _: [dict(x) for x in OPTIONS]
    mode = sys.argv[2]
    release_old, old_done = threading.Event(), threading.Event()
    if mode in ('partial', 'overlap'):
        # Test-only fault: discard the connection immediately after one file
        # was stored. This deliberately prevents later multipart processing.
        current = threading.local()
        first = threading.Event()
        upload = handler.viewer_service.upload
        class InterruptHandler(handler):
            def do_POST(self):
                current.connection = self.connection
                current.old = False
                try:
                    return super().do_POST()
                finally:
                    if current.old:
                        old_done.set()
                    del current.connection
        def interrupt_after_first(*args, **kwargs):
            result = upload(*args, **kwargs)
            if not first.is_set():
                first.set()
                if mode == 'partial':
                    current.connection.shutdown(socket.SHUT_RDWR)
                    raise ConnectionResetError('synthetic partial upload disconnect')
                current.old = True
                if not release_old.wait(12):
                    raise RuntimeError('old upload was not released')
            return result
        handler.viewer_service.upload = interrupt_after_first
        handler = InterruptHandler
    server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
    stop = threading.Event()
    service = CoordinatorService(server, handler)
    runner = threading.Thread(target=service.run, args=(stop,))
    runner.start()
    try:
        deadline = time.monotonic() + 8
        while not service.healthy():
            if not runner.is_alive() or time.monotonic() >= deadline:
                raise RuntimeError('coordinator did not become healthy')
            time.sleep(0.01)
        print(json.dumps({'port': server.server_port}), flush=True)
        for line in sys.stdin:
            request = json.loads(line)
            if request['op'] == 'inspect':
                task = handler.viewer_service.store.task(request['uuid'])
                partial = Path(task['directory']) / 'all.zip.partial'
                intent = Path(task['directory']) / 'all.zip.intent'
                print(json.dumps({'files': [{'name': f['name'], 'sha256': f['sha256']} for f in task['files']],
                                  'phase': task['phase'], 'stage': task['stage'],
                                  'archive_sha256': hashlib.sha256(Path(task['archive_path']).read_bytes()).hexdigest() if task['archive_path'] else None,
                                  'partial_bytes': partial.stat().st_size if partial.is_file() else None,
                                  'intent_exists': intent.is_file(),
                                  'old_active': mode == 'overlap' and first.is_set() and not old_done.is_set()}), flush=True)
            elif request['op'] == 'finish':
                task = handler.viewer_service.store.task(request['uuid'])
                if task['phase'] != 'processing' or not task['engine_uuid']:
                    raise RuntimeError('fake engine has not started')
                handler.viewer_service.engines[task['node_id']].finish(task['engine_uuid'], 40)
                print(json.dumps({'finished': True}), flush=True)
            elif request['op'] == 'release':
                release_old.set()
                if not old_done.wait(12):
                    raise RuntimeError('old upload did not finish')
                print(json.dumps({'old_done': True}), flush=True)
            elif request['op'] == 'stop':
                break
            else:
                raise ValueError('unsupported command')
    finally:
        release_old.set()
        stop.set()
        runner.join(15)
        if runner.is_alive():
            raise RuntimeError('coordinator still active; retain test state')
        server.server_close()


if __name__ == '__main__':
    main()
