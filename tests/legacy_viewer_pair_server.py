"""Synthetic loopback coordinator owned by the paired test; never production."""
import json
import socket
import sys
import threading
import time
import io
import zipfile
import hashlib
import struct
import zlib
from http.server import ThreadingHTTPServer
from pathlib import Path

from processing.app import CoordinatorService, make_handler
from processing.config import Config
from tests.test_viewer_service import FakeEngine, FakeFleet, OPTIONS


def synthetic_assets():
    # An invented textured square, with matching OBJ and embedded-texture GLB.
    def chunk(kind, value):
        return struct.pack('>I',len(value))+kind+value+struct.pack('>I',zlib.crc32(kind+value)&0xffffffff)
    png = b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>IIBBBBB',16,16,8,2,0,0,0))+chunk(b'IDAT',zlib.compress((b'\x00'+bytes([220,130,60])*16)*16))+chunk(b'IEND',b'')
    points = [(0,0,0),(1,0,0),(1,1,0),(0,0,0),(1,1,0),(0,1,0)]
    position = b''.join(struct.pack('<fff',*p) for p in points)
    normal = b''.join(struct.pack('<fff',0,0,1) for _ in points)
    uv = b''.join(struct.pack('<ff',p[0],p[1]) for p in points)
    parts=[];views=[]
    for value in (position,normal,uv,png):
        offset=sum(map(len,parts));views.append({'buffer':0,'byteOffset':offset,'byteLength':len(value)})
        parts.append(value+b'\0'*((-len(value))%4))
    blob=b''.join(parts)
    desc={'asset':{'version':'2.0'},'scene':0,'scenes':[{'nodes':[0]}],'nodes':[{'mesh':0}],
          'meshes':[{'primitives':[{'attributes':{'POSITION':0,'NORMAL':1,'TEXCOORD_0':2},'material':0}]}],
          'materials':[{'pbrMetallicRoughness':{'baseColorTexture':{'index':0}}}],
          'textures':[{'source':0}],'images':[{'bufferView':3,'mimeType':'image/png'}],
          'accessors':[{'bufferView':i,'componentType':5126,'count':6,'type':t} for i,t in enumerate(('VEC3','VEC3','VEC2'))],
          'bufferViews':views,'buffers':[{'byteLength':len(blob)}]}
    header=json.dumps(desc,separators=(',',':')).encode();header+=b' '*((-len(header))%4)
    glb=struct.pack('<III',0x46546c67,2,12+8+len(header)+8+len(blob))+struct.pack('<II',len(header),0x4e4f534a)+header+struct.pack('<II',len(blob),0x004e4942)+blob
    obj=b'mtllib model.mtl\nv 0 0 0\nv 1 0 0\nv 1 1 0\nv 0 1 0\nvt 0 0\nvt 1 0\nvt 1 1\nvt 0 1\nvn 0 0 1\nusemtl surface\nf 1/1/1 2/2/1 3/3/1\nf 1/1/1 3/3/1 4/4/1\n'
    return {'odm_texturing/odm_textured_model_geo.glb':glb,
            'odm_texturing/odm_textured_model_geo.obj':obj,
            'odm_texturing/model.mtl':b'newmtl surface\nmap_Kd model.png\n',
            'odm_texturing/model.png':png,
            'entwine_pointcloud/ept.json':b'{}',
            '3d_tiles/model/tileset.json':b'{}'}


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
            for name, value in synthetic_assets().items():
                z.writestr(name, value)
        for engine in handler.viewer_service.engines.values():
            engine.archive = archive.getvalue()
            engine.interrupt_download_once = sys.argv[2] == 'result-retry'
    handler.viewer_service.probe = lambda role, origin, token: {'ready': True, 'maxImages': 1000, 'taskQueueCount': 0}
    handler.viewer_service._catalog = lambda: {'ready': True, 'commonOptions': [dict(x) for x in OPTIONS]}
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
