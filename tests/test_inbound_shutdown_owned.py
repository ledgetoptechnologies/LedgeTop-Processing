"""Bounded child-process check of an owned synthetic HTTP coordinator."""
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from unittest.mock import patch
from http.server import ThreadingHTTPServer


def child():
    from processing.app import make_handler, CoordinatorService
    from processing.config import Config
    from processing.prototype import Queue
    from processing import viewer_http
    from tests.test_viewer_http import multipart, BOUNDARY
    with tempfile.TemporaryDirectory(prefix='ltds-inbound-stop-') as directory:
        config = Config(api_token='a'*32, local_node_url='http://cluster.invalid:19001',
            main_node_url='http://main.invalid:19002', local_max_photos=1000,
            local_max_input_bytes=20*1024**3, prototype_state_dir=str(Path(directory)/'state'),
            prototype_node_ids=('cluster','main'), treedar_node_ids=('main',),
            viewer_enabled=True, viewer_token='b'*32, per_node_slots=True,
            viewer_parallel_dispatch=True)
        handler=make_handler(config)
        handler.viewer_service.probe=lambda *_args: {'ready': True, 'maxImages': None, 'taskQueueCount': 0}
        identifier=str(uuid.uuid4())
        handler.viewer_service.store.initialize(identifier,'synthetic',[],None,'production')
        server=ThreadingHTTPServer(('127.0.0.1',0),handler)
        service=CoordinatorService(server,handler)
        stop=threading.Event(); entered=threading.Event(); exits=[]
        runner=threading.Thread(target=lambda:exits.append(service.run(stop)))
        original=viewer_http._Body.fill
        def observed(body):
            entered.set()
            return original(body)
        client=None
        try:
            with patch.object(viewer_http._Body,'fill',observed):
                runner.start()
                deadline=time.monotonic()+5
                while not service.healthy() and time.monotonic()<deadline: time.sleep(.01)
                if not service.healthy(): raise AssertionError('service not healthy')
                client=socket.create_connection(server.server_address,timeout=4)
                body=multipart([('images','owned.jpg',b'synthetic-photo')])
                headers=(f'POST /task/new/upload/{identifier}?token={config.viewer_token} HTTP/1.1\r\n'
                    f'Host: localhost\r\nContent-Type: multipart/form-data; boundary={BOUNDARY}\r\n'
                    f'Content-Length: {len(body)}\r\nConnection: close\r\n\r\n').encode()
                client.sendall(headers+body[:8])
                if not entered.wait(3): raise AssertionError('request did not reach body reader')
                stop.set()
                deadline=time.monotonic()+4
                while service.health()['state']!='draining' and time.monotonic()<deadline: time.sleep(.01)
                if not runner.is_alive(): raise AssertionError('shutdown returned with active upload')
                unexpected=None
                try:
                    try: unexpected=Queue(config.prototype_state_dir,node_ids=('cluster','main'))
                    except RuntimeError: pass
                    else: raise AssertionError('queue authority released early')
                finally:
                    if unexpected is not None: unexpected.close()
                client.shutdown(socket.SHUT_WR)
                client.close(); client=None
                runner.join(5)
                if runner.is_alive() or exits!=[0]: raise AssertionError('shutdown did not complete')
            replacement=Queue(config.prototype_state_dir,node_ids=('cluster','main'),viewer_enabled=True,
                              per_node_slots=True)
            try:
                from processing.viewer_tasks import TaskStore
                if TaskStore(replacement,Path(config.prototype_state_dir)/'viewer-tasks').task(identifier)['files']:
                    raise AssertionError('incomplete upload persisted')
            finally: replacement.close()
        finally:
            if client is not None: client.close()
            stop.set()
            if runner.ident is not None: runner.join(5)
            if runner.is_alive(): os._exit(91)
            server.server_close()
            handler.viewer_service.close()
            handler.prototype_queue.close()


class OwnedShutdownTests(unittest.TestCase):
    def test_stalled_inbound_shutdown_retains_authority_until_disconnect(self):
        result=subprocess.run([sys.executable,'-B','-m','tests.test_inbound_shutdown_owned','--child'],
                              capture_output=True,text=True,timeout=20)
        self.assertEqual(result.returncode,0,result.stderr)


if __name__ == '__main__':
    if sys.argv[1:]==['--child']: child()
    else: unittest.main()
