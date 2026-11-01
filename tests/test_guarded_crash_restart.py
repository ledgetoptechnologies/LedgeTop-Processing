"""One self-killing, bounded synthetic coordinator; no live nodes or cleanup."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from processing.prototype import Queue
from processing.viewer_service import ViewerService
from processing.viewer_tasks import TaskStore


CHILD = r'''
import io,json,os,signal,sys,threading,uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from processing import engine_upload_contract,guarded_native_engine
from processing.engine_ingress_guard import BackendPin,Binding,EngineIngressGuard,OwnerPin
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.transfer_fence import install_offline
from processing.viewer_service import ViewerService
from processing.viewer_tasks import TaskStore
root=Path(sys.argv[1]); paths=patch('processing.node_gate.canonical_gate_path',side_effect=lambda engine:root/('gate-'+engine));paths.start()
engine_upload_contract.QUALIFIED=True;guarded_native_engine.QUALIFIED=True
class Handler(BaseHTTPRequestHandler):
 def do_POST(self):
  self.rfile.read(int(self.headers['Content-Length']))
  if '/upload/' in self.path:os.kill(os.getpid(),signal.SIGKILL)
  data=json.dumps({'uuid':self.headers['Set-UUID']}).encode()
  self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
 def log_message(self,*_):pass
server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
threading.Thread(target=server.serve_forever,daemon=True).start()
config=SimpleNamespace(prototype_state_dir=str(root/'state'),viewer_max_input_bytes=20*1024**3,
 viewer_max_archive_bytes=1024*1024,local_node_url='http://synthetic.invalid',local_node_token='',
 main_node_url='http://synthetic.invalid',main_node_token='',local_max_photos=1000,local_max_input_bytes=20*1024**3)
queue=Queue(root/'state',node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
store=TaskStore(queue,root/'state'/'viewer-tasks');pin=OwnerPin.capture(queue);guards={}
with queue._db() as db:install_offline(db,enabled=True)
for node in ('cluster','main'):
 binding=Binding('engine-'+node,node,queue.authority_id,'http://127.0.0.1:18080',1,server.server_port,
  'coordinator-'+'a'*40,'management-'+'b'*40,'backend-'+'c'*40,deadline_seconds=5)
 with NodeGate(binding.engine_id) as gate:gate.provision(node,binding.coordinator_origin,queue.authority_id)
 guards[node]=EngineIngressGuard(queue,binding,owner_pin=pin,
  backend_pin=BackendPin('a'*64,'sha256:'+'b'*64,binding.backend_port),
  backend_probe=lambda *_:None,engine_probe=lambda _,eid=binding.engine_id:eid)
service=ViewerService(queue,config,store=store,guarded_engines=guards,
 probe=lambda *args:{'ready':True,'maxImages':None,'taskQueueCount':0})
service._catalog=lambda:{'ready':True,'commonOptions':[]}
service._selected_options=lambda _node:[]
identifier=str(uuid.uuid4());service.initialize(identifier,'Synthetic',[],None,'production')
service.upload(identifier,'photo.jpg',io.BytesIO(b'synthetic-photo'),15);service.commit(identifier)
marker=root/'task.json';marker.write_text(json.dumps({'identifier':identifier}));
with marker.open('rb') as file:os.fsync(file.fileno())
service.tick()
raise SystemExit('synthetic coordinator unexpectedly survived upload')
'''


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux self-SIGKILL fixture required')
class GuardedCrashRestartTests(unittest.TestCase):
    def test_crash_during_guarded_upload_holds_started_and_never_replays(self):
        root = Path(tempfile.mkdtemp(prefix='lt-guarded-crash-')).resolve()
        print('retained synthetic crash root:', root, flush=True)
        result = subprocess.run([sys.executable, '-B', '-c', CHILD, str(root)],
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=8, check=False)
        self.assertEqual(result.returncode, -signal.SIGKILL, result.stderr.decode(errors='replace'))
        identifier = json.loads((root/'task.json').read_text())['identifier']
        queue = Queue(root/'state', node_ids=('cluster','main'), treedar_node_ids=('main',), viewer_enabled=True)
        try:
            store = TaskStore(queue, root/'state'/'viewer-tasks')
            task = store.task(identifier)
            self.assertEqual((task['job']['status'],task['job']['heldReason'],task['phase']),
                             ('held','coordinator_restart','upload_pending'))
            with queue._db() as db:
                self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0], 'started')
                self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0], 'started')
            engine = Mock(); probe = Mock(side_effect=AssertionError('unexpected probe'))
            config = SimpleNamespace(prototype_state_dir=str(root/'state'),viewer_max_input_bytes=20*1024**3,
                viewer_max_archive_bytes=1024*1024,local_node_url='http://synthetic.invalid',local_node_token='',
                main_node_url='http://synthetic.invalid',main_node_token='',local_max_photos=1000,local_max_input_bytes=20*1024**3)
            service = ViewerService(queue, config, store=store, engines={'cluster':engine,'main':engine}, probe=probe)
            service.tick()
            self.assertEqual(engine.mock_calls, []); probe.assert_not_called()
            self.assertEqual(store.task(identifier)['job']['status'], 'held')
            with queue._db() as db:
                self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0], 'started')
        finally:
            queue.close()


if __name__ == '__main__': unittest.main()
