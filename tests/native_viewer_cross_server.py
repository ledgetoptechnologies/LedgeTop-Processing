"""Network-none synthetic Viewer/native-coordinator rehearsal; fixture control only."""
import io,json,os,subprocess,tempfile,threading,time,zipfile
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from processing.app import CoordinatorService,make_handler
from processing.config import Config
from processing.synthetic_engine import SyntheticEngine,handler_for

def main():
    viewer=os.environ['VIEWER_SOURCE'];root=tempfile.TemporaryDirectory(prefix='native-viewer-fixture-')
    engines=[];servers=[];threads=[];running={};guard=threading.RLock();overlap=[];removals=[]
    archive=io.BytesIO()
    with zipfile.ZipFile(archive,'w') as z:
        for name,content in [('odm_texturing/odm_textured_model_geo.glb','glb'),('odm_texturing/odm_textured_model_geo.obj','mtllib model.mtl\nv 0 0 0\n'),('odm_texturing/model.mtl','newmtl surface\nmap_Kd texture.jpg\n'),('odm_texturing/texture.jpg','texture'),('entwine_pointcloud/ept.json','{}'),('3d_tiles/model/tileset.json','{}')]:z.writestr(name,content)
    data=archive.getvalue()
    class Engine(SyntheticEngine):
        def commit(self,identifier):
            with guard:
                if any(t['code']==20 for e in engines for t in e.tasks.values()):overlap.append(identifier);raise RuntimeError('overlap')
                return super().commit(identifier)
        def download(self,identifier):return io.BytesIO(data),len(data)
        def remove(self,identifier):removals.append(identifier);raise RuntimeError('fixture forbids engine deletion')
    try:
        # Fail before creating coordinator or engines if the pinned source or
        # fixture image cannot load the real Viewer worker dependencies.
        preflight="const b=process.env.VIEWER_SOURCE+'/apps/viewer/server';for(const [f,n] of [['database','openDatabase'],['repository','ViewerRepository'],['processingRepository','ProcessingRepository'],['storageManager','StorageManager'],['processingWorker','processOne'],['providerAuth','NO_AUTH_MARKER']]){if(require(b+'/'+f)[n]===undefined)throw Error('missing Viewer export '+f+':'+n)}"
        subprocess.run(['node','-e',preflight],env={**os.environ,'VIEWER_SOURCE':viewer,'NODE_PATH':'/src/apps/viewer/node_modules'},check=True,timeout=15)
        subprocess.run(['node','--input-type=module','-e',"await import(process.env.VIEWER_SOURCE+'/apps/viewer/server/explicitSourceUnitInspection.mjs')"],env={**os.environ,'VIEWER_SOURCE':viewer},check=True,timeout=15)
        for engine in (Engine(1000,3600),Engine(None,3600)):
            engines.append(engine);server=ThreadingHTTPServer(('127.0.0.1',0),handler_for(engine,'synthetic-node-token-nnnnnnnn'))
            thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02});servers.append(server);thread.start();threads.append(thread)
        cfg=Config(api_token='synthetic-api-token-aaaaaaaa',local_node_url='http://127.0.0.1:'+str(servers[0].server_port),main_node_url='http://127.0.0.1:'+str(servers[1].server_port),local_max_photos=1000,local_max_input_bytes=20*1024**3,prototype_state_dir=str(Path(root.name)/'coordinator'),prototype_node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True,viewer_token='synthetic-production-token-pppppppp',viewer_staging_token='synthetic-staging-token-ssssssss',local_node_token='synthetic-node-token-nnnnnnnn',main_node_token='synthetic-node-token-nnnnnnnn')
        def start(port=0):
            handler=make_handler(cfg);server=ThreadingHTTPServer(('127.0.0.1',port),handler);stop=threading.Event();service=CoordinatorService(server,handler);exits=[]
            thread=threading.Thread(target=lambda:exits.append(service.run(stop)));thread.start();running.update(handler=handler,server=server,thread=thread,stop=stop,exits=exits)
            until=time.monotonic()+10
            while not service.healthy():
                if time.monotonic()>until or not thread.is_alive():raise RuntimeError('coordinator startup failed')
                time.sleep(.05)
            return server.server_port
        def stop():
            running['stop'].set();running['thread'].join(15)
            if running['thread'].is_alive() or running['exits']!=[0]:raise RuntimeError('coordinator shutdown uncertain')
        port=start()
        def task(identifier):return running['handler'].viewer_service.store.task(identifier)
        def wait(predicate):
            end=time.monotonic()+30
            while time.monotonic()<end:
                if predicate():return
                time.sleep(.05)
            raise RuntimeError('fixture state timeout')
        class Control(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_GET(self):
                try:
                    parts=self.path.strip('/').split('/');op=parts[0]
                    if op=='wait-running':wait(lambda:task(parts[1])['phase']=='processing')
                    elif op=='assert-queued':
                        assert task(parts[1])['job']['status']=='running';assert all(task(x)['job']['status']=='queued' for x in parts[2:]);assert not overlap
                    elif op=='coordinator-stop':stop()
                    elif op=='coordinator-start':start(port)
                    elif op=='finish':
                        t=task(parts[1]);engine=engines[0 if t['node_id']=='cluster' else 1]
                        with guard,engine.lock:engine.tasks[t['engine_uuid']]['code']=40
                        wait(lambda:task(parts[1])['job']['status']=='succeeded')
                    elif op=='expect-next':
                        wait(lambda:task(parts[1])['phase']=='processing')
                        if parts[2]!='none':assert task(parts[2])['job']['status']=='queued'
                        assert not overlap
                    elif op=='assert-retained':
                        assert not removals and not overlap and sum(e.commits for e in engines)==3
                        for identifier in parts[1:]:
                            t=task(identifier);engine=engines[0 if t['node_id']=='cluster' else 1]
                            assert t['job']['status']=='succeeded' and engine.tasks[t['engine_uuid']]['code']==40
                            assert Path(t['archive_path']).read_bytes()==data
                    else:raise RuntimeError('unknown fixture operation')
                    self.send_response(200);self.send_header('Content-Length','2');self.end_headers();self.wfile.write(b'{}')
                except Exception as exc:
                    body=json.dumps({'fixtureError':type(exc).__name__}).encode();self.send_response(500);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
        control=ThreadingHTTPServer(('127.0.0.1',0),Control);control_thread=threading.Thread(target=control.serve_forever,kwargs={'poll_interval':.02});control_thread.start()
        try:
            env={**os.environ,'VIEWER_SOURCE':viewer,'FIXTURE_ROOT':root.name,'COORDINATOR_PORT':str(port),'CONTROL_PORT':str(control.server_port),'NODE_PATH':'/src/apps/viewer/node_modules'}
            subprocess.run(['node','tests/native_viewer_cross_client.cjs'],env=env,check=True,timeout=130)
        finally:control.shutdown();control.server_close();control_thread.join(10)
        if running['thread'].is_alive():stop()
    finally:
        if running.get('thread') and running['thread'].is_alive():running['stop'].set();running['thread'].join(15)
        for server in servers:server.shutdown();server.server_close()
        for thread in threads:thread.join(10)
        if any(t.is_alive() for t in threads) or (running.get('thread') and running['thread'].is_alive()):os._exit(2)
        root.cleanup()
if __name__=='__main__':main()
