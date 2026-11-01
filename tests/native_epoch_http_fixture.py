"""Bounded synthetic-only loopback listener for cross-language tests."""
import json
import os
from pathlib import Path
import select
import shutil
import sys
import tempfile
import threading
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from processing import config
from processing.app import make_handler
from processing.config import Config
from processing.prototype import Queue
from tests.test_native_manifest_binding import TOKEN,STAGING


class FixtureServer(ThreadingHTTPServer):
    """Track accepted handlers before starting their worker threads."""
    def __init__(self,*args,**kwargs):
        self._condition=threading.Condition();self._active=0
        super().__init__(*args,**kwargs)

    def process_request(self,request,client_address):
        with self._condition:self._active+=1
        try:super().process_request(request,client_address)
        except BaseException:
            with self._condition:self._active-=1;self._condition.notify_all()
            raise

    def process_request_thread(self,request,client_address):
        try:super().process_request_thread(request,client_address)
        finally:
            with self._condition:self._active-=1;self._condition.notify_all()

    def wait_idle(self,timeout):
        with self._condition:return self._condition.wait_for(lambda:self._active==0,timeout)


def main():
    if os.name!='posix' or os.environ.get('PROCESSING_SYNTHETIC_EPOCH_HTTP_FIXTURE')!='1':
        raise RuntimeError('explicit POSIX synthetic fixture opt-in required')
    root=Path(tempfile.mkdtemp(prefix='native-epoch-http-'))
    lease=root/'leases';lease.mkdir();state=root/'state'
    handler=None;server=None;thread=None;started=False;clean=False;listener_safe=True
    try:
        initial=Queue(state,node_ids=('cluster',),viewer_enabled=True);initial.close()
        settings=Config('a'*32,'http://127.0.0.1:1','http://127.0.0.1:2',1000,20*1024**3,
            prototype_state_dir=str(state),prototype_node_ids=('cluster',),treedar_node_ids=('cluster',),
            viewer_enabled=True,viewer_token=TOKEN,viewer_staging_token=STAGING,authority_lease_dir=str(lease),
            viewer_manifest_binding=True,viewer_plan_binding=True,viewer_plan_epoch=True)
        with patch.object(config,'NATIVE_MANIFEST_BINDING_QUALIFIED',True), \
             patch.object(config,'NATIVE_PLAN_BINDING_QUALIFIED',True), \
             patch.object(config,'NATIVE_PLAN_EPOCH_QUALIFIED',True):
            handler=make_handler(settings);handler.viewer_service._catalog=lambda:{'ready':True}
            server=FixtureServer(('127.0.0.1',0),handler)
            thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02});thread.start();started=True
            print(json.dumps({'host':'127.0.0.1','port':server.server_port,'fixture':'synthetic-v2'}),flush=True)
            ready,_,_=select.select([sys.stdin],[],[],120)
            if not ready or sys.stdin.readline().strip()!='STOP':raise RuntimeError('fixture stop not received')
            clean=True
    finally:
        if server is not None:
            try:
                if started and thread.is_alive():server.shutdown()
                server.server_close()
            except Exception:clean=False;listener_safe=False
        if started:
            thread.join(5)
            if thread.is_alive():clean=False;listener_safe=False
        if server is not None and not server.wait_idle(5):clean=False;listener_safe=False
        if handler is not None and listener_safe:
            try:handler.viewer_service.close()
            except Exception:clean=False
            else:
                try:handler.prototype_queue.close()
                except Exception:clean=False
                else:
                    try:handler.prototype_queue.authority_lease.close()
                    except Exception:clean=False
        if clean:
            try:shutil.rmtree(root)
            except Exception:clean=False
        if not clean:print('synthetic fixture retained temporary state at '+str(root),file=sys.stderr,flush=True)
    if not clean:raise RuntimeError('synthetic fixture cleanup uncertain')


if __name__=='__main__':
    try:main()
    except Exception:
        print('synthetic fixture failed',file=sys.stderr,flush=True)
        raise SystemExit(1)
