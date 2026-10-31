"""Synthetic empty coordinator/worker fixture for real Viewer submission."""
import base64
from dataclasses import replace
import hashlib
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
from pathlib import Path
import ssl
import shutil
import subprocess
import tempfile
import threading
import time
import zipfile
from unittest.mock import patch

from ops.reference_worker_daemon import DaemonStateLock
from processing.config import Config
from processing.direct_output_slot import _identity
from processing.viewer_archive_spool import ViewerArchiveSpool
from processing.viewer_http import ViewerFacade
from processing.reference_runtime import ReferenceRuntime,ReferenceSettings
from processing.viewer_staged_ack_schema import initialize_staged_ack_schema
from tests import test_viewer_node_enrollment as enrollment_tests
from tests import test_viewer_node_inputs as input_tests
from processing.viewer_local_result import capture,send_completion
from processing.viewer_chunk_spool import SyntheticChunkSpool
from processing.viewer_synthetic_chunk_sender import send as send_chunks
from processing.viewer_service import ViewerService,NativeEngine
from processing.viewer_synthetic_submission import submit
from processing.viewer_worker_claim import ViewerWorkerClaims
from tests.test_viewer_synthetic_submission import SyntheticSubmissionTests
from tests.test_viewer_worker_claim import ViewerWorkerClaimTests,ENV_POLICY,TASK


def empty_coordinator(self):
    self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
    self.cert=self.root/'cert';self.key=self.root/'key'
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
        '-subj','/CN=synthetic-loopback','-addext','subjectAltName=IP:127.0.0.1',
        '-addext','basicConstraints=critical,CA:TRUE','-keyout',str(self.key),'-out',str(self.cert)],
        check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
    self.tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);self.tls.load_cert_chain(self.cert,self.key)
    self.client=ssl.create_default_context(cafile=str(self.cert));self.server=None;self.thread=None;self.queue=None
    self.start_queue()
    self.claims=ViewerWorkerClaims(self.queue,self.store,local_max_photos=1000,local_max_input_bytes=100000,node_environments=ENV_POLICY)
    self.start_server()


def stop_worker(worker, cancel, timeout=10):
    """Do not tear down fixture state while its owned worker may still use it."""
    cancel.set()
    if worker is not None:
        worker.join(timeout)
        if worker.is_alive():
            os.write(2,b'synthetic worker did not stop; terminating owned fixture process\n')
            os._exit(2)


def main():
    original=zipfile.ZipFile.writestr
    entries=[('odm_texturing/odm_textured_model_geo.glb','glb'),
             ('odm_texturing/odm_textured_model_geo.obj','mtllib model.mtl\nv 0 0 0\n'),
             ('odm_texturing/model.mtl','newmtl surface\nmap_Kd texture.jpg\n'),
             ('odm_texturing/texture.jpg','texture'),('entwine_pointcloud/ept.json','{}'),
             ('3d_tiles/model/tileset.json','{}')]
    def write(archive,name,data,*args,**kwargs):
        if name=='synthetic.txt':
            for entry,body in entries:original(archive,entry,body)
            return
        return original(archive,name,data,*args,**kwargs)
    synthetic=SyntheticSubmissionTests('test_exact_submission_once_and_replay_without_cleanup')
    runtime=None
    discovery=provider=None;discovery_thread=provider_thread=worker_thread=None;failure=[];done=threading.Event();cancel=threading.Event()
    try:
        original_enrollment=enrollment_tests.NodeEnrollmentTests.setUp
        def stage_enrollment(obj):
            original_enrollment(obj);obj.config['allowed_environments']=['staging']
        def stage_inputs(obj,owner):
            from processing.viewer_node_inputs import ViewerNodeInputs
            return ViewerNodeInputs(obj.base.make(owner),obj.base.fixture.job,'enroll-1','staging',input_root=obj.inputs,input_root_identity=obj.input_id,max_total=100000,max_object=100000)
        with patch.object(ViewerWorkerClaimTests,'setUp',empty_coordinator),patch.object(enrollment_tests.NodeEnrollmentTests,'setUp',stage_enrollment),patch.object(input_tests.NodeInputsTests,'make',stage_inputs),patch.object(zipfile.ZipFile,'writestr',write):synthetic.setUp()
        fixture=synthetic.base.base.fixture
        class Discovery(BaseHTTPRequestHandler):
            def do_GET(self):
                value=([{'name':'dsm','type':'bool','value':False}] if self.path=='/options' else
                       {'version':'2.2.3','engine':'odm','engineVersion':'synthetic'})
                raw=json.dumps(value).encode();self.send_response(200);self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
            def log_message(self,*_args):pass
        discovery=ThreadingHTTPServer(('127.0.0.1',0),Discovery)
        discovery_thread=threading.Thread(target=discovery.serve_forever,kwargs={'poll_interval':.05});discovery_thread.start()
        discover_origin='http://127.0.0.1:'+str(discovery.server_port)
        spool_root=fixture.root/'coordinator-spool';spool_root.mkdir(mode=0o700)
        fd=os.open(spool_root,os.O_RDONLY|os.O_DIRECTORY)
        try:spool_identity=_identity(fd)
        finally:os.close(fd)
        spool=ViewerArchiveSpool(fixture.claims,root=spool_root,root_identity=spool_identity,max_archive_bytes=65536,enabled=True)
        fixture.api.viewer_spool=spool
        fixture.api.viewer_chunks=SyntheticChunkSpool(spool,synthetic_fixture=True)
        config=Config('synthetic-legacy-token',discover_origin,discover_origin,1000,100000,
                      prototype_state_dir=str(fixture.root/'state'),viewer_enabled=True)
        service=ViewerService(fixture.queue,config,store=fixture.store,engines={},viewer_spool=spool,viewer_completion_projection=True)
        initialize_staged_ack_schema(fixture.queue)
        bearer=base64.urlsafe_b64encode(b'c'*32).decode().rstrip('=')
        workers={node:dict(bearer=key*32,workerId='worker-'+node,instanceId='instance-'+node,resultOrigin='https://127.0.0.1:'+port,resultBearer=other*32) for node,key,port,other in [('cluster','a','1234','r'),('main','m','1235','s')]}
        config=replace(service.config,prototype_node_ids=('cluster','main'),viewer_worker_claim_enabled=True,viewer_worker_claim_env_policy=(('cluster',('staging',)),('main',('staging',))))
        settings=ReferenceSettings(dict(host='127.0.0.1',port=0,certificate=str(fixture.cert),privateKey=str(fixture.key),clientCA=str(fixture.cert)),{'v'*32:'staging'},workers,dict(maxConcurrent=1,maxObjectBytes=65536),staged_ack_bearers={bearer:{'environment':'staging','principalId':'viewer-stage'}},viewer_archive_spool=dict(root=str(spool_root),rootIdentity=spool_identity,maxArchiveBytes=65536))
        with patch('processing.reference_runtime.STAGED_ACK_SCHEMA_QUALIFIED',True),patch('processing.reference_runtime.VIEWER_ARCHIVE_SPOOL_SCHEMA_QUALIFIED',True):
            runtime=ReferenceRuntime(fixture.queue,config,settings,viewer_service=service);runtime.start()
        facade=ViewerFacade(service,'viewer-production-token','viewer-staging-token')
        class Provider(BaseHTTPRequestHandler):
            def handle_request(self):
                if not facade.handle(self):self.send_error(404)
            do_GET=handle_request;do_POST=handle_request
            def log_message(self,*_args):pass
        provider=ThreadingHTTPServer(('127.0.0.1',0),Provider)
        provider_thread=threading.Thread(target=provider.serve_forever,kwargs={'poll_interval':.05});provider_thread.start()
        def execute():
            try:
                deadline=time.monotonic()+45
                while time.monotonic()<deadline and not cancel.is_set():
                    try:
                        task=fixture.store.task(TASK)
                        if task['phase']=='queued':break
                    except KeyError:pass
                    time.sleep(.05)
                else:
                    if cancel.is_set():return
                    raise RuntimeError('Viewer submission did not reach queue')
                fixture.job=task['job_id']
                if len(task['files'])!=1 or Path(task['files'][0]['path']).read_bytes()!=b'synthetic':raise RuntimeError('Viewer input mismatch')
                with DaemonStateLock(synthetic.base.base.worker) as owner:
                    inputs=stage_inputs(synthetic.base,owner);plan=inputs.fetch_plan()
                    if plan['options']!=[{'name':'dsm','value':True}]:raise RuntimeError('Viewer plan mismatch')
                    inputs.freeze_inventory();object_id=synthetic.base.object_id(inputs);inputs.transfer(object_id)
                    with inputs._db() as db:row=db.execute('SELECT slot_key FROM objects WHERE object_id=?',(object_id,)).fetchone()
                    if (synthetic.base.inputs/('object-'+object_id)/('slot-'+row[0]+'.alloc')).read_bytes()!=b'synthetic':raise RuntimeError('node input mismatch')
                    submit(inputs,NativeEngine(synthetic.origin),synthetic_fixture=True)
                    if [item[0] for item in synthetic.calls]!=['init','upload','commit']:raise RuntimeError('engine call sequence mismatch')
                    if b'"name": "dsm", "value": true' not in synthetic.calls[0][3] or b'\r\n\r\nsynthetic\r\n' not in synthetic.calls[1][3]:
                        raise RuntimeError('engine plan or bytes mismatch')
                    result=capture(inputs.worker,NativeEngine(synthetic.origin),synthetic_fixture=True)
                    receipt=send_chunks(inputs.worker,synthetic_fixture=True,chunk_bytes=256)
                    if (receipt['kind']!='viewer-coordinator-archive-receipt'
                            or receipt['archiveSha256']!=result['archiveSha256']
                            or receipt['archiveBytes']!=len(synthetic.archive)):
                        raise RuntimeError('promoted archive receipt mismatch')
                    with fixture.queue._db() as db:
                        session=db.execute('SELECT state,chunk_count FROM viewer_worker_chunk_sessions WHERE job_id=?',(fixture.job,)).fetchone()
                        permits=db.execute("SELECT COUNT(*) FROM viewer_worker_chunk_permits WHERE job_id=? AND state='finished'",(fixture.job,)).fetchone()[0]
                        row=db.execute('SELECT state,receipt FROM viewer_worker_spool WHERE job_id=?',(fixture.job,)).fetchone()
                    if not session or session['state']!='sealed' or session['chunk_count']<2 or permits!=session['chunk_count'] or not row or row['state']!='sealed' or json.loads(row['receipt'])!=receipt:
                        raise RuntimeError('chunk promotion state mismatch')
                    send_completion(inputs.worker,synthetic_fixture=True)
                    if result['archiveSha256']!=hashlib.sha256(synthetic.archive).hexdigest():raise RuntimeError('archive hash mismatch')
                done.set()
            except BaseException as error:
                failure.append(type(error).__name__+': '+str(error));done.set()
        worker_thread=threading.Thread(target=execute,daemon=True);worker_thread.start()
        shutil.copyfile(fixture.cert,str(Path(os.environ['BRIDGE_READY']).parent/'synthetic-ca.pem'))
        ready=dict(port=provider.server_port,task=TASK,archiveSha256=hashlib.sha256(synthetic.archive).hexdigest(),token='viewer-staging-token',ackPort=runtime._server.server_port,authority=fixture.queue.authority_id,bearer=bearer)
        Path(os.environ['BRIDGE_READY']).write_text(json.dumps(ready))
        deadline=time.monotonic()+120
        while not Path(os.environ['BRIDGE_STOP']).exists() and time.monotonic()<deadline:
            if done.is_set() and failure:raise RuntimeError(failure[0])
            time.sleep(.1)
        worker_thread.join(10)
        if worker_thread.is_alive() or failure or not done.is_set():raise RuntimeError('synthetic worker incomplete')
        retained=dict(nodeInputCopies=len(list(synthetic.base.inputs.rglob('*.alloc'))),
                      nodeResultCopies=len(list(synthetic.base.base.output.glob('*.alloc'))),
                      coordinatorSpoolCopies=len(list(spool_root.glob('*.alloc'))),
                      coordinatorJobStatus=fixture.queue.get(fixture.job)['status'])
        if retained!={'nodeInputCopies':1,'nodeResultCopies':1,'coordinatorSpoolCopies':1,'coordinatorJobStatus':'running'}:
            raise AssertionError('synthetic retention changed')
        with fixture.queue._db() as db:
            ack=db.execute('SELECT COUNT(*) FROM viewer_staged_acks WHERE task_id=? AND environment=?',(TASK,'staging')).fetchone()[0]
        if ack!=1:raise AssertionError('staged acknowledgment count mismatch')
        retained['stagedMetadataAcks']=ack
        Path(os.environ['BRIDGE_STOP']+'.result').write_text(json.dumps(retained))
    finally:
        stop_worker(worker_thread,cancel)
        if runtime is not None:runtime.close()
        if provider is not None:provider.shutdown();provider.server_close();provider_thread.join(5)
        if discovery is not None:discovery.shutdown();discovery.server_close();discovery_thread.join(5)
        synthetic.doCleanups()


if __name__=='__main__':main()
