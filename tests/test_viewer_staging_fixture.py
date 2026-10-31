"""Synthetic staging-only node/spool setup; no live data or runtime activation."""
import base64
import io
import http.client
import json
import unittest
from unittest.mock import patch

from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_local_result import send_completion
from processing.viewer_node_inputs import ViewerNodeInputs
from tests.viewer_full_bridge_server import empty_coordinator
from tests import test_viewer_node_enrollment as enrollment_tests
from tests import test_viewer_node_inputs as input_tests
from tests import test_viewer_spool_provider as spool_tests
from tests import test_viewer_worker_claim as claim_tests
TASK=claim_tests.TASK

CAP=base64.urlsafe_b64encode(b'b'*32).decode().rstrip('=')

def staging_coordinator(fixture):
    empty_coordinator(fixture)
    fixture.store.reserve(TASK,'staging',CAP)
    fixture.store.initialize(TASK,'synthetic',[],[],'staging',CAP)
    fixture.store.upload(TASK,'photo.jpg',io.BytesIO(b'synthetic'),9,'staging',CAP)
    fixture.store.commit(TASK,'staging',CAP)
    fixture.job=fixture.store.task(TASK)['job_id']

def staging_enrollment(fixture):
    original_enrollment(fixture)
    fixture.config['allowed_environments']=['staging']

original_enrollment=enrollment_tests.NodeEnrollmentTests.setUp

def staging_inputs(fixture,owner):
    return ViewerNodeInputs(fixture.base.make(owner),fixture.base.fixture.job,'enroll-1','staging',
        input_root=fixture.inputs,input_root_identity=fixture.input_id,max_total=100000,max_object=100000)

class StagingFixtureTests(unittest.TestCase):
    def test_node_spool_and_completion_are_created_as_staging(self):
        fixture=spool_tests.ViewerSpoolProviderTests('test_authenticated_provider_download_keeps_running_and_retains_data')
        with patch.object(claim_tests.ViewerWorkerClaimTests,'setUp',staging_coordinator),patch.object(enrollment_tests.NodeEnrollmentTests,'setUp',staging_enrollment),patch.object(input_tests.NodeInputsTests,'make',staging_inputs):
            fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.assertEqual(fixture.fixture.store.task(TASK)['environment'],'staging')
        with DaemonStateLock(fixture.base.base.base.base.base.worker) as owner:
            send_completion(fixture.base.base.worker(owner),synthetic_fixture=True)
        with fixture.fixture.queue._db() as db:
            claim=db.execute('SELECT environment FROM viewer_worker_claims WHERE job_id=?',(fixture.fixture.job,)).fetchone()
            self.assertEqual(claim['environment'],'staging')
        self.assertEqual(fixture.fixture.queue.get(fixture.fixture.job)['status'],'running')
        self.assertEqual(len(list(fixture.base.root.glob('*.alloc'))),1)

    def test_assembled_tls_staging_ack_spool(self):
        from dataclasses import replace
        from processing.reference_runtime import ReferenceRuntime,ReferenceSettings
        from processing.viewer_staged_ack_schema import initialize_staged_ack_schema
        from processing.viewer_staged_ack import ROUTE
        from processing import reference_protocol as protocol
        fixture=spool_tests.ViewerSpoolProviderTests('test_authenticated_provider_download_keeps_running_and_retains_data')
        with patch.object(claim_tests.ViewerWorkerClaimTests,'setUp',staging_coordinator),patch.object(enrollment_tests.NodeEnrollmentTests,'setUp',staging_enrollment),patch.object(input_tests.NodeInputsTests,'make',staging_inputs):fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        with DaemonStateLock(fixture.base.base.base.base.base.worker) as owner:
            send_completion(fixture.base.base.worker(owner),synthetic_fixture=True)
        initialize_staged_ack_schema(fixture.fixture.queue)
        bearer=base64.urlsafe_b64encode(b'c'*32).decode().rstrip('=')
        workers={node:dict(bearer=key*32,workerId='worker-'+node,instanceId='instance-'+node,
                resultOrigin='https://127.0.0.1:'+port,resultBearer=other*32)
                for node,key,port,other in [('cluster','a','1234','r'),('main','m','1235','s')]}
        config=replace(fixture.service.config,prototype_node_ids=('cluster','main'),viewer_worker_claim_enabled=True,
            viewer_worker_claim_env_policy=(('cluster',('staging',)),('main',('staging',))))
        policy=dict(root=str(fixture.base.root),rootIdentity=fixture.base.root_identity,maxArchiveBytes=65536)
        settings=ReferenceSettings(dict(host='127.0.0.1',port=0,certificate=str(fixture.fixture.cert),
            privateKey=str(fixture.fixture.key),clientCA=str(fixture.fixture.cert)),{'v'*32:'staging'},workers,
            dict(maxConcurrent=1,maxObjectBytes=65536),staged_ack_bearers={bearer:{'environment':'staging','principalId':'viewer-stage'}},viewer_archive_spool=policy)
        with patch('processing.reference_runtime.STAGED_ACK_SCHEMA_QUALIFIED',True),patch('processing.reference_runtime.VIEWER_ARCHIVE_SPOOL_SCHEMA_QUALIFIED',True):
            runtime=ReferenceRuntime(fixture.fixture.queue,config,settings,viewer_service=fixture.service);runtime.start()
        self.addCleanup(runtime.close)
        def call(method,path,token=bearer,body=None):
            conn=http.client.HTTPSConnection('127.0.0.1',runtime._server.server_port,context=fixture.fixture.client,timeout=5)
            try:
                headers={'Authorization':'Bearer '+token,'X-LedgeTop-Capability':CAP}
                if body is not None:headers['Content-Type']='application/json'
                conn.request(method,path,body,headers);resp=conn.getresponse();return resp.status,json.loads(resp.read())
            finally:conn.close()
        path=ROUTE+TASK
        status,commitment=call('GET',path+'/commitment');self.assertEqual(status,200,commitment)
        self.assertEqual(commitment['environment'],'staging')
        self.assertEqual(call('GET',path+'/commitment',token=CAP)[0],401)
        import uuid
        value=dict(commitment,attemptId=str(uuid.uuid4()),modelId=str(uuid.uuid4()),modelVersionId=str(uuid.uuid4()),stagingCommitId=str(uuid.uuid4()))
        raw=protocol.canonical_json(value)
        self.assertEqual(call('POST',path,body=protocol.canonical_json(dict(value,generation=value['generation']+1)))[0],409)
        self.assertTrue(call('POST',path,body=raw)[1]['created'])
        self.assertFalse(call('POST',path,body=raw)[1]['created'])
        self.assertEqual(call('GET',path+'/generations/'+str(value['generation']))[1]['assertion'],value)
        self.assertEqual(fixture.fixture.queue.get(fixture.fixture.job)['status'],'running')
        self.assertEqual(len(list(fixture.base.root.glob('*.alloc'))),1)
