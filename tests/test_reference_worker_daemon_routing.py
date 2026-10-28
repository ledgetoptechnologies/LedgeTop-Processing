"""Closed daemon opt-in and real component composition; no real engine launch."""
import copy
import json
import os
from pathlib import Path
import shutil
import socket
import ssl
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

from ops import reference_worker_daemon as daemon
from ops.native_node_fixture import NativeNodeFixture
from processing.node_gate import NodeGate
from processing.reference_execution import ExecutionRouter
from processing.reference_provenance import LidarProvenanceClient
from processing.treedar_delivery import TreeDarDelivery
from processing.treedar_engine import TreeDarEngine, TreeDarHeld, SOURCE_FILES, source_digest


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux daemon composition fixture required')
class WorkerDaemonRoutingTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.root.chmod(0o700)
        self.gate_patch=patch('processing.node_gate.canonical_gate_path',return_value=self.root/'gate')
        self.gate_patch.start();self.addCleanup(self.gate_patch.stop)
        self.backend=NativeNodeFixture(self.root/'native.sqlite',synthetic_enabled=True)
        self.backend.start();self.addCleanup(self.backend.close)
        self.ca=self.root/'ca.pem'
        roots=ssl.create_default_context().get_ca_certs(binary_form=True)
        self.assertTrue(roots,'local CA fixture required')
        self.ca.write_text(ssl.DER_cert_to_PEM_cert(roots[0]),encoding='ascii')
        self.key=self.root/'key.pem'
        self.key.write_text('synthetic unused server-chain placeholder',encoding='ascii');self.key.chmod(0o600)
        self.source=self.root/'installed'
        (self.source/'processing').mkdir(parents=True)
        repository=Path(__file__).resolve().parents[1]
        for name in SOURCE_FILES:
            shutil.copyfile(repository/'processing'/name,self.source/'processing'/name)
        self.unix=socket.socket(socket.AF_UNIX)
        self.unix.bind(str(self.root/'docker.sock'));self.addCleanup(self.unix.close)
        self.state=self.root/'state'
        self.config=dict(schemaVersion=1,stateRoot=str(self.state),scratchRoot=str(self.root/'scratch'),
            nodeId='main',workerId='worker-main',workerInstanceId='instance-main',authorityId=str(uuid.uuid4()),
            engineId='synthetic-physical-engine',gateGeneration=1,gateCoordinatorOrigin='http://127.0.0.1:18080',
            coordinatorOrigin='https://coordinator.invalid',brokerOrigin='https://broker.invalid',engineOrigin=self.backend.origin,
            engineToken='',workerBearer='synthetic-private-worker-bearer-0001',resultReadBearer='synthetic-private-reader-bearer-0002',
            caFile=str(self.ca),serverCertFile=str(self.ca),serverKeyFile=str(self.key),listenHost='127.0.0.1',listenPort=9445,
            outputBudget=1048576,safetyFloor=1024,transferTtl=600,cleanupGrace=3600,pollSeconds=1,
            engineVersion='synthetic-transport-only',shareResults=True)
        self.docker_calls=[]
        self.listeners=[]

    def tree_settings(self):
        return dict(imageId='sha256:'+'b'*64,sourceRoot=str(self.source),sourceSha256=source_digest(self.source),engineVersion='0.0.1')

    def route(self, tree=True):
        self.config['executionRouting']=dict(schemaVersion=1,mode='per-job-v1',treedar=self.tree_settings() if tree else None)

    def load(self, config=None):
        path=self.root/'private.json'
        path.write_text(json.dumps(self.config if config is None else config),encoding='utf8');path.chmod(0o600)
        return daemon.load_config(str(path))

    def provision(self, config):
        with NodeGate(config['engineId']) as gate:
            gate.provision(config['nodeId'],config['gateCoordinatorOrigin'],config['authorityId'])

    def docker(self, controller, arguments, **kwargs):
        self.docker_calls.append(list(arguments))
        self.assertEqual(controller.config.docker_executable,'/usr/bin/docker')
        self.assertEqual(controller.config.docker_socket,str(self.root/'docker.sock'))
        if arguments==['info','--format','{{.ID}}']:return self.config['engineId']
        if arguments[:3]==['image','inspect','--format']:return controller.config.image_id
        self.fail('factory must never mutate Docker')

    def listener(self, api, host, port, *, ssl_context):
        self.assertIsInstance(ssl_context,ssl.SSLContext)
        self.listeners.append(api)
        return object()

    def build(self, config=None, *, provision=False):
        settings=self.load() if config is None else config
        if provision:self.provision(settings)
        # Existing HTTP/TLS tests cover the listener. Only its certificate load
        # and bind are replaced here; client TLS, HTTP engine probes and every
        # journal/component are real. Docker's fixed read-only transport is stubbed.
        with patch.object(ssl.SSLContext,'load_cert_chain',return_value=None), \
                patch.object(daemon,'create_reference_http_server',side_effect=self.listener), \
                patch.object(daemon,'TREEDAR_DOCKER_SOCKET',str(self.root/'docker.sock')), \
                patch.object(TreeDarEngine,'_docker',autospec=True,side_effect=self.docker):
            return daemon.build_worker(settings)

    def test_legacy_defaults_keep_odm_provider_and_create_no_routing_state(self):
        settings=self.load()
        self.assertNotIn('executionRouting',settings)
        self.assertFalse(settings['cleanupEnabled'])
        self.assertFalse(settings['engineCleanupEnabled'])
        self.assertEqual(settings['allowedEnvironments'],['production','staging'])
        driver,_=self.build(settings,provision=True)
        self.assertIsNone(driver.execution_router)
        self.assertIsNone(driver.provenance_client)
        self.assertIs(self.listeners[-1].results.worker,driver.worker)
        for name in ('execution-routes.sqlite','lidar-provenance.sqlite','treedar-controller.sqlite','treedar-delivery.sqlite'):
            self.assertFalse((self.state/name).exists())
        self.assertEqual(self.docker_calls,[])
        self.assertFalse(any(self.backend.counts().values()))

    def test_cluster_odm_only_opt_in_uses_router_without_treedar_or_issuer(self):
        self.config['nodeId']='cluster';self.config['workerId']='worker-cluster'
        self.route(tree=False)
        driver,_=self.build(provision=True)
        self.assertIsInstance(driver.execution_router,ExecutionRouter)
        self.assertIsNone(driver.execution_router.treedar_worker)
        self.assertIsNone(driver.provenance_client)
        self.assertIs(driver.execution_router.odm_worker,driver.worker)
        self.assertIs(self.listeners[-1].results.worker,driver.execution_router)
        self.assertEqual(self.docker_calls,[])
        self.assertFalse((self.state/'lidar-provenance.sqlite').exists())

    def test_main_tree_composition_shares_exact_components_and_only_probes_local_docker(self):
        self.route()
        driver,_=self.build(provision=True)
        router,client=driver.execution_router,driver.provenance_client
        self.assertIsInstance(client,LidarProvenanceClient)
        self.assertIsInstance(router.treedar_worker,TreeDarDelivery)
        tree=router.treedar_worker
        self.assertIs(router.odm_worker,driver.worker)
        self.assertIs(client.broker,driver.broker)
        self.assertEqual(client.origin,driver.broker.origin)
        self.assertEqual(tree.controller.verifier,client.verify)
        for component in (router,tree,tree.controller):
            self.assertIs(component.ledger,driver.ledger)
        self.assertIs(tree.outbox,driver.outbox)
        self.assertIs(tree.failure_outbox,driver.failure_outbox)
        self.assertEqual((tree.engine_id,tree.authority_id,tree.gate_generation,tree.origin),
            (driver.worker.engine_id,driver.worker.authority_id,driver.worker.gate_generation,driver.worker.origin))
        self.assertIsNone(tree.controller.config.timeout_seconds)
        self.assertIs(self.listeners[-1].results.worker,router)
        self.assertEqual([call[0] for call in self.docker_calls],['info','image'])
        for name in ('execution-routes.sqlite','lidar-provenance.sqlite','treedar-controller.sqlite','treedar-delivery.sqlite'):
            self.assertEqual((self.state/name).stat().st_mode & 0o777,0o600)
        with NodeGate(self.config['engineId']) as gate:
            self.assertIsNone(gate.snapshot()['reservation'])
        api=self.listeners[-1]
        reply=api.handle('GET','/ltds/worker/v1/jobs/unknown/results/object',
            {'Authorization':'Bearer '+self.config['workerBearer']},protected_transport=True)
        self.assertEqual(reply.status,401)
        self.assertFalse(any(self.backend.counts().values()))

    def test_routing_config_is_closed_versioned_main_only_and_secret_safe(self):
        self.route()
        invalid=[]
        for field,value in (('schemaVersion',2),('mode','automatic')):
            setting=copy.deepcopy(self.config);setting['executionRouting'][field]=value;invalid.append(setting)
        for field,value in (('imageId','registry/tree:latest'),('imageId','sha256:123'),('sourceSha256','bad'),
                ('sourceRoot','relative/source'),('engineVersion','version with space'),('dockerSocket','/other.sock'),
                ('dockerExecutable','/bin/sh'),('command',['/bin/sh']),('cpus',1),('memory','1g'),('pidsLimit',64),
                ('uid',1000),('timeoutSeconds',100),('issuerUrl','https://unconfigured.invalid')):
            setting=copy.deepcopy(self.config);setting['executionRouting']['treedar'][field]=value;invalid.append(setting)
        setting=copy.deepcopy(self.config);setting['nodeId']='cluster';invalid.append(setting)
        setting=copy.deepcopy(self.config);setting['executionRouting']['extra']=True;invalid.append(setting)
        setting=copy.deepcopy(self.config);del setting['executionRouting']['treedar'];invalid.append(setting)
        for setting in invalid:
            with self.subTest(routing=setting['executionRouting']):
                with self.assertRaises(ValueError) as error:self.load(setting)
                self.assertEqual(str(error.exception),'invalid private worker configuration')
                self.assertNotIn(self.config['workerBearer'],str(error.exception))
        self.assertFalse(any(self.backend.counts().values()))
        self.assertEqual(self.docker_calls,[])

    def test_source_pin_and_runtime_changes_refuse_before_claim_or_container_mutation(self):
        self.route()
        driver,_=self.build(provision=True)
        changed=self.load()
        changed['executionRouting']['treedar']['imageId']='sha256:'+'c'*64
        with self.assertRaises(TreeDarHeld):self.build(changed)
        self.assertEqual(len(self.listeners),1)
        self.assertEqual([call[0] for call in self.docker_calls],['info','image'])
        with driver._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM driver_jobs').fetchone()[0],0)

    def test_same_main_configuration_reopens_the_existing_journals_without_handover(self):
        self.route()
        first,_=self.build(provision=True)
        second,_=self.build()
        self.assertEqual(first.execution_router.path,second.execution_router.path)
        self.assertEqual(first.provenance_client.path,second.provenance_client.path)
        self.assertEqual(first.execution_router.treedar_worker.controller.path,
                         second.execution_router.treedar_worker.controller.path)
        with NodeGate(self.config['engineId']) as gate:
            state=gate.snapshot()
            self.assertEqual(state['generation'],1)
            self.assertEqual(state['authorityId'],self.config['authorityId'])
            self.assertIsNone(state['reservation'])
        self.assertEqual([call[0] for call in self.docker_calls],['info','image','info','image'])

    def test_wrong_source_pin_refuses_without_container_mutation(self):
        self.route()
        self.config['executionRouting']['treedar']['sourceSha256']='d'*64
        with self.assertRaises(TreeDarHeld):self.build(provision=True)
        self.assertEqual(self.listeners,[])
        self.assertEqual([call[0] for call in self.docker_calls],['info','image'])

    def test_existing_legacy_driver_claim_is_never_adopted_by_factory(self):
        driver,_=self.build(provision=True)
        with driver._db() as db:
            db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,'claim_pending','{}')",('legacy-job','legacy-request'))
        self.route(tree=False)
        with self.assertRaises(ValueError):self.build()
        with driver._db() as db:
            self.assertEqual(db.execute('SELECT stage FROM driver_jobs WHERE job=?',('legacy-job',)).fetchone()[0],'claim_pending')
        self.assertEqual(len(self.listeners),1)
        self.assertEqual(self.docker_calls,[])

