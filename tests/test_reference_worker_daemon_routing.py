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
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import uuid
import hashlib

from ops import reference_worker_daemon as daemon
from ops.native_node_fixture import NativeNodeFixture
from processing.node_gate import NodeGate
from processing.reference_execution import ExecutionRouter
from processing.reference_provenance import LidarProvenanceClient
from processing.reference_readers import ReaderSession
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

    def load(self, config=None, **kwargs):
        path=self.root/'private.json'
        path.write_text(json.dumps(self.config if config is None else config),encoding='utf8');path.chmod(0o600)
        return daemon.load_config(str(path),**kwargs)

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
        return SimpleNamespace(server_close=lambda:None)

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
            driver,server=daemon.build_worker(settings)
            self.addCleanup(server.server_close)
            return driver,server

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

    def test_disabled_direct_preparation_does_not_read_unprovisioned_files(self):
        self.config['directPreparation']=dict(enabled=False,host='127.0.0.1',port=19999,
            certificate='/absent/cert',privateKey='/absent/key',clientCA='/absent/ca',clients={},
            sourceAttestationKeys=[],signingKey='/absent/signer',issuerId='worker',keyId='worker-key',
            mountPath='/absent/mount',mountId='a'*32,markerName='.marker')
        loaded=self.load()
        self.assertFalse(loaded['directPreparation']['enabled'])
        _,server=self.build(loaded,provision=True)
        self.assertFalse(hasattr(server,'worker_direct_owner'))

    def test_enabled_direct_listener_is_composed_but_closed_before_start(self):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from ops.qualify_enrolled_phase_transport import configuration_material
        tls=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))['configurationFiles']
        for name in ('fixture-ca.pem','fixture-cert.pem','fixture-key.pem'):
            (self.root/name).write_bytes(tls[name]);(self.root/name).chmod(0o600)
        signer=Ed25519PrivateKey.generate();signer_path=self.root/'direct-signer.pem'
        signer_path.write_bytes(signer.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
        signer_path.chmod(0o600)
        public=signer.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        marker_name='.synthetic-direct-'+uuid.uuid4().hex;marker=Path('/tmp')/marker_name
        marker.write_text('a'*32);marker.chmod(0o600);self.addCleanup(lambda:marker.unlink(missing_ok=True))
        sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
        self.config['directPreparation']=dict(enabled=True,host='127.0.0.1',port=port,
            certificate=str(self.root/'fixture-cert.pem'),privateKey=str(self.root/'fixture-key.pem'),clientCA=str(self.root/'fixture-ca.pem'),
            clients={'b'*64:{'principal':'coord-a','environment':'staging'}},
            sourceAttestationKeys=[dict(environment='staging',issuerId='viewer',keyId='viewer-key',principalId='viewer-a',publicKeyPem=public)],
            signingKey=str(signer_path),issuerId='worker',keyId='worker-key',mountPath='/tmp',mountId='a'*32,markerName=marker_name)
        # /tmp is intentionally unprotected; this composition test separately
        # stubs file trust while the protected-reader tests cover that boundary.
        with patch('processing.direct_protected_transport._read',side_effect=lambda path,**kw:Path(path).read_bytes()):
            path=self.root/'private.json';path.write_text(json.dumps(self.config),encoding='utf8');path.chmod(0o600)
            digest=hashlib.sha256(path.read_bytes()).hexdigest()
            pins={name:hashlib.sha256(Path(self.config['directPreparation'][name]).read_bytes()).hexdigest()
                  for name in ('certificate','privateKey','clientCA','signingKey')}
            with self.assertRaises(ValueError):daemon.load_config(str(path))
            with self.assertRaises(ValueError):daemon.load_config(str(path),expected_direct_config_sha256='0'*64)
            loaded=daemon.load_config(str(path),expected_direct_config_sha256=digest,expected_direct_credential_sha256=pins)
        _,server=self.build(loaded,provision=True)
        self.assertEqual(server.worker_direct_owner.status(),'not_started')
        server.server_close()
        self.assertEqual(server.worker_direct_owner.status(),'closed')
        self.config['directPreparation']['clients']={'not-a-fingerprint':{'principal':'coord-a','environment':'staging'}}
        with self.assertRaises(ValueError):self.load(expected_direct_config_sha256=digest)

    def test_enabled_direct_requires_protected_exact_enrollment_digest(self):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from ops.qualify_enrolled_phase_transport import configuration_material
        with tempfile.TemporaryDirectory(dir='/run') as directory:
            root=Path(directory);root.chmod(0o700)
            tls=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))['configurationFiles']
            for name in ('fixture-ca.pem','fixture-cert.pem','fixture-key.pem'):
                (root/name).write_bytes(tls[name]);(root/name).chmod(0o600)
            signer=Ed25519PrivateKey.generate();signer_path=root/'signer.pem'
            signer_path.write_bytes(signer.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()));signer_path.chmod(0o600)
            public=signer.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode()
            self.config['directPreparation']=dict(enabled=True,host='127.0.0.1',port=19999,
                certificate=str(root/'fixture-cert.pem'),privateKey=str(root/'fixture-key.pem'),clientCA=str(root/'fixture-ca.pem'),
                clients={'b'*64:{'principal':'coord-a','environment':'staging'}},
                sourceAttestationKeys=[dict(environment='staging',issuerId='viewer',keyId='viewer-key',principalId='viewer-a',publicKeyPem=public)],
                signingKey=str(signer_path),issuerId='worker',keyId='worker-key',mountPath='/tmp',mountId='a'*32,markerName='.marker')
            path=root/'config.json';path.write_text(json.dumps(self.config));path.chmod(0o600)
            digest=hashlib.sha256(path.read_bytes()).hexdigest()
            pins={name:hashlib.sha256(Path(self.config['directPreparation'][name]).read_bytes()).hexdigest()
                  for name in ('certificate','privateKey','clientCA','signingKey')}
            with self.assertRaises(ValueError):daemon.load_config(str(path))
            with self.assertRaises(ValueError):daemon.load_config(str(path),expected_direct_config_sha256='0'*64)
            self.assertTrue(daemon.load_config(str(path),expected_direct_config_sha256=digest,
                                              expected_direct_credential_sha256=pins)['directPreparation']['enabled'])
            for name in pins:
                altered=dict(pins);altered[name]='0'*64
                with self.subTest(credential=name),self.assertRaises(ValueError):
                    daemon.load_config(str(path),expected_direct_config_sha256=digest,expected_direct_credential_sha256=altered)
                credential=Path(self.config['directPreparation'][name]);original=credential.read_bytes()
                try:
                    credential.write_bytes(original+b'\n')
                    with self.assertRaises(ValueError):
                        daemon.load_config(str(path),expected_direct_config_sha256=digest,expected_direct_credential_sha256=pins)
                finally:credential.write_bytes(original)
            path.write_text(json.dumps(dict(self.config,engineVersion='changed')))
            with self.assertRaises(ValueError):daemon.load_config(str(path),expected_direct_config_sha256=digest)
            absent=copy.deepcopy(self.config);absent.pop('directPreparation')
            disabled=copy.deepcopy(self.config);disabled['directPreparation']['enabled']=False
            for substituted in (absent,disabled):
                path.write_text(json.dumps(substituted))
                with self.subTest(substitution='disabled' if 'directPreparation' in substituted else 'absent'):
                    with self.assertRaises(ValueError):daemon.load_config(str(path),expected_direct_config_sha256=digest)
            # A matching digest does not make an unsafe ancestor trusted.
            unsafe=self.root/'unsafe-config.json';unsafe.write_text(json.dumps(self.config));unsafe.chmod(0o600)
            with self.assertRaises(ValueError):daemon.load_config(str(unsafe),expected_direct_config_sha256=hashlib.sha256(unsafe.read_bytes()).hexdigest())

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

    def test_source_range_v2_remains_explicit_opt_in(self):
        default = self.load()
        self.assertFalse(default['sourceRangeV2Enabled'])
        self.config['sourceRangeV2Enabled'] = True
        settings = self.load()
        driver, _ = self.build(settings, provision=True)
        self.assertTrue(driver.source_range_v2_enabled)
        self.assertIsNotNone(driver.source_range_client)
        self.assertEqual(self.docker_calls, [])

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
        self.assertEqual([call[0] for call in self.docker_calls],['info','image']*2)
        for name in ('execution-routes.sqlite','lidar-provenance.sqlite','treedar-delivery.sqlite'):
            self.assertEqual((self.state/name).stat().st_mode & 0o777,0o600)
        self.assertEqual(tree.controller.path.stat().st_mode & 0o777,0o600)
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

    def test_live_owner_blocks_runtime_changes_and_idle_migration_retains_old_journal(self):
        self.route()
        driver,server=self.build(provision=True)
        old=driver.execution_router.treedar_worker.controller
        changed=self.load()
        changed['executionRouting']['treedar']['imageId']='sha256:'+'c'*64
        with self.assertRaises(ValueError):self.build(changed)
        self.assertEqual(len(self.listeners),1)
        server.server_close()
        replacement,_=self.build(changed)
        self.assertNotEqual(old.path,replacement.execution_router.treedar_worker.controller.path)
        self.assertTrue(old.path.exists())
        self.assertEqual(old.config.image_id,'sha256:'+'b'*64)
        self.assertEqual([call[0] for call in self.docker_calls],['info','image']*4)
        with driver._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM driver_jobs').fetchone()[0],0)
        self.assertEqual(driver.step(),{'state':'held','reason':'worker_state_owner_unavailable'})

    def test_same_main_configuration_reopens_the_existing_journals_without_handover(self):
        self.route()
        first,server=self.build(provision=True)
        server.server_close()
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
        self.assertEqual([call[0] for call in self.docker_calls],['info','image']*3)

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

    def test_factory_failure_closes_owned_reader_lease_without_starting_threads(self):
        observed=[]
        def failed_listener(api,*args,**kwargs):
            observed.append(api.results.reader_session)
            raise OSError('synthetic listener bind failure')
        with patch.object(self,'listener',side_effect=failed_listener):
            with self.assertRaises(OSError):self.build(provision=True)
        self.assertEqual(len(observed),1)
        with self.assertRaises(Exception):observed[0].check()
        with ReaderSession(observed[0].ledger):pass

    def test_server_close_retains_lease_until_successful_handler_join_and_is_idempotent(self):
        failed=[True]
        def listener(api,*args,**kwargs):
            self.listeners.append(api)
            def close():
                if failed[0]:raise OSError('synthetic uncertain handler join')
            return SimpleNamespace(server_close=close)
        with patch.object(self,'listener',side_effect=listener):
            _,server=self.build(provision=True)
        session=self.listeners[-1].results.reader_session
        self.assertFalse(server.daemon_threads)
        self.assertTrue(server.block_on_close)
        with self.assertRaises(OSError):server.server_close()
        session.check()
        with self.assertRaises(Exception):
            with ReaderSession(session.ledger):pass
        failed[0]=False
        server.server_close();server.server_close()
        with ReaderSession(session.ledger):pass

    def test_direct_factory_has_exclusive_reader_owner_and_rejects_unsupported_platform(self):
        settings=self.load()
        _,server=self.build(settings,provision=True)
        session=self.listeners[-1].results.reader_session
        with self.assertRaises(Exception):self.build(settings)
        session.check()
        server.server_close()
        with patch.object(daemon.sys,'platform','win32'):
            with self.assertRaisesRegex(ValueError,'requires Linux'):daemon.build_worker(settings)

    def test_direct_server_close_waits_for_live_driver_stage_and_fences_stale_driver(self):
        driver,server=self.build(provision=True)
        entered,release,closed=threading.Event(),threading.Event(),threading.Event()
        def blocked():
            entered.set();release.wait(3)
            return {'state':'idle'}
        driver._step=blocked
        active=threading.Thread(target=driver.step)
        active.start();self.assertTrue(entered.wait(2))
        closer=threading.Thread(target=lambda:(server.server_close(),closed.set()))
        closer.start()
        try:
            self.assertFalse(closed.wait(0.05))
            with self.assertRaises(ValueError):self.build()
        finally:
            release.set();active.join(3);closer.join(3)
        self.assertTrue(closed.is_set())
        replacement,_=self.build()
        self.assertIsNotNone(replacement)
        self.assertEqual(driver.step(),{'state':'held','reason':'worker_state_owner_unavailable'})

    def test_worker_service_joins_driver_before_releasing_factory_owner(self):
        serving,shutdown=threading.Event(),threading.Event()
        def listener(api,*args,**kwargs):
            self.listeners.append(api)
            def serve_forever(**kwargs):serving.set();shutdown.wait(3)
            return SimpleNamespace(server_close=lambda:None,serve_forever=serve_forever,shutdown=shutdown.set)
        with patch.object(self,'listener',side_effect=listener):driver,server=self.build(provision=True)
        entered,release,stop=threading.Event(),threading.Event(),threading.Event()
        def step():
            entered.set();release.wait(3)
            return {'state':'idle'}
        # Deliberately bypass the driver mutex here: service join ordering must
        # independently retain ownership even for a custom bounded driver.
        driver.step=step
        service=daemon.WorkerService(driver,server,poll_seconds=0.1)
        result=[];thread=threading.Thread(target=lambda:result.append(service.run(stop)))
        thread.start();self.assertTrue(entered.wait(2));self.assertTrue(serving.wait(2));stop.set()
        try:
            thread.join(0.05);self.assertTrue(thread.is_alive())
            with self.assertRaises(ValueError):self.build()
        finally:
            release.set();thread.join(3)
        self.assertEqual(result,[0])
        replacement,_=self.build()
        self.assertIsNotNone(replacement)

    def test_worker_service_keeps_ownership_until_direct_listener_drains(self):
        serving,shutdown=threading.Event(),threading.Event()
        def listener(api,*args,**kwargs):
            self.listeners.append(api)
            return SimpleNamespace(server_close=lambda:None,serve_forever=lambda **kw:(serving.set(),shutdown.wait(3)),shutdown=shutdown.set)
        with patch.object(self,'listener',side_effect=listener):driver,server=self.build(provision=True)
        release=threading.Event();draining=threading.Event();stop=threading.Event()
        class DirectOwner:
            def start(self):pass
            def status(self):return 'running'
            def close(self,timeout):draining.set();return release.wait(.02)
        server.worker_direct_owner=DirectOwner()
        driver.step=lambda:{'state':'idle'}
        service=daemon.WorkerService(driver,server,poll_seconds=.1)
        result=[];thread=threading.Thread(target=lambda:result.append(service.run(stop)));thread.start()
        self.assertTrue(serving.wait(2));stop.set();self.assertTrue(draining.wait(2))
        try:
            thread.join(.05);self.assertTrue(thread.is_alive())
            with self.assertRaises(ValueError):self.build()
        finally:release.set();thread.join(3)
        self.assertEqual(result,[0])

    def test_direct_listener_failure_immediately_marks_service_unhealthy(self):
        server=SimpleNamespace(worker_direct_owner=SimpleNamespace(status=lambda:'failed'))
        service=daemon.WorkerService(SimpleNamespace(),server,poll_seconds=60)
        service._state='ready'
        self.assertEqual(service.health()['state'],'failed')
        self.assertEqual(service.health()['reason'],'direct_preparation_listener_failed')

    def test_state_owner_check_rejects_replaced_lock_path_and_wrong_root(self):
        root=self.root/'owner'
        with daemon.DaemonStateLock(root) as owner:
            self.assertIs(owner.check(root),owner)
            with self.assertRaises(ValueError):owner.check(self.root/'other')
            path=root/'worker-daemon.lock';saved=root/'original-lock'
            path.rename(saved)
            try:
                path.write_bytes(b'');path.chmod(0o600)
                with self.assertRaises(ValueError):owner.check(root)
            finally:
                path.unlink();saved.rename(path)
            self.assertIs(owner.check(root),owner)

