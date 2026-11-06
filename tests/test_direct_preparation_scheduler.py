"""Local mTLS worker + real queue preparation, never a custody ACK."""
from contextlib import closing
import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from cryptography.hazmat.primitives import serialization
from processing.direct_preparation_scheduler import DirectPreparationScheduler
from processing.direct_snapshot_http import create_snapshot_server
from processing.direct_worker_preparation_api import DirectPreparationAPI
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_source_intake import validate_source_intake
from processing.reference_store import PROFILE,ReferenceStore
from processing.reference_runtime import _DynamicAPI
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_driver import ReferenceDriver, _NoRedirects
from processing.reference_http import create_reference_http_server
from processing.reference_tls import verified_client_context
from urllib.request import build_opener,ProxyHandler,HTTPSHandler
from processing.scratch_policy import ScratchLedger
from tests import test_direct_protected_transport as protected_fixture
from tests import test_direct_snapshot_http as tls_fixture
from tests import test_direct_worker_receiver_v2 as worker_fixture

class SchedulerTest(unittest.TestCase):
    def test_real_worker_preparation_through_enrolled_mutual_tls(self):
        f=worker_fixture.WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        h=tls_fixture.SnapshotListenerTests();h.setUp();self.addCleanup(h.doCleanups)
        secure=protected_fixture.ProtectedTransportTests();secure.setUp();self.addCleanup(secure.doCleanups)
        for src,name in ((h.good,'client.pem'),(h.goodkey,'client.key'),(h.root/'fixture-ca.pem','server-ca.pem')):
            path=secure.root/name;path.write_bytes(src.read_bytes());path.chmod(0o600)
        fresh=f.root/'scheduler-worker';fresh.mkdir(mode=0o700)
        ledger=ScratchLedger(fresh/'ledger.sqlite',fresh,capacity_probe=lambda _:2000000,clock=lambda:100,safety_floor_bytes=100)
        origin='https://127.0.0.1:8443'
        driver=SimpleNamespace(worker=SimpleNamespace(engine_id='fixture-v2',origin=origin,gate_generation=1),
            direct_source_policy=lambda:dict(directPreparationEnabled=True,capabilityFingerprint=f.options['capability_fingerprint'],
                optionsFingerprint=f.options['options_fingerprint'],allowedOdmOptions=[]))
        backend=DirectPreparationAPI(driver=driver,ledger=ledger,worker=f.worker,mount_path='/tmp',mount_id='a'*32,
            marker_name=f.spool.marker.name,source_keys=f.options['trusted_source_keys'],signer=f.proof_key,
            issuer_id='worker-issuer',key_id='worker-key')
        server=create_snapshot_server('127.0.0.1',0,enabled=True,ssl_context=h.context,
            client_scopes={h.fingerprint:{'principal':'coordinator-a','environment':'staging'}},prepare_backend=backend)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        try:
            with patch('processing.node_gate.canonical_gate_path',return_value=fresh/'gate'):
                with NodeGate('fixture-v2') as gate:gate.provision('cluster',origin,f.worker['authorityId'])
                with tempfile.TemporaryDirectory() as directory:
                    with closing(sqlite3.connect(Path(directory)/'jobs.sqlite3')) as db,db:
                        db.execute('CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY,authority_id TEXT NOT NULL)')
                        db.execute('INSERT INTO coordinator_identity VALUES(1,?)',(f.worker['authorityId'],))
                    queue=Queue(directory,reference_enabled=True);queue.allowed_profiles.add(PROFILE)
                    try:
                        store=ReferenceStore(queue,source_routing_keys=f.options['trusted_source_keys'])
                        opts=f.options;checked=validate_source_intake(f.payload,**{k:v for k,v in opts.items() if k!='authenticated_principal'})
                        store.submit_source_intake(checked,capability_fingerprint=opts['capability_fingerprint'],
                            options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'],
                            source_spool=None,trusted_source_keys=opts['trusted_source_keys'],authority_id=opts['authority_id'],
                            ingress_mode='direct-v2',authenticated_principal='viewer-staging')
                        secure.node.update(origin='https://127.0.0.1:'+str(server.server_port),
                            caFile=str(secure.root/'server-ca.pem'),clientCertFile=str(secure.root/'client.pem'),
                            clientKeyFile=str(secure.root/'client.key'),issuerId='worker-issuer',keyId='worker-key',
                            workerPublicKeyPem=f.proof_key.public_key().public_bytes(serialization.Encoding.PEM,
                                serialization.PublicFormat.SubjectPublicKeyInfo).decode())
                        config={'schemaVersion':6,'directWorkers':{'enabled':True,'nodes':{n:secure.node for n in queue.node_ids}}}
                        secure.config.write_text(json.dumps(config));secure.config.chmod(0o600)
                        sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
                        pins=dict(ca=sha(secure.root/'server-ca.pem'),certificate=sha(secure.root/'client.pem'),
                            privateKey=sha(secure.root/'client.key'),bearer=sha(secure.bearer))
                        enrollment={n:dict(configPath=str(secure.config),configSha256=sha(secure.config),credentialSha256=pins,
                                           gateGeneration=1,outputBudget=100,ttlSeconds=10) for n in queue.node_ids}
                        workers={n:dict(identity=dict(f.worker,nodeId=n),issuerId='worker-issuer',keyId='worker-key',
                                        publicKey=f.proof_key.public_key()) for n in queue.node_ids}
                        worker_wire={n:dict(identity=v['identity'],issuerId=v['issuerId'],keyId=v['keyId'],
                            publicKeyPem=v['publicKey'].public_bytes(serialization.Encoding.PEM,
                                serialization.PublicFormat.SubjectPublicKeyInfo).decode()) for n,v in workers.items()}
                        enrollment_file=secure.root/'scheduler-enrollment.json'
                        enrollment_file.write_text(json.dumps(dict(schemaVersion=1,trustedWorkers=worker_wire,enrollment=enrollment)))
                        enrollment_file.chmod(0o600)
                        if os.geteuid()==0:
                            with self.assertRaisesRegex(ValueError,'unprivileged'):
                                DirectPreparationScheduler.from_protected_file(store,str(enrollment_file))
                            privilege_patch=patch('processing.direct_preparation_scheduler._unprivileged',return_value=True)
                            privilege_patch.start();self.addCleanup(privilege_patch.stop)
                            os.chown(enrollment_file,10001,10001)
                            with self.assertRaises(ValueError):
                                DirectPreparationScheduler.from_protected_file(store,str(enrollment_file))
                            os.chown(enrollment_file,0,0)
                        scheduler=DirectPreparationScheduler.from_protected_file(store,str(enrollment_file))
                        token='synthetic-worker-bearer-with-enough-length'
                        worker_api=ReferenceWorkerAPI(store,None,bearer_workers={token:dict(node_id='cluster',
                            worker_id='worker',worker_instance_id='instance')})
                        policy_api=_DynamicAPI(SimpleNamespace(worker=worker_api,coordinator_queue=queue),object(),None,
                            lambda:dict(nodes=[dict(role='cluster',available=True)]),lambda:{})
                        policy_api._direct_preparation_scheduler=scheduler
                        policy_server=create_reference_http_server(policy_api,'127.0.0.1',0,ssl_context=h.context)
                        policy_thread=threading.Thread(target=policy_server.serve_forever);policy_thread.start()
                        def stop_policy():
                            policy_server.shutdown();policy_server.server_close();policy_thread.join(5)
                        self.addCleanup(stop_policy)
                        policy_origin='https://127.0.0.1:'+str(policy_server.server_port)
                        policy_tls=verified_client_context(str(h.root/'fixture-ca.pem'))
                        policy_tls.load_cert_chain(str(h.good),str(h.goodkey))
                        policy_driver=object.__new__(ReferenceDriver)
                        policy_driver.worker=SimpleNamespace(authority_id=queue.authority_id,node_id='cluster',
                            worker_id='worker',origin=policy_origin)
                        policy_driver.worker_instance_id='instance';policy_driver.origin=policy_origin
                        policy_driver._initial_origin=policy_origin
                        policy_driver._bearer=token;policy_driver._ssl_context=policy_tls
                        policy_driver._open=build_opener(ProxyHandler({}),_NoRedirects(),HTTPSHandler(context=policy_tls)).open
                        policy_driver._initial_open=policy_driver._open
                        driver.direct_source_policy=policy_driver.direct_source_policy
                        with patch('processing.reference_runtime._available_discovery_snapshot',side_effect=lambda value:value), \
                             patch('processing.reference_runtime.discovered_policy',return_value=(opts['capability_fingerprint'],opts['options_fingerprint'],set())):
                            self.assertTrue(policy_driver.direct_source_policy()['directPreparationEnabled'])
                            with queue._db() as db:db.execute("UPDATE nodes SET state='draining' WHERE id='cluster'")
                            self.assertFalse(policy_driver.direct_source_policy()['directPreparationEnabled'])
                            with queue._db() as db:db.execute("UPDATE nodes SET state='active' WHERE id='cluster'")
                            result=scheduler.run_once(capability_fingerprint=opts['capability_fingerprint'],
                                options_fingerprint=opts['options_fingerprint'],allowed_odm_options=set(),revalidate_policy=lambda *_:True)
                            enrollment_file.write_text(enrollment_file.read_text()+' ')
                            self.assertFalse(policy_driver.direct_source_policy()['directPreparationEnabled'])
                        self.assertEqual((result['nodeId'],result['state']),('cluster','receiving'))
                        self.assertFalse(result['sourceCustodyAccepted']);self.assertFalse(result['processingAuthorized'])
                        with NodeGate('fixture-v2') as gate:self.assertEqual(gate.snapshot()['reservation']['job']['id'],f.payload['providerTaskId'])
                    finally:queue.close()
        finally:
            server.shutdown();server.server_close();thread.join(5)
        self.assertFalse(thread.is_alive())
