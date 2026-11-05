"""Three tiny real Viewer V2 attempts across two protected synthetic workers.

ODM execution is a deterministic edge transport; admission, physical gates,
source custody, TLS result delivery, Viewer copy receipts and ingest are real.
This does not qualify photogrammetry, production deployment or publication.
"""
from contextlib import ExitStack, nullcontext
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.request import Request, urlopen

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing.reference_api import Response
from processing.node_gate import NodeGate
from tests import test_viewer_result_v2_roundtrip as fixture


class _ProcessingTransport(fixture._NativeTransport):
    def __init__(self, node, archive):
        super().__init__(lambda: None, archive)
        self.node=node

    def initialize(self, task):
        super().initialize(task)
        self.tasks[task['engine_uuid']]=20

    def read(self, route, **kwargs):
        result=super().read(route, **kwargs)
        if route=='/info':
            result['taskQueueCount']=sum(code==20 for code in self.tasks.values())
            result['maxImages']=1 if self.node=='cluster' else None
        return result

    def output_page(self, job, *, line, limit):
        return ([f'{self.node}: {job}: synthetic processing'][line:line+limit],False)


@unittest.skipUnless(sys.platform.startswith('linux'),'real worker directory fsync requires Linux')
@unittest.skipUnless(os.environ.get('LTDS_VIEWER_WORKTREE'),'actual Viewer checkout required')
class TwoNodeViewerPipelineTests(unittest.TestCase):
    def test_three_production_attempts_concurrent_nodes_delivery_retry_and_independent_viewer_imports(self):
        viewer=Path(os.environ['LTDS_VIEWER_WORKTREE']).resolve()/'apps'/'viewer'
        node=os.environ.get('LTDS_NODE') or shutil.which('node')
        self.assertTrue(node and (viewer/'server'/'sourceIntakeClientV2.js').is_file())
        with tempfile.TemporaryDirectory(prefix='ltds-two-node-viewer-') as temporary, ExitStack() as stack:
            root=Path(temporary);root.chmod(0o700)
            queue=fixture.Queue(root/'queue',node_ids=('cluster','main'),reference_enabled=True)
            stack.callback(queue.close)
            stack.enter_context(patch('processing.node_gate.canonical_gate_path',side_effect=lambda engine:root/('gate-'+engine)))
            stack.enter_context(patch('processing.source_spool.os.path.ismount',return_value=True))
            spool_root=root/'spool';spool_root.mkdir(mode=0o700)
            marker=spool_root/'.ltds-source-spool-id';marker.write_text('c'*64);marker.chmod(0o600)
            spool=fixture.SourceSpool(spool_root,mount_id='c'*64,safety_floor_bytes=0,max_object_bytes=1048576,chunk_bytes=65536)
            store=fixture.ReferenceStore(queue,node_limits={'cluster':{'maxImages':1},'main':{'maxImages':None}})
            key,cert=fixture._write_tls_material(root)
            client_tls=ssl.create_default_context(cafile=str(cert))
            server_tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);server_tls.load_cert_chain(str(cert),str(key))
            source_key,snapshot_key,copy_key=(Ed25519PrivateKey.generate() for _ in range(3))
            source_keys=[dict(environment='production',issuerId='pairing-source-issuer',keyId='pairing-source-key',
                principalId='viewer-production',publicKeyPem=fixture._pem_public(source_key))]
            copy_keys={'pairing-copy-key':dict(issuerId='pairing-copy-issuer',publicKeyPem=fixture._pem_public(copy_key))}
            viewer_bearer=fixture.BEARER
            credentials={role:fixture.WORKER_BEARER+'-'+role for role in queue.node_ids}
            readers={role:fixture.RESULT_BEARER+'-'+role for role in queue.node_ids}
            archive=root/'result.zip'
            environment=os.environ.copy();environment.pop('NODE_PATH',None)
            environment['LTDS_VIEWER_APP_ROOT']=str(viewer)
            built=subprocess.run([node,str(Path(__file__).parent/'fixtures'/'viewer_result_roundtrip_archive.js'),str(archive)],
                cwd=viewer,env=environment,capture_output=True,text=True,timeout=30)
            self.assertEqual(built.returncode,0,built.stderr[-3000:])
            archive_bytes=archive.read_bytes()
            workers={};transports={};origins={};drivers={};servers=[];threads=[]
            engine_edges=stack.enter_context(ExitStack())
            def close_servers():
                for server in reversed(servers):server.shutdown();server.server_close()
                for thread in threads:thread.join(3)
                self.assertTrue(all(not thread.is_alive() for thread in threads),'task-owned TLS thread leaked')
            stack.callback(close_servers)
            def serve(api):
                server=fixture.create_reference_http_server(api,'127.0.0.1',0,ssl_context=server_tls,request_timeout=5)
                thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02})
                thread.start();servers.append(server);threads.append(thread)
                return server,'https://127.0.0.1:'+str(server.server_address[1])
            for role in queue.node_ids:
                owned=root/role;owned.mkdir(mode=0o700);scratch=owned/'scratch';scratch.mkdir(mode=0o700)
                engine_id='paired-engine-'+role
                worker=fixture.ReferenceEngineWorker(owned/'worker.sqlite',
                    ledger=fixture.ScratchLedger(owned/'ledger.sqlite',scratch,capacity_probe=lambda _:10**9,
                        safety_floor_bytes=1024,cleanup_grace_seconds=0),
                    outbox=fixture.WorkerOutbox(owned/'outbox.sqlite'),engine=fixture.NativeEngine('http://127.0.0.1:1'),
                    engine_id=engine_id,node_id=role,authority_id=queue.authority_id,coordinator_origin='https://127.0.0.1:1',
                    gate_generation=1,worker_id='paired-worker-'+role,engine_version='synthetic-engine-1',
                    allowed_odm_options={'numeric-probe'},viewer_receipt_keys=copy_keys,
                    engine_probe=lambda deadline,value=engine_id:value)
                transport=_ProcessingTransport(role,archive_bytes)
                for method in ('initialize','upload','action','read','download','output_page'):
                    engine_edges.enter_context(patch.object(worker.engine,method,side_effect=getattr(transport,method)))
                workers[role]=worker;transports[role]=transport
                result_api=fixture.ReferenceResultAPI(fixture.ReferenceResults(worker),bearer_readers={readers[role]:dict(
                    node_id=role,worker_id=worker.worker_id,environments=('production',),share_policy=lambda _:True)})
                _,origins[role]=serve(result_api)
            proxy=fixture.ResultProxy(origins,worker_bearers=readers,ssl_context=client_tls,admit_read=lambda _:nullcontext())
            api=fixture.ReferenceControlAPI(store,viewer_bearers={viewer_bearer:'production'},
                worker_bearers={credentials[role]:dict(node_id=role,worker_id=workers[role].worker_id,
                    worker_instance_id='paired-instance-'+role) for role in queue.node_ids},
                capability_fingerprint=fixture.CAPABILITY_FINGERPRINT,options_fingerprint=fixture.OPTIONS_FINGERPRINT,
                allowed_odm_options={'numeric-probe'},result_proxy=proxy,viewer_receipt_keys=copy_keys,
                source_spool=spool,source_attestation_keys=source_keys,authority_id=queue.authority_id)
            original_handle=api.handle
            blocked={'cluster':False,'attempts':0};copies={};cluster_delivery_requests=[]
            def faults(method,path,headers,body=b'',**kwargs):
                if (method=='POST' and path=='/ltds/worker/v1/results'
                        and {key.lower():value for key,value in headers.items()}.get('authorization')=='Bearer '+credentials['cluster']):
                    cluster_delivery_requests.append(bytes(body))
                if (method=='POST' and path=='/ltds/worker/v1/results' and blocked['cluster']
                        and {key.lower():value for key,value in headers.items()}.get('authorization')=='Bearer '+credentials['cluster']):
                    blocked['attempts']+=1
                    return Response(503,{'error':'synthetic_delivery_unavailable'})
                response=original_handle(method,path,headers,body,**kwargs)
                if method=='POST' and path.endswith('/acceptance') and response.status==200:
                    identifier=path.split('/')[4]
                    copies.setdefault(identifier,[]).append((bytes(body),response.body))
                return response
            api.handle=faults
            coordinator,origin=serve(api)
            dropped=set();reply=coordinator.RequestHandlerClass._reply
            def lose_owned_reply(handler,response):
                path=handler.path.split('?',1)[0];kind=None;identifier=None
                if response.status in (200,201):
                    if handler.command=='POST' and path=='/ltds/v2/jobs':
                        kind='post';identifier=response.body.get('jobId')
                    elif handler.command=='PUT' and '/source/' in path:kind='put';identifier=path.split('/')[4]
                    elif handler.command=='POST' and path.endswith('/source/complete'):kind='complete';identifier=path.split('/')[4]
                    elif handler.command=='POST' and path.endswith('/acceptance'):kind='acceptance';identifier=path.split('/')[4]
                if kind and (kind,identifier) not in dropped:
                    dropped.add((kind,identifier));handler.close_connection=True
                    try:handler.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:pass
                    handler.connection.close();return
                return reply(handler,response)
            coordinator.RequestHandlerClass._reply=lose_owned_reply
            for role,worker in workers.items():
                worker.origin=origin
                with NodeGate(worker.engine_id) as gate:gate.provision(role,origin,queue.authority_id)
                drivers[role]=fixture.ReferenceDriver(worker,worker.ledger,worker.outbox,
                    fixture.BrokerClient(origin,ssl_context=client_tls),origin,credentials[role],
                    state_path=root/role/'driver.sqlite',ssl_context=client_tls,output_budget=1048576,transfer_ttl=240,
                    safety_floor=1024,failure_outbox=fixture.FailureOutbox(root/role/'failure.sqlite'),
                    worker_instance_id='paired-instance-'+role,engine_cleanup=fixture.EngineCacheCleanup(worker,
                        path=root/role/'engine-cleanup.sqlite',viewer_receipt_keys=copy_keys))
            def refresh():
                store.update_fleet_snapshot({role:dict(ready=True,taskQueueCount=transports[role].read('/info')['taskQueueCount'],
                    maxImages=1 if role=='cluster' else None,maxInputBytes=None) for role in queue.node_ids})
            def run_js(config,name):
                config_path=Path(config['databasePath']).parent/'config.json';config_path.write_text(json.dumps(config));config_path.chmod(0o600)
                env=dict(environment,LTDS_PAIRING_CONFIG=str(config_path),NODE_EXTRA_CA_CERTS=str(cert))
                if os.environ.get('LTDS_VIEWER_NODE_MODULES'):env['NODE_PATH']=os.environ['LTDS_VIEWER_NODE_MODULES']
                result=subprocess.run([node,str(Path(__file__).parent/'fixtures'/name)],cwd=viewer,env=env,
                    capture_output=True,text=True,timeout=90)
                self.assertEqual(result.returncode,0,result.stderr[-4000:])
                return json.loads(result.stdout.strip().splitlines()[-1])
            attempts=[]
            for index in range(3):
                directory=root/('viewer-'+str(index));directory.mkdir(mode=0o700)
                (directory/'photo.jpg').write_bytes(fixture.SOURCE_BYTES)
                config=dict(origin=origin,viewerBearer=viewer_bearer,authorityId=queue.authority_id,
                    environment='production',principalId='viewer-production',databasePath=str(directory/'viewer.sqlite'),
                    sourceFilePath=str(directory/'photo.jpg'),capabilityFingerprint=fixture.CAPABILITY_FINGERPRINT,
                    optionsFingerprint=fixture.OPTIONS_FINGERPRINT,snapshotPrivateKeyPem=fixture._pem_private(snapshot_key),
                    snapshotPublicKeyPem=fixture._pem_public(snapshot_key),attestationPrivateKeyPem=fixture._pem_private(source_key),
                    attestationPublicKeyPem=fixture._pem_public(source_key),copyPrivateKeyPem=fixture._pem_private(copy_key),
                    copyPublicKeyPem=fixture._pem_public(copy_key),resultBytesBase64=__import__('base64').b64encode(archive_bytes).decode(),
                    modelsRoot=str(directory/'models'),datasetMount=str(directory/'datasets'),cacheMount=str(directory/'cache'),
                    trashMount=str(directory/'trash'),ingestOpsBaseUrl='http://operations.test')
                for name in ('datasetMount','cacheMount','trashMount'):Path(config[name]).mkdir()
                run_js(config,'viewer_source_intake_v2_pairing.js')
                state=run_js(config,'viewer_result_roundtrip_setup.js');config.update(state);attempts.append(config)
            identifiers=[item['providerTaskId'] for item in attempts]
            self.assertEqual(len(set(identifiers)),3)
            def advance(role,identifier,stage,seconds=8):
                end=time.monotonic()+seconds;last=None
                while time.monotonic()<end:
                    refresh();last=drivers[role].step()
                    with drivers[role]._db() as db:
                        local=db.execute('SELECT stage FROM driver_jobs WHERE job=?',(identifier,)).fetchone()
                    if local and local['stage']==stage:return last
                    self.assertNotEqual(last['state'],'held',last)
                    time.sleep(.02)
                self.fail(f'{role} failed to reach {stage}: {last}')
            advance('cluster',identifiers[0],'executing')
            advance('main',identifiers[1],'executing')
            self.assertEqual([store.get(job)['nodeId'] for job in identifiers[:2]],['cluster','main'])
            self.assertEqual([store.get(job)['status'] for job in identifiers],['running','running','queued'])
            for role in queue.node_ids:
                with NodeGate(workers[role].engine_id) as gate:self.assertIsNotNone(gate.snapshot()['reservation'])
            self.assertEqual(drivers['cluster']._request('GET','/next')[1],{'job':None,'execution':None})
            self.assertEqual(drivers['main']._request('GET','/next')[1],{'job':None,'execution':None})
            # Status is observed through the same protected client API, including
            # a queued third task with no invented processing node assignment.
            for index,item in enumerate(attempts):
                request=Request(origin+'/ltds/v1/jobs/'+item['providerTaskId'],headers={'Authorization':'Bearer '+viewer_bearer})
                with urlopen(request,context=client_tls,timeout=5) as response:status=json.loads(response.read())
                self.assertEqual(status['jobId'],item['providerTaskId'])
                self.assertEqual((status['status'],status['nodeId']),
                    ('running',queue.node_ids[index]) if index<2 else ('queued',None))
            # Production Viewer log requests traverse coordinator->correct protected worker.
            for role,item in zip(queue.node_ids,attempts):
                request=Request(origin+'/ltds/v1/jobs/'+item['providerTaskId']+'/logs?generation=1&after=0&limit=20',
                    headers={'Authorization':'Bearer '+viewer_bearer})
                with urlopen(request,context=client_tls,timeout=5) as response:page=json.loads(response.read())
                self.assertEqual((page['jobId'],page['attemptId'],page['nodeId'],page['environment']),
                    (item['providerTaskId'],item['attemptId'],role,'production'))
                self.assertIn(role+': '+item['providerTaskId'],page['lines'][0]['message'])
            blocked['cluster']=True;transports['cluster'].tasks[identifiers[0]]=40
            advance('cluster',identifiers[0],'delivering')
            drivers['cluster'].step()
            self.assertGreater(blocked['attempts'],0)
            self.assertEqual(store.get(identifiers[0])['status'],'running')
            self.assertEqual(store.get(identifiers[2])['status'],'queued')
            transports['main'].tasks[identifiers[1]]=40
            advance('main',identifiers[1],'awaiting_acceptance')
            def accept_and_ingest(index,role):
                item=attempts[index]
                ack=run_js(item,'viewer_result_roundtrip_reconcile.js')
                self.assertTrue(ack['accepted'] and ack['resultBytesRetained'] and ack['firstReplyLost'])
                accepted_path=Path(item['modelsRoot'])/item['taskId']/item['attemptId']/'reference-v2-results'/'accepted_objects'/'odm-archive.bin'
                self.assertTrue(accepted_path.is_file(),'this attempt must own its durable Viewer result copy')
                self.assertEqual(hashlib.sha256(accepted_path.read_bytes()).digest(),hashlib.sha256(archive_bytes).digest())
                advance(role,item['providerTaskId'],'completed')
                ingested=run_js(item,'viewer_result_roundtrip_ingest.js')
                self.assertEqual(set(ingested['assetKinds']),{'ept','glb','obj'})
                self.assertEqual(ingested['attemptStatus'],'derivatives')
                self.assertEqual(store.get(item['providerTaskId'])['phase'],'completed')
                self.assertEqual(copies[item['providerTaskId']][0][0],copies[item['providerTaskId']][1][0])
                return ack
            receipts=[accept_and_ingest(1,'main')]
            self.assertEqual(store.get(identifiers[0])['status'],'running','failed delivery must retain its physical node')
            advance('main',identifiers[2],'executing')
            self.assertEqual(store.get(identifiers[2])['nodeId'],'main')
            with NodeGate(workers['cluster'].engine_id) as gate:self.assertIsNotNone(gate.snapshot()['reservation'])
            blocked['cluster']=False
            advance('cluster',identifiers[0],'awaiting_acceptance')
            receipts.append(accept_and_ingest(0,'cluster'))
            transports['main'].tasks[identifiers[2]]=40
            advance('main',identifiers[2],'awaiting_acceptance')
            receipts.append(accept_and_ingest(2,'main'))
            self.assertGreaterEqual(len(cluster_delivery_requests),2)
            self.assertEqual(cluster_delivery_requests[0],cluster_delivery_requests[-1],
                'metadata retry must preserve the exact durable delivery envelope')
            self.assertEqual(len({item['receiptId'] for item in receipts}),3)
            self.assertEqual(len({item['storageReceiptId'] for item in receipts}),3)
            for identifier,role in zip(identifiers,('cluster','main','main')):
                self.assertEqual(transports[role].calls.count(('initialize',identifier)),1)
                self.assertEqual(store.get(identifier)['status'],'succeeded')
            self.assertEqual(len(dropped),12,'each attempt recovered its own three intake and one Viewer ACK losses')
