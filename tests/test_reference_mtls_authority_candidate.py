"""Isolated pinned mTLS reference authority; no broker/daemon integration."""
import hashlib
import json
import os
import shutil
import socket
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

from processing import node_reference_authority_session as session
from processing import node_mutation_broker as broker
from tests.test_reference_broker_authority_candidate import ReferenceBrokerAuthorityCandidateTests
from processing.engine_ingress_guard import BackendPin
from processing.node_gate import NodeGate
from processing.reference_engine_worker import ReferenceEngineWorker


@unittest.skipUnless(shutil.which('openssl'), 'synthetic openssl fixture required')
class ReferenceMTLSCandidateTests(ReferenceBrokerAuthorityCandidateTests):
    REMOTE_FENCE_REQUIRED = True
    def _session(self, *, lose_after_held, prior_grant=None, expire=False):
        name='synthetic-mtls-'+uuid.uuid4().hex
        key,cert = self.root/(name+'.key'),self.root/(name+'.crt')
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=localhost','-keyout',str(key),'-out',str(cert)],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
        pin = hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert.read_text())).hexdigest()
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert,key)
        server_context.load_verify_locations(cafile=str(cert))
        server_context.verify_mode = ssl.CERT_REQUIRED
        client_context = ssl.create_default_context(cafile=str(cert))
        client_context.load_cert_chain(cert,key)
        client_raw,server_raw = socket.socketpair()
        client_raw.settimeout(3);server_raw.settimeout(3)
        errors=[]
        def serve():
            try:
                channel=server_context.wrap_socket(server_raw,server_side=True)
                session.serve_reference_init(channel,witness=self.authority,secret=secret,peer_sha256=pin,timeout_seconds=0.2 if expire else 3)
            except Exception as exc: errors.append(type(exc).__name__)
        thread=threading.Thread(target=serve)
        secret=b'synthetic-signing-key-'*2
        remote=None
        try:
            thread.start()
            channel=client_context.wrap_socket(client_raw,server_hostname='localhost')
            remote=session.RemoteReferenceInitAuthority(channel,authority=self.queue.authority_id,
                epoch=self.queue.startup_id,peer_sha256=pin,timeout_seconds=3)
            grant=prior_grant or self.grant();signature=broker.sign_grant(grant,secret)
            if prior_grant is not None:
                with self.assertRaises(session.SessionHeld):
                    with remote.hold(grant,3,body=b'init',signature=signature,create=True):
                        self.fail('completed init reissued authority')
            elif expire:
                with self.assertRaises((session.SessionHeld,OSError)):
                    with remote.hold(grant,3,body=b'init',signature=signature,create=True):
                        time.sleep(0.4)
                        remote.completed(grant,{'uuid':grant['jobId'],'success':True})
            elif lose_after_held:
                with self.assertRaisesRegex(RuntimeError,'synthetic disconnect'):
                    with remote.hold(grant,3,body=b'init',signature=signature,create=True):
                        raise RuntimeError('synthetic disconnect')
            else:
                with remote.hold(grant,3,body=b'init',signature=signature,create=True):
                    remote.completed(grant,{'uuid':grant['jobId'],'success':True})
        finally:
            client_raw.close();server_raw.close()
            if thread.ident is not None:thread.join(4)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors,['TimeoutError'] if expire else ['SessionHeld'] if lose_after_held or prior_grant is not None else [])
        with self.queue._db() as db:
            state=db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()[0]
            self.assertEqual(state,'started' if lose_after_held or expire else 'complete')
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_broker_completion_candidate WHERE operation_id=?',
                                        (grant['operationId'],)).fetchone()[0],0 if lose_after_held or expire else 1)
        if lose_after_held or expire:
            with self.queue._db() as db:
                self.assertFalse(self.queue._transfer_admission_clear(db))
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute("UPDATE reference_jobs SET phase='held' WHERE job_id=?",(grant['jobId'],))
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute("UPDATE reference_claims SET worker_id='other' WHERE job_id=?",(grant['jobId'],))
        return grant

    def test_mtls_init_success(self):
        with patch.object(session,'REFERENCE_REMOTE_QUALIFIED',True):self._session(lose_after_held=False)

    def test_mtls_disconnect_holds_intent(self):
        with patch.object(session,'REFERENCE_REMOTE_QUALIFIED',True):self._session(lose_after_held=True)

    def test_mtls_completed_init_cannot_reissue(self):
        with patch.object(session,'REFERENCE_REMOTE_QUALIFIED',True):
            grant=self._session(lose_after_held=False)
            self._session(lose_after_held=False,prior_grant=grant)

    def test_mtls_server_expiry_retains_claim_fence(self):
        with patch.object(session,'REFERENCE_REMOTE_QUALIFIED',True):self._session(lose_after_held=False,expire=True)

    def _remote_broker_flow(self,mode):
        cert_root=Path(os.environ['LT_REMOTE_CERT_DIR']) if mode=='crash' else self.root
        server_key,server_cert=cert_root/'server.key',cert_root/'server.crt'
        client_key,client_cert=cert_root/'node.key',cert_root/'node.crt'
        if mode!='crash':
            for key,cert,subject in ((server_key,server_cert,'/CN=localhost'),(client_key,client_cert,'/CN=synthetic-node')):
                subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                    '-subj',subject,'-keyout',str(key),'-out',str(cert)],stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,check=True,timeout=10)
        server_pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(server_cert.read_text())).hexdigest()
        client_pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(client_cert.read_text())).hexdigest()
        server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(server_cert,server_key);server_context.load_verify_locations(cafile=str(client_cert));server_context.verify_mode=ssl.CERT_REQUIRED
        client_context=ssl.create_default_context(cafile=str(server_cert));client_context.load_cert_chain(client_cert,client_key)
        secret=b'synthetic-signing-key-'*2
        resources=[];errors=[]
        def connect(kind):
            client_raw,server_raw=socket.socketpair()
            client_raw.settimeout(3);server_raw.settimeout(3)
            record={'client':client_raw,'server':server_raw,'thread':None}
            resources.append(record)
            def serve():
                try:
                    channel=server_context.wrap_socket(server_raw,server_side=True)
                    record['serverChannel']=channel
                    function={'init':session.serve_reference_init,'upload':session.serve_reference_upload,
                              'commit':session.serve_reference_commit,'proof':session.serve_reference_proof,
                              'reconcile':session.serve_reference_reconcile}[kind]
                    function(channel,witness=self.authority,secret=secret,peer_sha256='0'*64 if mode=='wrong_client_pin' else client_pin,timeout_seconds=10 if mode.startswith('paged_') else 3, **({'paged':True} if mode.startswith('paged_') and kind in ('commit','proof') else {}))
                except Exception as exc:errors.append(type(exc).__name__)
            thread=threading.Thread(target=serve);record['thread']=thread;thread.start()
            channel=client_context.wrap_socket(client_raw,server_hostname='localhost')
            record['channel']=channel
            cls={'init':session.RemoteReferenceInitAuthority,'upload':session.RemoteReferenceUploadAuthority,
                 'commit':session.RemoteReferenceCommitAuthority,'proof':session.RemoteReferenceProofAuthority,
                 'reconcile':session.RemoteReferenceReconcileAuthority}[kind]
            return cls(channel,authority=self.queue.authority_id,epoch=self.queue.startup_id,
                       peer_sha256='0'*64 if mode=='wrong_server_pin' else server_pin,timeout_seconds=10 if mode.startswith('paged_') else 3, **({'paged':True,'page_secret':secret} if mode.startswith('paged_') and kind in ('commit','proof') else {}))
        def close_all():
            for record in resources:
                for key in ('channel','serverChannel','client','server'):
                    if key in record:
                        try:record[key].close()
                        except OSError:pass
            for record in resources:
                if record['thread'] is not None and record['thread'].ident is not None:
                    record['thread'].join(4)
            self.assertFalse(any(record['thread'] is not None and record['thread'].is_alive() for record in resources))
        self.addCleanup(close_all)
        gate_root=self.root/'gate'
        with patch.object(session,'REFERENCE_REMOTE_QUALIFIED',True), patch.object(session,'REFERENCE_PAGING_QUALIFIED',True), patch.object(broker,'QUALIFIED',True), \
             patch('processing.node_gate.canonical_gate_path',return_value=gate_root), \
             patch.object(broker,'canonical_gate_path',return_value=gate_root):
            with NodeGate('synthetic') as gate:gate.provision('cluster','http://127.0.0.1:18080',self.queue.authority_id)
            state=gate_root/'mutation-broker';state.mkdir(mode=0o700)
            if mode=='wrong_server_pin':
                with self.assertRaises(session.SessionHeld):connect('init')
                close_all()
                with self.queue._db() as db:self.assertIsNone(db.execute('SELECT 1 FROM broker_intents').fetchone())
                return
            remote=connect('init')
            adapter=broker.LoopbackNodeODM('synthetic',BackendPin('a'*64,'sha256:'+'b'*64,12345),'')
            service=broker.NodeMutationBroker(state,node_id='cluster',engine_id='synthetic',authority_id=self.queue.authority_id,
                coordinator_epoch=self.queue.startup_id,coordinator_origin='http://127.0.0.1:18080',gate_generation=1,
                secret=secret,adapter=adapter,authority_witness=remote)
            self.addCleanup(service.close)
            if mode.startswith('paged_'):
                from processing.reference_protocol import manifest_sha256
                count=int(mode.split('_')[1])
                with self.queue._db() as db:
                    row=db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?',(self.job['jobId'],)).fetchone()
                    submission=json.loads(row[0]);manifest=submission['manifest'];first=manifest['objects'][0]
                    manifest['objects']=[first]+[dict(first,id='synthetic-%05d'%i,name='synthetic-%05d.jpg'%i) for i in range(1,count)]
                    manifest['objectCount']=count;manifest['totalBytes']=sum(o['byteSize'] for o in manifest['objects'])
                    manifest['inputCounts']['images']=count;manifest['sha256']=manifest_sha256(manifest)
                    submission['manifestSha256']=manifest['sha256'];self.job['manifestSha256']=manifest['sha256']
                    db.execute('UPDATE reference_jobs SET input_json=?,manifest_sha=? WHERE job_id=?',
                               (json.dumps(submission),manifest['sha256'],self.job['jobId']))
            grant=self.grant();signature=broker.sign_grant(grant,secret)
            with patch.object(adapter,'exchange',return_value={'uuid':grant['jobId'],'success':True}) as init:
                if mode=='wrong_client_pin':
                    with self.assertRaises((session.SessionHeld,OSError)):
                        service.mutate(grant,signature,b'init')
                    init.assert_not_called()
                    close_all()
                    with self.queue._db() as db:self.assertIsNone(db.execute('SELECT 1 FROM broker_intents').fetchone())
                    return
                service.mutate(grant,signature,b'init')
                self.assertEqual(init.call_count,1)
            remote_upload=connect('upload')
            operation=str(uuid.uuid4())
            expected=dict(operationId=operation,jobId=grant['jobId'],generation=grant['taskGeneration'],
                manifestSha256=grant['inputManifestSha256'],objectId='image-1',fileName='Survey_001.JPG',
                fileBytes=3,fileSha256=hashlib.sha256(b'abc').hexdigest(),claimTokenSha256=grant['claimTokenSha256'],
                initOperationId=grant['operationId'])
            worker=object.__new__(ReferenceEngineWorker)
            worker.engine_id,worker.node_id,worker.worker_id='synthetic','cluster','worker'
            worker.authority_id,worker.origin,worker.gate_generation=self.queue.authority_id,'http://127.0.0.1:18080',1
            worker.ledger=MagicMock()
            worker.ledger._reservation.return_value=dict(job=grant['jobId'],generation=grant['taskGeneration'],
                environment=grant['environment'],settings=__import__('json').dumps({'manifest_sha256':grant['inputManifestSha256']}))
            path=self.root/'remote-input.jpg';path.write_bytes(b'abc')
            worker._file=MagicMock(return_value=(path,3,expected['fileSha256']))
            def consume(upload,sock,**kwargs):
                sock.settimeout(2);parts=[]
                while data:=sock.recv(1024):parts.append(data)
                self.assertEqual(b''.join(parts),b'abc')
                return {'success':True}
            with patch.object(adapter,'exchange_upload',side_effect=consume) as upload:
                receipt=service.send_remote_reference_upload_candidate(grant,signature,object_id='image-1',operation_id=operation,
                    issue_signature=broker.sign_reference_issue(grant,'image-1',operation,secret),expected_issue=expected,
                    remote=remote_upload,worker=worker,reservation_id='synthetic',relative_path='input.jpg')
                self.assertEqual(upload.call_count,1);self.assertEqual(receipt['state'],'complete')
            if mode.startswith('paged_'):
                manifest=self.store.get(grant['jobId'],private=True)['submission']['manifest']
                scope=hashlib.sha256(broker._json(grant)).hexdigest()
                service.db.execute('BEGIN IMMEDIATE')
                with self.queue._db() as db:
                    for obj in manifest['objects'][1:]:
                        op=str(uuid.uuid4())
                        issue=(op,grant['jobId'],grant['taskGeneration'],grant['inputManifestSha256'],obj['id'],obj['name'],obj['byteSize'],obj['sha256'],grant['claimTokenSha256'],grant['operationId'])
                        digest=hashlib.sha256(broker._json(list(issue))).hexdigest()
                        receipt=dict(objectId=obj['id'],fileName=obj['name'],fileBytes=obj['byteSize'],sha256=obj['sha256'],operationId=op,state='complete',scopeSha256=scope)
                        receipt['signature']=broker._reference_receipt_signature(receipt,secret)
                        db.execute('INSERT INTO reference_broker_upload_issue_candidate VALUES(?,?,?,?,?,?,?,?,?,?)',issue)
                        db.execute("INSERT INTO broker_intents VALUES(?,?,?,'complete')",(op,grant['jobId'],digest))
                        db.execute('INSERT INTO reference_broker_upload_receipt_candidate VALUES(?,?,?)',(op,digest,broker._json(receipt).decode()))
                        service.db.execute("INSERT INTO operations VALUES(?,'upload',?,?,'complete',?)",(op,obj['name'],digest,broker._json({'success':True}).decode()))
                service.db.commit()
            remote_commit=connect('commit')
            commit_id=str(uuid.uuid4())
            manifest=self.store.get(grant['jobId'],private=True)['submission']['manifest']
            if mode=='missing':
                with self.queue._db() as db:
                    db.execute('DELETE FROM reference_broker_upload_receipt_candidate WHERE operation_id=?',(operation,))
            lost=broker.BrokerHeld('synthetic remote commit response lost')
            def crash_after_remote_forward(*args,**kwargs):
                marker=Path(os.environ['LT_REMOTE_CRASH_MARKER'])
                payload=json.dumps(dict(root=str(self.root),queue=str(self.queue.path),broker=str(service.path),
                                        operation=commit_id,epoch=self.queue.startup_id)).encode()
                fd=os.open(marker,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
                try:os.write(fd,payload);os.fsync(fd)
                finally:os.close(fd)
                directory=os.open(marker.parent,os.O_RDONLY|os.O_DIRECTORY)
                try:os.fsync(directory)
                finally:os.close(directory)
                os._exit(77)
            from processing import reference_paged_wire as page_wire
            real_send=page_wire.send_duplex_pages
            def page_send(sock,context,records,**kwargs):
                if mode.endswith('_tamper') and context['operation']=='commit':
                    records=[dict(item) for item in records];records[0]['issueSha256']='0'*64
                return real_send(sock,context,records,**kwargs)
            real_write=page_wire._write
            def page_write(sock,frame,deadline):
                if type(frame) is dict and frame.get('type')=='page':
                    if mode.endswith('_drop'):raise session.SessionHeld('synthetic dropped page')
                    if mode.endswith('_reorder'):frame=dict(frame,sequence=frame['sequence']+1)
                return real_write(sock,frame,deadline)
            negative=mode.endswith(('_tamper','_drop','_reorder'))
            with patch.object(page_wire,'_write',side_effect=page_write), patch.object(page_wire,'send_duplex_pages',side_effect=page_send), \
                 patch.object(session,'MAX_FRAME',128 if mode=='oversize' else session.MAX_FRAME), \
                 patch.object(adapter,'exchange',return_value={'uuid':grant['jobId'],'success':True},
                              side_effect=lost if mode=='loss' else crash_after_remote_forward if mode=='crash' else None) as commit:
                kwargs=dict(operation_id=commit_id,body=b'',content_type='application/json',
                    commit_signature=broker.sign_reference_commit(grant,commit_id,b'','application/json',secret),manifest=manifest)
                if (mode.startswith('paged_') and not negative) or mode in ('success','crash','proof_loss','premature','failure30','failure50'):
                    result=service.commit_remote_reference_candidate(grant,signature,remote=remote_commit,**kwargs)
                    self.assertEqual(result['uuid'],grant['jobId']);self.assertEqual(commit.call_count,1)
                else:
                    with self.assertRaises((session.SessionHeld,broker.BrokerHeld,OSError)):
                        service.commit_remote_reference_candidate(grant,signature,remote=remote_commit,**kwargs)
                    self.assertEqual(commit.call_count,1 if mode=='loss' else 0)
                    resources[-1]['thread'].join(4)
                    self.assertFalse(resources[-1]['thread'].is_alive())
                    if mode=='loss':
                        retry=connect('commit')
                        with self.assertRaises((session.SessionHeld,broker.BrokerHeld,OSError)):
                            service.commit_remote_reference_candidate(grant,signature,remote=retry,**kwargs)
                        self.assertEqual(commit.call_count,1)
            if (mode.startswith('paged_') and not negative) or mode in ('success','proof_loss'):
                proof_remote=connect('proof')
                if mode=='proof_loss':
                    with self.assertRaisesRegex(RuntimeError,'synthetic proof disconnect'):
                        with service.hold_remote_verified_commit_candidate(grant,signature,commit_operation_id=commit_id,
                                delivery=None,remote=proof_remote,manifest=manifest):
                            raise RuntimeError('synthetic proof disconnect')
                    resources[-1]['thread'].join(4);self.assertFalse(resources[-1]['thread'].is_alive())
                else:
                    with service.hold_remote_verified_commit_candidate(grant,signature,commit_operation_id=commit_id,
                            delivery=None,remote=proof_remote,manifest=manifest):
                        other=sqlite3.connect(self.queue.path,timeout=0.05)
                        try:
                            with self.assertRaises(sqlite3.OperationalError):
                                other.execute("UPDATE jobs SET status='held' WHERE id=?",(grant['jobId'],))
                        finally:other.close()
            if mode=='premature':
                from processing import reference_engine_worker as worker_module
                factory=object.__new__(session.RemoteReferenceSessionFactory)
                factory.authority,factory.epoch=self.queue.authority_id,self.queue.startup_id
                factory.open=connect;service.witness=factory
                worker.path=self.root/'remote-worker.sqlite3';worker.engine=MagicMock()
                worker.engine.read.return_value={'uuid':grant['jobId'],'status':{'code':10}}
                identity=dict(job=self.store.get(grant['jobId'],private=True),remoteBrokerProof=dict(
                    initGrant=grant,initSignature=signature,commitOperationId=commit_id))
                with sqlite3.connect(worker.path) as worker_db:
                    worker_db.execute('CREATE TABLE executions (job TEXT PRIMARY KEY,identity TEXT,receipt TEXT,phase TEXT,outbox_id TEXT,result TEXT)')
                    worker_db.execute('INSERT INTO executions(job,identity,receipt,phase) VALUES(?,?,?,?)',(grant['jobId'],json.dumps(identity),
                        uuid.UUID(grant['jobId']).hex,'remote_processing_held'))
                with patch.object(worker_module,'BROKER_WORKER_CANDIDATE_QUALIFIED',True):
                    before=len(resources)
                    with self.assertRaisesRegex(ValueError,'not ready to poll'):
                        worker.poll(grant['jobId'],broker=service,factory=factory)
                    self.assertEqual(len(resources),before)
                    self.assertEqual(worker.resume_remote_broker_candidate(grant['jobId'],broker=service,factory=factory),'remote_processing')
                    self.assertIsNone(worker.poll(grant['jobId'],broker=service,factory=factory))
                    self.assertEqual(worker.engine.read.call_count,1)
                    from processing import reference_protocol as protocol
                    from processing.reference_delivery import DeliveryReceiver
                    result=dict(schemaVersion=1,jobId=grant['jobId'],generation=grant['taskGeneration'],environment=grant['environment'],
                        pipelineDigest=grant['pipelineDigest'],engineVersions={'odm':'synthetic'},
                        objects=[dict(id='odm-archive',role='odm_archive',byteSize=1,sha256=hashlib.sha256(b'x').hexdigest())])
                    result['resultManifestSha256']=protocol.digest(result)
                    binding=dict(jobId=grant['jobId'],generation=grant['taskGeneration'],environment=grant['environment'],
                        inputManifestSha256=grant['inputManifestSha256'],pipelineDigest=grant['pipelineDigest'],
                        resultManifestSha256=result['resultManifestSha256'])
                    payload=dict(**binding,resultManifest=result,terminalState='succeeded');outbox_id=protocol.digest(binding)
                    with worker._db() as worker_db:
                        worker_db.execute("UPDATE executions SET phase='awaiting_delivery',outbox_id=?,result=? WHERE job=?",
                                          (outbox_id,json.dumps(result),grant['jobId']))
                    worker.outbox=MagicMock()
                    worker.outbox.get.return_value={'id':outbox_id,'payload':payload,'state':'pending'}
                    with NodeGate('synthetic') as gate:
                        gate.prepare_completion(uuid.UUID(grant['jobId']).hex,dict(status='succeeded',result={'resultManifestSha256':result['resultManifestSha256']}))
                    before=len(resources)
                    self.assertEqual(worker.poll(grant['jobId'],broker=service,factory=factory),outbox_id)
                    self.assertEqual(len(resources),before)
                    envelope=dict(outboxId=outbox_id,payloadSha256=protocol.digest(payload),payload=payload)
                    receiver=DeliveryReceiver(self.store)
                    receipt=receiver.receive(envelope,node_id='cluster',worker_id='worker',worker_instance_id='instance',
                        claim_request_id='claim-1',claim_token=self.claim['claimToken'])
                    self.assertTrue(receipt['durable'])
                    worker.outbox.get.return_value={'id':outbox_id,'payload':payload,'state':'acknowledged'}
                    worker.acknowledge_delivery(grant['jobId'],broker=service,factory=factory)
                    with NodeGate('synthetic') as gate:self.assertIsNone(gate.snapshot()['reservation'])
            if mode in ('failure30','failure50'):
                from processing.reference_failure import FailureOutbox, FailureReceiver, make_failure
                from processing.reference_driver import ReferenceDriver
                from types import SimpleNamespace
                failure=make_failure(job_id=grant['jobId'],generation=grant['taskGeneration'],environment=grant['environment'],
                    input_manifest_sha256=grant['inputManifestSha256'],pipeline_digest=grant['pipelineDigest'],
                    terminal_state='failed' if mode=='failure30' else 'cancelled',
                    error_code='engine_failed' if mode=='failure30' else 'engine_cancelled')
                failure_outbox=FailureOutbox(self.root/'remote-failure.sqlite3')
                failure_id=failure_outbox.persist(failure,engine_terminal=True)
                completion={'status':'failed','result':{k:failure[k] for k in ('failureSha256','terminalState','errorCode')}}
                proof_remote=connect('proof')
                with service.hold_remote_verified_commit_candidate(grant,signature,commit_operation_id=commit_id,
                        delivery=None,remote=proof_remote,manifest=manifest) as gate:
                    gate.prepare_completion(uuid.UUID(grant['jobId']).hex,completion)
                attempt=failure_outbox.claim_next();receiver=FailureReceiver(self.store)
                envelope={k:attempt[k] for k in ('outboxId','payloadSha256','payload')}
                durable=receiver.receive(envelope,node_id='cluster',worker_id='worker',worker_instance_id='instance',
                    claim_request_id='claim-1',claim_token=self.claim['claimToken'])
                failure_outbox.acknowledge(failure_id,durable)
                delivery=['failure',failure_id,failure['failureSha256'],failure['terminalState'],failure['errorCode']]
                release_remote=connect('proof')
                with service.hold_remote_verified_commit_candidate(grant,signature,commit_operation_id=commit_id,
                        delivery=delivery,remote=release_remote,manifest=manifest) as gate:
                    gate.acknowledge(uuid.UUID(grant['jobId']).hex,dict(id=grant['jobId'],workerId='worker',nodeId='cluster',
                        authorityId=self.queue.authority_id,**completion))
                with NodeGate('synthetic') as gate:self.assertIsNone(gate.snapshot()['reservation'])
                factory=object.__new__(session.RemoteReferenceSessionFactory)
                factory.authority,factory.epoch=self.queue.authority_id,self.queue.startup_id;factory.open=connect
                service.witness=factory
                worker._row=MagicMock(return_value={'phase':'failure_delivered','identity':json.dumps({'remoteBrokerProof':
                    dict(initGrant=grant,initSignature=signature,commitOperationId=commit_id)})})
                driver=object.__new__(ReferenceDriver);driver.remote_reference=SimpleNamespace(factory=factory,broker=service)
                driver.worker=worker;driver.failure_outbox=failure_outbox;driver._kind=MagicMock(return_value='odm')
                driver._adapter=MagicMock();driver._save=MagicMock()
                data={'failure':failure,'failureOutboxId':failure_id}
                with self.queue._db() as db:before=db.execute('SELECT COUNT(*) FROM broker_intents').fetchone()[0]
                local_proof=service.db.execute("SELECT * FROM operations WHERE operation='proof' ORDER BY rowid DESC LIMIT 1").fetchone()
                self.assertEqual(local_proof['state'],'complete')
                def reject_reconcile(candidate_grant,candidate_signature,candidate_delivery):
                    rejected=connect('reconcile')
                    try:
                        with self.assertRaises((session.SessionHeld,broker.BrokerHeld,OSError)):
                            with service.hold_remote_reconciled_failure_candidate(candidate_grant,candidate_signature,
                                    commit_operation_id=commit_id,delivery=candidate_delivery,remote=rejected):
                                self.fail('invalid failure proof crossed reconciliation')
                    finally:rejected.sock.close()
                    resources[-1]['thread'].join(4);self.assertFalse(resources[-1]['thread'].is_alive())
                    with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM broker_intents').fetchone()[0],before)
                reject_reconcile(grant,signature,['failure',failure_id,'0'*64,failure['terminalState'],failure['errorCode']])
                stale=dict(grant,coordinatorEpoch=str(uuid.uuid4()))
                reject_reconcile(stale,broker.sign_grant(stale,secret),delivery)
                service.db.execute("UPDATE operations SET state='started' WHERE id=?",(local_proof['id'],));service.db.commit()
                reject_reconcile(grant,signature,delivery)
                service.db.execute("UPDATE operations SET state='complete' WHERE id=?",(local_proof['id'],));service.db.commit()
                service.db.execute('DELETE FROM operations WHERE id=?',(local_proof['id'],));service.db.commit()
                reject_reconcile(grant,signature,delivery)
                service.db.execute('INSERT INTO operations VALUES(?,?,?,?,?,?)',tuple(local_proof));service.db.commit()
                with self.queue._db() as db:
                    db.execute("UPDATE broker_intents SET state='started' WHERE operation_id=?",(local_proof['id'],))
                reject_reconcile(grant,signature,delivery)
                with self.queue._db() as db:
                    db.execute("UPDATE broker_intents SET state='complete' WHERE operation_id=?",(local_proof['id'],))
                bad_gate=MagicMock();bad_gate.__enter__.return_value=bad_gate
                bad_gate.snapshot.return_value={'reservation':None,'lastAcknowledgment':None,'nodeId':'cluster',
                    'authorityId':self.queue.authority_id,'coordinatorOrigin':'http://127.0.0.1:18080','generation':1}
                with patch.object(broker,'NodeGate',return_value=bad_gate):
                    reject_reconcile(grant,signature,delivery)
                bad_gate.acknowledge.assert_not_called()
                # Lose the final read-only confirmation: checkpoint must not advance.
                original_write=session._write
                dropped=[]
                def drop_final(sock,value,deadline):
                    if sock.server_side and value.get('state')=='complete':
                        dropped.append(True)
                        raise OSError('synthetic lost reconcile ack')
                    return original_write(sock,value,deadline)
                with patch.object(session,'_write',side_effect=drop_final):
                    with self.assertRaises((session.SessionHeld,OSError)):
                        driver._deliver_failure(grant['jobId'],'claim-1',data)
                driver._save.assert_not_called()
                self.assertEqual(dropped,[True])
                self.assertEqual(commit.call_count,1)
                result=driver._deliver_failure(grant['jobId'],'claim-1',data)
                self.assertEqual(result['state'],'awaiting_failure_acceptance')
                driver._save.assert_called_once_with(grant['jobId'],'awaiting_failure_acceptance',data)
                self.assertEqual(commit.call_count,1)
                with self.queue._db() as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM broker_intents').fetchone()[0],before)
            close_all()
            if (mode.startswith('paged_') and not negative) or mode in ('success','premature'):self.assertEqual(errors,[])
            with self.queue._db() as db:
                self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(operation,)).fetchone()[0],'complete')
                self.assertEqual(db.execute('SELECT 1 FROM reference_broker_upload_receipt_candidate WHERE operation_id=?',(operation,)).fetchone() is not None,mode!='missing')
                commit_row=db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(commit_id,)).fetchone()
                self.assertEqual(commit_row[0] if commit_row else None,{'paged_1000_tamper':'started','paged_1000_drop':'started','paged_1000_reorder':'started','paged_1000':'complete','paged_10000':'complete','success':'complete','premature':'complete','failure30':'complete','failure50':'complete','proof_loss':'complete','loss':'started','missing':None,'oversize':None}[mode])
                if negative or mode in ('loss','proof_loss'):self.assertFalse(self.queue._transfer_admission_clear(db))
            local=service.db.execute('SELECT state FROM operations WHERE id=?',(commit_id,)).fetchone()
            self.assertEqual(local['state'] if local else None,{'paged_1000_tamper':None,'paged_1000_drop':None,'paged_1000_reorder':None,'paged_1000':'complete','paged_10000':'complete','success':'complete','premature':'complete','failure30':'complete','failure50':'complete','proof_loss':'complete','loss':'started','missing':None,'oversize':None}[mode])
            if (mode.startswith('paged_') and not negative) or mode in ('success','proof_loss'):
                local_proof=service.db.execute("SELECT state FROM operations WHERE operation='proof'").fetchone()
                self.assertEqual(local_proof['state'],'started' if mode=='proof_loss' else 'complete')

    def test_remote_paged_seeded_1000_drop(self):self._remote_broker_flow('paged_1000_drop')
    def test_remote_paged_seeded_1000_reorder(self):self._remote_broker_flow('paged_1000_reorder')
    def test_remote_paged_seeded_1000_tamper(self):self._remote_broker_flow('paged_1000_tamper')
    def test_remote_paged_seeded_1000(self):self._remote_broker_flow('paged_1000')
    def test_remote_paged_seeded_10000(self):self._remote_broker_flow('paged_10000')
    def test_remote_broker_init_and_upload(self):self._remote_broker_flow('success')
    def test_remote_commit_missing_receipt_holds(self):self._remote_broker_flow('missing')
    def test_remote_commit_lost_response_cannot_replay(self):self._remote_broker_flow('loss')
    def test_remote_commit_oversize_holds_before_intent(self):self._remote_broker_flow('oversize')
    def test_remote_result_proof_disconnect_holds(self):self._remote_broker_flow('proof_loss')
    def test_remote_premature_poll_does_not_strand_proof(self):self._remote_broker_flow('premature')
    def test_remote_failure30_release_and_lost_reconcile_ack(self):self._remote_broker_flow('failure30')
    def test_remote_failure50_release_and_lost_reconcile_ack(self):self._remote_broker_flow('failure50')
    def test_remote_rejects_wrong_server_pin(self):self._remote_broker_flow('wrong_server_pin')
    def test_remote_rejects_wrong_client_pin(self):self._remote_broker_flow('wrong_client_pin')

    def test_remote_commit_mocked_forward_process_crash(self):
        marker_name=os.environ.get('LT_REMOTE_CRASH_MARKER')
        if marker_name and sys.argv==['-c','--lt-remote-crash-child']:
            self._remote_broker_flow('crash')
            self.fail('synthetic forward did not terminate child')
        root=Path(tempfile.mkdtemp(prefix='lt-remote-crash-parent-')).resolve()
        print('retained synthetic remote crash root:',root,flush=True)
        marker=root/'accepted.json'
        for name,subject in (('server','/CN=localhost'),('node','/CN=synthetic-node')):
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                '-subj',subject,'-keyout',str(root/(name+'.key')),'-out',str(root/(name+'.crt'))],
                stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
        code="import unittest; suite=unittest.defaultTestLoader.loadTestsFromName('tests.test_reference_mtls_authority_candidate.ReferenceMTLSCandidateTests.test_remote_commit_mocked_forward_process_crash'); result=unittest.TextTestRunner().run(suite); raise SystemExit(not result.wasSuccessful())"
        child=subprocess.Popen([sys.executable,'-c',code,'--lt-remote-crash-child'],stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT,env=dict(os.environ,LT_REMOTE_CRASH_MARKER=str(marker),LT_REMOTE_CERT_DIR=str(root)))
        try:
            try:output,_=child.communicate(timeout=20)
            except subprocess.TimeoutExpired:self.fail('owned remote crash child timed out')
        finally:
            if child.poll() is None:
                child.kill();child.communicate(timeout=3)
        self.assertEqual(child.returncode,77,output.decode(errors='replace')[-2000:])
        details=json.loads(marker.read_text());owner=Path(details['root']).resolve()
        queue_path,local_path=Path(details['queue']),Path(details['broker'])
        self.assertTrue(queue_path.resolve().is_relative_to(owner));self.assertTrue(local_path.resolve().is_relative_to(owner))
        from processing.broker_intent_fence import clear
        from processing.node_reference_remote_fence import attest
        coordinator=sqlite3.connect('file:'+str(queue_path)+'?mode=ro',uri=True)
        local=sqlite3.connect('file:'+str(local_path)+'?mode=ro',uri=True)
        try:
            attest(coordinator);self.assertFalse(clear(coordinator))
            self.assertEqual(coordinator.execute('SELECT state FROM broker_intents WHERE operation_id=?',(details['operation'],)).fetchone()[0],'started')
            self.assertEqual(local.execute('SELECT state FROM operations WHERE id=?',(details['operation'],)).fetchone()[0],'started')
        finally:coordinator.close();local.close()
        restarted=__import__('processing.prototype',fromlist=['Queue']).Queue(owner/'queue')
        try:
            self.assertNotEqual(restarted.startup_id,details['epoch'])
            with restarted._db() as db:
                attest(db);self.assertFalse(restarted._transfer_admission_clear(db))
                self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(details['operation'],)).fetchone()[0],'started')
        finally:restarted.close()

    def test_real_factory_tcp_mtls_dispatch_init(self):
        server_key,server_cert=self.root/'factory-server.key',self.root/'factory-server.crt'
        node_key,node_cert=self.root/'factory-node.key',self.root/'factory-node.crt'
        for key,cert,subject in ((server_key,server_cert,'/CN=localhost'),(node_key,node_cert,'/CN=synthetic-node')):
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                '-subj',subject,'-addext','subjectAltName=IP:127.0.0.1','-keyout',str(key),'-out',str(cert)],
                stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
        server_pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(server_cert.read_text())).hexdigest()
        node_pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(node_cert.read_text())).hexdigest()
        server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(server_cert,server_key);server_context.load_verify_locations(cafile=str(node_cert));server_context.verify_mode=ssl.CERT_REQUIRED
        listener=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.addCleanup(listener.close)
        listener.bind(('127.0.0.1',0));listener.listen(2);listener.settimeout(3)
        resources={'channel':None};errors=[];secret=b'synthetic-signing-key-'*2
        def serve():
            try:
                for _ in range(2):
                    raw,_=listener.accept();raw.settimeout(3)
                    channel=server_context.wrap_socket(raw,server_side=True);resources['channel']=channel
                    session.serve_reference_operation(channel,witness=self.authority,secret=secret,peer_sha256=node_pin,timeout_seconds=3)
            except Exception as exc:errors.append(type(exc).__name__)
        thread=threading.Thread(target=serve)
        try:
            with self.assertRaises(session.SessionHeld):
                session.RemoteReferenceSessionFactory(host='127.0.0.1',port=listener.getsockname()[1],
                    authority=self.queue.authority_id,epoch=self.queue.startup_id,peer_sha256=server_pin,
                    ca_file=server_cert,cert_file=node_cert,key_file=node_key,timeout_seconds=3)
            from processing import node_reference_plan as plans
            with patch.object(session,'REFERENCE_REMOTE_QUALIFIED',True), patch.object(plans,'REFERENCE_PLAN_QUALIFIED',True):
                thread.start()
                factory=session.RemoteReferenceSessionFactory(host='127.0.0.1',port=listener.getsockname()[1],
                    authority=self.queue.authority_id,epoch=self.queue.startup_id,peer_sha256=server_pin,
                    ca_file=server_cert,cert_file=node_cert,key_file=node_key,timeout_seconds=3)
                plan_client=factory.open('plan')
                plan=plan_client.fetch(job_id=self.job['jobId'],claim_token=self.claim['claimToken'],worker_id='worker',
                    worker_instance_id='instance',claim_request_id='claim-1')
                self.assertEqual(plan['initGrant']['engineId'],'synthetic')
                self.assertEqual(plan['initGrant']['coordinatorEpoch'],self.queue.startup_id)
                remote=factory.open('init')
                grant=self.grant();signature=broker.sign_grant(grant,secret)
                with remote.hold(grant,3,body=b'init',signature=signature,create=True):
                    remote.completed(grant,{'uuid':grant['jobId'],'success':True})
        finally:
            if resources['channel'] is not None:
                try:resources['channel'].close()
                except OSError:pass
            listener.close()
            if thread.ident is not None:thread.join(4)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()[0],'complete')

    def test_claim_bound_plan_is_deterministic_and_rejects_changed_claim(self):
        from processing import node_reference_plan as plans
        job=self.store.get(self.job['jobId'],private=True)
        secret=b'synthetic-signing-key-'*2
        args=dict(claim_token=self.claim['claimToken'],worker_id='worker',worker_instance_id='instance',
                  claim_request_id='claim-1',secret=secret)
        with self.assertRaisesRegex(ValueError,'not qualified'):
            plans.issue_reference_plan(self.authority,job,**args)
        with patch.object(plans,'REFERENCE_PLAN_QUALIFIED',True):
            first=plans.issue_reference_plan(self.authority,job,**args)
            self.assertEqual((first['initGrant']['engineId'],first['initGrant']['gateGeneration']),('synthetic',1))
            with self.assertRaises(TypeError):
                plans.issue_reference_plan(self.authority,job,engine_id='forged',**args)
            self.assertEqual(first,plans.issue_reference_plan(self.authority,job,**args))
            with self.assertRaises(broker.BrokerHeld):
                plans.issue_reference_plan(self.authority,job,**dict(args,claim_token='not-the-claim'))
            changed=json.loads(json.dumps(job));changed['submission']['pipeline']['odm']['options']['projection']='changed'
            with self.assertRaisesRegex(ValueError,'submission changed'):
                plans.issue_reference_plan(self.authority,changed,**args)
            with patch.object(plans.protocol,'MAX_BODY_BYTES',64):
                with self.assertRaisesRegex(ValueError,'wire bound'):
                    plans.issue_reference_plan(self.authority,job,**args)

    def test_daemon_remote_opt_in_never_falls_back(self):
        from ops import reference_worker_daemon as daemon
        self.assertIsNone(daemon._reference_remote_broker_policy({}))
        for value in (None,False,{}, {'enabled':True}):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError,'not qualified'):
                daemon._reference_remote_broker_policy({'referenceRemoteBroker':value})
        cert=self.root/'dummy-cert.pem';cert.write_text('synthetic fixture',encoding='utf-8');cert.chmod(0o600)
        config=dict(schemaVersion=1,stateRoot=str(self.root/'state'),scratchRoot=str(self.root/'scratch'),
            nodeId='cluster',workerId='worker',workerInstanceId='instance',authorityId=self.queue.authority_id,
            engineId='synthetic',gateGeneration=1,gateCoordinatorOrigin='http://127.0.0.1:18080',
            coordinatorOrigin='https://coordinator.invalid',brokerOrigin='https://broker.invalid',engineOrigin='http://127.0.0.1:3000',
            engineToken='',workerBearer='synthetic-private-worker-bearer-0001',resultReadBearer='synthetic-private-reader-bearer-0002',
            caFile=str(cert),serverCertFile=str(cert),serverKeyFile=str(cert),listenHost='127.0.0.1',listenPort=9445,
            outputBudget=4096,safetyFloor=0,transferTtl=600,cleanupGrace=3600,pollSeconds=1,
            engineVersion='synthetic',shareResults=True)
        file=self.root/'remote-policy.json'
        file.write_text(json.dumps(config),encoding='utf-8');file.chmod(0o600)
        self.assertEqual(daemon.load_config(str(file))['nodeId'],'cluster')
        for value in (False,True):
            config['referenceRemoteBroker']=value
            file.write_text(json.dumps(config),encoding='utf-8');file.chmod(0o600)
            with patch.object(daemon,'NativeEngine') as engine, \
                 patch.object(daemon,'_reference_remote_broker_policy',wraps=daemon._reference_remote_broker_policy) as policy:
                with self.assertRaisesRegex(ValueError,'invalid private worker configuration'):
                    daemon.load_config(str(file))
                policy.assert_called_once()
                policy.reset_mock()
                with self.assertRaisesRegex(ValueError,'not qualified'):
                    daemon._build_worker(config,MagicMock(),{})
                policy.assert_called_once()
                engine.assert_not_called()
        from processing import reference_engine_worker as worker_module
        config['referenceRemoteBroker']=dict(host='127.0.0.1',port=12345,epoch=self.queue.startup_id,peerSha256='a'*64,
            caFile=str(cert),clientCertFile=str(cert),clientKeyFile=str(cert),nodeMutationSecretHex='12'*32,
            backendContainerId='b'*64,backendImageId='sha256:'+'c'*64,backendPort=3000)
        with patch.object(daemon,'REFERENCE_REMOTE_DAEMON_QUALIFIED',True), patch.object(broker,'QUALIFIED',True), \
             patch.object(session,'REFERENCE_REMOTE_QUALIFIED',True), patch.object(worker_module,'BROKER_WORKER_CANDIDATE_QUALIFIED',True):
            for field in ('engineCleanupEnabled','cleanupEnabled'):
                with self.subTest(field=field), self.assertRaisesRegex(ValueError,'cleanup is not qualified'):
                    daemon._reference_remote_broker_policy(dict(config,**{field:True}))
