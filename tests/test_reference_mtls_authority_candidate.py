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
                    function={'init':session.serve_reference_init,'upload':session.serve_reference_upload,
                              'commit':session.serve_reference_commit}[kind]
                    function(channel,witness=self.authority,secret=secret,peer_sha256='0'*64 if mode=='wrong_client_pin' else client_pin,timeout_seconds=3)
                except Exception as exc:errors.append(type(exc).__name__)
            thread=threading.Thread(target=serve);record['thread']=thread;thread.start()
            channel=client_context.wrap_socket(client_raw,server_hostname='localhost')
            record['channel']=channel
            cls={'init':session.RemoteReferenceInitAuthority,'upload':session.RemoteReferenceUploadAuthority,
                 'commit':session.RemoteReferenceCommitAuthority}[kind]
            return cls(channel,authority=self.queue.authority_id,epoch=self.queue.startup_id,
                       peer_sha256='0'*64 if mode=='wrong_server_pin' else server_pin,timeout_seconds=3)
        def close_all():
            for record in resources:
                for key in ('channel','client','server'):
                    if key in record:
                        try:record[key].close()
                        except OSError:pass
            for record in resources:
                if record['thread'] is not None and record['thread'].ident is not None:
                    record['thread'].join(4)
            self.assertFalse(any(record['thread'] is not None and record['thread'].is_alive() for record in resources))
        self.addCleanup(close_all)
        gate_root=self.root/'gate'
        with patch.object(session,'REFERENCE_REMOTE_QUALIFIED',True), patch.object(broker,'QUALIFIED',True), \
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
            with patch.object(adapter,'exchange',return_value={'uuid':grant['jobId'],'success':True},
                              side_effect=lost if mode=='loss' else crash_after_remote_forward if mode=='crash' else None) as commit:
                kwargs=dict(operation_id=commit_id,body=b'',content_type='application/json',
                    commit_signature=broker.sign_reference_commit(grant,commit_id,b'','application/json',secret),manifest=manifest)
                if mode in ('success','crash'):
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
            close_all()
            if mode=='success':self.assertEqual(errors,[])
            with self.queue._db() as db:
                self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(operation,)).fetchone()[0],'complete')
                self.assertEqual(db.execute('SELECT 1 FROM reference_broker_upload_receipt_candidate WHERE operation_id=?',(operation,)).fetchone() is not None,mode!='missing')
                commit_row=db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(commit_id,)).fetchone()
                self.assertEqual(commit_row[0] if commit_row else None,{'success':'complete','loss':'started','missing':None}[mode])
                if mode=='loss':self.assertFalse(self.queue._transfer_admission_clear(db))
            local=service.db.execute('SELECT state FROM operations WHERE id=?',(commit_id,)).fetchone()
            self.assertEqual(local['state'] if local else None,{'success':'complete','loss':'started','missing':None}[mode])

    def test_remote_broker_init_and_upload(self):self._remote_broker_flow('success')
    def test_remote_commit_missing_receipt_holds(self):self._remote_broker_flow('missing')
    def test_remote_commit_lost_response_cannot_replay(self):self._remote_broker_flow('loss')
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
