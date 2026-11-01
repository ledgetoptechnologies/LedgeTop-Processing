"""Synthetic mTLS stream with actual Queue, lease and node broker."""
import hashlib
import shutil
import socket
import sqlite3
import ssl
import subprocess
import threading
import time
import unittest
from unittest.mock import Mock

from processing import node_authority_session as authority_session
from processing.node_authority_session import RemoteAuthoritySession, SessionHeld, serve_coordinator
from processing.node_mutation_broker import BrokerHeld, LocalCoordinatorAuthority, NodeMutationBroker, issue_init_grant, issue_upload_grant, sign_grant
from processing.node_gate import NodeGate
from processing.prototype import Queue, VIEWER_PROFILE
from processing.viewer_tasks import TaskStore
from processing.remote_upload_transport import RemoteUploadSender, UploadEvidence, receive_upload
from processing.durable_upload_evidence import DurableUploadEvidence, EvidenceError
from tests.test_node_mutation_broker import NodeMutationBrokerTests


@unittest.skipUnless(shutil.which('openssl'), 'synthetic openssl fixture required')
class NodeAuthoritySessionTests(NodeMutationBrokerTests):
    def test_invalid_server_binding_closes_owned_socket(self):
        channel=Mock()
        with self.assertRaises(SessionHeld):
            serve_coordinator(channel,queue=self.owner_queue,witness=None,secret=self.secret,peer_sha256='0'*64)
        channel.close.assert_called_once_with()

    def test_session_challenge_refuses_missing_authority_marker(self):
        task=TaskStore(self.owner_queue,self.root/'owner-queue'/'viewer-tasks').task(self.task)
        grant=self.grant()
        kwargs=dict(job_id=self.job,claim_token=task['claim_token'],engine_id=self.engine,
                    gate_generation=1,operation_id=grant['operationId'],body=self.body,
                    content_type=grant['contentType'],secret=self.secret,_authority_required=True)
        issue_init_grant(self.owner_queue,**kwargs)
        with self.owner_queue._db() as db:db.execute('DROP TABLE coordinator_authority_lease')
        with self.assertRaises(BrokerHeld):issue_init_grant(self.owner_queue,**kwargs)

    def test_lost_completion_ack_leaves_both_complete_and_fresh_session_replays(self):
        key=self.root/'lost-ack.key';cert=self.root/'lost-ack.crt'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=localhost','-keyout',str(key),'-out',str(cert)],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
        pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert.read_text())).hexdigest()
        server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert,key);server_context.load_verify_locations(cafile=str(cert));server_context.verify_mode=ssl.CERT_REQUIRED
        client_context=ssl.create_default_context(cafile=str(cert));client_context.load_cert_chain(cert,key)
        resources=[]; errors=[]
        def open_session():
            client_raw,server_raw=socket.socketpair();client_raw.settimeout(3);server_raw.settimeout(3)
            record={'client_raw':client_raw,'server_raw':server_raw,'remote':None,'thread':None}
            resources.append(record)
            def serve():
                try:
                    channel=server_context.wrap_socket(server_raw,server_side=True)
                    serve_coordinator(channel,queue=self.owner_queue,witness=LocalCoordinatorAuthority(self.owner_queue),secret=self.secret,peer_sha256=pin,session_seconds=20,idle_seconds=12)
                except Exception as exc:errors.append(type(exc).__name__)
            thread=threading.Thread(target=serve);record['thread']=thread;thread.start()
            client=client_context.wrap_socket(client_raw,server_hostname='localhost')
            remote=RemoteAuthoritySession(client,authority=self.authority,epoch=self.epoch,claim_token=self.owner_claim['claimToken'],peer_sha256=pin,timeout_seconds=3)
            record['remote']=remote
            return remote
        def cleanup():
            for record in resources:
                if record['remote'] is not None:record['remote'].close()
                else:record['client_raw'].close()
                record['server_raw'].close()
                if record['thread'] is not None and record['thread'].ident is not None:
                    record['thread'].join(5);self.assertFalse(record['thread'].is_alive())
        self.addCleanup(cleanup)
        first=open_session();self.broker.close()
        broker=NodeMutationBroker(self.root/'gate'/'mutation-broker',**dict(self.config,authority_witness=first));self.addCleanup(broker.close)
        grant=self.grant();signature=sign_grant(grant,self.secret)
        original=authority_session._write;lost=[]
        def drop_ack(sock,value,deadline):
            if sock.server_side and value.get('sequence')==3:
                lost.append(True);sock.shutdown(socket.SHUT_RDWR);raise OSError('synthetic lost completion acknowledgement')
            return original(sock,value,deadline)
        with unittest.mock.patch.object(authority_session,'_write',side_effect=drop_ack):
            with self.assertRaises(SessionHeld):broker.mutate(grant,signature,self.body)
        self.assertEqual(lost,[True]);self.assertEqual(len(self.calls),1)
        self.assertEqual(broker.db.execute('SELECT state FROM operations').fetchone()[0],'complete')
        with self.owner_queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()[0],'complete')
        broker.close();first.close();resources[0]['thread'].join(5);self.assertFalse(resources[0]['thread'].is_alive())
        second=open_session()
        replay=NodeMutationBroker(self.root/'gate'/'mutation-broker',**dict(self.config,authority_witness=second));self.addCleanup(replay.close)
        self.assertEqual(replay.mutate(grant,signature,self.body)['uuid'],self.task)
        self.assertEqual(len(self.calls),1)
        self.assertEqual(errors,['OSError'])

    def test_mtls_upload_uses_coordinator_descriptor_and_node_broker(self):
        self.begin_upload()
        upload,signature=issue_upload_grant(self.owner_queue,store=self.owner_store,job_id=self.job,
            claim_token=self.owner_claim['claimToken'],engine_id=self.engine,gate_generation=1,
            operation_id=str(__import__('uuid').uuid4()),file_key='synthetic.jpg',secret=self.secret)
        key=self.root/'data.key';cert=self.root/'data.crt'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=localhost','-keyout',str(key),'-out',str(cert)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
        pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert.read_text())).hexdigest()
        server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert,key);server_context.load_verify_locations(cafile=str(cert));server_context.verify_mode=ssl.CERT_REQUIRED
        client_context=ssl.create_default_context(cafile=str(cert));client_context.load_cert_chain(cert,key)
        authority_client,authority_server=socket.socketpair();data_client,data_server=socket.socketpair()
        for channel in (authority_client,authority_server,data_client,data_server):channel.settimeout(3)
        evidence=DurableUploadEvidence(self.owner_queue);errors=[];results=[];threads=[];resources=[]
        def auth():
            try:
                channel=server_context.wrap_socket(authority_server,server_side=True);resources.append(channel)
                serve_coordinator(channel,queue=self.owner_queue,witness=LocalCoordinatorAuthority(self.owner_queue),secret=self.secret,
                    store=self.owner_store,upload_evidence=evidence,peer_sha256=pin,session_seconds=20,idle_seconds=12)
            except Exception as error:errors.append(('authority',type(error).__name__))
        authority_thread=threading.Thread(target=auth);threads.append(authority_thread);authority_thread.start()
        remote=None;broker=None
        try:
            auth_tls=client_context.wrap_socket(authority_client,server_hostname='localhost');resources.append(auth_tls)
            remote=RemoteAuthoritySession(auth_tls,authority=self.authority,epoch=self.epoch,
                claim_token=self.owner_claim['claimToken'],peer_sha256=pin,timeout_seconds=3)
            self.broker.close();broker=NodeMutationBroker(self.root/'gate'/'mutation-broker',**dict(self.config,authority_witness=remote))
            def node():
                try:
                    channel=server_context.wrap_socket(data_server,server_side=True);resources.append(channel)
                    results.append(receive_upload(channel,peer_sha256=pin,broker=broker,timeout=8))
                except Exception as error:errors.append(('node',type(error).__name__))
            node_thread=threading.Thread(target=node);threads.append(node_thread);node_thread.start()
            data_tls=client_context.wrap_socket(data_client,server_hostname='localhost');resources.append(data_tls)
            sender=RemoteUploadSender()
            self.assertEqual(sender.send(data_tls,peer_sha256=pin,store=self.owner_store,claim_token=self.owner_claim['claimToken'],
                grant=upload,signature=signature,secret=self.secret,evidence=evidence,timeout=8),{'success':True})
            node_thread.join(5);self.assertFalse(node_thread.is_alive());self.assertEqual(sender._unreaped,[])
            self.assertEqual(results[0]['success'],True);self.assertEqual(len(self.calls),2)
            self.assertIn(b'\r\n\r\nx\r\n--',self.calls[-1][1])
            self.assertEqual(broker.db.execute("SELECT state FROM operations WHERE operation='upload'").fetchone()[0],'complete')
            with self.owner_queue._db() as db:self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(upload['operationId'],)).fetchone()[0],'complete')
            retry_client,retry_server=socket.socketpair();retry_client.settimeout(3);retry_server.settimeout(3)
            resources.extend([retry_client,retry_server])
            def retry_node():
                try:
                    channel=server_context.wrap_socket(retry_server,server_side=True);resources.append(channel)
                    results.append(receive_upload(channel,peer_sha256=pin,broker=broker,timeout=8))
                except Exception as error:errors.append(('retry',type(error).__name__))
            retry_thread=threading.Thread(target=retry_node);threads.append(retry_thread);retry_thread.start()
            retry_tls=client_context.wrap_socket(retry_client,server_hostname='localhost');resources.append(retry_tls)
            self.assertEqual(sender.send(retry_tls,peer_sha256=pin,store=self.owner_store,claim_token=self.owner_claim['claimToken'],
                grant=upload,signature=signature,secret=self.secret,evidence=evidence,timeout=8),{'success':True})
            retry_thread.join(5);self.assertFalse(retry_thread.is_alive());self.assertEqual(len(self.calls),2)
        finally:
            if broker is not None:broker.close()
            if remote is not None:remote.close()
            for channel in resources+[authority_client,authority_server,data_client,data_server]:
                try:channel.close()
                except OSError:pass
            for thread in threads:
                thread.join(5);self.assertFalse(thread.is_alive())
        self.assertEqual(errors,[])
        with self.owner_queue._db() as db:
            with self.assertRaises(sqlite3.IntegrityError):db.execute('DELETE FROM broker_sender_evidence')
        self.owner_lease.close();self.owner_queue.close()
        restarted=Queue(self.root/'owner-queue',node_ids=('cluster','main'),viewer_enabled=True)
        try:
            recovered=DurableUploadEvidence(restarted)
            self.assertTrue(recovered.wait_verified(upload,time.monotonic()+.1))
            self.assertEqual(restarted.get(self.job)['status'],'held')
            self.assertIsNone(restarted.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]}))
            with restarted._db() as db:db.execute('DROP TRIGGER broker_sender_evidence_update')
            with self.assertRaises(EvidenceError):recovered.wait_verified(upload,time.monotonic()+.1)
        finally:restarted.close()

    def test_interrupted_mtls_upload_retains_both_intents(self):
        self.begin_upload()
        upload,signature=issue_upload_grant(self.owner_queue,store=self.owner_store,job_id=self.job,
            claim_token=self.owner_claim['claimToken'],engine_id=self.engine,gate_generation=1,
            operation_id=str(__import__('uuid').uuid4()),file_key='synthetic.jpg',secret=self.secret)
        key=self.root/'data.key';cert=self.root/'data.crt'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=localhost','-keyout',str(key),'-out',str(cert)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
        pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert.read_text())).hexdigest()
        server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert,key);server_context.load_verify_locations(cafile=str(cert));server_context.verify_mode=ssl.CERT_REQUIRED
        client_context=ssl.create_default_context(cafile=str(cert));client_context.load_cert_chain(cert,key)
        authority_client,authority_server=socket.socketpair();data_client,data_server=socket.socketpair()
        for channel in (authority_client,authority_server,data_client,data_server):channel.settimeout(3)
        evidence=DurableUploadEvidence(self.owner_queue);errors=[];results=[];threads=[];resources=[];entered=threading.Event()
        def auth():
            try:
                channel=server_context.wrap_socket(authority_server,server_side=True);resources.append(channel)
                serve_coordinator(channel,queue=self.owner_queue,witness=LocalCoordinatorAuthority(self.owner_queue),secret=self.secret,
                    store=self.owner_store,upload_evidence=evidence,peer_sha256=pin,session_seconds=20,idle_seconds=12)
            except Exception as error:errors.append(('authority',type(error).__name__))
        authority_thread=threading.Thread(target=auth);threads.append(authority_thread);authority_thread.start()
        remote=None;broker=None
        try:
            auth_tls=client_context.wrap_socket(authority_client,server_hostname='localhost');resources.append(auth_tls)
            remote=RemoteAuthoritySession(auth_tls,authority=self.authority,epoch=self.epoch,
                claim_token=self.owner_claim['claimToken'],peer_sha256=pin,timeout_seconds=3)
            self.broker.close();broker=NodeMutationBroker(self.root/'gate'/'mutation-broker',**dict(self.config,authority_witness=remote))
            def node():
                try:
                    channel=server_context.wrap_socket(data_server,server_side=True);resources.append(channel)
                    original=self.adapter.exchange_upload
                    def signal(*args):entered.set();return original(*args)
                    with unittest.mock.patch.object(self.adapter,'exchange_upload',side_effect=signal):
                        results.append(receive_upload(channel,peer_sha256=pin,broker=broker,timeout=8))
                except Exception as error:errors.append(('node',type(error).__name__))
            node_thread=threading.Thread(target=node);threads.append(node_thread);node_thread.start()
            data_tls=client_context.wrap_socket(data_client,server_hostname='localhost');resources.append(data_tls)
            authority_session._write(data_tls,{'grant':upload,'signature':signature},time.monotonic()+3)
            self.assertEqual(authority_session._read(data_tls,time.monotonic()+3),{'state':'ready'})
            self.assertTrue(entered.wait(4))
            data_tls.shutdown(socket.SHUT_RDWR);data_tls.close()
            node_thread.join(5);self.assertFalse(node_thread.is_alive())
            self.assertEqual(broker.db.execute("SELECT state FROM operations WHERE operation='upload'").fetchone()[0],'started')
            with self.owner_queue._db() as db:self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(upload['operationId'],)).fetchone()[0],'started')
            self.assertFalse(evidence.wait_verified(upload,time.monotonic()+.01))
        finally:
            if broker is not None:broker.close()
            if remote is not None:remote.close()
            for channel in resources+[authority_client,authority_server,data_client,data_server]:
                try:channel.close()
                except OSError:pass
            for thread in threads:
                thread.join(5);self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors),1);self.assertEqual(errors[0][0],'node')

    def test_remote_upload_challenge_holds_intent_and_refuses_completion(self):
        self.begin_upload()
        upload,signature=issue_upload_grant(self.owner_queue,store=self.owner_store,job_id=self.job,
            claim_token=self.owner_claim['claimToken'],engine_id=self.engine,gate_generation=1,
            operation_id=str(__import__('uuid').uuid4()),file_key='synthetic.jpg',secret=self.secret)
        key=self.root/'upload.key';cert=self.root/'upload.crt'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=localhost','-keyout',str(key),'-out',str(cert)],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
        pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert.read_text())).hexdigest()
        server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert,key);server_context.load_verify_locations(cafile=str(cert));server_context.verify_mode=ssl.CERT_REQUIRED
        client_context=ssl.create_default_context(cafile=str(cert));client_context.load_cert_chain(cert,key)
        client_raw,server_raw=socket.socketpair();client_raw.settimeout(3);server_raw.settimeout(3)
        errors=[]
        def serve():
            try:
                channel=server_context.wrap_socket(server_raw,server_side=True)
                serve_coordinator(channel,queue=self.owner_queue,witness=LocalCoordinatorAuthority(self.owner_queue),secret=self.secret,
                    store=self.owner_store,peer_sha256=pin,session_seconds=20,idle_seconds=12)
            except Exception as error:errors.append(type(error).__name__)
        thread=threading.Thread(target=serve);thread.start();remote=None
        try:
            client=client_context.wrap_socket(client_raw,server_hostname='localhost')
            remote=RemoteAuthoritySession(client,authority=self.authority,epoch=self.epoch,
                claim_token=self.owner_claim['claimToken'],peer_sha256=pin,timeout_seconds=3)
            with remote.hold(upload,3,body=b'',signature=signature):
                remote.check()
                with self.owner_queue._db() as db:
                    self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(upload['operationId'],)).fetchone()[0],'started')
                with self.assertRaises(SessionHeld):remote.completed(upload)
            with self.owner_queue._db() as db:
                self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(upload['operationId'],)).fetchone()[0],'started')
            self.assertEqual(len(self.calls),1)
        finally:
            if remote is not None:remote.close()
            else:client_raw.close()
            server_raw.close();thread.join(5);self.assertFalse(thread.is_alive())
        self.assertEqual(errors,['SessionHeld'])

    def test_remote_init_confirms_durable_intent_and_exact_replay(self):
        key=self.root/'success.key';cert=self.root/'success.crt'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=localhost','-keyout',str(key),'-out',str(cert)],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
        pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert.read_text())).hexdigest()
        server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert,key);server_context.load_verify_locations(cafile=str(cert));server_context.verify_mode=ssl.CERT_REQUIRED
        client_context=ssl.create_default_context(cafile=str(cert));client_context.load_cert_chain(cert,key)
        client_raw,server_raw=socket.socketpair();client_raw.settimeout(3);server_raw.settimeout(3)
        errors=[];channels=[]
        def serve():
            try:
                channel=server_context.wrap_socket(server_raw,server_side=True);channels.append(channel)
                serve_coordinator(channel,queue=self.owner_queue,witness=LocalCoordinatorAuthority(self.owner_queue),secret=self.secret,peer_sha256=pin,session_seconds=20,idle_seconds=12)
            except Exception as error:errors.append(type(error).__name__)
        thread=threading.Thread(target=serve);thread.start();remote=None
        try:
            client=client_context.wrap_socket(client_raw,server_hostname='localhost')
            remote=RemoteAuthoritySession(client,authority=self.authority,epoch=self.epoch,claim_token=self.owner_claim['claimToken'],peer_sha256=pin,timeout_seconds=3)
            self.broker.close();broker=NodeMutationBroker(self.root/'gate'/'mutation-broker',**dict(self.config,authority_witness=remote));self.addCleanup(broker.close)
            grant=self.grant();signature=sign_grant(grant,self.secret)
            self.assertEqual(broker.mutate(grant,signature,self.body)['uuid'],self.task)
            with self.owner_queue._db() as db:self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()[0],'complete')
            self.assertEqual(broker.mutate(grant,signature,self.body)['uuid'],self.task)
            self.assertEqual(len(self.calls),1)
        finally:
            if remote is not None:remote.close()
            else:client_raw.close()
            thread.join(5)
            if thread.is_alive():server_raw.close();thread.join(5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors,[])

    def test_tls_eof_after_forward_preserves_started_intent_and_reservation(self):
        key=self.root/'synthetic.key';cert=self.root/'synthetic.crt'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=localhost','-keyout',str(key),'-out',str(cert)],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
        pem=cert.read_text();der=ssl.PEM_cert_to_DER_cert(pem);pin=hashlib.sha256(der).hexdigest()
        server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert,key);server_context.load_verify_locations(cafile=str(cert))
        server_context.verify_mode=ssl.CERT_REQUIRED
        client_context=ssl.create_default_context(cafile=str(cert))
        client_context.load_cert_chain(cert,key)
        client_raw,server_raw=socket.socketpair();client_raw.settimeout(3);server_raw.settimeout(3)
        errors=[];done=threading.Event();resources={'server':None,'client':None,'remote':None}
        witness=LocalCoordinatorAuthority(self.owner_queue)
        def serve():
            try:
                channel=server_context.wrap_socket(server_raw,server_side=True)
                resources['server']=channel
                serve_coordinator(channel,queue=self.owner_queue,witness=witness,secret=self.secret,
                                  peer_sha256=pin,session_seconds=20,idle_seconds=12)
            except Exception as error:errors.append(type(error).__name__)
            finally:done.set()
        thread=threading.Thread(target=serve)
        def cleanup():
            if resources['remote'] is not None:resources['remote'].close()
            for channel in (resources['client'],resources['server'],client_raw,server_raw):
                if channel is not None:
                    try:channel.close()
                    except OSError:pass
            if thread.ident is not None:thread.join(5);self.assertFalse(thread.is_alive())
        self.addCleanup(cleanup)
        thread.start()
        client=client_context.wrap_socket(client_raw,server_hostname='localhost')
        resources['client']=client
        token=TaskStore(self.owner_queue,self.root/'owner-queue'/'viewer-tasks').task(self.task)['claim_token']
        with self.assertRaises(SessionHeld):
            RemoteAuthoritySession(client,authority=self.authority,epoch=self.epoch,
                                   claim_token=token,peer_sha256='0'*64,timeout_seconds=3)
        remote=RemoteAuthoritySession(client,authority=self.authority,epoch=self.epoch,
                                      claim_token=token,peer_sha256=pin,timeout_seconds=3)
        resources['remote']=remote
        self.broker.close()
        broker=NodeMutationBroker(self.root/'gate'/'mutation-broker',**dict(self.config,authority_witness=remote))
        self.addCleanup(broker.close);grant=self.grant();signature=sign_grant(grant,self.secret)
        with remote.hold(grant,3,body=self.body,signature=signature):
            with self.assertRaises(RuntimeError):self.owner_queue.close()
            time.sleep(5.2)
            remote.check()
        original=self.adapter.exchange
        def lost_after_forward(*args):
            reply=original(*args);remote.close();return reply
        try:
            with unittest.mock.patch.object(self.adapter,'exchange',side_effect=lost_after_forward):
                with self.assertRaises(SessionHeld):broker.mutate(grant,signature,self.body)
            self.assertTrue(done.wait(5));thread.join(2);self.assertFalse(thread.is_alive())
            self.assertEqual(errors,[]);self.assertEqual(len(self.calls),1)
            self.assertEqual(broker.db.execute('SELECT state FROM operations').fetchone()[0],'started')
            with self.owner_queue._db() as db:
                self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()[0],'started')
            with NodeGate(self.engine) as gate:self.assertIsNotNone(gate.snapshot()['reservation'])
            with self.assertRaises(SessionHeld):broker.mutate(grant,signature,self.body)
            self.assertEqual(len(self.calls),1)
        finally:
            remote.close();thread.join(5)
            self.assertFalse(thread.is_alive())
        broker.close();self.owner_lease.close();self.owner_queue.close()
        restarted=Queue(self.root/'owner-queue',node_ids=('cluster','main'),viewer_enabled=True)
        try:
            self.assertEqual(restarted.get(self.job)['status'],'held')
            self.assertIsNone(restarted.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]}))
            with restarted._db() as db:
                self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()[0],'started')
        finally:restarted.close()


if __name__=='__main__':unittest.main()
