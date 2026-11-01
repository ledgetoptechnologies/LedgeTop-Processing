"""Synthetic mTLS stream with actual Queue, lease and node broker."""
import hashlib
import shutil
import socket
import ssl
import subprocess
import threading
import time
import unittest
from unittest.mock import Mock, patch

from processing.node_authority_session import (RemoteAuthoritySession, SessionHeld, serve_coordinator,
                                               serve_node_recovery, request_node_recovery)
from processing.node_mutation_broker import BrokerHeld, LocalCoordinatorAuthority, NodeMutationBroker, issue_init_grant, sign_grant
from processing.node_gate import NodeGate
from processing.viewer_tasks import TaskStore
from tests.test_node_mutation_broker import NodeMutationBrokerTests


@unittest.skipUnless(shutil.which('openssl'), 'synthetic openssl fixture required')
class NodeAuthoritySessionTests(NodeMutationBrokerTests):
    def test_recovery_endpoint_returns_pinned_actual_node_snapshot(self):
        key=self.root/'recovery-synthetic.key';cert=self.root/'recovery-synthetic.crt'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=localhost','-keyout',str(key),'-out',str(cert)],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
        pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert.read_text())).hexdigest()
        server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert,key);server_context.load_verify_locations(cafile=str(cert))
        server_context.verify_mode=ssl.CERT_REQUIRED
        client_context=ssl.create_default_context(cafile=str(cert));client_context.load_cert_chain(cert,key)
        client_raw,server_raw=socket.socketpair();client_raw.settimeout(3);server_raw.settimeout(3)
        resources={'server':None,'client':None};errors=[]
        def serve():
            try:
                resources['server']=server_context.wrap_socket(server_raw,server_side=True)
                serve_node_recovery(resources['server'],engine_id=self.engine,peer_sha256=pin,timeout_seconds=3)
            except Exception as error:errors.append(type(error).__name__)
        thread=threading.Thread(target=serve)
        def cleanup():
            for channel in (resources['client'],resources['server'],client_raw,server_raw):
                if channel is not None:
                    try:channel.close()
                    except OSError:pass
            if thread.ident is not None:thread.join(5);self.assertFalse(thread.is_alive())
        self.addCleanup(cleanup)
        with patch('processing.node_authority_session.RECOVERY_ENDPOINT_QUALIFIED',True):
            thread.start()
            resources['client']=client_context.wrap_socket(client_raw,server_hostname='localhost')
            report=request_node_recovery(resources['client'],node_id=self.node,engine_id=self.engine,
                authority_id=self.authority,peer_sha256=pin,timeout_seconds=3)
            thread.join(5);self.assertFalse(thread.is_alive())
        self.assertEqual(errors,[])
        self.assertEqual(report['identity']['coordinatorEpoch'],self.epoch)
        self.assertEqual(report['operations'],[])
        self.assertFalse(report['authorizesAdmission'])

    def test_recovery_endpoint_disabled_by_default_closes_socket(self):
        channel=Mock()
        with self.assertRaises(SessionHeld):
            serve_node_recovery(channel,engine_id=self.engine,peer_sha256='0'*64)
        channel.close.assert_called_once_with()
        channel=Mock()
        with self.assertRaises(SessionHeld):
            request_node_recovery(channel,node_id=self.node,engine_id=self.engine,
                                  authority_id=self.authority,peer_sha256='0'*64)
        channel.close.assert_called_once_with()

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
            with NodeGate(self.engine) as gate:self.assertIsNotNone(gate.snapshot()['reservation'])
            with self.assertRaises(SessionHeld):broker.mutate(grant,signature,self.body)
            self.assertEqual(len(self.calls),1)
        finally:
            remote.close();thread.join(5)
            self.assertFalse(thread.is_alive())


if __name__=='__main__':unittest.main()
