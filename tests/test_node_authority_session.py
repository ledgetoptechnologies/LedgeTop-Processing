"""Synthetic mTLS stream with actual Queue, lease and node broker."""
import hashlib
import shutil
import socket
import ssl
import subprocess
import threading
import unittest

from processing.node_authority_session import RemoteAuthoritySession, SessionHeld, serve_coordinator
from processing.node_mutation_broker import LocalCoordinatorAuthority, NodeMutationBroker, sign_grant
from processing.node_gate import NodeGate
from processing.viewer_tasks import TaskStore
from tests.test_node_mutation_broker import NodeMutationBrokerTests


@unittest.skipUnless(shutil.which('openssl'), 'synthetic openssl fixture required')
class NodeAuthoritySessionTests(NodeMutationBrokerTests):
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
        client_raw,server_raw=socket.socketpair();errors=[];done=threading.Event()
        witness=LocalCoordinatorAuthority(self.owner_queue)
        def serve():
            try:
                channel=server_context.wrap_socket(server_raw,server_side=True)
                serve_coordinator(channel,queue=self.owner_queue,witness=witness,secret=self.secret,
                                  peer_sha256=pin,session_seconds=10,idle_seconds=3)
            except Exception as error:errors.append(type(error).__name__)
            finally:done.set()
        thread=threading.Thread(target=serve);thread.start()
        client=client_context.wrap_socket(client_raw,server_hostname='localhost')
        token=TaskStore(self.owner_queue,self.root/'owner-queue'/'viewer-tasks').task(self.task)['claim_token']
        with self.assertRaises(SessionHeld):
            RemoteAuthoritySession(client,authority=self.authority,epoch=self.epoch,
                                   claim_token=token,peer_sha256='0'*64,timeout_seconds=3)
        remote=RemoteAuthoritySession(client,authority=self.authority,epoch=self.epoch,
                                      claim_token=token,peer_sha256=pin,timeout_seconds=3)
        self.broker.close()
        broker=NodeMutationBroker(self.root/'gate'/'mutation-broker',**dict(self.config,authority_witness=remote))
        self.addCleanup(broker.close);grant=self.grant();signature=sign_grant(grant,self.secret)
        with remote.hold(grant,3,body=self.body,signature=signature):
            with self.assertRaises(RuntimeError):self.owner_queue.close()
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
