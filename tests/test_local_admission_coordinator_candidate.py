"""Synthetic peer/intent replay only; no listener, network or NAS."""
import json
import hashlib
import ssl
import subprocess
import shutil
from contextlib import contextmanager, ExitStack
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing.crossfs_admission_candidate import CrossFsHeld
from processing.local_capacity_authority_candidate import LocalCapacityAuthority
from processing.local_capacity_protocol_candidate import LocalCapacityProtocol
from processing.local_admission_coordinator_candidate import LocalAdmissionCoordinator, UnixCapacityConnector
from processing.scratch_policy import ScratchLedger


@unittest.skipUnless(os.name=='posix','POSIX peer/lock fixture required')
class LocalCoordinatorCandidateTests(unittest.TestCase):
    def setUp(self):
        self.host=Path(tempfile.mkdtemp(prefix='lt-local-authority-')).resolve()
        self.coordinator=Path(tempfile.mkdtemp(prefix='lt-local-coordinator-')).resolve()
        print('Retained synthetic roots:',self.host,self.coordinator,flush=True)
        self.host_journal=self.host/'admission.sqlite';self.host_journal.touch(mode=0o600)
        self.coordinator_journal=self.coordinator/'intent.sqlite';self.coordinator_journal.touch(mode=0o600)
        self.ledger=ScratchLedger(self.host/'ledger.sqlite',self.host,capacity_probe=lambda _:10000,safety_floor_bytes=100)
        j=self.host_journal.stat();r=self.ledger.root.stat();d=self.ledger.path.stat()
        self.binding=self.host/'authority.json'
        self.binding.write_text(json.dumps({'schemaVersion':1,'authorityId':'a'*32,'journal':str(self.host_journal),
            'journalIdentity':[j.st_dev,j.st_ino],'ledgers':{'local':{'root':str(self.ledger.root),
            'journal':str(self.ledger.path),'floor':100,'identity':[r.st_dev,r.st_ino,d.st_dev,d.st_ino]}}}))
        self.binding.chmod(0o600)
        for name,value in [('processing.local_capacity_authority_candidate.LOCAL_CAPACITY_AUTHORITY_QUALIFIED',True),
            ('processing.crossfs_admission_candidate.CROSSFS_QUALIFIED',True),
            ('processing.crossfs_admission_candidate.CROSSFS_CANONICAL_JOURNAL',self.host_journal),
            ('processing.local_capacity_protocol_candidate.LOCAL_CAPACITY_PROTOCOL_QUALIFIED',True),
            ('processing.local_admission_coordinator_candidate.LOCAL_CAPACITY_COORDINATOR_QUALIFIED',True)]:
            p=patch(name,value);p.start();self.addCleanup(p.stop)
        self.plan=dict(attempt_id='attempt',job_id='job',generation=1,manifest_sha256='a'*64,
            environment='staging',ttl_seconds=60,demands=[{'ledger':'local','bytes':300}])
    @contextmanager
    def service(self,protocol,peer_uid=None):
        path=self.host/'protocol.sock'
        listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        stopping=threading.Event();thread=None
        try:
            listener.bind(str(path));listener.listen(2);listener.settimeout(.2)
            def serve():
                while not stopping.is_set():
                    try:channel,_=listener.accept()
                    except socket.timeout:continue
                    except OSError:break
                    try:protocol.serve_one(channel)
                    except (CrossFsHeld,OSError):pass
                    finally:channel.close()
            thread=threading.Thread(target=serve);thread.start()
            yield UnixCapacityConnector(path,os.getuid() if peer_uid is None else peer_uid)
        finally:
            stopping.set();listener.close()
            if thread is not None and thread.ident is not None:
                thread.join(4)
                self.assertFalse(thread.is_alive())

    def test_lost_reply_replays_exact_intent_without_second_reservation(self):
        import processing.local_admission_coordinator_candidate as coordinator_module
        from processing.local_capacity_protocol_candidate import _read_exact as actual_read
        with DaemonStateLock(self.coordinator) as owner, LocalCapacityAuthority(self.host,self.binding,self.host_journal,{'local':self.ledger}) as authority:
            protocol=LocalCapacityProtocol(authority,trusted_uid=os.getuid(),plans={'attempt':self.plan})
            driver=LocalAdmissionCoordinator(owner=owner,journal=self.coordinator_journal,
                expected_uid=os.getuid(),expected_authority='a'*32)
            def lost(sock,length,deadline):
                actual_read(sock,length,deadline)
                raise OSError('synthetic lost reply')
            with self.service(protocol) as connector:
                with patch.object(coordinator_module,'_read_exact',side_effect=lost):
                    with self.assertRaises(CrossFsHeld):driver.reserve(attempt_id='attempt',expected_plan=self.plan,connector=connector)
                with driver._db() as db:self.assertEqual(db.execute('select state from local_admission').fetchone()[0],'intent')
                result=driver.reserve(attempt_id='attempt',expected_plan=self.plan,connector=connector)
                self.assertEqual(result['state'],'receipt_recorded')
                self.assertEqual(result['receipt']['state'],'admitted')
                self.assertEqual(driver.observe_pin(attempt_id='attempt',expected_plan=self.plan,connector=connector)['state'],'pin_observed')
                with self.ledger._transaction() as db:
                    self.assertEqual(db.execute('select count(*) from reservations').fetchone()[0],1)
                    reservation=db.execute('select id from reservations').fetchone()[0]
                self.ledger.set_engine_state(reservation,'terminal',terminal_attested=True)
                with self.assertRaises(CrossFsHeld):
                    driver.observe_pin(attempt_id='attempt',expected_plan=self.plan,connector=connector)
                altered=dict(self.plan,job_id='other')
                with self.assertRaises(CrossFsHeld):driver.reserve(attempt_id='attempt',expected_plan=altered,connector=connector)

    @unittest.skipUnless(shutil.which('openssl'),'openssl required')
    def test_pinned_mtls_fixed_plan_reserve_and_pin(self):
        from processing.remote_capacity_session_candidate import serve_capacity_session, request_capacity_session
        paths=[]
        for stem in ('server','client'):
            key=self.host/(stem+'.key');cert=self.host/(stem+'.crt')
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                '-subj','/CN=localhost','-keyout',str(key),'-out',str(cert)],
                stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
            paths.append((key,cert))
        (server_key,server_cert),(client_key,client_cert)=paths
        server_pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(server_cert.read_text())).hexdigest()
        client_pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(client_cert.read_text())).hexdigest()
        server_ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);server_ctx.load_cert_chain(server_cert,server_key)
        server_ctx.load_verify_locations(cafile=str(client_cert));server_ctx.verify_mode=ssl.CERT_REQUIRED
        client_ctx=ssl.create_default_context(cafile=str(server_cert));client_ctx.load_cert_chain(client_cert,client_key)
        with patch('processing.remote_capacity_session_candidate.REMOTE_CAPACITY_QUALIFIED',True):
            with LocalCapacityAuthority(self.host,self.binding,self.host_journal,{'local':self.ledger}) as authority:
                for op in ('reserve','pin'):
                    expected_plan=json.loads(json.dumps(self.plan))
                    original_acquire=authority.acquire
                    def mutate_original(**kwargs):
                        self.plan['demands'][0]['bytes']=301
                        return original_acquire(**kwargs)
                    client_raw,server_raw=socket.socketpair();client_raw.settimeout(3);server_raw.settimeout(3)
                    record={};errors=[];thread=None
                    def serve():
                        try:
                            channel=server_ctx.wrap_socket(server_raw,server_side=True);record['server']=channel
                            serve_capacity_session(channel,authority=authority,plans={'attempt':self.plan},client_pin=client_pin)
                        except Exception as error:errors.append(type(error).__name__)
                        finally:
                            if 'server' in record:record['server'].close()
                    try:
                        with patch.object(authority,'acquire',side_effect=mutate_original):
                            thread=threading.Thread(target=serve);thread.start()
                            channel=client_ctx.wrap_socket(client_raw,server_hostname='localhost');record['client']=channel
                            value=request_capacity_session(channel,operation=op,attempt='attempt',plan=expected_plan,
                                authority_id='a'*32,server_pin=server_pin)
                            self.assertEqual(value['state'],'observed')
                            self.assertEqual(self.plan['demands'][0]['bytes'],301)
                    finally:
                        for key in ('client','server'):
                            if key in record:record[key].close()
                        client_raw.close();server_raw.close()
                        if thread is not None and thread.ident is not None:
                            thread.join(4);self.assertFalse(thread.is_alive())
                    self.assertEqual(errors,[])
                    self.plan=expected_plan
                with self.ledger._transaction() as db:
                    self.assertEqual(db.execute('select remaining from reservations').fetchone()[0],300)

    @unittest.skipUnless(shutil.which('openssl'),'openssl required')
    def test_two_pinned_authorities_lost_response_replay_and_fresh_pins(self):
        from processing.multihost_capacity_journal_candidate import PinnedCapacityConnector, MultiHostCapacityJournal
        import processing.remote_capacity_session_candidate as remote
        from processing.remote_capacity_session_candidate import serve_capacity_session
        host2=Path(tempfile.mkdtemp(prefix='lt-local-authority-second-')).resolve()
        print('Retained second synthetic authority root:',host2,flush=True)
        journal2=host2/'admission.sqlite';journal2.touch(mode=0o600)
        ledger2=ScratchLedger(host2/'ledger.sqlite',host2,capacity_probe=lambda _:10000,safety_floor_bytes=100)
        j=journal2.stat();r=ledger2.root.stat();d=ledger2.path.stat();binding2=host2/'authority.json'
        binding2.write_text(json.dumps({'schemaVersion':1,'authorityId':'b'*32,'journal':str(journal2),
            'journalIdentity':[j.st_dev,j.st_ino],'ledgers':{'local':{'root':str(ledger2.root),
            'journal':str(ledger2.path),'floor':100,'identity':[r.st_dev,r.st_ino,d.st_dev,d.st_ino]}}}))
        binding2.chmod(0o600)
        certs=[]
        for stem in ('server-one','server-two','client'):
            key=self.host/(stem+'.key');cert=self.host/(stem+'.crt')
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                '-subj','/CN=localhost','-addext','subjectAltName=IP:127.0.0.1',
                '-keyout',str(key),'-out',str(cert)],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,check=True,timeout=10)
            certs.append((key,cert))
        (key1,cert1),(key2,cert2),(client_key,client_cert)=certs
        pins=[hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert.read_text())).hexdigest() for cert in (cert1,cert2)]
        client_pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(client_cert.read_text())).hexdigest()
        client_ctx=ssl.create_default_context();client_ctx.load_verify_locations(cafile=str(cert1))
        client_ctx.load_verify_locations(cafile=str(cert2));client_ctx.load_cert_chain(client_cert,client_key)
        contexts=[]
        for key,cert in ((key1,cert1),(key2,cert2)):
            ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.load_cert_chain(cert,key)
            ctx.load_verify_locations(cafile=str(client_cert));ctx.verify_mode=ssl.CERT_REQUIRED;contexts.append(ctx)
        listeners=[];threads=[];stop=threading.Event()
        def stop_services():
            stop.set()
            for listener in listeners:listener.close()
            for thread in threads:
                if thread.ident is not None:thread.join(5)
            self.assertFalse(any(thread.is_alive() for thread in threads))
        try:
            with patch('processing.remote_capacity_session_candidate.REMOTE_CAPACITY_QUALIFIED',True), \
                 patch('processing.multihost_capacity_journal_candidate.MULTIHOST_CAPACITY_QUALIFIED',True), ExitStack() as stack:
                owner=stack.enter_context(DaemonStateLock(self.coordinator))
                authority1=stack.enter_context(LocalCapacityAuthority(self.host,self.binding,self.host_journal,{'local':self.ledger}))
                with patch('processing.crossfs_admission_candidate.CROSSFS_CANONICAL_JOURNAL',journal2):
                    authority2=stack.enter_context(LocalCapacityAuthority(host2,binding2,journal2,{'local':ledger2}))
                stack.callback(stop_services)
                for authority,ctx in zip((authority1,authority2),contexts):
                    listener=socket.socket(socket.AF_INET,socket.SOCK_STREAM);listeners.append(listener)
                    listener.bind(('127.0.0.1',0));listener.listen(4);listener.settimeout(.2)
                    def serve(listener=listener,authority=authority,ctx=ctx):
                        while not stop.is_set():
                            try:raw,_=listener.accept()
                            except socket.timeout:continue
                            except OSError:break
                            raw.settimeout(3);channel=None
                            try:
                                channel=ctx.wrap_socket(raw,server_side=True)
                                serve_capacity_session(channel,authority=authority,plans={'attempt':self.plan},client_pin=client_pin)
                            except (CrossFsHeld,OSError,ssl.SSLError):pass
                            finally:
                                if channel is not None:channel.close()
                                else:raw.close()
                    thread=threading.Thread(target=serve);threads.append(thread);thread.start()
                connectors={name:PinnedCapacityConnector(host='127.0.0.1',port=listener.getsockname()[1],
                    context=client_ctx,server_pin=pin,authority_id=identity)
                    for name,listener,pin,identity in zip(('one','two'),listeners,pins,('a'*32,'b'*32))}
                coordinator=MultiHostCapacityJournal(owner=owner,journal=self.coordinator_journal,authorities=connectors)
                plans={'one':self.plan,'two':self.plan}
                original=remote._write;dropped=[]
                def drop_second(channel,value,deadline):
                    if channel.server_side and channel.getsockname()[1]==listeners[1].getsockname()[1] and value.get('state')=='admitted' and not dropped:
                        dropped.append(True);raise OSError('synthetic lost remote reply')
                    return original(channel,value,deadline)
                with patch.object(remote,'_write',side_effect=drop_second):
                    with self.assertRaises(CrossFsHeld):coordinator.observe(attempt_id='attempt',plans=plans)
                self.assertEqual(dropped,[True])
                with coordinator._db() as db:
                    self.assertEqual(db.execute('select state from multi_attempts').fetchone()[0],'collecting')
                    self.assertEqual(db.execute('select count(*) from multi_receipts').fetchone()[0],1)
                # Reopen the journal object; both authorities are already holding.
                coordinator=MultiHostCapacityJournal(owner=owner,journal=self.coordinator_journal,authorities=connectors)
                self.assertEqual(coordinator.observe(attempt_id='attempt',plans=plans)['state'],'observed_only')
                first=connectors['one'];original_observe=first.observe;old_pin=first.server_pin
                def mutate_identity(**kwargs):
                    value=original_observe(**kwargs)
                    first.server_pin='f'*64 if old_pin!='f'*64 else 'e'*64
                    return value
                try:
                    with patch.object(first,'observe',side_effect=mutate_identity):
                        with self.assertRaisesRegex(CrossFsHeld,'identity changed'):
                            coordinator.observe(attempt_id='attempt',plans=plans)
                finally:first.server_pin=old_pin
                # Allocating within a hold preserves its immutable identity.
                with ledger2._transaction() as db:
                    second_reservation=db.execute('select id from reservations').fetchone()[0]
                (host2/'synthetic-allocated-byte').write_bytes(b'x')
                ledger2.record_allocated(second_reservation,1)
                self.assertEqual(coordinator.observe(attempt_id='attempt',plans=plans)['state'],'observed_only')
                ledger2.set_engine_state(second_reservation,'terminal',terminal_attested=True)
                with self.assertRaises(CrossFsHeld):
                    coordinator.observe(attempt_id='attempt',plans=plans)
                for ledger in (self.ledger,ledger2):
                    with ledger._transaction() as db:self.assertEqual(db.execute('select count(*) from reservations').fetchone()[0],1)
        finally:
            stop_services()

    @unittest.skipUnless(shutil.which('openssl'),'openssl required')
    def test_coordinator_process_death_after_host_receipt_replays(self):
        from processing.multihost_capacity_journal_candidate import PinnedCapacityConnector, MultiHostCapacityJournal
        from processing.remote_capacity_session_candidate import serve_capacity_session
        pairs=[]
        for stem in ('crash-server','crash-client'):
            key=self.host/(stem+'.key');cert=self.host/(stem+'.crt')
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                '-subj','/CN=localhost','-addext','subjectAltName=IP:127.0.0.1',
                '-keyout',str(key),'-out',str(cert)],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,check=True,timeout=10)
            pairs.append((key,cert))
        (server_key,server_cert),(client_key,client_cert)=pairs
        pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(server_cert.read_text())).hexdigest()
        client_pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(client_cert.read_text())).hexdigest()
        server_ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);server_ctx.load_cert_chain(server_cert,server_key)
        server_ctx.load_verify_locations(cafile=str(client_cert));server_ctx.verify_mode=ssl.CERT_REQUIRED
        client_ctx=ssl.create_default_context(cafile=str(server_cert));client_ctx.load_cert_chain(client_cert,client_key)
        listener=socket.socket(socket.AF_INET,socket.SOCK_STREAM);stop=threading.Event();thread=None
        try:
            listener.bind(('127.0.0.1',0));listener.listen(4);listener.settimeout(.2)
            with patch('processing.remote_capacity_session_candidate.REMOTE_CAPACITY_QUALIFIED',True), \
                 patch('processing.multihost_capacity_journal_candidate.MULTIHOST_CAPACITY_QUALIFIED',True), \
                 LocalCapacityAuthority(self.host,self.binding,self.host_journal,{'local':self.ledger}) as authority:
                def serve():
                    while not stop.is_set():
                        try:raw,_=listener.accept()
                        except socket.timeout:continue
                        except OSError:break
                        raw.settimeout(3);channel=None
                        try:
                            channel=server_ctx.wrap_socket(raw,server_side=True)
                            serve_capacity_session(channel,authority=authority,plans={'attempt':self.plan},client_pin=client_pin)
                        except (CrossFsHeld,OSError,ssl.SSLError):pass
                        finally:
                            if channel is not None:channel.close()
                            else:raw.close()
                thread=threading.Thread(target=serve);thread.start()
                try:
                    result=subprocess.run([sys.executable,'-m','tests.capacity_crash_child',str(self.coordinator),
                        str(listener.getsockname()[1]),str(server_cert),str(client_cert),str(client_key),pin],
                        stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
                    self.assertEqual(result.returncode,23)
                    connector=PinnedCapacityConnector(host='127.0.0.1',port=listener.getsockname()[1],
                        context=client_ctx,server_pin=pin,authority_id='a'*32)
                    with DaemonStateLock(self.coordinator) as owner:
                        driver=MultiHostCapacityJournal(owner=owner,journal=self.coordinator_journal,authorities={'one':connector})
                        with driver._db() as db:
                            self.assertEqual(db.execute('select state from multi_attempts').fetchone()[0],'collecting')
                            self.assertEqual(db.execute('select count(*) from multi_receipts').fetchone()[0],0)
                        self.assertEqual(driver.observe(attempt_id='attempt',plans={'one':self.plan})['state'],'observed_only')
                    with self.ledger._transaction() as db:
                        self.assertEqual(db.execute('select count(*) from reservations').fetchone()[0],1)
                finally:
                    stop.set();listener.close();thread.join(5);self.assertFalse(thread.is_alive())
        finally:
            stop.set();listener.close()
            if thread is not None and thread.ident is not None:
                thread.join(5);self.assertFalse(thread.is_alive())

    def test_wrong_peer_and_missing_response_leave_intent(self):
        with DaemonStateLock(self.coordinator) as owner, LocalCapacityAuthority(self.host,self.binding,self.host_journal,{'local':self.ledger}) as authority:
            protocol=LocalCapacityProtocol(authority,trusted_uid=os.getuid(),plans={'attempt':self.plan})
            driver=LocalAdmissionCoordinator(owner=owner,journal=self.coordinator_journal,
                expected_uid=os.getuid()+1,expected_authority='a'*32)
            with self.service(protocol,peer_uid=os.getuid()+1) as connector:
                with self.assertRaises(CrossFsHeld):driver.reserve(attempt_id='attempt',expected_plan=self.plan,connector=connector)
            with driver._db() as db:self.assertEqual(db.execute('select state from local_admission').fetchone()[0],'intent')
