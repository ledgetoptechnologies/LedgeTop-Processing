"""Synthetic loopback mTLS and owned SQLite state; no deployed service."""
import datetime
import base64
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import socket
import shutil
import sqlite3
import subprocess
import sys
import ssl
import tempfile
import threading
import unittest
from contextlib import closing
from unittest.mock import patch
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from ops.reference_worker_daemon import DaemonStateLock
from processing import reference_v2_remote_anchor_candidate as m
from processing import reference_v2_anchor_client_candidate as client

@unittest.skipUnless(os.name=='posix','owned POSIX test state required')
class RemoteAnchorTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp());self.threads=[]
        def cleanup():
            if any(thread.is_alive() for thread in self.threads):return
            shutil.rmtree(self.root)
        self.addCleanup(cleanup)
        self.flag=patch.object(m,'QUALIFIED',True);self.flag.start();self.addCleanup(self.flag.stop)
        self.auth_flag=patch.object(m,'AUTHORITY_QUALIFIED',True);self.auth_flag.start();self.addCleanup(self.auth_flag.stop)
        self.owner=DaemonStateLock(self.root);self.owner.__enter__()
        def release_owner():
            if not any(thread.is_alive() for thread in self.threads):self.owner.__exit__(None,None,None)
        self.addCleanup(release_owner)
        self.store=m.Store(self.root/'anchor.sqlite',owner=self.owner,domain='test',ledger='viewer',epoch='epoch-1',enabled=True,bootstrap=(0,'a'*64))
        self.request=dict(domain='test',ledger='viewer',epoch='epoch-1',operation='op-1',previousRevision=0,previousDigest='a'*64,nextRevision=1,nextDigest='b'*64)
        now=datetime.datetime.now(datetime.timezone.utc);self.ca_key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
        subject=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'synthetic-ca')])
        ca=(x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(self.ca_key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(days=1)).not_valid_after(now+datetime.timedelta(days=1)).add_extension(x509.BasicConstraints(ca=True,path_length=None),critical=True).sign(self.ca_key,hashes.SHA256()))
        ca_path=self.root/'ca.pem';ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
        def cert(name):
            key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
            value=(x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,name)])).issuer_name(subject).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(days=1)).not_valid_after(now+datetime.timedelta(days=1)).add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),critical=False).sign(self.ca_key,hashes.SHA256()))
            cert_path=self.root/(name+'.pem');key_path=self.root/(name+'.key');cert_path.write_bytes(value.public_bytes(serialization.Encoding.PEM));key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
            return cert_path,key_path,hashlib.sha256(value.public_bytes(serialization.Encoding.DER)).hexdigest()
        server,server_key,self.server_pin=cert('server');client,client_key,self.client_pin=cert('client')
        self.server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);self.server_context.load_cert_chain(server,server_key);self.server_context.load_verify_locations(cafile=str(ca_path));self.server_context.verify_mode=ssl.CERT_REQUIRED
        self.client_context=ssl.create_default_context(cafile=str(ca_path));self.client_context.load_cert_chain(client,client_key)

    def session(self,request,*,server_pin=None,client_pin=None,drop=False):
        server_raw,client_raw=socket.socketpair();server_raw.settimeout(5);client_raw.settimeout(5);errors=[];channel=[]
        def worker():
            try:
                with self.server_context.wrap_socket(server_raw,server_side=True) as tls:
                    channel.append(tls)
                    if drop:
                        m.wire._peer(tls,client_pin or self.client_pin,True);incoming=m.wire._read(tls,m.time.monotonic()+5);self.store.cas(incoming['transition']);return
                    m.serve(tls,store=self.store,peer_sha256=client_pin or self.client_pin)
            except Exception as exc:errors.append(exc)
        thread=threading.Thread(target=worker,daemon=False);self.threads.append(thread);started=False
        try:
            thread.start();started=True
            with self.client_context.wrap_socket(client_raw,server_hostname='127.0.0.1') as tls:
                result=m.exchange(tls,request=request,peer_sha256=server_pin or self.server_pin,nonce='1'*64)
            return result,errors
        finally:
            client_raw.close();server_raw.close()
            for tls in channel:
                try:tls.close()
                except OSError:pass
            if started:
                thread.join(15)
                if thread.is_alive():raise RuntimeError('owned test thread did not stop; fixture retained')

    def test_mtls_cas_exact_replay_and_conflict(self):
        first,errors=self.session(self.request);self.assertFalse(errors);self.assertEqual(first['digest'],'b'*64)
        replay,errors=self.session(self.request);self.assertFalse(errors);self.assertEqual(replay,first)
        altered=dict(self.request,nextDigest='c'*64)
        with self.assertRaises(Exception):self.session(altered)
        reopened=m.Store(self.root/'anchor.sqlite',owner=self.owner,domain='test',ledger='viewer',epoch='epoch-1',enabled=True)
        self.assertEqual(reopened.cas(self.request),first)
    def test_lost_response_replay_and_wrong_peer_hold(self):
        with self.assertRaises(Exception):self.session(self.request,drop=True)
        receipt,errors=self.session(self.request);self.assertFalse(errors);self.assertEqual(receipt['revision'],1)
        new=dict(self.request,operation='op-2',previousRevision=1,previousDigest='b'*64,nextRevision=2,nextDigest='c'*64)
        with self.assertRaises(Exception):self.session(new,server_pin='0'*64)
        with self.assertRaises(Exception):self.session(new,client_pin='0'*64)
        self.assertEqual(self.store.cas(new)['revision'],2)
    def test_separate_client_journal_retains_pending_after_lost_response(self):
        journal=self.root/'viewer-intent.sqlite'
        with patch.object(client,'QUALIFIED',True):
            with closing(sqlite3.connect(journal)) as db,db:
                db.execute('PRAGMA synchronous=FULL');client.install(db)
                self.assertEqual(client.prepare(db,self.request),'pending')
            with self.assertRaises(Exception):self.session(self.request,drop=True)
            with closing(sqlite3.connect(journal)) as db,db:
                self.assertEqual(db.execute('SELECT state FROM anchor_intents').fetchone()[0],'pending')
                with self.assertRaises(m.AnchorHeld):client.prepare(db,dict(self.request,nextDigest='c'*64))
            result,errors=self.session(self.request);self.assertFalse(errors)
            with closing(sqlite3.connect(journal)) as db,db:
                client.complete(db,self.request,result);client.complete(db,self.request,result)
                self.assertEqual(db.execute('SELECT state FROM anchor_intents').fetchone()[0],'recorded')
                with self.assertRaises(m.AnchorHeld):client.complete(db,self.request,dict(result,digest='c'*64))
    def test_absent_real_authority_never_advances(self):
        with patch.object(m,'AUTHORITY_QUALIFIED',False):
            with self.assertRaises(Exception):self.session(self.request)
        self.assertEqual(self.store.cas(self.request)['revision'],1)
    def test_owned_child_exits_after_committed_cas_before_reply(self):
        self.owner.__exit__(None,None,None)
        script='''import base64,json,os,sys
from ops.reference_worker_daemon import DaemonStateLock
from processing import reference_v2_remote_anchor_candidate as m
m.QUALIFIED=True
with DaemonStateLock(sys.argv[1]) as owner:
 s=m.Store(sys.argv[2],owner=owner,domain="test",ledger="viewer",epoch="epoch-1",enabled=True)
 s.cas(json.loads(base64.b64decode(sys.argv[3])))
 os._exit(77)
'''
        child=None
        try:
            payload=base64.b64encode(json.dumps(self.request).encode()).decode()
            child=subprocess.Popen([sys.executable,'-c',script,str(self.root),str(self.root/'anchor.sqlite'),payload],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:out,err=child.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill();out,err=child.communicate(timeout=5);self.fail('owned child timed out')
            self.assertEqual(child.returncode,77,(out,err))
        finally:
            if child is not None and child.poll() is None:child.kill();child.wait(timeout=5)
            self.owner.__enter__()
        reopened=m.Store(self.root/'anchor.sqlite',owner=self.owner,domain='test',ledger='viewer',epoch='epoch-1',enabled=True)
        self.assertEqual(reopened.cas(self.request)['revision'],1)
        with self.assertRaises(m.AnchorHeld):reopened.cas(dict(self.request,nextDigest='c'*64))
