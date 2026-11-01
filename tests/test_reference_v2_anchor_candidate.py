"""Owned synthetic POSIX files on separate devices, never production trust."""
import base64
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from ops.reference_worker_daemon import DaemonStateLock
from processing import reference_v2_anchor_candidate as a,reference_v2_registry_candidate as r,reference_v2_envelope_candidate as e

@unittest.skipUnless(os.name=='posix' and os.path.isdir('/dev/shm'),'separate POSIX synthetic filesystems required')
class AnchorTests(unittest.TestCase):
    def setUp(self):
        self.primary=tempfile.TemporaryDirectory();self.anchor=tempfile.TemporaryDirectory(dir='/dev/shm')
        self.addCleanup(self.primary.cleanup);self.addCleanup(self.anchor.cleanup)
        self.source=Path(self.primary.name)/'registry.json';self.path=Path(self.anchor.name)/'anchor.sqlite'
        if self.source.parent.stat().st_dev==self.path.parent.stat().st_dev:self.skipTest('distinct devices unavailable')
        self.key=Ed25519PrivateKey.from_private_bytes(bytes([42])*32)
        from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
        self.public=self.key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
        leaf=bytes.fromhex('d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a')
        self.data=dict(version=1,epoch='epoch-1',revision=2,issuedAt=1000,expiresAt=1100,keys=[dict(issuer='coordinator',keyId='key-1',purpose='grant',audience='viewer',operation='admit',publicKey=base64.urlsafe_b64encode(leaf).decode().rstrip('='),notBefore=900,notAfter=1200,revoked=False)])
        for module in (a,r):
            p=patch.object(module,'QUALIFIED',True);p.start();self.addCleanup(p.stop)
        self.roles=(('coordinator','grant','viewer','admit'),)
        self.raw,self.sig=self.write(self.data)
        self.owner=DaemonStateLock(self.path.parent);self.owner.__enter__();self.addCleanup(self.owner.__exit__,None,None,None)
        self.client=a.LocalAnchor(self.path,owner=self.owner,root_key=self.public,registry_path=self.source,enabled=True,bootstrap=('epoch-1',2,hashlib.sha256(self.raw).hexdigest()))
    def write(self,data):
        raw=e._canonical(data);self.source.write_bytes(raw);self.source.chmod(0o600)
        return raw,base64.urlsafe_b64encode(self.key.sign(r.DOMAIN+raw)).decode().rstrip('=')
    def check(self,raw,sig):return self.client.verify_and_advance(raw,sig,now=1050,role_policy=self.roles)
    def test_cas_exact_replay_and_reopen_holds_rollback(self):
        self.check(self.raw,self.sig);new=dict(self.data,revision=3);new_raw,new_sig=self.write(new)
        self.check(new_raw,new_sig);self.check(new_raw,new_sig)
        reopened=a.LocalAnchor(self.path,owner=self.owner,root_key=self.public,registry_path=self.source,enabled=True)
        self.client=reopened
        stale_raw,stale_sig=self.write(self.data)
        with self.assertRaises(Exception):self.check(stale_raw,stale_sig)
        conflicting=dict(new,expiresAt=1099);raw,sig=self.write(conflicting)
        with self.assertRaises(a.AnchorHeld):self.check(raw,sig)
        ahead=dict(new,revision=5);raw,sig=self.write(ahead)
        with self.assertRaises(a.AnchorHeld):self.check(raw,sig)
    def test_changed_bytes_wrong_root_and_missing_hold(self):
        self.source.write_bytes(b'changed')
        with self.assertRaises(a.AnchorHeld):self.check(self.raw,self.sig)
        with self.assertRaises(a.AnchorHeld):a.LocalAnchor(self.path,owner=self.owner,root_key=bytes(32),registry_path=self.source,enabled=True)
        with self.assertRaises(a.AnchorHeld):a.LocalAnchor(self.path.parent/'absent.sqlite',owner=self.owner,root_key=self.public,registry_path=self.source,enabled=True)
    def test_same_filesystem_rejected(self):
        same=self.path.parent/'same.json';same.write_bytes(self.raw);same.chmod(0o600)
        with self.assertRaises(a.AnchorHeld):a.LocalAnchor(self.path.parent/'same.sqlite',owner=self.owner,root_key=self.public,registry_path=same,enabled=True)
    def test_owned_child_exit_after_commit_keeps_ahead_checkpoint(self):
        new_raw,new_sig=self.write(dict(self.data,revision=3))
        self.owner.__exit__(None,None,None)
        script='''import os,sys,base64
from ops.reference_worker_daemon import DaemonStateLock
from processing import reference_v2_anchor_candidate as a,reference_v2_registry_candidate as r
a.QUALIFIED=True;r.QUALIFIED=True
with DaemonStateLock(sys.argv[1]) as owner:
 c=a.LocalAnchor(sys.argv[2],owner=owner,root_key=bytes.fromhex(sys.argv[3]),registry_path=sys.argv[4],enabled=True)
 c.verify_and_advance(base64.b64decode(sys.argv[5]),sys.argv[6],now=1050,role_policy=(("coordinator","grant","viewer","admit"),))
 os._exit(77)
'''
        child=None
        try:
            child=subprocess.Popen([sys.executable,'-c',script,str(self.path.parent),str(self.path),self.public.hex(),str(self.source),base64.b64encode(new_raw).decode(),new_sig],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:out,err=child.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill();out,err=child.communicate(timeout=5);self.fail('owned synthetic child timed out')
            self.assertEqual(child.returncode,77,(out,err))
        finally:
            if child is not None and child.poll() is None:child.kill();child.wait(timeout=5)
            self.owner.__enter__()
        self.client=a.LocalAnchor(self.path,owner=self.owner,root_key=self.public,registry_path=self.source,enabled=True)
        self.check(new_raw,new_sig)
        old,old_sig=self.write(self.data)
        with self.assertRaises(Exception):self.check(old,old_sig)
