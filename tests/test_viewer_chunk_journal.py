"""Synthetic direct component tests: never attached to coordinator ingress."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
import unittest
from processing.direct_output_slot import open_slot,_identity
from processing.viewer_chunk_journal import SyntheticChunkJournal,ChunkHeld,open_synthetic_chunk_slot

@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux allocation required')
class ChunkJournalTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name);self.root.chmod(0o700)
        fd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY)
        try:self.identity=_identity(fd)
        finally:os.close(fd)
        self.key='a'*64;self.data=b'abcdefgh';self.binding=dict(jobId='11111111-1111-4111-8111-111111111111',taskId='22222222-2222-4222-8222-222222222222',generation=1,environment='staging',archiveBytes=8,archiveSha256=hashlib.sha256(self.data).hexdigest())
        slot=self.open(create=True);SyntheticChunkJournal(slot,self.binding,legacy_state='new',synthetic_fixture=True);slot.close()
    def open(self,create=False):
        if create:return open_slot(self.root,self.identity,self.key,65536,max_budget=65536,create=True)
        return open_synthetic_chunk_slot(self.root,self.identity,self.key,65536,max_budget=65536,binding=self.binding,legacy_state='new',synthetic_fixture=True)
    def test_exact_replay_sequence_and_full_seal(self):
        slot=self.open()
        try:
            j=SyntheticChunkJournal(slot,self.binding,legacy_state='new',synthetic_fixture=True)
            self.assertEqual(j.write(0,0,b'abcd'),j.write(0,0,b'abcd'))
            with self.assertRaises(Exception):j.write(0,0,b'xxxx')
            with self.assertRaises(Exception):j.write(1,3,b'efgh')
            j.write(1,4,b'efgh');self.assertEqual(j.finish(2)['sha256'],self.binding['archiveSha256'])
            self.assertEqual(j.finish(2)['bytes'],8)
        finally:slot.close()
    def test_concurrent_replay_and_tamper_fail_closed(self):
        slot=self.open()
        try:
            journal=SyntheticChunkJournal(slot,self.binding,legacy_state='new',synthetic_fixture=True)
            with ThreadPoolExecutor(max_workers=2) as pool:
                a=pool.submit(journal.write,0,0,b'abcd');b=pool.submit(journal.write,0,0,b'abcd')
                self.assertEqual(a.result(timeout=5),b.result(timeout=5))
            os.pwrite(slot._fd,b'X',0);os.fsync(slot._fd)
            with self.assertRaises(ChunkHeld):journal.write(0,0,b'abcd')
        finally:slot.close()

    def test_ordinary_open_rejects_existing_or_partial_chunk_marker(self):
        with self.assertRaises(Exception):open_slot(self.root,self.identity,self.key,65536,max_budget=65536)
        other='b'*64
        slot=open_slot(self.root,self.identity,other,65536,max_budget=65536,create=True);slot.close()
        (self.root/('slot-'+other+'.alloc.chunks-v1.partial')).write_bytes(b'partial')
        with self.assertRaises(Exception):open_slot(self.root,self.identity,other,65536,max_budget=65536)
        with self.assertRaises(ChunkHeld):open_synthetic_chunk_slot(self.root,self.identity,self.key,65536,max_budget=65536,binding=self.binding,legacy_state='new')

    def test_legacy_receiving_never_reopened(self):
        slot=self.open()
        try:
            with self.assertRaises(ChunkHeld):SyntheticChunkJournal(slot,self.binding,legacy_state='receiving',synthetic_fixture=True)
        finally:slot.close()
    def test_kill_at_chunk_boundaries(self):
        code='''import json,os,signal,sys
from unittest.mock import patch
from processing.direct_output_slot import OutputSlot
import processing.viewer_chunk_journal as m
root,identity,key,binding,mode=sys.argv[1:]
slot=m.open_synthetic_chunk_slot(root,json.loads(identity),key,65536,max_budget=65536,binding=json.loads(binding),legacy_state='new',synthetic_fixture=True)
j=m.SyntheticChunkJournal(slot,json.loads(binding),legacy_state='new',synthetic_fixture=True)
def stop():os.kill(os.getpid(),signal.SIGKILL)
oldpersist=m._persist;oldwrite=OutputSlot.write_at;oldsync=OutputSlot.sync
def persist(root,identity,name,value):
 oldpersist(root,identity,name,value)
 if mode=='intent' and name.endswith('.intent'):stop()
 if mode=='receipt' and name.endswith('.receipt'):stop()
def write(self,*args):
 oldwrite(self,*args)
 if mode=='write':stop()
def sync(self):
 oldsync(self)
 if mode=='sync':stop()
with patch.object(m,'_persist',persist),patch.object(OutputSlot,'write_at',write),patch.object(OutputSlot,'sync',sync):j.write(0,0,b'abcd')
'''
        for mode in ('intent','write','sync','receipt'):
            with self.subTest(mode=mode):
                if mode!='intent':
                    self.tearDown();self.doCleanups();self.setUp()
                child=subprocess.run([sys.executable,'-c',code,str(self.root),json.dumps(self.identity),self.key,json.dumps(self.binding),mode],capture_output=True,timeout=10)
                self.assertEqual(child.returncode,-signal.SIGKILL)
                slot=self.open()
                try:
                    j=SyntheticChunkJournal(slot,self.binding,legacy_state='new',synthetic_fixture=True)
                    if mode=='receipt':self.assertEqual(j.write(0,0,b'abcd')['schemaVersion'],1)
                    else:
                        with self.assertRaises(ChunkHeld):j.write(0,0,b'abcd')
                finally:slot.close()
