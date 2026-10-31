"""Synthetic direct component tests: never attached to coordinator ingress."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
from unittest.mock import patch
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

    def test_marker_created_between_scan_and_lock_blocks_ordinary_open(self):
        import processing.direct_output_slot as slots
        key='b'*64
        slot=open_slot(self.root,self.identity,key,65536,max_budget=65536,create=True);slot.close()
        arrived=threading.Event();release=threading.Event();original=slots.fcntl.flock
        def barrier(fd,operation):
            if threading.current_thread().name.startswith('ordinary'):
                arrived.set()
                if not release.wait(5):raise RuntimeError('lock barrier timeout')
            return original(fd,operation)
        with patch.object(slots.fcntl,'flock',side_effect=barrier):
            with ThreadPoolExecutor(max_workers=1,thread_name_prefix='ordinary') as pool:
                future=pool.submit(open_slot,self.root,self.identity,key,65536,max_budget=65536)
                try:
                    self.assertTrue(arrived.wait(5))
                    chunk=open_synthetic_chunk_slot(self.root,self.identity,key,65536,max_budget=65536,
                        binding=self.binding,legacy_state='new',synthetic_fixture=True)
                    chunk.close()
                finally:release.set()
                with self.assertRaisesRegex(Exception,'slot_chunk_journal_requires_inspection'):future.result(timeout=5)
        # Reopening the dedicated fixture slot proves the rejected open released its lock.
        chunk=open_synthetic_chunk_slot(self.root,self.identity,key,65536,max_budget=65536,
            binding=self.binding,legacy_state='new',synthetic_fixture=True);chunk.close()

    def test_marker_race_on_create_holds_new_allocation(self):
        import processing.direct_output_slot as slots
        key='c'*64;arrived=threading.Event();release=threading.Event();original=slots.fcntl.flock
        def barrier(fd,operation):
            if threading.current_thread().name.startswith('ordinary-create'):
                arrived.set()
                if not release.wait(5):raise RuntimeError('lock barrier timeout')
            return original(fd,operation)
        with patch.object(slots.fcntl,'flock',side_effect=barrier):
            with ThreadPoolExecutor(max_workers=1,thread_name_prefix='ordinary-create') as pool:
                future=pool.submit(open_slot,self.root,self.identity,key,65536,max_budget=65536,create=True)
                try:
                    self.assertTrue(arrived.wait(5))
                    (self.root/('slot-'+key+'.alloc.chunks-v1.partial')).write_bytes(b'partial')
                finally:release.set()
                with self.assertRaisesRegex(Exception,'slot_chunk_journal_requires_inspection'):future.result(timeout=5)
        self.assertTrue((self.root/('slot-'+key+'.alloc')).exists())
        self.assertFalse((self.root/('slot-'+key+'.json')).exists())

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
    def test_header_reopens_without_enumerating_4096_records(self):
        import resource
        from processing.viewer_chunk_journal import MAX_CHUNKS
        other='b'*64;data=b'x'*MAX_CHUNKS
        binding=dict(self.binding,archiveBytes=len(data),archiveSha256=hashlib.sha256(data).hexdigest())
        slot=open_slot(self.root,self.identity,other,len(data),max_budget=len(data),create=True)
        try:
            journal=SyntheticChunkJournal(slot,binding,legacy_state='new',synthetic_fixture=True)
            with patch('processing.viewer_chunk_journal.os.listdir',side_effect=AssertionError('quadratic scan')):
                for i in range(MAX_CHUNKS):
                    journal=SyntheticChunkJournal(slot,binding,legacy_state='new',synthetic_fixture=True)
                    journal.write(i,i,b'x')
                self.assertEqual(journal.finish(MAX_CHUNKS)['sha256'],binding['archiveSha256'])
            print('synthetic chunks=%d journal_entries=%d rss_kib=%d'%(MAX_CHUNKS,MAX_CHUNKS*2+2,resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),flush=True)
        finally:slot.close()

    def test_missing_header_with_existing_marker_still_holds(self):
        key='b'*64;slot=open_slot(self.root,self.identity,key,65536,max_budget=65536,create=True)
        try:
            (self.root/('slot-'+key+'.alloc.chunks-v1.partial')).write_bytes(b'uncertain')
            with self.assertRaisesRegex(ChunkHeld,'journal_requires_inspection'):
                SyntheticChunkJournal(slot,self.binding,legacy_state='new',synthetic_fixture=True)
        finally:slot.close()
    def test_existing_modified_header_does_not_trigger_scan_or_repair(self):
        path=self.root/('slot-'+self.key+'.alloc.chunks-v1.header')
        original=path.read_bytes();path.write_bytes(b'{}')
        with patch('processing.viewer_chunk_journal.os.listdir',side_effect=AssertionError('unexpected scan')):
            with self.assertRaises(Exception):self.open()
        self.assertEqual(path.read_bytes(),b'{}');self.assertNotEqual(original,b'{}')
    def test_existing_header_partial_chunk_intent_remains_held(self):
        slot=self.open()
        try:
            journal=SyntheticChunkJournal(slot,self.binding,legacy_state='new',synthetic_fixture=True)
            intent,_=journal._names(0)
            (self.root/intent).write_bytes(b'partial')
            with patch('processing.viewer_chunk_journal.os.listdir',side_effect=AssertionError('unexpected scan')):
                reopened=SyntheticChunkJournal(slot,self.binding,legacy_state='new',synthetic_fixture=True)
                with self.assertRaisesRegex(ChunkHeld,'chunk_requires_inspection'):reopened.write(0,0,b'abcd')
            self.assertEqual((self.root/intent).read_bytes(),b'partial')
        finally:slot.close()
