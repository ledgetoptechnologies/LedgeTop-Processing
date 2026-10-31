import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from processing import direct_output_slot as slots


@unittest.skipUnless(os.name == 'posix' and hasattr(os,'posix_fallocate'), 'Linux allocation required')
class OutputSlotTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.root.chmod(0o700)
        fd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY)
        try:self.identity=slots._identity(fd)
        finally:os.close(fd)
        self.digest='a'*64;self.size=65536
        self.filename='slot-'+self.digest+'.alloc';self.metadata='slot-'+self.digest+'.json'

    def open(self,*,create=False):
        return slots.open_slot(self.root,self.identity,self.digest,self.size,max_budget=self.size,create=create)

    def test_same_inode_preallocated_and_bounds_enforced(self):
        slot=self.open(create=True)
        try:
            info=(self.root/self.filename).stat()
            self.assertEqual(info.st_size,self.size);self.assertGreaterEqual(info.st_blocks*512,self.size)
            inode=info.st_ino;slot.write_at(0,b'first');slot.write_at(self.size-4,b'last');slot.sync()
            with self.assertRaisesRegex(slots.SlotHeld,'out_of_bounds'):slot.write_at(self.size-1,b'bad')
            self.assertEqual((self.root/self.filename).stat().st_ino,inode)
        finally:slot.close()
        reopened=self.open()
        try:
            self.assertEqual((self.root/self.filename).stat().st_ino,inode)
            self.assertEqual(os.pread(reopened._fd,5,0),b'first')
            self.assertEqual(os.pread(reopened._fd,4,self.size-4),b'last')
        finally:reopened.close()

    def test_untrusted_budget_cannot_exceed_configured_limit(self):
        with self.assertRaisesRegex(slots.SlotHeld,'slot_budget_invalid'):
            slots.open_slot(self.root,self.identity,self.digest,self.size,max_budget=self.size-1,create=True)
        self.assertFalse((self.root/self.filename).exists())

    def test_process_exit_restart_reopens_same_slot(self):
        code='''import json,sys
from processing.direct_output_slot import open_slot
root,ident,digest,mode=sys.argv[1:];slot=open_slot(root,json.loads(ident),digest,65536,max_budget=65536,create=(mode=='create'))
slot.write_at(11,b'persist');slot.sync();slot.close()
'''
        for mode in ('create','reopen'):
            subprocess.run([sys.executable,'-c',code,str(self.root),__import__('json').dumps(self.identity),self.digest,mode],
                           check=True,capture_output=True,timeout=5)
        slot=self.open()
        try:self.assertEqual(os.pread(slot._fd,7,11),b'persist')
        finally:slot.close()

    def test_other_process_cannot_open_owned_slot(self):
        slot=self.open(create=True)
        code='''import json,sys
from processing.direct_output_slot import open_slot
open_slot(sys.argv[1],json.loads(sys.argv[2]),sys.argv[3],65536,max_budget=65536)
'''
        try:
            result=subprocess.run([sys.executable,'-c',code,str(self.root),__import__('json').dumps(self.identity),self.digest],
                                  capture_output=True,text=True,timeout=5)
            self.assertNotEqual(result.returncode,0);self.assertIn('slot_busy',result.stderr)
        finally:slot.close()

    def test_allocation_failure_leaves_ambiguous_slot_never_reuses(self):
        with patch.object(slots.os,'posix_fallocate',side_effect=OSError(28,'synthetic ENOSPC')):
            with self.assertRaisesRegex(slots.SlotHeld,'operation_uncertain'):self.open(create=True)
        self.assertTrue((self.root/self.filename).is_file());self.assertFalse((self.root/self.metadata).exists())
        with self.assertRaises(slots.SlotHeld):self.open()
        with self.assertRaises(slots.SlotHeld):self.open(create=True)
        self.assertTrue((self.root/self.filename).exists())

    @unittest.skipUnless(os.environ.get('DIRECT_SLOT_REAL_ENOSPC')=='1','requires bounded small test filesystem')
    def test_real_filesystem_enospc_retains_uncommitted_file(self):
        info=os.statvfs(self.root);free=info.f_bavail*info.f_frsize
        if not 0 < free <= 16*1024*1024:self.skipTest('test filesystem is not bounded')
        with self.assertRaises(slots.SlotHeld):
            slots.open_slot(self.root,self.identity,self.digest,free+1024*1024,max_budget=free+1024*1024,create=True)
        self.assertTrue((self.root/self.filename).exists())
        self.assertFalse((self.root/self.metadata).exists())

    def test_process_killed_after_allocation_retains_uncommitted_file(self):
        ready=self.root/'ready'
        code='''import json,os,sys,time
from pathlib import Path
import processing.direct_output_slot as module
root,identity,digest,ready=sys.argv[1:];original=module.os.fsync;calls=0
def pause(fd):
 global calls
 calls+=1
 if calls==2:
  Path(ready).write_text('ready');time.sleep(30)
 return original(fd)
module.os.fsync=pause
module.open_slot(root,json.loads(identity),digest,65536,max_budget=65536,create=True)
'''
        child=subprocess.Popen([sys.executable,'-c',code,str(self.root),__import__('json').dumps(self.identity),self.digest,str(ready)],
                               stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            end=time.monotonic()+5
            while not ready.exists() and time.monotonic()<end:
                if child.poll() is not None:self.fail('allocation child exited before barrier')
                time.sleep(.02)
            self.assertTrue(ready.exists());child.kill();child.wait(timeout=5)
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=5)
        self.assertEqual((self.root/self.filename).stat().st_size,self.size)
        self.assertFalse((self.root/self.metadata).exists())
        with self.assertRaises(slots.SlotHeld):self.open()

    def test_metadata_failure_keeps_preallocated_file_for_inspection(self):
        original=slots.os.fsync;calls=[]
        def fail_metadata(fd):
            calls.append(fd)
            if len(calls)==3:raise OSError('synthetic metadata fsync failure')
            return original(fd)
        with patch.object(slots.os,'fsync',side_effect=fail_metadata):
            with self.assertRaises(slots.SlotHeld):self.open(create=True)
        self.assertEqual((self.root/self.filename).stat().st_size,self.size)
        self.assertTrue((self.root/self.metadata).exists())
        reopened=self.open();reopened.close()  # complete metadata can be revalidated and resynced

    def test_file_metadata_root_and_mount_substitution_fail(self):
        slot=self.open(create=True)
        moved=self.root/(self.filename+'.retained');os.rename(self.root/self.filename,moved)
        (self.root/self.filename).write_bytes(b'foreign')
        with self.assertRaises(slots.SlotHeld):slot.write_at(0,b'x')
        slot.close()
        with self.assertRaises(slots.SlotHeld):self.open()
        self.assertTrue(moved.exists())
        with self.assertRaises(slots.SlotHeld):
            slots.open_slot(self.root,[self.identity[0],self.identity[1],self.identity[2]+1],self.digest,self.size,max_budget=self.size)

    def test_metadata_replacement_detected_before_write(self):
        slot=self.open(create=True);old=self.root/(self.metadata+'.retained')
        os.rename(self.root/self.metadata,old)
        (self.root/self.metadata).write_bytes(old.read_bytes())
        with self.assertRaises(slots.SlotHeld):slot.write_at(0,b'x')
        slot.close();self.assertTrue(old.exists())

    def test_changed_root_mode_and_wrong_budget_are_held(self):
        slot=self.open(create=True);slot.close()
        with self.assertRaises(slots.SlotHeld):
            slots.open_slot(self.root,self.identity,self.digest,self.size+1,max_budget=self.size+1)
        self.root.chmod(0o755)
        try:
            with self.assertRaisesRegex(slots.SlotHeld,'root_identity_changed'):self.open()
        finally:self.root.chmod(0o700)
        self.assertTrue((self.root/self.filename).exists())

    def test_active_root_path_replacement_holds_writes(self):
        slot=self.open(create=True)
        moved=self.root.with_name(self.root.name+'-held')
        os.rename(self.root,moved);self.root.mkdir(mode=0o700)
        try:
            with self.assertRaisesRegex(slots.SlotHeld,'root_identity_changed'):slot.write_at(0,b'x')
        finally:
            slot.close();self.root.rmdir();os.rename(moved,self.root)
