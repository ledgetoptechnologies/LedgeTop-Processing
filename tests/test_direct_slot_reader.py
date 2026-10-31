import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from processing.direct_outbox_contract import validate_binding
from processing.direct_output_slot import SlotHeld, _identity, open_slot
from processing.direct_slot_capture import begin_capture, open_sealed_reader, verify_sealed
from tests.test_direct_outbox_contract import binding
from tests.test_direct_slot_capture import zip_bytes


@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux allocation required')
class LockedReaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700)
        fd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY)
        try:self.root_id=_identity(fd)
        finally:os.close(fd)
        self.binding=binding();self.binding['outputBudgetBytes']=65536;self.data=zip_bytes()
        slot=open_slot(self.root,self.root_id,validate_binding(self.binding),65536,max_budget=65536,create=True);slot.close()
        capture=begin_capture(self.root,self.root_id,self.binding,max_budget=65536,declared_length=len(self.data))
        try:self.seal=capture.consume(io.BytesIO(self.data))
        finally:capture.close()

    def reader(self):return open_sealed_reader(self.root,self.root_id,self.binding,max_budget=65536)
    def verify(self):return verify_sealed(self.root,self.root_id,self.binding,max_budget=65536)

    def test_stream_holds_exclusive_lock_until_closed(self):
        with self.reader() as reader:
            self.assertEqual(reader.length,len(self.data));self.assertEqual(reader.sha256,self.seal['sha256'])
            with self.assertRaises(SlotHeld):self.verify()
            chunks=[]
            while chunk:=reader.read(13):chunks.append(chunk)
            self.assertEqual(b''.join(chunks),self.data)
            with self.assertRaises(SlotHeld):self.verify()
        self.assertEqual(self.verify(),self.seal)

    def test_partial_close_or_transport_error_replays_from_start(self):
        with self.assertRaises(OSError):
            with self.reader() as reader:
                self.assertEqual(reader.read(5),self.data[:5])
                raise OSError('synthetic disconnected consumer')
        with self.reader() as reader:
            self.assertEqual(reader.read(1024),self.data)
            self.assertEqual(reader.read(1024),b'')
        self.assertEqual(self.verify(),self.seal)

    def test_mutation_of_already_read_prefix_aborts_before_last_chunk(self):
        path=self.root/('slot-'+validate_binding(self.binding)+'.alloc')
        with self.reader() as reader:
            self.assertEqual(reader.read(5),self.data[:5])
            with path.open('r+b') as stream:stream.write(b'changed')
            with self.assertRaisesRegex(SlotHeld,'reader_content_changed'):
                while reader.read(7):pass
        self.assertTrue(path.exists())
        with self.assertRaises(SlotHeld):self.verify()

    def test_changed_seal_path_holds_stream(self):
        path=self.root/('slot-'+validate_binding(self.binding)+'.seal');old=path.with_name(path.name+'.old')
        with self.reader() as reader:
            self.assertEqual(reader.read(5),self.data[:5]);path.rename(old);path.write_bytes(old.read_bytes())
            with self.assertRaises(SlotHeld):reader.read(5)
        self.assertTrue(old.exists())

    def test_killed_reader_releases_lock_and_replay_revalidates(self):
        ready=self.root/'reader-ready'
        code='''import json,sys,time
from pathlib import Path
from processing.direct_slot_capture import open_sealed_reader
root,identity,binding,ready=sys.argv[1:]
reader=open_sealed_reader(root,json.loads(identity),json.loads(binding),max_budget=65536)
reader.read(5);Path(ready).write_text('ready');time.sleep(30)
'''
        child=subprocess.Popen([sys.executable,'-c',code,str(self.root),json.dumps(self.root_id),json.dumps(self.binding),str(ready)],
                               stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            until=time.monotonic()+5
            while not ready.exists() and time.monotonic()<until:
                if child.poll() is not None:self.fail('reader child exited before barrier')
                time.sleep(.02)
            self.assertTrue(ready.exists())
            with self.assertRaises(SlotHeld):self.verify()
            child.kill();child.wait(timeout=5)
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=5)
        with self.reader() as reader:self.assertEqual(reader.read(1024),self.data)
        self.assertEqual(self.verify(),self.seal)

    def test_binding_mismatch_and_invalid_read_size_fail(self):
        with self.assertRaises(SlotHeld):
            open_sealed_reader(self.root,self.root_id,dict(self.binding,environment='staging'),max_budget=65536)
        with self.reader() as reader:
            for size in (0,-1,2**21,True):
                with self.assertRaises(SlotHeld):reader.read(size)
            self.assertEqual(reader.read(1024),self.data)
