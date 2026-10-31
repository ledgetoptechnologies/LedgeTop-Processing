import base64
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile
from unittest.mock import patch

from processing.direct_outbox_contract import validate_binding
from processing.direct_output_slot import SlotHeld, _identity, open_slot
from processing.direct_slot_capture import begin_capture, verify_sealed
import processing.direct_slot_capture as capture_module
from tests.test_direct_outbox_contract import binding


def zip_bytes():
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as z:z.writestr('synthetic.txt',b'fixture')
    return stream.getvalue()


@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux allocation required')
class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700)
        fd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY)
        try:self.root_id=_identity(fd)
        finally:os.close(fd)
        self.binding=binding();self.binding['outputBudgetBytes']=65536;self.data=zip_bytes()

    def allocate(self,value):
        slot=open_slot(self.root,self.root_id,validate_binding(value),65536,max_budget=65536,create=True)
        slot.close()

    def begin(self,value=None,declared=None):
        return begin_capture(self.root,self.root_id,value or self.binding,max_budget=65536,declared_length=declared)

    def verify(self,value=None):
        return verify_sealed(self.root,self.root_id,value or self.binding,max_budget=65536)

    def test_stream_to_eof_seals_then_replay_reverifies(self):
        self.allocate(self.binding);capture=self.begin(declared=len(self.data))
        try:sealed=capture.consume(io.BytesIO(self.data))
        finally:capture.close()
        self.assertEqual(sealed['bytes'],len(self.data));self.assertEqual(self.verify(),sealed)
        self.assertEqual(self.verify(),sealed)
        with self.assertRaises(SlotHeld):self.begin(declared=len(self.data))
        with self.assertRaises(SlotHeld):open_slot(self.root,self.root_id,validate_binding(self.binding),65536,max_budget=65536)

    def test_unknown_length_is_counted_at_eof_and_zip_checked(self):
        self.allocate(self.binding);capture=self.begin()
        try:self.assertEqual(capture.consume(io.BytesIO(self.data))['bytes'],len(self.data))
        finally:capture.close()
        self.assertEqual(self.verify()['bytes'],len(self.data))

    def test_mismatch_truncation_invalid_zip_and_partial_are_held(self):
        for case in ('short','long','bad','interrupted'):
            with self.subTest(case=case):
                value=dict(self.binding,engineId='engine-'+case);self.allocate(value)
                capture=self.begin(value,declared=len(self.data))
                try:
                    if case=='interrupted':
                        class Broken:
                            def read(self,size):raise OSError('network lost')
                        with self.assertRaises(OSError):capture.consume(Broken())
                    else:
                        data=self.data[:-1] if case=='short' else self.data+b'x' if case=='long' else b'x'*len(self.data)
                        with self.assertRaises(SlotHeld):capture.consume(io.BytesIO(data))
                finally:capture.close()
                with self.assertRaises(SlotHeld):self.verify(value)
                with self.assertRaises(SlotHeld):self.begin(value)
                self.assertTrue((self.root/('slot-'+validate_binding(value)+'.alloc')).is_file())

    def test_after_seal_byte_or_binding_mutation_is_not_accepted(self):
        self.allocate(self.binding);capture=self.begin()
        try:capture.consume(io.BytesIO(self.data))
        finally:capture.close()
        path=self.root/('slot-'+validate_binding(self.binding)+'.alloc')
        with path.open('r+b') as stream:stream.write(b'not a zip')
        with self.assertRaises(SlotHeld):self.verify()
        altered=dict(self.binding,environment='staging')
        with self.assertRaises(SlotHeld):self.verify(altered)
        self.assertTrue(path.exists())

    def test_verification_does_not_hold_a_lock_after_return(self):
        self.allocate(self.binding);capture=self.begin()
        try:sealed=capture.consume(io.BytesIO(self.data))
        finally:capture.close()
        self.assertEqual(self.verify(),sealed)
        # Once verify returns, it no longer owns the slot or pins its bytes.
        path=self.root/('slot-'+validate_binding(self.binding)+'.alloc')
        with path.open('r+b') as stream:stream.write(b'changed')
        with self.assertRaises(SlotHeld):self.verify()

    def test_valid_shorter_archive_can_seal_without_trusted_length(self):
        full=io.BytesIO();short=io.BytesIO()
        with zipfile.ZipFile(full,'w') as archive:
            archive.writestr('first.txt',b'one');archive.writestr('second.txt',b'two')
        with zipfile.ZipFile(short,'w') as archive:archive.writestr('first.txt',b'one')
        self.assertLess(len(short.getvalue()),len(full.getvalue()))
        self.allocate(self.binding);capture=self.begin()
        try:sealed=capture.consume(io.BytesIO(short.getvalue()))
        finally:capture.close()
        # Both archives are structurally valid. EOF and local hash alone do
        # not establish the intended remote archive's completeness.
        self.assertEqual(sealed['bytes'],len(short.getvalue()))
        self.assertEqual(self.verify(),sealed)

    def test_interrupted_seal_record_is_held_and_retained(self):
        self.allocate(self.binding);capture=self.begin()
        original=capture_module.os.write
        def partial(fd,data):return original(fd,data[:max(1,len(data)//2)])
        try:
            with patch.object(capture_module.os,'write',side_effect=partial):
                with self.assertRaises(SlotHeld):capture.consume(io.BytesIO(self.data))
        finally:capture.close()
        path=self.root/('slot-'+validate_binding(self.binding)+'.seal')
        self.assertTrue(path.exists())
        with self.assertRaises(SlotHeld):self.verify()
        with self.assertRaises(SlotHeld):self.begin()
        self.assertTrue(path.exists())

    def test_killed_process_at_write_sync_and_journal_boundaries(self):
        code='''import base64,io,json,os,sys,time
from pathlib import Path
import processing.direct_slot_capture as capture
import processing.direct_output_slot as slot
root,identity,raw,ready,phase,data=sys.argv[1:];identity=json.loads(identity);binding=json.loads(raw)
def stop():Path(ready).write_text('ready');time.sleep(30)
original_write=slot.os.pwrite;original_digest=capture._digest_and_zip;original_persist=capture._persist
def write(fd,data,offset):
 result=original_write(fd,data,offset)
 if phase=='write':stop()
 return result
def digest(obj,length):
 if phase=='after-sync':stop()
 return original_digest(obj,length)
def persist(root,identity,name,record):
 if name.endswith('.seal') and phase=='before-seal':stop()
 result=original_persist(root,identity,name,record)
 if name.endswith('.seal') and phase=='after-seal':stop()
 return result
slot.os.pwrite=write;capture._digest_and_zip=digest;capture._persist=persist
obj=capture.begin_capture(root,identity,binding,max_budget=65536)
obj.consume(io.BytesIO(base64.b64decode(data)))
'''
        for phase in ('write','after-sync','before-seal','after-seal'):
            with self.subTest(phase=phase):
                value=dict(self.binding,engineId='engine-'+phase);self.allocate(value)
                ready=self.root/('ready-'+phase)
                child=subprocess.Popen([sys.executable,'-c',code,str(self.root),json.dumps(self.root_id),json.dumps(value),str(ready),phase,
                                        base64.b64encode(self.data).decode()],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                try:
                    until=time.monotonic()+5
                    while not ready.exists() and time.monotonic()<until:
                        if child.poll() is not None:self.fail('capture child exited before barrier')
                        time.sleep(.02)
                    self.assertTrue(ready.exists());child.kill();child.wait(timeout=5)
                finally:
                    if child.poll() is None:child.kill();child.wait(timeout=5)
                if phase=='after-seal':self.assertEqual(self.verify(value)['bytes'],len(self.data))
                else:
                    with self.assertRaises(SlotHeld):self.verify(value)
                    with self.assertRaises(SlotHeld):self.begin(value)
                self.assertTrue((self.root/('slot-'+validate_binding(value)+'.alloc')).exists())
