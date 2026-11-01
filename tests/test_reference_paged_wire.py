"""Synthetic bounded page framing; not a live authority protocol test."""
import json
import socket
import threading
import time
import unittest

from processing import reference_paged_wire as pages
from processing import reference_protocol as protocol
from processing.node_authority_session import SessionHeld


class BufferSocket:
    def __init__(self,data=b''):self.data=bytearray(data);self.written=bytearray()
    def settimeout(self,_value):pass
    def sendall(self,value):self.written.extend(value)
    def recv(self,size):
        value=bytes(self.data[:size]);del self.data[:size];return value


def frames(raw):
    result=[]
    while raw:
        size=int.from_bytes(raw[:4],'big');result.append(json.loads(raw[4:4+size]));raw=raw[4+size:]
    return result


def wire(items):
    value=bytearray()
    for item in items:
        encoded=pages._encode(item);value.extend(len(encoded).to_bytes(4,'big'));value.extend(encoded)
    return bytes(value)


class PagedWireTests(unittest.TestCase):
    def setUp(self):
        self.context=dict(protocol='reference-pages-v1',operation='commit',authority='a',epoch='b',job='c',
                          manifest='d',grant='e',operationId='f',nonce='a'*64,count=100)
        self.records=[dict(objectId=f'object-{i:05}',fileName=f'image-{i:05}.jpg',issueSha256='a'*64)
                      for i in range(100)]

    def test_missing_duplicate_reordered_tampered_and_oversize_fail_closed(self):
        output=BufferSocket();pages.send_pages(output,self.context,self.records,count=100,deadline=time.monotonic()+5)
        original=frames(bytes(output.written))
        variants=[original[:-1],[original[0],original[0],*original[1:]],
                  [dict(original[0],sequence=1),*original[1:]],
                  [*original[:-1],dict(original[-1],sha256='0'*64)]]
        for variant in variants:
            with self.subTest(variant=len(variant)),self.assertRaises(SessionHeld):
                pages.receive_pages(BufferSocket(wire(variant)),self.context,count=100,deadline=time.monotonic()+5)
        with self.assertRaises(SessionHeld):
            pages.receive_pages(BufferSocket((pages.PAGE_BYTES+1).to_bytes(4,'big')),self.context,count=100,deadline=time.monotonic()+5)
        with self.assertRaises(SessionHeld):
            pages.receive_pages(BufferSocket(bytes(output.written)),dict(self.context,nonce='b'*64),count=100,deadline=time.monotonic()+5)
        with self.assertRaisesRegex(SessionHeld,'trailing'):
            pages.receive_pages(BufferSocket(bytes(output.written)+b'x'),self.context,count=100,deadline=time.monotonic()+5,require_eof=True)
        duplicate=b'{"type":"page","type":"page"}'
        with self.assertRaises(SessionHeld):
            pages.receive_pages(BufferSocket(len(duplicate).to_bytes(4,'big')+duplicate),self.context,count=100,deadline=time.monotonic()+5)
        with self.assertRaisesRegex(SessionHeld,'deadline'):
            pages.receive_pages(BufferSocket(bytes(output.written)),self.context,count=100,deadline=time.monotonic()-1)
        immutable=pages.receive_pages(BufferSocket(bytes(output.written)),self.context,count=100,deadline=time.monotonic()+5,require_eof=True)
        self.assertIsInstance(immutable,tuple)
        self.assertTrue(all(type(item) is bytes for item in immutable))

    def test_10000_records_socketpair_and_full_json_bound(self):
        records=[dict(objectId=f'object-{i:05}',fileName=f'image-{i:05}-'+('x'*40)+'.jpg',issueSha256='a'*64)
                 for i in range(10000)]
        context=dict(self.context,count=10000)
        reader,writer=socket.socketpair();errors=[]
        deadline=time.monotonic()+15;started=time.monotonic();cpu=time.process_time()
        def send():
            try:pages.send_pages(writer,context,records,count=10000,deadline=deadline)
            except BaseException as error:errors.append(type(error).__name__)
            finally:writer.close()
        thread=threading.Thread(target=send)
        try:
            thread.start();received=pages.receive_pages(reader,context,count=10000,deadline=deadline,require_eof=True)
        finally:
            reader.close()
            if thread.ident is not None:thread.join(3)
            writer.close()
        self.assertFalse(thread.is_alive());self.assertEqual(errors,[]);self.assertEqual([json.loads(item) for item in received],records)
        body=protocol.canonical_json({'schemaVersion':1,'uploads':records})
        self.assertLessEqual(len(body),protocol.MAX_BODY_BYTES)
        print('synthetic 10000 records: body_bytes=%d elapsed_s=%.3f cpu_s=%.3f' %
              (len(body),time.monotonic()-started,time.process_time()-cpu),flush=True)
        with self.assertRaises(ValueError):
            protocol.decode_submission(b' '*(protocol.MAX_BODY_BYTES+1))

    def test_duplex_ready_and_trailing_frame_rejected_before_return(self):
        secret=b'synthetic-page-secret-'*2
        for inject in (False,True):
            reader,writer=socket.socketpair();errors=[];deadline=time.monotonic()+4
            def send():
                try:
                    if inject:
                        pages.send_pages(writer,self.context,self.records,count=100,deadline=deadline)
                        pages._write(writer,{'type':'unexpected'},deadline)
                        pages._read_page(writer,deadline)
                    else:
                        pages.send_duplex_pages(writer,self.context,self.records,count=100,deadline=deadline,secret=secret)
                except BaseException as error:errors.append(type(error).__name__)
                finally:writer.close()
            thread=threading.Thread(target=send)
            try:
                thread.start()
                if inject:
                    with self.assertRaises(SessionHeld):
                        pages.receive_duplex_pages(reader,self.context,count=100,deadline=deadline,secret=secret)
                else:
                    received=pages.receive_duplex_pages(reader,self.context,count=100,deadline=deadline,secret=secret)
                    self.assertEqual([json.loads(item) for item in received],self.records)
            finally:
                reader.close()
                if thread.ident is not None:thread.join(3)
                writer.close()
            self.assertFalse(thread.is_alive())
            if not inject:self.assertEqual(errors,[])

    def test_real_socket_deadline_is_bounded(self):
        reader,writer=socket.socketpair()
        try:
            with self.assertRaises((SessionHeld,OSError)):
                pages.receive_pages(reader,self.context,count=100,deadline=time.monotonic()+0.05)
        finally:reader.close();writer.close()
