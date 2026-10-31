"""Kill the disposable coordinator during a real archive capture request."""
import multiprocessing
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid

from tests.test_viewer_http import BOUNDARY, TOKEN, multipart
from tests.test_viewer_service import synthetic_zip


def stalled_coordinator(root, control, stalled):
    import tests.test_viewer_archive_http_recovery as fixture
    original=fixture.FakeEngine

    class Engine(original):
        def download(self, identifier):
            response=super().download(identifier)
            read=response.read;calls=0
            def blocked(size=-1):
                nonlocal calls
                calls+=1
                if calls==1:return read(7)
                stalled.set()
                # A timeout prevents a stranded fixture if the controller fails.
                threading.Event().wait(15)
                raise RuntimeError('synthetic transfer stall expired')
            response.read=blocked
            return response

    fixture.FakeEngine=Engine
    fixture.synthetic_coordinator(root,control,'normal')


class ArchiveTransferKillTest(unittest.TestCase):
    def test_killed_during_engine_download_retains_uncertain_partial(self):
        context=multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as root:
            parent,child=context.Pipe();stalled=context.Event()
            process=context.Process(target=stalled_coordinator,args=(root,child,stalled));process.start();child.close()
            active=[(process,parent)];request_thread=None
            identifier=str(uuid.uuid4())
            def request(port,route,body=None,headers=None):
                return urlopen(Request('http://127.0.0.1:%d%s?token=%s'%(port,route,TOKEN),data=body,headers=headers or {}),timeout=8)
            try:
                self.assertTrue(parent.poll(10));_,port,source=parent.recv();self.assertEqual(source,synthetic_zip())
                headers={'Content-Type':'multipart/form-data; boundary='+BOUNDARY}
                with request(port,'/task/new/init',multipart([('name',None,b'Synthetic'),('options',None,b'[]')]),dict(headers,**{'set-uuid':identifier})):pass
                with request(port,'/task/new/upload/'+identifier,multipart([('images','photo.jpg',b'synthetic')]),headers):pass
                with request(port,'/task/new/commit/'+identifier,b''):pass
                for command in ('tick','finish'):
                    parent.send((command,identifier));self.assertTrue(parent.poll(5));self.assertEqual(parent.recv(),'ok')
                result=[]
                def transfer():
                    try:
                        with request(port,'/task/'+identifier+'/download/all.zip') as response:result.append(response.read())
                    except Exception as error:result.append(type(error).__name__)
                request_thread=threading.Thread(target=transfer);request_thread.start()
                self.assertTrue(stalled.wait(8),'engine transfer did not reach stall')
                partial=list(Path(root).rglob('all.zip.partial'));self.assertEqual(len(partial),1)
                # Coordinator is this test's child, still live, and blocked in
                # the second engine read before receipt or archive publication.
                self.assertTrue(process.is_alive());process.kill();process.join(5);self.assertFalse(process.is_alive())
                self.assertNotEqual(process.exitcode,0)
                request_thread.join(8);self.assertFalse(request_thread.is_alive());self.assertEqual(len(result),1)
                self.assertIsInstance(result[0],str)
                retained=partial[0].read_bytes();parent.close();active.clear()
                from tests.test_viewer_archive_http_recovery import synthetic_coordinator
                parent,child=context.Pipe();process=context.Process(target=synthetic_coordinator,args=(root,child,'normal'))
                process.start();child.close();active.append((process,parent))
                self.assertTrue(parent.poll(10));_,port,source_after=parent.recv();self.assertEqual(source_after,source)
                with self.assertRaises(HTTPError) as held:request(port,'/task/'+identifier+'/download/all.zip')
                self.assertIn(held.exception.code,(409,503));self.assertEqual(partial[0].read_bytes(),retained)
                self.assertFalse(list(Path(root).rglob('all.zip.receipt')))
                parent.send(('stop',None));process.join(5);self.assertFalse(process.is_alive());self.assertEqual(process.exitcode,0)
                parent.close();active.clear()
            finally:
                for process,parent in active:
                    if process.is_alive():process.kill()
                    process.join(5);self.assertFalse(process.is_alive());parent.close()
                if request_thread is not None:request_thread.join(10);self.assertFalse(request_thread.is_alive())
