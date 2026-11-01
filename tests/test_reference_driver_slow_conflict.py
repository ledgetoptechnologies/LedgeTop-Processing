"""A slow conflict response body must not stall the worker driver."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time
import unittest
from urllib.request import urlopen

from processing.reference_driver import DriverHeld, ReferenceDriver


class SlowConflictTests(unittest.TestCase):
    def test_conflict_headers_are_processed_without_waiting_for_body(self):
        release=threading.Event();started=threading.Event()
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200 if self.path.endswith('/contradiction') else 409)
                self.send_header('Content-Type','application/json')
                self.send_header('Content-Length','100')
                if self.path.endswith('/inspection'):
                    self.send_header('X-LedgeTop-Reference-Inspection','1')
                if self.path.endswith('/duplicate'):
                    self.send_header('X-LedgeTop-Reference-Inspection','1')
                    self.send_header('X-LedgeTop-Reference-Inspection','1')
                if self.path.endswith('/malformed'):
                    self.send_header('X-LedgeTop-Reference-Inspection','unexpected')
                if self.path.endswith('/contradiction'):
                    self.send_header('X-LedgeTop-Reference-Inspection','1')
                self.end_headers()
                try:self.wfile.write(b'{');self.wfile.flush()
                except OSError:pass
                started.set();release.wait(3)
            def log_message(self,*_):pass
        server=None;thread=None;running=False
        try:
            server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
            server.daemon_threads=False;server.block_on_close=True
            thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01})
            thread.start();running=True
            driver=object.__new__(ReferenceDriver)
            driver.origin='http://127.0.0.1:'+str(server.server_port)
            driver._bearer='synthetic-worker-token';driver._open=urlopen
            begin=time.monotonic()
            with self.assertRaises(DriverHeld) as raised:
                driver._request('GET','/inspection')
            self.assertEqual(str(raised.exception),'stored_reference_inspection')
            self.assertLess(time.monotonic()-begin,1.5)
            self.assertTrue(started.wait(1))
            begin=time.monotonic()
            self.assertEqual(driver._request('GET','/ordinary'),(409,None))
            self.assertLess(time.monotonic()-begin,1.5)
            for path in ('/duplicate','/malformed','/contradiction'):
                begin=time.monotonic()
                with self.assertRaises(DriverHeld) as ambiguous:
                    driver._request('GET',path)
                self.assertEqual(str(ambiguous.exception),'reference_inspection_protocol_error')
                self.assertLess(time.monotonic()-begin,1.5)
        finally:
            release.set()
            if server is not None:
                if running:server.shutdown()
                handlers=list(server._threads) if isinstance(server._threads,list) else []
                server.server_close()
                if running:thread.join(timeout=3)
                self.assertFalse(running and thread.is_alive())
                self.assertFalse(any(handler.is_alive() for handler in handlers))


if __name__ == '__main__': unittest.main()
