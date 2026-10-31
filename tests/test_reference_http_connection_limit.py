import shutil, socket, ssl, subprocess, tempfile, threading, time, unittest
from pathlib import Path
from processing.reference_http import create_reference_http_server
from processing.reference_api import Response

@unittest.skipUnless(shutil.which("openssl"), "openssl required")
class ConnectionLimitTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  root=Path(self.tmp.name);cert=root/"cert";key=root/"key"
  subprocess.run(["openssl","req","-x509","-newkey","rsa:2048","-nodes","-days","1","-subj","/CN=localhost","-keyout",str(key),"-out",str(cert)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
  context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(cert,key)
  self.client=ssl._create_unverified_context();self.calls=[]
  outer=self
  class API:
   def handle(self,*args,**kwargs):outer.calls.append(1);return Response(200,{"ok":True})
  self.server=create_reference_http_server(API(),ssl_context=context,request_timeout=.5,max_connections=2)
  self.server.daemon_threads=False;self.server.block_on_close=True
  self.thread=threading.Thread(target=self.server.serve_forever,kwargs={"poll_interval":.02});self.thread.start();self.addCleanup(self.close)
  self.address=self.server.server_address
 def close(self):
  self.server.shutdown();self.server.server_close();self.thread.join(3);self.assertFalse(self.thread.is_alive())
 def test_slow_body_and_handshake_are_bounded_and_release_permits(self):
  a=self.client.wrap_socket(socket.create_connection(self.address,timeout=2),server_hostname="localhost")
  self.addCleanup(a.close)
  a.sendall(b"POST /x HTTP/1.1\r\nHost: localhost\r\nContent-Length: 8\r\n\r\na")
  b=socket.create_connection(self.address,timeout=2);self.addCleanup(b.close)
  time.sleep(.1)
  c=socket.create_connection(self.address,timeout=2);c.settimeout(2);self.addCleanup(c.close)
  self.assertEqual(c.recv(1),b"");self.assertEqual(self.calls,[])
  time.sleep(.7)
  d=self.client.wrap_socket(socket.create_connection(self.address,timeout=2),server_hostname="localhost");self.addCleanup(d.close)
  d.sendall(b"GET /x HTTP/1.1\r\nHost: localhost\r\n\r\n");self.assertIn(b"200",d.recv(512));self.assertEqual(self.calls,[1])
 def test_thread_start_failure_closes_socket_and_releases_slot(self):
  from unittest.mock import patch
  from http.server import ThreadingHTTPServer
  a,b=socket.socketpair();self.addCleanup(b.close)
  with patch.object(ThreadingHTTPServer,'process_request',side_effect=RuntimeError('synthetic')):
   with self.assertRaises(RuntimeError):self.server.process_request(a,('127.0.0.1',1))
  self.assertEqual(b.recv(1),b'')
  c=self.client.wrap_socket(socket.create_connection(self.address,timeout=2),server_hostname='localhost');self.addCleanup(c.close)
  c.sendall(b'GET /x HTTP/1.1\r\nHost: localhost\r\n\r\n');self.assertIn(b'200',c.recv(512))
 def test_accepted_socket_closed_if_timeout_setup_fails(self):
  from unittest.mock import Mock,patch
  sock=Mock();sock.settimeout.side_effect=OSError('timeout setup failed')
  listener=Mock();listener.accept.return_value=(sock,('127.0.0.1',1))
  server=create_reference_http_server(object(),max_connections=1)
  try:
   with patch.object(server,'socket',listener):
    with self.assertRaisesRegex(OSError,'timeout setup failed'):server.get_request()
   sock.close.assert_called_once_with()
  finally:server.server_close()

 def test_invalid_limit_fails_before_bind(self):
  for value in (0,257,True,"2"):
   with self.subTest(value=value),self.assertRaises(ValueError):create_reference_http_server(object(),max_connections=value)
