import array,json,os,socket,struct,unittest
from ops.fixture_bootstrap import BootstrapError,bootstrap_descriptors,read_bootstrap

class Tests(unittest.TestCase):
    def test_valid_two_descriptors_are_cloexec(self):
        a,b=socket.socketpair(); owned=os.open('/dev/null',os.O_RDONLY)
        try:
            payload=json.dumps({'bootstrap':'x'*40}).encode()
            a.sendmsg([struct.pack('!I',len(payload))+payload],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[owned,owned]))])
            token,fds=read_bootstrap(b)
            try:
                self.assertEqual(token,'x'*40); self.assertEqual(len(fds),2)
                self.assertTrue(all(not os.get_inheritable(fd) for fd in fds))
            finally:
                for fd in fds: os.close(fd)
        finally: os.close(owned); a.close(); b.close()
    def test_invalid_fd_counts_and_truncation_close_received_fds(self):
        for count in (1,3,20):
            with self.subTest(count=count):
                a,b=socket.socketpair(); owned=os.open('/dev/null',os.O_RDONLY)
                try:
                    baseline=len(os.listdir('/proc/self/fd'))
                    payload=json.dumps({'bootstrap':'x'*40}).encode()
                    a.sendmsg([struct.pack('!I',len(payload))+payload],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[owned]*count))])
                    with self.assertRaises(BootstrapError): read_bootstrap(b)
                    self.assertEqual(len(os.listdir('/proc/self/fd')),baseline)
                finally: os.close(owned); a.close(); b.close()
    def test_setup_exception_closes_received_descriptors(self):
        a,b=socket.socketpair(); owned=os.open('/dev/null',os.O_RDONLY)
        try:
            baseline=len(os.listdir('/proc/self/fd'))
            payload=json.dumps({'bootstrap':'x'*40}).encode()
            a.sendmsg([struct.pack('!I',len(payload))+payload],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[owned,owned]))])
            with self.assertRaisesRegex(RuntimeError,'setup'):
                with bootstrap_descriptors(b): raise RuntimeError('setup')
            self.assertEqual(len(os.listdir('/proc/self/fd')),baseline)
        finally: os.close(owned); a.close(); b.close()
    def test_truncated_frame_closes_received_fds(self):
        a,b=socket.socketpair(); owned=os.open('/dev/null',os.O_RDONLY)
        try:
            baseline=len(os.listdir('/proc/self/fd'))
            a.sendmsg([b'\x00\x00'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[owned,owned]))])
            a.shutdown(socket.SHUT_WR)
            with self.assertRaises(EOFError): read_bootstrap(b)
            self.assertEqual(len(os.listdir('/proc/self/fd')),baseline)
        finally: os.close(owned); a.close(); b.close()

if __name__=='__main__': unittest.main()
