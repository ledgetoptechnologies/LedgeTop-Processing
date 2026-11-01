import array,contextlib,json,os,select,signal,socket,sqlite3,struct,subprocess,sys,tempfile,time,unittest
from pathlib import Path
from tests.test_separate_namespace_owner import exchange,parent_birth,read_exact

class OwnedFixture:
    def __init__(self,journal,previous=()):
        self.client,self.control=socket.socketpair(); self.client.settimeout(10)
        self.child=None; self.initfd=None; self.wrapperfd=None
        helper=Path(__file__).with_name('separate_namespace_owner_fixture.py')
        try:
            with contextlib.ExitStack() as stack:
                fds=[]
                for name in ('net','user','pid'):
                    fd=os.open('/proc/self/ns/'+name,os.O_RDONLY|os.O_CLOEXEC); stack.callback(os.close,fd); fds.append(fd)
                args=['unshare','--user','--map-root-user','--net','--pid','--fork','--kill-child=KILL',sys.executable,str(helper),
                      *(str(fd) for fd in fds),str(self.control.fileno()),'run',journal]
                self.child=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,
                    start_new_session=True,pass_fds=tuple(fds)+(self.control.fileno(),))
            self.control.close()
            self.secret='synthetic-control-'+'z'*40
            packet=json.dumps({'bootstrap':self.secret}).encode()
            frame=struct.pack('!I',len(packet))+packet
            ancillary=[] if not previous else [(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',previous))]
            sent=self.client.sendmsg([frame],ancillary)
            if sent<len(frame): self.client.sendall(frame[sent:])
            size=struct.unpack('!I',read_exact(self.client,4))[0]
            if not 0<size<=4096: raise ValueError('bad bootstrap response')
            self.ready=json.loads(read_exact(self.client,size))
            init=self.ready['initHostPid']; identity=parent_birth(init)
            if identity[0]!=self.child.pid: raise RuntimeError('fixture init parent changed')
            self.initfd=os.pidfd_open(init,0)
            if parent_birth(init)!=identity: raise RuntimeError('fixture init identity changed')
            self.wrapperfd=os.pidfd_open(self.child.pid,0)
        except BaseException:
            self.cleanup(); raise
    def cmd(self,action,token=None):
        return exchange(self.client,{'token':self.secret if token is None else token,'action':action})
    def kill(self):
        signal.pidfd_send_signal(self.wrapperfd,signal.SIGKILL)
        self.observe_stopped()
    def observe_stopped(self):
        if not select.select([self.initfd],[],[],5)[0]: raise TimeoutError('init not exited')
        deadline=time.monotonic()+5
        while os.waitid(os.P_PID,self.child.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT) is None:
            if time.monotonic()>=deadline: raise TimeoutError('wrapper not exited')
            time.sleep(.02)
        try: os.killpg(self.child.pid,signal.SIGKILL)
        except ProcessLookupError: pass
        # Keep the exact wrapper zombie unreaped so its pidfd exposes PID
        # and birth identity to the next trusted namespace owner.
    def cleanup(self):
        self.client.close(); self.control.close()
        if self.initfd is not None:
            try: signal.pidfd_send_signal(self.initfd,signal.SIGKILL)
            except ProcessLookupError: pass
        if self.child is not None and self.child.returncode is None:
            try: os.killpg(self.child.pid,signal.SIGKILL)
            except ProcessLookupError: pass
            self.child.communicate(timeout=3)
        for fd in (self.initfd,self.wrapperfd):
            if fd is not None: os.close(fd)
        self.initfd=self.wrapperfd=None

class Tests(unittest.TestCase):
    def test_stop_then_new_owner_requires_pidfds_and_authenticated_open(self):
        with tempfile.TemporaryDirectory() as root:
            journal=str(Path(root)/'journal.sqlite'); first=second=None
            try:
                first=OwnedFixture(journal)
                self.assertTrue(first.ready['recoverable'])
                self.assertFalse(first.cmd('status')['gate']['open'])
                self.assertEqual(first.cmd('open','wrong'),{'error':'unauthorized'})
                self.assertTrue(first.cmd('open')['gate']['open'])
                self.assertTrue(first.cmd('drain')['gate']['drained'])
                self.assertTrue(first.cmd('stop')['stopReady'])
                first.kill()
                second=OwnedFixture(journal,(first.initfd,first.wrapperfd))
                self.assertTrue(second.ready['recoverable'])
                self.assertFalse(second.cmd('status')['gate']['open'])
                self.assertEqual(second.cmd('open','wrong'),{'error':'unauthorized'})
                self.assertTrue(second.cmd('open')['gate']['open'])
            finally:
                try:
                    if second is not None: second.cleanup()
                finally:
                    if first is not None: first.cleanup()
    def test_stop_intent_with_missing_pidfds_does_not_reopen(self):
        with tempfile.TemporaryDirectory() as root:
            journal=str(Path(root)/'journal.sqlite'); first=second=None
            try:
                first=OwnedFixture(journal); first.cmd('open'); first.cmd('drain'); first.cmd('stop'); first.kill()
                second=OwnedFixture(journal)
                self.assertFalse(second.ready['recoverable'])
                self.assertEqual(second.cmd('open'),{'error':'recovery_unverified'})
            finally:
                try:
                    if second is not None: second.cleanup()
                finally:
                    if first is not None: first.cleanup()
    def test_stop_intent_with_live_pidfds_does_not_reopen(self):
        with tempfile.TemporaryDirectory() as root:
            journal=str(Path(root)/'journal.sqlite'); first=second=None
            try:
                first=OwnedFixture(journal); first.cmd('open'); first.cmd('drain'); first.cmd('stop')
                second=OwnedFixture(journal,(first.initfd,first.wrapperfd))
                self.assertFalse(second.ready['recoverable'])
                self.assertEqual(second.cmd('open'),{'error':'recovery_unverified'})
            finally:
                try:
                    if second is not None: second.cleanup()
                finally:
                    if first is not None: first.cleanup()
    def test_unrelated_dead_wrapper_pidfd_does_not_reopen(self):
        with tempfile.TemporaryDirectory() as root:
            journal=str(Path(root)/'journal.sqlite'); first=second=other=None; otherfd=None
            try:
                first=OwnedFixture(journal); first.cmd('open'); first.cmd('drain'); first.cmd('stop'); first.kill()
                other=subprocess.Popen([sys.executable,'-c','pass'],start_new_session=True)
                otherfd=os.pidfd_open(other.pid,0)
                deadline=time.monotonic()+4
                while os.waitid(os.P_PID,other.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT) is None:
                    if time.monotonic()>=deadline: raise TimeoutError('unrelated fixture exit')
                    time.sleep(.02)
                second=OwnedFixture(journal,(first.initfd,otherfd))
                self.assertFalse(second.ready['recoverable'])
                self.assertEqual(second.cmd('open'),{'error':'recovery_unverified'})
            finally:
                try:
                    if second is not None: second.cleanup()
                finally:
                    try:
                        if other is not None:
                            if other.returncode is None:
                                try: os.killpg(other.pid,signal.SIGKILL)
                                except ProcessLookupError: pass
                                other.wait(timeout=3)
                    finally:
                        if otherfd is not None: os.close(otherfd)
                        if first is not None: first.cleanup()
    def test_crash_after_open_intent_refuses_restart(self):
        with tempfile.TemporaryDirectory() as root:
            journal=str(Path(root)/'journal.sqlite'); first=second=None
            try:
                first=OwnedFixture(journal)
                with self.assertRaises(EOFError): first.cmd('fixture_crash_open_intent')
                first.observe_stopped()
                second=OwnedFixture(journal,(first.initfd,first.wrapperfd))
                self.assertFalse(second.ready['recoverable'])
                self.assertEqual(second.cmd('open'),{'error':'recovery_unverified'})
            finally:
                try:
                    if second is not None: second.cleanup()
                finally:
                    if first is not None: first.cleanup()
    def test_crash_after_open_never_authorizes_restart(self):
        with tempfile.TemporaryDirectory() as root:
            journal=str(Path(root)/'journal.sqlite'); first=second=None
            try:
                first=OwnedFixture(journal); self.assertTrue(first.cmd('open')['gate']['open'])
                first.kill()
                second=OwnedFixture(journal,(first.initfd,first.wrapperfd))
                self.assertFalse(second.ready['recoverable'])
                self.assertEqual(second.cmd('open'),{'error':'recovery_unverified'})
                self.assertFalse(second.cmd('status')['gate']['open'])
            finally:
                try:
                    if second is not None: second.cleanup()
                finally:
                    if first is not None: first.cleanup()

if __name__=='__main__': unittest.main()
