import contextlib,json,os,select,signal,socket,sqlite3,struct,subprocess,sys,tempfile,time,unittest
from pathlib import Path
from tests.owned_uid_mapping import map_owned_child

def read_exact(sock,n):
    buf=bytearray()
    while len(buf)<n:
        part=sock.recv(n-len(buf))
        if not part: raise EOFError('control closed')
        buf.extend(part)
    return bytes(buf)
def exchange(sock,value):
    raw=json.dumps(value).encode(); sock.sendall(struct.pack('!I',len(raw))+raw)
    n=struct.unpack('!I',read_exact(sock,4))[0]
    if not 0<n<=4096: raise ValueError('bad response')
    return json.loads(read_exact(sock,n))
def parent_birth(pid):
    value=Path('/proc',str(pid),'stat').read_text(); fields=value[value.rfind(')')+2:].split()
    return int(fields[1]),int(fields[19])

class Tests(unittest.TestCase):
    def test_separate_uid_worker_drain_and_exact_wrapper_stop(self):
        helper=Path(__file__).with_name('separate_namespace_owner_fixture.py')
        with tempfile.TemporaryDirectory() as directory:
            journal=str(Path(directory)/'gate.sqlite')
            client,owner=socket.socketpair(); client.settimeout(10)
            child=None
            try:
                with contextlib.ExitStack() as stack:
                    fds=[]
                    for name in ('net','user','pid'):
                        fd=os.open('/proc/self/ns/'+name,os.O_RDONLY|os.O_CLOEXEC); stack.callback(os.close,fd); fds.append(fd)
                    ready_r,ready_w=os.pipe(); go_r,go_w=os.pipe()
                    for fd in (ready_r,ready_w,go_r,go_w): stack.callback(os.close,fd)
                    launcher=Path(__file__).with_name('separate_uid_owner_launcher.py')
                    args=[sys.executable,str(launcher),str(ready_w),str(go_r),'unshare','--net','--pid','--fork','--kill-child=KILL',sys.executable,str(helper),
                          *(str(fd) for fd in fds),str(owner.fileno()),'runuid',journal]
                    child=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,
                                           start_new_session=True,pass_fds=tuple(fds)+(owner.fileno(),ready_w,go_r))
                    if not select.select([ready_r],[],[],4)[0] or os.read(ready_r,1)!=b'R': raise RuntimeError('user namespace unavailable')
                    mapfd=os.pidfd_open(child.pid,0)
                    try: map_owned_child(child.pid,mapfd)
                    finally: os.close(mapfd)
                    os.write(go_w,b'G')
            except BaseException:
                if child is not None:
                    try: os.killpg(child.pid,signal.SIGKILL)
                    except ProcessLookupError: pass
                    child.communicate(timeout=3)
                client.close(); owner.close(); raise
            owner.close(); wrapperfd=initfd=None
            try:
                secret='synthetic-control-'+'c'*40
                ready=exchange(client,{'bootstrap':secret}); self.assertTrue(ready['ready'])
                init=ready['initHostPid']; identity=parent_birth(init)
                self.assertEqual(identity[0],child.pid)
                initfd=os.pidfd_open(init,0); self.assertEqual(parent_birth(init),identity)
                wrapperfd=os.pidfd_open(child.pid,0)
                def cmd(action,token=secret): return exchange(client,{'token':token,'action':action})
                self.assertEqual(cmd('open','wrong'),{'error':'unauthorized'})
                self.assertFalse(cmd('status')['gate']['open'])
                self.assertTrue(cmd('open')['gate']['open'])
                self.assertEqual(cmd('drain','wrong'),{'error':'unauthorized'})
                self.assertTrue(cmd('status')['gate']['open'])
                self.assertTrue(cmd('start')['started'])
                deadline=time.monotonic()+5
                while not (status:=cmd('status'))['workerStarted']:
                    if time.monotonic()>=deadline: raise TimeoutError('worker start')
                    time.sleep(.02)
                self.assertEqual(status['gate']['active'],1)
                gate=cmd('drain')['gate']; self.assertFalse(gate['open']); self.assertEqual(gate['active'],1)
                self.assertEqual(cmd('stop'),{'error':'not_drained'})
                self.assertTrue(cmd('release')['released'])
                deadline=time.monotonic()+6
                while True:
                    result=cmd('collect')
                    if 'result' in result: break
                    if time.monotonic()>=deadline: raise TimeoutError('worker completion')
                    time.sleep(.02)
                value=result['result']
                self.assertFalse(value['controlFdLeaked']); self.assertFalse(value['marked'])
                self.assertEqual(value['uid'],65534); self.assertTrue(value['ownerEnvDenied'])
                self.assertNotEqual(value['direct'],0); self.assertEqual((value['status'],value['body']),(200,'1048577'))
                self.assertTrue(cmd('status')['gate']['drained'])
                self.assertTrue(cmd('stop')['stopReady'])
                with contextlib.closing(sqlite3.connect(journal)) as db:
                    self.assertEqual(db.execute('SELECT intent FROM gate').fetchone()[0],'stop_requested')
                signal.pidfd_send_signal(wrapperfd,signal.SIGKILL)
                self.assertTrue(select.select([initfd],[],[],5)[0])
                deadline=time.monotonic()+5
                while os.waitid(os.P_PID,child.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT) is None:
                    if time.monotonic()>=deadline: raise TimeoutError('wrapper exit')
                    time.sleep(.02)
                try: os.killpg(child.pid,signal.SIGKILL)
                except ProcessLookupError: pass
                child.communicate(timeout=3)
            finally:
                client.close()
                if initfd is not None:
                    try: signal.pidfd_send_signal(initfd,signal.SIGKILL)
                    except ProcessLookupError: pass
                    os.close(initfd)
                if wrapperfd is not None: os.close(wrapperfd)
                if child.returncode is None:
                    try: os.killpg(child.pid,signal.SIGKILL)
                    except ProcessLookupError: pass
                    child.communicate(timeout=3)

if __name__=='__main__': unittest.main()
