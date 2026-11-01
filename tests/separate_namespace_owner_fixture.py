"""Trusted-harness-only separate owner process fixture."""
import array, fcntl, hashlib, json, os, select, socket, struct, subprocess, sys, threading, time, tempfile, sqlite3, hmac
from pathlib import Path

if len(sys.argv)!=7 or os.getpid()!=1: raise SystemExit('dedicated namespace init and handles required')
netfd,userfd,pidfd,ctlfd = map(int,sys.argv[1:5])
journal_path=sys.argv[6]
if sys.argv[5] not in ('run','runuid'): raise SystemExit('trusted harness required')
separate_uid=sys.argv[5]=='runuid'
for name,fd,kind in (('net',netfd,0x40000000),('user',userfd,0x10000000),('pid',pidfd,0x20000000)):
    if fcntl.ioctl(fd,0xb703)!=kind: raise SystemExit('wrong namespace descriptor')
    a,b=os.fstat(fd),os.stat('/proc/self/ns/'+name)
    if (a.st_dev,a.st_ino)==(b.st_dev,b.st_ino): raise SystemExit('namespace is not isolated')
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ops.synthetic_nft_root import checked_nft_root
root=checked_nft_root(os.environ.get('LEDGETOP_SYNTHETIC_NFT_ROOT',''))
env = dict(os.environ, LD_LIBRARY_PATH=root + '/usr/lib/x86_64-linux-gnu')
nft = root + '/usr/sbin/nft'
expected = {
    'usr/sbin/nft': '3f1c21553e62716ef1abfcf31f51ff94eb73ff4234a6653bac754a42f936d1c7',
    'usr/lib/x86_64-linux-gnu/libmnl.so.0': 'a166aeee69e4b89b3cdb94e9b4cf1391081c1d141e19d24f1964e76990a18435',
    'usr/lib/x86_64-linux-gnu/libnftnl.so.11': '31de1bbd42e0976d6fbde5cc8b805036189506d092dd0964dc8547b2e2ad57e5',
    'usr/lib/x86_64-linux-gnu/libnftables.so.1': '992de9571a44b904f0f55626160bdd60b690227cbffb8b7f962b0950e22bead3',
}
for relative, digest in expected.items():
    if hashlib.sha256((Path(root) / relative).read_bytes()).hexdigest() != digest:
        raise SystemExit('refuse: nft fixture tool changed')



from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ops.loopback_streaming_gateway import Admission, handler_for
from ops.linux_marked_connection import MarkedConnection
subprocess.run(['ip','link','set','lo','up'],check=True,timeout=3)
DATA='synthetic-data-'+'d'*40
started,release=threading.Event(),threading.Event()
class Backend(BaseHTTPRequestHandler):
    def setup(self): super().setup(); self.connection.settimeout(3)
    def log_message(self,*args): pass
    def do_POST(self):
        remaining=int(self.headers['Content-Length']); count=0
        while remaining:
            chunk=self.rfile.read(min(65536,remaining))
            if not chunk: return
            remaining-=len(chunk); count+=len(chunk)
        started.set()
        if not release.wait(6): return
        body=str(count).encode(); self.send_response(200); self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body)
class Server(ThreadingHTTPServer):
    daemon_threads=False; block_on_close=True; allow_reuse_address=True
backend=Server(('127.0.0.1',0),Backend); port=backend.server_port
rules=('add table inet codex_fixture\nadd chain inet codex_fixture output { type filter hook output priority 0; policy accept; }\n'
       f'add rule inet codex_fixture output tcp dport {port} meta mark != 0x42 reject\n')
subprocess.run([nft,'-f','-'],input=rules,text=True,env=env,check=True,capture_output=True,timeout=4)
nsfd=os.open('/proc/self/ns/net',os.O_RDONLY|os.O_CLOEXEC)
def factory(host,port_arg,*,timeout):
    return MarkedConnection(host,port_arg,timeout=timeout,namespace_fd=nsfd,upstream=('127.0.0.1',port),fixture=True)
gate=Admission(DATA)
gateway=Server(('127.0.0.1',0),handler_for(gate,('127.0.0.1',port),connection_factory=factory))
gport=gateway.server_port
threads=[]; started_servers=[]; worker=None
control=socket.socket(fileno=ctlfd); control.settimeout(12)
peer=struct.unpack('3i',control.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,struct.calcsize('3i')))
if peer[1]!=os.getuid(): raise SystemExit('control peer UID mismatch')
def read_exact(n):
    value=bytearray()
    while len(value)<n:
        data=control.recv(n-len(value))
        if not data: raise EOFError()
        value.extend(data)
    return bytes(value)
def recv():
    size=struct.unpack('!I',read_exact(4))[0]
    if not 0<size<=1024: raise ValueError('frame')
    return json.loads(read_exact(size))
def send(value):
    raw=json.dumps(value,separators=(',',':')).encode(); control.sendall(struct.pack('!I',len(raw))+raw)
# The inherited socketpair is the trusted fixture's only control channel.
from ops.fixture_bootstrap import bootstrap_descriptors
with bootstrap_descriptors(control) as (secret,rights):
    control.settimeout(12)
    old_init_fd,old_wrapper_fd=rights if rights else (-1,-1)
    db=sqlite3.connect(journal_path,isolation_level=None)
    db.execute('PRAGMA synchronous=FULL')
    db.execute('CREATE TABLE IF NOT EXISTS gate (id INTEGER PRIMARY KEY CHECK(id=1),intent TEXT NOT NULL,wrapper_pid INTEGER NOT NULL,wrapper_birth INTEGER NOT NULL,init_pid INTEGER NOT NULL,init_birth INTEGER NOT NULL)')
    def proc_identity(pid):
        raw=Path('/proc',str(pid),'stat').read_text()
        fields=raw[raw.rfind(')')+2:].split()
        return int(fields[1]),int(fields[19])
    host_pid=next(int(line.split()[1]) for line in open('/proc/self/status',encoding='utf-8') if line.startswith('NSpid:'))
    wrapper_pid,init_birth=proc_identity(host_pid)
    _,wrapper_birth=proc_identity(wrapper_pid)
    prior=db.execute('SELECT intent,wrapper_pid,wrapper_birth FROM gate WHERE id=1').fetchone()
    if prior is None:
        db.execute("INSERT INTO gate VALUES(1,'startup_closed',?,?,?,?)",(wrapper_pid,wrapper_birth,host_pid,init_birth))
        recoverable=True
    else:
        def dead_pidfd(fd):
            return fd>=0 and os.readlink('/proc/self/fd/'+str(fd))=='anon_inode:[pidfd]' and bool(select.select([fd],[],[],0)[0])
        def exact_dead_wrapper(fd,expected_pid,expected_birth):
            if not dead_pidfd(fd): return False
            info=Path('/proc/self/fdinfo',str(fd)).read_text()
            identities=[int(line.split()[1]) for line in info.splitlines() if line.startswith('Pid:')]
            return identities==[expected_pid] and proc_identity(expected_pid)[1]==expected_birth
        try: recoverable=prior[0]=='stop_requested' and dead_pidfd(old_init_fd) and exact_dead_wrapper(old_wrapper_fd,prior[1],prior[2])
        except (OSError,ValueError): recoverable=False
worker_code=r'''import http.client,json,os,socket,sys
bport,gport,token,ownerfd,ownerino=int(sys.argv[1]),int(sys.argv[2]),sys.argv[3],int(sys.argv[4]),int(sys.argv[5])
ownerpid=int(sys.argv[6]); separate_uid=sys.argv[7]=='1'
try: leaked=os.fstat(ownerfd).st_ino==ownerino
except OSError: leaked=False
uid=os.getuid(); owner_env_denied=None
if separate_uid:
    try:
        fd=os.open('/proc/'+str(ownerpid)+'/environ',os.O_RDONLY); os.close(fd); owner_env_denied=False
    except PermissionError: owner_env_denied=True
s=socket.socket();s.settimeout(2)
try:
    try:s.setsockopt(socket.SOL_SOCKET,socket.SO_MARK,0x42); marked=True
    except PermissionError:marked=False
    direct=s.connect_ex(('127.0.0.1',bport))
finally:s.close()
c=http.client.HTTPConnection('127.0.0.1',gport,timeout=8)
try:
    c.request('POST','/upload',b'x'*(1024*1024+1),{'X-Synthetic-Gateway-Token':token})
    r=c.getresponse(); result={'controlFdLeaked':leaked,'marked':marked,'direct':direct,'status':r.status,'body':r.read().decode()}
    if separate_uid: result.update(uid=uid,ownerEnvDenied=owner_env_denied)
    print(json.dumps(result))
finally:c.close()
'''
try:
    for server in (backend,gateway):
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.05}); thread.start(); threads.append(thread); started_servers.append(server)
    send({'ready':True,'initHostPid':host_pid,'recoverable':recoverable})
    while True:
        request=recv()
        if not isinstance(request,dict) or set(request)!={'token','action'} or not isinstance(request['token'],str) or not hmac.compare_digest(request['token'],secret):
            send({'error':'unauthorized'}); continue
        action=request['action']
        if action=='open':
            if not recoverable or gate.status(DATA)['active']!=0:
                send({'error':'recovery_unverified'}); continue
            db.execute('BEGIN IMMEDIATE'); db.execute("UPDATE gate SET intent='open_intent',wrapper_pid=?,wrapper_birth=?,init_pid=?,init_birth=? WHERE id=1",(wrapper_pid,wrapper_birth,host_pid,init_birth)); db.execute('COMMIT')
            gate.set_open(DATA,True)
            db.execute('BEGIN IMMEDIATE'); db.execute("UPDATE gate SET intent='open' WHERE id=1"); db.execute('COMMIT')
            send({'gate':gate.status(DATA)})
        elif action=='fixture_crash_open_intent':
            db.execute('BEGIN IMMEDIATE'); db.execute("UPDATE gate SET intent='open_intent',wrapper_pid=?,wrapper_birth=?,init_pid=?,init_birth=? WHERE id=1",(wrapper_pid,wrapper_birth,host_pid,init_birth)); db.execute('COMMIT')
            os._exit(86)
        elif action=='start':
            if worker is not None: send({'error':'already_started'}); continue
            worker=subprocess.Popen(['setpriv','--bounding-set=-net_admin,-net_raw','--no-new-privs',*(['--reuid=65534','--regid=65534','--keep-groups'] if separate_uid else []),sys.executable,'-c',worker_code,str(port),str(gport),DATA,str(ctlfd),str(os.fstat(ctlfd).st_ino),str(host_pid),'1' if separate_uid else '0'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,close_fds=True)
            send({'started':True})
        elif action=='status': send({'gate':gate.status(DATA),'workerStarted':started.is_set()})
        elif action=='drain':
            db.execute('BEGIN IMMEDIATE'); db.execute("UPDATE gate SET intent='drain_requested' WHERE id=1"); db.execute('COMMIT')
            gate.set_open(DATA,False); send({'gate':gate.status(DATA)})
        elif action=='release': release.set(); send({'released':True})
        elif action=='collect':
            if worker is None or worker.poll() is None: send({'error':'worker_running'}); continue
            out,err=worker.communicate(timeout=2)
            if worker.returncode: send({'error':'worker_failed'}); continue
            send({'result':json.loads(out)})
        elif action=='stop':
            if not gate.status(DATA)['drained']: send({'error':'not_drained'}); continue
            db.execute('BEGIN IMMEDIATE'); db.execute("UPDATE gate SET intent='stop_requested' WHERE id=1"); db.execute('COMMIT')
            send({'stopReady':True})
        else: send({'error':'unknown'})
finally:
    release.set(); gate.set_open(DATA,False)
    try:
        if worker is not None and worker.poll() is None:
            worker.kill(); worker.communicate(timeout=3)
    finally:
        try:
            for server in started_servers: server.shutdown()
            for thread in threads: thread.join(4)
        finally:
            try:
                for server in (gateway,backend): server.server_close()
            finally:
                db.close(); control.close(); os.close(nsfd)
