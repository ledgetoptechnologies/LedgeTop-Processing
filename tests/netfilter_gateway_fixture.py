"""Synthetic netfilter test helper: run only in disposable user+net namespace."""
import fcntl, hashlib, json, os, socket, subprocess, sys, threading
from pathlib import Path

def namespace(pid, name):
    info = os.stat(f'/proc/{pid}/ns/{name}')
    return [info.st_dev, info.st_ino]

if len(sys.argv) != 5 or sys.argv[4] not in ('run', 'check-only'):
    raise SystemExit('trusted harness and parent namespace identities required')
parent_pid, parent_net_fd, parent_user_fd = map(int, sys.argv[1:4])
if os.getppid() != parent_pid:
    raise SystemExit('refuse: parent process changed')
# NS_GET_NSTYPE validates these are network and user namespace handles.
if (fcntl.ioctl(parent_net_fd, 0xb703) != 0x40000000
        or fcntl.ioctl(parent_user_fd, 0xb703) != 0x10000000):
    raise SystemExit('refuse: invalid namespace descriptor types')
def fd_identity(fd):
    info = os.fstat(fd)
    return [info.st_dev, info.st_ino]
parent_net, parent_user = fd_identity(parent_net_fd), fd_identity(parent_user_fd)
if (namespace('self', 'net') == parent_net or namespace('self', 'user') == parent_user):
    raise SystemExit('refuse: network and user namespaces must both differ from trusted parent')
if sys.argv[4] == 'check-only':
    raise SystemExit(0)

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

import http.client
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops.loopback_streaming_gateway import Admission, handler_for

subprocess.run(['ip', 'link', 'set', 'lo', 'up'], check=True, timeout=3)
TOKEN = 'synthetic-' + 'x' * 40
started, release = threading.Event(), threading.Event()
class Backend(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def setup(self):
        super().setup(); self.connection.settimeout(3)
    def log_message(self, *args): pass
    def do_POST(self):
        n = int(self.headers['Content-Length'])
        total = 0
        while total < n:
            block = self.rfile.read(min(65536, n-total))
            if not block: return
            total += len(block)
        started.set()
        if not release.wait(4): return
        data = str(total).encode()
        self.send_response(200); self.send_header('Content-Length',str(len(data))); self.end_headers()
        self.wfile.write(data)

class OwnedServer(ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = True

def start(server):
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05})
    thread.start()
    return thread

def stop(server, thread):
    server.shutdown(); thread.join(4); server.server_close()
    if thread.is_alive(): raise RuntimeError('server thread did not exit')

backend = OwnedServer(('127.0.0.1',0), Backend)
bport = backend.server_port
rules = ('add table inet codex_fixture\n'
         'add chain inet codex_fixture output { type filter hook output priority 0; policy accept; }\n'
         f'add rule inet codex_fixture output tcp dport {bport} meta mark != 0x42 reject\n')
subprocess.run([nft, '-f', '-'], input=rules, text=True, env=env, check=True, timeout=4, capture_output=True)
from ops.linux_marked_connection import MarkedConnection
namespace_fd = os.open('/proc/self/ns/net', os.O_RDONLY | os.O_CLOEXEC)
def connection_factory(host, port, *, timeout):
    return MarkedConnection(host, port, timeout=timeout, namespace_fd=namespace_fd, upstream=('127.0.0.1', bport), fixture=True)
admission = Admission(TOKEN)
gateway = OwnedServer(('127.0.0.1',0), handler_for(admission, ('127.0.0.1', bport), connection_factory=connection_factory))
gport = gateway.server_port
backend_thread = gateway_thread = None
client = None
client_code = r'''import http.client, json, socket, sys
bport, gport, token = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
s = socket.socket(); s.settimeout(2)
try:
    try: s.setsockopt(socket.SOL_SOCKET, socket.SO_MARK, 0x42); marked = True
    except PermissionError: marked = False
    direct = s.connect_ex(('127.0.0.1', bport))
finally: s.close()
if sys.argv[4] == 'direct':
    print(json.dumps({'mark':marked, 'direct':direct})); sys.exit(0)
c = http.client.HTTPConnection('127.0.0.1',gport,timeout=6)
try:
    c.request('POST','/task/new/upload',b'x'*(1024*1024+1),{'X-Synthetic-Gateway-Token':token})
    r=c.getresponse(); print(json.dumps({'mark':marked,'direct':direct,'status':r.status,'body':r.read().decode()}))
finally: c.close()
'''
def client_command(mode):
    return ['setpriv','--bounding-set=-net_admin,-net_raw','--no-new-privs',sys.executable,'-c',client_code,str(bport),str(gport),TOKEN,mode]
def direct_check():
    item = subprocess.run(client_command('direct'),capture_output=True,text=True,timeout=4,check=True)
    value = json.loads(item.stdout)
    if value['mark'] or value['direct'] == 0: raise RuntimeError('unprivileged bypass')
try:
    backend_thread = start(backend); gateway_thread = start(gateway)
    if not admission.set_open(TOKEN, True): raise RuntimeError('open failed')
    client = subprocess.Popen(client_command('stream'),stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    if not started.wait(4): raise RuntimeError('stream did not start')
    admission.set_open(TOKEN, False)
    if admission.status(TOKEN)['active'] != 1: raise RuntimeError('drain lost active request')
    direct_check()
    release.set()
    out, err = client.communicate(timeout=6)
    if client.returncode: raise RuntimeError('stream failed: ' + err[:500])
    data = json.loads(out)
    if data != {'mark':False,'direct':data['direct'],'status':200,'body':'1048577'} or data['direct']==0:
        raise RuntimeError('stream mismatch')
    deadline = time.monotonic()+4
    while not admission.status(TOKEN)['drained'] and time.monotonic()<deadline: time.sleep(.01)
    if not admission.status(TOKEN)['drained']: raise RuntimeError('drain incomplete')
    stop(backend, backend_thread); backend_thread = None
    direct_check()
    backend = OwnedServer(('127.0.0.1',bport),Backend); backend_thread = start(backend)
    direct_check()
    admission.set_open(TOKEN, True)
    release.set()
    result = subprocess.run(client_command('stream'),capture_output=True,text=True,timeout=6,check=True)
    data = json.loads(result.stdout)
    if data['status']!=200 or data['body']!='1048577' or data['direct']==0 or data['mark']:
        raise RuntimeError('post-restart stream mismatch')
    print(json.dumps({'directDeniedBeforeDuringAfterRestart':True,'streamAndDrainSucceeded':True}))
finally:
    release.set(); admission.set_open(TOKEN, False)
    if client is not None and client.poll() is None:
        client.kill(); client.communicate(timeout=3)
    if gateway_thread is not None: stop(gateway, gateway_thread)
    else: gateway.server_close()
    if backend_thread is not None: stop(backend, backend_thread)
    else: backend.server_close()
    os.close(namespace_fd)
