"""Synthetic actual v3 terminal transport; no stale app-lease reclaim authority."""
from contextlib import closing
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path, PurePosixPath
import queue
import re
import socket
import ssl
import subprocess
import sys
import threading
import time
import unittest

from tests import test_reference_viewer_app_joint as joint
from processing import reference_protocol as protocol

VIEWER_COMMIT='21cb9e40a4ee5bcee0ea97827a393978ac8d5714'
MANIFEST_SHA='129de64ea0994240f1e0d0eab5c1e6451e8da8f2896e6a208803bb6823bb2dd6'
HELPER=Path(__file__).parent/'fixtures/reference_viewer_terminal_joint.cjs'
FROZEN=Path(__file__).parent/'fixtures/viewer_app_21cb_source_manifest.json'
REQUIRED_SOURCE=joint.REQUIRED_SOURCE|{'apps/viewer/server/referenceTerminalDraft.js','apps/viewer/server/referenceGrantReconciliationDraft.js'}
sha=joint.sha


def frozen_manifest(value):
    if sha(protocol.canonical_json(value))!=MANIFEST_SHA:
        raise ValueError('terminal_viewer_source_closure_changed')
    return value


class DropTerminalAckProxy:
    """Owned loopback TLS forwarder: actual commit then one lost response."""
    def __init__(self, origin, cert, key):
        self.records=[]; self.lock=threading.Lock(); self.dropped=False
        target=origin.removeprefix('https://'); outer=self
        context=ssl.create_default_context(cafile=str(cert))
        class Handler(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def log_message(self, *_): pass
            def do_GET(self): self.forward()
            def do_POST(self): self.forward()
            def forward(self):
                self.connection.settimeout(10)
                raw_length=self.headers.get('Content-Length','0')
                if not raw_length.isdecimal() or not 0<=int(raw_length)<=4*1024**2:
                    self.send_error(400); return
                raw=self.rfile.read(int(raw_length))
                if len(raw)!=int(raw_length): self.send_error(400); return
                headers={k:v for k,v in self.headers.items() if k.lower() not in
                    ('host','content-length','connection','transfer-encoding')}
                connection=http.client.HTTPSConnection(target,context=context,timeout=10)
                try:
                    connection.request(self.command,self.path,raw,headers)
                    response=connection.getresponse(); body=response.read(4*1024**2+1)
                    if len(body)>4*1024**2: raise ValueError('fixture_response_bound')
                    terminal=self.command=='POST' and self.path.endswith('/terminal-acceptance')
                    drop=False
                    if terminal:
                        value=protocol.decode_submission(body)
                        with outer.lock:
                            outer.records.append(dict(path=self.path,request=raw,response=value))
                            if not outer.dropped and response.status==200 and value.get('terminalAccepted') is True:
                                outer.dropped=True;drop=True
                    if drop:
                        # The upstream FULL transaction is already committed.
                        self.close_connection=True; self.connection.shutdown(socket.SHUT_RDWR); return
                    self.send_response(response.status)
                    for k,v in response.getheaders():
                        if k.lower() not in ('content-length','connection','transfer-encoding','server','date'):
                            self.send_header(k,v)
                    self.send_header('Content-Length',str(len(body))); self.end_headers();self.wfile.write(body)
                finally:
                    connection.close()
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.server.daemon_threads=False;self.server.block_on_close=True
        tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(cert,key)
        self.server.socket=tls.wrap_socket(self.server.socket,server_side=True)
        self.thread=threading.Thread(target=self.server.serve_forever);self.thread.start()
        self.origin='https://127.0.0.1:'+str(self.server.server_port)

    def close(self):
        self.server.shutdown();self.server.server_close();self.thread.join(5)
        if self.thread.is_alive():raise AssertionError('owned_proxy_thread_alive')


class TerminalJointGuardTests(unittest.TestCase):
    def test_combined_manifest_pin_and_expected_modules(self):
        value=frozen_manifest(json.loads(FROZEN.read_bytes()))
        self.assertEqual(value['commit'],VIEWER_COMMIT)
        self.assertEqual(len(value['files']),173)
        self.assertTrue(REQUIRED_SOURCE<={r['path'] for r in value['files']})
        value['commit']='0'*40
        with self.assertRaisesRegex(ValueError,'source_closure_changed'):frozen_manifest(value)

    def test_terminal_helper_refuses_before_config_reads_without_opt_in(self):
        completed=subprocess.run([os.environ.get('VIEWER_APP_NODE','node'),str(HELPER),'missing.json'],capture_output=True,text=True,
            timeout=10,env={k:v for k,v in os.environ.items() if k in ('PATH','SystemRoot','SYSTEMROOT')})
        self.assertEqual(completed.returncode,1)
        self.assertIn('SYNTHETIC_FIXTURE_REQUIRED',completed.stderr)

    def test_actual_sqlite_snapshot_has_closed_plain_rows_and_all_lease_fields(self):
        script="""const assert=require('node:assert/strict');const {DatabaseSync}=require('node:sqlite');
const {appJobSnapshot}=require(process.argv[1]);const d=new DatabaseSync(':memory:');
try{d.exec("CREATE TABLE viewer_reference_attempt_journal(attempt_id,job_id,lease_owner,lease_generation,lease_expires_ms);INSERT INTO viewer_reference_attempt_journal VALUES('a','j','owner',1,100)");
const before=appJobSnapshot(d);assert.equal(Object.getPrototypeOf(before[0]),Object.prototype);
assert.deepEqual(before,[{attempt_id:'a',job_id:'j',lease_owner:'owner',lease_generation:1,lease_expires_ms:100}]);
d.exec('UPDATE viewer_reference_attempt_journal SET lease_generation=2,lease_expires_ms=200');
const after=appJobSnapshot(d);assert.notEqual(JSON.stringify(before),JSON.stringify(after));
assert.equal(after[0].lease_owner,'owner');assert.equal(after[0].lease_generation,2);assert.equal(after[0].lease_expires_ms,200);
}finally{d.close()}"""
        completed=subprocess.run([os.environ.get('VIEWER_APP_NODE','node'),'--no-warnings','-e',script,str(HELPER)],
            capture_output=True,text=True,timeout=10)
        self.assertEqual(completed.returncode,0,completed.stderr)


@unittest.skipUnless(sys.platform.startswith('linux') and os.environ.get('VIEWER_TERMINAL_REFERENCE_ROOT'),
    'explicit reviewed Linux Node24/21cb source joint fixture required')
class ViewerTerminalJointTests(joint.ViewerAppV3JointTests):
    # Override the inherited f848 opt-in marker as well as this sibling's guard.
    __unittest_skip__=not (sys.platform.startswith('linux') and os.environ.get('VIEWER_TERMINAL_REFERENCE_ROOT'))
    # Keep original successful flow independently pinned and untouched.
    test_actual_app_draft_wire_three_jobs_and_durable_result_bridge=None

    @classmethod
    def setUpClass(cls):
        from tests.test_reference_parent_guard import ParentGuardTests
        cls.node=Path(os.environ['VIEWER_APP_NODE']).absolute()
        if joint.binary_sha(cls.node)!=os.environ['VIEWER_APP_NODE_SHA256']:
            raise ValueError('joint_node_binary_changed')
        version=subprocess.run([str(cls.node),'--version'],capture_output=True,text=True,timeout=10,check=True).stdout.strip()
        if not version.startswith('v') or int(version[1:].split('.')[0])!=24:raise ValueError('joint_node24_required')
        cls.viewer_root=Path(os.environ['VIEWER_TERMINAL_REFERENCE_ROOT']).absolute()
        cls.viewer_manifest=Path(os.environ['VIEWER_TERMINAL_REFERENCE_MANIFEST'])
        cls.viewer_manifest_sha=os.environ['VIEWER_TERMINAL_REFERENCE_MANIFEST_SHA256']
        frozen_manifest(json.loads(FROZEN.read_bytes()))
        frozen_manifest(verify_viewer_source(cls.viewer_root,cls.viewer_manifest,cls.viewer_manifest_sha))
        ParentGuardTests.setUpClass.__func__(cls)

    def setUp(self):
        super().setUp()
        self.proxy=DropTerminalAckProxy(self.origin,self.cert,self.key);self.stack.callback(self.proxy.close)
        config=json.loads(self.node_config.read_bytes())
        config.update(viewerCommit=VIEWER_COMMIT,coordinatorOrigin=self.proxy.origin)
        self.node_config.write_text(json.dumps(config))

    def start_peer(self):
        verify_viewer_source(self.viewer_root,self.viewer_manifest,self.viewer_manifest_sha)
        self.assertEqual(joint.binary_sha(self.node),os.environ['VIEWER_APP_NODE_SHA256'])
        env={k:v for k,v in os.environ.items() if k in ('PATH','SystemRoot','SYSTEMROOT','TMPDIR','TEMP','TMP')}
        env.update(LTDS_VIEWER_APP_SYNTHETIC='1',NODE_ENV='test')
        child=subprocess.Popen([str(self.node),'--no-warnings',str(HELPER),str(self.node_config)],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=env)
        messages=queue.Queue();self.peer=(child,messages)
        def read():
            for line in child.stdout:messages.put(line)
            messages.put(None)
        reader=threading.Thread(target=read);reader.start();self.peer=(child,messages,reader)
        return self.reply()

    def reply(self):
        child,messages,*_=self.peer
        try:line=messages.get(timeout=20)
        except queue.Empty:self.fail('owned Node fixture did not return within20s')
        if line is None:
            exit_code=child.wait(timeout=10)
            raw=child.stderr.buffer.read(4097);code='DIAGNOSTIC_UNAVAILABLE'
            if len(raw)<=4096:
                try:
                    value=json.loads(raw)
                    if set(value)=={'error'} and type(value['error']) is str and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}',value['error']):
                        code=value['error']
                except (ValueError,TypeError):pass
            print(json.dumps(dict(ownedNodeUnexpectedExit=exit_code,errorCode=code)),flush=True)
            self.fail('owned Node fixture exited before reply: '+code)
        self.assertLessEqual(len(line),65536)
        return json.loads(line)

    def test_actual_failed_terminal_ack_loss_and_saved_receipt_reopen(self):
        self.run_terminal(30)

    def test_actual_cancelled_terminal_ack_loss_and_saved_receipt_reopen(self):
        self.run_terminal(50)

    def run_terminal(self, terminal_code):
        ready=self.start_peer();seed=self.command('seed',count=1)
        jobs=[r['job_id'] for r in seed['jobs']]
        mapping=[(r['attempt_id'],r['job_id']) for r in seed['jobs']]
        self.assertEqual(len(set(jobs)),1)
        self.assertIn(terminal_code,(30,50))
        with self.native._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM calls').fetchone()[0],0)
            # Test-owned backend only: expose the chosen terminal status in the
            # actual commit transaction, before any worker can observe success.
            db.execute('CREATE TRIGGER fixture_terminal_commit AFTER UPDATE OF committed ON tasks '
                'WHEN OLD.committed=0 AND NEW.committed=1 BEGIN UPDATE tasks SET status='
                +str(terminal_code)+' WHERE uuid=NEW.uuid; END')
        self.start_worker(ready['brokerOrigin'])
        deadline=time.monotonic()+90;lost=None
        while lost is None:
            state=self.command('terminal-pump')
            self.assertEqual([(r['attempt_id'],r['job_id']) for r in state['jobs']],mapping)
            with self.native._db() as db:
                self.assertLessEqual(db.execute('SELECT COUNT(*) FROM tasks WHERE status IN (10,20)').fetchone()[0],1)
            if state['uncertain'] is not None:
                self.assertEqual(state['uncertain'],'UNCERTAIN')
                self.assertTrue(self.proxy.dropped);lost=state;break
            self.assertLess(time.monotonic(),deadline);time.sleep(.1)
        self.assertEqual(len(lost['terminalReceipts']),1)
        self.assertEqual(lost['terminalReceipts'][0]['acked'],0)
        self.assertEqual(lost['receiptFactories'],1)
        frozen_jobs=lost['jobs'];saved=lost['terminalReceipts'][0]
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_terminal_acceptances').fetchone()[0],1)
            receipt_before=tuple(db.execute('SELECT receipt_id,storage_receipt_id,job_id,generation,digest,receipt_json FROM reference_terminal_acceptances').fetchone())
        self.close_peer();self.start_peer()
        # The exact app claim stays held. Only the saved terminal ledger replays.
        replay=self.command('terminal-replay')
        self.assertEqual(replay['jobs'],frozen_jobs)
        self.assertEqual(replay['receiptFactories'],0)
        self.assertEqual(replay['terminalReceipts'],[{**saved,'acked':1}])
        # Do not turn a lost response into authority to reclaim the app attempt.
        self.assertTrue(any(r['lease_owner'] for r in replay['jobs']))
        self.assertEqual(len(self.proxy.records),2)
        self.assertEqual([r['path'] for r in self.proxy.records],
            ['/ltds/v1/jobs/'+jobs[0]+'/terminal-acceptance']*2)
        self.assertEqual(self.proxy.records[0]['request'],self.proxy.records[1]['request'])
        for record in self.proxy.records:
            self.assertEqual(set(record['response']),{'job','created','terminalAccepted'})
        self.assertEqual({k:v for k,v in self.proxy.records[0]['response'].items() if k!='created'},
            {k:v for k,v in self.proxy.records[1]['response'].items() if k!='created'})
        self.assertTrue(self.proxy.records[0]['response']['created'])
        self.assertFalse(self.proxy.records[1]['response']['created'])
        self.assertTrue(self.proxy.records[1]['response']['terminalAccepted'])

        while True:
            with self.driver._db() as db:
                rows=db.execute('SELECT job,stage FROM driver_jobs').fetchall()
            if [(r[0],r[1]) for r in rows]==[(jobs[0],'completed')]:break
            self.assertLess(time.monotonic(),deadline);time.sleep(.1)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_terminal_acceptances').fetchone()[0],1)
            receipt_after=tuple(db.execute('SELECT receipt_id,storage_receipt_id,job_id,generation,digest,receipt_json FROM reference_terminal_acceptances').fetchone())
            self.assertEqual(receipt_after,receipt_before)
            self.assertEqual(sha(receipt_after[-1].encode()),saved['payloadSha256'])
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_acceptances').fetchone()[0],0)
        self.assertEqual(replay['acceptanceCount'],0)
        self.assertEqual(replay['ingestCalls'],0)
        self.assertEqual(replay['retainedFiles'],0)
        self.assertEqual(replay['modelCount'],0)
        self.assertTrue(replay['sourcePresent'])
        self.assertFalse(replay['productionLaneEnabled'])
        counts=self.native.counts()
        with self.native._db() as db:
            self.assertEqual([tuple(r) for r in db.execute('SELECT uuid,status,committed,uploaded FROM tasks')],
                [(jobs[0],terminal_code,1,1)])
        for action in ('initialize','upload','commit'):self.assertEqual(counts.get(action,0),1)
        for action in ('download','remove'):self.assertEqual(counts.get(action,0),0)
        sources=list(self.driver.ledger.root.glob('jobs/*/image-1.input'))
        self.assertEqual(len(sources),1);self.assertEqual(sources[0].read_bytes(),b'abc')
        self.assertEqual(self.store.get(jobs[0])['status'],'failed' if terminal_code==30 else 'cancelled')
        self.assertEqual(self.store.get(jobs[0])['error'],'engine_failed' if terminal_code==30 else 'engine_cancelled')
        self.assertEqual(self.store.get(jobs[0])['phase'],'completed')
        verify_viewer_source(self.viewer_root,self.viewer_manifest,self.viewer_manifest_sha)
        self.close_peer()
        print(json.dumps(dict(scope='synthetic-terminal-wire-no-app-reclaim',terminalCode=terminal_code,
            jobId=jobs[0],sameReceiptReplay=True,terminalAckDroppedOnce=True,appClaimStillHeld=True,
            nativeCounts=counts,viewerCommit=VIEWER_COMMIT,nodeChildren=self.child_outcomes)),flush=True)


def verify_viewer_source(root,manifest_path,manifest_sha):
    """Frozen source identity only; not runtime/app admission authority."""
    root=Path(root).absolute();manifest_path=Path(manifest_path)
    if root.resolve()!=root or any(v.is_symlink() for v in (root,*root.parents)):
        raise ValueError('viewer_source_path_changed')
    if manifest_path.stat().st_size>1024**2:raise ValueError('viewer_source_manifest_changed')
    raw=manifest_path.read_bytes()
    if len(raw)>1024**2 or sha(raw)!=manifest_sha:raise ValueError('viewer_source_manifest_changed')
    value=json.loads(raw)
    if set(value)!={'schemaVersion','commit','files'} or type(value['schemaVersion']) is not int or value['schemaVersion']!=1 or value['commit']!=VIEWER_COMMIT:
        raise ValueError('viewer_source_manifest_changed')
    rows=value['files']
    if type(rows) is not list or not 1<=len(rows)<=1024:raise ValueError('viewer_source_manifest_changed')
    seen=set();total=0
    for row in rows:
        if type(row) is not dict or set(row)!={'path','sha256'}:raise ValueError('viewer_source_manifest_changed')
        name=row['path']
        if type(name) is not str:raise ValueError('viewer_source_manifest_changed')
        relative=PurePosixPath(name)
        if str(relative)!=name or relative.is_absolute() or '..' in relative.parts or '\\' in name or name in seen:
            raise ValueError('viewer_source_manifest_changed')
        seen.add(name);path=root/name
        if path.resolve()!=path or any(v.is_symlink() for v in (path,*path.parents)) or not path.is_file() or path.stat().st_nlink!=1:
            raise ValueError('viewer_source_file_changed')
        before=path.stat()
        if before.st_size>4*1024**2:raise ValueError('viewer_source_file_changed')
        data=path.read_bytes();after=path.stat();total+=len(data)
        if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns):raise ValueError('viewer_source_file_changed')
        if len(data)>4*1024**2 or total>32*1024**2 or sha(data)!=row['sha256']:raise ValueError('viewer_source_file_changed')
    actual={str(v.relative_to(root)).replace('\\','/') for v in (root/'apps/viewer/server').rglob('*') if v.is_file()}
    if not REQUIRED_SOURCE<=seen or not actual<=seen:raise ValueError('viewer_source_closure_incomplete')
    return value



if __name__=='__main__':unittest.main()
