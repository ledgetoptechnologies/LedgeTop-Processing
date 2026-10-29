"""Actual Viewer draft components + real v3 TLS, explicitly synthetic authority.

Opt in only with a reviewed f848 source manifest and an exact Linux Node24
binary pin. No production app entrypoint, custody, model publication, NAS,
scientific processing, grant issuer or arbitrary-crash reclaim is qualified.
"""
from contextlib import ExitStack, closing
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import queue
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

VIEWER_COMMIT='f8488b023858454c1d4dcd7d2cce3cd2920febda'
HELPER=Path(__file__).parent/'fixtures/reference_viewer_app_joint.cjs'
FROZEN_VIEWER=Path(__file__).parent/'fixtures/viewer_app_f848_source_manifest.json'
REQUIRED_SOURCE={
    'apps/viewer/server/'+name for name in (
    'database.js','referenceAppDispatch.js','referenceSubmissionDraft.js','referenceProvider.js',
    'referenceProtocol.js','referenceOptions.js','referenceCaTransport.js','referenceTransferBroker.js',
    'referenceHttp.js','referenceResultBridge.js','referenceResultJournal.js','referenceResultSpool.js',
    'referenceAcceptance.js','referenceCatalogProjection.js','referenceCatalogIdentity.js','migrationIdentityGuard.js',
    'config.js','proxyGate.js','controllerOrigins.js','providerCredentials.js','providerAdmission.js','providerAuth.js',
    'directClientSourceMigration.js')}
REQUIRED_SOURCE.update(('package.json','package-lock.json','pnpm-lock.yaml','apps/viewer/package.json','apps/viewer/package-lock.json'))


def sha(raw):return hashlib.sha256(raw).hexdigest()


def require_frozen_viewer_manifest(value):
    canonical=json.dumps(value,sort_keys=True,separators=(',',':')).encode()
    if sha(canonical)!='4681332b1fdd8dad8d123cadd6898f754adbe3b40bc5fb354eda51a877c687cf':
        raise ValueError('viewer_source_frozen_closure_changed')
    return value


def binary_sha(path):
    path=Path(path).absolute()
    if path.resolve()!=path or any(v.is_symlink() for v in (path,*path.parents)):
        raise ValueError('joint_node_binary_changed')
    before=path.stat();digest=hashlib.sha256()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1:raise ValueError('joint_node_binary_changed')
    with path.open('rb') as source:
        for block in iter(lambda:source.read(1024**2),b''):digest.update(block)
    after=path.stat()
    if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns):
        raise ValueError('joint_node_binary_changed')
    return digest.hexdigest()


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


class ViewerJointGuardTests(unittest.TestCase):
    def test_committed_closure_pin_cannot_be_replaced_by_caller_manifest(self):
        frozen=require_frozen_viewer_manifest(json.loads(FROZEN_VIEWER.read_bytes()))
        self.assertEqual(frozen['commit'],VIEWER_COMMIT);self.assertEqual(len(frozen['files']),171)
        self.assertEqual(next(r['sha256'] for r in frozen['files'] if r['path']=='apps/viewer/package-lock.json'),
            '7ebcefea8db70bba3bf238594a3a555b04901ef8482137963b3ebc97bdb00f84')
        changed=json.loads(json.dumps(frozen));changed['files'][0]['sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'frozen_closure_changed'):require_frozen_viewer_manifest(changed)

    @unittest.skipUnless(os.environ.get('VIEWER_APP_NODE') or shutil.which('node'),'Node executable required for inert guard')
    def test_helper_refuses_before_config_or_source_reads_without_opt_in(self):
        result=subprocess.run([os.environ.get('VIEWER_APP_NODE') or shutil.which('node'),'--no-warnings',str(HELPER),'missing-config.json'],
            env={k:v for k,v in os.environ.items() if k in ('PATH','SystemRoot','SYSTEMROOT')},
            capture_output=True,text=True,timeout=10)
        self.assertEqual(result.returncode,1)
        self.assertEqual(json.loads(result.stderr)['error'],'SYNTHETIC_FIXTURE_REQUIRED')

    def test_manifest_pin_rejects_wrong_commit_before_loading_app(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).absolute();path=root/'manifest.json'
            raw=json.dumps(dict(schemaVersion=1,commit='0'*40,files=[])).encode();path.write_bytes(raw)
            with self.assertRaisesRegex(ValueError,'manifest_changed'):verify_viewer_source(root,path,sha(raw))


@unittest.skipUnless(sys.platform.startswith('linux') and os.environ.get('VIEWER_APP_REFERENCE_ROOT'),
    'explicit reviewed Linux Node24/f848 source joint fixture required')
class ViewerAppV3JointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from tests.test_reference_parent_guard import ParentGuardTests
        cls.node=Path(os.environ['VIEWER_APP_NODE']).absolute()
        if binary_sha(cls.node)!=os.environ['VIEWER_APP_NODE_SHA256']:raise ValueError('joint_node_binary_changed')
        version=subprocess.run([str(cls.node),'--version'],capture_output=True,text=True,timeout=10,check=True).stdout.strip()
        if not version.startswith('v') or int(version[1:].split('.')[0])!=24:raise ValueError('joint_node24_required')
        cls.viewer_root=Path(os.environ['VIEWER_APP_REFERENCE_ROOT']).absolute()
        cls.viewer_manifest=Path(os.environ['VIEWER_APP_REFERENCE_MANIFEST'])
        cls.viewer_manifest_sha=os.environ['VIEWER_APP_REFERENCE_MANIFEST_SHA256']
        require_frozen_viewer_manifest(json.loads(FROZEN_VIEWER.read_bytes()))
        require_frozen_viewer_manifest(verify_viewer_source(cls.viewer_root,cls.viewer_manifest,cls.viewer_manifest_sha))
        ParentGuardTests.setUpClass.__func__(cls)

    def setUp(self):
        from ops.native_node_fixture import NativeNodeFixture
        from ops.node_ingress import save
        from processing.app import make_handler
        from processing.config import Config
        from processing.node_gate import NodeGate
        from processing.reference_store import PROFILE
        from tests.test_reference_worker_service import free_port
        from tests.test_service import start_server,TOKEN
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.root=Path(self.stack.enter_context(tempfile.TemporaryDirectory()));self.root.chmod(0o700)
        self.gate=self.root/'gate'
        self.stack.enter_context(patch('processing.node_gate.canonical_gate_path',return_value=self.gate))
        self.native=NativeNodeFixture(self.root/'native.sqlite',engine_version='synthetic-joint-v3',synthetic_enabled=True).start()
        self.stack.callback(self.native.close)
        self.worker_port=free_port();coordinator_port=free_port()
        self.bearer='synthetic-app-joint-worker-'+'a'*32;self.reader='synthetic-app-joint-reader-'+'b'*32
        self.viewer_bearer='synthetic-app-joint-viewer-'+'c'*32
        reference=dict(schemaVersion=1,listener=dict(host='127.0.0.1',port=coordinator_port,
            certificate=str(self.cert),privateKey=str(self.key),clientCA=str(self.cert)),
            viewerBearers=dict(staging=self.viewer_bearer),workers=dict(
                cluster=dict(bearer=self.bearer,workerId='worker',instanceId='instance',resultOrigin='https://127.0.0.1:'+str(self.worker_port),resultBearer=self.reader),
                main=dict(bearer='synthetic-unused-main-worker-'+'d'*32,workerId='main-worker',instanceId='main-instance',resultOrigin='https://127.0.0.1:19979',resultBearer='synthetic-unused-main-reader-'+'e'*32)),
            streamBudget=dict(maxConcurrent=1,maxObjectBytes=4194304))
        config_path=self.root/'coordinator.json';save(config_path,reference)
        config=Config(TOKEN,self.native.origin,self.native.origin,1000,20000,
            prototype_state_dir=str(self.root/'queue'),prototype_node_ids=('cluster','main'),
            treedar_node_ids=('main',),reference_config_path=str(config_path))
        handler=make_handler(config);self.queue=handler.prototype_queue;self.queue.allowed_profiles.add(PROFILE)
        self.stack.callback(self.queue.close)
        control=start_server(handler);self.stack.callback(control.server_close);self.stack.callback(control.shutdown)
        self.control_origin='http://127.0.0.1:'+str(control.server_port)
        runtime=handler.reference_listener;runtime.start();self.stack.callback(runtime.close)
        self.store=runtime.store;self.origin='https://127.0.0.1:'+str(coordinator_port)
        with NodeGate('joint-v3-engine') as gate:gate.provision('cluster',self.control_origin,self.queue.authority_id)
        self.viewer_state=self.root/'viewer';self.viewer_state.mkdir(mode=0o700)
        self.node_config=self.root/'node-config.json'
        save(self.node_config,dict(scope='processing-viewer-app-joint-synthetic-v1',viewerCommit=VIEWER_COMMIT,
            viewerRoot=str(self.viewer_root/'apps/viewer'),root=str(self.viewer_state),environment='staging',
            viewerManifest=str(self.viewer_manifest),viewerManifestSha256=self.viewer_manifest_sha,
            coordinatorOrigin=self.origin,viewerBearer=self.viewer_bearer,caFile=str(self.cert),keyFile=str(self.key)))
        self.peer=None;self.child_outcomes=[];self.stack.callback(self.close_peer)

    def start_peer(self):
        verify_viewer_source(self.viewer_root,self.viewer_manifest,self.viewer_manifest_sha)
        self.assertEqual(binary_sha(self.node),os.environ['VIEWER_APP_NODE_SHA256'])
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
        self.assertIsNotNone(line,'owned Node fixture exited before reply')
        self.assertLessEqual(len(line),65536);return json.loads(line)

    def command(self,action,**values):
        self.peer[0].stdin.write(json.dumps(dict(action=action,**values))+'\n');self.peer[0].stdin.flush()
        return self.reply()

    def close_peer(self,expected=0):
        if self.peer is None:return
        child,messages,reader=self.peer
        try:
            if child.poll() is None:
                child.stdin.write('{"action":"close"}\n');child.stdin.flush();self.reply()
            code=child.wait(timeout=10);self.assertEqual(code,expected)
            self.child_outcomes.append(dict(pid=child.pid,exitCode=code,expectedExitCode=expected))
        finally:
            if child.poll() is None:
                # Only this exact task-owned fixture process; no stale PID use.
                child.terminate();child.wait(timeout=10)
            child.stdin.close();reader.join(3);self.assertFalse(reader.is_alive())
            child.stdout.close();child.stderr.close();self.peer=None

    def start_worker(self,broker):
        from ops import reference_worker_daemon as daemon
        from ops import bootstrap_worker_input_accounting as bootstrap
        from ops.node_ingress import save
        from processing.viewer_service import NativeEngine
        from tests.test_reference_worker_service import private_config
        config=private_config(self.root,self.cert,self.key,NativeEngine(self.native.origin),self.queue.authority_id,
            coordinator=self.origin,broker=broker,worker='worker',instance='instance',bearer=self.bearer,
            reader=self.reader,gate='joint-v3-engine',gate_origin=self.control_origin)
        config.update(listenPort=self.worker_port,cleanupEnabled=False,engineCleanupEnabled=False,
            inputCopyAccounting=dict(schemaVersion=3,fetchJournal=dict(schemaVersion=1,policyId='indexed-worker-fetch-three-retained-v1')))
        path=self.root/'worker.json';save(path,config);loaded=daemon.load_config(str(path))
        bootstrap.bootstrap_input_copy_accounting(loaded)
        self.driver,server=daemon.build_worker(loaded)
        self.stack.callback(server.server_close)
        service=daemon.WorkerService(self.driver,server,poll_seconds=.1);stop=threading.Event();results=[]
        worker=threading.Thread(target=lambda:results.append(service.run(stop)));worker.start()
        def close():
            stop.set();worker.join(10);self.assertFalse(worker.is_alive());self.assertEqual(results,[0])
        self.stack.callback(close)

    def test_actual_app_draft_wire_three_jobs_and_durable_result_bridge(self):
        from ops.native_node_fixture import ARCHIVE
        first=self.start_peer();self.assertTrue(first['ready'])
        # Deliberate failure after DROP TRIGGER must roll back before the child
        # exits, preserving exact protected migration DDL for real reopening.
        child=self.peer[0];child.stdin.write('{"action":"seed-failure","count":3}\n');child.stdin.flush()
        self.assertEqual(child.wait(timeout=10),1)
        with closing(sqlite3.connect((self.viewer_state/'application.sqlite').as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
            for guard in first['guardHashes']:
                sql=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(guard['name'],)).fetchone()
                self.assertIsNotNone(sql);self.assertEqual(sha(sql[0].encode()),guard['sha256'])
            self.assertEqual(db.execute('SELECT COUNT(*) FROM processing_attempts').fetchone()[0],0)
        self.close_peer(expected=1)
        self.start_peer();seed=self.command('seed',count=3)
        self.assertEqual(len(seed['jobs']),3);jobs=[r['job_id'] for r in seed['jobs']]
        mapping=[(r['attempt_id'],r['job_id']) for r in seed['jobs']]
        self.assertTrue(all(jobs));self.assertTrue(all(r['lease_owner'] is None for r in seed['jobs']))
        # Only a graceful completed no-effect dispatch handoff, not crash reclaim.
        self.close_peer();reopened=self.start_peer()
        self.assertEqual([(r['attempt_id'],r['job_id']) for r in self.command('snapshot')['jobs']],mapping)
        self.start_worker(reopened['brokerOrigin'])
        deadline=time.monotonic()+90
        while True:
            state=self.command('pump')
            self.assertEqual([(r['attempt_id'],r['job_id']) for r in state['jobs']],mapping)
            with self.native._db() as db:
                self.assertLessEqual(db.execute('SELECT COUNT(*) FROM tasks WHERE status IN (10,20)').fetchone()[0],1)
                db.execute('UPDATE tasks SET status=40 WHERE status=20')
            with self.driver._db() as db:
                rows=db.execute('SELECT job,stage FROM driver_jobs').fetchall();stages=[r[1] for r in rows]
                self.assertTrue({r[0] for r in rows}<=set(jobs))
            if len(stages)==3 and stages.count('completed')==3:break
            self.assertLess(time.monotonic(),deadline,'synthetic vertical did not reach durable acceptance');time.sleep(.05)
        self.assertEqual(state['grantCount'],3);self.assertEqual(state['acknowledged'],3)
        self.assertEqual(state['retainedFiles'],3);self.assertEqual(state['ingestedFixtureFiles'],3)
        self.assertTrue(state['sourcePresent']);self.assertFalse(state['productionLaneEnabled']);self.assertEqual(state['modelCount'],0)
        for job in jobs:self.assertEqual(self.store.get(job)['nodeId'],'cluster')
        with self.driver._db() as db:self.assertEqual({r[0] for r in db.execute('SELECT job FROM driver_jobs')},set(jobs))
        with self.queue._db() as db:self.assertEqual({r[0] for r in db.execute('SELECT job_id FROM reference_jobs')},set(jobs))
        for archive in (self.viewer_state/'results').glob('*.zip'):self.assertEqual(archive.read_bytes(),ARCHIVE)
        self.assertEqual(self.native.counts().get('remove',0),0)
        for action in ('initialize','upload','commit','download'):self.assertEqual(self.native.counts().get(action,0),3)
        self.assertEqual(len(list(self.driver.ledger.root.glob('jobs/*/image-1.input'))),3)
        for source in self.driver.ledger.root.glob('jobs/*/image-1.input'):self.assertEqual(source.read_bytes(),b'abc')
        self.assertEqual(len(list(self.driver.ledger.root.glob('jobs/*/all.zip'))),3)
        verify_viewer_source(self.viewer_root,self.viewer_manifest,self.viewer_manifest_sha)
        self.close_peer()
        print(json.dumps(dict(scope='actual-app-draft-real-v3-wire-SYNTHETIC-authority-not-publication',jobs=3,
            route='cluster',actualResultBridge=True,productionEnabled=False,published=False,sourceAndResultsRetained=True,
            gracefulPreEffectReopen=True,seedFailureRolledBack=True,viewerCommit=VIEWER_COMMIT,
            jobIds=jobs,nativeCounts=self.native.counts(),loadedModules=state['loadedModules'],
            viewerSourceManifestSha256=self.viewer_manifest_sha,nodeBinarySha256=os.environ['VIEWER_APP_NODE_SHA256'],
            nodeChildren=self.child_outcomes)),flush=True)


if __name__=='__main__':unittest.main()
