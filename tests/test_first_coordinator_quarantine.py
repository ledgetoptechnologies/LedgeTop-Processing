"""Normal application quarantine feasibility; synthetic state, real TLS/tmpfs.

Run inside a task-owned Docker network=none namespace with no published ports.
This tests process restart in one immutable source/image boundary, not a public
Docker recreation, installed cutover, live NAS, or successful backend processing.
"""
import copy
from datetime import datetime,timezone
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from contextlib import closing

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from tests import test_coordinator_predecessor_census as old_fixture
from tests import test_reference_dynamic_source_intake_scale as tls_fixture
from tests.viewer_receipt_signer import SyntheticViewerSigner
from ops.coordinator_candidate_projection import freeze_expectations,verify_projection
from ops import coordinator_startup_policy as startup_policy


@unittest.skipUnless(os.name == 'posix', 'actual Linux lease and tmpfs qualification')
class FirstCoordinatorQuarantineTests(unittest.TestCase):
    def setUp(self):
        self.assertEqual(sorted(path.name for path in Path('/sys/class/net').iterdir()), ['lo'],
                         'qualification requires Docker network=none')
        self.old = old_fixture.ClosedPredecessorCensusTests()
        self.addCleanup(self.old.doCleanups)
        self.old.setUp(); self.old.add_closed_direct_plans()
        self.root = Path(self.old.temp.name)
        self.leases = self.root/'leases'; self.leases.mkdir(mode=0o711); self.leases.chmod(0o711)
        self.lease_file = self.leases/(self.old.authority+'.lease')
        self.lease_file.write_bytes(b'original-synthetic-enrolled-diagnostic'); self.lease_file.chmod(0o600)
        self.key,self.cert = tls_fixture._write_tls_material(self.root)
        self.native,self.reference = self.port(),self.port()
        self.api_token='synthetic-native-'+'a'*32
        self.operator='synthetic-operator-'+'b'*32
        self.viewer='synthetic-reference-viewer-'+'c'*32
        self.worker='synthetic-worker-cluster-'+'d'*32
        self.config_path=self.root/'ordinary-bootstrap.json'
        self.bootstrap=dict(schemaVersion=2,
            listener=dict(host='0.0.0.0',port=self.reference,certificate=str(self.cert),privateKey=str(self.key),clientCA=str(self.cert)),
            viewerBearers=dict(staging=self.viewer),
            workers={node:dict(bearer=self.worker if node=='cluster' else 'synthetic-worker-main-'+'e'*32,
                workerId='worker-'+node,instanceId='instance-'+node,resultOrigin='https://127.0.0.1:'+str(19991+index),
                resultBearer='synthetic-result-'+node+'-'+'f'*32) for index,node in enumerate(('cluster','main'))},
            streamBudget=dict(maxConcurrent=1,maxObjectBytes=1024**2),
            recoveryBindings={node:dict(engineId='physical-'+node,authorityId=self.old.authority,
                coordinatorOrigin='http://127.0.0.1:'+str(self.native),gateGeneration=1,outputBudget=1024**2)
                for node in ('cluster','main')})
        self.env={key:value for key,value in os.environ.items() if not key.startswith(('PROCESSING_','LOCAL_','MAIN_'))}
        self.env.update(PROCESSING_API_TOKEN=self.api_token,PROCESSING_PROTOTYPE_OPERATOR_TOKEN=self.operator,
            PROCESSING_BIND_HOST='0.0.0.0',PROCESSING_PORT=str(self.native),PROCESSING_PROTOTYPE_NODE_IDS='cluster,main',
            PROCESSING_TREEDAR_NODE_IDS='main',PROCESSING_PROTOTYPE_STATE_DIR=str(self.old.root),
            PROCESSING_AUTHORITY_LEASE_DIR=str(self.leases),PROCESSING_REFERENCE_CONFIG=str(self.config_path),
            PROCESSING_VIEWER_ENABLED='0',LOCAL_NODE_URL='http://127.0.0.1:19001',MAIN_NODE_URL='http://127.0.0.1:19002',
            PYTHONDONTWRITEBYTECODE='1')
        self.child=None
        self.addCleanup(self.stop_child)
        self.write_config(self.bootstrap)

    @staticmethod
    def port():
        with socket.socket() as value:
            value.bind(('127.0.0.1',0));return value.getsockname()[1]

    def write_config(self,value):
        self.config_path.write_text(json.dumps(value),encoding='utf-8');self.config_path.chmod(0o600)

    def request(self,method,path,token=None,payload=None,*,tls=False,headers_only=False):
        context=ssl.create_default_context(cafile=str(self.cert)) if tls else None
        connection=(http.client.HTTPSConnection('127.0.0.1',self.reference,timeout=3,context=context) if tls else
                    http.client.HTTPConnection('127.0.0.1',self.native,timeout=3))
        headers={'Authorization':'Bearer '+token} if token else {}
        if headers_only:
            headers.update({'Content-Length':'100000000','Content-Type':'application/json'})
            body=None
        else:
            body=json.dumps(payload).encode() if payload is not None else None
            if body is not None: headers['Content-Type']='application/json'
        try:
            connection.request(method,path,body=body,headers=headers)
            response=connection.getresponse();raw=response.read(8193)
            self.assertLessEqual(len(raw),8192)
            return response.status,dict(response.getheaders()),json.loads(raw)
        finally:connection.close()

    def start_child(self):
        self.assertIsNone(self.child)
        self.child=subprocess.Popen([sys.executable,'-m','processing.app'],cwd=str(Path(__file__).resolve().parents[1]),
            env=self.env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
        self.child_identity=(self.child.pid,Path('/proc/'+str(self.child.pid)+'/stat').read_text().split()[21])
        deadline=time.monotonic()+12
        while time.monotonic()<deadline:
            if self.child.poll() is not None:
                out,err=self.child.communicate();self.fail('normal constructor exited: '+err)
            try:
                status,_,_=self.request('GET','/healthz')
                if status==200:return
            except (OSError,http.client.HTTPException):pass
            time.sleep(0.025)
        self.fail('normal application readiness deadline exceeded')

    def stop_child(self):
        if self.child is None:return
        process,self.child=self.child,None
        if process.poll() is None:process.send_signal(signal.SIGTERM)
        try:out,err=process.communicate(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill();out,err=process.communicate(timeout=3)
            self.fail('owned normal application failed graceful shutdown: '+err)
        self.assertEqual(process.returncode,0,err)
        observed=Path('/proc/'+str(process.pid)+'/stat')
        if observed.exists():self.assertNotEqual(observed.read_text().split()[21],self.child_identity[1])

    def legacy_rows(self):
        result={}
        with closing(sqlite3.connect(self.old.path.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
            for kind,name,_,sql in self.old.schema['objects']:
                if kind!='table':continue
                with closing(sqlite3.connect(':memory:')) as expected:
                    if name=='sqlite_sequence':columns=['name','seq']
                    else:
                        expected.execute(sql)
                        columns=[row[1] for row in expected.execute('PRAGMA table_info("'+name+'")')]
                projection=','.join('"'+column+'"' for column in columns)
                result[name]=sorted([tuple(row) for row in db.execute('SELECT '+projection+' FROM "'+name+'"')],key=repr)
                actual=[row[1] for row in db.execute('PRAGMA table_info("'+name+'")')]
                extra=set(actual)-set(columns)
                self.assertLessEqual(extra,{'target_engine_version'} if name=='nodes' else set())
                if extra:self.assertEqual(db.execute('SELECT COUNT(*) FROM nodes WHERE target_engine_version IS NOT NULL').fetchone()[0],0)
        return result

    def artifact_bytes(self):
        values={}
        for path in (self.old.root/'viewer-tasks').rglob('*'):
            if path.is_file():values[str(path.relative_to(self.old.root))]=(hashlib.sha256(path.read_bytes()).hexdigest(),path.stat().st_mode)
        values['coordinator.lock']=((self.old.root/'coordinator.lock').read_bytes(),(self.old.root/'coordinator.lock').stat().st_mode)
        values['original-enrolled-lease']=(self.lease_file.read_bytes(),self.lease_file.stat().st_mode)
        return values

    def final_config(self):
        self.spool=self.root/'synthetic-mounted-spool';self.spool.mkdir(mode=0o700)
        marker_name='.ltds-q-'+uuid.uuid4().hex
        marker=Path('/tmp')/marker_name
        marker.write_bytes(b'a'*64);marker.chmod(0o600)
        def cleanup_marker():
            self.stop_child()
            marker.unlink(missing_ok=True)
        self.addCleanup(cleanup_marker)
        self.assertTrue(os.path.ismount('/tmp'))
        source_public=Ed25519PrivateKey.generate().public_key().public_bytes(
            serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        value=copy.deepcopy(self.bootstrap)
        value.update(schemaVersion=6,viewerReceiptKeys=SyntheticViewerSigner().trusted_keys,viewerAcceptance={},
            sourceSpool=dict(root=str(self.spool),mountPath='/tmp',mountId='a'*64,markerName=marker_name,
                             safetyFloorBytes=0,maxObjectBytes=1024**2,chunkBytes=65536),
            sourceAttestationKeys=[dict(environment='staging',issuerId='viewer-test',keyId='source-v1',
                                      principalId='viewer-staging',publicKeyPem=source_public)])
        return value

    def test_normal_quarantine_seal_and_frozen_restart_preserve_legacy_data(self):
        before_rows,before_files=self.legacy_rows(),self.artifact_bytes()
        baseline=self.old.census();final_config=self.final_config()
        source=Path(__file__).resolve().parents[1]
        policy=startup_policy.build_startup_policy(source,
            expected_source_hashes={name:hashlib.sha256((source/name).read_bytes()).hexdigest() for name in startup_policy.SOURCES},
            candidate_image_id='sha256:'+'a'*64,candidate_source_commit='b'*40,
            bootstrap_config_bytes=json.dumps(self.bootstrap).encode(),final_config_bytes=json.dumps(final_config).encode(),
            node_endpoints=[dict(id='cluster',url='http://127.0.0.1:19001'),dict(id='main',url='http://127.0.0.1:19002')],
            authority_id=self.old.authority)
        now=lambda:datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        bootstrap_earliest=now()
        self.start_child()
        status,_,state=self.request('GET','/v1/prototype/coordinator-freeze',self.operator)
        self.assertEqual(status,200);self.assertEqual(state['phase'],'open')
        self.assertEqual(state['authorityId'],self.old.authority)
        original_startup=state['startupId'];plan=str(uuid.uuid4())
        # No discovery or backend is mocked: these are unbound real loopback URLs.
        status,_,_=self.request('POST','/ltds/v2/jobs',self.viewer,{},tls=True)
        self.assertEqual(status,503)
        for action in ('intent','drain','seal'):
            request=dict(requestId=str(uuid.uuid5(uuid.UUID(plan),action)),planId=plan,
                         expectedRevision=state['revision'],action=action,startupId=original_startup)
            status,_,state=self.request('POST','/v1/prototype/coordinator-freeze',self.operator,request)
            self.assertEqual(status,200,state)
        self.assertEqual(state['phase'],'frozen')
        for tls,path,token in ((False,'/v1/prototype/jobs',self.api_token),
                               (True,'/ltds/v2/jobs',self.viewer),(True,'/ltds/worker/v1/claim',self.worker)):
            status,headers,body=self.request('POST',path,token,tls=tls,headers_only=True)
            self.assertEqual((status,body),(503,dict(error='coordinator_maintenance')))
            self.assertEqual(headers['Retry-After'],'2')
        self.stop_child()
        self.assertEqual(self.legacy_rows(),before_rows)
        self.assertEqual(self.artifact_bytes(),before_files)
        # Reconstruct expected control rows from authenticated RPC identities
        # and independently statted ORIGINAL physical bindings, never DB values.
        database=self.old.path.stat();root_info=self.old.root.stat()
        main_lock=(self.old.root/'coordinator.lock').stat()
        external=self.lease_file.stat();external_dir=self.leases.stat()
        binding=dict(authorityId=self.old.authority,database=str(self.old.path),
            databaseIdentity=[database.st_dev,database.st_ino],root=str(self.old.root),
            rootIdentity=[root_info.st_dev,root_info.st_ino],lease=str(self.old.root/'coordinator.lock'),
            leaseIdentity=[main_lock.st_dev,main_lock.st_ino,main_lock.st_mode,main_lock.st_nlink,main_lock.st_uid],
            authorityLease=dict(authorityId=self.old.authority,directory=str(self.leases),
                directoryIdentity=[external_dir.st_dev,external_dir.st_ino,external_dir.st_mode,external_dir.st_uid,external_dir.st_gid],
                path=str(self.lease_file),
                fileIdentity=[external.st_dev,external.st_ino,external.st_mode,external.st_nlink,external.st_uid,external.st_gid]))
        expected=freeze_expectations(authority_id=self.old.authority,plan_id=plan,
            owner_sha256=hashlib.sha256(self.operator.encode('ascii')).hexdigest(),
            bootstrap_startup_id=original_startup,current_startup_id=original_startup,
            request_ids={action:str(uuid.uuid5(uuid.UUID(plan),action)) for action in ('intent','drain','seal')},
            operation_binding=binding,reserved_control_bytes=4096,current_closed=True)
        with closing(sqlite3.connect(self.old.path)) as db:
            db.row_factory=sqlite3.Row
            for table,values in expected.items():
                actual=[dict(row) for row in db.execute('SELECT * FROM "'+table+'"')]
                self.assertEqual(sorted(map(json.dumps,actual)),sorted(map(json.dumps,values)),table)
        context=dict(bootstrapStartupId=original_startup,currentStartupId=original_startup,planId=plan,
            ownerSha256=hashlib.sha256(self.operator.encode('ascii')).hexdigest(),
            requestIds={action:str(uuid.uuid5(uuid.UUID(plan),action)) for action in ('intent','drain','seal')},
            operationBinding=binding,sourceReaderBinding=None,sourceCustody=None,
            bootstrapWindow=dict(earliest=bootstrap_earliest,latest=now()),
            finalWindow=dict(earliest=bootstrap_earliest,latest=now()))
        def verify(phase):
            from ops.coordinator_predecessor_census import _canonical
            return verify_projection(self.old.root,baseline=baseline,policy_bytes=policy,
                expected_policy_sha256=hashlib.sha256(policy).hexdigest(),
                expected_baseline_sha256=hashlib.sha256(_canonical(baseline)).hexdigest(),key=self.old.key,
                phase=phase,gate_binding=context,uid=os.geteuid(),max_artifact_bytes=16*1024*1024)
        self.assertTrue(verify('bootstrap-frozen')['retainedFilesValidated'])
        self.assertTrue(verify('rollback-bootstrap')['retainedFilesValidated'])
        self.write_config(final_config)
        final_earliest=now()
        self.start_child()
        status,_,state=self.request('GET','/v1/prototype/coordinator-freeze',self.operator)
        self.assertEqual(status,200);self.assertEqual(state['phase'],'frozen')
        self.assertEqual(state['planId'],plan);self.assertNotEqual(state['startupId'],original_startup)
        status,_,custody=self.request('GET','/v1/prototype/coordinator-custody',self.operator)
        self.assertEqual(status,200);self.assertEqual(custody['status'],'verified')
        reader_root=self.old.root/'source-readers';reader_lease=reader_root/'lease'
        physical=reader_lease.stat();physical_root=reader_root.stat();database=self.old.path.stat()
        source_binding=dict(authorityId=self.old.authority,database=str(self.old.path),
            databaseIdentity=[database.st_dev,database.st_ino],root=str(reader_root),
            rootIdentity=[physical_root.st_dev,physical_root.st_ino],
            leaseIdentity=[physical.st_dev,physical.st_ino,physical.st_size,physical.st_mode,physical.st_nlink,
                           physical.st_uid,physical.st_mtime_ns,physical.st_ctime_ns],
            marker=reader_lease.read_bytes().hex())
        context.update(currentStartupId=state['startupId'],sourceReaderBinding=source_binding,sourceCustody=custody,
                       finalWindow=dict(earliest=final_earliest,latest=now()))
        self.assertTrue(verify('final-frozen')['retainedFilesValidated'])
        for tls,path,token in ((False,'/v1/prototype/jobs',self.api_token),(True,'/ltds/v2/jobs',self.viewer)):
            status,_,body=self.request('POST',path,token,tls=tls,headers_only=True)
            self.assertEqual((status,body),(503,dict(error='coordinator_maintenance')))
        self.stop_child()
        self.assertEqual(self.legacy_rows(),before_rows)
        self.assertEqual(self.artifact_bytes(),before_files)
        self.assertEqual(list(self.spool.iterdir()),[])
        self.assertTrue(verify('rollback-final')['retainedFilesValidated'])
        with closing(sqlite3.connect(self.old.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0],22)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_jobs').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_source_intakes').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM coordinator_operation_pins').fetchone()[0],0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM source_reader_pins WHERE state IN ('active','uncertain')").fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM coordinator_freeze_receipts').fetchone()[0],3)


if __name__=='__main__':unittest.main()
