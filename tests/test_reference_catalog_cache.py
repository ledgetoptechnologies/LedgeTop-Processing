"""Real native HTTP/TLS + authoritative Queue, synthetic option definitions."""
import copy
from dataclasses import replace
import json
import os
import sqlite3
import subprocess
import time
import unittest
from contextlib import contextmanager
from contextlib import closing
from unittest.mock import patch
from urllib.request import Request

from processing.reference_api import Response
from processing.reference_runtime import ReferenceRuntime,ReferenceSettings
from processing.reference_catalog_cache import CatalogAdmissionPolicy,CatalogHeld
from processing.prototype import Queue
from processing import reference_protocol as p
from tests import test_reference_routing as routing

class NativeCatalog:
    def __init__(self):
        self.definitions=[dict(name='future-quality',type='int',value=1,help='Synthetic quality')]
        self.info=dict(version='2.2.3',engine='ODM',engineVersion='synthetic-1',taskQueueCount=0,maxImages=None)
        self.code=200;self.calls=[]
    def handle(self,method,path,headers,body=b'',**kw):
        self.calls.append((method,path))
        value=self.definitions if path=='/options' else self.info if path=='/info' else {'error':'not_found'}
        return Response(self.code if path in ('/options','/info') else 404,value)

@unittest.skipUnless(os.name=='posix','Linux authoritative Queue/TLS qualification')
class CatalogCacheTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):routing.ReferenceRoutingTests.setUpClass()
    @classmethod
    def tearDownClass(cls):routing.ReferenceRoutingTests.doClassCleanups()
    def setUp(self):
        self.fixture=routing.ReferenceRoutingTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        f=self.fixture;f.queue.close()
        self.queue=Queue(f.root/'queue',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',),catalog_admission=True)
        self.addCleanup(self.queue.close);self.natives={};origins={}
        for node in ('cluster','main'):
            self.natives[node]=NativeCatalog();origins[node]=f.server(self.natives[node],False)
        self.config=replace(f.config,local_node_url=origins['cluster'],main_node_url=origins['main'],reference_main_fallback=False)
        self.settings=ReferenceSettings(dict(host='127.0.0.1',port=0,certificate=str(f.cert),privateKey=str(f.key),clientCA=str(f.cert)),
            {'synthetic-viewer-'+'a'*32:'staging','synthetic-production-'+'b'*32:'production'},
            {node:dict(worker,bearer=node+'-bearer-000000000000') for node,worker in f.workers.items()},
            dict(maxConcurrent=1,maxObjectBytes=1048576),routing_bindings=f.bindings,catalog_admission=CatalogAdmissionPolicy(60))
        self.now=[time.time()]
        self.runtime=self.make_runtime();self.addCleanup(self.runtime.close)
    def make_runtime(self):return ReferenceRuntime(self.queue,self.config,self.settings,catalog_clock=lambda:self.now[0])
    def call(self,method,path,body=b'',production=False,headers=None):
        token='synthetic-production-'+'b'*32 if production else 'synthetic-viewer-'+'a'*32
        return self.runtime.api.handle(method,p.PREFIX+path,headers or {'Authorization':'Bearer '+token},body,protected_transport=True)
    def raw(self):
        value=copy.deepcopy(self.fixture.fixture['submission'])
        import uuid
        value['submissionId']=str(uuid.uuid4())
        cap=self.call('GET','/capabilities');self.assertEqual(cap.status,200)
        value['capabilityFingerprint']=cap.body['capabilityFingerprint']
        value['pipeline']['odm']['optionsFingerprint']=cap.body['engines']['odm']['optionsFingerprint']
        value['pipeline']['odm']['options']={'future-quality':1}
        return value
    def submit(self,value,production=False):return self.call('POST','/jobs',p.canonical_json(value),production)
    def offline(self):
        for node in ('cluster','main'):
            self.offline_node(node)
    def offline_node(self,node):
        port=int((self.config.local_node_url if node=='cluster' else self.config.main_node_url).rsplit(':',1)[1])
        for server,thread in self.fixture.servers:
            if server.server_port==port:server.shutdown();thread.join(2);server.server_close()
    def cold_reopen(self):
        self.runtime.close();self.queue.close()
        self.queue=Queue(self.fixture.root/'queue',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',),catalog_admission=True)
        self.addCleanup(self.queue.close);self.runtime=self.make_runtime();self.addCleanup(self.runtime.close)
    def test_both_offline_metadata_only_survives_restart_and_preserves_fifo(self):
        value=self.raw();self.offline()
        with patch('processing.reference_runtime.catalog',side_effect=AssertionError('no legacy catalog')):
            first=self.submit(value);self.assertEqual(first.status,201)
            second=copy.deepcopy(value)
            import uuid
            second['submissionId']=str(uuid.uuid4())
            prod=self.submit(second,True);self.assertEqual(prod.status,201)
        self.assertEqual(first.headers['X-LTDS-Catalog-Source'],'cached')
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_option_evidence').fetchone()[0],2)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claims').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_transfer_grants').fetchone()[0],0)
            self.assertEqual(db.execute("SELECT id FROM jobs WHERE status='queued' ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END,seq").fetchone()[0],prod.body['jobId'])
        self.runtime.close();self.queue.close()
        self.queue=Queue(self.fixture.root/'queue',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',),catalog_admission=True)
        self.addCleanup(self.queue.close);self.runtime=self.make_runtime();self.addCleanup(self.runtime.close)
        self.assertEqual(self.call('GET','/capabilities').status,200)
        replay=self.submit(value);self.assertEqual(replay.status,200);self.assertEqual(replay.body['jobId'],first.body['jobId'])
    def test_authenticated_only_and_enabled_worker_profile_required(self):
        before=[len(n.calls) for n in self.natives.values()]
        self.assertEqual(self.call('GET','/capabilities',headers={'Authorization':'Bearer wrong'}).status,401)
        self.assertEqual(before,[len(n.calls) for n in self.natives.values()])
        raw=self.raw();job=self.submit(raw).body
        api=self.runtime.control.worker;headers={'Authorization':'Bearer cluster-bearer-000000000000'}
        self.assertEqual(api.handle('GET','/ltds/worker/v1/next',headers,protected_transport=True).status,409)
        headers['X-LTDS-Option-Evidence']='1'
        claim=api.handle('POST','/ltds/worker/v1/claim',headers,p.canonical_json(dict(jobId=job['jobId'],claimRequestId='original-request')),protected_transport=True)
        self.assertEqual(claim.status,200)
        path='/ltds/worker/v1/jobs/'+job['jobId']+'/option-evidence'
        self.assertEqual(api.handle('GET',path,headers,protected_transport=True).status,401)
        headers['X-LTDS-Claim-Token']=claim.body['claimToken']
        evidence=api.handle('GET',path,headers,protected_transport=True)
        self.assertEqual(evidence.status,200);self.assertEqual(evidence.body['claimRequestId'],'original-request')
        self.assertEqual(evidence.body['selectedDefinitions'],self.natives['cluster'].definitions)
        self.assertNotIn(claim.body['claimToken'],p.canonical_json(evidence.body).decode())
        headers['X-LTDS-Claim-Token']='wrong'
        self.assertEqual(api.handle('GET',path,headers,protected_transport=True).status,409)
    def test_invalid_unknown_version_and_auth_invalidate_not_fallback(self):
        for kind in ('auth','version','bad_options'):
            with self.subTest(kind=kind):
                self.natives['cluster'].code=200;self.natives['cluster'].info['engineVersion']='synthetic-1';self.natives['cluster'].definitions[0]['type']='int'
                self.assertEqual(self.call('GET','/capabilities').status,200)
                if kind=='auth':self.natives['cluster'].code=401
                if kind=='version':self.natives['cluster'].info['engineVersion']='changed'
                if kind=='bad_options':self.natives['cluster'].definitions=[{'oops':True}]
                self.assertEqual(self.call('GET','/capabilities').status,503)
                with self.queue._db() as db:self.assertEqual(self.runtime.store.catalog_admission._state(db)['state'],'invalidated')
                self.natives['cluster'].definitions=[dict(name='future-quality',type='int',value=1,help='Synthetic quality')]
        self.offline();self.assertEqual(self.call('GET','/capabilities').status,503)
    def test_expired_and_backward_clock_holds(self):
        self.raw();self.offline();self.now[0]+=61
        self.assertEqual(self.call('GET','/capabilities').status,503)
        self.now[0]-=62;self.assertEqual(self.call('GET','/capabilities').status,503)
    def test_job_and_definition_record_commit_together_and_retain_old_epoch(self):
        value=self.raw();admission=self.runtime.store.catalog_admission
        with patch.object(admission,'insert_evidence',side_effect=RuntimeError('lost insert')):
            self.assertEqual(self.submit(value).status,500)
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_jobs').fetchone()[0],0)
        job=self.submit(value).body
        for native in self.natives.values():native.definitions[0]['help']='Changed definition'
        self.assertEqual(self.call('GET','/capabilities').status,200)
        with self.queue._db() as db:
            old=admission.read_evidence(db,job['jobId'])
            self.assertEqual(old['selectedDefinitions'][0]['help'],'Synthetic quality')
        changed=self.raw()
        with self.queue._db() as db:
            used=db.execute('SELECT SUM(length(CAST(input_json AS BLOB))) FROM reference_jobs').fetchone()[0]
            for table in ('reference_catalog_state','reference_option_evidence'):
                used+=db.execute(f'SELECT SUM(length(CAST(payload AS BLOB))+length(digest)) FROM {table}').fetchone()[0]
            used+=176
        self.runtime.store.max_metadata_bytes=used
        self.assertEqual(self.submit(changed).status,409)

    def test_schema_trigger_marker_and_bool_version_drift_refuse_cold_reopen(self):
        self.raw();self.runtime.close()
        for kind in ('trigger','missing_state','bool_version'):
            with self.subTest(kind=kind):
                with self.queue._db() as db:
                    original=[tuple(r) for r in db.execute('SELECT * FROM reference_catalog_state')]
                    if kind=='trigger':db.execute('DROP TRIGGER reference_catalog_fence_insert')
                    if kind=='missing_state':db.execute('DELETE FROM reference_catalog_state')
                    if kind=='bool_version':
                        state=json.loads(original[0][1]);state['schemaVersion']=True
                        db.execute('UPDATE reference_catalog_state SET payload=?,digest=?',(p.canonical_json(state).decode(),p.digest(state)))
                self.queue.close();before=self.queue.path.read_bytes()
                with self.assertRaises(CatalogHeld):Queue(self.fixture.root/'queue',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',),catalog_admission=True)
                self.assertEqual(self.queue.path.read_bytes(),before)
                # Test-only restore, never an application repair path.
                with closing(sqlite3.connect(self.queue.path)) as db,db:
                    if kind=='trigger':
                        from processing.reference_catalog_cache import _SCHEMA
                        db.execute(_SCHEMA['reference_catalog_fence_insert'])
                    else:
                        db.execute('DELETE FROM reference_catalog_state')
                        db.executemany('INSERT INTO reference_catalog_state VALUES(?,?,?,?,?,?,?)',original)
                self.queue=Queue(self.fixture.root/'queue',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',),catalog_admission=True)
                self.addCleanup(self.queue.close)
    def test_corrupt_cache_is_held_without_repair_and_modeoff_refuses_marker(self):
        self.raw()
        with self.queue._db() as db:db.execute("UPDATE reference_catalog_state SET digest=?",('0'*64,))
        before=self.queue.path.read_bytes()
        self.assertEqual(self.call('GET','/capabilities').status,503)
        self.assertEqual(self.queue.path.read_bytes(),before)
        self.runtime.close();self.queue.close()
        with self.assertRaisesRegex(RuntimeError,'catalog admission ABI'):
            Queue(self.fixture.root/'queue',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',))
    def test_permanent_fence_refuses_unqualified_sql_and_rolls_back(self):
        self.raw();before=self.queue.path.read_bytes()
        with closing(sqlite3.connect(self.queue.path)) as db:
            with self.assertRaisesRegex(sqlite3.OperationalError,'reference_catalog_writer_v1'):
                db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES('old','old','staging','reference-pipeline-v1','queued')")
        self.assertEqual(self.queue.path.read_bytes(),before)

    def test_actual_tls_diagnostics_remain_headers_and_public_v1_body_closed(self):
        origin=self.fixture.server(self.runtime.api,True)
        def request():
            with self.fixture.open(Request(origin+p.PREFIX+'/capabilities',headers={'Authorization':'Bearer synthetic-viewer-'+'a'*32}),timeout=5) as response:
                return json.loads(response.read()),dict(response.headers)
        body,headers=request()
        self.assertEqual(headers['X-LTDS-Catalog-Source'],'fresh')
        self.offline();cached,cached_headers=request()
        self.assertEqual(cached,body);self.assertEqual(cached_headers['X-LTDS-Catalog-Source'],'cached')
        self.assertIn('X-LTDS-Catalog-Observed-At',cached_headers)
        self.assertEqual(set(body),{'protocol','schemaVersion','environment','capabilityFingerprint','limits','engines'})

    def test_unknown_cache_commit_ack_never_returns_success_or_creates_job(self):
        self.raw();original=self.queue._db;lost=[False]
        @contextmanager
        def lost_ack():
            with original() as db:yield db
            if not lost[0]:lost[0]=True;raise RuntimeError('synthetic unknown commit acknowledgment')
        with patch.object(self.queue,'_db',lost_ack):
            self.assertEqual(self.call('GET','/capabilities').status,503)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_jobs').fetchone()[0],0)
        self.assertEqual(self.call('GET','/capabilities').status,200)

    def test_bool_evidence_version_and_metadata_overflow_are_readonly_holds(self):
        value=self.raw();job=self.submit(value).body;cache=self.runtime.store.catalog_admission
        with self.queue._db() as db:
            row=db.execute('SELECT payload FROM reference_option_evidence').fetchone()
            evidence=json.loads(row[0]);evidence['schemaVersion']=True
            db.execute('UPDATE reference_option_evidence SET payload=?,digest=?',(p.canonical_json(evidence).decode(),p.digest(evidence)))
        before=self.queue.path.read_bytes()
        with self.queue._db() as db,self.assertRaises(CatalogHeld):cache.read_evidence(db,job['jobId'])
        self.assertEqual(before,self.queue.path.read_bytes())

    def test_default_off_opens_no_catalog_and_creates_no_marker_or_fence(self):
        plain=Queue(self.fixture.root/'plain',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',))
        self.addCleanup(plain.close)
        settings=replace(self.settings,catalog_admission=None)
        with patch('processing.reference_catalog_cache.CatalogAdmission',side_effect=AssertionError('defaultoff must not initialize cache')):
            runtime=ReferenceRuntime(plain,self.config,settings);self.addCleanup(runtime.close)
        with plain._db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'reference_catalog_%' OR name='reference_option_evidence'").fetchone()[0],0)

    def test_changed_private_token_and_real_ca_scope_refuse_offline_restart_without_repair(self):
        self.raw();self.offline();self.runtime.close();self.queue.close()
        original_config,original_settings=self.config,self.settings
        extra=self.fixture.root/'extra-ca.pem';key=self.fixture.root/'extra-ca.key'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=another-real-synthetic-ca','-keyout',str(key),'-out',str(extra)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        bundle=self.fixture.root/'combined-ca.pem';bundle.write_bytes(self.fixture.cert.read_bytes()+extra.read_bytes())
        for kind in ('token','ca'):
            with self.subTest(kind=kind):
                self.queue=Queue(self.fixture.root/'queue',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',),catalog_admission=True)
                self.addCleanup(self.queue.close)
                before=self.queue.path.read_bytes()
                self.config=replace(original_config,local_node_token='changed-configured-native-token') if kind=='token' else original_config
                self.settings=replace(original_settings,listener=dict(original_settings.listener,clientCA=str(bundle))) if kind=='ca' else original_settings
                from processing.reference_runtime import RuntimeConfigurationError
                with self.assertRaises(RuntimeConfigurationError):self.make_runtime()
                self.assertEqual(self.queue.path.read_bytes(),before)
                self.queue.close()
        self.config,self.settings=original_config,original_settings
        self.queue=Queue(self.fixture.root/'queue',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',),catalog_admission=True)
        self.addCleanup(self.queue.close);self.runtime=self.make_runtime();self.addCleanup(self.runtime.close)
        self.assertEqual(self.call('GET','/capabilities').status,200)

    def test_every_fresh_connection_enforces_physical_page_cap_and_full_transaction_rollback(self):
        self.raw()
        with self.queue._db() as db:
            self.assertEqual(tuple(db.execute('PRAGMA max_page_count').fetchone()),(32768,))
        before=self.queue.path.read_bytes()
        with self.assertRaisesRegex(sqlite3.OperationalError,'full'):
            with self.queue._db() as db:
                self.assertEqual(tuple(db.execute('PRAGMA max_page_count').fetchone()),(32768,))
                db.execute('BEGIN IMMEDIATE')
                db.execute('CREATE TABLE synthetic_allocation_probe(payload BLOB NOT NULL)')
                db.execute('INSERT INTO synthetic_allocation_probe VALUES(zeroblob(?))',(32768*4096,))
        self.assertEqual(self.queue.path.read_bytes(),before)
        with self.queue._db() as db:
            self.assertEqual(tuple(db.execute('PRAGMA max_page_count').fetchone()),(32768,))
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='synthetic_allocation_probe'").fetchone())

    def test_auth_invalidation_at_exact_quota_remains_held_after_outage_and_restart(self):
        self.raw()
        with self.queue._db() as db:
            used=db.execute('SELECT length(CAST(payload AS BLOB))+length(digest)+176 FROM reference_catalog_state').fetchone()[0]
        self.runtime.store.max_metadata_bytes=used
        self.natives['cluster'].code=401
        self.assertEqual(self.call('GET','/capabilities').status,503)
        with self.queue._db() as db:
            row=db.execute('SELECT invalidated,pending,length(high_water) FROM reference_catalog_state').fetchone()
            self.assertEqual(tuple(row),(1,'0'*32,64))
        self.offline();self.assertEqual(self.call('GET','/capabilities').status,503)
        self.cold_reopen();self.assertEqual(self.call('GET','/capabilities').status,503)

    def uncertain_invalidation(self,after_commit):
        self.raw();self.natives['cluster'].code=401;original=self.queue._db;calls=[0]
        @contextmanager
        def faulty():
            calls[0]+=1;ordinal=calls[0]
            with original() as db:
                yield db
                if ordinal==2 and not after_commit:raise sqlite3.OperationalError('synthetic invalidation I/O failure')
            if ordinal==2 and after_commit:raise RuntimeError('synthetic invalidation commit ACK loss')
        with patch.object(self.queue,'_db',faulty):self.assertEqual(self.call('GET','/capabilities').status,503)
        with self.queue._db() as db:
            row=db.execute('SELECT invalidated,pending FROM reference_catalog_state').fetchone()
            self.assertTrue(row['invalidated']==1 or row['pending']!='0'*32)
            self.assertEqual(self.runtime.store.catalog_admission._state(db)['state'],'invalidated')
        self.offline();self.cold_reopen()
        self.assertEqual(self.call('GET','/capabilities').status,503)
    def test_invalidation_write_rollback_after_auth_failure_preserves_durable_pending_hold(self):
        self.uncertain_invalidation(False)
    def test_invalidation_commit_ack_loss_after_auth_failure_preserves_durable_hold(self):
        self.uncertain_invalidation(True)

    def partial_known_drift(self,kind):
        self.raw();self.offline_node('main')
        if kind=='definition':self.natives['cluster'].definitions[0]['help']='Changed at same engineVersion'
        else:self.natives['cluster'].info['version']='new-node-app-version'
        self.assertEqual(self.call('GET','/capabilities').status,503)
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT invalidated FROM reference_catalog_state').fetchone()[0],1)
        self.offline_node('cluster');self.cold_reopen()
        self.assertEqual(self.call('GET','/capabilities').status,503)
    def test_known_fresh_definition_drift_with_other_node_offline_invalidates_entire_cache(self):
        self.partial_known_drift('definition')
    def test_known_fresh_node_app_version_drift_with_other_node_offline_invalidates_entire_cache(self):
        self.partial_known_drift('application_version')

    def test_control_marker_bitflip_refuses_read_and_cold_reopen_without_repair(self):
        self.raw();self.natives['cluster'].code=401
        self.assertEqual(self.call('GET','/capabilities').status,503)
        with self.queue._db() as db:db.execute('UPDATE reference_catalog_state SET invalidated=0')
        before=self.queue.path.read_bytes();self.offline()
        self.assertEqual(self.call('GET','/capabilities').status,503)
        self.assertEqual(self.queue.path.read_bytes(),before)
        self.runtime.close();self.queue.close()
        with self.assertRaisesRegex(CatalogHeld,'catalog_control_changed'):
            Queue(self.fixture.root/'queue',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',),catalog_admission=True)
        self.assertEqual(self.queue.path.read_bytes(),before)

    def test_control_checksum_cannot_be_transplanted_across_payload_generations(self):
        self.raw()
        with self.queue._db() as db:old=db.execute('SELECT control_sha256 FROM reference_catalog_state').fetchone()[0]
        for native in self.natives.values():native.definitions[0]['help']='New verified full definition'
        self.assertEqual(self.call('GET','/capabilities').status,200)
        with self.queue._db() as db:db.execute('UPDATE reference_catalog_state SET control_sha256=?',(old,))
        before=self.queue.path.read_bytes()
        self.assertEqual(self.call('GET','/capabilities').status,503)
        self.assertEqual(self.queue.path.read_bytes(),before)
