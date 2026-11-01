"""Synthetic native proof/transport, actual Queue/TLS/SQLite/leases/ZIP APIs."""
from contextlib import closing,ExitStack
from dataclasses import replace
from datetime import datetime,timedelta,timezone
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import ssl
import shutil
import struct
import subprocess
import tempfile
import threading
import unittest
import zipfile
from unittest.mock import patch

from processing import reference_treedar_publication as m,reference_protocol as p,reference_parent_guard as guard
from processing.reference_http import create_reference_http_server
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_delivery import DeliveryReceiver
from processing.reference_odm_stage import NativeODMStage,NativeRuntimeProbe
from processing.node_gate import NodeGate
from tests.test_reference_treedar_bundle import FIXTURE
from tests.test_reference_treedar_stage import Capture,Inputs,Provenance,FakeDocker
from tests import test_reference_treedar_stage as tree_fixture
from tests import test_reference_odm_stage as odm
from tests.test_reference_pipeline import ActualGuard
from processing.reference_pipeline import PipelineJournal
from processing.reference_treedar_stage import TreeStagePolicy
from processing.treedar_engine import TreeDarConfig,SOURCE_FILES,source_digest


class LargeTreeFixture(unittest.TestCase):
    """Independent larger parent reservation for retained multi-copy tests."""
    open=tree_fixture.TreeStageTests.open
    begin=tree_fixture.TreeStageTests.begin
    actions=tree_fixture.TreeStageTests.actions
    def setUp(self):
        self.host=odm.NativeODMStageTests();self.addCleanup(self.host.doCleanups);self.host.setUp();h=self.host
        source=h.root/'source';(source/'processing').mkdir(parents=True,mode=0o700);source.chmod(0o700)
        for name in SOURCE_FILES:
            target=source/'processing'/name;shutil.copyfile(Path(__file__).resolve().parents[1]/'processing'/name,target);target.chmod(0o600)
        config=TreeDarConfig('/usr/bin/docker',str(h.root/'docker.sock'),'sha256:'+'b'*64,h.policy.engine_id,h.policy.authority_id,h.policy.coordinator_origin,1,h.policy.worker_id,str(source),source_digest(source),uid=os.geteuid(),gid=os.getegid())
        self.policy=TreeStagePolicy(config,max_raw_bytes=10000,max_files=64)
        h.policy=replace(h.policy,treedar_runtime_sha256=self.policy.sha256,treedar_output_budget=1000000)
        with closing(sqlite3.connect(h.ledger.path)) as db,db:
            row=db.execute('SELECT settings FROM reservations').fetchone();settings=json.loads(row[0]);settings['output_budget']=1000100
            db.execute('UPDATE reservations SET settings=?,remaining=remaining+999900',(json.dumps(settings),))
        self.receipt=dict(schemaVersion=1,receiptId='original-source',jobId=h.claim['jobId'],generation=1,environment='staging',objectId='original-lidar',objectSha256=h.claim['submission']['manifest']['objects'][-1]['sha256'],byteSize=3,sourceKind='lidar',verticalUnits='metre',verticalDatum='navd88')
        self.authenticated=True;h.journal=PipelineJournal(h.root/'tree-pipeline.sqlite',h.policy,verify_provenance=lambda receipt:self.authenticated and receipt==self.receipt)
        h.guard=ActualGuard(h);h.plan=h.journal.bind_parent(h.claim,claim_request_id='parent-claim',reservation_id='parent-reservation',gate_receipt_id='a'*32,guard=h.guard);h.inputs=odm.OriginalInputs(h)
        h.stage=NativeODMStage(h.root/'tree-prior.sqlite',h.runtime,journal=h.journal,ledger=h.ledger,engine=h.native,parent_guard=h.guard,state_owner=h.owner,input_resolver=h.inputs,runtime_probe=NativeRuntimeProbe(h.runtime,runner=h.docker))
        h.advance();h.stage.tick(h.claim['jobId']);h.reconcile();self.capture=Capture(h.journal)
        h.journal.advance(h.claim['jobId'],'treedar',self.capture,h.guard,provenance=self.receipt);self.intent=self.capture.intent
        self.inputs=Inputs(h);self.provenance=Provenance(h,self.receipt);self.docker=FakeDocker(self.policy);self.stage=self.open()


@unittest.skipUnless(os.name=='posix','Linux publication leases')
class PublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory();cls.addClassCleanup(cls.tmp.cleanup);root=Path(cls.tmp.name);cls.cert=root/'cert.pem';cls.key=root/'key.pem'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=synthetic-loopback','-addext','subjectAltName=IP:127.0.0.1','-addext','basicConstraints=critical,CA:TRUE','-keyout',str(cls.key),'-out',str(cls.cert)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=10)

    def setUp(self):
        self.fixture=LargeTreeFixture();self.addCleanup(self.fixture.doCleanups);self.fixture.setUp();f=self.fixture;h=f.host;self.host=h
        transfer=dict(reservationId='parent-reservation',generation=1,expiresAt=(datetime.now(timezone.utc)+timedelta(seconds=240)).isoformat(timespec='milliseconds').replace('+00:00','Z'),manifestSha256=h.claim['manifestSha256'],byteBudget=h.claim['submission']['manifest']['totalBytes'],nonce='synthetic-transfer')
        h.store.reserve(h.claim['jobId'],transfer,node_id='main');h.store.update_phase(h.claim['jobId'],'fetching');h.store.update_phase(h.claim['jobId'],'processing')
        secret='synthetic-publication-worker-0001';api=ReferenceWorkerAPI(h.store,DeliveryReceiver(h.store),bearer_workers={secret:dict(node_id='main',worker_id='main-worker',worker_instance_id='instance-one')})
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(self.cert,self.key);self.server=create_reference_http_server(api,ssl_context=context)
        self.server.daemon_threads=False;self.server.block_on_close=True
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=False);self.addCleanup(self.stop);self.thread.start()
        trust=ssl.create_default_context(cafile=str(self.cert));policy=guard.ClaimStatePolicy('https://127.0.0.1:'+str(self.server.server_port),h.policy.authority_id,'main','main-worker','instance-one','worker-main',guard.tls_trust_sha256(trust))
        client=guard.ClaimStateClient(policy,guard.WorkerCredential('worker-main',secret),ssl_context=trust)
        driver=h.root/'publication-driver.sqlite';binding=dict(origin=policy.origin,nodeId='main',workerId='main-worker',workerInstanceId='instance-one',authorityId=h.policy.authority_id,engineId=h.policy.engine_id,gateOrigin=h.policy.coordinator_origin,gateGeneration=1,outputBudget=h.policy.odm_output_budget+h.policy.treedar_output_budget,environments=sorted(h.policy.allowed_environments))
        with closing(sqlite3.connect(driver)) as db,db:
            db.execute('CREATE TABLE driver_binding(singleton INTEGER PRIMARY KEY,binding TEXT)');db.execute('INSERT INTO driver_binding VALUES(1,?)',(json.dumps(binding),))
            db.execute('CREATE TABLE driver_jobs(job TEXT PRIMARY KEY,claim_request TEXT,data TEXT)');db.execute('INSERT INTO driver_jobs VALUES(?,?,?)',(h.claim['jobId'],'parent-claim',json.dumps(dict(claim=h.store.get(h.claim['jobId'],private=True),reservation=transfer))))
        driver.chmod(0o600);tokens=guard.DriverClaimTokens(driver,journal=h.journal,policy=policy,state_owner=h.owner)
        h.guard=guard.ProductionParentGuard(journal=h.journal,ledger=h.ledger,client=client,tokens=tokens,state_owner=h.owner)
        h.stage=NativeODMStage(h.root/'tree-prior.sqlite',h.runtime,journal=h.journal,ledger=h.ledger,engine=h.native,parent_guard=h.guard,state_owner=h.owner,input_resolver=h.inputs,runtime_probe=NativeRuntimeProbe(h.runtime,runner=h.docker))
        f.stage=f.open();f.begin();f.docker.container['State']=dict(Status='exited',Running=False,ExitCode=0)
        prep=json.loads(f.stage._row(h.claim['jobId'])['preparation']);self.output=Path(prep['output']);run=self.output/'run'
        run.mkdir(mode=0o700,exist_ok=True);run.chmod(0o700);objects=[]
        for name,body in FIXTURE['payloadsBase64'].items():
            # Current pinned bootstrap emits flat files only.
            if '/' in name:continue
            path=run/name;path.write_bytes(base64.b64decode(body));path.chmod(0o600);relative='run/'+name
            objects.append(dict(id='treedar-'+hashlib.sha256(relative.encode()).hexdigest()[:32],role=m.b.NATIVE_FILES[name][0],byteSize=path.stat().st_size,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),relativePath=relative))
        parent=f.intent['parentBinding'];obj=f.intent['stage']['inputObjects'][0]
        admission=dict(admissionVersion=p.TREEDAR_ADMISSION_VERSION,jobId=parent['jobId'],generation=1,environment='staging',objectId=obj['id'],objectSha256=obj['sha256'],byteSize=obj['byteSize'],sourceKind='lidar',provenanceReceiptId='original-source',genuineAcquisition=True,pointCount=10,groundPointCount=5,groundCoverage=1.0,crs='EPSG:32616',crsWktSha256='a'*64,pointCountAssurance='uncompressed_byte_layout_and_stream',verticalUnits='metre',verticalDatum='navd88',verticalDatumAssurance='authenticated_declaration_no_conversion',rasterGridCells=10)
        proof=dict(schemaVersion=1,job={k:parent[k] for k in ('jobId','generation','environment','manifestSha256','pipelineDigest')},engineVersion='0.0.1',admission=admission,objects=sorted(objects,key=lambda v:v['relativePath']))
        (self.output/'proof.json').write_bytes(p.canonical_json(proof));(self.output/'proof.json').chmod(0o600)
        f.stage.tick(h.claim['jobId']);self.originals={str(x):x.read_bytes() for x in self.output.rglob('*') if x.is_file()}
        limits=m.b.BundleLimits(8192,10000,64,8192,2048,100000)
        self.policy=m.PublicationPolicy(limits,control_budget=262144);self.publication=self.open();self.job=h.claim['jobId']

    def stop(self):
        if self.thread.is_alive():
            shutdown=threading.Thread(target=self.server.shutdown,daemon=True);shutdown.start();shutdown.join(5)
            self.thread.join(5)
            if shutdown.is_alive() or self.thread.is_alive():
                self.server.server_close()
                raise RuntimeError('synthetic TLS server did not stop')
        self.server.server_close()
    def open(self):return m.NativeBundlePublication(self.host.root/'publication.sqlite',self.policy,stage=self.fixture.stage)
    def begin(self):return self.publication.begin(self.fixture.intent)
    def assert_retained(self):
        for name,body in self.originals.items():self.assertEqual(Path(name).read_bytes(),body)
        self.assertEqual(self.host.reservation()['state'],'engine_started')
        with NodeGate(self.host.policy.engine_id) as gate:self.assertIsNone(gate.snapshot()['reservation']['completionDigest'])
        with self.assertRaises(Exception):self.host.journal.final_result(self.job)

    def test_spatial_receipt_requires_fresh_exact_issuer_and_retains_parent(self):
        import io,time
        from processing import reference_treedar_spatial_candidate as spatial
        from processing.reference_transfer import BrokerClient, TransferObject, ValidatedGrant
        from processing.reference_provenance import LidarProvenanceClient, ProvenanceUnavailable
        h=self.host;stage=self.fixture.stage;job=h.store.get(self.job,private=True)
        objects=tuple(TransferObject(item['id'],item['byteSize'],item['sha256']) for item in job['submission']['manifest']['objects'])
        grant=ValidatedGrant(self.job,'parent-reservation',1,'staging',time.time()+120,objects,'synthetic-private-token')
        wrapper=dict(schemaVersion=1,inputManifestSha256=job['manifestSha256'],pipelineDigest=job['pipelineDigest'],provenance=self.fixture.receipt)
        broker=BrokerClient('https://synthetic.invalid',ssl_context=ssl.create_default_context())
        class Reply(io.BytesIO):
            status=200
            def __init__(self,raw,url):super().__init__(raw);self.url=url;self.headers={'Content-Type':'application/json','Content-Length':str(len(raw))}
            def geturl(self):return self.url
        calls=[];revoked=[False]
        def opener(request,*,timeout):
            calls.append(request.full_url)
            if revoked[0]:
                from urllib.error import HTTPError
                raise HTTPError(request.full_url,403,'synthetic revoked',{},io.BytesIO())
            return Reply(p.canonical_json(wrapper),request.full_url)
        broker._open=opener
        issuer=LidarProvenanceClient(broker,state_path=h.root/'spatial-issuer.sqlite',policy=stage.policy.admission)
        stage.journal.verify_provenance=issuer.verify
        wkt='synthetic-test-wkt-not-a-real-crs'
        admission=dict(sourceKind='lidar',genuineAcquisition=True,crs='EPSG:32616',crsWkt=wkt,crsWktSha256=hashlib.sha256(wkt.encode()).hexdigest())
        # The LAS and issuer are synthetic and the scientific validator is mocked;
        # this test checks orchestration, exact re-reads and retention only.
        before=self.fixture.actions()[:]
        with patch.object(spatial,'QUALIFIED',True),patch.object(spatial,'validate_lidar',return_value=admission):
            receipt=spatial.LocalSpatialReceipt(h.root/'spatial.sqlite',stage=stage,issuer=issuer,enabled=True)
            value=receipt.qualify(self.fixture.intent,job,grant)
            self.assertEqual(value['transport'],'held');self.assertFalse(value['parentSettled'])
            self.assertEqual(value['record']['admission']['crsWkt'],wkt)
            self.assertEqual(len(calls),2);self.assertEqual(receipt.qualify(self.fixture.intent,job,grant),value)
            self.assertEqual(len(calls),4)
            self.assertEqual(self.fixture.actions(),before);self.assert_retained()
            with patch.object(spatial,'validate_lidar',return_value=dict(admission,crsWkt='changed')):
                with self.assertRaises(spatial.SpatialHeld):receipt.qualify(self.fixture.intent,job,grant)
            source=h.ledger.root/self.fixture.inputs.resolve(self.fixture.intent)['original-lidar']
            source.write_bytes(b'abd')
            try:
                with self.assertRaises(Exception):receipt.qualify(self.fixture.intent,job,grant)
            finally:source.write_bytes(b'abc')
            def revoke_during_validation(*args,**kwargs):revoked[0]=True;return admission
            with patch.object(spatial,'validate_lidar',side_effect=revoke_during_validation):
                with self.assertRaises(ProvenanceUnavailable):receipt.qualify(self.fixture.intent,job,grant)
            self.assertFalse(issuer.verify(self.fixture.receipt))
            issuer.broker._open=lambda *args,**kwargs:None
            with self.assertRaises(spatial.SpatialHeld):receipt.qualify(self.fixture.intent,job,grant)

    def test_actual_guard_full_publication_preserves_parent_and_native_sources(self):
        result=self.begin();self.assertEqual(result['phase'],'published');self.assertFalse(result['parentSettled']);self.assertEqual(result['receipt']['state'],'local_native_bundle_published')
        row=self.publication._copy(self.job);_,_,partial,final=self.publication._paths(row);self.assertFalse(partial.exists());self.assertTrue(final.exists());self.assertEqual(final.stat().st_nlink,1)
        self.assert_retained();self.assertEqual(self.fixture.actions(),['create','start'])

    def test_fresh_outer_preview_binds_actual_bundle_and_preserves_sources(self):
        from processing import reference_treedar_outer_candidate as outer
        published=self.begin();before=self.publication.path.read_bytes()
        preview=self.publication.preview_outer_manifest(self.job)
        self.assertEqual(preview['state'],'preview_not_custody')
        self.assertEqual(preview['transportRole'],'unassigned')
        self.assertEqual(preview['mixedCompletion'],'unqualified')
        self.assertEqual(preview['binding'],published['receipt']['binding'])
        self.assertEqual(preview['archive'],published['receipt']['archive'])
        row=self.publication._copy(self.job);_,_,_,final=self.publication._paths(row)
        with zipfile.ZipFile(final) as archive:
            inner=archive.read('treedar-bundle.json')
        self.assertEqual(preview['inner']['zipEntryByteSize'],len(inner))
        self.assertEqual(preview['inner']['zipEntrySha256'],hashlib.sha256(inner).hexdigest())
        self.assertEqual(preview['inner']['manifestSha256WithoutDigestField'],published['receipt']['manifestSha256'])
        self.assertEqual(preview['nativeFiles'],published['receipt']['nativeFiles'])
        self.assertEqual(preview['spatial']['definitionStatus'],'digest_only')
        self.assertIsNone(preview['spatial']['wkt'])
        self.assertEqual(preview['spatial']['sharedRasterGeometry']['files'],['dtm.tif','chm.tif','chm_raw.tif','crowns.tif'])
        with self.publication._operation():
            source,admission=self.publication._qualify(self.publication._intent(self.job),include_admission=True)
            receipt=self.publication._archive(self.publication._copy(self.job))
            config=self.fixture.stage.policy.config
            self.assertEqual(outer.validate_preview(preview,source,admission,receipt,config,self.policy.limits),preview)
            for mutate in (lambda value:value['binding']['sourceLidar'].update(objectId='other-lidar'),
                           lambda value:value['nativeFiles'][0].update(role='dem'),
                           lambda value:value['inner'].update(zipEntrySha256='0'*64),
                           lambda value:value['spatial'].update(definitionStatus='verified'),
                           lambda value:value.update(transportRole='odm_archive')):
                altered=copy.deepcopy(preview);mutate(altered)
                with self.assertRaises(Exception):outer.validate_preview(altered,source,admission,receipt,config,self.policy.limits)
        self.assertEqual(self.publication.path.read_bytes(),before)
        self.assertEqual(self.fixture.actions(),['create','start'])
        self.assert_retained()

    def test_outer_preview_holds_if_original_native_proof_changes(self):
        self.begin();proof=self.output/'proof.json';proof.write_bytes(proof.read_bytes()+b' ')
        with self.assertRaises(Exception):self.publication.preview_outer_manifest(self.job)
        self.assertEqual(self.fixture.actions(),['create','start'])
        self.assertEqual(self.host.reservation()['state'],'engine_started')

    def test_readback_is_sql_only_and_not_fresh_authorization(self):
        self.begin();before=self.publication.path.read_bytes()
        with patch.object(self.fixture.stage,'_docker',side_effect=AssertionError('no Docker')),patch.object(self.host.guard,'check',side_effect=AssertionError('no fresh claim')):
            self.assertEqual(self.publication.read_receipt(self.job)['verification'],'historical_sql_only')
        self.assertEqual(before,self.publication.path.read_bytes())

    def test_synthetic_admission_and_unknown_native_object_do_not_publish(self):
        # Seal matching native evidence after modifying proof; source semantics
        # still reject despite a self-consistent exit0 journal.
        proof_path=self.output/'proof.json';proof=json.loads(proof_path.read_bytes());proof['admission']['genuineAcquisition']=False;proof_path.write_bytes(p.canonical_json(proof))
        self.fixture.stage._phase(self.job,'processing');self.fixture.stage.tick(self.job)
        with self.assertRaises(Exception):self.begin()
        self.assertIsNone(self.publication.read_receipt(self.job));self.assertEqual(self.fixture.actions(),['create','start'])

    def test_lost_link_checkpoint_reconciles_exact_existing_inode(self):
        original=self.publication._phase
        def fail(row,phase,**values):
            if phase=='linked':raise OSError('synthetic lost checkpoint')
            return original(row,phase,**values)
        with patch.object(self.publication,'_phase',side_effect=fail),self.assertRaises(OSError):self.begin()
        row=self.publication._copy(self.job);_,_,partial,final=self.publication._paths(row);self.assertEqual(partial.stat().st_ino,final.stat().st_ino)
        self.publication=self.open();self.assertEqual(self.publication.reconcile(self.job)['phase'],'published');self.assert_retained()

    def test_lost_unlink_checkpoint_reconciles_final_without_copying(self):
        original=self.publication._phase
        def fail(row,phase,**values):
            if phase=='published':raise OSError('synthetic lost final checkpoint')
            return original(row,phase,**values)
        with patch.object(self.publication,'_phase',side_effect=fail),self.assertRaises(OSError):self.begin()
        self.publication=self.open();self.assertEqual(self.publication.reconcile(self.job)['copyNumber'],1);self.assert_retained()

    def test_complete_zip_without_receipt_reconciles_without_recopy(self):
        original=self.publication._phase
        def fail(row,phase,**values):
            if phase=='ready':raise OSError('lost ZIP receipt')
            return original(row,phase,**values)
        with patch.object(self.publication,'_phase',side_effect=fail),self.assertRaises(OSError):self.begin()
        self.publication=self.open();self.assertEqual(self.publication.reconcile(self.job)['copyNumber'],1);self.assert_retained()

    def test_unknown_artifact_and_config_pid_drift_hold(self):
        self.begin();row=self.publication._copy(self.job);_,policy,_,_=self.publication._paths(row)
        (Path(policy.output_root)/'unknown').write_bytes(b'x')
        with self.assertRaises(Exception):self.publication.reconcile(self.job)
        with patch.object(self.publication,'_pid',-1),self.assertRaises(Exception):self.publication.read_receipt(self.job)

    def test_owner_flock_refuses_parallel_publication(self):
        with self.publication._operation(),self.assertRaises(m.PublicationHeld):self.begin()
        with self.publication._operation(),self.assertRaisesRegex(m.PublicationHeld,'publication_busy'):self.open()
        self.begin();row=self.publication._copy(self.job);plan,policy,_,_=self.publication._paths(row)
        concrete=m._Guard(self.publication,self.job,row['number'],policy,plan['expected'])
        with self.assertRaisesRegex(m.PublicationHeld,'publication_writer_lease_required'):concrete.check({},mutation=False)

    def test_new_copy_never_replays_engine_and_keeps_incomplete_bytes(self):
        # Simulate an interrupted ZIP writer, not a successfully closed ZIP.
        def incomplete(writer,fd):os.write(fd,b'PK\x03\x04interrupted');raise OSError('copy interrupted')
        with patch.object(m.b.BundlePartialWriter,'write_to',incomplete),self.assertRaises(OSError):self.begin()
        row=self.publication._copy(self.job);_,_,partial,_=self.publication._paths(row);old=partial.read_bytes()
        self.assertEqual(self.open().reconcile(self.job)['copyNumber'],2)
        self.assertEqual(partial.read_bytes(),old);self.assert_retained();self.assertEqual(self.fixture.actions(),['create','start'])

    def test_repeated_incomplete_copy_recovery_stops_at_cap_and_retains_every_inode(self):
        def incomplete(writer,fd):os.write(fd,b'PK\x03\x04interrupted');raise OSError('copy interrupted')
        retained=[]
        with patch.object(m.b.BundlePartialWriter,'write_to',incomplete):
            for number in range(1,4):
                with self.assertRaises(OSError):
                    self.begin() if number==1 else self.open().reconcile(self.job)
                row=self.publication._copy(self.job);self.assertEqual(row['number'],number)
                _,_,partial,_=self.publication._paths(row);retained.append((partial,partial.read_bytes(),partial.stat().st_ino))
            with self.assertRaisesRegex(m.PublicationHeld,'copy_attempt_cap_reached'):self.open().reconcile(self.job)
        for path,body,inode in retained:self.assertEqual((path.read_bytes(),path.stat().st_ino),(body,inode))
        self.assert_retained();self.assertEqual(self.fixture.actions(),['create','start'])

    def test_insufficient_remaining_capacity_never_overwrites_or_releases_old_partial(self):
        def incomplete(writer,fd):os.write(fd,b'PK\x03\x04interrupted');raise OSError('copy interrupted')
        with patch.object(m.b.BundlePartialWriter,'write_to',incomplete),self.assertRaises(OSError):self.begin()
        row=self.publication._copy(self.job);_,_,partial,_=self.publication._paths(row);before=partial.read_bytes();reservation=self.host.reservation()
        self.host.ledger.record_allocated('parent-reservation',reservation['allocated']+reservation['remaining']-1)
        with self.assertRaisesRegex(m.PublicationHeld,'retained_copy_capacity_insufficient'):self.open().reconcile(self.job)
        self.assertEqual(partial.read_bytes(),before);self.assert_retained()

    def test_link_directory_fsync_loss_reconciles_both_retained_names(self):
        original=m._sync_directory
        def failed(path):
            if any(Path(path).glob('*.zip')):raise OSError('link directory fsync lost')
            return original(path)
        with patch.object(m,'_sync_directory',side_effect=failed),self.assertRaises(OSError):self.begin()
        row=self.publication._copy(self.job);self.assertEqual(row['phase'],'link_pending')
        self.assertEqual(self.open().reconcile(self.job)['phase'],'published');self.assert_retained()

    def test_complete_archive_body_corruption_holds_without_automatic_recopy(self):
        self.begin();row=self.publication._copy(self.job);_,_,_,final=self.publication._paths(row)
        with zipfile.ZipFile(final) as archive:info=archive.infolist()[0];offset=info.header_offset+30+len(info.filename.encode())+len(info.extra)
        with final.open('r+b') as stream:stream.seek(offset);first=stream.read(1);stream.seek(offset);stream.write(bytes([first[0]^1]))
        with self.assertRaisesRegex(m.PublicationHeld,'corrupt_publication_archive'):self.open().reconcile(self.job)
        self.assertEqual(self.publication._copy(self.job)['number'],1);self.assertTrue(final.exists());self.assert_retained()

    def test_fresh_native_failure_or_provenance_denial_blocks_after_historical_success(self):
        self.begin();self.fixture.provenance.valid=False
        self.assertEqual(self.publication.read_receipt(self.job)['phase'],'published')
        with self.assertRaises(Exception):self.publication.reconcile(self.job)
        self.fixture.provenance.valid=True;self.fixture.docker.container['State']['ExitCode']=1
        with self.assertRaises(Exception):self.publication.reconcile(self.job)
        self.assert_retained()

    def test_retained_original_image_and_odm_corruption_are_not_ignored(self):
        original=self.host.claim['submission']['manifest']['objects'][0]
        (self.host.ledger.root/self.host.input_paths[original['id']]).write_bytes(b'bad')
        with self.assertRaises(Exception):self.begin()
        self.assertIsNone(self.publication.read_receipt(self.job))

    def test_sql_receipt_corruption_and_missing_original_owner_hold(self):
        self.begin()
        with closing(sqlite3.connect(self.publication.path)) as db,db:
            row=db.execute('SELECT receipt FROM publication_copies').fetchone();value=json.loads(row[0]);value['binding']['generation']=2
            db.execute('UPDATE publication_copies SET receipt=?',(json.dumps(value),))
        with self.assertRaises(Exception):self.publication.read_receipt(self.job)
        with patch.object(self.host.owner,'check',side_effect=ValueError('missing daemon owner')),self.assertRaises(Exception):self.publication.read_receipt(self.job)

    def test_central_directory_preflight_rejects_count_and_size_before_zipfile_allocation(self):
        path=self.host.root/'bounded-eocd.bin';path.write_bytes(b'PK\x03\x04'+struct.pack('<4s4H2LH',b'PK\x05\x06',0,0,10000,10000,1000000,0,0))
        fd=os.open(path,os.O_RDONLY)
        try:
            real_pread=os.pread;counts=[]
            def bounded(fd,count,offset):counts.append(count);return real_pread(fd,count,offset)
            with patch.object(m.os,'pread',side_effect=bounded),patch.object(m.zipfile,'ZipFile',side_effect=AssertionError('no allocation')):
                with self.assertRaisesRegex(m.PublicationHeld,'bounded_publication_directory_required'):m.NativeBundlePublication._zip_census(fd,path.stat().st_size,['x'])
            self.assertEqual(counts,[22])
        finally:os.close(fd)

    def test_source_intent_checkpoint_without_copy_recovers_first_copy_without_double_charge(self):
        with patch.object(self.publication,'_new_copy',side_effect=OSError('lost original row reply')),self.assertRaises(OSError):self.begin()
        before=self.host.reservation()['allocated'];self.assertIsNone(self.publication._copy(self.job))
        result=self.open().reconcile(self.job);self.assertEqual(result['copyNumber'],1)
        row=self.publication._copy(self.job);_,policy,_,_=self.publication._paths(row);self.assertEqual(policy.allocation_base,before);self.assert_retained()

    def stop_preparation(self,where):
        original_phase=self.publication._phase;original_mkdir=Path.mkdir;original_open=os.open;original_write=m._write_new
        with ExitStack() as patches:
            if where in ('before_output','after_output','after_state'):
                def mkdir(path,*args,**kwargs):
                    selected=path.name.endswith('-output' if where!='after_state' else '-state')
                    if selected and where=='before_output':raise OSError('before directory create')
                    result=original_mkdir(path,*args,**kwargs)
                    if selected:raise OSError('after directory create')
                    return result
                patches.enter_context(patch.object(Path,'mkdir',mkdir))
            elif where=='after_partial':
                def opened(path,*args,**kwargs):
                    fd=original_open(path,*args,**kwargs)
                    if str(path).endswith('.zip.partial'):os.close(fd);raise OSError('partial creation reply lost')
                    return fd
                patches.enter_context(patch.object(m.os,'open',side_effect=opened))
            elif where=='after_owned':patches.enter_context(patch.object(m.b,'BundlePartialWriter',side_effect=OSError('owned checkpoint reply lost')))
            elif where=='partial_intent':
                def written(path,payload):
                    if str(path).endswith('.intent.json'):
                        fd=original_open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
                        try:os.write(fd,p.canonical_json(payload)[:17]);os.fsync(fd)
                        finally:os.close(fd)
                        raise OSError('intent write interrupted')
                    return original_write(path,payload)
                patches.enter_context(patch.object(m,'_write_new',side_effect=written))
            elif where=='before_streaming':
                def phase(row,phase,**values):
                    if phase=='streaming':raise OSError('intent checkpoint lost')
                    return original_phase(row,phase,**values)
                patches.enter_context(patch.object(self.publication,'_phase',side_effect=phase))
            else:raise AssertionError(where)
            with self.assertRaises(Exception):self.begin()
        return self.publication._copy(self.job)

    def test_each_early_creation_boundary_is_retained_and_retried_without_adoption(self):
        for where in ('before_output','after_output','after_state','after_partial','after_owned','partial_intent','before_streaming'):
            with self.subTest(where=where):
                case=self if where=='before_output' else PublicationTests()
                if case is not self:case.setUp()
                try:
                    row=case.stop_preparation(where);_,policy,partial,final=case.publication._paths(row)
                    old_files={str(path):(path.stat().st_ino,path.read_bytes()) for root in (Path(policy.output_root),Path(policy.state_root)) if root.exists() for path in root.iterdir()}
                    result=case.open().reconcile(case.job);case.assertEqual(result['phase'],'published');case.assertEqual(result['copyNumber'],2)
                    with case.publication._db(readonly=True) as db:abandoned=dict(db.execute('SELECT * FROM publication_copies WHERE job=? AND number=1',(case.job,)).fetchone())
                    case.assertEqual(abandoned['phase'],'abandoned_preparation');case.assertFalse(final.exists())
                    for path,(inode,body) in old_files.items():case.assertEqual((Path(path).stat().st_ino,Path(path).read_bytes()),(inode,body))
                    maximum=policy.allocation_base+policy.limits.maxArchiveBytes+policy.limits.maxManifestBytes+policy.limits.maxControlProofBytes
                    case.assertGreaterEqual(case.host.reservation()['allocated'],maximum);case.assert_retained();case.assertEqual(case.fixture.actions(),['create','start'])
                finally:
                    if case is not self:case.doCleanups()

    def test_abandoned_missing_root_receipt_is_immutable_on_later_recovery(self):
        row=self.stop_preparation('before_output');_,policy,_,_=self.publication._paths(row)
        self.assertEqual(self.open().reconcile(self.job)['copyNumber'],2)
        Path(policy.output_root).mkdir(mode=0o700)
        with self.assertRaisesRegex(m.PublicationHeld,'abandoned_preparation_census_changed'):self.open().reconcile(self.job)

    def test_nonempty_partial_or_unknown_preparation_path_holds_without_new_copy(self):
        row=self.stop_preparation('after_owned');_,policy,partial,_=self.publication._paths(row)
        partial.write_bytes(b'not authorized before streaming')
        with self.assertRaisesRegex(m.PublicationHeld,'early_preparation_bytes_exceeded'):self.open().reconcile(self.job)
        self.assertEqual(self.publication._copy(self.job)['number'],1)
        partial.write_bytes(b'');(Path(policy.state_root)/'foreign').write_bytes(b'x')
        with self.assertRaisesRegex(m.PublicationHeld,'unknown_preparation_artifact'):self.open().reconcile(self.job)

    def test_abandoned_partial_inode_replacement_and_mode_drift_hold(self):
        row=self.stop_preparation('after_partial');_,policy,partial,_=self.publication._paths(row)
        self.assertEqual(self.open().reconcile(self.job)['copyNumber'],2)
        fd=os.open(partial,os.O_RDONLY)
        try:
            partial.unlink();partial.write_bytes(b'');partial.chmod(0o600)
            with self.assertRaisesRegex(m.PublicationHeld,'abandoned_preparation_census_changed'):self.open().reconcile(self.job)
        finally:os.close(fd)
        partial.chmod(0o644)
        with self.assertRaises(Exception):self.open().reconcile(self.job)

    def test_preexisting_dedicated_directory_is_not_quarantined_as_our_creation(self):
        publication_id='bundle-'+self.fixture.intent['attemptId']+'-1'
        directory=self.host.ledger.root/'jobs'/'parent-reservation'/(publication_id+'-output');directory.mkdir(mode=0o700)
        with self.assertRaisesRegex(m.PublicationHeld,'copy_paths_must_initially_be_absent'):self.begin()
        self.assertIsNone(self.publication._copy(self.job));self.assertTrue(directory.exists())

    def test_opaque_intent_hashing_is_bounded_by_observed_size_plus_one(self):
        row=self.stop_preparation('partial_intent');_,policy,_,_=self.publication._paths(row)
        path=Path(policy.state_root)/(policy.publication_id+'.intent.json');initial=path.stat().st_size;real_read=os.read;counts=[]
        def growing(fd,count):
            # First read is the empty partial's one-byte overflow probe.
            observed=os.fstat(fd)
            if observed.st_ino==path.stat().st_ino:
                with path.open('ab') as stream:stream.write(b'x'*1000000)
                block=real_read(fd,count);counts.append((count,len(block)));return block
            return real_read(fd,count)
        with patch.object(m.os,'read',side_effect=growing),self.assertRaisesRegex(m.PublicationHeld,'preparation_artifact_changed'):
            self.publication._preparation_census(row)
        self.assertEqual(counts,[(initial+1,initial+1)])
