"""Synthetic coordinator DB only; tests patch hard gates, never a live route."""
import copy
import json
import os
import subprocess
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch
from processing.prototype import Queue
from processing.reference_store import ReferenceStore, PROFILE, JCSRuntimeUnavailable
from processing.reference_prepared_api_candidate import PreparedSubmissionAPI
from processing.prepared_v3_copy_v2_candidate import PreparedCopyV2Candidate, SCHEMA, migrate, _fixture_quota
from processing.maintenance import Conflict
from processing.prepared_envelope_jcs_candidate import PROTOCOL
from processing import reference_protocol as p
from tests.test_viewer_service_identity_candidate import AUTH, pem


class CandidateCoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.queue=Queue(self.temp.name);self.queue.allowed_profiles.add(PROFILE);self.addCleanup(self.queue.close)
        self.store=ReferenceStore(self.queue,jcs_runtime_enabled=True)
        f=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        legacy=dict(capabilityFingerprint=f['capabilities']['capabilityFingerprint'],optionsFingerprint=f['capabilities']['engines']['odm']['optionsFingerprint'],allowedOdmOptions=['resolution'],availableEngines=['odm'])
        policy=dict(capabilityFingerprint=legacy['capabilityFingerprint'],optionsFingerprint=legacy['optionsFingerprint'],availableEngines=['odm'],optionDefinitions={'resolution':{'type':'float','domain':{'min':-100,'max':100}}})
        self.token='synthetic-owner-token-00001'
        self.api=PreparedSubmissionAPI(self.store,credentials=[dict(token=self.token,principalId='owner-a',environment='production')],legacy_bearer_tokens=[],policies={'production':legacy,'staging':legacy},jcs_policies={'production':policy,'staging':policy},expected_authority_id=self.queue.authority_id)
        config=[dict(owner='owner-a',environment='production',authority=self.queue.authority_id,service='viewer-service-a',keys=[dict(keyId='key-a',issuerId='issuer-a',publicKeyPem=pem(1),revoked=False)])]
        self.candidate=PreparedCopyV2Candidate(self.api,config)
        self.envelope=dict(schemaVersion=3,protocol=PROTOCOL,submission=copy.deepcopy(f['submission']),viewerTaskId=str(uuid.uuid4()),viewerAttemptId=str(uuid.uuid4()),activationId=str(uuid.uuid4()))
        self.headers={'Authorization':'Bearer '+self.token,'X-LTDS-Expected-Coordinator-Authority':self.queue.authority_id}
        self.fixture=f

    def test_hard_gate_and_migration_stay_closed_before_db_schema_change(self):
        with self.assertRaises(JCSRuntimeUnavailable):migrate(self.queue)
        path='/ltds/prepared/v3/submissions/'+self.envelope['submission']['submissionId']+'/accepted-copies-v2'
        self.assertEqual(self.candidate.handle('POST',path,self.headers,b'{}',protected_transport=True).status,503)
        self.assertEqual(self.candidate.handle('POST',path,{**self.headers,'Authorization':'Bearer wrong'},b'{}',protected_transport=True).status,401)
        with self.queue._db() as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='fixture_v3_copy_v2'").fetchone())

    def test_gate_patched_invalid_owner_does_not_create_rows(self):
        # No gate is patched in production; fixture schema is created only in owned temp SQLite.
        with self.queue._db() as db:db.execute(SCHEMA)
        path='/ltds/prepared/v3/submissions/'+self.envelope['submission']['submissionId']+'/accepted-copies-v2'
        raw=p.canonical_json({'acceptance':{},'viewerReceipt':{'keyId':'key-a','issuerId':'issuer-a'}})
        with patch('processing.prepared_v3_copy_v2_candidate.migration_ready',return_value=True),patch.object(PreparedCopyV2Candidate,'integration_ready',return_value=True):
            self.assertEqual(self.candidate.handle('POST',path,self.headers,raw,protected_transport=True).status,404)
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM fixture_v3_copy_v2').fetchone()[0],0)

    def test_prior_copy_rows_count_against_quota_inside_transaction(self):
        with self.queue._db() as db:
            db.execute(SCHEMA)
            db.execute('INSERT INTO fixture_v3_copy_v2 VALUES(?,?,?,?,?,?,?)',('old',b'x'*1024,'a'*64,'r1','r2','r3','r4'))
        self.store.max_metadata_bytes=900
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            # Existing legacy accounting is unaware of the fixture table.
            self.store._quota(db,3)
            with self.assertRaises(Conflict):
                _fixture_quota(self.store,db,body=b'new',job_id='new',digest='b'*64,ids=['a','b','c','d'])
            self.assertEqual(db.execute('SELECT COUNT(*) FROM fixture_v3_copy_v2').fetchone()[0],1)
        self.store.max_metadata_bytes=10000
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            _fixture_quota(self.store,db,body=b'new',job_id='new',digest='b'*64,ids=['a','b','c','d'])

    def test_legacy_namespace_check_does_not_see_fixture_receipt_ids(self):
        # Demonstrates the hard gate is essential, not a safety guarantee.
        with self.queue._db() as db:
            db.execute(SCHEMA)
            db.execute('INSERT INTO fixture_v3_copy_v2 VALUES(?,?,?,?,?,?,?)',('old',b'x','a'*64,'r1','r2','r3','r4'))
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self.store._check_copy_identity_namespaces(db,{'r1'})
            self.assertEqual(db.execute('SELECT receipt_id FROM fixture_v3_copy_v2').fetchone()[0],'r1')

    def test_synthetic_gates_actual_viewer_signer_and_coordinator_transaction(self):
        source=os.environ.get('VIEWER_SOURCE')
        if not source:self.skipTest('detached exact Viewer signer required')
        revision=subprocess.run(['git','rev-parse','HEAD'],cwd=source,capture_output=True,text=True,timeout=10,check=True).stdout.strip()
        self.assertEqual(revision,'ad0bf46611de44545825db9147e90b68195902ab')
        created=self.api.handle('POST','/ltds/prepared/v3/submissions',self.headers,p.canonical_json(self.envelope),protected_transport=True)
        self.assertEqual(created.status,201);job=created.body
        found=self.store.find_viewer_envelope_v2(self.envelope['submission']['submissionId'],authenticated_principal='owner-a',authenticated_environment='production',expected_protocol=PROTOCOL,expected_authority_id=self.queue.authority_id)
        activation={k:self.envelope[k] for k in ('activationId','viewerTaskId','viewerAttemptId')};activation['envelopeSha256']=found['envelopeSha256']
        path='/ltds/prepared/v3/submissions/'+self.envelope['submission']['submissionId']
        with patch.object(ReferenceStore,'_v3_source_contract_ready',return_value=True):
            self.assertEqual(self.api.handle('POST',path+'/activation',self.headers,p.canonical_json(activation),protected_transport=True).status,200)
        result=copy.deepcopy(self.fixture['resultManifest']);result.update(jobId=job['jobId'],generation=job['generation'],environment=job['environment'],pipelineDigest=job['pipelineDigest'])
        result['resultManifestSha256']=p.digest({k:v for k,v in result.items() if k!='resultManifestSha256'})
        expected=dict(coordinatorAuthorityId=self.queue.authority_id,viewerPrincipalId='viewer-service-a',submissionId=self.envelope['submission']['submissionId'],activationId=activation['activationId'],viewerTaskId=activation['viewerTaskId'],viewerAttemptId=activation['viewerAttemptId'],preparedEnvelopeSha256=activation['envelopeSha256'],jobId=job['jobId'],generation=job['generation'],environment=job['environment'],inputManifestSha256=job['manifestSha256'],pipelineDigest=job['pipelineDigest'],resultManifestSha256=result['resultManifestSha256'])
        acceptance=copy.deepcopy(self.fixture['acceptance']);acceptance.update(jobId=job['jobId'],generation=job['generation'],environment=job['environment'],inputManifestSha256=job['manifestSha256'],pipelineDigest=job['pipelineDigest'],resultManifestSha256=result['resultManifestSha256'],acceptedObjectIds=[o['id'] for o in result['objects']])
        script="""const crypto=require('node:crypto');const root=process.argv[1];const p=require(root+'/apps/viewer/server/referenceProtocol');const c=require(root+'/apps/viewer/server/referenceReceiptV2Candidate');let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{const x=JSON.parse(s);const key=crypto.createPrivateKey({key:Buffer.concat([Buffer.from('302e020100300506032b657004220420','hex'),Buffer.alloc(32,1)]),format:'der',type:'pkcs8'}).export({type:'pkcs8',format:'pem'}).toString();const sign=c.createViewerReceiptV2SignerCandidate({issuerId:'issuer-a',keyId:'key-a',privateKeyPem:key});const viewerReceipt=sign(x);process.stdout.write(p.canonicalBytes({acceptance:x.acceptance,viewerReceipt}));});"""
        signed=subprocess.run(['node','-e',script,source],input=json.dumps(dict(expected=expected,acceptance=acceptance,resultManifest=result)),capture_output=True,text=True,timeout=10,check=True).stdout.encode()
        with self.queue._db() as db:
            db.execute(SCHEMA);db.execute("UPDATE jobs SET status='succeeded' WHERE id=?",(job['jobId'],))
            db.execute("UPDATE reference_jobs SET phase='awaiting_acceptance',result_json=? WHERE job_id=?",(p.canonical_json(result).decode(),job['jobId']))
        class Gate:
            def check_in_transaction(self, db):
                if not db.in_transaction:raise ValueError('transaction required')
        self.store._receipt_namespace_gate=Gate()
        # Synthetic gates do not establish any live migration, custody or writer boundary.
        with patch('processing.prepared_v3_copy_v2_candidate.migration_ready',return_value=True),patch.object(PreparedCopyV2Candidate,'integration_ready',return_value=True),patch.object(ReferenceStore,'_v3_result_contract_ready',return_value=True),patch.object(ReferenceStore,'_receipt_namespace_writer_boundary_ready',return_value=True):
            first=self.candidate.handle('POST',path+'/accepted-copies-v2',self.headers,signed,protected_transport=True)
            self.assertEqual((first.status,first.body['created']),(202,True))
            replay=self.candidate.handle('POST',path+'/accepted-copies-v2',self.headers,signed,protected_transport=True)
            self.assertEqual((replay.status,replay.body['created']),(202,False))
            self.assertEqual(self.candidate.handle('POST',path+'/accepted-copies-v2',self.headers,b' '+signed,protected_transport=True).status,409)
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT raw FROM fixture_v3_copy_v2').fetchone()[0],signed)
