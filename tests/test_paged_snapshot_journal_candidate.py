import base64
import copy
import json
import subprocess
import sys
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

from processing import reference_protocol as p
from processing.paged_snapshot_journal_candidate import DOMAIN, PROTOCOL, ServerSnapshotTrust, SnapshotHeld, initialize, inspect
from processing.reference_store import ReferenceStore
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization
from tests.test_paged_snapshot_signature_candidate import PagedSnapshotSignatureTests


class JournalTests(unittest.TestCase):
    statement_hash=PagedSnapshotSignatureTests.statement_hash
    def setUp(self):
        PagedSnapshotSignatureTests.setUp(self)
        self.statement.pop('sourceRevision');self.statement.pop('revocationEpoch')
        self.statement['protocol']=PROTOCOL
        with self.queue._db() as db:initialize(db)

    def sign_journal(self, statement=None):
        value=copy.deepcopy(statement or self.statement)
        value['signature']=base64.urlsafe_b64encode(self.private.sign(DOMAIN+p.canonical_json(value))).decode().rstrip('=')
        return p.canonical_json(value)

    def invoke(self,raw=None,now=1893457800000,principal='viewer-a',environment='staging'):
        trust=ServerSnapshotTrust(authority_id=self.queue.authority_id,key_entries=self.config,clock_provider=lambda:now)
        return inspect(self.store,self.env['submission']['submissionId'],raw or self.sign_journal(),principal=principal,environment=environment,trust=trust)

    def test_record_exact_replay_after_expiration_and_corrupt_collision(self):
        raw=self.sign_journal();first=self.invoke(raw);self.assertEqual(first['reason'],'signed_metadata_recorded_only')
        self.assertEqual(self.invoke(raw,now=1893460000000)['reason'],'snapshot_replay_status_only')
        changed=copy.deepcopy(self.statement);changed['sourceCount']=2
        with self.assertRaises(SnapshotHeld):self.invoke(self.sign_journal(changed))
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM paged_snapshot_journal_v1').fetchone()[0],1)

    def test_concurrent_same_and_changed_statements(self):
        raw=self.sign_journal();changed=copy.deepcopy(self.statement);changed['inputManifestSha256']='f'*64
        def run(value):
            try:return self.invoke(value)['reason']
            except SnapshotHeld:return 'conflict'
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(run,[raw,self.sign_journal(changed)]))
        self.assertCountEqual(results,['signed_metadata_recorded_only','conflict'])
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM paged_snapshot_journal_v1').fetchone()[0],1)

    def test_gcp_scope_and_noncanonical_bytes_hold(self):
        changed=copy.deepcopy(self.statement);changed['gcpEvidenceSha256']='a'*64
        with self.assertRaises(SnapshotHeld):self.invoke(self.sign_journal(changed))
        with self.assertRaises(SnapshotHeld):self.invoke(b' '+self.sign_journal())
        with self.assertRaises(SnapshotHeld):self.invoke(principal='other')
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM paged_snapshot_journal_v1').fetchone()[0],0)

    def test_changed_authority_and_partial_schema_hold(self):
        with self.queue._db() as db:
            db.execute("UPDATE coordinator_identity SET authority_id=? WHERE singleton=1",('00000000-0000-4000-8000-000000000000',))
        with self.assertRaises(SnapshotHeld):self.invoke()
        with self.queue._db() as db:
            db.execute('UPDATE coordinator_identity SET authority_id=? WHERE singleton=1',(self.queue.authority_id,))
            db.execute('DROP TABLE paged_snapshot_journal_v1')
            db.execute('CREATE TABLE paged_snapshot_journal_v1 (issuer_id TEXT)')
        with self.assertRaises(SnapshotHeld):self.invoke()

    def test_same_issuer_snapshot_cannot_be_reused_after_key_rotation(self):
        self.invoke();other=Ed25519PrivateKey.generate()
        pem=other.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        entry=dict(self.config[0],keyId='key-2',publicKeyPem=pem)
        value=copy.deepcopy(self.statement);value['keyId']='key-2'
        value['signature']=base64.urlsafe_b64encode(other.sign(DOMAIN+p.canonical_json(value))).decode().rstrip('=')
        trust=ServerSnapshotTrust(authority_id=self.queue.authority_id,key_entries=self.config+[entry],clock_provider=lambda:1893457800000)
        with self.assertRaises(SnapshotHeld):inspect(self.store,self.env['submission']['submissionId'],p.canonical_json(value),principal='viewer-a',environment='staging',trust=trust)

    def test_same_issuer_snapshot_cannot_be_reused_across_jobs(self):
        self.invoke();other=copy.deepcopy(self.env)
        other['submission']['submissionId']=str(uuid.uuid4());other['viewerTaskId']=str(uuid.uuid4());other['viewerAttemptId']=str(uuid.uuid4());other['activationId']=str(uuid.uuid4())
        job,_=self.store.prepare_viewer_envelope_jcs_v3(json.dumps(other).encode(),authenticated_principal='viewer-a',authenticated_environment='staging',trusted_policy=self.policy,expected_authority_id=self.queue.authority_id)
        found=self.store.find_viewer_envelope_v2(other['submission']['submissionId'],authenticated_principal='viewer-a',authenticated_environment='staging',expected_protocol=other['protocol'],expected_authority_id=self.queue.authority_id)
        statement=copy.deepcopy(self.statement);statement.update(jobId=job['jobId'],generation=job['generation'],submissionId=other['submission']['submissionId'],viewerTaskId=other['viewerTaskId'],viewerAttemptId=other['viewerAttemptId'],activationId=other['activationId'],preparedEnvelopeSha256=found['envelopeSha256'])
        trust=ServerSnapshotTrust(authority_id=self.queue.authority_id,key_entries=self.config,clock_provider=lambda:1893457800000)
        with self.assertRaises(SnapshotHeld):inspect(self.store,other['submission']['submissionId'],self.sign_journal(statement),principal='viewer-a',environment='staging',trust=trust)

    def test_reopen_database_exact_replay(self):
        raw=self.sign_journal();self.invoke(raw)
        self.store=ReferenceStore(self.queue,jcs_runtime_enabled=True)
        self.assertEqual(self.invoke(raw)['reason'],'snapshot_replay_status_only')

    def test_process_restart_recovers_committed_lost_reply(self):
        raw=self.sign_journal();self.invoke(raw)  # Discard successful response after commit.
        request=dict(root=self.temp.name,authority=self.queue.authority_id,entries=self.config,
                     submission=self.env['submission']['submissionId'],raw=raw.decode())
        self.queue.close()
        script='''import json,sys
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE
from processing.paged_snapshot_journal_candidate import inspect,ServerSnapshotTrust
a=json.load(sys.stdin);q=Queue(a['root']);q.allowed_profiles.add(PROFILE)
try:
 s=ReferenceStore(q,jcs_runtime_enabled=True)
 t=ServerSnapshotTrust(authority_id=a['authority'],key_entries=a['entries'],clock_provider=lambda:1893460000000)
 print(json.dumps(inspect(s,a['submission'],a['raw'].encode(),principal='viewer-a',environment='staging',trust=t)))
finally:q.close()
'''
        result=subprocess.run([sys.executable,'-c',script],input=json.dumps(request),capture_output=True,text=True,check=True,timeout=20)
        self.assertEqual(json.loads(result.stdout)['reason'],'snapshot_replay_status_only')
