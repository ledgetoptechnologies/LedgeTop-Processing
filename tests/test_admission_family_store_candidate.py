import copy
import json
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

from processing import reference_protocol as protocol
from processing.admission_family_candidate import FamilyHeld
from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.reference_prepared_api_candidate import PreparedSubmissionAPI
from processing.reference_store import PROFILE, ReferenceStore


class FamilyStoreIntegration(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.queue=Queue(self.temp.name);self.queue.allowed_profiles.add(PROFILE);self.addCleanup(self.queue.close)
        # Test-only bootstrap, deliberately not automatic in ReferenceStore.
        ReferenceStore(self.queue,jcs_runtime_enabled=True)
        from processing.admission_family_candidate import initialize_empty
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE'); initialize_empty(db); db.commit()
        self.store=ReferenceStore(self.queue,jcs_runtime_enabled=True,admission_family_candidate=True)
        self.fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        self.raw=copy.deepcopy(self.fixture['submission']);self.raw['submissionId']=str(uuid.uuid4())
        self.checked=protocol.validate_submission(self.raw,environment='production',capability_fingerprint=self.raw['capabilityFingerprint'])
        caps=self.fixture['capabilities']
        legacy=dict(capabilityFingerprint=caps['capabilityFingerprint'],optionsFingerprint=caps['engines']['odm']['optionsFingerprint'],allowedOdmOptions=['resolution'],availableEngines=['odm'])
        policy=dict(capabilityFingerprint=legacy['capabilityFingerprint'],optionsFingerprint=legacy['optionsFingerprint'],availableEngines=['odm'],optionDefinitions={'resolution':{'type':'float','domain':{'min':-100,'max':100}}})
        self.token='synthetic-prepared-viewer-token-001'
        self.api=PreparedSubmissionAPI(self.store,credentials=[dict(token=self.token,principalId='viewer-a',environment='production')],legacy_bearer_tokens=[],policies={'production':legacy,'staging':legacy},jcs_policies={'production':policy,'staging':policy},expected_authority_id=self.queue.authority_id)

    def post(self,envelope,token=None):
        return self.api.handle('POST','/ltds/prepared/v3/submissions',{'Authorization':'Bearer '+(token or self.token),'X-LTDS-Expected-Coordinator-Authority':self.queue.authority_id},json.dumps(envelope).encode(),protected_transport=True)

    def envelope(self):
        return dict(schemaVersion=3,protocol='ltds-viewer-prepared-envelope-jcs-v3',submission=self.raw,viewerTaskId=str(uuid.uuid4()),viewerAttemptId=str(uuid.uuid4()),activationId=str(uuid.uuid4()))

    def test_v1_atomic_replay_and_missing_marker_fail_closed(self):
        first,created=self.store.submit(self.checked);self.assertTrue(created)
        replay,created=self.store.submit(self.checked);self.assertFalse(created);self.assertEqual(first['jobId'],replay['jobId'])
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');db.execute('DELETE FROM reference_admission_family_candidate WHERE job_id=?',(first['jobId'],));db.commit()
        with self.assertRaises(Conflict):self.store.submit(self.checked)
        with self.assertRaises(Conflict):self.store.get(first['jobId'])
        with self.assertRaises(Conflict):ReferenceStore(self.queue,admission_family_candidate=True,jcs_runtime_enabled=True)

    def test_prepared_authenticated_exact_replay_and_missing_owner_hold(self):
        envelope=self.envelope()
        self.assertEqual(self.post(envelope,'synthetic-wrong-viewer-token-001').status,401)
        initial=self.post(envelope);self.assertEqual(initial.status,201,initial.body)
        repeat=self.post(envelope);self.assertEqual(repeat.status,200);self.assertEqual(initial.body['jobId'],repeat.body['jobId'])
        changed=copy.deepcopy(envelope);changed['viewerAttemptId']=str(uuid.uuid4())
        self.assertEqual(self.post(changed).status,409)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');db.execute('DELETE FROM reference_viewer_submission_owners WHERE job_id=?',(initial.body['jobId'],));db.commit()
        with self.assertRaises(Conflict):self.store.get(initial.body['jobId'])
        self.assertNotEqual(self.post(envelope).status,200)

    def test_marker_failure_rolls_back_whole_actual_submit(self):
        with patch('processing.admission_family_candidate._snapshot',side_effect=FamilyHeld('synthetic')):
            with self.assertRaises(Conflict):self.store.submit(self.checked)
        with self.queue._db() as db:
            for table in ('jobs','reference_jobs','reference_admission_family_candidate'):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0],0)

    def test_prepared_full_evidence_deletion_and_family_spoof_hold(self):
        envelope=self.envelope(); initial=self.post(envelope)
        self.assertEqual(initial.status,201,initial.body);job=initial.body['jobId']
        # Direct SQL tampering simulates corruption outside the qualified writer
        # boundary. It is detection coverage, not protection from a DB admin.
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            for table in ('reference_viewer_submission_owners','reference_viewer_envelopes_v2','reference_viewer_v3_policy'):
                db.execute('DELETE FROM '+table+' WHERE job_id=?',(job,))
            db.commit()
        with self.assertRaises(Conflict):self.store.get(job)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            import sqlite3
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE reference_admission_family_candidate SET family='bogus' WHERE job_id=?",(job,))
            db.rollback()

    def test_marker_failure_rolls_back_prepared_envelope_and_owner(self):
        with patch('processing.admission_family_candidate._snapshot',side_effect=FamilyHeld('synthetic')):
            response=self.post(self.envelope())
            self.assertNotEqual(response.status,201)
        with self.queue._db() as db:
            for table in ('jobs','reference_jobs','reference_viewer_submission_owners','reference_viewer_envelopes_v2','reference_viewer_v3_policy','reference_admission_family_candidate'):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0],0)

    def test_readonly_preflight_rejects_orphan_marker_without_chmod(self):
        orphan=str(uuid.uuid4())
        with self.queue._db() as db:
            db.execute('PRAGMA foreign_keys=OFF')
            db.execute('BEGIN IMMEDIATE')
            db.execute("INSERT INTO reference_admission_family_candidate VALUES(?,?,?,?,?,?,?,?,?)",
                (orphan,self.queue.authority_id,1,'production','a'*64,'legacy-v1',None,None,None))
            db.commit()
        import os
        path=Path(self.queue.path);before=(path.stat().st_mode,path.stat().st_mtime_ns,path.read_bytes())
        with self.assertRaisesRegex(Conflict,'orphan'):
            ReferenceStore(self.queue,jcs_runtime_enabled=True,admission_family_candidate=True)
        after=(path.stat().st_mode,path.stat().st_mtime_ns,path.read_bytes())
        self.assertEqual(before,after)

    def test_corrupt_prepared_envelope_or_policy_is_detected(self):
        response=self.post(self.envelope());self.assertEqual(response.status,201,response.body)
        job=response.body['jobId']
        for table,column,value in (('reference_viewer_envelopes_v2','envelope_json','{}'),
                                   ('reference_viewer_v3_policy','policy_json','{}')):
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE')
                db.execute('UPDATE '+table+' SET '+column+'=? WHERE job_id=?',(value,job))
                with self.assertRaises(Conflict):self.store._row(db,job)
                db.rollback()

    def test_corrupt_activation_is_detected(self):
        envelope=self.envelope();response=self.post(envelope)
        self.assertEqual(response.status,201,response.body);job=response.body['jobId']
        found=self.store.find_viewer_envelope_v2(self.raw['submissionId'],authenticated_principal='viewer-a',
            authenticated_environment='production',expected_protocol=envelope['protocol'])
        body=dict(activationId=envelope['activationId'],viewerTaskId=envelope['viewerTaskId'],
            viewerAttemptId=envelope['viewerAttemptId'],envelopeSha256=found['envelopeSha256'])
        with patch.object(ReferenceStore,'_v3_source_contract_ready',return_value=True), \
             patch.object(ReferenceStore,'_activation_bound_grant_ready',return_value=True):
            r=self.api.handle('POST','/ltds/prepared/v3/submissions/'+self.raw['submissionId']+'/activation',
                {'Authorization':'Bearer '+self.token,'X-LTDS-Expected-Coordinator-Authority':self.queue.authority_id},
                json.dumps(body).encode(),protected_transport=True)
            self.assertEqual(r.status,200,r.body)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');db.execute("UPDATE reference_viewer_activation SET binding_json='{}' WHERE job_id=?",(job,))
            with self.assertRaises(Conflict):self.store._row(db,job)
            db.rollback()

    def test_candidate_cannot_bootstrap_itself_from_empty_database(self):
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');db.execute('DROP TABLE reference_admission_family_candidate');db.commit()
        path=Path(self.queue.path);before=(path.stat().st_mode,path.stat().st_mtime_ns,path.read_bytes())
        with self.assertRaisesRegex(Conflict,'bootstrap evidence'):
            ReferenceStore(self.queue,jcs_runtime_enabled=True,admission_family_candidate=True)
        self.assertEqual(before,(path.stat().st_mode,path.stat().st_mtime_ns,path.read_bytes()))

    def test_candidate_disabled_refuses_candidate_state(self):
        with self.assertRaises(Conflict):ReferenceStore(self.queue,jcs_runtime_enabled=True)


if __name__=='__main__': unittest.main()
