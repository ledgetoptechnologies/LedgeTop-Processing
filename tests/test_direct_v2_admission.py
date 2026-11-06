"""Unwired V2 direct admission, never a source-custody ACK."""
import tempfile
import unittest
from types import SimpleNamespace

from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.reference_store import PROFILE, ReferenceStore
from tests import test_reference_source_intake as fixtures


class DirectV2AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.queue=Queue(self.temp.name,reference_enabled=True);self.addCleanup(self.queue.close)
        self.queue.allowed_profiles.add(PROFILE);self.store=ReferenceStore(self.queue)
        self.fixture=fixtures.ReferenceSourceIntakeTests();self.fixture.setUp()
        self.fixture.authority_id=self.queue.authority_id
        self.validated=self.fixture.validate()
        self.options=dict(capability_fingerprint='a'*64,
            options_fingerprint=self.fixture.payload['pipeline']['odm']['optionsFingerprint'],
            allowed_odm_options=set(),source_spool=None,trusted_source_keys=self.fixture.trusted_keys,
            authority_id=self.queue.authority_id,ingress_mode='direct-v2',authenticated_principal='viewer-staging')

    def test_direct_mode_persists_full_signed_input_without_spool_or_claim(self):
        job,created=self.store.submit_source_intake(self.validated,**self.options)
        self.assertTrue(created);self.assertEqual(job['status'],'held')
        with self.queue._db() as db:
            row=db.execute('''SELECT s.reserved_bytes,s.source_state,s.derived_state,m.mode,j.held_reason
                FROM reference_source_intakes s JOIN reference_source_intake_modes m ON m.job_id=s.job_id
                JOIN jobs j ON j.id=s.job_id WHERE s.job_id=?''',(job['jobId'],)).fetchone()
            self.assertEqual(tuple(row),(0,'receiving','receiving','direct-v2','direct_admission_pending'))
        replay,created=self.store.submit_source_intake(self.validated,**self.options)
        self.assertFalse(created);self.assertEqual(replay['jobId'],job['jobId'])
        with self.assertRaises(Conflict):
            self.store.submit_source_intake(self.validated,**dict(self.options,authenticated_principal='other'))
        with self.assertRaises(Conflict):
            self.store.submit_source_intake(self.validated,**dict(self.options,ingress_mode='spool-v1',
                source_spool=SimpleNamespace(check_capacity=lambda *args,**kwargs:None)))

