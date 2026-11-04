"""Isolated synthetic SQLite prototype; no live data, routes, or external services."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid

from processing import reference_protocol as p
from processing import viewer_copy_journal_candidate as j
from processing.viewer_copy_v2_candidate import CandidateUnavailable
from tests.test_viewer_copy_v2_candidate import ViewerCopyV2CandidateTests


class FixtureJournalTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='ltds-copy-journal-')
        self.path=Path(self.temp.name)/'journal.sqlite'
        self.owner_token=uuid.uuid4().hex
        try:
            with (self.path.parent/'.fixture-owner').open('x',encoding='ascii') as marker:marker.write(self.owner_token)
            self.f=ViewerCopyV2CandidateTests('test_valid_full_closure_and_public_api_disabled')
            self.f.setUp()
            self.db=j._open_fixture(self.path,self.owner_token)
        except BaseException:
            self.temp.cleanup()
            raise

    def tearDown(self):
        try:self.db.close()
        finally:self.temp.cleanup()

    def raw(self):return p.canonical_json({'acceptance':self.f.a,'viewerReceipt':self.f.r})
    def write(self,raw=None):
        return j._journal_fixture(self.db,self.raw() if raw is None else raw,expected=self.f.expected,
            result_manifest=self.f.manifest,trusted_keys=self.f.pins)

    def test_reopen_exact_replay_and_changed_raw_conflict(self):
        with self.assertRaises(CandidateUnavailable):j.journal_verified_copy()
        raw=self.raw();self.assertEqual(self.write(raw),'stored')
        self.db.close();self.db=j._open_fixture(self.path,self.owner_token)
        self.assertEqual(self.write(raw),'replay')
        with self.assertRaises(j.JournalConflict):self.write(b' '+raw)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM fixture_copies').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM fixture_ids').fetchone()[0],4)

    def test_unrelated_database_path_rejected(self):
        with self.assertRaises(ValueError):j._open_fixture(self.path.parent/'different.sqlite',self.owner_token)
        with self.assertRaises(ValueError):j._open_fixture(self.path,'0'*32)

    def test_cross_submission_receipt_namespace_conflict_is_atomic(self):
        self.assertEqual(self.write(),'stored')
        self.f.r['submissionId']='99999999-9999-4999-8999-999999999999'
        self.f.expected['submissionId']=self.f.r['submissionId'];self.f.rebind()
        with self.assertRaises(j.JournalConflict):self.write()
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM fixture_copies').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM fixture_ids').fetchone()[0],4)

    def test_same_submission_different_principal_conflicts(self):
        self.assertEqual(self.write(),'stored')
        self.f.r['viewerPrincipalId']='different-principal'
        self.f.expected['viewerPrincipalId']='different-principal'
        self.f.pins[self.f.r['keyId']]['viewerPrincipalId']='different-principal'
        self.f.rebind()
        with self.assertRaises(j.JournalConflict):self.write()
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM fixture_copies').fetchone()[0],1)

    def test_process_exit_before_commit_rolls_back_on_reopen(self):
        self.db.close()
        script="""import os,sys
from tests.test_viewer_copy_v2_candidate import ViewerCopyV2CandidateTests
from processing import reference_protocol as p
from processing import viewer_copy_journal_candidate as j
f=ViewerCopyV2CandidateTests('test_valid_full_closure_and_public_api_disabled');f.setUp()
db=j._open_fixture(sys.argv[1],sys.argv[2])
raw=p.canonical_json({'acceptance':f.a,'viewerReceipt':f.r})
j._journal_fixture(db,raw,expected=f.expected,result_manifest=f.manifest,trusted_keys=f.pins,before_commit=lambda:os._exit(31))
"""
        try:
            child=subprocess.run([sys.executable,'-c',script,str(self.path),self.owner_token],timeout=10,check=False)
            self.assertEqual(child.returncode,31)
        finally:self.db=j._open_fixture(self.path,self.owner_token)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM fixture_copies').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM fixture_ids').fetchone()[0],0)
        self.assertEqual(self.write(),'stored')

    def test_process_exit_after_commit_before_response_replays(self):
        self.db.close()
        script="""import os,sys
from tests.test_viewer_copy_v2_candidate import ViewerCopyV2CandidateTests
from processing import reference_protocol as p
from processing import viewer_copy_journal_candidate as j
f=ViewerCopyV2CandidateTests('test_valid_full_closure_and_public_api_disabled');f.setUp()
db=j._open_fixture(sys.argv[1],sys.argv[2])
raw=p.canonical_json({'acceptance':f.a,'viewerReceipt':f.r})
j._journal_fixture(db,raw,expected=f.expected,result_manifest=f.manifest,trusted_keys=f.pins)
os._exit(32)
"""
        try:
            child=subprocess.run([sys.executable,'-c',script,str(self.path),self.owner_token],
                timeout=10,check=False,capture_output=True)
            self.assertEqual(child.returncode,32);self.assertEqual(child.stdout,b'')
        finally:self.db=j._open_fixture(self.path,self.owner_token)
        self.assertEqual(self.write(),'replay')
        with self.assertRaises(j.JournalConflict):self.write(b' '+self.raw())
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM fixture_copies').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM fixture_ids').fetchone()[0],4)

    def test_concurrent_identical_writers_have_one_record(self):
        self.db.close()
        script="""import sys,time
from pathlib import Path
from tests.test_viewer_copy_v2_candidate import ViewerCopyV2CandidateTests
from processing import reference_protocol as p
from processing import viewer_copy_journal_candidate as j
f=ViewerCopyV2CandidateTests('test_valid_full_closure_and_public_api_disabled');f.setUp()
db=j._open_fixture(sys.argv[1],sys.argv[2])
Path(sys.argv[3]).touch()
deadline=time.monotonic()+5
while not Path(sys.argv[4]).exists():
 if time.monotonic()>deadline:raise TimeoutError('barrier')
 time.sleep(.01)
raw=p.canonical_json({'acceptance':f.a,'viewerReceipt':f.r})
print(j._journal_fixture(db,raw,expected=f.expected,result_manifest=f.manifest,trusted_keys=f.pins),flush=True)
db.close()
"""
        children=[]
        try:
            for index in range(2):
                children.append(subprocess.Popen([sys.executable,'-c',script,str(self.path),self.owner_token,
                    str(self.path.parent/('ready'+str(index))),str(self.path.parent/'go')],stdout=subprocess.PIPE,stderr=subprocess.PIPE))
            import time
            deadline=time.monotonic()+5
            while not all((self.path.parent/('ready'+str(i))).exists() for i in range(2)):
                if time.monotonic()>deadline:raise TimeoutError('writers not ready')
                time.sleep(.01)
            (self.path.parent/'go').touch()
            results=[child.communicate(timeout=10) for child in children]
            self.assertEqual([child.returncode for child in children],[0,0],str(results))
            self.assertEqual(sorted(out.decode().strip() for out,err in results),['replay','stored'])
        finally:
            for child in children:
                if child.poll() is None:child.kill()
                child.communicate(timeout=5)
            self.db=j._open_fixture(self.path,self.owner_token)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM fixture_copies').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM fixture_ids').fetchone()[0],4)

