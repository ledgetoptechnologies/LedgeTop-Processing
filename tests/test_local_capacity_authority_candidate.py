"""Owned local-only lifecycle fixture; no production service or NAS."""
import json
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from unittest.mock import patch
from processing.crossfs_admission_candidate import CrossFsHeld
from processing.local_capacity_authority_candidate import LocalCapacityAuthority
from processing.scratch_policy import ScratchLedger
from ops.reference_worker_daemon import DaemonStateLock


@unittest.skipUnless(os.name=='posix','POSIX lock fixture required')
class LocalCapacityAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='lt-local-capacity-')).resolve()
        print('Retained synthetic root:',self.root,flush=True)
        self.journal=self.root/'journal.sqlite';self.journal.touch(mode=0o600)
        self.ledger=ScratchLedger(self.root/'ledger.sqlite',self.root,
                                  capacity_probe=lambda _:10000,safety_floor_bytes=100)
        info=self.journal.stat()
        root_info=self.ledger.root.stat(); ledger_info=self.ledger.path.stat()
        self.binding=self.root/'authority.json'
        value={'schemaVersion':1,'authorityId':'a'*32,'journal':str(self.journal),
            'journalIdentity':[info.st_dev,info.st_ino],
            'ledgers':{'local':{'root':str(self.ledger.root),'journal':str(self.ledger.path.absolute()),'floor':100,
                'identity':[root_info.st_dev,root_info.st_ino,ledger_info.st_dev,ledger_info.st_ino]}}}
        self.binding.write_text(json.dumps(value),encoding='utf-8');self.binding.chmod(0o600)
        for name,value in [('processing.local_capacity_authority_candidate.LOCAL_CAPACITY_AUTHORITY_QUALIFIED',True),
                           ('processing.crossfs_admission_candidate.CROSSFS_QUALIFIED',True),
                           ('processing.crossfs_admission_candidate.CROSSFS_CANONICAL_JOURNAL',self.journal)]:
            p=patch(name,value);p.start();self.addCleanup(p.stop)
        self.args=dict(attempt_id='attempt',job_id='job',generation=1,manifest_sha256='a'*64,
                       environment='staging',ttl_seconds=60,demands=[{'ledger':'local','bytes':300}])

    def authority(self):
        return LocalCapacityAuthority(self.root,self.binding,self.journal,{'local':self.ledger})

    def test_exclusive_owner_and_idempotent_acquire(self):
        with self.authority() as authority:
            with self.assertRaises(ValueError):
                with self.authority():self.fail('second owner admitted')
            self.assertEqual(authority.acquire(**self.args)['state'],'admitted')
            self.assertEqual(authority.acquire(**self.args)['state'],'admitted')
        with self.authority() as reopened:
            self.assertEqual(reopened.acquire(**self.args)['state'],'admitted')

    def test_changed_binding_refuses_and_releases_owned_lock(self):
        authority=self.authority()
        with self.assertRaises(CrossFsHeld):
            with authority:
                self.binding.write_text('{}',encoding='utf-8')
                authority.acquire(**self.args)
        # A fresh owner must actually acquire the same retained lock.
        with DaemonStateLock(self.root) as fresh:
            fresh.check(self.root)

    def test_replaced_ledger_journal_holds(self):
        with self.authority() as authority:
            os.rename(self.ledger.path,self.root/'retained-ledger.sqlite')
            self.ledger.path.touch(mode=0o600)
            try:
                with self.assertRaisesRegex(CrossFsHeld,'ledger identity changed'):
                    authority.acquire(**self.args)
            finally:
                # Restore the same owned inode to permit the owner lock to close.
                os.rename(self.ledger.path,self.root/'retained-replacement.sqlite')
                os.rename(self.root/'retained-ledger.sqlite',self.ledger.path)

    def test_other_process_cannot_own_same_state(self):
        child = ("import sys; from ops.reference_worker_daemon import DaemonStateLock\n"
                 "try:\n with DaemonStateLock(sys.argv[1]): pass\n"
                 "except ValueError: sys.exit(7)\n"
                 "sys.exit(2)\n")
        with self.authority():
            result=subprocess.run([sys.executable,'-c',child,str(self.root)],
                stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=8)
            self.assertEqual(result.returncode,7,'other process unexpectedly acquired owner lock')
        control=subprocess.run([sys.executable,'-c',child,str(self.root)],
            stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=8)
        self.assertEqual(control.returncode,2,'child could not acquire lock after parent released')

    def test_disabled_refuses_before_use(self):
        with patch('processing.local_capacity_authority_candidate.LOCAL_CAPACITY_AUTHORITY_QUALIFIED',False):
            with self.assertRaises(CrossFsHeld):self.authority()
