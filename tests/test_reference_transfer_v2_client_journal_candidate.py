"""Owned synthetic client journal tests; no network or real authority."""
import base64
import copy
import json
import os
from pathlib import Path
import subprocess
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing import reference_transfer_v2_contract_candidate as contract
from processing import reference_transfer_v2_client_journal_candidate as journal

@unittest.skipUnless(os.name=='posix','owned POSIX journal required')
class V2ClientJournalCandidateTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.vector=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-transfer-v2-hash-vector.json').read_text())
        self.raw=self.vector['canonicalAdmission'].encode();self.value=json.loads(self.raw)
        self.secret=b'synthetic-client-proof-key-'*2
        self.patches=[patch.object(contract,'QUALIFIED',True),patch.object(journal,'QUALIFIED',True),
                      patch.object(journal,'EVIDENCE_QUALIFIED',True),patch.object(journal,'ABSENCE_QUALIFIED',True),
                      patch.object(journal,'ADOPTION_QUALIFIED',True)]
        for item in self.patches:item.start();self.addCleanup(item.stop)
        self.owner=DaemonStateLock(self.root);self.owner.__enter__();self.addCleanup(self.owner.__exit__,None,None,None)
        self.client=journal.V2ClientJournal(self.root/'v2.sqlite',self.owner,enabled=True)
    def record(self,status,revision=1,fenced=True,charged=0):
        binding=self.value['binding']
        return dict(requestId=self.value['requestId'],requestHash=self.vector['requestSha256'],deliveryId=self.value['deliveryId'],
            status=status,chargedBytes=charged,ledgerEpoch='synthetic-ledger-epoch',revision=revision,recoveryGeneration=0,fenced=fenced,
            authorityId=binding['authorityId'],authorityEpoch=binding['epoch'],claimLineageSha256=binding['claimLineageSha256'])
    def test_one_way_dispatch_and_conflicting_identity_hold(self):
        self.assertEqual(self.client.prepare(self.raw),'prepared')
        self.client.start_once(self.value['requestId'],self.value['deliveryId'])
        self.assertEqual(self.client.prepare(self.raw),'dispatch_started')
        with self.assertRaises(journal.JournalHeld):self.client.start_once(self.value['requestId'],self.value['deliveryId'])
        changed=copy.deepcopy(self.value);changed['deliveryId']=base64.urlsafe_b64encode(b'y'*32).decode().rstrip('=')
        with self.assertRaises(journal.JournalHeld):self.client.prepare(journal._encode(changed).encode())
    def test_changed_persisted_identity_cannot_start_dispatch(self):
        self.client.prepare(self.raw)
        request=self.value['requestId'];delivery=self.value['deliveryId']
        for table,column in (('v2_requests','request_hash'),('v2_requests','canonical'),('v2_deliveries','admission')):
            with self.subTest(column=column):
                with sqlite3.connect(self.client.path) as db:
                    previous=db.execute(f'SELECT {column} FROM {table}').fetchone()[0]
                    db.execute(f'UPDATE {table} SET {column}=?',('changed',))
                with self.assertRaisesRegex(journal.JournalHeld,'persisted dispatch identity changed'):
                    self.client.start_once(request,delivery)
                with sqlite3.connect(self.client.path) as db:
                    self.assertEqual(db.execute('SELECT state FROM v2_deliveries').fetchone()[0],'prepared')
                    db.execute(f'UPDATE {table} SET {column}=?',(previous,))
        self.client.start_once(request,delivery)
    def test_unknown_requires_newer_same_ledger_evidence_and_no_retry(self):
        self.client.prepare(self.raw);self.client.start_once(self.value['requestId'],self.value['deliveryId'])
        unknown=self.record('unknown');signature=journal._mac(self.secret,journal.DOMAIN,unknown)
        self.assertEqual(self.client.observe_synthetic(unknown,signature,secret=self.secret),'unknown')
        older=self.record('recorded',revision=1,charged=3)
        with self.assertRaises(journal.JournalHeld):self.client.observe_synthetic(older,journal._mac(self.secret,journal.DOMAIN,older),secret=self.secret)
        resolved=self.record('recorded',revision=2,charged=3)
        self.assertEqual(self.client.observe_synthetic(resolved,journal._mac(self.secret,journal.DOMAIN,resolved),secret=self.secret),'recorded')
        self.assertEqual(self.client.observe_synthetic(resolved,journal._mac(self.secret,journal.DOMAIN,resolved),secret=self.secret),'recorded')
        with self.assertRaises(journal.JournalHeld):self.client.start_once(self.value['requestId'],self.value['deliveryId'])
    def test_absence_requires_fence_and_exact_original_authority(self):
        self.client.prepare(self.raw);self.client.start_once(self.value['requestId'],self.value['deliveryId'])
        absent=self.record('absent',fenced=False)
        with self.assertRaises(journal.JournalHeld):self.client.observe_synthetic(absent,journal._mac(self.secret,journal.DOMAIN,absent),secret=self.secret)
        absent['fenced']=True;absent['authorityEpoch']='wrong'
        with self.assertRaises(journal.JournalHeld):self.client.observe_synthetic(absent,journal._mac(self.secret,journal.DOMAIN,absent),secret=self.secret)
        absent['authorityEpoch']=self.value['binding']['epoch']
        self.assertEqual(self.client.observe_synthetic(absent,journal._mac(self.secret,journal.DOMAIN,absent),secret=self.secret),'absent')
    def test_adoption_is_separate_monotonic_and_fences_original(self):
        self.client.prepare(self.raw);self.client.start_once(self.value['requestId'],self.value['deliveryId']);b=self.value['binding']
        adoption=dict(requestId=self.value['requestId'],requestHash=self.vector['requestSha256'],recoveryGeneration=1,
            recoveryActor='other-worker',authorityId=b['authorityId'],authorityEpoch='new-synthetic-epoch',originalEpoch=b['epoch'],
            originalClaimLineageSha256=b['claimLineageSha256'],priorFenced=True)
        self.client.adopt_synthetic(adoption,journal._mac(self.secret,journal.ADOPTION_DOMAIN,adoption),secret=self.secret)
        self.client.adopt_synthetic(adoption,journal._mac(self.secret,journal.ADOPTION_DOMAIN,adoption),secret=self.secret)
        old=self.record('recorded',charged=3)
        with self.assertRaises(journal.JournalHeld):self.client.observe_synthetic(old,journal._mac(self.secret,journal.DOMAIN,old),secret=self.secret)
        skipped=dict(adoption,recoveryGeneration=3)
        with self.assertRaises(journal.JournalHeld):self.client.adopt_synthetic(skipped,journal._mac(self.secret,journal.ADOPTION_DOMAIN,skipped),secret=self.secret)

    def test_adoption_before_dispatch_blocks_original_start(self):
        self.client.prepare(self.raw);b=self.value['binding']
        adoption=dict(requestId=self.value['requestId'],requestHash=self.vector['requestSha256'],recoveryGeneration=1,
            recoveryActor='other-worker',authorityId=b['authorityId'],authorityEpoch='new-synthetic-epoch',originalEpoch=b['epoch'],
            originalClaimLineageSha256=b['claimLineageSha256'],priorFenced=True)
        self.client.adopt_synthetic(adoption,journal._mac(self.secret,journal.ADOPTION_DOMAIN,adoption),secret=self.secret)
        with self.assertRaises(journal.JournalHeld):self.client.start_once(self.value['requestId'],self.value['deliveryId'])

@unittest.skipUnless(os.name=='posix','owned POSIX journal required')
class V2ClientJournalProcessCrashTests(unittest.TestCase):
    def test_committed_dispatch_survives_owned_process_death(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            raw=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-transfer-v2-hash-vector.json').read_text())['canonicalAdmission'].encode()
            script='''import os,sys,base64\nfrom pathlib import Path\nfrom ops.reference_worker_daemon import DaemonStateLock\nfrom processing import reference_transfer_v2_contract_candidate as c, reference_transfer_v2_client_journal_candidate as j\nc.QUALIFIED=True;j.QUALIFIED=True\nroot=Path(sys.argv[1]);raw=base64.b64decode(sys.argv[2]);value=__import__('json').loads(raw)\nwith DaemonStateLock(root) as owner:\n client=j.V2ClientJournal(root/'v2.sqlite',owner,enabled=True);client.prepare(raw);client.start_once(value['requestId'],value['deliveryId']);print('dispatch-committed',flush=True);os._exit(77)\n'''
            child=subprocess.Popen([sys.executable,'-c',script,str(root),base64.b64encode(raw).decode()],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                try:stdout,stderr=child.communicate(timeout=12)
                except subprocess.TimeoutExpired:child.kill();child.communicate(timeout=3);self.fail('owned synthetic child timed out')
                self.assertEqual(child.returncode,77,stderr.decode(errors='replace')[-1000:]);self.assertEqual(stdout,b'dispatch-committed\n')
            finally:
                if child.poll() is None:child.kill();child.wait(timeout=3)
            value=json.loads(raw)
            with patch.object(contract,'QUALIFIED',True),patch.object(journal,'QUALIFIED',True),DaemonStateLock(root) as owner:
                client=journal.V2ClientJournal(root/'v2.sqlite',owner,enabled=True)
                self.assertEqual(client.prepare(raw),'dispatch_started')
                with self.assertRaises(journal.JournalHeld):client.start_once(value['requestId'],value['deliveryId'])
