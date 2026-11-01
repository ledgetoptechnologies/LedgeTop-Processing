"""Owned synthetic offline v2 delivery tests; no HTTP or production data."""
import base64
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing import reference_transfer_v2_contract_candidate as contract
from processing import reference_transfer_v2_client_journal_candidate as journal
from processing import reference_transfer_v2_offline_response_candidate as offline

@unittest.skipUnless(os.name=='posix','owned POSIX fixture required')
class OfflineV2DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.v=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-transfer-v2-hash-vector.json').read_text())
        self.raw=self.v['canonicalAdmission'].encode();self.value=json.loads(self.raw)
        self.owner=DaemonStateLock(self.root);self.owner.__enter__();self.addCleanup(self.owner.__exit__,None,None,None)
        for module in (contract,journal):
            item=patch.object(module,'QUALIFIED',True);item.start();self.addCleanup(item.stop)
        item=patch.object(offline,'OFFLINE_SYNTHETIC_QUALIFIED',True);item.start();self.addCleanup(item.stop)
        self.client=journal.V2ClientJournal(self.root/'v2.sqlite',self.owner,enabled=True)
        self.client.prepare(self.raw);self.client.start_once(self.value['requestId'],self.value['deliveryId'])
        self.headers=[('LT-Reference-Version','2'),('LT-Request-SHA256',self.v['requestSha256']),
            ('LT-Delivery-Id',self.value['deliveryId']),('LT-Delivery-Admitted','true'),('Content-Length','3'),('Cache-Control','no-store')]
    def receive(self,chunks,headers=None):
        return offline.receive_offline(self.client,request_id=self.value['requestId'],delivery_id=self.value['deliveryId'],
            status=200,headers=self.headers if headers is None else headers,chunks=chunks)
    def test_exact_response_stages_verified_bytes_once(self):
        target=self.receive([b'a',b'bc']);self.assertEqual(target.read_bytes(),b'abc')
        with self.client._tx() as db:self.assertEqual(db.execute('SELECT state FROM v2_local_streams').fetchone()[0],'verified')
        with self.assertRaises(journal.JournalHeld):self.receive([b'abc'])
        self.assertEqual(target.read_bytes(),b'abc')
    def test_invalid_headers_are_rejected_before_stream_intent(self):
        def replace(name,value):
            return [(key,value if key.lower()==name.lower() else val) for key,val in self.headers]
        for headers in (self.headers+[('content-length','3')],self.headers+[('Content-Encoding','gzip')],
                        self.headers+[('Transfer-Encoding','chunked')],self.headers+[('Content-Range','bytes 0-2/3')],
                        [item for item in self.headers if item[0]!='Content-Length'],replace('Content-Length','03'),
                        replace('Content-Length','4'),replace('LT-Reference-Version','1'),
                        replace('LT-Request-SHA256','0'*64),replace('LT-Delivery-Id','other'),
                        replace('LT-Delivery-Admitted','false')):
            with self.subTest(headers=len(headers)),self.assertRaises(journal.JournalHeld):self.receive([b'abc'],headers)
        for status in (206,301,302,307,308,500):
            with self.subTest(status=status),self.assertRaises(journal.JournalHeld):
                offline.receive_offline(self.client,request_id=self.value['requestId'],delivery_id=self.value['deliveryId'],
                    status=status,headers=self.headers,chunks=[b'abc'])
        with self.client._tx() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM v2_local_streams').fetchone()[0],0)
    def test_partial_body_retained_and_never_replayed(self):
        with self.assertRaises(journal.JournalHeld):self.receive([b'a'])
        target=self.root/('v2-input-'+self.value['deliveryId']+'.part');self.assertEqual(target.read_bytes(),b'a')
        with self.client._tx() as db:self.assertEqual(db.execute('SELECT state FROM v2_local_streams').fetchone()[0],'started')
        with self.assertRaises(journal.JournalHeld):self.receive([b'abc'])
        self.assertEqual(target.read_bytes(),b'a')
    def test_trailing_body_bytes_hold_and_preserve_no_verified_record(self):
        with self.assertRaises(journal.JournalHeld):self.receive([b'abc',b'x'])
        target=self.root/('v2-input-'+self.value['deliveryId']+'.part');self.assertEqual(target.read_bytes(),b'abc')
        with self.client._tx() as db:self.assertEqual(db.execute('SELECT state FROM v2_local_streams').fetchone()[0],'started')
    def test_same_length_changed_bytes_hold_and_are_retained(self):
        with self.assertRaises(journal.JournalHeld):self.receive([b'abd'])
        target=self.root/('v2-input-'+self.value['deliveryId']+'.part');self.assertEqual(target.read_bytes(),b'abd')
        with self.client._tx() as db:self.assertEqual(db.execute('SELECT state FROM v2_local_streams').fetchone()[0],'started')
    def test_empty_object_requires_exact_zero_size_and_empty_digest(self):
        value=json.loads(self.raw);value['requestId']=base64.urlsafe_b64encode(b'e'*32).decode().rstrip('=')
        value['deliveryId']=base64.urlsafe_b64encode(b'f'*32).decode().rstrip('=')
        value['binding'].update(objectLength=0,objectSha256=hashlib.sha256(b'').hexdigest(),range={'kind':'empty'})
        raw=journal._encode(value).encode();_,digest=contract.parse_admission(raw)
        self.client.prepare(raw);self.client.start_once(value['requestId'],value['deliveryId'])
        headers=[('LT-Reference-Version','2'),('LT-Request-SHA256',digest),('LT-Delivery-Id',value['deliveryId']),
            ('LT-Delivery-Admitted','true'),('Content-Length','0'),('Cache-Control','no-store')]
        result=offline.receive_offline(self.client,request_id=value['requestId'],delivery_id=value['deliveryId'],status=200,headers=headers,chunks=[])
        self.assertEqual(result.read_bytes(),b'')
    def test_durable_stream_fence_blocks_adoption_without_holding_sqlite_during_io(self):
        binding=self.value['binding'];secret=b'synthetic-local-proof-key-'*2
        adoption=dict(requestId=self.value['requestId'],requestHash=self.v['requestSha256'],recoveryGeneration=1,
            recoveryActor='other-worker',authorityId=binding['authorityId'],authorityEpoch='synthetic-new-epoch',
            originalEpoch=binding['epoch'],originalClaimLineageSha256=binding['claimLineageSha256'],priorFenced=True)
        def chunks():
            yield b'a'
            with self.assertRaisesRegex(journal.JournalHeld,'local stream requires fencing'):
                self.client.adopt_synthetic(adoption,journal._mac(secret,journal.ADOPTION_DOMAIN,adoption),secret=secret)
            yield b'bc'
        with patch.object(journal,'ADOPTION_QUALIFIED',True):
            target=self.receive(chunks())
        self.assertEqual(target.read_bytes(),b'abc')
    def test_deadline_during_stream_retains_partial_and_started_state(self):
        now=[0.0]
        def chunks():
            yield b'a';now[0]=11.0;yield b'bc'
        with self.assertRaisesRegex(journal.JournalHeld,'deadline'):
            offline.receive_offline(self.client,request_id=self.value['requestId'],delivery_id=self.value['deliveryId'],
                status=200,headers=self.headers,chunks=chunks(),clock=lambda:now[0],timeout_seconds=10)
        target=self.root/('v2-input-'+self.value['deliveryId']+'.part')
        self.assertEqual(target.read_bytes(),b'a')
        with self.client._tx() as db:self.assertEqual(db.execute('SELECT state FROM v2_local_streams').fetchone()[0],'started')

@unittest.skipUnless(os.name=='posix','owned POSIX fixture required')
class OfflineV2DeliveryCrashTests(unittest.TestCase):
    def test_owned_child_dies_during_partial_body_no_replay(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);v=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-transfer-v2-hash-vector.json').read_text())
            raw=v['canonicalAdmission'].encode();value=json.loads(raw)
            script='''import os,sys,base64,json\nfrom pathlib import Path\nfrom ops.reference_worker_daemon import DaemonStateLock\nfrom processing import reference_transfer_v2_contract_candidate as c,reference_transfer_v2_client_journal_candidate as j,reference_transfer_v2_offline_response_candidate as o\nc.QUALIFIED=True;j.QUALIFIED=True;o.OFFLINE_SYNTHETIC_QUALIFIED=True\nroot=Path(sys.argv[1]);v=json.loads(base64.b64decode(sys.argv[2]));raw=v['canonicalAdmission'].encode();a=json.loads(raw)\nh=[('LT-Reference-Version','2'),('LT-Request-SHA256',v['requestSha256']),('LT-Delivery-Id',a['deliveryId']),('LT-Delivery-Admitted','true'),('Content-Length','3'),('Cache-Control','no-store')]\ndef chunks():\n yield b'a'\n print('partial-written',flush=True);os._exit(78)\nwith DaemonStateLock(root) as owner:\n client=j.V2ClientJournal(root/'v2.sqlite',owner,enabled=True);client.prepare(raw);client.start_once(a['requestId'],a['deliveryId']);o.receive_offline(client,request_id=a['requestId'],delivery_id=a['deliveryId'],status=200,headers=h,chunks=chunks())\n'''
            child=subprocess.Popen([sys.executable,'-c',script,str(root),base64.b64encode(json.dumps(v).encode()).decode()],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                try:stdout,stderr=child.communicate(timeout=12)
                except subprocess.TimeoutExpired:child.kill();child.communicate(timeout=3);self.fail('owned synthetic child timed out')
                self.assertEqual(child.returncode,78,stderr.decode(errors='replace')[-1000:]);self.assertEqual(stdout,b'partial-written\n')
            finally:
                if child.poll() is None:child.kill();child.wait(timeout=3)
            target=root/('v2-input-'+value['deliveryId']+'.part');self.assertEqual(target.read_bytes(),b'a')
            with patch.object(contract,'QUALIFIED',True),patch.object(journal,'QUALIFIED',True),patch.object(offline,'OFFLINE_SYNTHETIC_QUALIFIED',True),DaemonStateLock(root) as owner:
                client=journal.V2ClientJournal(root/'v2.sqlite',owner,enabled=True)
                with client._tx() as db:self.assertEqual(db.execute('SELECT state FROM v2_local_streams').fetchone()[0],'started')
                headers=[('LT-Reference-Version','2'),('LT-Request-SHA256',v['requestSha256']),('LT-Delivery-Id',value['deliveryId']),('LT-Delivery-Admitted','true'),('Content-Length','3'),('Cache-Control','no-store')]
                with self.assertRaises(journal.JournalHeld):offline.receive_offline(client,request_id=value['requestId'],delivery_id=value['deliveryId'],status=200,headers=headers,chunks=[b'abc'])
                self.assertEqual(target.read_bytes(),b'a')
