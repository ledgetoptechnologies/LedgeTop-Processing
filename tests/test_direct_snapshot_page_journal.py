"""Synthetic reservation-scoped page intake; no real Viewer or custody."""
import base64
import copy
import hashlib
import importlib.util
import json
import os
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock,patch
from processing import reference_protocol as p
from processing.direct_snapshot_page_journal import SnapshotPageJournal, SESSION_OVERHEAD
from processing.direct_v2_reservation import DirectV2Reservations
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.maintenance import Conflict
from processing.direct_v2_snapshot_verifier import SNAPSHOT_DOMAIN
from tests.test_direct_v2_snapshot_verifier import SnapshotTests,RES
from tests import test_direct_snapshot_http as http_fixture

def b64(value):return base64.urlsafe_b64encode(value).decode().rstrip('=')

class SnapshotJournalTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.root=Path(temp.name);self.queue=Queue(self.root,reference_enabled=True);self.addCleanup(self.queue.close)
        self.store=ReferenceStore(self.queue)
        self.snapshot=SnapshotTests();self.snapshot.setUp();s=self.snapshot
        self.reservations=object.__new__(DirectV2Reservations)
        self.reservations.store=self.store;self.reservations.queue=self.queue
        self.reservations._snapshot_pins={('viewer','staging','issuer','key'):s.key.public_key()}
        self.reservations.inspect_held_snapshot=Mock(return_value={'state':'source_pages_verified_not_custody',
            'sourceCustodyAccepted':False,'processingAuthorized':False})
        self.api=SnapshotPageJournal(self.store,self.reservations)
        self.scope=('viewer','staging');self.snapshot_id=s.intent['snapshotId']
        self.row={'principal':'viewer','current_environment':'staging',
                  'source_attestation_json':json.dumps({'proof':{'sourceSnapshotSha256':s.sha}})}
        self.scope_patch=patch('processing.direct_snapshot_page_journal.validate_receiving',return_value=self.row)
        self.scope_patch.start();self.addCleanup(self.scope_patch.stop)
        self.begin=p.canonical_json({'intentBase64url':b64(s.raw),'signatureBase64url':b64(s.signature)})

    def call(self,op,body=b'',scope=None,snapshot=None):
        return self.api._dispatch_from_listener(scope or self.scope,RES,snapshot or self.snapshot_id,op,body)

    def test_exact_replay_owner_and_completed_pages(self):
        self.assertEqual(self.call('begin',self.begin).status,200)
        self.assertEqual(self.call('begin',self.begin).status,200)
        self.assertEqual(self.call('begin',self.begin,scope=('other','staging')).status,409)
        self.assertEqual(self.call('begin',self.begin,snapshot='99999999-9999-4999-8999-999999999999').status,409)
        self.assertEqual(self.call('complete').status,409)
        page=self.snapshot.frames[:-1]
        self.assertEqual(self.call('pages/0',page).status,200)
        self.assertEqual(self.call('pages/0',page).status,200)
        self.assertEqual(self.call('pages/0',page.replace(b'flight',b'wrong_')).status,409)
        response=self.call('complete');self.assertEqual(response.status,200)
        self.assertFalse(response.body['sourceCustodyAccepted'])
        self.assertEqual(self.reservations.inspect_held_snapshot.call_args.kwargs['source_frames'],self.snapshot.frames)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_snapshot_sessions').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_snapshot_pages').fetchone()[0],1)
            self.assertGreaterEqual(self.store._metadata_used_bytes(db),len(self.snapshot.raw)+len(self.snapshot.frames)+SESSION_OVERHEAD)
            with self.assertRaises(sqlite3.IntegrityError):db.execute('DELETE FROM direct_snapshot_pages')

    def test_inspection_keeps_operation_pin_until_reader_returns(self):
        self.assertEqual(self.call('begin',self.begin).status,200)
        self.assertEqual(self.call('pages/0',self.snapshot.frames[:-1]).status,200)
        def inspect(*args,**kwargs):
            with self.queue._db() as db:
                pins=db.execute("SELECT COUNT(*) FROM coordinator_operation_pins WHERE kind='source-io-admission' AND state='active'").fetchone()[0]
            self.assertGreaterEqual(pins,1)
            return {'sourceCustodyAccepted':False}
        self.reservations.inspect_held_snapshot.side_effect=inspect
        self.assertEqual(self.call('complete').status,200)
        with self.queue._db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM coordinator_operation_pins WHERE kind='source-io-admission' AND state='active'").fetchone()[0],0)

    def test_noncanonical_encoding_wrong_key_and_quota_hold(self):
        data=json.loads(self.begin);data['intentBase64url']+='='
        self.assertEqual(self.call('begin',p.canonical_json(data)).status,409)
        data=json.loads(self.begin);data['signatureBase64url']=b64(bytes(64))
        self.assertEqual(self.call('begin',p.canonical_json(data)).status,409)
        with patch.object(self.store,'_quota',side_effect=Conflict('full')):
            self.assertEqual(self.call('begin',self.begin).status,409)
        with self.queue._db() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='direct_snapshot_sessions'").fetchone())
        self.assertEqual(self.call('begin',self.begin).status,200)

    def test_six_small_signed_sessions_use_actual_bounded_allowance(self):
        # Separate reservations/snapshots prove we are not replaying one row.
        rows={};requests=[]
        for _ in range(6):
            reservation=str(uuid.uuid4());snapshot=str(uuid.uuid4())
            intent=copy.deepcopy(self.snapshot.intent);intent['snapshotId']=snapshot
            raw=p.canonical_json(intent);signature=self.snapshot.key.sign(SNAPSHOT_DOMAIN+raw)
            rows[reservation]={'principal':'viewer','current_environment':'staging',
                'source_attestation_json':json.dumps({'proof':{'sourceSnapshotSha256':hashlib.sha256(raw).hexdigest()}})}
            requests.append((reservation,snapshot,p.canonical_json({'intentBase64url':b64(raw),'signatureBase64url':b64(signature)}),raw,intent))
        with patch('processing.direct_snapshot_page_journal.validate_receiving',side_effect=lambda _db,key:rows[key]):
            for reservation,snapshot,body,raw,intent in requests:
                response=self.api._dispatch_from_listener(self.scope,reservation,snapshot,'begin',body)
                self.assertEqual(response.status,200);self.assertFalse(response.body['sourceCustodyAccepted'])
            with self.queue._db() as db:
                saved=db.execute('SELECT intent,frame_bytes,allowance FROM direct_snapshot_sessions').fetchall()
                self.assertEqual(len(saved),6)
                for row in saved:self.assertEqual(row['allowance'],len(row['intent'])+row['frame_bytes']+SESSION_OVERHEAD)
                self.assertLess(sum(row['allowance'] for row in saved),6*1024*1024)
                used=self.store._metadata_used_bytes(db)
            reservation=str(uuid.uuid4());snapshot=str(uuid.uuid4())
            intent=copy.deepcopy(self.snapshot.intent);intent['snapshotId']=snapshot;raw=p.canonical_json(intent)
            rows[reservation]={'principal':'viewer','current_environment':'staging',
                'source_attestation_json':json.dumps({'proof':{'sourceSnapshotSha256':hashlib.sha256(raw).hexdigest()}})}
            body=p.canonical_json({'intentBase64url':b64(raw),'signatureBase64url':b64(self.snapshot.key.sign(SNAPSHOT_DOMAIN+raw))})
            self.store.max_metadata_bytes=used+len(raw)+intent['sourceFrameBytes']+SESSION_OVERHEAD-1
            self.assertEqual(self.api._dispatch_from_listener(self.scope,reservation,snapshot,'begin',body).status,409)
            with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_snapshot_sessions').fetchone()[0],6)
            with patch.object(self.store,'_quota',side_effect=Conflict('full')):
                reservation,snapshot,body,*_=requests[-1]
                # Exact replay does not consume extra quota.
                self.assertEqual(self.api._dispatch_from_listener(self.scope,reservation,snapshot,'begin',body).status,200)

    def test_old_journal_version_is_held_without_erasure(self):
        self.assertEqual(self.call('begin',self.begin).status,200)
        with self.queue._db() as db:db.execute('UPDATE coordinator_identity SET direct_snapshot_version=1 WHERE singleton=1')
        self.assertEqual(self.call('begin',self.begin).status,409)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_snapshot_sessions').fetchone()[0],1)
            db.execute('UPDATE coordinator_identity SET direct_snapshot_version=2 WHERE singleton=1')

    def test_smaller_accounting_value_than_signed_intent_is_held(self):
        self.assertEqual(self.call('begin',self.begin).status,200)
        from processing.direct_snapshot_page_journal import TRIGGERS
        with self.queue._db() as db:
            db.execute('DROP TRIGGER direct_snapshot_sessions_no_update')
            db.execute('UPDATE direct_snapshot_sessions SET frame_bytes=frame_bytes-1,allowance=allowance-1')
            db.execute(TRIGGERS[1])
        self.assertEqual(self.call('begin',self.begin).status,409)

    @unittest.skipUnless(os.environ.get('LTDS_OLD_SNAPSHOT_SOURCE'),'previous snapshot source fixture not supplied')
    def test_previous_snapshot_binary_refuses_v2_journal(self):
        self.assertEqual(self.call('begin',self.begin).status,200)
        spec=importlib.util.spec_from_file_location('processing.old_direct_snapshot_journal',os.environ['LTDS_OLD_SNAPSHOT_SOURCE'])
        old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
        with self.queue._db() as db:
            with self.assertRaises(Conflict):old.metadata_bytes(db)

    @unittest.skipUnless(os.environ.get('LTDS_OLD_SNAPSHOT_SOURCE'),'previous snapshot source fixture not supplied')
    def test_actual_v1_snapshot_journal_is_held_without_changes(self):
        spec=importlib.util.spec_from_file_location('processing.old_direct_snapshot_journal_v1',os.environ['LTDS_OLD_SNAPSHOT_SOURCE'])
        old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');old.ensure(db)
            db.execute('INSERT INTO direct_snapshot_sessions VALUES(?,?,?,?,?,?,?)',
                (RES,self.snapshot_id,'viewer','staging',self.snapshot.raw,self.snapshot.signature,old.ALLOWANCE))
            db.execute('UPDATE coordinator_identity SET direct_snapshot_session_count=1 WHERE singleton=1')
        self.assertEqual(self.call('begin',self.begin).status,409)
        with self.queue._db() as db:
            row=db.execute('SELECT * FROM direct_snapshot_sessions').fetchone()
            self.assertEqual((row['intent'],row['signature'],row['allowance']),
                             (self.snapshot.raw,self.snapshot.signature,old.ALLOWANCE))
            self.assertEqual(db.execute('SELECT direct_snapshot_version FROM coordinator_identity WHERE singleton=1').fetchone()[0],1)

    def test_real_mtls_listener_dispatches_page_journal(self):
        fixture=http_fixture.SnapshotListenerTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        fixture.scopes[fixture.fingerprint]={'principal':'viewer','environment':'staging'}
        fixture.backend=self.api;server=fixture.start()
        prefix=f'/internal/v2/reservations/{RES}/source-snapshots/{self.snapshot_id}/'
        def post(part,body):return fixture.request(server,cert=fixture.good,key=fixture.goodkey,path=prefix+part,body=body)
        self.assertEqual(post('begin',self.begin)[0],200)
        self.assertEqual(post('pages/0',self.snapshot.frames[:-1])[0],200)
        status,response=post('complete',b'');self.assertEqual(status,200)
        self.assertFalse(response['sourceCustodyAccepted'])
        self.assertFalse(response['processingAuthorized'])
        self.assertEqual(post('begin',self.begin)[0],200)

    def test_restart_replays_exact_session_and_guards_missing_history(self):
        self.assertEqual(self.call('begin',self.begin).status,200)
        self.assertEqual(self.call('pages/0',self.snapshot.frames[:-1]).status,200)
        self.queue.close()
        self.queue=Queue(self.root,reference_enabled=True);self.addCleanup(self.queue.close)
        self.store=ReferenceStore(self.queue)
        self.reservations.store=self.store;self.reservations.queue=self.queue
        self.api=SnapshotPageJournal(self.store,self.reservations)
        self.assertEqual(self.call('begin',self.begin).status,200)
        self.assertEqual(self.call('pages/0',self.snapshot.frames[:-1]).status,200)
        self.assertEqual(self.call('complete').status,200)
        with self.queue._db() as db:
            db.execute('DROP TRIGGER direct_snapshot_pages_no_delete')
        self.assertEqual(self.call('complete').status,409)
        from processing.direct_snapshot_page_journal import TRIGGERS
        with self.queue._db() as db:
            db.execute('DELETE FROM direct_snapshot_pages')
            db.execute(TRIGGERS[5])
        self.assertEqual(self.call('complete').status,409)

if __name__=='__main__':unittest.main()
