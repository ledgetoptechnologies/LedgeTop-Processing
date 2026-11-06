"""Synthetic reservation-scoped page intake; no real Viewer or custody."""
import base64
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock,patch
from processing import reference_protocol as p
from processing.direct_snapshot_page_journal import SnapshotPageJournal, ALLOWANCE
from processing.direct_v2_reservation import DirectV2Reservations
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.maintenance import Conflict
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
            self.assertGreaterEqual(self.store._metadata_used_bytes(db),ALLOWANCE)
            with self.assertRaises(sqlite3.IntegrityError):db.execute('DELETE FROM direct_snapshot_pages')

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
