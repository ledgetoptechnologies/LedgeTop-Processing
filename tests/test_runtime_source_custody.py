"""Real normal-service observation; local synthetic mounted spool, no source I/O."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time
import unittest
import uuid

from tests import test_first_coordinator_quarantine as quarantine


def guarded_normal_child():
    from processing import app
    from processing.reference_source_readers import SourceReaders
    from processing.reference_source_readers import SourceReaderError
    from processing.source_spool import SourceSpool
    original = app.CoordinatorService.run
    def run(service, stop):
        def refuse(*_args, **_kwargs):
            raise AssertionError('observation attempted a custody constructor')
        # make_handler has constructed the actual runtime. All subsequent
        # observation and frozen requests must use that existing instance.
        SourceReaders.__init__ = refuse
        SourceSpool.__init__ = refuse
        check = SourceReaders.check
        def checked(session):
            if (Path(os.environ['LTDS_CUSTODY_TEST_ROOT'])/'fail-existing-check').exists():
                raise SourceReaderError('synthetic existing session failure')
            return check(session)
        SourceReaders.check = checked
        return original(service, stop)
    app.CoordinatorService.run = run
    raise SystemExit(app.main())


@unittest.skipUnless(sys.platform.startswith('linux'), 'actual Linux process and existing flock observations')
class RuntimeSourceCustodyTests(unittest.TestCase):
    def setUp(self):
        self.fixture = quarantine.FirstCoordinatorQuarantineTests()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.route = '/v1/prototype/coordinator-custody'

    def start(self, *, source=True):
        if source:self.fixture.write_config(self.fixture.final_config())
        self.fixture.env['LTDS_CUSTODY_TEST_ROOT']=str(self.fixture.root)
        self.fixture.child = subprocess.Popen([sys.executable, '-B', '-c',
            'from tests.test_runtime_source_custody import guarded_normal_child; guarded_normal_child()'],
            cwd=str(Path(__file__).resolve().parents[1]), env=self.fixture.env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
        child = self.fixture.child
        self.fixture.child_identity = (child.pid, Path('/proc/'+str(child.pid)+'/stat').read_text().split()[21])
        deadline = time.monotonic()+8
        while time.monotonic()<deadline:
            self.assertIsNone(child.poll())
            try:
                if self.fixture.request('GET','/healthz')[0] == 200:return
            except OSError:pass
            time.sleep(.025)
        self.fail('normal custody observation service did not become ready')

    def observe(self, token=None):
        code, _, body = self.fixture.request('GET', self.route, token or self.fixture.operator)
        self.assertEqual(code,200,body)
        fields = {'schemaVersion','protocol','status','authorityId','startupId','processId','processStartTicks',
            'sourceReadersPresent','epoch','bindingSha256','sessionVerified','leaseVerified','journalMatches'}
        self.assertEqual(set(body),fields)
        self.assertEqual(body['schemaVersion'],1)
        self.assertEqual(body['protocol'],'ltds-runtime-source-custody-v1')
        return body

    def freeze(self):
        _, _, state = self.fixture.request('GET','/v1/prototype/coordinator-freeze',self.fixture.operator)
        plan = str(uuid.uuid4())
        for action in ('intent','drain','seal'):
            code, _, state = self.fixture.request('POST','/v1/prototype/coordinator-freeze',self.fixture.operator,
                dict(requestId=str(uuid.uuid4()),planId=plan,expectedRevision=state['revision'],
                    action=action,startupId=state['startupId']))
            self.assertEqual(code,200,state)
        return state

    def dump(self):
        with closing(sqlite3.connect(self.fixture.old.path.as_uri()+'?mode=ro',uri=True)) as db:
            return '\n'.join(db.iterdump())

    def test_operator_auth_frozen_actual_runtime_and_no_constructor_or_database_writes(self):
        self.start()
        for token in (None,'wrong',self.fixture.api_token,self.fixture.viewer):
            code, _, body = self.fixture.request('GET',self.route,token)
            self.assertEqual(code,401)
            self.assertEqual(body,{'error':'unauthorized'})
        first = self.observe()
        self.assertEqual(first['status'],'verified',first)
        self.assertEqual(first['processId'],self.fixture.child.pid)
        self.assertEqual(first['processStartTicks'],self.fixture.child_identity[1])
        self.assertEqual(first['authorityId'],self.fixture.old.authority)
        self.assertTrue(all(first[key] is True for key in ('sourceReadersPresent','sessionVerified','leaseVerified','journalMatches')))
        self.assertRegex(first['epoch'],r'^[a-f0-9]{64}$')
        self.assertRegex(first['bindingSha256'],r'^[a-f0-9]{64}$')
        with closing(sqlite3.connect(self.fixture.old.path.as_uri()+'?mode=ro',uri=True)) as db:
            binding, epoch = db.execute('SELECT binding,epoch FROM source_reader_binding WHERE id=1').fetchone()
        self.assertEqual(first['epoch'],epoch)
        self.assertEqual(first['bindingSha256'],hashlib.sha256(binding.encode()).hexdigest())
        state = self.freeze();self.assertEqual(first['startupId'],state['startupId'])
        before = self.dump()
        self.assertEqual(self.observe(),first)
        self.assertEqual(self.dump(),before,'frozen observation must not create operation pins or mutate rows')
        self.assertEqual(list(self.fixture.spool.iterdir()),[])
        self.fixture.stop_child()

    def test_absent_runtime_source_session_is_explicit_not_constructed(self):
        self.start(source=False)
        self.freeze();before=self.dump()
        body=self.observe()
        self.assertEqual(body['status'],'absent')
        self.assertFalse(body['sourceReadersPresent'])
        self.assertIsNone(body['epoch']);self.assertIsNone(body['bindingSha256'])
        self.assertFalse(body['sessionVerified']);self.assertFalse(body['leaseVerified']);self.assertFalse(body['journalMatches'])
        self.assertEqual(self.dump(),before)

    def test_database_epoch_cannot_impersonate_the_existing_runtime_session(self):
        self.start();self.freeze()
        actual=self.observe();self.assertEqual(actual['status'],'verified')
        changed='0'*64 if actual['epoch']!='0'*64 else '1'*64
        with closing(sqlite3.connect(self.fixture.old.path)) as db, db:
            db.execute('UPDATE source_reader_binding SET epoch=? WHERE id=1',(changed,))
            db.execute("INSERT INTO source_reader_epochs VALUES(?,'active')",(changed,))
        try:
            before=self.dump();body=self.observe()
            self.assertEqual(body['status'],'unavailable')
            self.assertTrue(body['sourceReadersPresent'])
            self.assertIsNone(body['epoch']);self.assertIsNone(body['bindingSha256'])
            self.assertFalse(body['sessionVerified']);self.assertFalse(body['leaseVerified']);self.assertFalse(body['journalMatches'])
            self.assertEqual(self.dump(),before)
        finally:
            with closing(sqlite3.connect(self.fixture.old.path)) as db, db:
                db.execute('UPDATE source_reader_binding SET epoch=? WHERE id=1',(actual['epoch'],))
                db.execute('DELETE FROM source_reader_epochs WHERE epoch=?',(changed,))
        self.assertEqual(self.observe(),actual)

    def test_failed_existing_session_check_reports_unavailable_without_fabricated_proof(self):
        self.start();self.freeze();actual=self.observe()
        marker=self.fixture.root/'fail-existing-check';marker.touch()
        try:
            before=self.dump();body=self.observe()
            self.assertEqual(body['status'],'unavailable')
            self.assertTrue(body['sourceReadersPresent'])
            self.assertIsNone(body['epoch']);self.assertIsNone(body['bindingSha256'])
            self.assertEqual(self.dump(),before)
        finally:marker.unlink()
        self.assertEqual(self.observe(),actual)

