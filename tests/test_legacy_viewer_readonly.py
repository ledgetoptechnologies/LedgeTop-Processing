"""Actual ordinary child app, retained synthetic 693 files, no engine services."""
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
import unittest
from unittest.mock import patch
import zipfile
from contextlib import closing

from processing.config import from_environment
from tests import test_first_coordinator_quarantine as quarantine


def faulted_normal_child(mode, marker_root):
    """Only descriptor-I/O fault seams change; constructor/HTTP/leases are real."""
    from processing import legacy_viewer_readonly as legacy
    from processing.app import main
    root = Path(marker_root)
    if mode == 'blocked-read':
        original = legacy._ArchiveOwner.read
        def read(owner, size=-1):
            if owner.streaming:
                (root / 'stream-blocked').write_text('owned synthetic stream')
                while not (root / 'release-stream').exists(): time.sleep(.01)
            return original(owner, size)
        legacy._ArchiveOwner.read = read
    elif mode in ('uncertain-close', 'false-close-success'):
        original = legacy._close_owned_fd
        def close(fd):
            if os.readlink('/proc/self/fd/' + str(fd)).endswith('/all.zip'):
                marker = root / 'close-attempt'
                if marker.exists(): raise AssertionError('blind close retry')
                marker.write_text('one attempted archive close')
                if mode == 'false-close-success': return
                raise OSError('synthetic unknown close outcome')
            return original(fd)
        legacy._close_owned_fd = close
    elif mode == 'bounded-scandir':
        from types import SimpleNamespace
        original = legacy.os.scandir
        class UnknownFirst:
            def __init__(self, iterator): self.iterator,self.count=iterator,0
            def __iter__(self): return self
            def __next__(self):
                self.count += 1
                if self.count != 1: raise AssertionError('unbounded unknown namespace scan')
                (root/'scandir-first-only').write_text('one unknown entry')
                return SimpleNamespace(name='unknown-private-entry')
            def close(self): self.iterator.close()
        def scandir(path):
            iterator=original(path)
            return UnknownFirst(iterator) if type(path) is int else iterator
        legacy.os.scandir=scandir
    else: raise AssertionError('unknown synthetic fault')
    raise SystemExit(main())


@unittest.skipUnless(os.name == 'posix', 'Linux original leases and nofollow descriptors')
class LegacyReadonlyDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = quarantine.FirstCoordinatorQuarantineTests()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.production = 'synthetic-legacy-production-' + 'p' * 32
        self.staging = 'synthetic-legacy-staging-' + 's' * 32
        self.fixture.env.update(PROCESSING_LEGACY_VIEWER_READ_ONLY='1',
            PROCESSING_VIEWER_TOKEN=self.production, PROCESSING_VIEWER_STAGING_TOKEN=self.staging)
        self.task, self.job, self.directory = self.fixture.old.tasks[0]
        self.path = '/task/' + self.task

    def get(self, suffix, token=None):
        return self.fixture.request('GET', self.path + suffix + '?token=' + (token or self.production))

    def assert_error(self, suffix, status=503):
        code, _, body = self.get(suffix)
        self.assertEqual(code, status, body)
        return body

    def wait_marker(self, name):
        deadline = time.monotonic() + 5
        while not (self.fixture.root / name).exists() and time.monotonic() < deadline:
            self.assertIsNone(self.fixture.child.poll())
            time.sleep(.01)
        self.assertTrue((self.fixture.root / name).exists())

    def start_faulted(self, mode):
        self.fixture.child = subprocess.Popen([sys.executable, '-B', '-c',
            'from tests.test_legacy_viewer_readonly import faulted_normal_child; import sys; faulted_normal_child(sys.argv[1],sys.argv[2])',
            mode, str(self.fixture.root)], cwd=str(Path(__file__).resolve().parents[1]),
            env=self.fixture.env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
        child = self.fixture.child
        self.fixture.child_identity=(child.pid,Path('/proc/'+str(child.pid)+'/stat').read_text().split()[21])
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            self.assertIsNone(child.poll())
            try:
                if self.fixture.request('GET','/healthz')[0] == 200: return
            except OSError: pass
            time.sleep(.025)
        self.fail('normal child readiness deadline exceeded')

    def assert_original_leases_excluded(self):
        import fcntl
        for path in (self.fixture.old.root/'coordinator.lock', self.fixture.lease_file):
            with path.open('a+b') as contender:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(contender,fcntl.LOCK_EX|fcntl.LOCK_NB)

    def archive_pin_states(self):
        with closing(sqlite3.connect(self.fixture.old.path.as_uri()+'?mode=ro',uri=True)) as db:
            return [row[0] for row in db.execute("SELECT state FROM coordinator_operation_pins WHERE kind='legacy-archive'")]

    def binary_download(self):
        import http.client
        connection=http.client.HTTPConnection('127.0.0.1',self.fixture.native,timeout=5)
        try:
            connection.request('GET',self.path+'/download/all.zip?token='+self.production)
            response=connection.getresponse();data=response.read(1024*1024)
            self.assertEqual(response.status,200)
            self.assertEqual(data,(self.directory/'all.zip').read_bytes())
        finally:connection.close()

    def test_normal_child_reads_metadata_output_and_exact_cached_archive_without_mutation(self):
        before_rows, before_files = self.fixture.legacy_rows(), self.fixture.artifact_bytes()
        self.fixture.start_child()
        code, _, body = self.get('/info')
        self.assertEqual(code, 200, body)
        self.assertEqual((body['uuid'], body['status']['code'], body['imagesCount']), (self.task, 40, 23))
        self.assertEqual(self.get('/output')[2], [])
        code, _, tasks = self.fixture.request('GET', '/task/list?token=' + self.production)
        self.assertEqual(code, 200, tasks)
        self.assertEqual({item['uuid'] for item in tasks}, {item[0] for item in self.fixture.old.tasks})
        # Actual HTTP download is binary and cannot use the fixture JSON reader.
        import http.client
        connection = http.client.HTTPConnection('127.0.0.1', self.fixture.native, timeout=3)
        try:
            connection.request('GET', self.path + '/download/all.zip?token=' + self.production)
            response = connection.getresponse(); result = response.read(1024 * 1024)
            self.assertEqual(response.status, 200)
            self.assertEqual(result, (self.directory / 'all.zip').read_bytes())
        finally: connection.close()
        for route in ('/task/new/init', '/task/new/commit/' + self.task, '/task/cancel', '/task/remove'):
            code, _, body = self.fixture.request('POST', route + '?token=' + self.production, headers_only=True)
            self.assertEqual((code, body), (405, {'error': 'legacy_read_only'}))
        self.fixture.stop_child()
        self.assertEqual(self.fixture.legacy_rows(), before_rows)
        self.assertEqual(self.fixture.artifact_bytes(), before_files)

    def test_credentials_environment_and_frozen_gate_precede_any_delivery(self):
        self.fixture.start_child()
        self.assert_error('/info', 200)
        self.assertEqual(self.fixture.request('GET', self.path + '/info?token=' + self.production + '&invalid=1')[0], 400)
        self.assertEqual(self.get('/info', self.staging)[0], 404)
        self.assertEqual(self.get('/info', 'unknown')[0], 401)
        self.assertEqual(self.fixture.request('GET', '/task/list?token=' + self.staging)[2], [])
        code, _, gate = self.fixture.request('GET', '/v1/prototype/coordinator-freeze', self.fixture.operator)
        import uuid
        plan = str(uuid.uuid4())
        for action in ('intent', 'drain', 'seal'):
            code, _, gate = self.fixture.request('POST', '/v1/prototype/coordinator-freeze', self.fixture.operator,
                dict(requestId=str(uuid.uuid4()),planId=plan,expectedRevision=gate['revision'],
                     action=action,startupId=gate['startupId']))
            self.assertEqual(code, 200, gate)
        self.assertEqual(self.get('/info')[2], {'error': 'coordinator_maintenance'})
        self.assertEqual(self.get('/download/all.zip')[0], 503)

    def test_missing_receipt_or_cache_has_no_engine_fallback(self):
        (self.directory / 'all.zip.receipt').unlink()
        self.fixture.start_child()
        self.assert_error('/download/all.zip')
        self.assertEqual(self.get('/info')[0], 200)

    def test_optional_original_archive_intent_must_match_and_remain_private(self):
        receipt = json.loads((self.directory / 'all.zip.receipt').read_text())
        identity = {key: receipt[key] for key in ('version', 'task', 'generation', 'job', 'node', 'engine')}
        intent = self.directory / 'all.zip.intent'
        raw = json.dumps(identity).encode()
        intent.write_bytes(raw); intent.chmod(0o600)
        self.fixture.start_child()
        self.binary_download()
        changed = dict(identity, generation=identity['generation'] + 1)
        intent.write_text(json.dumps(changed)); self.assert_error('/download/all.zip')
        intent.write_bytes(raw); intent.chmod(0o640); self.assert_error('/download/all.zip')
        intent.chmod(0o600)
        other = self.fixture.root / 'other-intent'; other.write_bytes(raw); other.chmod(0o600)
        intent.unlink(); intent.symlink_to(other); self.assert_error('/download/all.zip')
        intent.unlink(); os.link(other, intent); self.assert_error('/download/all.zip')
        intent.unlink(); intent.write_bytes(raw); intent.chmod(0o600)
        self.binary_download()

    def test_missing_archive_and_unknown_inventory_are_explicitly_unavailable(self):
        archive = self.directory / 'all.zip'; original = archive.read_bytes(); archive.unlink()
        self.fixture.start_child()
        self.assert_error('/download/all.zip')
        archive.write_bytes(original); archive.chmod(0o600)
        unexpected = self.directory / 'all.zip.partial'; unexpected.write_bytes(b'synthetic partial')
        unexpected.chmod(0o600)
        self.assert_error('/download/all.zip')
        self.assertEqual(unexpected.read_bytes(), b'synthetic partial')

    def test_archive_symlink_hardlink_foreign_mode_and_hash_corruption_refuse(self):
        archive = self.directory / 'all.zip'; original = archive.read_bytes()
        self.fixture.start_child()
        archive.write_bytes(b'x' * len(original))
        self.assert_error('/download/all.zip')
        archive.write_bytes(original); archive.chmod(0o640)
        self.assert_error('/download/all.zip')
        archive.chmod(0o600)
        external = self.fixture.root / 'external.zip'; external.write_bytes(original); external.chmod(0o600)
        archive.unlink(); archive.symlink_to(external)
        self.assert_error('/download/all.zip')
        archive.unlink(); os.link(external, archive)
        self.assert_error('/download/all.zip')

    def test_wrong_receipt_and_unsafe_zip_inventory_refuse_without_repair(self):
        receipt_path = self.directory / 'all.zip.receipt'; receipt = json.loads(receipt_path.read_text())
        receipt['generation'] += 1; receipt_path.write_text(json.dumps(receipt))
        self.fixture.start_child()
        self.assert_error('/download/all.zip')
        receipt['generation'] -= 1
        archive = self.directory / 'all.zip'
        with zipfile.ZipFile(archive, 'w') as value: value.writestr('../escaped.txt', b'synthetic')
        data = archive.read_bytes(); receipt.update(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        receipt_path.write_text(json.dumps(receipt))
        self.assert_error('/download/all.zip')
        self.assertFalse((self.fixture.root / 'escaped.txt').exists())

    def test_changed_job_claim_binding_does_not_authorize_download(self):
        self.fixture.old.mutate('UPDATE jobs SET claim_hash=? WHERE id=?', ('0' * 64, self.job))
        self.fixture.start_child()
        self.assert_error('/info')
        self.assert_error('/download/all.zip')

    def test_configuration_default_off_and_mutually_exclusive(self):
        env = dict(self.fixture.env)
        env.pop('PROCESSING_LEGACY_VIEWER_READ_ONLY')
        with patch.dict(os.environ, env, clear=True): self.assertFalse(from_environment().legacy_viewer_read_only)
        env['PROCESSING_LEGACY_VIEWER_READ_ONLY'] = '1'
        env['PROCESSING_VIEWER_ENABLED'] = '1'
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(ValueError): from_environment()

    def test_actual_blocked_http_stream_sigterm_keeps_main_and_external_leases_until_verified_close(self):
        self.start_faulted('blocked-read')
        errors=[]
        def download():
            try:self.binary_download()
            except BaseException as error:errors.append(error)
        client=threading.Thread(target=download)
        client.start()
        try:
            self.wait_marker('stream-blocked')
            self.assertEqual(self.archive_pin_states(),['active'])
            self.fixture.child.send_signal(signal.SIGTERM)
            time.sleep(1.2)
            self.assertIsNone(self.fixture.child.poll())
            self.assert_original_leases_excluded()
        finally:
            (self.fixture.root/'release-stream').touch()
            client.join(6)
        self.assertFalse(client.is_alive());self.assertEqual(errors,[])
        self.fixture.stop_child()
        self.assertEqual(self.archive_pin_states(),[])

    def test_actual_uncertain_archive_close_keeps_registered_pin_and_process_without_retry(self):
        self.uncertain_close_case('uncertain-close')

    def test_close_returning_success_with_live_fd_cannot_retire_the_registered_owner(self):
        self.uncertain_close_case('false-close-success')

    def uncertain_close_case(self, mode):
        self.start_faulted(mode)
        child=self.fixture.child
        try:
            self.binary_download()
            self.wait_marker('close-attempt')
            deadline=time.monotonic()+3
            while self.archive_pin_states()!=['uncertain'] and time.monotonic()<deadline:time.sleep(.01)
            self.assertEqual(self.archive_pin_states(),['uncertain'])
            child.send_signal(signal.SIGTERM)
            time.sleep(1.2)
            self.assertIsNone(child.poll())
            self.assert_original_leases_excluded()
            self.assertEqual((self.fixture.root/'close-attempt').read_text(),'one attempted archive close')
        finally:
            # No production retry/force-close operation exists. Terminate only
            # this fault-injected child; OS process death closes its descriptors.
            if child.poll() is None:child.kill()
            child.communicate(timeout=5)
            self.fixture.child=None
        self.assertEqual(self.archive_pin_states(),['uncertain'])

    def test_directory_inventory_refuses_first_unknown_without_materializing_the_tail(self):
        self.start_faulted('bounded-scandir')
        self.assert_error('/download/all.zip')
        self.wait_marker('scandir-first-only')
        self.assertEqual(self.archive_pin_states(),[])

    def test_legacy_uncertain_pin_state_is_prepaid_in_real_sql_metadata_usage(self):
        from processing.coordinator_freeze import metadata_bytes, active_metadata_bytes
        with closing(sqlite3.connect(':memory:')) as db:
            db.row_factory=sqlite3.Row
            db.execute('CREATE TABLE coordinator_freeze_state(singleton INTEGER,phase TEXT)')
            db.execute("INSERT INTO coordinator_freeze_state VALUES(1,'open')")
            db.execute('CREATE TABLE coordinator_operation_pins(reference TEXT,epoch TEXT,kind TEXT,start_revision INTEGER,state TEXT)')
            db.execute("INSERT INTO coordinator_operation_pins VALUES('exact-owner','original-epoch','legacy-archive',0,'active')")
            before=(metadata_bytes(db),active_metadata_bytes(db))
            db.execute("UPDATE coordinator_operation_pins SET state='uncertain'")
            self.assertEqual((metadata_bytes(db),active_metadata_bytes(db)),before)


if __name__ == '__main__': unittest.main()
