"""Actual ordinary child app, retained synthetic 693 files, no engine services."""
import hashlib
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
import zipfile

from processing.config import from_environment
from tests import test_first_coordinator_quarantine as quarantine


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


if __name__ == '__main__': unittest.main()
