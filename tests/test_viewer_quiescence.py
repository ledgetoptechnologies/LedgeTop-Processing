import contextlib
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from ops import viewer_quiescence

# Synthetic copy of the Viewer tables the check reads (Viewer 821186f4, database.js).
SCHEMA = '''
CREATE TABLE processing_providers (id TEXT PRIMARY KEY, provider_type TEXT NOT NULL, display_name TEXT NOT NULL,
  endpoint TEXT NOT NULL, auth_env_key TEXT, enabled INTEGER NOT NULL DEFAULT 0, admission_limit INTEGER NOT NULL DEFAULT 1,
  capabilities_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE processing_attempts (id TEXT PRIMARY KEY, task_id TEXT NOT NULL, attempt_number INTEGER NOT NULL,
  provider_id TEXT, provider_task_id TEXT, options_json TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending','admitted','initializing','uploading','committed','queued_upstream',
    'running','ingesting','derivatives','ready_for_review','published','failed','cancelled')),
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE processing_jobs (id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL,
  job_type TEXT NOT NULL CHECK(job_type IN ('submit','reconcile','ingest')),
  status TEXT NOT NULL CHECK(status IN ('pending','leased','complete','failed','cancelled')),
  lease_owner TEXT, lease_expires_at TEXT, heartbeat_at TEXT, attempt_count INTEGER NOT NULL DEFAULT 0,
  available_at TEXT NOT NULL, error_code TEXT, error_message TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE derivative_jobs (id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL,
  derivative_type TEXT NOT NULL CHECK(derivative_type IN ('ept','mesh_tiles','lod_audit')),
  status TEXT NOT NULL CHECK(status IN ('pending','leased','complete','failed','cancelled')),
  request_json TEXT NOT NULL DEFAULT '{}', result_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
'''
T = '2026-10-30T12:00:00.000Z'
FUTURE = '2999-01-01T00:00:00.000Z'


def run(*argv):
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        status = viewer_quiescence.main([str(argument) for argument in argv])
    return status, json.loads(output.getvalue())


class ViewerQuiescenceTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / 'viewer.db'
        self.db = sqlite3.connect(self.path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript(SCHEMA)
        self.db.execute("INSERT INTO processing_providers VALUES ('ledgetop','nodeodm','LedgeTop',"
                        "'http://192.0.2.10:3005',NULL,1,1,'{}',?,?)", (T, T))
        self.db.commit()

    def tearDown(self):
        self.db.close()
        self.temporary.cleanup()

    def attempt(self, status, identifier='secret-task-attempt', provider='ledgetop'):
        self.db.execute('INSERT INTO processing_attempts VALUES (?,?,1,?,NULL,?,?,?,?)',
                        (identifier, 'secret-task-id', provider, '{}', status, T, T))
        self.db.commit()

    def job(self, status, available=T, identifier='job-1', attempt='secret-task-attempt'):
        self.db.execute("INSERT INTO processing_jobs (id,attempt_id,job_type,status,available_at,created_at,updated_at) "
                        "VALUES (?,?,'submit',?,?,?,?)", (identifier, attempt, status, available, T, T))
        self.db.commit()

    def test_empty_viewer_is_quiet(self):
        status, result = run(self.path)
        self.assertEqual((status, result['quiet']), (0, True))

    def test_settled_attempts_and_finished_jobs_are_quiet(self):
        for number, status in enumerate(('ready_for_review', 'published', 'failed', 'cancelled')):
            self.attempt(status, identifier='a%d' % number)
        self.job('complete', attempt='a0')
        self.job('failed', identifier='job-2', attempt='a1')
        self.assertEqual(run(self.path)[0], 0)

    def test_every_provider_active_status_is_not_quiet(self):
        for status in viewer_quiescence.PROVIDER_ACTIVE:
            self.db.execute('DELETE FROM processing_attempts')
            self.attempt(status)
            code, result = run(self.path)
            self.assertEqual(code, 1, status)
            self.assertEqual(result['processingAttempts']['providerActive'], 1)
            self.assertEqual(result['processingAttempts']['providerActiveByProvider'][0]['endpoint'],
                             'http://192.0.2.10:3005')

    def test_pending_retry_scheduled_later_is_not_quiet(self):
        self.attempt('failed')
        self.job('pending', available=FUTURE)
        code, result = run(self.path)
        self.assertEqual(code, 1)
        self.assertEqual(result['processingJobs']['scheduledLater'], 1)

    def test_leased_job_is_not_quiet(self):
        self.attempt('failed')
        self.job('leased')
        self.assertEqual(run(self.path)[0], 1)

    def test_viewer_local_derivative_work_is_counted_conservatively(self):
        self.attempt('derivatives')
        self.assertEqual(run(self.path)[0], 1)
        self.db.execute("UPDATE processing_attempts SET status='ready_for_review'")
        self.db.execute("INSERT INTO derivative_jobs (id,attempt_id,derivative_type,status,created_at,updated_at) "
                        "VALUES ('d1','secret-task-attempt','ept','pending',?,?)", (T, T))
        self.db.commit()
        code, result = run(self.path)
        self.assertEqual((code, result['derivativeJobs']['active']), (1, 1))

    def test_uncheckpointed_wal_writes_are_seen_on_the_live_file(self):
        self.db.execute('PRAGMA wal_autocheckpoint=0')
        self.attempt('running')  # committed, still only in the -wal file
        self.assertTrue(Path(str(self.path) + '-wal').stat().st_size > 0)
        self.assertEqual(run(self.path)[0], 1)

    def test_backup_snapshot_mode(self):
        self.attempt('uploading')
        snapshot = Path(self.temporary.name) / 'snapshot.db'
        target = sqlite3.connect(snapshot)
        self.db.backup(target)
        target.close()
        self.assertEqual(run(snapshot, '--snapshot')[0], 1)

    def test_read_only_and_reveals_no_identifiers(self):
        self.attempt('running')
        self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        _, result = run(self.path)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
        text = json.dumps(result)
        for private in ('secret-task-id', 'secret-task-attempt', 'job-1'):
            self.assertNotIn(private, text)

    def test_unexpected_schema_or_status_refuses(self):
        self.db.execute('PRAGMA ignore_check_constraints=ON')
        self.db.execute("INSERT INTO processing_attempts VALUES ('x','t',1,'ledgetop',NULL,'{}','paused',?,?)", (T, T))
        self.db.commit()
        status, result = run(self.path)
        self.assertEqual((status, result['quiet']), (2, False))
        self.assertIn('paused', result['error'])
        self.db.execute('DELETE FROM processing_attempts')
        self.db.execute('DROP TABLE derivative_jobs')
        self.db.commit()
        status, result = run(self.path)
        self.assertEqual(status, 2)
        self.assertIn('derivative_jobs', result['error'])
        status, result = run(Path(self.temporary.name) / 'missing.db')
        self.assertEqual(status, 2)


if __name__ == '__main__':
    unittest.main()
