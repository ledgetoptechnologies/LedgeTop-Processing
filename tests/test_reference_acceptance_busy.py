"""Only real SQLite contention becomes a retryable acceptance response."""
import sqlite3
import unittest
import uuid
from unittest.mock import Mock, patch

from processing.reference_api import ReferenceAPI
from processing.reference_worker_api import ReferenceWorkerAPI


class AcceptanceBusyTests(unittest.TestCase):
    def test_actual_sqlite_lock_is_retryable_for_both_acceptance_routes(self):
        uri = 'file:busy-' + uuid.uuid4().hex + '?mode=memory&cache=shared'
        first = sqlite3.connect(uri, uri=True)
        second = sqlite3.connect(uri, uri=True, timeout=.1)
        try:
            first.execute('CREATE TABLE sample(value INTEGER)')
            first.commit(); first.execute('BEGIN IMMEDIATE')
            with self.assertRaises(sqlite3.OperationalError) as contention:
                second.execute('BEGIN IMMEDIATE')
            try: second.execute('SELECT * FROM absent_table')
            except sqlite3.OperationalError as failure: permanent = failure
            else: self.fail('expected missing-table error')
            for error, expected in ((contention.exception, (503, 'acceptance_store_busy')),
                                    (permanent, (500, 'internal_error'))):
                store = Mock()
                store.get.side_effect = error
                with patch('processing.reference_terminal.TerminalService'), patch('processing.reference_failure.FailureReceiver'):
                    public = ReferenceAPI(store, bearer_tokens={'synthetic-viewer-token': 'staging'},
                        capability_fingerprint='a'*64, options_fingerprint='b'*64)
                    worker = ReferenceWorkerAPI(store, Mock(), bearer_workers={'synthetic-worker-token':
                        {'node_id':'node', 'worker_id':'worker', 'worker_instance_id':'instance'}})
                response = public.handle('POST', '/ltds/v1/jobs/job-1/acceptance',
                    {'Authorization':'Bearer synthetic-viewer-token'}, b'{}', protected_transport=True)
                self.assertEqual((response.status, response.body['error']), expected)
                response = worker.handle('GET', '/ltds/worker/v1/jobs/job-1/acceptance',
                    {'Authorization':'Bearer synthetic-worker-token', 'X-Claim-Token':'claim'},
                    protected_transport=True)
                self.assertEqual((response.status, response.body['error']), expected)
        finally:
            first.rollback(); first.close(); second.close()


if __name__ == '__main__': unittest.main()
