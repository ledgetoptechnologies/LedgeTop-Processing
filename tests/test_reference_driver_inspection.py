"""Background corruption remains visible without stopping unrelated work."""
from contextlib import contextmanager
import json
import sqlite3
import threading
import unittest
from unittest.mock import Mock

from processing.reference_driver import DriverHeld, ReferenceDriver


class DriverInspectionTests(unittest.TestCase):
    def test_inspection_is_visible_idle_and_alongside_foreground(self):
        driver=object.__new__(ReferenceDriver)
        driver._state_owner=None
        driver._lock=threading.Lock()
        driver._combined_policy_initial=None
        driver._background=Mock(side_effect=DriverHeld('stored_reference_inspection'))
        driver._step=Mock(side_effect=[{'state':'idle'},{'state':'processing','jobId':'synthetic'}])
        self.assertEqual(driver.step(),{'state':'held','reason':'stored_reference_inspection'})
        self.assertEqual(driver.step(),{'state':'processing','jobId':'synthetic',
            'background':{'state':'held','reason':'stored_reference_inspection'}})
        driver._background=Mock(side_effect=DriverHeld('reference_inspection_protocol_error'))
        driver._step=Mock(return_value={'state':'idle'})
        self.assertEqual(driver.step(),{'state':'held','reason':'reference_inspection_protocol_error'})

    def test_actual_background_relay_reports_job_and_preserves_checkpoint(self):
        db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row
        db.execute('CREATE TABLE driver_jobs(job TEXT,stage TEXT,data TEXT,background_checked INTEGER,created INTEGER)')
        db.execute('INSERT INTO driver_jobs VALUES(?,?,?,?,?)',('job-1','awaiting_acceptance',
            json.dumps({'claim':{'claimToken':'synthetic-secret'}}),0,1));db.commit()
        driver=object.__new__(ReferenceDriver)
        @contextmanager
        def transaction():
            with db:yield db
        driver._db=transaction;driver._next_background_order=lambda _db:1
        driver._request=Mock(side_effect=DriverHeld('stored_reference_inspection'))
        driver.engine_cleanup=None;driver.cleanup_executor=None
        driver._state_owner=None;driver._lock=threading.Lock();driver._combined_policy_initial=None
        driver._step=Mock(return_value={'state':'idle'})
        try:
            result=driver.step()
            self.assertEqual(result,{'state':'held','reason':'stored_reference_inspection','jobId':'job-1'})
            self.assertNotIn('synthetic-secret',repr(result))
            self.assertEqual(db.execute('SELECT stage FROM driver_jobs').fetchone()[0],'awaiting_acceptance')
        finally:db.close()


if __name__ == '__main__': unittest.main()
