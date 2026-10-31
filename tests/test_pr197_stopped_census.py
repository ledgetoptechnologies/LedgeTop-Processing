import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from ops.pr197_stopped_census import stopped_census,CensusHeld,SCHEMAS,DEFAULT_LIMITS
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore


@unittest.skipUnless(os.name=='posix' and hasattr(os,'O_DIRECTORY'),'Linux census required')
class StoppedCensusTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        queue=Queue(self.root,node_ids=('cluster','main'),viewer_enabled=True)
        TaskStore(queue,self.root/'viewer-tasks')
        with queue._db() as db:
            db.execute('INSERT INTO owned_maintenance_schedules VALUES(?,?,?,?,?,?)',
                       ('synthetic','cluster','a','b','{}','{}'))
        queue.close()

    def inspect(self):
        # Unit fixture only: actual read-only mount enforcement is covered by
        # the isolated Docker rehearsal, never by this mock.
        with patch('ops.pr197_stopped_census._mount'):
            return stopped_census(self.root,bytes(range(32)))

    def test_versioned_schema_and_nonempty_schedule(self):
        self.assertTrue(all(len(item)==64 for item in SCHEMAS))
        result=self.inspect()
        self.assertIn(result['schemaSha256'],SCHEMAS)
        self.assertEqual(result['census']['database']['rowCounts']['owned_maintenance_schedules'],1)
        self.assertFalse(result['cutoverAuthorized']);self.assertFalse(result['rollbackAuthorized'])

    def test_unknown_schema_and_sidecar_hold(self):
        db=sqlite3.connect(self.root/'jobs.sqlite3');db.execute('CREATE TABLE unexpected(x)');db.commit();db.close()
        with self.assertRaisesRegex(CensusHeld,'unqualified_predecessor_schema'):self.inspect()
        (self.root/'jobs.sqlite3-wal').touch()
        with self.assertRaisesRegex(CensusHeld,'sqlite_sidecar_requires_checkpoint'):self.inspect()

    def test_full_reference_schema_profile(self):
        from processing.reference_store import ReferenceStore
        from processing.reference_delivery import DeliveryReceiver
        from processing.reference_failure import FailureReceiver
        from processing.reference_terminal import TerminalService
        queue=Queue(self.root,node_ids=('cluster','main'),viewer_enabled=True,reference_enabled=True)
        store=ReferenceStore(queue);DeliveryReceiver(store);FailureReceiver(store);TerminalService(store);queue.close()
        result=self.inspect()
        self.assertEqual(result['schemaSha256'],'249bfba270623180a562922e921a7b95f28c836b833837cc546d7322c38a16d6')

    def test_file_change_during_census_holds(self):
        photo=self.root/'viewer-tasks'/'synthetic';photo.write_bytes(b'abc')
        from ops import state_census
        original=state_census._content;changed=[]
        def mutate(key,path,expected):
            if path==photo and not changed:
                changed.append(True);path.write_bytes(b'changed')
            return original(key,path,expected)
        with patch('ops.state_census._content',side_effect=mutate):
            with self.assertRaises(CensusHeld):self.inspect()

    def test_comparison_detects_schedule_and_file_changes(self):
        from ops.state_census import compare
        photo=self.root/'viewer-tasks'/'synthetic';photo.write_bytes(b'abc')
        before=self.inspect()['census']
        db=sqlite3.connect(self.root/'jobs.sqlite3')
        db.execute("UPDATE owned_maintenance_schedules SET receipt_json='changed'");db.commit();db.close()
        photo.write_bytes(b'def')
        after=self.inspect()['census']
        result=compare(before,after,bytes(range(32)),allow_paths=('jobs.sqlite3:mtime,bytes,content',))
        self.assertFalse(result['identical'])
        self.assertTrue(result['database']);self.assertTrue(result['files'])

    def test_caps_refuse_without_removing_source(self):
        photo=self.root/'viewer-tasks'/'synthetic';photo.write_bytes(b'abc')
        for name,value in (('file_bytes',2),('total_bytes',2),('database_bytes',2),('rows',1),('entries',1)):
            limits=DEFAULT_LIMITS.copy();limits[name]=value
            with self.subTest(name=name),patch('ops.pr197_stopped_census._mount'):
                with self.assertRaises(CensusHeld):stopped_census(self.root,bytes(range(32)),limits=limits)
                self.assertEqual(photo.read_bytes(),b'abc')
        invalid=DEFAULT_LIMITS.copy();invalid['file_bytes']=True
        with self.assertRaisesRegex(CensusHeld,'bounded_census_limits_required'):
            stopped_census(self.root,bytes(range(32)),limits=invalid)


if __name__=='__main__':unittest.main()
