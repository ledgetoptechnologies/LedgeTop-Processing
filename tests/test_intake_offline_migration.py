import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from processing import intake_offline_migration as migration

class IntakeOfflineMigrationTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='intake-offline-test-'))
        self.addCleanup(lambda: print('retained synthetic migration root:',self.root,flush=True))
        self.db=sqlite3.connect(self.root/'synthetic.db')
        self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE viewer_tasks(uuid TEXT PRIMARY KEY, generation INTEGER, removed INTEGER, directory TEXT, environment TEXT, stage TEXT, job_id TEXT)')
        self.db.execute('CREATE TABLE viewer_files(uuid TEXT, name_key TEXT, path TEXT, PRIMARY KEY(uuid,name_key))')
        self.db.execute('CREATE TABLE viewer_incarnations(uuid TEXT, generation INTEGER)')
        self.db.commit()
    def install(self):
        with patch.object(migration,'QUALIFIED',True):
            migration.install_empty_offline(self.db)
            migration.authorize_connection(self.db)
    def test_default_disabled_and_nonempty_refused_atomically(self):
        with self.assertRaises(migration.MigrationUnavailable):migration.install_empty_offline(self.db)
        self.db.execute("INSERT INTO viewer_tasks VALUES('task',1,0,'dir','production','uploading',NULL)");self.db.commit()
        with patch.object(migration,'QUALIFIED',True):
            with self.assertRaises(migration.MigrationUnavailable):migration.install_empty_offline(self.db)
        self.assertIsNone(migration._schema(self.db,'table','viewer_intake_journal_v1'))
    def test_legacy_connection_rejected_and_trigger_tamper_detected(self):
        self.install()
        self.db.execute("INSERT INTO viewer_tasks VALUES('task',1,0,'dir','production','uploading',NULL)");self.db.commit()
        legacy=sqlite3.connect(self.root/'synthetic.db');self.addCleanup(legacy.close)
        for sql in ("UPDATE viewer_tasks SET generation=2", "DELETE FROM viewer_tasks", "INSERT INTO viewer_files VALUES('task','a','input')"):
            with self.assertRaises(sqlite3.OperationalError):legacy.execute(sql)
        legacy.close()
        self.db.execute('DROP TRIGGER viewer_intake_guard_viewer_tasks_update_v1')
        with self.assertRaises(migration.MigrationUnavailable):migration.attest(self.db)
    def test_cross_name_collisions_held_with_recursive_triggers_both_modes(self):
        self.install()
        record=('op','task',1,'photo','directory','temp','final',1,None,'prepared')
        self.db.execute('INSERT INTO viewer_intake_journal_v1 VALUES(?,?,?,?,?,?,?,?,?,?)',record)
        for recursive in (0,1):
            self.db.execute('PRAGMA recursive_triggers='+str(recursive))
            for temporary,final in (('temp','different'),('final','different'),('different','temp'),('different','final')):
                with self.subTest(recursive=recursive,temporary=temporary,final=final):
                    with self.assertRaises(sqlite3.IntegrityError):
                        self.db.execute('INSERT OR REPLACE INTO viewer_intake_journal_v1 VALUES(?,?,?,?,?,?,?,?,?,?)',
                            ('other','task',1,'other','directory',temporary,final,1,None,'prepared'))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM viewer_intake_journal_v1').fetchone()[0],1)

    def test_ten_thousand_actual_guarded_journal_inserts(self):
        import time
        self.install();start=time.monotonic()
        self.db.executemany('INSERT INTO viewer_intake_journal_v1 VALUES(?,?,?,?,?,?,?,?,?,?)',
            ((str(n),'task',1,str(n),'directory','temp'+str(n),'final'+str(n),1,None,'prepared') for n in range(10000)))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM viewer_intake_journal_v1').fetchone()[0],10000)
        print('synthetic 10000 guarded journal inserts seconds:',round(time.monotonic()-start,4),flush=True)

    def test_only_ordered_immutable_journal_transitions_and_no_replace(self):
        self.install()
        record=('op','task',1,'photo','directory','temp','final',1,None,'prepared')
        self.db.execute('INSERT INTO viewer_intake_journal_v1 VALUES(?,?,?,?,?,?,?,?,?,?)',record)
        self.db.execute('PRAGMA recursive_triggers=OFF')
        with self.assertRaises(sqlite3.IntegrityError):self.db.execute('INSERT OR REPLACE INTO viewer_intake_journal_v1 VALUES(?,?,?,?,?,?,?,?,?,?)',record)
        with self.assertRaises(sqlite3.IntegrityError):self.db.execute("UPDATE viewer_intake_journal_v1 SET state='committed'")
        self.db.execute("UPDATE viewer_intake_journal_v1 SET state='sealed',sha256=?",('a'*64,))
        with self.assertRaises(sqlite3.IntegrityError):self.db.execute("UPDATE viewer_intake_journal_v1 SET state='cleanup',final='other'")
        self.db.execute("UPDATE viewer_intake_journal_v1 SET state='cleanup'")
        self.db.execute("UPDATE viewer_intake_journal_v1 SET state='committed'")
        with self.assertRaises(sqlite3.IntegrityError):self.db.execute('DELETE FROM viewer_intake_journal_v1')

if __name__=='__main__':unittest.main()
