import sqlite3
import unittest

from ops.prepared_update_census_candidate import (DDL, PreparedCensusHeld, census_prepared,
    require_predecessor_rollback_proof, verify_candidate_additions)


class PreparedUpdateCensusTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.execute('BEGIN')

    def create(self):
        for statement in DDL:self.db.execute(statement)
        self.db.execute('INSERT INTO reference_viewer_activation_schema VALUES(1,1)')

    def test_empty_exact_addition_and_rollback_hold(self):
        before=census_prepared(self.db);self.create();after=census_prepared(self.db)
        verify_candidate_additions(before,after)
        require_predecessor_rollback_proof(before)
        with self.assertRaises(PreparedCensusHeld):require_predecessor_rollback_proof(after)
        with self.assertRaises(PreparedCensusHeld):verify_candidate_additions(after,before)

    def test_partial_and_malformed_schema_hold(self):
        self.db.execute(DDL[0])
        with self.assertRaises(PreparedCensusHeld):census_prepared(self.db)
        self.db.execute('DROP TABLE reference_viewer_activation')
        self.create()
        self.db.execute('ALTER TABLE reference_viewer_v3_policy ADD COLUMN foreign_field TEXT')
        with self.assertRaises(PreparedCensusHeld):census_prepared(self.db)

    def test_extra_trigger_or_nonempty_rows_hold(self):
        self.create()
        self.db.execute("CREATE TRIGGER unexpected AFTER INSERT ON reference_viewer_v3_policy BEGIN SELECT 1; END")
        with self.assertRaises(PreparedCensusHeld):census_prepared(self.db)
        self.db.execute('DROP TRIGGER unexpected')
        self.db.execute("INSERT INTO reference_viewer_v3_policy VALUES('synthetic','{}','bad')")
        with self.assertRaises(PreparedCensusHeld):census_prepared(self.db)

    def test_marker_missing_or_malformed_holds(self):
        self.create();self.db.execute('DELETE FROM reference_viewer_activation_schema')
        with self.assertRaises(PreparedCensusHeld):census_prepared(self.db)

    def test_read_transaction_required(self):
        self.db.commit()
        with self.assertRaises(PreparedCensusHeld):census_prepared(self.db)


if __name__=='__main__':unittest.main()
