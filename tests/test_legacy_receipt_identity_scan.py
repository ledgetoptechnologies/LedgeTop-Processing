import copy, json, sqlite3, unittest
from pathlib import Path
from processing.reference_copy_hold_integrity import legacy_reserved_ids

class LegacyIdentityScanTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:');self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE reference_acceptances(receipt_id TEXT,receipt_json TEXT)')
        self.fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['acceptance']
    def scan(self, table_id, payload):
        self.db.execute('DELETE FROM reference_acceptances')
        self.db.execute('INSERT INTO reference_acceptances VALUES(?,?)',(table_id,payload))
        return legacy_reserved_ids(self.db)
    def test_valid_closed_receipt_retains_both_identities(self):
        self.assertEqual(self.scan(self.fixture['receiptId'],json.dumps(self.fixture)),{self.fixture['receiptId'],self.fixture['storageReceiptId']})
    def test_duplicate_keys_and_mismatched_row_fail_closed(self):
        wire=json.dumps(self.fixture)
        duplicate=wire[:-1]+',"storageReceiptId":"hidden"}'
        with self.assertRaises(ValueError):self.scan(self.fixture['receiptId'],duplicate)
        with self.assertRaises(ValueError):self.scan('different-row-id',wire)
        ambiguous=copy.deepcopy(self.fixture);ambiguous['storageReceiptId']=ambiguous['receiptId']
        with self.assertRaises(ValueError):self.scan(self.fixture['receiptId'],json.dumps(ambiguous))
    def test_unknown_and_missing_fields_fail_closed(self):
        for key in ('unknown','schemaVersion'):
            bad=copy.deepcopy(self.fixture)
            if key=='unknown':bad[key]='unexpected'
            else:del bad[key]
            with self.subTest(key=key),self.assertRaises(ValueError):self.scan(self.fixture['receiptId'],json.dumps(bad))

if __name__=='__main__':unittest.main()
