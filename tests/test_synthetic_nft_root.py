import os,tempfile,unittest
from pathlib import Path
from ops.synthetic_nft_root import checked_nft_root

class Tests(unittest.TestCase):
    def test_missing_relative_and_nonexistent_rejected(self):
        for value in ('','relative/path','/not-a-real-nft-fixture-path-6b68'):
            with self.subTest(value=value), self.assertRaises(ValueError): checked_nft_root(value)
    def test_valid_absolute_and_symlink_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            base=Path(directory).resolve(); real=base/'real'; real.mkdir()
            link=base/'link'; link.symlink_to(real,target_is_directory=True)
            self.assertEqual(checked_nft_root(str(real)),str(real))
            with self.assertRaises(ValueError): checked_nft_root(str(link))
            with self.assertRaises(ValueError): checked_nft_root(str(real)+'/.' )
            with self.assertRaises(ValueError): checked_nft_root(str(real)+'//' )

if __name__=='__main__': unittest.main()
