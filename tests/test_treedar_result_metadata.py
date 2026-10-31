import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from processing.treedar_engine import _read_result_metadata, TreeDarHeld, MAX_RESULT_METADATA_BYTES


@unittest.skipUnless(os.name=='posix' and hasattr(os,'O_NOFOLLOW'),'Linux output verification required')
class TreeDarMetadataTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.path=self.root/'summary.json'

    def test_normal_json_and_per_file_limit(self):
        self.path.write_bytes(b'{"n_points_used":3}')
        self.assertEqual(_read_result_metadata(self.path,100),{'n_points_used':3})
        with self.assertRaises(TreeDarHeld):_read_result_metadata(self.path,4)

    def test_oversized_and_non_object_json_hold(self):
        self.path.write_bytes(b' '* (MAX_RESULT_METADATA_BYTES+1))
        with self.assertRaises(TreeDarHeld):_read_result_metadata(self.path,MAX_RESULT_METADATA_BYTES+10)
        self.path.write_bytes(b'[]')
        with self.assertRaises(TreeDarHeld):_read_result_metadata(self.path,100)

    def test_symlink_is_never_followed(self):
        other=self.root/'other';other.write_bytes(b'{}');self.path.symlink_to(other)
        with self.assertRaises((TreeDarHeld,OSError)):_read_result_metadata(self.path,100)

    def test_fifo_is_never_read(self):
        os.mkfifo(self.path)
        with self.assertRaises(TreeDarHeld):_read_result_metadata(self.path,100)

    def test_hardlink_holds(self):
        self.path.write_bytes(b'{}');os.link(self.path,self.root/'other')
        with self.assertRaises(TreeDarHeld):_read_result_metadata(self.path,100)

    def test_mutation_during_read_holds(self):
        self.path.write_bytes(b'{"x":1}')
        real_read=os.read; changed=[]
        def mutate(fd,count):
            data=real_read(fd,count)
            if data and not changed:
                changed.append(True);self.path.write_bytes(b'{"x":2}')
            return data
        with patch('processing.treedar_engine.os.read',side_effect=mutate):
            with self.assertRaises(TreeDarHeld):_read_result_metadata(self.path,100)


if __name__=='__main__':unittest.main()
