"""Test-only directory retention; controlled child handles, no workers started."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock,patch

from tests.fixtures.retained_qualification_artifacts import RetainedQualificationArtifacts


@unittest.skipUnless(os.name=='posix','original directory descriptor cleanup')
class FixtureRetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.parent=Path(self.temp.name);self.parent.chmod(0o700)

    def retired(self):
        return [dict(process=Mock(pid=100+index,poll=Mock(return_value=0)),startTime=str(500+index)) for index in (0,1)]

    def confirm(self,value):
        with patch('ops.qualify_enrolled_worker_preparation.process_identity',side_effect=FileNotFoundError):
            value.confirm_children_retired(self.retired())

    def test_success_cleanup_only_after_complete_confirmed_child_retirement(self):
        with RetainedQualificationArtifacts(self.parent) as value:
            root=value.root;(root/'proof.txt').write_bytes(b'owned evidence')
            self.confirm(value)
        self.assertFalse(root.exists());self.assertTrue((self.parent/(root.name+'.cleanup-complete.json')).exists())

    def test_uncertain_shutdown_preserves_original_directory_and_bytes(self):
        with self.assertRaisesRegex(RuntimeError,'factory_worker_shutdown_uncertain'):
            with RetainedQualificationArtifacts(self.parent) as value:
                root=value.root;(root/'private-state').write_bytes(b'unchanged')
                raise RuntimeError('factory_worker_shutdown_uncertain')
        self.assertEqual((root/'private-state').read_bytes(),b'unchanged')
        self.assertTrue((self.parent/(root.name+'.retained.json')).exists())
        self.assertFalse((self.parent/(root.name+'.cleanup-started.json')).exists())

    def test_missing_or_live_child_closure_cannot_enable_cleanup(self):
        with self.assertRaisesRegex(RuntimeError,'actual_fixture_child_closure_required'):
            with RetainedQualificationArtifacts(self.parent) as value:
                root=value.root;(root/'state').write_bytes(b'held')
        self.assertEqual((root/'state').read_bytes(),b'held')
        with self.assertRaisesRegex(RuntimeError,'actual_fixture_child_still_present'):
            with RetainedQualificationArtifacts(self.parent) as value:
                second=value.root;(second/'state').write_bytes(b'also held')
                with patch('ops.qualify_enrolled_worker_preparation.process_identity',return_value='500'):
                    value.confirm_children_retired(self.retired())
        self.assertEqual((second/'state').read_bytes(),b'also held')

    def test_symlink_or_replaced_root_refuses_before_deleting_any_owned_state(self):
        external=self.parent/'external';external.write_bytes(b'not a cleanup target')
        with self.assertRaisesRegex(RuntimeError,'original_regular_fixture_artifact_required'):
            with RetainedQualificationArtifacts(self.parent) as value:
                root=value.root;(root/'owned').write_bytes(b'keep');(root/'link').symlink_to(external)
                self.confirm(value)
        self.assertEqual((root/'owned').read_bytes(),b'keep');self.assertEqual(external.read_bytes(),b'not a cleanup target')
        with self.assertRaisesRegex(RuntimeError,'owned_fixture_artifact_directory_changed'):
            with RetainedQualificationArtifacts(self.parent) as value:
                root=value.root;(root/'state').write_bytes(b'original');self.confirm(value)
                moved=self.parent/(root.name+'-moved');root.rename(moved);root.mkdir(mode=0o700);(root/'replacement').write_bytes(b'unrelated')
        self.assertEqual((moved/'state').read_bytes(),b'original');self.assertEqual((root/'replacement').read_bytes(),b'unrelated')


if __name__=='__main__':unittest.main()
