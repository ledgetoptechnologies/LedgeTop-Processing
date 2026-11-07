"""Source guard tests use tiny synthetic modules; no cutover actions run."""
import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from ops import verified_cutover_entry as guard


class VerifiedControllerEntryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.source = root/'source'; (self.source/'ops').mkdir(parents=True)
        (self.source/'ops'/'__init__.py').write_bytes(b'')
        (self.source/'ops'/'enrolled_coordinator_driver.py').write_bytes(
            b'from pathlib import Path\ndef main(args):\n Path(args[0]).write_text("ran")\n return int(args[1]) if len(args)>1 else 0\n')
        self.archive = root/'source.tar'
        with tarfile.open(self.archive, 'w') as out:
            out.add(self.source, arcname='source')
        self.sha = hashlib.sha256(self.archive.read_bytes()).hexdigest()
        self.marker = root/'marker'
        self.args = ['--archive',str(self.archive),'--source',str(self.source),'--sha256',self.sha,
                     '--controller','coordinator','--',str(self.marker)]

    def test_exact_archive_and_tree_execute_verified_module(self):
        self.assertEqual(guard.main(self.args), 0)
        self.assertEqual(self.marker.read_text(), 'ran')

    def test_controller_exit_code_propagates(self):
        self.assertEqual(guard.main(self.args + ['7']), 7)
        self.assertEqual(self.marker.read_text(), 'ran')

    def test_changed_archive_or_source_and_extra_file_never_execute(self):
        for kind in ('archive', 'source', 'extra'):
            with self.subTest(kind=kind):
                original_archive = self.archive.read_bytes()
                original_source = (self.source/'ops'/'enrolled_coordinator_driver.py').read_bytes()
                if kind == 'archive': self.archive.write_bytes(original_archive + b'x')
                elif kind == 'source': (self.source/'ops'/'enrolled_coordinator_driver.py').write_bytes(b'changed')
                else: (self.source/'ops'/'extra.py').write_bytes(b'extra')
                with patch.object(guard.subprocess, 'run') as run:
                    self.assertEqual(guard.main(self.args), 2); run.assert_not_called()
                self.assertFalse(self.marker.exists())
                self.archive.write_bytes(original_archive)
                (self.source/'ops'/'enrolled_coordinator_driver.py').write_bytes(original_source)
                (self.source/'ops'/'extra.py').unlink(missing_ok=True)

    def test_unsafe_link_member_never_executes(self):
        with tarfile.open(self.archive, 'a') as out:
            member = tarfile.TarInfo('source/ops/link'); member.type = tarfile.SYMTYPE; member.linkname = '/etc/passwd'
            out.addfile(member)
        sha = hashlib.sha256(self.archive.read_bytes()).hexdigest()
        self.args[self.args.index('--sha256')+1] = sha
        with patch.object(guard.subprocess, 'run') as run:
            self.assertEqual(guard.main(self.args), 2); run.assert_not_called()

    def test_hardlink_traversal_and_duplicate_members_never_execute(self):
        original = self.archive.read_bytes()
        for kind in ('hardlink', 'traversal', 'duplicate'):
            with self.subTest(kind=kind):
                self.archive.write_bytes(original)
                with tarfile.open(self.archive, 'a') as out:
                    name = {'hardlink':'source/ops/link', 'traversal':'source/../outside',
                            'duplicate':'source/ops/__init__.py'}[kind]
                    member = tarfile.TarInfo(name)
                    if kind == 'hardlink':
                        member.type = tarfile.LNKTYPE; member.linkname = 'source/ops/__init__.py'
                    out.addfile(member, io.BytesIO(b''))
                self.args[self.args.index('--sha256')+1] = hashlib.sha256(self.archive.read_bytes()).hexdigest()
                with patch.object(guard.subprocess, 'run') as run:
                    self.assertEqual(guard.main(self.args), 2); run.assert_not_called()
                self.assertFalse(self.marker.exists())


if __name__ == '__main__': unittest.main()
