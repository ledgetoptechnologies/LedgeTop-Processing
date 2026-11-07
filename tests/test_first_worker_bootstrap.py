import hashlib
import io
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
import subprocess
import sys
import uuid
from unittest.mock import patch

from ops import first_worker_bootstrap
from ops.first_worker_bootstrap import read_pinned, unpack, validate_archive


def archive(*entries):
    result = io.BytesIO()
    with tarfile.open(fileobj=result, mode='w') as tar:
        for name, kind, content in entries:
            item = tarfile.TarInfo(name)
            if kind == 'link':
                item.type = tarfile.SYMTYPE
                item.linkname = 'outside'
                tar.addfile(item)
            else:
                item.size = len(content)
                tar.addfile(item, io.BytesIO(content))
    return result.getvalue()


class BootstrapArchiveTests(unittest.TestCase):
    def test_extracts_regular_file(self):
        data = archive(('ops/example.py', 'file', b'value'))
        with tempfile.TemporaryDirectory() as root:
            target = Path(root) / 'new'
            unpack(data, target)
            self.assertEqual((target / 'ops/example.py').read_bytes(), b'value')

    def test_rejects_escaping_links_duplicates_and_normalized_aliases(self):
        for data in (archive(('../bad', 'file', b'x')), archive(('/bad', 'file', b'x')),
                     archive(('ok', 'link', b'')), archive(('x', 'file', b'1'), ('x', 'file', b'2')),
                     archive(('a//b', 'file', b'x'))):
            with self.subTest(size=len(data)), self.assertRaises(ValueError):
                validate_archive(data)

    @unittest.skipUnless(os.name == 'posix', 'POSIX private-file permissions')
    def test_pinned_input_rejects_wrong_hash_owner_and_mode(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'input'; path.write_bytes(b'example'); path.chmod(0o600)
            good = hashlib.sha256(b'example').hexdigest()
            self.assertEqual(read_pinned(path, good, os.geteuid()), b'example')
            with self.assertRaises(ValueError): read_pinned(path, '0' * 64, os.geteuid())
            with self.assertRaises(ValueError): read_pinned(path, good, os.geteuid() + 1)
            path.chmod(0o644)
            with self.assertRaises(ValueError): read_pinned(path, good, os.geteuid())

    @unittest.skipUnless(os.name == 'posix', 'POSIX dirfd and FIFO behavior')
    def test_fifo_swap_between_stat_and_open_is_nonblocking_and_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'input'; path.write_bytes(b'example'); path.chmod(0o600)
            real_open = os.open
            def swap(name, flags, *args, **kwargs):
                if name == 'input' and 'dir_fd' in kwargs:
                    path.rename(Path(root) / 'original')
                    os.mkfifo(path, 0o600)
                    self.assertTrue(flags & os.O_NONBLOCK)
                return real_open(name, flags, *args, **kwargs)
            with patch('ops.first_worker_bootstrap.os.open', side_effect=swap):
                with self.assertRaises(ValueError):
                    read_pinned(path, hashlib.sha256(b'example').hexdigest(), os.geteuid())

    @unittest.skipUnless(os.name == 'posix', 'POSIX dirfd behavior')
    def test_swapped_parent_directory_is_detected(self):
        with tempfile.TemporaryDirectory() as root:
            parent = Path(root) / 'parent'; parent.mkdir()
            path = parent / 'input'; path.write_bytes(b'example'); path.chmod(0o600)
            real_open = os.open
            def swap(name, flags, *args, **kwargs):
                if name == 'input' and 'dir_fd' in kwargs:
                    parent.rename(Path(root) / 'old-parent'); parent.mkdir()
                return real_open(name, flags, *args, **kwargs)
            with patch('ops.first_worker_bootstrap.os.open', side_effect=swap):
                with self.assertRaisesRegex(ValueError, 'input_path_changed'):
                    read_pinned(path, hashlib.sha256(b'example').hexdigest(), os.geteuid())

    def test_member_limit_checked_during_iteration(self):
        data = archive(('one', 'file', b'1'), ('two', 'file', b'2'), ('three', 'file', b'3'))
        with patch('ops.first_worker_bootstrap.MAX_FILES', 2), \
                patch('tarfile.TarFile.getmembers', side_effect=AssertionError('unbounded getmembers')):
            with self.assertRaisesRegex(ValueError, 'archive_count_invalid'):
                validate_archive(data)

    @unittest.skipUnless(os.name == 'posix' and os.geteuid() == 0 and
                         os.environ.get('LTDS_BOOTSTRAP_ROOT_FIXTURE') == '1',
                         'isolated root container with disposable /opt required')
    def test_isolated_root_bundle_import_and_replay_refusal(self):
        def tar_bytes(files):
            data = io.BytesIO()
            with tarfile.open(fileobj=data, mode='w') as tar:
                for name, content, mode in files:
                    item = tarfile.TarInfo(name); item.mode = mode; item.size = len(content)
                    tar.addfile(item, io.BytesIO(content))
            return data.getvalue()
        with tempfile.TemporaryDirectory(prefix='ltds-root-bootstrap-') as temp:
            root = Path(temp); root.chmod(0o700)
            source = tar_bytes([('ops/__init__.py', b'', 0o644),
                                ('ops/enroll_reference_worker.py', b'MARKER = 42\n', 0o644)])
            executable = Path(sys.executable).read_bytes()
            deps = tar_bytes([('bin/python', executable, 0o755),
                              ('pyvenv.cfg', b'home = /usr/local/bin\ninclude-system-site-packages = false\n', 0o644)])
            inputs = {}
            for name, contents in (('source', source), ('dependencies', deps), ('manifest', b'{}')):
                path = root / name; path.write_bytes(contents); path.chmod(0o600); os.chown(path, 1000, 1000)
                inputs[name] = (path, hashlib.sha256(contents).hexdigest())
            target = Path('/opt') / ('ltds-bootstrap-test-' + uuid.uuid4().hex)
            command = [sys.executable, '-I', '-B', str(Path(first_worker_bootstrap.__file__)),
                       '--target', str(target), '--owner', '1000']
            for name, (path, sha) in inputs.items():
                command.extend(['--' + name, str(path), '--' + name + '-sha256', sha])
            result = subprocess.run(command, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(target.stat().st_uid, 0)
            self.assertEqual(target.stat().st_mode & 0o777, 0o700)
            self.assertEqual((target / 'manifest.json').stat().st_mode & 0o777, 0o600)
            for path in (target / 'source/ops/enroll_reference_worker.py', target / 'dependencies/bin/python'):
                self.assertEqual(path.stat().st_uid, 0)
                self.assertEqual(path.stat().st_mode & 0o077, 0)
            imported = subprocess.run([str(target / 'dependencies/bin/python'), '-I', '-B', '-c',
                'import sys; sys.path.insert(0, sys.argv[1]); from ops.enroll_reference_worker import MARKER; print(MARKER)',
                str(target / 'source')], capture_output=True, text=True, timeout=15)
            self.assertEqual((imported.returncode, imported.stdout.strip()), (0, '42'), imported.stderr)
            replay = subprocess.run(command, capture_output=True, text=True, timeout=15)
            self.assertNotEqual(replay.returncode, 0)
            self.assertIn('new_root_target_required', replay.stderr)

    @unittest.skipUnless(os.name == 'posix' and os.geteuid() == 0 and
                         os.environ.get('LTDS_BOOTSTRAP_ROOT_FIXTURE') == '1',
                         'isolated root container with disposable /opt required')
    def test_bootstrap_then_protected_installer_prepare_and_inspect(self):
        """Run the real installer from bootstrap output against its fake host boundary."""
        repository = Path(first_worker_bootstrap.__file__).resolve().parent.parent
        source = io.BytesIO()
        with tarfile.open(fileobj=source, mode='w') as tar:
            for name in ('ops', 'processing', 'tests'):
                tar.add(repository / name, arcname=name)
        dependencies = io.BytesIO()
        with tarfile.open(fileobj=dependencies, mode='w') as tar:
            for name, contents, mode in (
                    ('bin/python', Path(sys.executable).read_bytes(), 0o755),
                    ('pyvenv.cfg', b'home = /usr/local/bin\ninclude-system-site-packages = false\n', 0o644)):
                item = tarfile.TarInfo(name); item.mode = mode; item.size = len(contents)
                tar.addfile(item, io.BytesIO(contents))
        with tempfile.TemporaryDirectory(prefix='ltds-integrated-bootstrap-') as temp:
            root = Path(temp); root.chmod(0o700)
            inputs = {}
            for name, raw in (('source', source.getvalue()), ('dependencies', dependencies.getvalue()),
                              ('manifest', b'{}')):
                path = root / name; path.write_bytes(raw); path.chmod(0o600); os.chown(path, 1000, 1000)
                inputs[name] = (path, hashlib.sha256(raw).hexdigest())
            target = Path('/opt') / ('ltds-integrated-' + uuid.uuid4().hex)
            command = [sys.executable, '-I', '-B', str(Path(first_worker_bootstrap.__file__)),
                       '--target', str(target), '--owner', '1000']
            for name, (path, sha) in inputs.items():
                command.extend(['--' + name, str(path), '--' + name + '-sha256', sha])
            prepared = subprocess.run(command, capture_output=True, text=True, timeout=40)
            self.assertEqual(prepared.returncode, 0, prepared.stderr)
            script = r'''
import sys
sys.path.insert(0, sys.argv[1])
sys.path.append(sys.argv[2])
from tests.test_enroll_reference_worker import FirstWorkerEnrollmentTests
fixture = FirstWorkerEnrollmentTests(methodName='test_inspection_of_absent_installation_creates_nothing')
fixture.setUp()
try:
    prepared = fixture.installer.prepare()
    inspected = fixture.installer.inspect()
    assert prepared == inspected and prepared['phase'] == 'prepared'
    assert not prepared['serviceStartIssued'] and not prepared['serviceEnableIssued']
    assert not any(x in fixture.boundary.calls for x in ('start', 'stop', 'enable', 'update'))
    print('protected installer synthetic prepare/inspect passed')
finally:
    fixture.doCleanups()
'''
            site_packages = next(p for p in sys.path if p.endswith('/site-packages'))
            run = subprocess.run([str(target / 'dependencies/bin/python'), '-I', '-B', '-c', script,
                                  str(target / 'source'), site_packages], capture_output=True, text=True, timeout=30)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertIn('protected installer synthetic prepare/inspect passed', run.stdout)
