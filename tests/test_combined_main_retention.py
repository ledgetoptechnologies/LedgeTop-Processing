"""Source/phase regressions; native mechanics require separate actual proof."""
from contextlib import contextmanager
from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

from ops import combined_main_retention as combined
from ops import retained_main_engine_swap as swap


class CombinedSourceTests(unittest.TestCase):
    def test_mount_fingerprint_normalizes_only_array_order(self):
        mounts = [dict(Type='bind', Source='/proc', Destination='/host/proc', RW=False),
                  dict(Type='bind', Source='/sys', Destination='/host/sys', RW=False)]
        expected = combined.mounts_fingerprint(mounts)
        self.assertEqual(combined.mounts_fingerprint(list(reversed(mounts))), expected)
        self.assertEqual(mounts[0]['Source'], '/proc')  # Input is not mutated.
        changes = [dict(mounts[0], RW=True), dict(mounts[0], Source='/elsewhere'),
                   dict(mounts[0], FutureInspectField='must-not-be-ignored')]
        for changed in changes:
            with self.subTest(changed=changed):
                self.assertNotEqual(combined.mounts_fingerprint([changed, mounts[1]]), expected)
        self.assertNotEqual(combined.mounts_fingerprint(mounts + [mounts[0]]), expected)
        self.assertNotEqual(combined.mounts_fingerprint(mounts[:1]), expected)

    def test_mount_fingerprint_refuses_non_array_or_non_object_rows(self):
        for mounts in ({}, (), [None], ['mount']):
            with self.subTest(mounts=mounts):
                with self.assertRaises(swap.Held):
                    combined.mounts_fingerprint(mounts)

    def test_cli_default_is_static_plan_without_runtime_or_source_reads(self):
        output = io.StringIO()
        with patch.object(combined, 'CombinedRuntime') as runtime, patch.object(
                combined, 'source_inventory', side_effect=AssertionError('no source scan in default plan')), redirect_stdout(output):
            self.assertEqual(combined.main([]), 0)
        runtime.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())['mutations'], 0)

    def test_cli_changed_preparation_refuses_before_runtime_and_redacts_paths(self):
        output = io.StringIO()
        with patch.object(combined, 'private_json', return_value={'sourceHashes': {}}), patch.object(
                combined, 'CombinedRuntime') as runtime, redirect_stdout(output):
            result = combined.main(['--stage', 'begin', '--receipt', '/private-test-path',
                '--credentials', '/do-not-echo-this-path', '--prepared-sha256', 'changed'])
        self.assertEqual(result, 2)
        runtime.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())['status'], 'held')
        self.assertNotIn('private-test-path', output.getvalue())
        self.assertNotIn('do-not-echo-this-path', output.getvalue())

    def test_normal_helper_still_refuses_nonempty_tmp(self):
        runtime = object.__new__(swap.Runtime)
        runtime.engine_state = lambda command: {}
        def docker(*args, **kwargs):
            if args[0] == 'top': return 'PID ARGS\n123 node /var/www/index.js\n'
            if args[:2] == ('exec', swap.ENGINE): return '.gitignore\n' + combined.OLD_UPLOAD + '\n'
            raise AssertionError('unexpected command')
        runtime.docker = docker
        with self.assertRaises(swap.Held): runtime.engine_idle(swap.ENGINE, None)

    def test_inactive_identity_requires_failed_original_job_and_absent_engine(self):
        authority = str(uuid.uuid4())
        job = dict(authorityId=authority, status='failed', requestId=combined.OLD_NATIVE, nodeId='main')
        runtime = object.__new__(combined.CombinedRuntime)
        runtime.api = SimpleNamespace(json=lambda *args: {'job': job})
        runtime.authority = lambda: authority
        self.assertEqual(runtime.inactive_identity({'tasks': {}})['engineTaskId'], combined.OLD_UPLOAD)
        job['status'] = 'running'
        with self.assertRaises(swap.Held): runtime.inactive_identity({'tasks': {}})
        job['status'] = 'failed'
        with self.assertRaises(swap.Held): runtime.inactive_identity({'tasks': {combined.OLD_UPLOAD: 40}})

    def test_todd_and_unknown_cache_tasks_refused_before_binding(self):
        operation = object.__new__(combined.Operation)
        for task in ('753963bd-0719-484f-85b2-2062dd0a97be', str(uuid.uuid4())):
            with self.subTest(task=task):
                with self.assertRaises(swap.Held): operation.bind_cleanup({'engineTaskId': task}, {})
        self.assertEqual(len(combined.ALLOWED_CACHE_TASKS), 2)

    def test_pdf_is_part_of_required_cleanup_source_binding(self):
        self.assertIn('processing/intermediate_pdf.py', combined.CLEANUP_FILES)

    def test_independent_probe_refuses_original_attributes_before_copy(self):
        nonce = str(uuid.uuid4())
        row = dict(uid=1235, gid=1236, mode=0o755, type='directory', xattrs={'user.sentinel': 'a' * 64})
        frame = dict(schemaVersion=1, probeNonce=nonce, pid=123, inventory={'.': row})
        with self.assertRaisesRegex(swap.Held, 'original_tmp_profile'):
            combined.validate_tmp_probe(frame, nonce)

    def test_nonroot_original_is_explicitly_outside_qualified_profile(self):
        nonce = str(uuid.uuid4())
        row = dict(uid=1235, gid=1236, mode=0o750, type='directory', xattrs={})
        frame = dict(schemaVersion=1, probeNonce=nonce, pid=123, inventory={'.': row})
        with self.assertRaisesRegex(swap.Held, 'root0_noattrs'):
            combined.validate_tmp_probe(frame, nonce)

    def test_root0_noattrs_profile_retains_actual_numeric_mode(self):
        nonce = str(uuid.uuid4())
        row = dict(uid=0, gid=0, mode=0o750, type='directory', xattrs={})
        frame = dict(schemaVersion=1, probeNonce=nonce, pid=123, inventory={'.': row})
        self.assertEqual(combined.validate_tmp_probe(frame, nonce)['.'], row)

    def test_unknown_reader_blocks_stop_or_cleanup_before_authority_call(self):
        operation = object.__new__(combined.Operation)
        operation.r = SimpleNamespace(authority=Mock())
        state = dict(requestId=str(uuid.uuid4()), ownedExecReader=dict(exitVerified=False, disposition='UNKNOWN'))
        with self.assertRaisesRegex(swap.Held, 'reader_unreconciled'):
            operation.own_updating(state)
        operation.r.authority.assert_not_called()

    @unittest.skipUnless(os.name == 'posix' and hasattr(os, 'pidfd_open'), 'Linux tracked-reader protocol')
    def test_reader_identity_durable_before_hash_approval_and_exit_verified(self):
        nonce, cid, identifier = str(uuid.uuid4()), 'c' * 64, 'e' * 64
        records, command = [], []
        def checkpoint(record): records.append(json.loads(json.dumps(record)))
        class Approval(io.BytesIO):
            def write(inner, raw):
                self.assertEqual(records[-1]['stage'], 'approval_intent')
                self.assertEqual(records[-1]['execId'], identifier)
                return super().write(raw)
        process = SimpleNamespace(pid=90000, stdin=Approval(), stdout=io.BytesIO(), returncode=0,
                                  wait=lambda **kwargs: 0, poll=lambda: 0)
        def spawn(args, **kwargs):
            command.extend(args)
            return process
        running = dict(ID=identifier, ContainerID=cid, Running=True, Pid=777, OpenStdin=True,
            ProcessConfig=dict(entrypoint='python3', arguments=[], privileged=False))
        calls = []
        def inspect(identifier):
            calls.append(identifier)
            if len(calls) == 3: return dict(ID=identifier, Running=False, ExitCode=0)
            return dict(running, ProcessConfig=dict(running['ProcessConfig'], arguments=command[-4:]))
        hello = dict(schemaVersion=1, phase='ready', probeNonce=nonce, pid=11, startTicks=22)
        final = dict(schemaVersion=1, phase='complete', probeNonce=nonce, pid=11,
            inventory={'.': dict(uid=0, gid=0, mode=0o755, type='directory', xattrs={})})
        runtime = SimpleNamespace(checkpoint_exec_reader=checkpoint,
            inspect=lambda _: dict(Id=cid, State=dict(Running=True, Pid=555), ExecIDs=[identifier]))
        with patch.object(combined.subprocess, 'Popen', spawn), patch.object(combined, 'read_probe_line', side_effect=[hello, final]), patch.object(
                combined, 'exec_inspect', inspect), patch.object(combined, 'verify_exec_identity', return_value=777), patch.object(
                combined.os, 'pidfd_open', side_effect=lambda pid: os.open('/dev/null', os.O_RDONLY)), patch.object(
                combined, 'process_start_ticks', return_value=99):
            value = combined.inventory_inside_container(runtime, cid, nonce)
        stages = [r['stage'] for r in records]
        self.assertLess(stages.index('identity_sealed_unapproved'), stages.index('approval_intent'))
        self.assertLess(stages.index('approval_intent'), stages.index('hash_reader_running'))
        self.assertTrue(records[-1]['exitVerified'])
        self.assertEqual(records[-1]['disposition'], 'EXITED')
        self.assertEqual(value, final['inventory'])
        self.assertTrue(process.stdout.closed)

    def test_missing_exec_identity_never_sends_hash_approval_and_stays_unknown(self):
        nonce, cid, records, writes = str(uuid.uuid4()), 'c' * 64, [], []
        class Approval(io.BytesIO):
            def write(inner, raw):
                writes.append(raw)
                return super().write(raw)
        process = SimpleNamespace(pid=90000, stdin=Approval(), stdout=io.BytesIO(), returncode=0,
                                  wait=lambda **kwargs: 0, poll=lambda: 0)
        runtime = SimpleNamespace(checkpoint_exec_reader=lambda row: records.append(json.loads(json.dumps(row))),
            inspect=lambda _: dict(Id=cid, State=dict(Running=True, Pid=555), ExecIDs=[]))
        hello = dict(schemaVersion=1, phase='ready', probeNonce=nonce, pid=11, startTicks=22)
        with patch.object(combined.subprocess, 'Popen', return_value=process), patch.object(
                combined, 'read_probe_line', return_value=hello), patch.object(combined, 'process_start_ticks', return_value=99):
            with self.assertRaisesRegex(swap.Held, 'exec_identity_missing'):
                combined.inventory_inside_container(runtime, cid, nonce)
        self.assertEqual(writes, [])
        self.assertEqual(records[-1]['disposition'], 'UNKNOWN')
        self.assertFalse(records[-1]['exitVerified'])
        self.assertTrue(process.stdin.closed and process.stdout.closed)

    def test_broken_stdin_close_does_not_skip_owned_cli_cleanup_or_mask_failure(self):
        nonce, cid, records = str(uuid.uuid4()), 'c' * 64, []
        class BrokenInput:
            closed = False
            def close(inner): raise BrokenPipeError('injected close flush failure')
        stopped = []
        process = SimpleNamespace(pid=90000, stdin=BrokenInput(), stdout=io.BytesIO(), returncode=None,
            poll=lambda: process.returncode, terminate=lambda: stopped.append('terminate'),
            wait=lambda **kwargs: setattr(process, 'returncode', -15), kill=lambda: stopped.append('kill'))
        runtime = SimpleNamespace(checkpoint_exec_reader=lambda row: records.append(json.loads(json.dumps(row))),
            inspect=lambda _: dict(Id=cid, State=dict(Running=True, Pid=555), ExecIDs=[]))
        hello = dict(schemaVersion=1, phase='ready', probeNonce=nonce, pid=11, startTicks=22)
        with patch.object(combined.subprocess, 'Popen', return_value=process), patch.object(
                combined, 'read_probe_line', return_value=hello), patch.object(combined, 'process_start_ticks', return_value=99):
            with self.assertRaisesRegex(swap.Held, 'exec_identity_missing'):
                combined.inventory_inside_container(runtime, cid, nonce)
        self.assertEqual(stopped, ['terminate'])
        self.assertEqual(records[-1]['disposition'], 'UNKNOWN')
        self.assertFalse(records[-1]['exitVerified'])
        self.assertTrue(process.stdout.closed)

    @unittest.skipUnless(os.name == 'posix', 'canonical Linux operation root')
    def test_runtime_root_mismatch_refused_before_physical_state_or_mutation(self):
        request = str(uuid.uuid4())
        root = Path('/var/lib') / ('lt-main-combined-' + uuid.UUID(request).hex)
        runtime = SimpleNamespace(root=Path('/var/lib/lt-main-combined-' + uuid.uuid4().hex),
                                  snapshot=Mock(), docker=Mock(), operator=Mock())
        # This is a source-only binding test: no directory is created.
        with self.assertRaisesRegex(swap.Held, 'runtime_and_operation_root_must_match'):
            combined.Operation(runtime, root, {'requestId': request}, reviewed_prepared_sha256='unused')
        runtime.snapshot.assert_not_called()
        runtime.docker.assert_not_called()
        self.assertEqual(runtime.operator.mock_calls, [])

    def test_root_nonce_request_mismatch_refused_before_mutation(self):
        root = Path('/var/lib') / ('lt-main-combined-' + uuid.uuid4().hex)
        runtime = SimpleNamespace(root=root, snapshot=Mock(), docker=Mock(), operator=Mock())
        with self.assertRaises(swap.Held):
            combined.Operation(runtime, root, {'requestId': str(uuid.uuid4())},
                               reviewed_prepared_sha256='unused')
        runtime.snapshot.assert_not_called()
        runtime.docker.assert_not_called()
        self.assertEqual(runtime.operator.mock_calls, [])

    def test_create_only_qualification_cannot_prepare_live_operation(self):
        qualification = dict(schemaVersion=1, passed=True, completedGates=['native-compose-create'],
                             baseImage=swap.BASE, targetImage=swap.TARGET)
        runtime = SimpleNamespace(snapshot=Mock(side_effect=AssertionError('must refuse before live inspection')))
        with patch.object(combined, 'private_json', return_value=qualification):
            with self.assertRaises(swap.Held):
                combined.prepare_fresh(runtime, Path('/unused'), combined.digest(combined.canonical(qualification)), [])
        runtime.snapshot.assert_not_called()

    def test_direct_phase_method_requires_lock(self):
        operation = object.__new__(combined.Operation)
        operation._executing = False
        with self.assertRaises(swap.Held): operation.state('prepared')

    def test_phase_checks_durable_stage_under_lock_before_dispatch(self):
        operation = object.__new__(combined.Operation)
        operation.root = Path('/unused-private-operation')
        operation._executing = False
        operation.r = SimpleNamespace(hold_closed=Mock())
        locked = []
        @contextmanager
        def lock(root):
            locked.append(True)
            try: yield
            finally: locked.pop()
        def read(path):
            self.assertTrue(locked)
            return {'stage': 'prepared'}
        def begin():
            self.assertTrue(locked)
            self.assertTrue(operation._executing)
            return 'one-owned-phase'
        operation.begin_and_preserve = begin
        with patch.object(swap, 'operation_lock', lock), patch.object(combined, 'private_json', read):
            self.assertEqual(operation.run_phase('begin'), 'one-owned-phase')
            with self.assertRaises(swap.Held): operation.run_phase('finish')
        operation.r.hold_closed.assert_not_called()
        self.assertFalse(operation._executing)


@unittest.skipUnless(os.name == 'posix', 'Linux metadata/stream source qualification')
class TmpPreservationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.before = self.root / 'tmp.before'
        self.before.mkdir()
        (self.before / 'sentinel.bin').write_bytes(b'old-upload-retained')
        self.inventory = combined.inventory_directory(self.before)
        self.identity = {'engineTaskId': combined.OLD_UPLOAD}
        self.seal = {'identity': self.identity, 'inventory': self.inventory}
        self.runtime = object.__new__(combined.CombinedRuntime)
        self.runtime.inactive_identity = lambda engine: self.identity
        self.runtime.docker = Mock(side_effect=AssertionError('no gratuitous copy'))

    def test_stopped_backup_reuses_sealed_copy_and_streams_source(self):
        with patch.object(combined, 'private_json', return_value=self.seal), patch.object(
                combined, 'inventory_container_tmp', return_value=self.inventory) as reader:
            self.runtime.backup_tmp({'engine': {}}, self.root)
            reader.assert_called_once_with(self.runtime, swap.ENGINE)
        self.runtime.docker.assert_not_called()
        self.assertFalse((self.root / 'tmp.after').exists())

    def test_mutated_preservation_copy_refused(self):
        (self.before / 'sentinel.bin').write_bytes(b'changed')
        with patch.object(combined, 'private_json', return_value=self.seal):
            with self.assertRaises(swap.Held): self.runtime.backup_tmp({'engine': {}}, self.root)
        self.runtime.docker.assert_not_called()

    def test_copy_and_archive_agree_but_disagree_with_original_uid_is_refused(self):
        original = json.loads(json.dumps(self.inventory))
        original['sentinel.bin']['uid'] += 1
        def read(path):
            return {'inventory': original} if path.name == 'inactive-tmp-source.private.json' else self.seal
        with patch.object(combined, 'private_json', read), patch.object(
                combined, 'inventory_container_tmp', return_value=self.inventory) as stream:
            with self.assertRaises(swap.Held): self.runtime.backup_tmp({'engine': {}}, self.root)
            stream.assert_not_called()
        self.runtime.docker.assert_not_called()

    def test_candidate_verification_uses_stream_without_tmp_after(self):
        with patch.object(combined, 'private_json', return_value=self.seal), patch.object(
                combined, 'inventory_container_tmp', return_value=self.inventory):
            self.runtime.verify_restored_tmp({}, self.root, 'synthetic-candidate')
        self.runtime.docker.assert_not_called()
        self.assertFalse((self.root / 'tmp.after').exists())

    @unittest.skipUnless(os.name == 'posix', 'Linux xattr refusal')
    def test_unqualified_tmp_xattr_refused(self):
        os.setxattr(self.before / 'sentinel.bin', 'user.lt_tmp_test', b'preserve-me')
        with self.assertRaises(swap.Held): combined.inventory_directory(self.before)

    @unittest.skipUnless(os.name == 'posix', 'Linux pipe/select streaming')
    def test_actual_tar_parser_matches_owned_inventory_without_extraction(self):
        content = io.BytesIO()
        with tarfile.open(fileobj=content, mode='w') as archive:
            for name, row in self.inventory.items():
                item = tarfile.TarInfo('tmp' if name == '.' else 'tmp/' + name)
                item.uid, item.gid, item.mode = row['uid'], row['gid'], row['mode']
                if row['type'] == 'directory':
                    item.type = tarfile.DIRTYPE
                    archive.addfile(item)
                else:
                    raw = (self.before / name).read_bytes()
                    item.size = len(raw)
                    archive.addfile(item, io.BytesIO(raw))
        archive_path = self.root / 'synthetic-stream.tar'
        archive_path.write_bytes(content.getvalue())
        # A regular synthetic file avoids blocking a producer on a small pipe
        # buffer. The actual Docker pipe is covered by the native fixture.
        stream = archive_path.open('rb', buffering=0)
        process = SimpleNamespace(stdout=stream, pid=999999, returncode=0,
                                  wait=lambda **kwargs: 0, poll=lambda: 0)
        runtime = SimpleNamespace(checkpoint_child=Mock())
        with patch.object(combined.subprocess, 'Popen', return_value=process):
            result = combined.inventory_container_tmp(runtime, 'synthetic-cid')
        self.assertEqual(result, self.inventory)
        self.assertTrue(stream.closed)


if __name__ == '__main__':
    unittest.main()
