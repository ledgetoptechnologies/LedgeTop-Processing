"""Owned helper cleanup on timeouts; no Docker daemon required."""
import unittest
from ops.update_reference_coordinator import Stopped, run_owned_helper


class DockerFixture:
    def __init__(self, *, timeout=False, wrong_identity=False, remove_fails=False):
        self.timeout, self.wrong_identity, self.remove_fails = timeout, wrong_identity, remove_fails
        self.container = None
        self.removed = []

    def docker(self, *args, timeout=60):
        if args[0] == 'ps':
            return (self.container['Id'] + '\n') if self.container else ''
        if args[0] == 'create':
            name = args[args.index('--name') + 1]
            label = args[args.index('--label') + 1].split('=', 1)[1]
            self.container = {'Id': 'a' * 64, 'Name': '/' + name,
                              'Config': {'Labels': {'io.ledgetop.update-helper': label}},
                              'State': {'Running': False}}
            return self.container['Id']
        if args[0] == 'start':
            if self.timeout:
                raise RuntimeError('CLI timed out')
            self.container = None  # --rm helper exited before the client returned
            return 'ok'
        if args[0] == 'rm':
            if self.remove_fails:
                raise RuntimeError('daemon inaccessible')
            self.removed.append(args[-1]); self.container = None
            return ''
        raise AssertionError(args)

    def inspected(self, identifier, *, timeout=60):
        value = dict(self.container)
        if self.wrong_identity:
            value['Name'] = '/somebody-elses-helper'
        return value


class OwnedHelperTests(unittest.TestCase):
    def test_success_confirms_no_helper_remains(self):
        fixture = DockerFixture()
        self.assertEqual(run_owned_helper(fixture, ('--rm', 'synthetic-image')), 'ok')
        self.assertEqual(fixture.removed, [])

    def test_cli_timeout_removes_only_verified_owned_helper(self):
        fixture = DockerFixture(timeout=True)
        with self.assertRaisesRegex(RuntimeError, 'CLI timed out'):
            run_owned_helper(fixture, ('--rm', 'synthetic-image'), timeout=1)
        self.assertEqual(fixture.removed, ['a' * 64])
        self.assertIsNone(fixture.container)

    def test_identity_mismatch_never_removes(self):
        fixture = DockerFixture(timeout=True, wrong_identity=True)
        with self.assertRaisesRegex(Stopped, 'helper_identity_ambiguous'):
            run_owned_helper(fixture, ('--rm', 'synthetic-image'))
        self.assertEqual(fixture.removed, [])

    def test_unconfirmed_cleanup_stops_the_update(self):
        fixture = DockerFixture(timeout=True, remove_fails=True)
        with self.assertRaisesRegex(Stopped, 'helper_cleanup_unconfirmed'):
            run_owned_helper(fixture, ('--rm', 'synthetic-image'))
        self.assertIsNotNone(fixture.container)


if __name__ == '__main__':
    unittest.main()
