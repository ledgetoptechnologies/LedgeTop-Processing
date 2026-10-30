import json
import os
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest

from ops import container_absent

OLD = 'a' * 64
OTHER = 'b' * 64

STUB = textwrap.dedent('''
    import json, os, sys
    behaviour = json.load(open(os.environ['STUB_BEHAVIOUR']))
    command = sys.argv[1]
    if command == 'container':
        command = 'inspect'
    entry = behaviour.get(command, {'status': 0, 'out': ''})
    sys.stdout.write(entry.get('out', ''))
    sys.stderr.write(entry.get('err', ''))
    sys.exit(entry.get('status', 0))
''')


class ContainerAbsentTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.stub = Path(self.temporary.name) / 'docker_stub.py'
        self.stub.write_text(STUB)
        self.behaviour = Path(self.temporary.name) / 'behaviour.json'
        self.environment = dict(os.environ)
        os.environ['STUB_BEHAVIOUR'] = str(self.behaviour)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.environment)
        self.temporary.cleanup()

    def run_check(self, **behaviour):
        defaults = {'info': {'status': 0, 'out': 'daemon-id\n'},
                    'ps': {'status': 0, 'out': OTHER + '\n'},
                    'inspect': {'status': 1, 'err': 'Error: No such container: ' + OLD + '\n'}}
        defaults.update(behaviour)
        self.behaviour.write_text(json.dumps(defaults))
        return container_absent.check(OLD, docker=(sys.executable, str(self.stub)))

    def test_absent_only_with_census_and_explicit_not_found(self):
        status, report = self.run_check()
        self.assertEqual((status, report['absent']), (0, True))

    def test_daemon_failure_is_never_absence(self):
        for broken in ({'status': 1, 'err': 'Cannot connect to the Docker daemon'}, {'status': 0, 'out': ''}):
            status, report = self.run_check(info=broken)
            self.assertEqual(status, 2, broken)
            self.assertFalse(report['absent'])

    def test_failed_or_unparseable_census_is_never_absence(self):
        for ps in ({'status': 1, 'err': 'boom'}, {'status': 0, 'out': 'not-an-id\n'}):
            status, report = self.run_check(ps=ps)
            self.assertEqual(status, 2, ps)

    def test_inspect_error_other_than_not_found_is_never_absence(self):
        for inspect in ({'status': 1, 'err': 'Cannot connect to the Docker daemon'},
                        {'status': 1, 'err': 'permission denied'}, {'status': 125, 'err': ''}):
            status, report = self.run_check(inspect=inspect)
            self.assertEqual(status, 2, inspect)

    def test_present_container_is_reported(self):
        self.assertEqual(self.run_check(ps={'status': 0, 'out': OLD + '\n' + OTHER + '\n'})[0], 1)
        self.assertEqual(self.run_check(inspect={'status': 0, 'out': OLD + '\n'})[0], 1)

    def test_requires_full_id(self):
        self.assertEqual(container_absent.check('abc123')[0], 2)

    def test_missing_docker_executable_is_never_absence(self):
        status, _ = container_absent.check(OLD, docker=(str(Path(self.temporary.name) / 'no-such-docker'),))
        self.assertEqual(status, 2)


if __name__ == '__main__':
    unittest.main()
