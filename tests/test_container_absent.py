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
HOST = 'unix:///var/run/docker.sock'

# The stub records every invocation (argv and the DOCKER_* environment it saw).
STUB = textwrap.dedent('''
    import json, os, sys
    behaviour = json.load(open(os.environ['STUB_BEHAVIOUR']))
    with open(os.environ['STUB_LOG'], 'a') as log:
        log.write(json.dumps({'argv': sys.argv[1:],
                              'docker_env': {k: v for k, v in os.environ.items() if k.upper().startswith('DOCKER_')},
                              'config_entries': sorted(os.listdir(os.environ['DOCKER_CONFIG']))
                              if os.path.isdir(os.environ.get('DOCKER_CONFIG', '')) else None}) + '\\n')
    arguments = sys.argv[1:]
    if arguments[:1] == ['--host']:
        arguments = arguments[2:]
    command = 'inspect' if arguments[0] == 'container' else arguments[0]
    entry = behaviour.get(command, {'status': 0, 'out': ''})
    sys.stdout.write(entry.get('out', ''))
    sys.stderr.write(entry.get('err', ''))
    sys.exit(entry.get('status', 0))
''')


class ContainerAbsentTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        base = Path(self.temporary.name)
        self.stub = base / 'docker_stub.py'
        self.stub.write_text(STUB)
        self.behaviour, self.log = base / 'behaviour.json', base / 'calls.log'
        self.environment = dict(os.environ)
        os.environ.update(STUB_BEHAVIOUR=str(self.behaviour), STUB_LOG=str(self.log),
                          DOCKER_HOST='tcp://other-daemon:2375', DOCKER_CONTEXT='remote',
                          DOCKER_TLS_VERIFY='1', DOCKER_CERT_PATH='/elsewhere', docker_host='tcp://lower:1')

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.environment)
        self.temporary.cleanup()

    def run_check(self, expect=None, host=HOST, **behaviour):
        defaults = {'info': {'status': 0, 'out': 'daemon-id\n'},
                    'ps': {'status': 0, 'out': OTHER + '\n'},
                    'inspect': {'status': 1, 'err': 'Error: No such container: ' + OLD + '\n'}}
        defaults.update(behaviour)
        self.behaviour.write_text(json.dumps(defaults))
        return container_absent.check(OLD, docker=(sys.executable, str(self.stub)), host=host,
                                      expect_daemon_id=expect)

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_absent_only_with_census_and_explicit_not_found(self):
        status, report = self.run_check()
        self.assertEqual((status, report['absent']), (0, True))

    def test_every_call_is_pinned_to_the_named_local_daemon_without_overrides(self):
        self.run_check()
        calls = self.calls()
        self.assertEqual(len(calls), 3)
        for call in calls:
            self.assertEqual(call['argv'][:2], ['--host', HOST])
            self.assertEqual(set(call['docker_env']), {'DOCKER_CONFIG'})
            self.assertEqual(call['config_entries'], [])  # fresh empty config: no saved context
        self.assertNotIn('docker_host', {k for c in calls for k in c['docker_env']})

    def test_remote_or_malformed_hosts_are_refused_before_any_call(self):
        for host in ('tcp://192.168.50.89:2375', 'ssh://user@host', 'unix://relative.sock', '', 'unix:///a b'):
            status, report = self.run_check(host=host)
            self.assertEqual(status, 2, host)
        self.assertEqual(self.calls(), [])

    def test_unexpected_daemon_is_never_absence(self):
        self.assertEqual(self.run_check(expect='daemon-id')[0], 0)
        status, report = self.run_check(expect='recorded-other-daemon')
        self.assertEqual((status, report['reason']), (2, 'unexpected daemon'))

    def test_daemon_failure_is_never_absence(self):
        for broken in ({'status': 1, 'err': 'Cannot connect to the Docker daemon'}, {'status': 0, 'out': ''}):
            status, report = self.run_check(info=broken)
            self.assertEqual(status, 2, broken)
            self.assertFalse(report['absent'])

    def test_failed_or_unparseable_census_is_never_absence(self):
        for ps in ({'status': 1, 'err': 'boom'}, {'status': 0, 'out': 'not-an-id\n'}):
            self.assertEqual(self.run_check(ps=ps)[0], 2, ps)

    def test_inspect_error_other_than_not_found_is_never_absence(self):
        for inspect in ({'status': 1, 'err': 'Cannot connect to the Docker daemon'},
                        {'status': 1, 'err': 'permission denied'}, {'status': 125, 'err': ''}):
            self.assertEqual(self.run_check(inspect=inspect)[0], 2, inspect)

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
