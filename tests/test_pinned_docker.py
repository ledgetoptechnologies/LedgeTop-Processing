import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest

from ops import pinned_docker

HOST = 'unix:///var/run/docker.sock'
ROOT = Path(__file__).resolve().parents[1]

STUB = textwrap.dedent('''
    import json, os, sys
    behaviour = json.load(open(os.environ['STUB_BEHAVIOUR']))
    with open(os.environ['STUB_LOG'], 'a') as log:
        log.write(json.dumps({'argv': sys.argv[1:],
                              'docker_env': sorted(k for k in os.environ if k.upper().startswith('DOCKER_')),
                              'config_entries': sorted(os.listdir(os.environ['DOCKER_CONFIG']))}) + '\\n')
    arguments = sys.argv[3:] if sys.argv[1:2] == ['--host'] else sys.argv[1:]
    if arguments[:1] == ['info']:
        sys.stdout.write(behaviour.get('daemon', 'daemon-id') + '\\n')
        sys.exit(behaviour.get('info_status', 0))
    sys.stdout.write('stdin=' + sys.stdin.read())
    sys.exit(behaviour.get('status', 0))
''')


class PinnedDockerTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        base = Path(self.temporary.name)
        self.stub = base / 'docker_stub.py'
        self.stub.write_text(STUB)
        self.behaviour, self.log = base / 'behaviour.json', base / 'calls.log'
        self.behaviour.write_text('{}')
        self.environment = dict(os.environ)
        os.environ.update(STUB_BEHAVIOUR=str(self.behaviour), STUB_LOG=str(self.log),
                          DOCKER_HOST='tcp://other-daemon:2375', DOCKER_CONTEXT='remote', DOCKER_TLS_VERIFY='1')

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.environment)
        self.temporary.cleanup()

    def invoke(self, *command, expect='daemon-id', host=HOST, stdin='', **behaviour):
        self.behaviour.write_text(json.dumps(behaviour))
        arguments = [sys.executable, '-B', str(ROOT / 'ops' / 'pinned_docker.py'), '--host', host,
                     '--docker', sys.executable + ' ' + str(self.stub)]
        if expect is not None:
            arguments += ['--expect-daemon-id', expect]
        return subprocess.run(arguments + ['--', *command], input=stdin, capture_output=True, text=True)

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_every_call_is_pinned_with_cleared_overrides(self):
        result = self.invoke('ps', '-a')
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertEqual([call['argv'][2] for call in calls], ['info', 'ps'])
        for call in calls:
            self.assertEqual(call['argv'][:2], ['--host', HOST])
            self.assertEqual(call['docker_env'], ['DOCKER_CONFIG'])
            self.assertEqual(call['config_entries'], [])

    def test_wrong_or_unavailable_daemon_runs_nothing(self):
        for behaviour in ({'daemon': 'another-daemon'}, {'info_status': 1}):
            self.log.unlink(missing_ok=True)
            result = self.invoke('stop', 'x', **behaviour)
            self.assertEqual(result.returncode, 125, behaviour)
            self.assertEqual([call['argv'][2] for call in self.calls()], ['info'])

    def test_global_options_remote_hosts_and_unpinned_commands_are_refused(self):
        for command in (('--context', 'remote', 'ps'), ('-H', 'tcp://x', 'ps'), ('--config', '/x', 'ps'), ()):
            self.assertEqual(self.invoke(*command).returncode, 125, command)
        for host in ('tcp://192.168.50.89:2375', 'ssh://host', 'unix://relative'):
            self.assertEqual(self.invoke('ps', host=host).returncode, 125, host)
        self.assertEqual(self.invoke('stop', 'x', expect=None).returncode, 125)
        self.assertEqual(self.calls(), [])
        recorded = self.invoke('info', '--format', '{{.ID}}', expect=None)
        self.assertEqual((recorded.returncode, recorded.stdout.strip()), (0, 'daemon-id'))

    def test_stdin_stdout_and_exit_status_pass_through(self):
        result = self.invoke('run', '-i', 'image', stdin='archive-bytes', status=3)
        self.assertEqual(result.returncode, 3)
        self.assertEqual(result.stdout, 'stdin=archive-bytes')


class RunbookPinningTest(unittest.TestCase):
    """Every Docker operation in the runbook must go through the pinned wrapper."""

    def blocks(self):
        text = (ROOT / 'docs' / 'd738-copy-cutover.md').read_text(encoding='utf-8')
        return re.findall(r'```sh\n(.*?)```', text, re.S)

    def test_docker_is_defined_as_the_pinned_wrapper_before_any_use(self):
        code = '\n'.join(self.blocks())
        definition = code.index('docker() {')
        self.assertIn('pinned_docker.py', code[definition:code.index('}', definition)])
        self.assertIn('--expect-daemon-id "$DAEMON_ID"', code[definition:code.index('}', definition)])
        before = code[:definition]
        uses = [line for line in before.splitlines() if re.search(r'(^|[\s;(|&])docker\s', line)]
        self.assertEqual(uses, [], 'Docker used before the pinned wrapper is defined')

    def test_no_call_bypasses_the_wrapper(self):
        code = '\n'.join(self.blocks())
        for pattern in (r'\bcommand\s+docker\b', r'\\docker\b', r'/docker\s', r'\benv\b[^\n]*\bdocker\s',
                        r'\bsudo\b[^\n]*\bdocker\b', r'\bexec\s+docker\b', r'"argv"\]\)\.returncode'):
            self.assertIsNone(re.search(pattern, code), pattern)

    def test_spec_argv_is_run_through_the_wrapper(self):
        code = '\n'.join(self.blocks())
        self.assertNotIn('subprocess.run(json.load(open(sys.argv[1]))["argv"])', code)
        self.assertGreaterEqual(code.count('docker "${ARGV[@]}"'), 2)


if __name__ == '__main__':
    unittest.main()
