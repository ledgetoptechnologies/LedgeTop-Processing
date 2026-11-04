"""Opt-in real Docker crash test; requires an explicitly mounted daemon socket."""
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.parse import quote

if os.name == 'posix':
    from ops.durable_helper_journal import HelperJournal


class UnixHTTP(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(5)
        self.sock.connect('/var/run/docker.sock')


def api(method, route, data=None):
    conn = UnixHTTP('localhost', timeout=5)
    try:
        body = None if data is None else json.dumps(data).encode()
        conn.request(method, route, body, {'Content-Type': 'application/json'})
        response = conn.getresponse(); raw = response.read()
        if response.status not in (200, 201, 204): raise RuntimeError('docker API rejected test request')
        return json.loads(raw) if raw else None
    finally: conn.close()


class DockerTransport:
    def docker(self, *args, timeout=30):
        if args[0] == 'ps':
            label = args[args.index('--filter') + 1].removeprefix('label=')
            filters = quote(json.dumps({'label': [label]}), safe='')
            return ''.join(c['Id'] + '\n' for c in api('GET', '/containers/json?all=1&filters=' + filters))
        if args[0] == 'create':
            name = args[args.index('--name') + 1]
            key, value = args[args.index('--label') + 1].split('=', 1)
            value = api('POST', '/containers/create?name=' + name, {
                'Image': 'python:3.11-slim', 'Cmd': ['python', '-c', 'import time;time.sleep(120)'],
                'Labels': {key: value}, 'HostConfig': {'NetworkMode': 'none', 'AutoRemove': True}})
            return value['Id']
        if args[0] == 'start':
            api('POST', '/containers/' + args[-1] + '/start')
            print('ready', flush=True)
            time.sleep(120)
            return ''
        if args[0] == 'rm':
            api('DELETE', '/containers/' + args[-1] + '?force=1'); return ''
        raise AssertionError(args)

    def inspected(self, identifier, *, timeout=30):
        return api('GET', '/containers/' + identifier + '/json')


@unittest.skipUnless(os.name == 'posix' and os.environ.get('OWNED_HELPER_DOCKER_TEST') == '1',
                     'requires opt-in disposable Docker daemon')
class RealDockerCrashTest(unittest.TestCase):
    def test_sigkill_then_recover_exact_helper(self):
        with tempfile.TemporaryDirectory() as root:
            code = '''import sys
from ops.update_reference_coordinator import run_owned_helper
from tests.test_owned_helper_docker_kill import DockerTransport
run_owned_helper(DockerTransport(), ('synthetic',), journal_root=sys.argv[1],
                 journal_binding={'operation':'2'*32,'engineId':'synthetic-docker-engine'})
'''
            child = subprocess.Popen([sys.executable, '-c', code, root], stdout=subprocess.PIPE, text=True)
            try:
                self.assertEqual(child.stdout.readline().strip(), 'ready')
                os.kill(child.pid, signal.SIGKILL); child.wait(timeout=5)
                binding={'operation':'2'*32,'engineId':'synthetic-docker-engine'}
                with HelperJournal(root,binding) as journal:
                    record = journal.read()
                    self.assertEqual(record['phase'], 'start_pending')
                    transport = DockerTransport()
                    self.assertTrue(transport.inspected(record['containerId'])['State']['Running'])
                    journal.recover(transport, authorize=lambda: None)
                    self.assertEqual(journal.read()['phase'], 'resolved')
            finally:
                if child.poll() is None: child.kill(); child.wait(timeout=5)
                child.stdout.close()
                # Cleanup only the exact journal-bound test helper on failure.
                try:
                    with HelperJournal(root,{'operation':'2'*32,'engineId':'synthetic-docker-engine'}) as journal:
                        if journal.read()['phase'] in ('created', 'start_pending'):
                            journal.recover(DockerTransport(), authorize=lambda: None)
                except FileNotFoundError: pass


if __name__ == '__main__': unittest.main()

