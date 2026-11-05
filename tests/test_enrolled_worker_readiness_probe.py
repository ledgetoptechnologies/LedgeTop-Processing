"""Read-only existing handover + real TLS challenge; no service operations."""
import copy
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import ssl
import sys
import tempfile
import threading
import unittest
import uuid
from unittest.mock import patch

from ops import enrolled_worker_readiness_probe as probe
from ops.reference_worker_handover import ReferenceWorkerHandover
from processing.reference_worker_readiness import PROTOCOL, source_digest
from tests.test_reference_dynamic_source_intake_scale import _write_tls_material


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux existing worker identity')
class ExistingReadinessProbeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name); key, cert = _write_tls_material(root)
        source = Path(__file__).resolve().parents[1]
        self.source_sha = source_digest(source)
        self.config = dict(nodeId='cluster', workerId='worker-cluster', workerInstanceId='instance-cluster',
            authorityId=str(uuid.uuid4()), engineId='engine', gateGeneration=3, engineVersion='3.8.5',
            maintenanceReadiness=dict(schemaVersion=1, bearer='synthetic-private-token-' + 'a'*32,
                sourceRoot=str(source), sourceSha256=self.source_sha, activation=None))
        path = root/'worker.json'; raw = json.dumps(self.config).encode(); path.write_bytes(raw); path.chmod(0o600)
        self.expected = {key: self.config[key] for key in ('nodeId','workerId','workerInstanceId','authorityId',
                                                          'engineId','gateGeneration','engineVersion')}
        self.expected.update(configSha256=hashlib.sha256(raw).hexdigest(), sourceSha256=self.source_sha, activation=None)
        test = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                test.assertEqual(self.headers.get('Authorization'), 'Bearer '+test.config['maintenanceReadiness']['bearer'])
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                value = dict(test.expected, schemaVersion=1, protocol=PROTOCOL, nonce=request['nonce'],
                    bootId=str(uuid.uuid4()), pid=os.getpid(), readerEpoch='synthetic-epoch',
                    driverBindingSha256='a'*64, ready=True, idle=True, optionsSha256='b'*64,
                    safeOptionsSha256='c'*64, loadedSourceSha256='d'*64)
                value.update(test.response_changes)
                body=json.dumps(value).encode(); self.send_response(200)
                self.send_header('Content-Type','application/json'); self.end_headers(); self.wfile.write(body)
        self.response_changes = {}
        self.server = HTTPServer(('127.0.0.1',0), Handler)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(cert,key)
        self.server.socket=context.wrap_socket(self.server.socket,server_side=True)
        thread=threading.Thread(target=self.server.serve_forever); thread.start()
        def close(): self.server.shutdown(); self.server.server_close(); thread.join(2); self.assertFalse(thread.is_alive())
        self.addCleanup(close)
        self.worker=dict(nodeId='cluster',sshHost='192.0.2.1',sshUid=os.geteuid(),
            settings=dict(schemaVersion=1,unitName='worker.service',unitFile=str(root/'worker.service'),
                unitSha256='e'*64,pythonExecutable=sys.executable,configPath=str(path),sourceRoot=str(source),
                sourceSha256=self.source_sha,readinessOrigin='https://127.0.0.1:'+str(self.server.server_port),
                readinessCAFile=str(cert)), expected=self.expected)
        self.process=dict(pid=os.getpid(),active='active',substate='running',cgroup='/synthetic-worker',
                          invocationId='invocation',startTime='123')
        class Control:
            def probe(_): return copy.deepcopy(test.process)
            def command(*_): raise AssertionError('readiness cannot change a service')
        self.control=Control()
        # Ordinary mutable development checkouts are not qualified installed
        # readonly sources. Actual serializer/readonly checks have separate tests.
        self.source_patch=patch.object(probe,'_source_digest',return_value=self.source_sha)
        self.source_patch.start(); self.addCleanup(self.source_patch.stop)

    def test_actual_tls_nonce_and_existing_ready_without_mutation_context(self):
        with patch.object(ReferenceWorkerHandover,'__enter__',side_effect=AssertionError('no lock creation')):
            value=probe.probe(self.worker,str(uuid.uuid4()),control=self.control)
        self.assertEqual(value['readiness']['configSha256'],self.expected['configSha256'])
        self.assertEqual(value['process'],self.process)
        self.assertEqual(set(Path(self.temp.name).glob('.*maintenance*')),set())

    def test_exact_expected_identity_and_nonce_mismatches_refuse(self):
        for change in ({'workerInstanceId':'other'},{'nonce':str(uuid.uuid4())},{'authorityId':str(uuid.uuid4())}):
            self.response_changes=change
            with self.assertRaises((ValueError,RuntimeError)): probe.probe(self.worker,str(uuid.uuid4()),control=self.control)

    def test_process_identity_must_remain_exact_across_readiness(self):
        original=self.control.probe; calls=[]
        def changed():
            value=original(); calls.append(1)
            if len(calls)>1: value['startTime']='124'
            return value
        self.control.probe=changed
        with self.assertRaises((ValueError,RuntimeError)):probe.probe(self.worker,str(uuid.uuid4()),control=self.control)

    def test_wrong_tls_trust_and_changed_config_pin_refuse(self):
        self.worker['expected']=dict(self.expected,configSha256='f'*64)
        with self.assertRaises((ValueError,RuntimeError)):probe.probe(self.worker,str(uuid.uuid4()),control=self.control)
        self.worker['expected']=self.expected
        self.worker['settings']['readinessCAFile']=str(Path(self.temp.name)/'missing-ca.pem')
        with self.assertRaises((OSError,ValueError,RuntimeError)):probe.probe(self.worker,str(uuid.uuid4()),control=self.control)

    def test_serialized_body_uses_existing_source_not_a_new_installed_module(self):
        script=probe.serialize_probe(self.worker,str(uuid.uuid4()))
        compile(script,'<serialized-probe>','exec')
        self.assertNotIn(b'from ops.enrolled_worker_readiness_probe import probe',script)
        self.assertNotIn(self.config['maintenanceReadiness']['bearer'].encode(),script)
        self.assertIn(b'worker_readiness_probe_unavailable',script)
        self.assertLess(len(script),65536)

    def test_preimport_footprint_matches_existing_readiness_and_serialized_body_executes(self):
        self.source_patch.stop()
        self.assertEqual(probe._source_digest(self.worker['settings']['sourceRoot']), self.source_sha)
        script=probe.serialize_probe(self.worker,str(uuid.uuid4()))
        # Only the systemd edge is synthetic; imports, readonly preflight,
        # private config, verified TLS challenge, and actual ready() are real.
        script=script.replace(b'probe(worker,challenge),',
            ('probe(worker,challenge,control=type("SyntheticControl",(),{"probe":lambda self:'+repr(self.process)+'})()),').encode())
        result=probe.BoundedSubprocess().run([sys.executable,'-I','-'],input_bytes=script,timeout=30,output_limit=16384)
        self.assertEqual(json.loads(result)['readiness']['workerId'],self.expected['workerId'])

    def test_systemd_runner_refuses_any_mutating_command_before_spawn(self):
        with patch.object(probe,'BoundedSubprocess',side_effect=AssertionError('must refuse first')):
            with self.assertRaises((ValueError,RuntimeError)):probe._read_only_runner(['/usr/bin/systemctl','--user','stop','worker.service'])


if __name__=='__main__': unittest.main()

