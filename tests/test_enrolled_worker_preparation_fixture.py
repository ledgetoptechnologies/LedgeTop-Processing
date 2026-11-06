"""Pure real composer/typed review; optional preseeded actual child proof.

Run in the reviewed nonroot readonly-source container. No INSTALL_BASE patch,
installer, systemd, Docker, SSH, coordinator Queue or native job is invoked.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import tarfile
import io
import unittest
import uuid
from unittest.mock import Mock,patch

from ops import qualify_enrolled_worker_preparation as w
from ops.qualify_enrolled_phase_transport import configuration_material
from ops import worker_prestart_review as review
from ops import enroll_reference_worker as install
from tests.test_enrolled_coordinator_deployment import bundle as bundle_fixture


@unittest.skipUnless(os.name=='posix' and hasattr(os,'geteuid') and os.geteuid()==10001,
                     'bounded UID10001 Linux fixture')
class PreparedWorkerFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        archive=os.environ.get('LTDS_QUALIFICATION_SOURCE_ARCHIVE')
        commit=os.environ.get('LTDS_QUALIFICATION_SOURCE_COMMIT')
        if not archive or not commit:raise unittest.SkipTest('explicit frozen source archive+revision required')
        cls.temp=tempfile.TemporaryDirectory(dir='/tmp');cls.root=Path(cls.temp.name);cls.root.chmod(0o700)
        cls.operation=str(uuid.uuid4());cls.image='sha256:'+'a'*64
        seed=cls.root/'seed';seed.mkdir(mode=0o700)
        w.seed_layout(Path(__file__).resolve().parents[1],archive,cls.operation,cls.image,commit,seed)
        cls.layout=json.loads((seed/'layout.json').read_bytes())
        # Same real seed artifacts, private unit-test paths rather than mounted
        # final aliases. This is preparation proof, not a factory execution.
        for name in ('source','dependencies'):cls.layout[name]['path']=str(seed/'inputs'/(name+'.tar'))

    @classmethod
    def tearDownClass(cls):
        if not hasattr(cls,'temp'):return
        for path in cls.root.rglob('*'):
            if path.is_dir() and not path.is_symlink():path.chmod(0o700)
        cls.temp.cleanup()

    def setUp(self):
        self.temp_node=tempfile.TemporaryDirectory(dir='/tmp');self.addCleanup(self.temp_node.cleanup)
        self.root_node=Path(self.temp_node.name);self.root_node.chmod(0o700)
        self.bundle,self.plan=bundle_fixture();self.material=configuration_material(self.operation,self.plan['authorityId'])
        self.material['final']['workers']=self.material['bootstrap']['workers']
        self.layout=copy.deepcopy(type(self).layout)
        def gate(entry,mode,packet,home,**kwargs):
            self.assertEqual(mode,'gate')
            from processing.node_gate import NodeGate,canonical_gate_path
            with patch.dict(os.environ,w.clean_environment(home),clear=True):
                with NodeGate(packet['engineId']) as actual:
                    actual.provision(packet['nodeId'],packet['coordinatorOrigin'],packet['authorityId'])
                path=canonical_gate_path(packet['engineId'])/'gate.json';value=json.loads(path.read_bytes())
                return dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                    **{key:value[key] for key in ('nodeId','authorityId','generation','coordinatorOrigin')})
        patcher=patch.object(w,'invoke',side_effect=gate);patcher.start();self.addCleanup(patcher.stop)

    def prepare(self,node):
        backend=Mock(origin='http://127.0.0.1:'+('19001' if node=='cluster' else '19002'),fixture_options=[])
        return w.prepare_node(self.root_node/node,self.material,node,self.plan['authorityId'],self.layout,
            port=18445 if node=='cluster' else 18446,backend=backend,controller_id='b'*64)

    def test_real_preparation_outputs_bind_installer_unit_and_no_configuration_leaks(self):
        prepared=[self.prepare(node) for node in ('cluster','main')]
        self.bundle['workers']=[value['manifest'] for value in prepared]
        raw=review.compose(self.bundle,self.plan,[value['review'] for value in prepared])
        self.assertEqual(review.validate(raw,self.bundle,self.plan)['scope'],'configuration-review')
        for value in prepared:
            manifest=json.loads(Path(value['review']['manifest']['path']).read_bytes());install.validate_manifest(manifest)
            home=Path(manifest['service']['home']);root=Path(manifest['installationRoot'])
            self.assertEqual(root.parent,Path('/opt/ledgetop-processing-reference'))
            self.assertEqual(Path(value['path']).read_bytes(),Path(manifest['config']['path']).read_bytes())
            self.assertEqual(Path(value['manifest']['settings']['unitFile']).read_text(),install.render_unit(manifest))
            self.assertEqual(value['manifest']['settings']['configPath'],str(home/'.config/ledgetop-processing/reference-worker.json'))
            self.assertEqual(value['manifest']['settings']['pythonExecutable'],str(root/'venv/bin/python'))
            self.assertEqual(value['config']['maintenanceReadiness']['sourceSha256'],self.layout['source']['footprintSha256'])
            for secret in (value['config']['workerBearer'],value['config']['resultReadBearer'],value['config']['engineToken'],
                           value['config']['maintenanceReadiness']['bearer']):self.assertNotIn(secret,raw.decode())
            self.assertNotIn('PRIVATE KEY',raw.decode());self.assertIsNone(value['config']['maintenanceReadiness']['activation'])
            self.assertEqual(value['config']['viewerReceiptKeys'],self.material['final']['viewerReceiptKeys'])

    def test_public_review_rejects_swapped_unit_home_interpreter_config_or_gate(self):
        values=[self.prepare(node) for node in ('cluster','main')]
        self.bundle['workers']=[value['manifest'] for value in values];entries=[value['review'] for value in values]
        raw=review.compose(self.bundle,self.plan,entries)
        for section,key,value in (('settings','pythonExecutable','/usr/local/bin/python'),
                ('settings','unitSha256','b'*64),('settings','configPath','/tmp/not-installed.json'),
                ('expected','configSha256','c'*64),('expected','gateGeneration',99),('settings','sourceRoot','/qualification')):
            bad=copy.deepcopy(self.bundle);bad['workers'][0][section][key]=value
            with self.subTest(section=section,key=key),self.assertRaises(Exception):review.validate(raw,bad,self.plan)
        swapped=copy.deepcopy(self.bundle);swapped['workers'].reverse()
        with self.assertRaises(Exception):review.validate(raw,swapped,self.plan)

    def test_changed_dependency_archive_fails_before_outputs_or_factory(self):
        bad=self.root_node/'changed.tar';w.write_private(bad,b'changed')
        self.layout['dependencies']['path']=str(bad)
        with patch('ops.reference_worker_daemon.build_worker',side_effect=AssertionError('factory forbidden')):
            with self.assertRaises(Exception):self.prepare('cluster')
        self.assertFalse((self.root_node/'cluster/prepared').exists())

    def test_proper_ca_and_nonca_server_leaf(self):
        from cryptography import x509
        from cryptography.x509.oid import ExtendedKeyUsageOID
        files=self.material['configurationFiles'];ca=x509.load_pem_x509_certificate(files['fixture-ca.pem'])
        leaf=x509.load_pem_x509_certificate(files['fixture-cert.pem']);leaf.verify_directly_issued_by(ca)
        self.assertTrue(ca.extensions.get_extension_for_class(x509.BasicConstraints).value.ca)
        self.assertFalse(leaf.extensions.get_extension_for_class(x509.BasicConstraints).value.ca)
        self.assertIn(ExtendedKeyUsageOID.SERVER_AUTH,leaf.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value)

    def test_final_trust_required_and_no_preparation_receipt_fabricated(self):
        self.material['final']['viewerReceiptKeys']={}
        # Compatibility with the final-only/valid-key composer patch. The
        # support module passes final bytes unchanged, never adds fake keys.
        with self.assertRaises(Exception):self.prepare('cluster')


class FixtureLifecycleUnitTests(unittest.TestCase):
    def test_source_archive_is_exact_complete_tree_not_just_declared_hash(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'source';source.mkdir();(source/'code.py').write_bytes(b'actual frozen bytes')
            def archive(payload,duplicate=False):
                stream=io.BytesIO()
                with tarfile.open(fileobj=stream,mode='w') as tar:
                    for _ in range(2 if duplicate else 1):
                        entry=tarfile.TarInfo('code.py');entry.size=len(payload);tar.addfile(entry,io.BytesIO(payload))
                return io.BytesIO(stream.getvalue())
            with archive(b'actual frozen bytes') as value:
                path=root/'source.tar';path.write_bytes(value.read());w.verify_source_archive(source,path)
            for payload,duplicate in ((b'other source',False),(b'actual frozen bytes',True)):
                with archive(payload,duplicate) as value:path.write_bytes(value.read())
                with self.assertRaises(RuntimeError):w.verify_source_archive(source,path)

    def test_layout_digest_refusal_precedes_archive_or_subprocess(self):
        with patch.object(w,'private_bytes',return_value=b'{}'),patch.object(w,'invoke') as invoke,patch.object(w.prep,'archive_pin') as archive:
            with self.assertRaisesRegex(RuntimeError,'reviewed_fixture_layout_digest_changed'):
                w.load_layout('/private/layout.json',str(uuid.uuid4()),source_commit='a'*40,candidate_image='sha256:'+'a'*64,expected_sha256='0'*64)
            invoke.assert_not_called();archive.assert_not_called()

    def test_unknown_child_shutdown_retains_without_kill(self):
        import subprocess
        process=Mock(pid=123);process.poll.return_value=None;process.wait.side_effect=subprocess.TimeoutExpired('owned',10)
        with patch.object(w,'process_identity',return_value='456'):
            with self.assertRaisesRegex(RuntimeError,'factory_worker_shutdown_uncertain'):
                w.stop_worker(dict(process=process,startTime='456'))
        process.send_signal.assert_called_once();process.kill.assert_not_called()

    def test_transport_diagnostic_excludes_payload_and_handles_nonexception_reason(self):
        from urllib.error import URLError
        secret='private-config-must-never-appear'
        value=w.transport_diagnostic(URLError(ConnectionRefusedError(111,secret)))
        self.assertEqual(value,dict(exceptionType='URLError',reasonType='ConnectionRefusedError',errno=111))
        self.assertNotIn(secret,json.dumps(value))
        self.assertEqual(w.transport_diagnostic(URLError(secret)),
                         dict(exceptionType='URLError',reasonType='non_exception_reason'))
        self.assertEqual(w.transport_diagnostic(ValueError(secret)),dict(exceptionType='ValueError'))

    def test_nonzero_reaped_child_result_is_retained_before_success_refusal(self):
        with tempfile.TemporaryDirectory() as temporary:
            process=Mock(pid=123,returncode=-15);process.poll.return_value=-15
            root=Path(temporary)
            with self.assertRaisesRegex(RuntimeError,'factory_worker_failed_before_shutdown'):
                w.stop_worker(dict(process=process,startTime='456',root=root))
            value=json.loads((root/'factory-child-exit.json').read_bytes())
            self.assertEqual(value['exitCode'],-15)
            self.assertTrue(value['retired'])
            self.assertEqual(value['pid'],123)
            process.send_signal.assert_not_called();process.kill.assert_not_called()

    def test_probe_diagnostic_program_is_valid_and_retains_production_masking(self):
        with tempfile.TemporaryDirectory() as temporary:
            home=Path(temporary)
            def run(argv,**kwargs):
                program=argv[-1]
                compile(program,'<fixture-probe>','exec')
                self.assertIn('transport_diagnostic(e)',program)
                self.assertIn('traceback.extract_tb',program)
                self.assertNotIn('str(e)',program)
                self.assertNotIn('repr(e)',program)
                self.assertEqual(kwargs['error_limit'],4096)
                raise RuntimeError('command_read_unavailable')
            with patch.object(w.BoundedSubprocess,'run',side_effect=run):
                with self.assertRaisesRegex(RuntimeError,'command_read_unavailable'):
                    w.invoke({'root':'/opt/example'},'probe',{},home)
            self.assertEqual(list(home.iterdir()),[])

    def test_wrong_planned_manifest_never_probes_child(self):
        with patch.object(w,'invoke') as invoke:
            with self.assertRaisesRegex(RuntimeError,'planned_worker_fixture_changed'):
                w.readiness(dict(manifest={'nodeId':'cluster'}),{'nodeId':'main'})
            invoke.assert_not_called()


if __name__=='__main__':unittest.main()
