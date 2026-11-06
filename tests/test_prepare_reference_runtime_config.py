"""Only newly created synthetic private inputs; no installed credentials or NAS."""
import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import unittest
import uuid
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization as s
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from ops import enroll_reference_worker as install
from ops import enrolled_coordinator_deployment as deployment
from ops import enrolled_worker_readiness_probe as probe
from ops import prepare_reference_runtime_config as prep
from processing.node_gate import ENGINE, GateError, canonical_gate_path
from tests import test_prepare_reference_worker_config as fixtures
from tests.test_enrolled_coordinator_deployment import bundle as deployment_bundle


GATE_ENGINES = ('e', 'actual.test-engine:abc', 'ABCD:EFGH:IJKL', str(uuid.UUID(int=7)), 'A'*256)
NON_GATE_ENGINES = ('', '.leading', ':leading', '-leading', 'white space', 'trailing\n', 'a/b', 'A'*257, '\u00e9ngine', None, 7)


class EngineGrammarAlignmentTests(unittest.TestCase):
    """Pure validators: every first-install stage agrees with the original Gate."""
    def manifest(self, engine):
        operation = str(uuid.uuid4()); authority = str(uuid.uuid4())
        name = hashlib.sha256(engine.encode()).hexdigest() if type(engine) is str else '0'*64
        artifact = dict(path='/private/artifact', sha256='1'*64, bytes=1, ownerUid=1000)
        return dict(schemaVersion=1, protocol=install.PROTOCOL, operationId=operation,
            host=dict(hostname='worker', bootId=str(uuid.uuid4()), dockerEngineId=engine),
            service=dict(name='fixture', uid=1000, gid=1000, home='/home/fixture'),
            installationRoot='/opt/ledgetop-processing-reference/'+operation,
            source=dict(artifact, commit='2'*40, footprintSha256='3'*64, requirementsSha256='4'*64),
            dependencies=dict(artifact, pythonSha256='5'*64, pythonVersion='3.10.12', versions=dict(
                cryptography='50.0.2', cffi='2.1.1', pycparser='2.23', rfc8785='0.1.4')),
            config=dict(artifact), unitSha256=hashlib.sha256(b'unit').hexdigest(),
            gate=dict(path='/home/fixture/.local/state/LedgeTop-Processing/node-gates/'+name+'/gate.json',
                sha256='6'*64, nodeId='main', authorityId=authority, generation=1,
                coordinatorOrigin='http://127.0.0.1:18080'),
            engine=dict(containerId='7'*64, imageId='sha256:'+'8'*64, version='3.8.5', loopbackPort=13004,
                catalogSha256='9'*64))

    def preparer(self, engine):
        m = self.manifest(engine)
        prep.node_binding(dict(schemaVersion=1, protocol=prep.BINDING_PROTOCOL, host=m['host'],
                               service=m['service'], gate=m['gate']))

    def installer(self, engine):
        with patch.object(install, 'render_unit', return_value='unit'):
            install.validate_manifest(self.manifest(engine))

    def bundle_worker(self):
        value, plan = deployment_bundle()
        return copy.deepcopy(value['workers'][0]), plan['authorityId']

    def deployed(self, engine):
        worker, authority = self.bundle_worker(); worker['expected']['engineId'] = engine
        deployment.worker(worker, authority)

    def readiness(self, engine):
        worker = self.bundle_worker()[0]; worker['expected']['engineId'] = engine
        probe.validate_request(worker, str(uuid.uuid4()))

    def stages(self):
        return (self.preparer, self.installer, self.deployed, self.readiness)

    def test_gate_valid_engine_passes_every_stage(self):
        for engine in GATE_ENGINES:
            canonical_gate_path(engine)
            for stage in self.stages():
                with self.subTest(stage=stage.__name__, length=len(engine)): stage(engine)

    def test_gate_invalid_engine_refuses_at_every_stage(self):
        for index, engine in enumerate(NON_GATE_ENGINES):
            with self.assertRaises(GateError): canonical_gate_path(engine)
            for stage in self.stages():
                with self.subTest(stage=stage.__name__, index=index):
                    with self.assertRaises(Exception): stage(engine)

    def test_node_and_worker_slugs_are_not_broadened(self):
        for field in ('nodeId', 'workerId', 'workerInstanceId'):
            for check in (deployment.worker, probe.validate_request):
                worker, authority = self.bundle_worker()
                worker['expected'][field] = 'colon:dotted.value'
                if field == 'nodeId': worker['nodeId'] = 'colon:dotted.value'
                with self.subTest(field=field, check=check.__name__):
                    with self.assertRaises(Exception):
                        check(worker, authority if check is deployment.worker else str(uuid.uuid4()))

    def test_stages_share_the_original_gate_definition(self):
        for module in (install, prep, deployment, probe):
            text = Path(module.__file__).read_text()
            self.assertIn('ENGINE.fullmatch', text); self.assertNotIn(ENGINE.pattern[:-2], text)

    def test_serialized_probe_imports_engine_only_after_source_verification(self):
        worker = self.bundle_worker()[0]; worker['expected']['engineId'] = 'actual.test-engine:abc'
        script = probe.serialize_probe(worker, str(uuid.uuid4())).decode()
        compile(script, '<serialized-probe>', 'exec')
        self.assertEqual(script.count('from processing.node_gate import ENGINE'), 1)
        self.assertLess(script.index('_source_digest(worker["settings"]["sourceRoot"])=='),
                        script.index('sys.path.insert(0,worker["settings"]["sourceRoot"])'))
        self.assertLess(script.index('sys.path.insert(0,worker["settings"]["sourceRoot"])'),
                        script.index('from processing.node_gate import ENGINE'))


@unittest.skipUnless(os.name == 'posix' and os.geteuid() != 0, 'owner-private Linux fixture')
class RuntimePreparationTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.PrivateWorkerPreparationTests(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        f = self.fixture; self.out = f.root/'runtime-output'
        manifest = f.file('manifest.json', prep.canonical(f.m))
        gate = f.file('copied-gate.json', prep.canonical(f.gate))
        self.keys = {}
        for role in ('source', 'snapshot', 'copy'):
            key = Ed25519PrivateKey.generate()
            self.keys[role] = dict(issuerId=role+'-issuer',keyId=role+'-key',privateKey=f.file(role+'.pem',
                key.private_bytes(s.Encoding.PEM,s.PrivateFormat.PKCS8,s.NoEncryption())))
        self.spec = dict(schemaVersion=1,protocol=prep.PROTOCOL,preparationId=str(uuid.uuid4()),authorityId=f.authority,
            listener=dict(host='0.0.0.0',port=8443,certificate='/run/processing-tls/coordinator.crt',
                privateKey='/run/processing-tls/coordinator.key',clientCA='/run/processing-tls/ca.crt'),
            coordinatorOrigin='https://192.0.2.9:18443',nodes=[dict(manifest=manifest,gate=gate,
                endpoint='http://127.0.0.1:13004',workerId='original-worker',instanceId='original-instance',
                resultOrigin='https://192.0.2.8:18445',bearer=f.file('claim-token',b'c'*40),
                resultBearer=f.file('result-token',b'r'*40),outputBudget=123456)],
            environments=dict(production=dict(providerId=str(uuid.uuid4()),principalId='viewer-source',
                bearer=f.file('viewer-token',b'v'*40),viewerCaPath='/run/processing-ca/ca.crt',**self.keys)),
            forbiddenCredentials=[f.file('legacy-token',b'l'*40)],streamBudget=dict(maxConcurrent=2,maxObjectBytes=1000000),
            sourceSpool=dict(root='/spool/objects',mountPath='/spool',mountId='a'*64,markerName='.spool-id',
                safetyFloorBytes=100,maxObjectBytes=1000000,chunkBytes=1024),outputDirectory=str(self.out))

    def use_binding(self, change=None):
        f=self.fixture
        binding=dict(schemaVersion=1,protocol=prep.BINDING_PROTOCOL,host=copy.deepcopy(f.m['host']),
                     service=copy.deepcopy(f.m['service']),gate=copy.deepcopy(f.m['gate']))
        if change: change(binding)
        self.spec['nodes'][0].pop('manifest',None)
        self.spec['nodes'][0]['binding']=f.file('node-binding.json',prep.canonical(binding))
        return binding

    def test_real_runtime_then_worker_composition_without_prerequisite_worker_config(self):
        from ops import prepare_reference_worker_config as worker
        f=self.fixture; self.use_binding()
        original_config=Path(f.m['config']['path'])
        self.assertFalse(original_config.exists())
        prep.prepare(self.spec)
        spec=copy.deepcopy(f.spec)
        spec['runtime']=f.file('prepared-runtime.json',(self.out/'final.json').read_bytes())
        # This is the existing documented pre-install intent, not installed evidence.
        self.assertEqual(spec['manifest']['config']['sha256'],'0'*64)
        self.assertEqual(spec['manifest']['config']['bytes'],1)
        manifest,raw=worker.compose(spec,f.now)
        self.assertEqual(manifest['config']['sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(manifest['config']['bytes'],len(raw))
        self.assertEqual(manifest['unitSha256'],hashlib.sha256(worker.install.render_unit(manifest).encode()).hexdigest())
        worker.install.validate_manifest(manifest)
        config=json.loads(raw)
        self.assertEqual(config['engineId'],f.engine);self.assertEqual(config['gateGeneration'],7)
        self.assertEqual(config['viewerReceiptKeys'],json.loads((self.out/'final.json').read_bytes())['viewerReceiptKeys'])
        self.assertFalse(original_config.exists());self.assertFalse(Path(config['stateRoot']).exists())

    def test_colon_dotted_gate_engine_prepares_through_binding(self):
        f=self.fixture; self.assertIn(':',f.engine); self.assertIn('.',f.engine); self.use_binding()
        self.assertTrue(prep.prepare(self.spec)['preparationOnly'])
        self.assertEqual(json.loads((self.out/'final.json').read_bytes())['recoveryBindings']['main']['engineId'],f.engine)

    def test_binding_and_manifest_modes_produce_identical_runtime_and_viewer_bytes(self):
        old=prep.compose(self.spec);self.use_binding();new=prep.compose(self.spec)
        for name in old:
            if name!='public-trust.json':self.assertEqual(old[name],new[name])
        trust=json.loads(new['public-trust.json'])
        self.assertEqual(set(trust['inputBindings'][0]),{'bindingSha256','gateSha256'})

    def test_binding_identity_and_profile_changes_refuse_before_output(self):
        mutations=(lambda b:b.update(schemaVersion=2),lambda b:b.update(installed=True),
            lambda b:b['gate'].update(path='/tmp/foreign-gate.json'),
            lambda b:b['host'].update(dockerEngineId='wrong-daemon'),
            lambda b:b['gate'].update(authorityId=str(uuid.uuid4())),
            lambda b:b['gate'].update(nodeId='other-node'),lambda b:b['gate'].update(generation=8),
            lambda b:b['gate'].update(coordinatorOrigin='http://127.0.0.1:19090'),
            lambda b:b['gate'].update(sha256='0'*64))
        for change in mutations:
            with self.subTest(change=change):
                self.use_binding(change)
                with self.assertRaises(Exception):prep.prepare(self.spec)
                self.assertFalse(self.out.exists())

    def test_binding_and_manifest_together_refuse(self):
        manifest=self.spec['nodes'][0]['manifest'];self.use_binding();self.spec['nodes'][0]['manifest']=manifest
        with self.assertRaisesRegex(Exception,'one_node_binding_source'):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_binding_artifact_and_copied_gate_are_independently_pinned(self):
        self.use_binding()
        Path(self.spec['nodes'][0]['gate']['path']).write_bytes(b'changed gate')
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_private_outputs_preserve_bindings_and_separate_signing_roles(self):
        before=(self.fixture.root/'runtime.json').read_bytes()
        receipt=prep.prepare(self.spec)
        boot=json.loads((self.out/'bootstrap.json').read_bytes()); final=json.loads((self.out/'final.json').read_bytes())
        viewer=json.loads((self.out/'viewer-production.json').read_bytes())
        self.assertEqual(boot['schemaVersion'],2);self.assertEqual(final['schemaVersion'],6)
        self.assertNotIn('sourceRelease',final);self.assertEqual(final['viewerAcceptance'],{})
        self.assertEqual(boot['recoveryBindings'],final['recoveryBindings'])
        self.assertEqual(final['recoveryBindings']['main']['gateGeneration'],7)
        self.assertNotIn('PRIVATE_KEY',str(final));self.assertNotIn('PRIVATE KEY',str(final))
        self.assertEqual(viewer['VIEWER_DEPLOYMENT_ENVIRONMENT'],'production')
        self.assertEqual(viewer['VIEWER_SOURCE_RELEASE_ENABLED'],'false')
        self.assertFalse(receipt['runtimePreflightProven']);self.assertEqual(stat.S_IMODE(self.out.stat().st_mode),0o700)
        for path in self.out.iterdir():self.assertEqual(stat.S_IMODE(path.stat().st_mode),0o600)
        for name,pin in receipt['files'].items():
            self.assertEqual(hashlib.sha256((self.out/name).read_bytes()).hexdigest(),pin['sha256'])
        for role in self.keys.values():self.assertNotIn(prep.pinned(role['privateKey']).decode(),str(receipt))
        self.assertEqual(before,(self.fixture.root/'runtime.json').read_bytes())

    def test_reused_signing_key_refuses_before_any_output(self):
        self.spec['environments']['production']['copy']['privateKey']=self.keys['source']['privateKey']
        with self.assertRaisesRegex(Exception,'distinct_environment_signing_roles'):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_authority_and_original_gate_drift_refuse(self):
        self.spec['authorityId']=str(uuid.uuid4())
        with self.assertRaisesRegex(Exception,'physical_binding'):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_key_hash_drift_refuses(self):
        artifact=self.keys['source']['privateKey'];Path(artifact['path']).write_bytes(b'changed')
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_symlink_key_refuses(self):
        artifact=self.keys['source']['privateKey']; original=Path(artifact['path'])
        link=original.with_suffix('.link');link.symlink_to(original);artifact['path']=str(link)
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_hardlinked_key_refuses(self):
        original=Path(self.keys['source']['privateKey']['path']);os.link(original,original.with_suffix('.link'))
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_private_key_permissions_refuse(self):
        Path(self.keys['source']['privateKey']['path']).chmod(0o644)
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_token_collision_refuses(self):
        self.spec['environments']['production']['bearer']=self.spec['nodes'][0]['bearer']
        with self.assertRaisesRegex(Exception,'disjoint_private_roles'):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_unknown_environment_or_field_refuses(self):
        self.spec['environments']['development']=self.spec['environments'].pop('production')
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_unknown_spec_field_refuses_before_output(self):
        self.spec['sourceRelease']={'enabled':True}
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_gate_hash_matches_but_identity_differs_refuses(self):
        gate=copy.deepcopy(self.fixture.gate);gate['engineId']='wrong-daemon'
        self.spec['nodes'][0]['gate']=self.fixture.file('wrong-gate',prep.canonical(gate))
        # Even a separately repinned manifest cannot silently change physical identity.
        m=copy.deepcopy(self.fixture.m);m['gate']['sha256']=self.spec['nodes'][0]['gate']['sha256']
        self.spec['nodes'][0]['manifest']=self.fixture.file('repinned-manifest',prep.canonical(m))
        with self.assertRaisesRegex(Exception,'physical_binding'):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_partial_fsync_failure_retains_and_refuses_retry(self):
        with patch.object(prep.os,'fsync',side_effect=OSError('synthetic fsync failure')):
            with self.assertRaises(OSError):prep.prepare(self.spec)
        self.assertTrue(self.out.is_dir()); retained={p.name:p.read_bytes() for p in self.out.iterdir()}
        with self.assertRaisesRegex(Exception,'new_private_output'):prep.prepare(self.spec)
        self.assertEqual(retained,{p.name:p.read_bytes() for p in self.out.iterdir()})

    def test_existing_output_never_overwritten(self):
        self.out.mkdir(mode=0o700);(self.out/'sentinel').write_bytes(b'original')
        with self.assertRaisesRegex(Exception,'new_private_output'):prep.prepare(self.spec)
        self.assertEqual((self.out/'sentinel').read_bytes(),b'original')

    def test_non_ed25519_private_key_refuses(self):
        self.spec['environments']['production']['source']['privateKey']=self.fixture.spec['tls']['privateKey']
        with self.assertRaisesRegex(Exception,'ed25519_private_signer'):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())
