"""Actual pure-validated preparation inputs; no installed/start/readiness claim."""
import copy
import hashlib
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from ops import worker_prestart_review as review
from ops import prepare_reference_worker_config as prep
from tests.test_enrolled_coordinator_deployment import bundle as bundle_fixture
from tests.test_prepare_reference_worker_config import PrivateWorkerPreparationTests


@unittest.skipUnless(os.name == 'posix' and os.geteuid() != 0, 'private nonroot Linux preparation fixtures')
class WorkerPrestartReviewTests(unittest.TestCase):
    def setUp(self):
        self.bundle, self.plan = bundle_fixture()
        self.entries = []; self.bundle['workers'] = []; common_install = None
        for node in ('cluster', 'main'):
            f = PrivateWorkerPreparationTests(); f.setUp(); self.addCleanup(f.doCleanups)
            if common_install is None: common_install = f.install
            f.m['installationRoot'] = str(common_install/f.op)
            patcher = patch.object(prep.install, 'INSTALL_BASE', common_install)
            patcher.start(); self.addCleanup(patcher.stop)
            f.authority = self.plan['authorityId']
            engine = 'prepared-' + node
            gate = f.home/'.local/state/LedgeTop-Processing/node-gates'/hashlib.sha256(engine.encode()).hexdigest()/'gate.json'
            gate.parent.mkdir(parents=True, mode=0o700)
            f.gate.update(engineId=engine, nodeId=node, authorityId=f.authority)
            gate.write_bytes(prep.canonical(f.gate)); gate.chmod(0o600)
            f.m['host']['dockerEngineId'] = engine
            f.m['gate'].update(path=str(gate), sha256=hashlib.sha256(gate.read_bytes()).hexdigest(),
                              nodeId=node, authorityId=f.authority)
            f.m['unitSha256'] = hashlib.sha256(prep.install.render_unit(f.m).encode()).hexdigest()
            worker = f.runtime['workers'].pop('main'); binding = f.runtime['recoveryBindings'].pop('main')
            binding.update(engineId=engine, authorityId=f.authority)
            f.runtime['workers'][node] = worker; f.runtime['recoveryBindings'][node] = binding; f.update_runtime()
            receipt = prep.prepare(f.spec, now=f.now)
            manifest = json.loads((f.output/'enrollment-manifest.json').read_bytes())
            config = json.loads((f.output/'worker-config.json').read_bytes())
            entry = copy.deepcopy(bundle_fixture()[0]['workers'][0])
            entry.update(nodeId=node, sshUid=os.getuid())
            root, home = Path(manifest['installationRoot']), Path(manifest['service']['home'])
            entry['settings'].update(unitName='ledgetop-processing-reference-worker.service',
                unitFile=str(home/'.config/systemd/user/ledgetop-processing-reference-worker.service'),
                unitSha256=manifest['unitSha256'], pythonExecutable=str(root/'venv/bin/python'),
                configPath=str(home/'.config/ledgetop-processing/reference-worker.json'),
                sourceRoot=str(root/'source'), sourceSha256=manifest['source']['footprintSha256'])
            entry['expected'].update(nodeId=node, workerId=config['workerId'], workerInstanceId=config['workerInstanceId'],
                authorityId=f.authority, engineId=engine, gateGeneration=7, engineVersion=manifest['engine']['version'],
                configSha256=receipt['workerConfigSha256'], sourceSha256=manifest['source']['footprintSha256'])
            self.bundle['workers'].append(entry)
            def reference(path, data):
                path.write_bytes(data); path.chmod(0o600)
                return dict(path=str(path), sha256=hashlib.sha256(data).hexdigest())
            self.entries.append(dict(nodeId=node, workerReferenceSha256=review.digest(entry),
                manifest=reference(f.root/'review-manifest.json', review.canonical(manifest)),
                preparationReceipt=reference(f.root/'review-prepared.json', review.canonical(receipt)),
                enrollment=dict(kind='install-intent', operationId=manifest['operationId'], manifestSha256=review.digest(manifest))))
        self.raw = review.compose(self.bundle, self.plan, self.entries)

    def test_deterministic_closed_review_reads_no_worker_config_or_credentials(self):
        allowed = {d['path'] for e in self.entries for d in (e['manifest'], e['preparationReceipt'])}
        actual = review.private_bytes
        def read(path, **kwargs):
            self.assertIn(str(path), allowed)
            return actual(path, **kwargs)
        with patch.object(review, 'private_bytes', side_effect=read):
            self.assertEqual(review.compose(self.bundle, self.plan, self.entries), self.raw)
            value = review.validate(self.raw, self.bundle, self.plan)
        self.assertEqual(value['scope'], 'configuration-review')
        self.assertNotIn('PRIVATE KEY', self.raw.decode())
        self.assertNotIn('planSha256', value); self.assertNotIn('deploymentSha256', value)

    def test_opaque_fixture_and_unknown_fields_refuse(self):
        for raw in (review.canonical(dict(fixtureOnly=True, installedQualified=False)),
                    review.canonical(dict(json.loads(self.raw), installedQualified=True))):
            with self.assertRaises(Exception): review.validate(raw, self.bundle, self.plan)

    def test_candidate_operation_and_worker_substitutions_refuse(self):
        for mutate in (lambda b,p:p['candidate'].update(sourceCommit='9'*40),
                       lambda b,p:p.update(authorityId='00000000-0000-4000-8000-000000000001'),
                       lambda b,p:b['workers'][0]['settings'].update(pythonExecutable='/usr/bin/python3'),
                       lambda b,p:b['workers'][0]['expected'].update(gateGeneration=8)):
            bundle, plan = copy.deepcopy(self.bundle), copy.deepcopy(self.plan); mutate(bundle, plan)
            with self.assertRaises(Exception): review.validate(self.raw, bundle, plan)

    def test_placeholder_completed_manifest_and_changed_receipt_refuse(self):
        for section, key, value in (('config', 'sha256', '0'*64), ('config', 'bytes', 1),
                                    ('dependencies', 'pythonSha256', '0'*64)):
            entries = copy.deepcopy(self.entries); descriptor = entries[0]['manifest']
            path = Path(descriptor['path']); original = path.read_bytes(); manifest = json.loads(original)
            manifest[section][key] = value; raw = review.canonical(manifest); path.write_bytes(raw)
            descriptor['sha256'] = hashlib.sha256(raw).hexdigest()
            try:
                with self.assertRaises(Exception): review.compose(self.bundle, self.plan, entries)
            finally: path.write_bytes(original)
        Path(self.entries[0]['preparationReceipt']['path']).write_bytes(b'{}')
        with self.assertRaises(Exception): review.validate(self.raw, self.bundle, self.plan)

    def test_install_intent_cannot_claim_installed_tree_or_ready(self):
        entries = copy.deepcopy(self.entries); entries[0]['enrollment']['installedTreeSha256'] = 'a'*64
        with self.assertRaises(Exception): review.compose(self.bundle, self.plan, entries)

    def test_historical_enrollment_is_bound_but_is_not_readiness(self):
        entries = copy.deepcopy(self.entries)
        manifest = json.loads(Path(entries[0]['manifest']['path']).read_bytes())
        receipt = dict(schemaVersion=1, protocol=prep.install.PROTOCOL, operationId=manifest['operationId'],
            manifestSha256=review.digest(manifest), sourceSha256=manifest['source']['footprintSha256'],
            configSha256=manifest['config']['sha256'], unitSha256=manifest['unitSha256'], unitInstalled=True,
            serviceStartIssued=False, serviceEnableIssued=False, engineMutationIssued=False,
            installedTreeSchemaVersion=1, installedTreeSha256='a'*64)
        path = Path(entries[0]['manifest']['path']).parent/'historical-enrollment.json'
        raw = review.canonical(receipt); path.write_bytes(raw); path.chmod(0o600)
        entries[0]['enrollment'].update(kind='enrolled-static', receipt=dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest()))
        value = review.validate(review.compose(self.bundle,self.plan,entries),self.bundle,self.plan)
        self.assertEqual(value['scope'],'configuration-review')
        receipt['serviceStartIssued'] = True; raw=review.canonical(receipt);path.write_bytes(raw)
        entries[0]['enrollment']['receipt']['sha256'] = hashlib.sha256(raw).hexdigest()
        with self.assertRaises(Exception):review.compose(self.bundle,self.plan,entries)

    def test_producer_is_exclusive_and_bound_gate_accepts_only_exact_bytes(self):
        root = Path(self.entries[0]['manifest']['path']).parent
        def private(name,value):
            p=root/name;p.write_bytes(review.canonical(value));p.chmod(0o600);return str(p)
        spec=dict(bundlePath=private('bundle.json',self.bundle),planPath=private('plan.json',self.plan),
                  entries=self.entries,outputPath=str(root/'review.json'))
        receipt=review.prepare(spec)
        self.assertEqual(receipt['sha256'],hashlib.sha256(self.raw).hexdigest())
        with self.assertRaises(FileExistsError):review.prepare(spec)
        self.plan['gates']['workerQualificationSha256']=receipt['sha256']
        self.bundle['qualifications']['workerQualificationSha256']=dict(path=spec['outputPath'],sha256=receipt['sha256'])
        self.assertEqual(review.verify(self.bundle,self.plan)['scope'],'configuration-review')
        Path(spec['outputPath']).write_bytes(b'{}')
        with self.assertRaises(Exception):review.verify(self.bundle,self.plan)

    def test_producer_failure_retains_partial_without_reusing_it(self):
        root=Path(self.entries[0]['manifest']['path']).parent
        paths=[]
        for name,value in (('bundle.json',self.bundle),('plan.json',self.plan)):
            p=root/name;p.write_bytes(review.canonical(value));p.chmod(0o600);paths.append(str(p))
        spec=dict(bundlePath=paths[0],planPath=paths[1],entries=self.entries,outputPath=str(root/'partial-review.json'))
        with patch.object(review.os,'fsync',side_effect=OSError('synthetic fsync')):
            with self.assertRaises(OSError):review.prepare(spec)
        self.assertTrue(Path(spec['outputPath']).exists())
        with self.assertRaises(FileExistsError):review.prepare(spec)

    def test_global_lock_rejects_before_host_or_journal_effect(self):
        from ops.enrolled_coordinator_driver import ConcreteCutoverDriver
        from unittest.mock import Mock
        driver = object.__new__(ConcreteCutoverDriver); driver.plan = self.plan; driver.bundle = self.bundle
        driver.host_operation = Mock()
        with patch.object(review, 'verify', side_effect=RuntimeError('typed review refused')):
            with self.assertRaisesRegex(RuntimeError, 'typed review refused'): driver.global_lock(self.plan)
        driver.host_operation.locked.assert_not_called()
