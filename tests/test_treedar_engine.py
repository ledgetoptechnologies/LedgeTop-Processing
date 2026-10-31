from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

from processing import reference_protocol as p
from processing.node_gate import NodeGate, GateError
from processing.scratch_policy import ScratchLedger
from processing.treedar_engine import (TreeDarEngine, TreeDarConfig, TreeDarHeld,
    SOURCE_FILES, REQUIRED_OUTPUTS, source_digest, _entries)


class FakeDocker:
    def __init__(self, config):
        self.config, self.calls = config, []
        self.id, self.state, self.exit_code, self.binding = 'a'*64, 'created', 0, None
        self.fail = None

    def __call__(self, command, timeout):
        args = command[3:]
        self.calls.append(args)
        action = args[0]
        if action == 'info':
            result = self.config.engine_id
        elif action == 'image':
            result = self.config.image_id
        elif action == 'create':
            self.binding = args[args.index('--label')+1].split('=',1)[1]
            result = self.id
        elif action == 'start':
            self.state, result = 'running', self.id
        elif action == 'inspect':
            result = json.dumps(dict(id=self.id,image=self.config.image_id,running=self.state=='running',
                status=self.state,exitCode=self.exit_code,binding=self.binding))
        elif action == 'stop':
            self.state, self.exit_code, result = 'exited', 143, self.id
        elif action == 'rm':
            result = self.id
        else:
            raise AssertionError(action)
        if action == self.fail:
            raise subprocess.TimeoutExpired('synthetic docker mutation', timeout)
        return subprocess.CompletedProcess(command,0,result,'')


class TreeDarPlatformTests(unittest.TestCase):
    def test_windows_launch_fails_before_local_state_creation(self):
        with patch('processing.treedar_engine.sys.platform','win32'):
            with self.assertRaisesRegex(TreeDarHeld,'linux_host_required'):
                TreeDarEngine('/unused',ledger=None,config=None,verify_provenance=None)


@unittest.skipUnless(sys.platform.startswith('linux'), 'requires Linux host ownership, Unix socket and durable directory semantics')
class TreeDarEngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.environment = patch.dict(os.environ, {'XDG_STATE_HOME':str(self.root / 'gate-state')})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.now = 100
        self.package = self.root / 'installed'
        (self.package / 'processing').mkdir(parents=True)
        repository = Path(__file__).resolve().parents[1]
        for name in SOURCE_FILES:
            shutil.copyfile(repository / 'processing' / name, self.package / 'processing' / name)
        self.unix = socket.socket(socket.AF_UNIX)
        self.unix.bind(str(self.root / 'docker.sock'))
        self.addCleanup(self.unix.close)
        self.config = TreeDarConfig(docker_executable=sys.executable,
            docker_socket=str(self.root / 'docker.sock'), image_id='sha256:'+'b'*64,
            engine_id='synthetic-test-engine',authority_id=str(uuid.uuid4()),
            coordinator_origin='http://127.0.0.1:8899',gate_generation=1,worker_id='worker-main',
            source_root=str(self.package),source_sha256=source_digest(self.package),
            uid=os.geteuid(),gid=os.getegid())
        with NodeGate(self.config.engine_id) as gate:
            gate.provision('main', self.config.coordinator_origin,self.config.authority_id)
        self.ledger = ScratchLedger(self.root / 'private' / 'scratch.sqlite',self.root,
            capacity_probe=lambda _:10**9,clock=lambda:self.now,safety_floor_bytes=0,cleanup_grace_seconds=0)
        directory = self.root / 'jobs' / 'r1'
        directory.mkdir(parents=True,mode=0o700)
        self.input = directory / 'cloud.input'
        self.input.write_bytes(b'abc')
        self.obj = dict(id='lidar1',kind='lidar',name='cloud.las',byteSize=3,sha256=hashlib.sha256(b'abc').hexdigest())
        self.manifest = dict(objects=[self.obj],totalBytes=3)
        self.manifest['sha256'] = p.manifest_sha256(self.manifest)
        self.pipeline = dict(odm=dict(requested=False,options={}),
            treedar=dict(requested=True,inputObjectIds=['lidar1'],provenanceReceiptId='prov1'),
            gaussian_splat=dict(requested=False))
        self.job = dict(jobId=str(uuid.uuid4()),generation=1,environment='staging',status='running',nodeId='main',
            manifestSha256=self.manifest['sha256'],pipelineDigest=p.digest(self.pipeline),
            submission=dict(manifest=self.manifest,pipeline=self.pipeline))
        self.provenance = dict(schemaVersion=1,receiptId='prov1',jobId=self.job['jobId'],generation=1,
            environment='staging',objectId='lidar1',objectSha256=self.obj['sha256'],byteSize=3,
            sourceKind='synthetic_lidar',verticalUnits='metre',verticalDatum='synthetic_local')
        self.ledger.reserve(job_id=self.job['jobId'],generation=1,reservation_id='r1',manifest_sha256=self.job['manifestSha256'],
            input_bytes=3,output_budget=1024**2,environment='staging',ttl_seconds=1000,worker_ready=True)
        self.ledger.record_allocated('r1',3)
        self.docker = FakeDocker(self.config)
        self.engine = self.open()

    def open(self):
        return TreeDarEngine(self.root / 'private' / 'engine.sqlite',ledger=self.ledger,config=self.config,
            verify_provenance=lambda receipt:receipt==self.provenance,runner=self.docker,clock=lambda:self.now)

    def start(self):
        return self.engine.start(self.job,reservation_id='r1',input_object=self.obj,
            input_relative_path='jobs/r1/cloud.input',provenance=self.provenance,allow_synthetic=True)

    def make_proof(self):
        identity = json.loads(self.engine._row(self.job['jobId'])['identity'])
        output = self.root / identity['outputRelativePath']
        run = output / 'run'
        run.mkdir()
        objects = []
        for name in sorted(REQUIRED_OUTPUTS):
            path = run / name
            path.write_bytes(b'synthetic output')
            relative = 'run/' + name
            objects.append(dict(id='treedar-'+hashlib.sha256(relative.encode()).hexdigest()[:32],
                role='report',byteSize=path.stat().st_size,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),relativePath=relative))
        admission = dict(jobId=self.job['jobId'],generation=1,environment='staging',objectId='lidar1',
            objectSha256=self.obj['sha256'],byteSize=3,sourceKind='synthetic_lidar',verticalUnits='metre',verticalDatum='synthetic_local')
        (output / 'proof.json').write_bytes(p.canonical_json(dict(schemaVersion=1,job=identity['job'],engineVersion='0.0.1',admission=admission,objects=objects)))
        self.docker.state = 'exited'
        return output

    def test_fixed_command_private_mounts_and_no_main_resource_limits(self):
        self.assertEqual(self.start(),'processing')
        create = next(args for args in self.docker.calls if args[0]=='create')
        for forbidden in ('--cpus','--memory','--pids-limit','--privileged','--volume'):
            self.assertNotIn(forbidden,create)
        self.assertFalse(any('OMP_NUM_THREADS' in arg or 'OPENBLAS_NUM_THREADS' in arg for arg in create))
        self.assertEqual(create[create.index('--network')+1],'none')
        self.assertEqual(create[create.index('--pull')+1],'never')
        self.assertIn('--read-only',create)
        self.assertNotIn(self.config.docker_socket,' '.join(create))
        self.assertEqual(create[-4:],['-B','-m','processing.treedar_engine','--inside'])
        self.assertIsNone(self.engine._row(self.job['jobId'])['deadline'])
        self.assertIsNone(self.engine.poll(self.job['jobId']))
        self.now=10**9
        self.assertIsNone(self.engine.poll(self.job['jobId']))
        self.assertFalse(any(args[0]=='stop' for args in self.docker.calls))

    def test_create_or_start_timeout_never_replays_mutation_after_restart(self):
        self.docker.fail='start'
        with self.assertRaises(TreeDarHeld):
            self.start()
        self.assertEqual(self.engine._row(self.job['jobId'])['phase'],'start_pending')
        self.docker.fail=None
        self.engine=self.open()
        self.assertEqual(self.start(),'start_pending')
        self.assertIsNone(self.engine.poll(self.job['jobId']))
        self.assertEqual(sum(args[0]=='create' for args in self.docker.calls),1)
        self.assertEqual(sum(args[0]=='start' for args in self.docker.calls),1)

    def test_unknown_create_retains_gate_and_created_container_is_not_restarted(self):
        self.docker.fail='create'
        with self.assertRaises(TreeDarHeld):
            self.start()
        self.docker.fail=None
        self.engine=self.open()
        self.assertEqual(self.start(),'create_pending')
        with self.assertRaisesRegex(TreeDarHeld,'container_terminal_state_unconfirmed'):
            self.engine.poll(self.job['jobId'])
        self.assertFalse(any(args[0]=='start' for args in self.docker.calls))
        with NodeGate(self.config.engine_id) as gate:
            self.assertIsNotNone(gate.snapshot()['reservation'])

    def test_provenance_expiry_during_preflight_refuses_before_gate_or_docker_mutation(self):
        original_host=self.engine._host
        for after,reason in ((120,'expired'),(90,'clock_rollback')):
            with self.subTest(reason=reason):
                self.now=100
                observations=[]
                def verify(receipt):
                    observations.append(self.now)
                    return receipt==self.provenance and 100<=self.now<110
                def elapsed_preflight():
                    original_host()
                    self.now=after
                self.engine.verifier=verify
                with patch.object(self.engine,'_host',elapsed_preflight):
                    with self.assertRaisesRegex(ValueError,'provenance_verification_failed'):
                        self.start()
                self.assertEqual(observations,[100,after])
                with NodeGate(self.config.engine_id) as gate:
                    self.assertIsNone(gate.snapshot()['reservation'])
                with self.engine._db() as db:
                    self.assertIsNone(db.execute('SELECT 1 FROM treedar_executions WHERE job=?',(self.job['jobId'],)).fetchone())
                self.assertFalse(any(args[0] in ('create','start') for args in self.docker.calls))

    def test_existing_launch_intent_does_not_repeat_new_admission_refresh(self):
        observations=[]
        def verify(receipt):
            observations.append(self.now)
            return receipt==self.provenance and len(observations)<=3
        self.engine.verifier=verify
        self.assertEqual(self.start(),'processing')
        self.assertEqual(len(observations),2)
        self.engine=self.open()
        self.engine.verifier=verify
        # Existing start retains the established initial validation, but never
        # calls _host/new-admission refresh or repeats an engine mutation.
        with patch.object(self.engine,'_host',side_effect=AssertionError('old intent readmitted')):
            self.assertEqual(self.start(),'processing')
        self.assertEqual(len(observations),3)
        self.assertEqual(sum(args[0]=='create' for args in self.docker.calls),1)
        self.assertEqual(sum(args[0]=='start' for args in self.docker.calls),1)

    def test_exact_stopped_success_proof_and_outputs_survive_replay(self):
        self.start()
        output=self.make_proof()
        proof=self.engine.poll(self.job['jobId'])
        self.assertEqual(proof['status'],'succeeded')
        self.assertTrue(proof['engineTerminal'])
        self.assertEqual(proof['resultManifest']['engineVersions'],{'treedar':'0.0.1'})
        self.assertEqual(len(proof['localObjects']),len(REQUIRED_OUTPUTS))
        self.assertEqual(self.open().poll(self.job['jobId']),proof)
        self.assertTrue((output/'run'/'dtm.tif').exists())
        with NodeGate(self.config.engine_id) as gate:
            self.assertIsNotNone(gate.snapshot()['reservation'])  # driver ACK owns release.

    def test_terminal_journal_before_ledger_settlement_recovers_without_launch(self):
        self.start(); self.make_proof()
        with patch.object(self.ledger,'set_engine_state',side_effect=OSError('synthetic crash')):
            with self.assertRaises(OSError):
                self.engine.poll(self.job['jobId'])
        self.assertIsNotNone(self.engine._row(self.job['jobId'])['proof'])
        self.engine=self.open()
        self.assertEqual(self.engine.poll(self.job['jobId'])['status'],'succeeded')
        with self.ledger._transaction() as db:
            self.assertEqual(self.ledger._reservation(db,'r1')['state'],'terminal')
        self.assertEqual(sum(args[0]=='start' for args in self.docker.calls),1)

    def test_nonzero_exit_is_failure_only_after_exact_stopped_inspection(self):
        self.start()
        self.docker.exit_code=1
        self.assertIsNone(self.engine.poll(self.job['jobId']))
        self.docker.state='exited'
        proof=self.engine.poll(self.job['jobId'])
        self.assertEqual(proof['status'],'failed')
        self.assertTrue(proof['engineTerminal'])

    def test_changed_output_hash_or_unlisted_output_retains_gate_and_files(self):
        self.start(); output=self.make_proof()
        (output/'run'/'dtm.tif').write_bytes(b'changed')
        with self.assertRaises(TreeDarHeld):
            self.engine.poll(self.job['jobId'])
        self.assertIsNone(self.engine._row(self.job['jobId'])['proof'])
        self.assertTrue((output/'run'/'dtm.tif').exists())

    def test_explicit_timeout_stop_uncertainty_never_repeats_stop(self):
        self.config=replace(self.config,timeout_seconds=600)
        self.docker.config=self.config
        # Fresh owned journal configuration, without modifying the old database.
        self.engine=TreeDarEngine(self.root/'private'/'timed.sqlite',ledger=self.ledger,config=self.config,
            verify_provenance=lambda r:r==self.provenance,runner=self.docker,clock=lambda:self.now)
        self.start()
        self.now=800
        self.docker.fail='stop'
        with self.assertRaises(TreeDarHeld):
            self.engine.poll(self.job['jobId'])
        self.docker.fail=None
        self.docker.state='running'
        with self.assertRaisesRegex(TreeDarHeld,'container_stop_requires_reconciliation'):
            self.engine.poll(self.job['jobId'])
        self.assertEqual(sum(args[0]=='stop' for args in self.docker.calls),1)

    def test_config_and_physical_gate_binding_changes_are_refused(self):
        for changed in ({'coordinator_origin':'http://user:secret@127.0.0.1:8899'},
                        {'coordinator_origin':'http://127.0.0.1:8899/path'},
                        {'engine_version':'https://example.invalid/secret'}):
            with self.subTest(change=changed), self.assertRaises((ValueError,GateError)):
                replace(self.config,**changed)
        other=replace(self.config,image_id='sha256:'+'c'*64)
        with self.assertRaisesRegex(TreeDarHeld,'execution_configuration_changed'):
            TreeDarEngine(self.root/'private'/'engine.sqlite',ledger=self.ledger,config=other,
                verify_provenance=lambda _:True,runner=self.docker)
        self.docker.config=replace(self.config,engine_id='other-engine')
        with self.assertRaisesRegex(TreeDarHeld,'physical_docker_engine_changed'):
            self.start()
        self.assertFalse(any(args[0]=='create' for args in self.docker.calls))

    def test_directory_enumeration_stops_at_configured_bound(self):
        directory=self.root/'enumeration'
        directory.mkdir()
        for index in range(3):
            (directory/str(index)).write_bytes(b'')
        with self.assertRaisesRegex(TreeDarHeld,'output_entry_budget_exceeded'):
            _entries(directory,2)
        self.assertEqual(len(_entries(directory,3)),3)

    def test_optional_removal_targets_exact_stopped_container_and_retains_files(self):
        self.start(); output=self.make_proof()
        self.engine.poll(self.job['jobId'])
        self.engine.remove_container(self.job['jobId'])
        self.assertEqual([args for args in self.docker.calls if args[0]=='rm'],[['rm','a'*64]])
        self.assertTrue((output/'run'/'dtm.tif').exists())
        self.engine.remove_container(self.job['jobId'])
        self.assertEqual(sum(args[0]=='rm' for args in self.docker.calls),1)


if __name__=='__main__':
    unittest.main()
