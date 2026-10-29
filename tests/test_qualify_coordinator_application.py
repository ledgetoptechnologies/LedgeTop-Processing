"""Synthetic local qualifier contracts; never reads installed private material."""
from dataclasses import asdict, replace
import copy
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from ops import qualify_coordinator_application as q
from ops import incremental_coordinator_binding as incremental
from tests import test_incremental_coordinator_binding as fixtures


def oracle_docker_args(*args):
    if os.name == 'nt':
        return ['C:/Program Files/Docker/Docker/resources/bin/docker.exe', '--host', 'npipe:////./pipe/dockerDesktopLinuxEngine', *args]
    return ['/usr/bin/docker', '--host', 'unix:///run/docker.sock', *args]


def oracle_docker_env():
    env = dict(os.environ)
    for key in ('DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH', 'DOCKER_API_VERSION'):
        env.pop(key, None)
    return env


@unittest.skipUnless(os.name == 'posix', 'Linux private descriptors and state ownership')
class QualifierTests(unittest.TestCase):
    def setUp(self):
        self.host = fixtures.IncrementalCoordinatorTests()
        self.host.setUp(); self.addCleanup(self.host.doCleanups)
        self.operation = self.host.root/'qualification-operation'
        self.env_raw = b'PROCESSING_API_TOKEN=synthetic-only-credential-not-a-real-secret\n'
        self.env_path = self.host.root/'synthetic-runtime.env'
        self.env_path.write_bytes(self.env_raw); self.env_path.chmod(0o600)
        for name, value in (('ENV_FILE', str(self.env_path)), ('OPERATION_ROOT', str(self.host.root))):
            scoped = patch.object(q, name, value); scoped.start(); self.addCleanup(scoped.stop)
        env = dict(PROCESSING_API_TOKEN='synthetic-only-credential-not-a-real-secret', PROCESSING_REFERENCE_CONFIG=q.CONFIG)
        self.binding = replace(self.host.binding, application_environment_sha256=q.digest(env))
        self.profile = q.QualificationBinding(asdict(self.binding), 'sha256:'+'b'*64, 'synthetic-local-engine-0001', native_env_file=str(self.env_path))
        self.baseline = dict(census=self.host.baseline, logicalSha256='a'*64, privateMaterialSha256='b'*64)
        self.calls = []
        self.original_file = q.private_file

    def read_file(self, path, limit=1048576):
        if str(path) == q.ENV_FILE: return self.env_raw
        return self.original_file(path, limit)

    def docker(self, *args, stdin=None):
        self.calls.append((args, stdin))
        if args[0] == 'info': return self.profile.engine_id
        if args[0] == 'inspect':
            return json.dumps([dict(Id=self.binding.predecessor_container, Image=self.binding.predecessor_image,
                State=dict(Running=True), Config=dict(User='10001:10001', Env=['PATH=/synthetic',
                    'PROCESSING_API_TOKEN=synthetic-only-credential-not-a-real-secret', 'PROCESSING_REFERENCE_CONFIG='+q.CONFIG]),
                Mounts=[dict(Type='volume', Name=q.LIVE_VOLUME, Destination='/prototype-state', RW=True),
                        dict(Type='bind', Source=q.TLS_ROOT, Destination='/run/processing-reference', RW=False)])])
        if args[:2] == ('image', 'inspect'):
            return json.dumps([dict(Id=args[2], Config=dict(User='10001:10001', Env=['PATH=/synthetic']))])
        if args[:2] == ('volume', 'ls'): return ''
        if args[:2] == ('volume', 'create'):
            self.assertTrue((self.operation/'volume-intent.json').exists()); return args[-1]
        if args[:2] == ('volume', 'inspect'):
            return json.dumps([dict(Name=args[2], Driver='local', Scope='local', Options=None,
                Labels={'com.ledgetop.qualifier':q.digest(asdict(self.profile))},
                Mountpoint='/var/lib/docker/volumes/'+args[2]+'/_data', CreatedAt='2026-10-28T00:00:00Z')])
        self.assertEqual(args[0], 'run')
        if '--helper' not in args:
            self.assertTrue((self.operation/'initialize-intent.json').exists())
            self.assertEqual(args[-1], q.initializer_source()); return ''
        phase = args[args.index('--helper')+1]
        envelope = json.loads(stdin)
        self.assertEqual(set(envelope), {'helpers', 'document'})
        document = envelope['document']
        self.assertEqual(document['binding'], asdict(self.profile))
        self.assertEqual(document['applicationEnvironmentKeys'], ['PROCESSING_API_TOKEN', 'PROCESSING_REFERENCE_CONFIG'])
        name = args[args.index('--name')+1]
        suffix = name.rsplit('-', 1)[1]
        self.assertTrue((self.operation/(suffix+'-intent.json')).exists())
        if phase == 'snapshot':
            self.assertIsNone(document['baseline']); return json.dumps(self.baseline)
        self.assertEqual(document['baseline'], self.baseline)
        return json.dumps(dict(state='qualified', logicalSha256='a'*64, privateMaterialSha256='b'*64,
            authorityId=self.binding.authority_id, recoveryEnabled=False, nativeClosed=True, referenceClosed=True))

    def qualify(self, transport=None):
        with patch.object(q, 'private_file', side_effect=self.read_file):
            return q.qualify(self.profile, self.operation, docker=transport or self.docker)

    def test_exact_surgical_pair_allowed_without_changing_old_profile_serialization(self):
        surgical = replace(self.binding,
            predecessor_source='6bbd468e1177746cb695e43195e75355b0681f23',
            candidate_source='d738edb209972a1723fa5af374578951b2a8448c')
        profile = replace(self.profile, incremental=asdict(surgical))
        self.assertEqual(profile.validate(), surgical)
        self.assertEqual(set(asdict(surgical)), set(asdict(self.binding)))
        self.assertEqual(q.SOURCES[surgical.candidate_source], (63, '86aa245deef9497cd1a512c9e9ce750a3c7ebf5f75077fe25e9d95be2b4a37a8'))
        self.assertEqual(self.profile.validate(), self.binding)
        for old, new in [(surgical.candidate_source,surgical.predecessor_source),
                         (self.binding.predecessor_source,surgical.candidate_source),
                         (surgical.predecessor_source,'f'*40)]:
            invalid = replace(surgical, predecessor_source=old, candidate_source=new)
            with self.subTest(pair=(old,new)), self.assertRaises(q.QualificationHeld):
                replace(profile, incremental=asdict(invalid)).validate()

    def test_full_intent_order_and_strict_isolation_recipe_without_secret_arguments(self):
        result = self.qualify()
        self.assertEqual(result['state'], 'qualified_copy_retained')
        self.assertEqual(result['completedPhases'], ['snapshot', 'old', 'candidate', 'rollback'])
        self.assertFalse(any(args[:2] == ('volume', 'rm') for args, _ in self.calls))
        for args, _ in self.calls:
            text = ' '.join(args)
            self.assertNotIn('synthetic-only-credential', text)
            self.assertNotIn('--publish', args); self.assertNotIn('-p', args)
            if args[0] != 'run': continue
            self.assertEqual(args[args.index('--network')+1], 'none')
            self.assertIn('--read-only', args)
            for arg in args:
                if q.LIVE_VOLUME in arg: self.assertTrue(arg.endswith(',readonly'))
        self.assertTrue((self.operation/'receipt.json').exists())
        self.assertEqual((self.operation/'binding.json').stat().st_mode & 0o777, 0o600)

    def test_uncertain_phase_retains_copy_and_original_intent_without_replay(self):
        def uncertain(*args, **kwargs):
            if '--helper' in args:
                self.calls.append((args, kwargs.get('stdin')))
                raise q.QualificationHeld('synthetic_unknown_response')
            return self.docker(*args, **kwargs)
        with self.assertRaises(q.QualificationHeld): self.qualify(uncertain)
        self.assertTrue((self.operation/'snapshot-intent.json').exists())
        self.assertFalse((self.operation/'snapshot.json').exists())
        before = len(self.calls)
        with self.assertRaises(FileExistsError): self.qualify()
        self.assertEqual(len(self.calls), before)
        self.assertFalse(any(args[:2] == ('volume', 'rm') for args, _ in self.calls))

    def test_foreign_private_paths_and_reverse_source_pair_are_refused(self):
        with self.assertRaises(q.QualificationHeld): replace(self.profile, native_env_file='/tmp/another-env').validate()
        reverse = asdict(self.binding)
        reverse['predecessor_source'], reverse['candidate_source'] = reverse['candidate_source'], reverse['predecessor_source']
        with self.assertRaises(q.QualificationHeld): replace(self.profile, incremental=reverse).validate()

    def test_wrong_live_mount_or_environment_holds_before_volume_creation(self):
        for mode in ('mount', 'environment'):
            self.operation = self.host.root/('qualification-'+mode)
            def bad(*args, **kwargs):
                raw = self.docker(*args, **kwargs)
                if args[0] == 'inspect':
                    value = json.loads(raw)
                    if mode == 'mount': value[0]['Mounts'][1]['RW'] = True
                    else: value[0]['Config']['Env'][-1] = 'PROCESSING_REFERENCE_CONFIG=/changed'
                    return json.dumps(value)
                return raw
            with self.subTest(mode=mode), self.assertRaises(q.QualificationHeld): self.qualify(bad)
            self.assertFalse(any(args[:2] == ('volume', 'create') for args, _ in self.calls))

    def test_foreign_or_nonclosed_helper_response_never_records_qualification(self):
        def bad(*args, **kwargs):
            raw = self.docker(*args, **kwargs)
            if '--helper' in args and args[args.index('--helper')+1] == 'application':
                value = json.loads(raw); value['authorityId'] = 'different'; return json.dumps(value)
            return raw
        with self.assertRaises(q.QualificationHeld): self.qualify(bad)
        self.assertFalse((self.operation/'receipt.json').exists())

    def test_numeric_boolean_helper_receipt_cannot_bypass_exact_contract(self):
        def bad(*args, **kwargs):
            raw = self.docker(*args, **kwargs)
            if '--helper' in args and args[args.index('--helper')+1] == 'application':
                value = json.loads(raw); value['recoveryEnabled'] = 0; return json.dumps(value)
            return raw
        with self.assertRaises(q.QualificationHeld): self.qualify(bad)
        self.assertFalse((self.operation/'receipt.json').exists())

    def test_operation_root_escape_holds_before_docker_or_directory_creation(self):
        self.operation = self.host.root/'another-parent'/'operation'
        with self.assertRaises(q.QualificationHeld): self.qualify()
        self.assertEqual(self.calls, [])
        self.assertFalse(self.operation.exists())

    def test_local_docker_ignores_redirect_context_and_pins_unix_socket(self):
        result = SimpleNamespace(returncode=0, stdout=b'local', stderr=b'')
        with patch.object(q.LocalDocker, '_facts', return_value=('fixed-local-identity',)), \
                patch.dict(os.environ, {'DOCKER_HOST':'tcp://foreign:2375', 'DOCKER_CONTEXT':'foreign', 'DOCKER_TLS_VERIFY':'1'}), \
                patch.object(q.subprocess, 'run', return_value=result) as run:
            self.assertEqual(q.LocalDocker()('info', '--format', '{{.ID}}'), 'local')
        self.assertEqual(run.call_args.args[0][:3], ['/usr/bin/docker', '--host', 'unix:///run/docker.sock'])
        self.assertNotIn('DOCKER_HOST', run.call_args.kwargs['env'])
        self.assertNotIn('DOCKER_CONTEXT', run.call_args.kwargs['env'])
        self.assertNotIn('DOCKER_TLS_VERIFY', run.call_args.kwargs['env'])

    def test_wrong_engine_identity_holds_before_volume_creation(self):
        def bad(*args, **kwargs):
            if args[0] == 'info': return 'another-engine-0001'
            return self.docker(*args, **kwargs)
        with self.assertRaisesRegex(q.QualificationHeld, 'local_engine_identity_changed'): self.qualify(bad)
        self.assertFalse(any(args[:2] == ('volume', 'create') for args, _ in self.calls))

    def test_foreign_volume_label_or_changed_identity_holds_before_initializer(self):
        for mode in ('label', 'identity'):
            self.operation = self.host.root/('volume-'+mode)
            inspected = []
            def bad(*args, **kwargs):
                raw = self.docker(*args, **kwargs)
                if args[:2] == ('volume', 'inspect'):
                    inspected.append(True); value = json.loads(raw)
                    if mode == 'label': value[0]['Labels'] = {'foreign':'not-ours'}
                    elif len(inspected) > 1: value[0]['CreatedAt'] = '2026-10-29T00:00:00Z'
                    return json.dumps(value)
                return raw
            with self.subTest(mode=mode), self.assertRaises(q.QualificationHeld): self.qualify(bad)
        self.assertFalse(any(args[0] == 'run' for args, _ in self.calls))

    def test_in_place_environment_restore_is_detected_by_host_fingerprint(self):
        def change(*args, **kwargs):
            raw = self.docker(*args, **kwargs)
            if '--helper' in args and args[args.index('--helper')+1] == 'snapshot':
                self.env_path.write_bytes(self.env_raw)
            return raw
        with self.assertRaisesRegex(q.QualificationHeld, 'environment_file_identity_changed'): self.qualify(change)
        self.assertTrue((self.operation/'snapshot.json').exists())
        self.assertFalse((self.operation/'old-intent.json').exists())

    def actual_snapshot(self, *, changing=False):
        destination = self.host.root/'copy'; destination.mkdir(mode=0o700)
        original_census = incremental.volume_census
        original_connect = q.sqlite3.connect
        before = (self.host.volume/'jobs.sqlite3').read_bytes()
        observed = []
        def census(root, **kwargs):
            if changing and str(root) == '/source' and len(observed) == 1:
                with self.host.queue._db() as db:
                    db.execute("UPDATE nodes SET revision=revision+1 WHERE id='main'")
            value = original_census(self.host.volume if str(root) == '/source' else root,
                authority=kwargs['authority'], uid=os.geteuid())
            if str(root) == '/source': observed.append(value)
            return value
        def connect(path, *args, **kwargs):
            if path == 'file:/source/jobs.sqlite3?mode=ro': path = self.host.volume.joinpath('jobs.sqlite3').as_uri()+'?mode=ro'
            return original_connect(path, *args, **kwargs)
        def files(path, limit=1048576):
            if str(path).startswith('/source/'):
                path = self.host.volume/Path(path).name
            return self.original_file(path, limit)
        with patch.object(incremental, 'volume_census', side_effect=census), patch.object(q, 'material_binding',
                return_value=(SimpleNamespace(prototype_state_dir=str(destination)), {})), \
                patch.object(q, 'private_material', return_value='b'*64), \
                patch.object(q.sqlite3, 'connect', side_effect=connect), patch.object(q, 'private_file', side_effect=files):
            try: result = q.snapshot(self.binding)
            finally:
                if changing: self.assertNotEqual((self.host.volume/'jobs.sqlite3').read_bytes(), before)
                else: self.assertEqual((self.host.volume/'jobs.sqlite3').read_bytes(), before)
        return result

    def test_real_consistent_sqlite_backup_and_full_census_preserve_source(self):
        result = self.actual_snapshot()
        self.assertEqual(result['logicalSha256'], q.logical(self.host.volume/'jobs.sqlite3'))
        incremental.compare_transition(result['census'], self.host.baseline, binding=self.binding)

    def test_source_change_after_backup_never_returns_a_valid_snapshot(self):
        with self.assertRaisesRegex(q.QualificationHeld, 'live_source_changed_during_snapshot'):
            self.actual_snapshot(changing=True)

    def test_private_file_bounds_and_symlinks_refuse_without_writes(self):
        path = self.host.root/'private-test'; path.write_bytes(b'abcdef')
        self.assertEqual(q.private_file(path, 6), b'abcdef')
        with self.assertRaises(q.QualificationHeld): q.private_file(path, 5)
        alias = self.host.root/'private-alias'; alias.symlink_to(path)
        with self.assertRaises(q.QualificationHeld): q.private_file(alias)
        self.assertEqual(path.read_bytes(), b'abcdef')


def synthetic_seed():
    """Private helper integration fixture ONLY, never an installed config reader."""
    q.exact_source(tuple(q.SOURCES)[0])
    from processing.app import make_handler
    from processing.config import from_environment
    import uuid
    root = Path('/run/processing-reference')
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
        '-subj', '/CN=synthetic-private-qualifier', '-addext', 'subjectAltName=IP:127.0.0.1',
        '-addext', 'basicConstraints=critical,CA:TRUE', '-keyout', str(root/'key.pem'),
        '-out', str(root/'cert.pem')], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    (root/'key.pem').chmod(0o600)
    value = dict(schemaVersion=1, listener=dict(host='0.0.0.0', port=8443, certificate=str(root/'cert.pem'),
        privateKey=str(root/'key.pem'), clientCA=str(root/'cert.pem')), viewerBearers={'staging':'synthetic-reference-'+'v'*32},
        workers={node:dict(bearer='synthetic-worker-'+node+'-'+'w'*32, workerId='worker-'+node, instanceId='instance-'+node,
            resultOrigin='https://127.0.0.1:19999', resultBearer='synthetic-reader-'+node+'-'+'r'*32)
            for node in ('cluster', 'main')}, streamBudget=dict(maxConcurrent=1, maxObjectBytes=4194304))
    runtime = root/'runtime.json'; runtime.write_bytes(q.packed(value)); runtime.chmod(0o600)
    handler = make_handler(from_environment()); queue = handler.prototype_queue
    try:
        with queue._db() as db:
            for index in range(12):
                db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,'staging','treedar-synthetic-v1',?)",
                    ('synthetic-'+str(index), 'request-'+str(index), 'failed' if index < 2 else 'succeeded'))
        for node in ('cluster', 'main'):
            receipt = queue.schedule_node_owned(node, dict(schemaVersion=1, requestId=str(uuid.uuid4()),
                authorityId=queue.authority_id, expectedRevision=queue.get_node(node)['revision'],
                drainAt='2026-01-01T00:00:00Z', notBefore='2026-01-01T00:00:00Z', afterJobId=None, targetImage='sha256:'+'a'*64))
            queue.cancel_node(node, dict(expectedRevision=receipt['revision'], planId=receipt['planId']))
    finally:
        handler.reference_listener.close(); handler.viewer_service.close(); queue.close()
    census = incremental.volume_census('/prototype-state', authority=queue.authority_id)
    return dict(census=census, configSha256=q.hashlib.sha256(runtime.read_bytes()).hexdigest())


class PrivateFileByteExactTests(unittest.TestCase):
    def test_crlf_and_binary_bytes_are_read_exactly_without_translation(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary).resolve()/'source.py'
            raw = b"first\r\nsecond\r\n\x1a\x00last\r\n"
            path.write_bytes(raw)
            before = path.stat()
            self.assertEqual(q.private_file(path, len(raw)), raw)
            after = path.stat()
            self.assertEqual((before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns),
                             (after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns))
            with self.assertRaises(q.QualificationHeld):
                q.private_file(path, len(raw)-1)
            self.assertEqual(path.read_bytes(), raw)


@unittest.skipUnless(os.environ.get('PRIVATE_QUALIFIER_IMAGE_ORACLE') == '1', 'explicit synthetic helper-image oracle required')
class SyntheticPrivateHelperImageTests(unittest.TestCase):
    def test_nonempty_owned_volume_initializer_preserves_root_and_sentinel(self):
        import uuid
        volume = 'ltds-synthetic-nonempty-qualifier-'+uuid.uuid4().hex
        image = 'sha256:649f53f2b5dc32cd9eb229c42b6bbd7b479249fba5a8d65c48d8afc4f157b249'
        label = 'com.ledgetop.qualifier='+q.digest(dict(syntheticNonemptyVolume=True))
        def call(*args):
            return subprocess.run(oracle_docker_args(*args), capture_output=True, timeout=20, env=oracle_docker_env())
        created = call('volume', 'create', '--label', label, volume)
        self.assertEqual(created.returncode, 0, created.stderr.decode())
        try:
            common = ['run', '--rm', '--pull', 'never', '--network', 'none', '--read-only', '--cpus', '2', '--memory', '2g', '--pids-limit', '128',
                '--cap-drop', 'ALL', '--cap-add', 'CHOWN', '--security-opt', 'no-new-privileges', '--user', '0', '--log-driver', 'none']
            def invoke(code, *, readonly=False):
                return call(*common, '--mount', 'type=volume,src='+volume+',dst=/copy'+(',readonly' if readonly else ''),
                    '--entrypoint', 'python', image, '-B', '-W', 'error::ResourceWarning', '-c', code)
            setup = invoke('import os;os.chmod("/copy",0o755);fd=os.open("/copy/sentinel",os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.write(fd,b"synthetic-sentinel-retain");os.fsync(fd);os.close(fd)')
            self.assertEqual(setup.returncode, 0, setup.stderr.decode())
            inspect_code = 'import os,stat,json,hashlib;info=os.lstat("/copy");fd=os.open("/copy/sentinel",os.O_RDONLY);raw=os.read(fd,128);os.close(fd);print(json.dumps(dict(uid=info.st_uid,gid=info.st_gid,mode=stat.S_IMODE(info.st_mode),bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())))'
            before = invoke(inspect_code, readonly=True)
            self.assertEqual(before.returncode, 0, before.stderr.decode())
            self.assertEqual(before.stderr, b'')
            held = invoke(q.initializer_source())
            self.assertEqual(held.returncode, 1)
            self.assertIn(b'new_empty_copy_root_required', held.stderr)
            self.assertNotIn(b'ResourceWarning', held.stderr)
            after = invoke(inspect_code, readonly=True)
            self.assertEqual(after.returncode, 0, after.stderr.decode())
            self.assertEqual(after.stderr, b'')
            self.assertEqual(json.loads(before.stdout), json.loads(after.stdout))
            self.assertEqual(json.loads(after.stdout)['uid'], 0)
            self.assertEqual(json.loads(after.stdout)['gid'], 0)
            self.assertEqual(json.loads(after.stdout)['mode'], 0o755)
        finally:
            removed = call('volume', 'rm', volume)
            self.assertEqual(removed.returncode, 0, removed.stderr.decode())

    def test_stream_source_tamper_and_oversize_refuse_without_any_private_mount(self):
        repo = Path(__file__).resolve().parents[1]
        pins, sources = q.helper_sources(repo)
        changed = dict(sources)
        changed['ops/incremental_coordinator_binding.py'] = q.base64.b64encode(b'raise AssertionError("must not execute")').decode()
        bodies = [q.packed(dict(helpers=changed, document={})), b' ' * 1048577]
        for body in bodies:
            result = subprocess.run(oracle_docker_args('run', '--rm', '--pull', 'never', '--network', 'none', '--read-only',
                '--cpus', '2', '--memory', '2g', '--pids-limit', '128', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                '--interactive', '--log-driver', 'none', '--entrypoint', 'python',
                'sha256:649f53f2b5dc32cd9eb229c42b6bbd7b479249fba5a8d65c48d8afc4f157b249',
                '-B', '-W', 'error::ResourceWarning', '-c', q.streamed_launcher(pins), '--helper', 'snapshot'),
                input=body, capture_output=True, timeout=15, env=oracle_docker_env())
            self.assertEqual(result.returncode, 2)
            self.assertEqual(json.loads(result.stdout), dict(state='held', code='qualification_helper_failed'))
            self.assertEqual(result.stderr, b'')

    def test_actual_snapshot_and_private_config_helper_old_candidate_old(self):
        import uuid
        repo = Path(__file__).resolve().parents[1]
        old = 'sha256:649f53f2b5dc32cd9eb229c42b6bbd7b479249fba5a8d65c48d8afc4f157b249'
        candidate = 'sha256:e96d957f3eff3f6a33fa673fa9f8ce211d82969c3724d16ba24721f6b376f42b'
        prefix = 'ltds-synthetic-private-qualifier-'+uuid.uuid4().hex
        volumes = [prefix+'-'+name for name in ('source', 'copy', 'tls')]
        created = []
        def docker(*args, stdin=None, expected=0):
            result = subprocess.run(oracle_docker_args(*args), input=stdin, capture_output=True, timeout=45, env=oracle_docker_env())
            self.assertEqual(result.returncode, expected, result.stderr.decode())
            return result.stdout.decode().strip()
        common = ['run', '--rm', '--pull', 'never', '--network', 'none', '--read-only', '--cpus', '2', '--memory', '2g', '--pids-limit', '128',
            '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--tmpfs', '/tmp:rw,nosuid,nodev,size=256m', '--log-driver', 'none']
        try:
            for volume in volumes:
                docker('volume', 'create', '--label', 'com.ledgetop.qualifier=synthetic-private-helper', volume); created.append(volume)
                docker(*common, '--user', '0', '--cap-add', 'CHOWN', '--mount', 'type=volume,src='+volume+',dst=/copy', '--entrypoint', 'python', old,
                    '-c', q.initializer_source())
            with tempfile.TemporaryDirectory() as temporary:
                env = Path(temporary)/'synthetic.env'
                env.write_text('\n'.join(('PROCESSING_API_TOKEN=synthetic-native-'+'n'*32,
                    'LOCAL_NODE_URL=http://127.0.0.1:19001', 'MAIN_NODE_URL=http://127.0.0.1:19001',
                    'PROCESSING_BIND_HOST=0.0.0.0', 'PROCESSING_PORT=18080', 'PROCESSING_PROTOTYPE_STATE_DIR=/prototype-state',
                    'PROCESSING_PROTOTYPE_NODE_IDS=cluster,main', 'PROCESSING_TREEDAR_NODE_IDS=main',
                    'PROCESSING_VIEWER_ENABLED=1', 'PROCESSING_VIEWER_TOKEN=synthetic-native-viewer-'+'y'*32,
                    'PROCESSING_REFERENCE_CONFIG='+q.CONFIG))+'\n'); env.chmod(0o600)
                helper_pins, helper_payload = q.helper_sources(repo)
                helper_launcher = q.streamed_launcher(helper_pins)
                seed_code = inspect.getsource(synthetic_seed).encode()
                seed_sha = q.hashlib.sha256(seed_code).hexdigest()
                marker = 'result=module.helper(sys.argv[-1],value["document"])'
                self.assertIn(marker, helper_launcher)
                fixture_entry = '''seed=base64.b64decode(value["document"]["syntheticSeed"],validate=True)
 assert len(seed)<=16384 and hashlib.sha256(seed).hexdigest()=='''+repr(seed_sha)+'''
 scope={"q":module,"incremental":incremental,"Path":__import__("pathlib").Path,"subprocess":__import__("subprocess")}
 exec(compile(seed,"<synthetic-seed-fixture>","exec"),scope)
 result=scope["synthetic_seed"]()'''
                seed_launcher = helper_launcher.replace(marker, fixture_entry)
                seed = json.loads(docker(*common, '--interactive', '--env-file', str(env), '-e', 'QUALIFICATION_SOURCE_COMMIT='+tuple(q.SOURCES)[0],
                    '--mount', 'type=volume,src='+volumes[0]+',dst=/prototype-state',
                    '--mount', 'type=volume,src='+volumes[2]+',dst=/run/processing-reference',
                    '--workdir', '/app', '--entrypoint', 'python', old,
                    '-B', '-W', 'error::ResourceWarning', '-c', seed_launcher,
                    stdin=q.packed(dict(helpers=helper_payload, document=dict(syntheticSeed=q.base64.b64encode(seed_code).decode())))))
                census = seed['census']
                environment = dict(line.split('=', 1) for line in env.read_text().splitlines())
                binding = incremental.IncrementalBinding('1'*64, old, tuple(q.SOURCES)[0], tuple(q.SOURCES)[1], census['authorityId'],
                    q.digest(environment), seed['configSha256'], '4'*64, 'sha256:'+'a'*64, '5'*64, 'sha256:'+'a'*64,
                    census['tables']['jobs']['sha256'], incremental.digest(census['ownedRows']), incremental.digest(census['schema']))
                profile = q.QualificationBinding(asdict(binding), candidate, 'synthetic-local-engine-0001')
                baseline = None
                for phase, image, commit in (('snapshot', old, tuple(q.SOURCES)[0]), ('application', old, tuple(q.SOURCES)[0]),
                        ('application', candidate, tuple(q.SOURCES)[1]), ('application', old, tuple(q.SOURCES)[0])):
                    args = common+['--interactive', '--env-file', str(env), '-e', 'QUALIFICATION_SOURCE_COMMIT='+commit,
                        '--mount', 'type=volume,src='+volumes[1]+',dst=/prototype-state',
                        '--mount', 'type=volume,src='+volumes[2]+',dst=/run/processing-reference,readonly', '--workdir', '/app']
                    if phase == 'snapshot': args += ['--mount', 'type=volume,src='+volumes[0]+',dst=/source,readonly']
                    if phase == 'snapshot':
                        wrong = asdict(profile); wrong['incremental']['application_environment_sha256'] = 'f'*64
                        denied = json.loads(docker(*args, '--entrypoint', 'python', image, '-B', '-W', 'error::ResourceWarning',
                            '-c', helper_launcher, '--helper', phase,
                            stdin=q.packed(dict(helpers=helper_payload, document=dict(binding=wrong, baseline=baseline,
                                applicationEnvironmentKeys=sorted(environment)))), expected=2))
                        self.assertEqual(denied, dict(state='held', code='qualification_helper_failed'))
                    result = json.loads(docker(*args, '--entrypoint', 'python', image, '-B', '-W', 'error::ResourceWarning',
                        '-c', helper_launcher, '--helper', phase,
                        stdin=q.packed(dict(helpers=helper_payload, document=dict(binding=asdict(profile), baseline=baseline,
                            applicationEnvironmentKeys=sorted(environment))))))
                    if phase == 'snapshot': baseline = result
                    else:
                        self.assertEqual(result['state'], 'qualified')
                        self.assertEqual(result['authorityId'], census['authorityId'])
                        self.assertFalse(result['recoveryEnabled'])
                        self.assertEqual(result['logicalSha256'], baseline['logicalSha256'])
                        self.assertEqual(result['privateMaterialSha256'], baseline['privateMaterialSha256'])
        finally:
            for volume in reversed(created): docker('volume', 'rm', volume)


if __name__ == '__main__':
    if sys.argv[-1:] == ['--synthetic-seed']:
        print(json.dumps(synthetic_seed()))
    else: unittest.main()
