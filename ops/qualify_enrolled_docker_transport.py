"""Opt-in local-daemon qualification using only disposable, ownership-bound data.

Run inside the reviewed Linux controller harness with its Docker CLI/socket.
No image pull, host data mount, service cutover or production configuration.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import tarfile
import tempfile
import time
import uuid

from ops import enrolled_coordinator_transport as transport


TASK_LABEL = 'org.ledgetop.transport-qualification'


class QualificationDocker(transport.DockerCLI):
    """Retain resources if a daemon mutation may still be in flight."""
    def __init__(self):
        super().__init__()
        self.mutation_unknown = False

    def command(self, *args, mutation=False, **kwargs):
        try:
            return super().command(*args, mutation=mutation, **kwargs)
        except BaseException:
            if mutation:
                self.mutation_unknown = True
            raise


def cleanup_owned(docker, operation, volumes):
    cleanup_errors = []
    # An empty scan does not exclude an unresolved daemon RPC applying later.
    # Do not remove backing volumes until every dispatched mutation is known.
    if docker.mutation_unknown:
        print(json.dumps({'retainedUnknownOperation': operation, 'taskVolumes': volumes}), flush=True)
        raise transport.TransportHeld('task_mutation_unknown_resources_retained')
    for identifier in docker.matching(['label=' + TASK_LABEL + '=' + operation]):
        if docker.mutation_unknown:
            break
        try:
            item = docker.inspect(identifier)
            transport.require(item['Config']['Labels'].get(TASK_LABEL) == operation, 'cleanup_owner_changed')
            if item['State']['Running']:
                docker.stop(identifier)
            transport.require(not docker.inspect(identifier)['State']['Running'], 'cleanup_stopped_required')
            docker.command('container', 'rm', identifier, mutation=True, timeout=10, output_limit=4096)
        except Exception:
            cleanup_errors.append('owned_container_cleanup_unverified')
    for name in volumes:
        if docker.mutation_unknown:
            break
        try:
            item = transport.decode(docker.command('volume', 'inspect', name, output_limit=65536))[0]
            transport.require(item.get('Labels', {}).get(TASK_LABEL) == operation, 'cleanup_volume_owner_changed')
            transport.require(not docker.matching(['volume=' + name]), 'cleanup_volume_still_mounted')
            docker.command('volume', 'rm', name, mutation=True, timeout=10, output_limit=4096)
        except Exception:
            cleanup_errors.append('owned_volume_cleanup_unverified')
    if docker.mutation_unknown or cleanup_errors:
        print(json.dumps({'retainedUnknownOperation': operation, 'taskVolumes': volumes}), flush=True)
        raise transport.TransportHeld('task_resource_cleanup_unverified')
    transport.require(not docker.matching(['label=' + TASK_LABEL + '=' + operation]), 'task_container_still_present')



def initialize_fixture(docker, image_id, operation, name, volume, source):
    value = transport.decode(docker.command('run', '--rm', '--name', name,
        '--label', TASK_LABEL + '=' + operation, '--network', 'none', '--read-only',
        '--user', '10001:10001', '--cpus', '0.25', '--memory', '128m', '--memory-swap', '128m',
        '--pids-limit', '32', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
        '--mount', 'type=volume,source=' + volume + ',target=/prototype-state',
        '--entrypoint', 'python', image_id, '-I', '-c', source,
        mutation=True, timeout=20, output_limit=4096))
    transport.require(value == {'uid': 10001, 'gid': 10001}, 'fixture_owner_required')


def qualify(image_id, source_commit, daemon_id):
    docker = QualificationDocker()
    actual_daemon = docker.command('info', '--format', '{{.ID}}', output_limit=4096).decode().strip()
    transport.require(actual_daemon == daemon_id, 'explicit_test_daemon_required')
    image = docker.image(image_id)
    transport.require(image['Config'].get('Labels', {}).get('org.opencontainers.image.revision') == source_commit,
                      'exact_qualified_image_required')
    operation = str(uuid.uuid4())
    suffix = uuid.uuid4().hex[:16]
    names = {'state': 'ltds-qualify-state-' + suffix, 'config': 'ltds-qualify-config-' + suffix}
    container_name = 'ltds-qualify-candidate-' + suffix
    completed = []
    volumes = []
    result = None
    try:
        for name in names.values():
            existing = docker.command('volume', 'ls', '--filter', 'name=' + name,
                                      '--format', '{{.Name}}', output_limit=4096).decode().splitlines()
            transport.require(name not in existing, 'new_task_volume_required')
            volumes.append(name)
            docker.command('volume', 'create', '--label', TASK_LABEL + '=' + operation, name,
                           mutation=True, output_limit=4096)
        fixture = {'synthetic': True, 'nonce': secrets.token_hex(16)}
        fixture_bytes = json.dumps(fixture, sort_keys=True, separators=(',', ':')).encode()
        setup = (
            'import json,os,pathlib\n'
            'root=pathlib.Path("/prototype-state")\n'
            'files={"runtime.json":' + repr(fixture_bytes) + ',"public.json":b"public",'
            '"large.json":b"x"*8192}\n'
            'for name,data in files.items():\n'
            ' p=root/name;p.write_bytes(data);p.chmod(0o644 if name=="public.json" else 0o600)\n'
            '(root/"alias.json").symlink_to("runtime.json")\n'
            'print(json.dumps({"uid":os.getuid(),"gid":os.getgid()}))\n'
        )
        initialize_fixture(docker, image_id, operation, 'ltds-qualify-config-setup-' + suffix,
                           names['config'], setup)
        state_bytes = 17 * 1024**2 + 123
        state_setup = ('import os,pathlib,json\np=pathlib.Path("/prototype-state/qualification-state.bin")\n'
            'with p.open("xb") as f:\n'
            ' for _ in range(272):f.write(b"q"*65536)\n'
            ' f.write(b"q"*123)\n'
            'p.chmod(0o600)\nprint(json.dumps({"uid":os.getuid(),"gid":os.getgid()}))\n')
        initialize_fixture(docker, image_id, operation, 'ltds-qualify-state-setup-' + suffix,
                           names['state'], state_setup)
        completed.append('private_fixture_volume_owner')
        with tempfile.TemporaryDirectory(prefix='ltds-docker-qualification-') as directory:
            root = Path(directory)
            root.chmod(0o700)
            environment = ('PROCESSING_API_TOKEN=' + secrets.token_urlsafe(32) + '\n'
                           'PROCESSING_BIND_HOST=127.0.0.1\n').encode()
            env_path = root / 'runtime.env'
            env_path.write_bytes(environment)
            env_path.chmod(0o600)
            recipe = dict(schemaVersion=1, name=container_name, imageId=image_id,
                user='10001:10001', workingDir='/app', environmentFile=str(env_path),
                environmentSha256=hashlib.sha256(environment).hexdigest(),
                mounts=[dict(type='volume', source=names['state'], target='/prototype-state', readOnly=False),
                        dict(type='volume', source=names['config'], target='/run/processing-reference', readOnly=True)],
                ports=[], network='none', tmpfs={'/tmp': 'rw,nosuid,nodev,noexec,size=64m,mode=1777'},
                labels={TASK_LABEL: operation}, command=['python', '-m', 'processing.app'], entrypoint=None,
                readOnly=True, init=True, pidsLimit=64, memoryBytes=256 * 1024**2,
                nanoCpus=250000000, stopSignal='SIGTERM', stopTimeout=-1,
                restartPolicy={'Name': 'no', 'MaximumRetryCount': 0})
            args = transport.create_arguments(recipe, operation_id=operation, role='bootstrap',
                                              intent_id=str(uuid.uuid4()))
            identifier = docker.command(*args, mutation=True, timeout=20, output_limit=4096).decode().strip()
            transport.require(transport.ID.fullmatch(identifier) is not None, 'created_exact_container_required')
            item = docker.inspect(identifier)
            transport.require(item['State']['Status'] == 'created' and item['State']['Running'] is False
                and item['Config']['Labels'].get(TASK_LABEL) == operation, 'never_started_owned_candidate_required')
            frozen_recipe = transport.immutable_container_recipe(item)
            transport.require(transport.container_environment(item).get('PROCESSING_API_TOKEN') ==
                              environment.decode().splitlines()[0].split('=', 1)[1], 'exact_environment_required')
            completed.append('closed_recipe_create_never_started')
            transport.require(docker.read_private_file(identifier, '/run/processing-reference/runtime.json') == fixture_bytes,
                              'stopped_private_config_exact_bytes_required')
            completed.append('never_started_private_config_readback')
            for path, maximum in (('public.json', 1024), ('alias.json', 1024), ('large.json', 16)):
                try:
                    docker.read_private_file(identifier, '/run/processing-reference/' + path, maximum=maximum)
                except transport.TransportHeld:
                    completed.append('reject_' + path.split('.')[0])
                else:
                    raise transport.TransportHeld('unsafe_config_readback_accepted')
            transport.require(docker.inspect(identifier)['State']['Status'] == 'created',
                              'readback_must_not_start_container')
            completed.append('readback_preserves_created_state')
            archive_path = root / 'original-state.tar'
            largest_chunk = 0
            with archive_path.open('xb') as archive_output:
                archive_path.chmod(0o600)
                def sink(chunk):
                    nonlocal largest_chunk
                    largest_chunk = max(largest_chunk, len(chunk))
                    transport.require(archive_output.write(chunk) == len(chunk), 'complete_fixture_sink_required')
                stream = docker.archive_to(identifier, '/prototype-state', sink,
                                           max_bytes=32 * 1024**2, timeout=20)
                archive_output.flush()
                os.fsync(archive_output.fileno())
            with archive_path.open('rb') as archive_input:
                archive_sha = hashlib.file_digest(archive_input, 'sha256').hexdigest()
            transport.require(stream == dict(clientExitCode=0, streamClosed=True,
                bytes=archive_path.stat().st_size, sha256=archive_sha) and largest_chunk <= 65536
                and stream['bytes'] > 16 * 1024**2, 'bounded_large_archive_stream_required')
            expected_hash = hashlib.sha256()
            for _ in range(272):
                expected_hash.update(b'q' * 65536)
            expected_hash.update(b'q' * 123)
            entries = 0
            with tarfile.open(archive_path, mode='r|') as archive:
                for member in archive:
                    if member.isdir():
                        transport.require(member.name in ('.', './'), 'exact_fixture_archive_root_required')
                        continue
                    transport.require(member.isfile() and member.name in
                        ('qualification-state.bin', './qualification-state.bin') and member.size == state_bytes
                        and member.uid == member.gid == 10001 and member.mode == 0o600,
                        'exact_fixture_archive_member_required')
                    contents = archive.extractfile(member)
                    transport.require(contents is not None, 'fixture_archive_bytes_required')
                    with contents:
                        actual_hash = hashlib.file_digest(contents, 'sha256').hexdigest()
                    transport.require(actual_hash == expected_hash.hexdigest(), 'fixture_archive_digest_changed')
                    entries += 1
            transport.require(entries == 1, 'exact_fixture_archive_inventory_required')
            completed.append('actual_17mib_bounded_archive_and_exact_bytes')
            docker.start(identifier)
            probe = b'''import json,os,pathlib,sys,time,urllib.request
# Match the reviewed serialized holder's fixed-image bootstrap under -I.
sys.path.insert(0,'/app')
import processing.runtime_source_custody
assert pathlib.Path(processing.runtime_source_custody.__file__).resolve()==pathlib.Path('/app/processing/runtime_source_custody.py')
deadline=time.monotonic()+10
while True:
 try:
  with urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=1) as response:
   health=json.loads(response.read())
  break
 except OSError:
  if time.monotonic()>=deadline:raise
  time.sleep(0.1)
try:pathlib.Path('/app/qualification-write-probe').write_bytes(b'x');writable=True
except OSError:writable=False
print(json.dumps({'uid':os.getuid(),'gid':os.getgid(),'health':health.get('status'),
 'sourceWritable':writable,'pid':os.getpid(),'start':pathlib.Path('/proc/self/stat').read_text().split(') ',1)[1].split()[19]}))
'''
            observed = docker.exec_python(identifier, probe, timeout=15, output_limit=4096)
            transport.require({k: observed[k] for k in ('uid', 'gid', 'health', 'sourceWritable')} ==
                              {'uid': 10001, 'gid': 10001, 'health': 'ok', 'sourceWritable': False},
                              'packaged_application_smoke_required')
            completed.append('packaged_health_uid_readonly_source')
            # A second bounded read observes the exact first probe's process identity.
            retired = docker.exec_python(identifier, (
                'import pathlib,json\np=pathlib.Path("/proc/' + str(observed['pid']) + '/stat")\n'
                'print(json.dumps({"stillAlive":p.exists() and p.read_text().split(") ",1)[1].split()[19]=='
                + repr(observed['start']) + '}))\n').encode(), timeout=5, output_limit=4096)
            transport.require(retired == {'stillAlive': False}, 'probe_process_retirement_required')
            completed.append('exact_probe_process_retired')
            transport.require(transport.immutable_container_recipe(docker.inspect(identifier)) == frozen_recipe,
                              'immutable_recipe_changed_after_start')
            docker.stop(identifier)
            stopped = docker.inspect(identifier)
            transport.require(stopped['State']['Running'] is False and stopped['State']['ExitCode'] == 0,
                              'graceful_application_exit_required')
            completed.append('non_escalating_graceful_stop')
            transport.require(docker.read_private_file(identifier, '/run/processing-reference/runtime.json') == fixture_bytes,
                              'stopped_config_changed')
            docker.rename(identifier, container_name + '-retained')
            retained = docker.inspect(identifier)
            transport.require(retained['Name'] == '/' + container_name + '-retained' and
                transport.immutable_container_recipe(retained) == frozen_recipe, 'exact_retained_rename_required')
            completed.append('stopped_private_readback_and_retained_rename')
            result = dict(schemaVersion=1, completed=completed, imageId=image_id, sourceCommit=source_commit)
    except Exception:
        print(json.dumps({'completedBeforeFailure': completed}), flush=True)
        raise
    finally:
        cleanup_owned(docker, operation, volumes)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', required=True)
    parser.add_argument('--source-commit', required=True)
    parser.add_argument('--expected-daemon-id', required=True)
    arguments = parser.parse_args()
    try:
        result = qualify(arguments.image, arguments.source_commit, arguments.expected_daemon_id)
    except Exception as error:
        print(json.dumps({'ok': False, 'error': str(error) if isinstance(error, transport.TransportHeld)
                          else type(error).__name__}))
        return 1
    print(json.dumps({'ok': True, **result, 'taskResourcesRemoved': True}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
