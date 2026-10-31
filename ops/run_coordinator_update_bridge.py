"""One-shot controller for the two existing SSH accounts; never provisions access.

Run from the exact source bundle staged on both processing PCs. The cluster
executor owns all deployment decisions. This controller only owns its two SSH
children and relays their live gate challenges. A transport failure is a hold,
never authorization to retry a deployment or clear a durable node reservation.
"""
import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import subprocess
import sys

from ops.coordinator_gate_bridge import BridgeBinding, BridgeError, RootRelay, helper_digest


AUTHORITY = '7131882e-38ce-4d3e-99cc-4ba5a94d2ee8'
ENGINES = {'main': '45bc889d-d645-442b-8fb9-d78b77b14fe9',
           'cluster': 'b557939c-6ac3-4e73-8b14-31caf7390f23'}
HOSTS = {'main': 'bkoltz@192.168.50.88', 'cluster': 'bkoltz@192.168.50.89'}
BASE = '/home/bkoltz/.local/share/ledgetop-processing-reference'
SSH = Path('C:/Windows/System32/OpenSSH/ssh.exe')
OPTIONS = ('-T', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
           '-o', 'ConnectTimeout=10', '-o', 'ForwardAgent=no',
           '-o', 'ForwardX11=no', '-o', 'ClearAllForwardings=yes',
           '-o', 'ControlMaster=no', '-o', 'ControlPath=none',
           '-o', 'PermitLocalCommand=no', '-o', 'ProxyCommand=none',
           '-o', 'ProxyJump=none', '-o', 'RemoteCommand=none')


def _commit(value):
    if type(value) is not str or not re.fullmatch(r'[a-f0-9]{40}', value):
        raise BridgeError('full_source_commit_required')


def _image(value):
    if type(value) is not str or not re.fullmatch(r'sha256:[a-f0-9]{64}', value):
        raise BridgeError('immutable_local_image_required')


def _private_remote_path(value, *, filename=False):
    if (type(value) is not str or not value.isascii()
            or not re.fullmatch(r'/[A-Za-z0-9/._-]+', value)
            or str(PurePosixPath(value)) != value
            or any(part in ('.', '..') for part in value.split('/')[1:])
            or not value.startswith(BASE + '/')):
        raise BridgeError('fixed_private_remote_path_required')
    if filename and not value.endswith('.json'):
        raise BridgeError('private_json_configuration_required')
    return value


def validate_config(config):
    if (type(config) is not dict
            or set(config) != {'schemaVersion', 'cluster', 'main', 'sessionNonce', 'timeoutSeconds'}
            or type(config['schemaVersion']) is not int or config['schemaVersion'] != 1
            or type(config['sessionNonce']) is not str
            or not re.fullmatch(r'[a-f0-9]{64}', config['sessionNonce'])
            or type(config['timeoutSeconds']) is not int or not 1 <= config['timeoutSeconds'] <= 30):
        raise BridgeError('closed_bridge_configuration_required')
    bindings = {}
    for node in ('main', 'cluster'):
        row = config[node]
        if (type(row) is not dict
                or set(row) != {'engineId', 'nodeId', 'authorityId', 'coordinatorOrigin', 'generation', 'helperSha256'}):
            raise BridgeError('closed_gate_binding_required')
        binding = BridgeBinding(engine_id=row['engineId'], node_id=row['nodeId'],
            authority_id=row['authorityId'], coordinator_origin=row['coordinatorOrigin'],
            gate_generation=row['generation'], helper_sha256=row['helperSha256'])
        if (binding.node_id != node or binding.engine_id != ENGINES[node]
                or binding.authority_id != AUTHORITY
                or binding.coordinator_origin != 'http://127.0.0.1:18080'
                or binding.helper_sha256 != helper_digest()):
            raise BridgeError('installed_processing_binding_required')
        bindings[node] = binding
    return bindings


@dataclass(frozen=True)
class RunSpec:
    action: str
    source_commit: str
    bundle_commit: str
    bundle_manifest_sha256: str
    image: str
    main_image: str
    operation_id: str
    receipt_dir: str
    gate_config: str
    predecessor_id: str
    predecessor_image: str
    predecessor_source: str
    lease_dir: str
    cluster_id: str
    cluster_name: str
    cluster_project: str

    def __post_init__(self):
        if self.action not in ('activate', 'rollback'):
            raise BridgeError('prepared_update_action_required')
        _commit(self.source_commit); _commit(self.bundle_commit); _image(self.image); _image(self.main_image)
        if type(self.bundle_manifest_sha256) is not str or not re.fullmatch(r'[a-f0-9]{64}',self.bundle_manifest_sha256):
            raise BridgeError('exact_bundle_manifest_required')
        if type(self.predecessor_id) is not str or not re.fullmatch(r'[a-f0-9]{64}',self.predecessor_id):
            raise BridgeError('exact_predecessor_container_required')
        _image(self.predecessor_image); _commit(self.predecessor_source)
        if not re.fullmatch(r'[a-f0-9]{64}',self.cluster_id) or any(not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}',v) for v in (self.cluster_name,self.cluster_project)):
            raise BridgeError('exact_cluster_binding_required')
        _private_remote_path(self.lease_dir)
        if type(self.operation_id) is not str or not re.fullmatch(r'[a-f0-9]{32}', self.operation_id):
            raise BridgeError('prepared_update_operation_required')
        _private_remote_path(self.receipt_dir)
        _private_remote_path(self.gate_config, filename=True)

    @property
    def source(self):
        return BASE + '/reference-' + self.bundle_commit + '/source'


# These programs are constant code. All variable inputs are one JSON argument,
# POSIX-quoted with shlex.join; no code/string formatting of a shell program.
BOOTSTRAP = '''import hashlib,json,os,pathlib,pwd,re,socket,stat,sys
p=json.loads(sys.argv[1]);root=pathlib.Path(p['source'])
if os.geteuid()!=1000 or pwd.getpwuid(os.geteuid()).pw_name!='bkoltz':raise RuntimeError('account mismatch')
if socket.gethostname().split('.')[0].lower()!=p['hostname']:raise RuntimeError('host mismatch')
if not root.is_absolute() or not root.is_dir() or any(x.is_symlink() for x in (root,*root.parents)):raise RuntimeError('source root invalid')
manifest=root/'cutover-source-manifest.json'
fd=os.open(manifest,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
try:
 st=os.fstat(fd)
 if not stat.S_ISREG(st.st_mode) or st.st_nlink!=1 or st.st_size>1048576:raise RuntimeError('manifest invalid')
 data=os.read(fd,1048577)
 if len(data)!=st.st_size or hashlib.sha256(data).hexdigest()!=p['bundleManifestSha256']:raise RuntimeError('manifest changed')
finally:os.close(fd)
def unique(items):
 out={}
 for k,v in items:
  if k in out:raise RuntimeError('duplicate manifest path')
  out[k]=v
 return out
expected=json.loads(data,object_pairs_hook=unique)
if not isinstance(expected,dict) or not expected or len(expected)>4096:raise RuntimeError('manifest invalid')
seen=set();pending=[root/'ops',root/'processing'];total=0;entries_seen=0
while pending:
 directory=pending.pop();ds=directory.lstat()
 if not stat.S_ISDIR(ds.st_mode) or stat.S_ISLNK(ds.st_mode):raise RuntimeError('source directory invalid')
 with os.scandir(directory) as entries:
  for entry in entries:
   entries_seen+=1
   if entries_seen>8192:raise RuntimeError('source tree limit')
   q=pathlib.Path(entry.path);relative=q.relative_to(root).as_posix();st=q.lstat()
   if stat.S_ISLNK(st.st_mode):raise RuntimeError('source symlink refused')
   if stat.S_ISDIR(st.st_mode):pending.append(q);continue
   if not stat.S_ISREG(st.st_mode) or st.st_nlink!=1 or st.st_size>4194304 or len(seen)>=4096 or relative not in expected:raise RuntimeError('source file invalid')
   fd=os.open(q,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
   try:
    a=os.fstat(fd);raw=b''
    if (a.st_dev,a.st_ino)!=(st.st_dev,st.st_ino):raise RuntimeError('source identity changed')
    while len(raw)<=4194304:
     chunk=os.read(fd,65536)
     if not chunk:break
     raw+=chunk
    z=os.fstat(fd)
    if len(raw)!=st.st_size or (z.st_dev,z.st_ino,z.st_size,z.st_mtime_ns)!=(a.st_dev,a.st_ino,a.st_size,a.st_mtime_ns) or hashlib.sha256(raw).hexdigest()!=expected[relative]:raise RuntimeError('source changed')
   finally:os.close(fd)
   total+=len(raw)
   if total>67108864:raise RuntimeError('source bound exceeded')
   seen.add(relative)
if seen!=set(expected):raise RuntimeError('source manifest incomplete')
sys.path.insert(0,str(root));sys.dont_write_bytecode=True
from ops import coordinator_gate_bridge as b
if pathlib.Path(b.__file__).resolve()!=root/'ops/coordinator_gate_bridge.py' or b.helper_digest()!=p['helperSha256']:raise RuntimeError('gate helper changed')
if p['role']=='main':
 r=p['binding'];binding=b.BridgeBinding(engine_id=r['engineId'],node_id=r['nodeId'],authority_id=r['authorityId'],coordinator_origin=r['coordinatorOrigin'],gate_generation=r['generation'],helper_sha256=r['helperSha256'])
 b.serve_main(sys.stdin.buffer,sys.stdout.buffer,binding=binding,operation_id=p['operation'],session_nonce=p['sessionNonce'],engine_probe=b.MainEngineProbe(expected_image=p['expectedMainImage']),timeout_seconds=p['timeoutSeconds'])
else:
 from ops.update_reference_coordinator import main
 raise SystemExit(main(p['arguments']))
'''


def ssh_argv(node, spec, config):
    if node not in HOSTS or type(spec) is not RunSpec:
        raise BridgeError('fixed_processing_ssh_target_required')
    bindings = validate_config(config)
    payload = dict(source=spec.source, bundleManifestSha256=spec.bundle_manifest_sha256, role=node, hostname='node' if node == 'main' else 'cluster',
        helperSha256=helper_digest(), operation=spec.operation_id,
        sessionNonce=config['sessionNonce'], timeoutSeconds=config['timeoutSeconds'])
    if node == 'main':
        payload['binding'] = bindings['main'].public()
        payload['expectedMainImage'] = spec.main_image
    else:
        payload['arguments'] = [spec.action, '--image', spec.image, '--source-commit', spec.source_commit,
            '--receipt-dir', spec.receipt_dir, '--verified-main-image', spec.main_image,
            '--gate-config', spec.gate_config, '--predecessor-id',spec.predecessor_id,
            '--predecessor-image',spec.predecessor_image,'--predecessor-source',spec.predecessor_source,
            '--lease-dir',spec.lease_dir,'--cluster-id',spec.cluster_id,'--cluster-name',spec.cluster_name,'--cluster-project',spec.cluster_project]
    command = shlex.join(['python3', '-I', '-B', '-c', BOOTSTRAP,
        json.dumps(payload, sort_keys=True, separators=(',', ':'), allow_nan=False)])
    return [str(SSH), *OPTIONS, HOSTS[node], command]


def _stop_owned(process):
    """Bounded cleanup of our child transport, never a remote Docker action."""
    for stream in (process.stdin,):
        try: stream.close()
        except (OSError, ValueError): pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.terminate()
        try: process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill(); process.wait(timeout=5)
    finally:
        if process.stdout: process.stdout.close()


def run(spec, config, evidence_dir, *, popen=subprocess.Popen, relay_factory=RootRelay):
    bindings = validate_config(config)
    if type(spec) is not RunSpec:
        raise BridgeError('prepared_update_spec_required')
    directory = Path(evidence_dir)
    if not directory.is_absolute() or directory.is_symlink():
        raise BridgeError('absolute_new_evidence_directory_required')
    directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    children = []; logs = []
    try:
        for node in ('main', 'cluster'):
            log = os.fdopen(os.open(directory / (node + '-ssh.stderr'),
                os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb')
            logs.append(log)
            child = popen(ssh_argv(node, spec, config), stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=log, shell=False, bufsize=0)
            children.append(child)
        result = relay_factory(children[0], children[1], binding=bindings['main'],
            operation_id=spec.operation_id, session_nonce=config['sessionNonce'],
            timeout_seconds=config['timeoutSeconds']).run()
        if type(result) is not int or result != 0:
            raise BridgeError('update_bridge_did_not_complete')
        # Relay success proves its close protocol and cluster exit. The actual
        # holder must also terminate normally after stdin EOF releases its gate.
        children[0].wait(timeout=5)
        if children[0].returncode != 0 or children[1].returncode != 0:
            raise BridgeError('owned_ssh_exit_not_successful')
        return 0
    finally:
        cleanup_failed = False
        for child in reversed(children):
            try: _stop_owned(child)
            except Exception: cleanup_failed = True
        for log in logs: log.close()
        if cleanup_failed:
            raise BridgeError('owned_ssh_cleanup_unconfirmed')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('activate', 'rollback'))
    for flag in ('source-commit', 'bundle-commit', 'bundle-manifest-sha256', 'image', 'main-image', 'operation-id', 'receipt-dir',
                 'gate-config', 'local-gate-config', 'evidence-dir', 'predecessor-id', 'predecessor-image',
                 'predecessor-source', 'lease-dir', 'cluster-id', 'cluster-name', 'cluster-project'):
        parser.add_argument('--' + flag, required=True)
    args = parser.parse_args(argv)
    try:
        if os.name != 'nt' or not SSH.is_file():
            raise BridgeError('existing_windows_openssh_controller_required')
        config_path = Path(args.local_gate_config)
        if not config_path.is_absolute() or config_path.is_symlink() or config_path.stat().st_size > 16384:
            raise BridgeError('bounded_local_gate_configuration_required')
        config = json.loads(config_path.read_text(encoding='utf-8'))
        spec = RunSpec(args.action, args.source_commit, args.bundle_commit, args.bundle_manifest_sha256, args.image, args.main_image,
            args.operation_id, args.receipt_dir, args.gate_config, args.predecessor_id,
            args.predecessor_image,args.predecessor_source,args.lease_dir,args.cluster_id,args.cluster_name,args.cluster_project)
        from ops.make_cutover_manifest import manifest_for_commit
        if hashlib.sha256(manifest_for_commit(spec.bundle_commit)).hexdigest()!=spec.bundle_manifest_sha256:
            raise BridgeError('bundle_manifest_commit_mismatch')
        return run(spec, config, args.evidence_dir)
    except Exception:
        print('Coordinator update bridge held. Inspect private SSH evidence and the existing checkpoint; do not replay.', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
