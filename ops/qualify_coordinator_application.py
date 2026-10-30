"""On-host, isolated private-config/state-copy qualification. Never an activator.

No SSH or NAS interface exists. Private material stays on this Docker host.
Every Docker mutation has a durable intent; existing/uncertain operations hold.
"""
from contextlib import closing
import base64
from dataclasses import asdict, dataclass
import hashlib
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import socket
import sqlite3
import ssl
import stat
import subprocess
import sys
import threading
import time
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

ENV_FILE = '/home/bkoltz/.local/share/ledgetop-processing-prototype/viewer-ba01f37/runtime.env'
TLS_ROOT = '/home/bkoltz/.local/share/ledgetop-processing-reference/reference-10a3848/coordinator-tls'
CONFIG = '/run/processing-reference/runtime.json'
LIVE_VOLUME = 'ledgetop-processing-prototype-c3fcb3a-state'
OPERATION_ROOT = '/home/bkoltz/.local/share/ledgetop-processing-qualifications'
DOCKER_BINARY = '/usr/bin/docker'
DOCKER_SOCKET = '/run/docker.sock'
DOCKER_HOST = 'unix:///run/docker.sock'
SOURCES = {
    '671fd2a6bde40088bdfad91ccf67eab45ee3d8d7': (49, 'c5d141da519e4787e26fb2c01dfc6506d4834f730885c802a940c36d87ddbb77'),
    '6bbd468e1177746cb695e43195e75355b0681f23': (63, '04a14d73f7f1652b82a61428be51f9dd751b9bc957094500f8eb776458387add'),
    'd738edb209972a1723fa5af374578951b2a8448c': (63, '86aa245deef9497cd1a512c9e9ce750a3c7ebf5f75077fe25e9d95be2b4a37a8'),
}

QUALIFIED_SOURCE_PAIRS = frozenset((
    ('671fd2a6bde40088bdfad91ccf67eab45ee3d8d7', '6bbd468e1177746cb695e43195e75355b0681f23'),
    ('6bbd468e1177746cb695e43195e75355b0681f23', 'd738edb209972a1723fa5af374578951b2a8448c'),
))


class QualificationHeld(ValueError): pass


def require(value, code):
    if not value: raise QualificationHeld(code)


def packed(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value): return hashlib.sha256(packed(value)).hexdigest()


def helper_sources(root):
    """Read two bounded code files privately; no permission changes or tree mount."""
    names = ('ops/qualify_coordinator_application.py', 'ops/incremental_coordinator_binding.py')
    raw = {name:private_file(Path(root)/name, 131072) for name in names}
    return ({name:hashlib.sha256(value).hexdigest() for name, value in raw.items()},
        {name:base64.b64encode(value).decode('ascii') for name, value in raw.items()})


def streamed_launcher(expected):
    """No private body/credentials in argv; only reviewed code digests appear."""
    return '''import sys,json,base64,hashlib,types
sys.path.insert(0,"/app")
try:
 raw=sys.stdin.buffer.read(1048577)
 assert len(raw)<=1048576
 value=json.loads(raw)
 assert type(value) is dict and set(value)=={"helpers","document"}
 expected='''+repr(expected)+'''
 assert type(value["helpers"]) is dict and set(value["helpers"])==set(expected)
 sources={name:base64.b64decode(value["helpers"][name],validate=True) for name in expected}
 assert all(len(raw)<=131072 and hashlib.sha256(raw).hexdigest()==expected[name] for name,raw in sources.items())
 package=types.ModuleType("ops");package.__path__=[];sys.modules["ops"]=package
 name="ops.qualify_coordinator_application"
 module=types.ModuleType(name);module.__file__="<qualified-stdin-source>";sys.modules[name]=module
 exec(compile(sources["ops/qualify_coordinator_application.py"],module.__file__,"exec"),module.__dict__)
 import os
 module.exact_source(os.environ["QUALIFICATION_SOURCE_COMMIT"])
 name="ops.incremental_coordinator_binding"
 incremental=types.ModuleType(name);incremental.__file__="<qualified-stdin-incremental>";sys.modules[name]=incremental
 exec(compile(sources["ops/incremental_coordinator_binding.py"],incremental.__file__,"exec"),incremental.__dict__)
 result=module.helper(sys.argv[-1],value["document"])
 print(json.dumps(result))
except Exception:
 print(json.dumps({"state":"held","code":"qualification_helper_failed"}))
 sys.exit(2)
'''


def initializer_source():
    """Verify the new mounted root and bounded emptiness before changing it."""
    return '''import os,stat
with open("/proc/self/mountinfo","rb") as source:
 raw=source.read(262145)
assert len(raw)<=262144,"bounded_mount_inventory_required"
matches=[row.split() for row in raw.decode("utf-8").splitlines() if len(row.split())>6 and row.split()[4]=="/copy"]
assert len(matches)==1 and "rw" in matches[0][5].split(","),"exact_writable_copy_mount_required"
before=os.lstat("/copy")
assert stat.S_ISDIR(before.st_mode) and not stat.S_ISLNK(before.st_mode) and os.path.realpath("/copy")=="/copy","copy_directory_required"
assert (before.st_uid,before.st_gid)==(0,0),"new_copy_owner_required"
fd=os.open("/copy",os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
try:
 opened=os.fstat(fd)
 assert (opened.st_dev,opened.st_ino)==(before.st_dev,before.st_ino),"copy_root_changed"
 with os.scandir(fd) as entries:
  assert next(entries,None) is None,"new_empty_copy_root_required"
 current=os.lstat("/copy");held=os.fstat(fd)
 facts=lambda value:(value.st_dev,value.st_ino,value.st_uid,value.st_gid,value.st_mode,value.st_mtime_ns,value.st_ctime_ns)
 assert facts(current)==facts(held)==facts(before),"copy_root_changed"
 os.fchmod(fd,0o700)
 os.fchown(fd,10001,10001)
 os.fsync(fd)
finally:
 os.close(fd)
'''


@dataclass(frozen=True)
class QualificationBinding:
    incremental: dict
    candidate_image: str
    engine_id: str
    native_env_file: str = ENV_FILE
    tls_root: str = TLS_ROOT
    config_path: str = CONFIG
    live_volume: str = LIVE_VOLUME

    def validate(self):
        from ops.incremental_coordinator_binding import IncrementalBinding
        binding = IncrementalBinding(**self.incremental)
        require((binding.predecessor_source, binding.candidate_source) in QUALIFIED_SOURCE_PAIRS, 'qualified_source_pair_required')
        require(re.fullmatch(r'sha256:[a-f0-9]{64}', self.candidate_image) is not None, 'immutable_candidate_required')
        require(type(self.engine_id) is str and re.fullmatch(r'[A-Za-z0-9:_-]{16,128}', self.engine_id) is not None, 'existing_local_engine_identity_required')
        require((self.native_env_file, self.tls_root, self.config_path, self.live_volume) ==
            (ENV_FILE, TLS_ROOT, CONFIG, LIVE_VOLUME), 'existing_private_recipe_required')
        return binding


def private_file(path, limit=1048576):
    path = Path(path)
    before = path.lstat()
    require(path.resolve() == path and not any(p.is_symlink() for p in (path, *path.parents)), 'private_path_changed')
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= limit, 'private_file_bound')
    descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
    try:
        opened = os.fstat(descriptor)
        require((opened.st_dev, opened.st_ino) == (before.st_dev, before.st_ino), 'private_inode_changed')
        raw = b''
        while len(raw) <= before.st_size:
            part = os.read(descriptor, min(65536, before.st_size-len(raw)+1))
            if not part: break
            raw += part
        after = path.lstat()
        require(len(raw) == before.st_size and (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) ==
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns), 'private_file_changed')
        return raw
    finally: os.close(descriptor)


def private_material():
    root = Path('/run/processing-reference')
    require(root.resolve() == root and not root.is_symlink(), 'private_mount_changed')
    records = []
    with os.scandir(root) as entries:
        for entry in entries:
            require(len(records) < 16, 'private_inventory_bound')
            path = root/entry.name
            raw = private_file(path)
            info = path.lstat()
            records.append(dict(name=entry.name, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest(),
                uid=info.st_uid, gid=info.st_gid, mode=stat.S_IMODE(info.st_mode), dev=info.st_dev, inode=info.st_ino))
    info = root.lstat()
    return digest(dict(root=dict(uid=info.st_uid, gid=info.st_gid, mode=stat.S_IMODE(info.st_mode), dev=info.st_dev, inode=info.st_ino),
        files=sorted(records, key=lambda r:r['name'])))


def logical(path):
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro', uri=True)) as db:
        schema = db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name').fetchall()
        result = {}
        for kind, name, _, _ in schema:
            if kind == 'table':
                require(re.fullmatch('[a-z_]+', name) is not None, 'unqualified_table')
                result[name] = sorted(db.execute('SELECT * FROM "'+name+'"').fetchall(), key=packed)
        return digest(dict(schema=schema, tables=result))


def exact_source(commit):
    root = Path('/app/processing')
    files = {}
    maximum = SOURCES[commit][0]
    with os.scandir(root) as entries:
        for entry in entries:
            require(len(files) < maximum and entry.name.endswith('.py') and entry.is_file(follow_symlinks=False), 'bounded_flat_source_required')
            files['processing/'+entry.name] = hashlib.sha256(private_file(root/entry.name, 131072)).hexdigest()
    require((len(files), digest(files)) == SOURCES[commit], 'application_source_changed')


def material_binding(binding):
    from processing.config import from_environment
    config = from_environment()
    require(config.prototype_state_dir == '/prototype-state' and config.reference_config_path == CONFIG
        and config.prototype_node_ids == ('cluster', 'main') and config.viewer_enabled
        and not config.photo_smoke_enabled, 'existing_schema1_recipe_required')
    raw = private_file(CONFIG, 262144)
    require(hashlib.sha256(raw).hexdigest() == binding.private_config_sha256, 'private_config_changed')
    value = json.loads(raw)
    require(type(value) is dict and value.get('schemaVersion') == 1 and type(value.get('schemaVersion')) is int
        and 'recoveryBindings' not in value, 'schema1_only')
    # Specific host LAN addresses cannot bind inside networknone. Never rewrite.
    require(config.bind_host in ('127.0.0.1', '0.0.0.0') and value['listener']['host'] in ('127.0.0.1', '0.0.0.0'), 'isolated_bind_address_required')
    return config, value


def snapshot(binding):
    from ops.incremental_coordinator_binding import volume_census, compare_transition
    before = volume_census('/source', authority=binding.authority_id)
    binding.check_baseline(before)
    config, _ = material_binding(binding)
    material = private_material()
    target = Path(config.prototype_state_dir)
    with os.scandir(target) as entries:
        require(next(entries, None) is None, 'new_disposable_state_required')
    os.chmod(target, before['rootMode'])
    for item in before['files']:
        destination = target/item['path']
        if item['kind'] == 'directory': destination.mkdir(mode=item['mode'])
        elif item['path'] == 'jobs.sqlite3':
            fd = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600); os.close(fd)
            started = time.monotonic()
            def progress(*_): require(time.monotonic()-started < 30, 'snapshot_deadline')
            with closing(sqlite3.connect('file:/source/jobs.sqlite3?mode=ro', uri=True)) as source:
                with closing(sqlite3.connect(destination)) as copy:
                    source.backup(copy, pages=128, progress=progress, sleep=0.01)
            os.chmod(destination, item['mode'])
        else:
            raw = private_file(Path('/source')/item['path'], 65536)
            fd = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, item['mode'])
            try:
                require(os.write(fd, raw) == len(raw), 'snapshot_short_write'); os.fsync(fd)
            finally: os.close(fd)
    # Backup and all directory entries must be durable before the helper reports.
    for item in before['files']:
        fd = os.open(target/item['path'], os.O_RDONLY | os.O_NOFOLLOW | (os.O_DIRECTORY if item['kind'] == 'directory' else 0))
        try: os.fsync(fd)
        finally: os.close(fd)
    fd = os.open(target, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)
    after = volume_census('/source', authority=binding.authority_id)
    require(before == after, 'live_source_changed_during_snapshot')
    copied = volume_census(target, authority=binding.authority_id)
    compare_transition(copied, before, binding=binding)
    require(material == private_material(), 'private_material_changed')
    return dict(census=copied, logicalSha256=logical(target/'jobs.sqlite3'), privateMaterialSha256=material)


def application(binding, baseline):
    from processing.app import make_handler, CoordinatorService
    from processing.prototype import Queue
    from ops.incremental_coordinator_binding import volume_census, compare_transition
    config, value = material_binding(binding)
    before = volume_census('/prototype-state', authority=binding.authority_id)
    compare_transition(before, baseline['census'], binding=binding)
    require(logical('/prototype-state/jobs.sqlite3') == baseline['logicalSha256'], 'copied_state_changed')
    require(private_material() == baseline['privateMaterialSha256'], 'private_material_changed')
    handler = make_handler(config)
    server = None; thread = None; stop = threading.Event(); outcomes = []
    try:
        require(handler.prototype_queue.authority_id == binding.authority_id, 'application_authority_changed')
        require(getattr(handler.reference_listener.control.worker, 'recovery', None) is None, 'recovery_not_permitted')
        server = ThreadingHTTPServer((config.bind_host, config.port), handler)
        service = CoordinatorService(server, handler)
        thread = threading.Thread(target=lambda: outcomes.append(service.run(stop)))
        thread.start()
        deadline = time.monotonic()+10
        while not service.healthy() and thread.is_alive() and time.monotonic() < deadline: stop.wait(0.01)
        require(service.healthy(), 'application_not_healthy')
        context = ssl.create_default_context(cafile=value['listener']['clientCA'])
        opener = build_opener(ProxyHandler({}), HTTPSHandler(context=context))
        def get(path, token=None, tls=False):
            port = value['listener']['port'] if tls else config.port
            headers = {} if token is None else {'Authorization':'Bearer '+token}
            url = ('https' if tls else 'http')+'://127.0.0.1:'+str(port)+path
            try:
                with opener.open(Request(url, headers=headers), timeout=3) as response:
                    return response.status, json.loads(response.read(262144))
            except HTTPError as error:
                try: return error.code, json.loads(error.read(262144))
                finally: error.close()
        require(get('/healthz')[0] == 200, 'native_health_failed')
        require(get('/v1/prototype/authority', config.api_token)[1]['authorityId'] == binding.authority_id, 'native_authority_failed')
        require(get('/ltds/v1/jobs/00000000-0000-4000-8000-000000000099', tls=True)[0] == 401, 'reference_auth_failed')
        viewer = next(iter(value['viewerBearers'].values()))
        require(get('/ltds/v1/jobs/00000000-0000-4000-8000-000000000099', viewer, True)[0] == 404, 'reference_readonly_role_failed')
        worker = next(iter(value['workers'].values()))['bearer']
        require(get('/ltds/v1/jobs/00000000-0000-4000-8000-000000000099', worker, True)[0] == 401, 'reference_role_isolation_failed')
        try: extra = Queue('/prototype-state', node_ids=('cluster', 'main'), reference_enabled=True)
        except (RuntimeError, ValueError): pass
        else:
            extra.close(); raise QualificationHeld('second_application_owner_admitted')
    finally:
        stop.set()
        if thread is not None:
            thread.join(15)
            require(not thread.is_alive(), 'application_shutdown_uncertain')
        else:
            if server: server.server_close()
            handler.reference_listener.close(); handler.viewer_service.close(); handler.prototype_queue.close()
    require(outcomes == [0] and not service.health()['nativeAlive'] and not service.health()['referenceAlive'], 'application_shutdown_failed')
    require(not handler.viewer_service._thread.is_alive(), 'dispatcher_shutdown_failed')
    for port in (config.port, value['listener']['port']):
        with socket.socket() as probe:
            probe.settimeout(1); require(probe.connect_ex(('127.0.0.1', port)) != 0, 'listener_not_closed')
    reopened = Queue('/prototype-state', node_ids=('cluster', 'main'), reference_enabled=True)
    try: require(reopened.authority_id == binding.authority_id, 'shutdown_authority_changed')
    finally: reopened.close()
    after = volume_census('/prototype-state', authority=binding.authority_id)
    compare_transition(after, baseline['census'], binding=binding)
    require(logical('/prototype-state/jobs.sqlite3') == baseline['logicalSha256'] and private_material() == baseline['privateMaterialSha256'], 'post_application_state_or_material_changed')
    return dict(state='qualified', logicalSha256=baseline['logicalSha256'], privateMaterialSha256=baseline['privateMaterialSha256'], authorityId=binding.authority_id, recoveryEnabled=False, nativeClosed=True, referenceClosed=True)


def helper(phase, document):
    require(os.geteuid() == 10001 and os.getegid() == 10001, 'application_user_changed')
    require(type(document) is dict and set(document) == {'binding', 'baseline', 'applicationEnvironmentKeys'}, 'closed_helper_input_required')
    commit = os.environ.get('QUALIFICATION_SOURCE_COMMIT')
    require(commit in SOURCES, 'application_source_required')
    exact_source(commit)
    profile = QualificationBinding(**document['binding']); binding = profile.validate()
    require(commit in (binding.predecessor_source, binding.candidate_source), 'application_source_required')
    keys = document['applicationEnvironmentKeys']
    require(type(keys) is list and 1 <= len(keys) <= 128 and all(type(key) is str and re.fullmatch('[A-Z][A-Z0-9_]{0,127}', key) for key in keys)
        and len(keys) == len(set(keys)) and 'HOSTNAME' not in keys, 'bounded_application_environment_descriptor_required')
    actual = {key:os.environ.get(key) for key in keys}
    require(all(type(value) is str for value in actual.values()) and digest(actual) == binding.application_environment_sha256, 'actual_helper_environment_changed')
    if phase == 'snapshot': return snapshot(binding)
    require(phase == 'application', 'unknown_helper_phase')
    return application(binding, document['baseline'])


class LocalDocker:
    def __init__(self):
        self._identity = self._facts()

    @staticmethod
    def _facts():
        require(os.name == 'posix', 'local_linux_docker_required')
        binary = Path(DOCKER_BINARY); connection = Path(DOCKER_SOCKET)
        executable = binary.lstat(); endpoint = connection.lstat()
        require(binary.resolve() == binary and stat.S_ISREG(executable.st_mode) and executable.st_uid == 0
            and not executable.st_mode & 0o022 and stat.S_ISSOCK(endpoint.st_mode) and endpoint.st_uid == 0, 'fixed_local_docker_required')
        return (executable.st_dev, executable.st_ino, executable.st_size, executable.st_mtime_ns,
            endpoint.st_dev, endpoint.st_ino)

    def __call__(self, *args, stdin=None):
        require(self._facts() == self._identity, 'local_docker_identity_changed')
        env = dict(os.environ)
        for key in ('DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH', 'DOCKER_API_VERSION'):
            env.pop(key, None)
        result = subprocess.run([DOCKER_BINARY, '--host', DOCKER_HOST, *args], input=stdin, capture_output=True, timeout=90, env=env)
        require(result.returncode == 0 and len(result.stdout) <= 2097152, 'docker_outcome_uncertain')
        return result.stdout.decode('utf-8')


def save(root, name, value):
    raw = packed(value)
    target = root/name
    fd = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    try:
        require(os.write(fd, raw) == len(raw), 'checkpoint_short_write'); os.fsync(fd)
    finally: os.close(fd)
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def qualify(profile, operation, *, docker=None):
    """Explicit one-shot operation. Any existing directory/unknown outcome holds."""
    binding = profile.validate(); docker = docker or LocalDocker()
    operation = Path(operation).absolute()
    require(os.name == 'posix' and operation.parent == Path(OPERATION_ROOT) and operation.resolve() == operation
        and not any(p.is_symlink() for p in operation.parents), 'private_local_operation_required')
    parent_info = operation.parent.lstat()
    require(stat.S_ISDIR(parent_info.st_mode) and parent_info.st_uid == os.geteuid()
        and stat.S_IMODE(parent_info.st_mode) == 0o700, 'private_qualification_root_required')
    # No implicit resumption or deletion of a partially completed qualification.
    os.mkdir(operation, 0o700)
    parent_fd = os.open(operation.parent, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(parent_fd)
    finally: os.close(parent_fd)
    save(operation, 'binding.json', asdict(profile))
    profile_bytes = packed(asdict(profile))
    module_root = Path(__file__).resolve().parents[1]
    helper_hashes, helper_payload = helper_sources(module_root)
    require(docker('info', '--format', '{{.ID}}').strip() == profile.engine_id, 'local_engine_identity_changed')
    old = json.loads(docker('inspect', binding.predecessor_container))[0]
    require(old['Id'] == binding.predecessor_container and old['Image'] == binding.predecessor_image
        and old['State']['Running'] and old['Config']['User'] == '10001:10001', 'actual_predecessor_changed')
    mounts = old['Mounts']
    require(sum(m['Type']=='volume' and m['Name']==profile.live_volume and m['Destination']=='/prototype-state' and m['RW'] for m in mounts) == 1
        and sum(m['Type']=='bind' and m['Source']==profile.tls_root and m['Destination']=='/run/processing-reference' and not m['RW'] for m in mounts) == 1, 'actual_private_mount_recipe_changed')
    env_raw = private_file(profile.native_env_file, 65536)
    env_info = Path(profile.native_env_file).lstat()
    require(env_info.st_uid == os.geteuid() and not env_info.st_mode & 0o077, 'private_environment_recipe_required')
    env_identity = (env_info.st_dev, env_info.st_ino, env_info.st_mode, env_info.st_uid, env_info.st_gid,
        env_info.st_size, env_info.st_mtime_ns, env_info.st_ctime_ns)
    application_env = {}
    for line in env_raw.decode().splitlines():
        if not line or line.startswith('#'): continue
        require('=' in line, 'environment_recipe_invalid')
        key, val = line.split('=', 1)
        require(re.fullmatch('[A-Z][A-Z0-9_]*', key) is not None and key not in application_env, 'environment_recipe_invalid')
        application_env[key] = val
    application_env['PROCESSING_REFERENCE_CONFIG'] = profile.config_path
    require(digest(application_env) == binding.application_environment_sha256, 'application_environment_changed')
    images = {}
    for image in (binding.predecessor_image, profile.candidate_image):
        info = json.loads(docker('image', 'inspect', image))[0]
        require(info['Id'] == image and info['Config']['User'] == '10001:10001', 'actual_image_changed')
        images[image] = info
    defaults = dict(item.split('=', 1) for item in images[binding.predecessor_image]['Config'].get('Env', []))
    require(dict(item.split('=', 1) for item in old['Config']['Env']) == dict(defaults, **application_env), 'live_environment_recipe_changed')
    require(images[binding.predecessor_image]['Config'].get('Env') == images[profile.candidate_image]['Config'].get('Env'), 'image_environment_defaults_changed')
    volume = 'ltds-private-app-qualifier-'+os.urandom(16).hex()
    save(operation, 'volume-intent.json', dict(volume=volume, label=digest(asdict(profile)), helperHashes=helper_hashes))
    require(not docker('volume', 'ls', '--filter', 'name=^'+volume+'$', '--format', '{{.Name}}').strip(), 'disposable_name_already_exists')
    require(docker('volume', 'create', '--label', 'com.ledgetop.qualifier='+digest(asdict(profile)), volume).strip() == volume, 'volume_create_outcome_uncertain')
    def volume_identity():
        values = json.loads(docker('volume', 'inspect', volume))
        fields = {'Name', 'Driver', 'Labels', 'Mountpoint', 'Options', 'Scope', 'CreatedAt'}
        require(type(values) is list and len(values) == 1 and type(values[0]) is dict and set(values[0]) == fields, 'closed_volume_identity_required')
        value = values[0]
        require(value['Name'] == volume and value['Driver'] == 'local' and value['Scope'] == 'local'
            and value['Options'] in (None, {}) and value['Labels'] == {'com.ledgetop.qualifier':digest(asdict(profile))}
            and type(value['CreatedAt']) is str and 1 <= len(value['CreatedAt']) <= 64
            and type(value['Mountpoint']) is str and Path(value['Mountpoint']).is_absolute()
            and Path(value['Mountpoint']).name == '_data' and Path(value['Mountpoint']).parent.name == volume, 'owned_local_volume_required')
        return value
    owned_volume = volume_identity()
    save(operation, 'volume-created.json', owned_volume)
    common = ['run', '--rm', '--pull', 'never', '--network', 'none', '--read-only', '--cpus', '2', '--memory', '2g', '--pids-limit', '128',
        '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--tmpfs', '/tmp:rw,nosuid,nodev,size=256m', '--log-driver', 'none']
    save(operation, 'initialize-intent.json', dict(volume=volume, containerName=volume+'-initialize'))
    require(volume_identity() == owned_volume, 'disposable_volume_identity_changed')
    docker(*common, '--name', volume+'-initialize', '--label', 'com.ledgetop.qualifier='+digest(asdict(profile)),
        '--user', '0', '--cap-add', 'CHOWN', '--mount', 'type=volume,src='+volume+',dst=/copy', '--entrypoint', 'python',
        binding.predecessor_image, '-c', initializer_source())
    save(operation, 'initialize-complete.json', dict(volume=volume))
    launcher = streamed_launcher(helper_hashes)
    phases = [('snapshot', binding.predecessor_image, binding.predecessor_source), ('old', binding.predecessor_image, binding.predecessor_source),
        ('candidate', profile.candidate_image, binding.candidate_source), ('rollback', binding.predecessor_image, binding.predecessor_source)]
    baseline = None
    for phase, image, commit in phases:
        require(packed(asdict(profile)) == profile_bytes, 'qualification_binding_changed')
        require(helper_sources(module_root)[0] == helper_hashes, 'qualification_helper_changed')
        require(private_file(profile.native_env_file, 65536) == env_raw, 'environment_file_changed')
        info = Path(profile.native_env_file).lstat()
        require((info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid, info.st_size, info.st_mtime_ns, info.st_ctime_ns) == env_identity, 'environment_file_identity_changed')
        require(docker('info', '--format', '{{.ID}}').strip() == profile.engine_id and volume_identity() == owned_volume, 'qualification_engine_or_volume_changed')
        save(operation, phase+'-intent.json', dict(phase=phase, image=image, sourceCommit=commit, volume=volume, containerName=volume+'-'+phase))
        args = common+['--name', volume+'-'+phase, '--label', 'com.ledgetop.qualifier='+digest(asdict(profile)),
            '--interactive', '--env-file', profile.native_env_file, '-e', 'PROCESSING_REFERENCE_CONFIG='+profile.config_path,
            '-e', 'QUALIFICATION_SOURCE_COMMIT='+commit, '--mount', 'type=volume,src='+volume+',dst=/prototype-state',
            '--mount', 'type=bind,src='+profile.tls_root+',dst=/run/processing-reference,readonly', '--workdir', '/app']
        if phase == 'snapshot': args += ['--mount', 'type=volume,src='+profile.live_volume+',dst=/source,readonly']
        raw = docker(*args, '--entrypoint', 'python', image, '-B', '-W', 'error::ResourceWarning', '-c', launcher,
            '--helper', 'snapshot' if phase == 'snapshot' else 'application',
            stdin=packed(dict(helpers=helper_payload, document=dict(binding=asdict(profile), baseline=baseline,
                applicationEnvironmentKeys=sorted(application_env)))))
        value = json.loads(raw)
        if phase == 'snapshot':
            require(type(value) is dict and set(value) == {'census', 'logicalSha256', 'privateMaterialSha256'}, 'closed_snapshot_result_required')
            binding.check_baseline(value['census'])
            for key in ('logicalSha256', 'privateMaterialSha256'):
                require(type(value[key]) is str and re.fullmatch('[a-f0-9]{64}', value[key]), 'snapshot_hash_required')
            baseline = value
        else:
            expected = dict(state='qualified', logicalSha256=baseline['logicalSha256'], privateMaterialSha256=baseline['privateMaterialSha256'],
                authorityId=binding.authority_id, recoveryEnabled=False, nativeClosed=True, referenceClosed=True)
            require(packed(value) == packed(expected), 'application_result_changed')
        save(operation, 'snapshot.json' if phase == 'snapshot' else phase+'-complete.json', value)
    require(private_file(profile.native_env_file, 65536) == env_raw, 'environment_file_changed')
    info = Path(profile.native_env_file).lstat()
    require((info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid, info.st_size, info.st_mtime_ns, info.st_ctime_ns) == env_identity, 'environment_file_identity_changed')
    receipt = dict(state='qualified_copy_retained', bindingSha256=digest(asdict(profile)), volume=volume, remoteEffects=False,
        liveStateWritable=False, noActivation=True, completedPhases=[phase for phase, _, _ in phases])
    save(operation, 'receipt.json', receipt)
    return receipt


def main():
    if len(sys.argv) == 3 and sys.argv[1] == '--helper':
        try:
            raw = sys.stdin.buffer.read(1048577)
            require(len(raw) <= 1048576, 'helper_input_bound')
            print(json.dumps(helper(sys.argv[2], json.loads(raw)))); return 0
        except Exception:
            print(json.dumps(dict(state='held', code='qualification_helper_failed'))); return 2
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binding', required=True, type=Path)
    parser.add_argument('--operation-dir', required=True, type=Path)
    parser.add_argument('--execute-isolated-qualification', action='store_true')
    args = parser.parse_args()
    try:
        profile = QualificationBinding(**json.loads(private_file(args.binding, 65536)))
        profile.validate()
        require(args.execute_isolated_qualification, 'explicit_isolated_qualification_required')
        print(json.dumps(qualify(profile, args.operation_dir))); return 0
    except Exception:
        print(json.dumps(dict(state='held', code='qualification_not_completed'))); return 2


if __name__ == '__main__': raise SystemExit(main())
