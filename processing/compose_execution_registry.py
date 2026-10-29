"""Private bounded provenance for exact, immutable Compose execution recipes.

No Docker execution, migration, cleanup or arbitrary receipt adoption. A source
model is supplied only by ComposeNode's actual CLI observation. Registered blobs
are retained for later updates and rollback; incomplete/unknown state holds.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat

DIRECTORY = '.compose-executions'
MAX_BYTES = 1024 * 1024
MAX_ENTRIES = 64
HEX = re.compile(r'[a-f0-9]{64}\Z')
IMAGE = re.compile(r'sha256:[a-f0-9]{64}\Z')
NAME = re.compile(r'[a-z0-9][a-z0-9_-]{0,62}\Z')


class ComposeProvenanceHeld(ValueError):
    pass


def require(value):
    if not value:
        raise ComposeProvenanceHeld('owned Compose execution provenance unavailable')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def facts(info):
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns,
            info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode), info.st_nlink]


def private(path, *, directory=False):
    path = Path(path).absolute()
    require(path.resolve() == path and not any(p.is_symlink() for p in (path, *path.parents)))
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode) and info.st_nlink == 1)
    if os.name != 'nt':
        require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == (0o700 if directory else 0o600))
    return info


def read(path, maximum=MAX_BYTES):
    path = Path(path)
    before = private(path)
    require(before.st_size <= maximum)
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    try:
        # Windows path stat reports creation time as ctime, whereas descriptor
        # stat can report last change time. Compare that field only on POSIX;
        # full path snapshots are still required unchanged before/after read.
        descriptor_facts = lambda value: facts(value) if os.name != 'nt' else facts(value)[:4] + facts(value)[5:]
        require(descriptor_facts(os.fstat(fd)) == descriptor_facts(before))
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            raw = stream.read(maximum + 1)
        require(len(raw) == before.st_size and descriptor_facts(os.fstat(fd)) == descriptor_facts(before)
                and facts(private(path)) == facts(before))
    finally:
        os.close(fd)
    return raw, facts(before)


def record(path):
    raw, _ = read(path, 16384)
    value = json.loads(raw)
    require(canonical(value) == raw)
    return value


def sync(directory):
    if os.name == 'nt':
        return
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def once(path, raw):
    require(len(raw) <= MAX_BYTES)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        pending = memoryview(raw)
        while pending:
            count = os.write(fd, pending)
            require(count > 0)
            pending = pending[count:]
        os.fsync(fd)
    finally:
        os.close(fd)
    sync(Path(path).parent)


def source_snapshot(path):
    path = Path(path).absolute()
    private(path.parent, directory=True)
    raw, identity = read(path)
    return dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest(), identity=identity)


def source_base(model, service):
    value = copy.deepcopy(model)
    require(type(value) is dict and type(value.get('services')) is dict
            and type(value['services'].get(service)) is dict)
    value['services'][service].pop('image', None)
    return digest(value)


class ComposeExecutionRegistry:
    def __init__(self, source, project, service, model):
        require(type(project) is str and NAME.fullmatch(project)
                and type(service) is str and NAME.fullmatch(service))
        self.source = Path(source).absolute()
        self.snapshot = source_snapshot(self.source)
        self.model = copy.deepcopy(model)
        require(self.model.get('name', project) == project)
        self.project, self.service = project, service
        self.base = source_base(model, service)
        self.root = self.source.parent / DIRECTORY

    def _binding(self):
        parent = private(self.source.parent, directory=True)
        root = private(self.root, directory=True)
        return dict(schemaVersion=1, purpose='owned-compose-resolved-executions',
                    sourcePath=str(self.source), sourceParentIdentity=[parent.st_dev, parent.st_ino],
                    registryIdentity=[root.st_dev, root.st_ino], project=self.project,
                    service=self.service, sourceBaseSha256=self.base)

    def _inventory(self):
        private(self.root, directory=True)
        require({p.name for p in self.root.iterdir()} == {'registry.json', 'reservations', 'artifacts', 'registrations'})
        actual = record(self.root / 'registry.json')
        require(canonical(actual) == canonical(self._binding()))
        names = {}
        for name in ('reservations', 'artifacts', 'registrations'):
            directory = self.root / name
            private(directory, directory=True)
            entries = list(directory.iterdir())
            require(len(entries) <= MAX_ENTRIES)
            require(all(p.suffix == '.json' and HEX.fullmatch(p.stem) for p in entries))
            for path in entries:
                require(private(path).st_size <= (MAX_BYTES if name == 'artifacts' else 16384))
            names[name] = {p.stem for p in entries}
        require(names['artifacts'] <= names['reservations'] and names['registrations'] <= names['artifacts'])
        for key in names['reservations']:
            self._reservation(key)
        for key in names['registrations']:
            self._registered(key)
        return names

    def _create(self):
        # An interrupted first registry creation is held, never silently adopted.
        self.root.mkdir(mode=0o700)
        sync(self.root.parent)
        for name in ('reservations', 'artifacts', 'registrations'):
            (self.root / name).mkdir(mode=0o700)
        once(self.root / 'registry.json', canonical(self._binding()))
        sync(self.root)

    def _reservation(self, key):
        require(HEX.fullmatch(key))
        reservation = record(self.root / 'reservations' / (key + '.json'))
        require(type(reservation) is dict and set(reservation) == {'schemaVersion', 'purpose', 'operationRoot', 'operationIdentity',
                'source', 'sourceBaseSha256', 'project', 'service', 'image', 'resolvedSha256', 'resolvedBytes'})
        require(type(reservation['schemaVersion']) is int and reservation['schemaVersion'] == 1
                and reservation['purpose'] == 'reserved-compose-execution'
                and reservation['resolvedSha256'] == key and reservation['sourceBaseSha256'] == self.base
                and reservation['project'] == self.project and reservation['service'] == self.service
                and type(reservation['image']) is str and IMAGE.fullmatch(reservation['image']))
        source = reservation['source']
        require(type(source) is dict and set(source) == {'path', 'sha256', 'identity'}
                and source['path'] == str(self.source)
                and type(source['sha256']) is str and HEX.fullmatch(source['sha256'])
                and type(source['identity']) is list and len(source['identity']) == 9
                and all(type(v) is int and v >= 0 for v in source['identity'])
                and source['identity'][-1] == 1
                and type(reservation['resolvedBytes']) is int and 0 < reservation['resolvedBytes'] <= MAX_BYTES
                and type(reservation['operationRoot']) is str and Path(reservation['operationRoot']).is_absolute()
                and type(reservation['operationIdentity']) is list and len(reservation['operationIdentity']) == 2
                and all(type(v) is int and v >= 0 for v in reservation['operationIdentity']))
        owner = private(Path(reservation['operationRoot']), directory=True)
        require(reservation['operationIdentity'] == [owner.st_dev, owner.st_ino])
        return reservation

    def _registered(self, key):
        reservation = self._reservation(key)
        artifact = self.root / 'artifacts' / (key + '.json')
        raw, identity = read(artifact)
        require(hashlib.sha256(raw).hexdigest() == key and len(raw) == reservation['resolvedBytes'])
        model = json.loads(raw)
        require(canonical(model) == raw and model.get('name', self.project) == self.project
                and model['services'][self.service]['image'] == reservation['image'])
        registered = record(self.root / 'registrations' / (key + '.json'))
        expected = dict(schemaVersion=1, purpose='registered-compose-execution', reservationSha256=digest(reservation),
                        artifactPath=str(artifact), artifactSha256=key, artifactIdentity=identity)
        require(canonical(registered) == canonical(expected))
        return dict(source=reservation['source'], sourceBaseSha256=self.base, project=self.project,
                    service=self.service, executed=dict(path=str(artifact), sha256=key, identity=identity,
                    image=reservation['image']), reservationSha256=digest(reservation))

    def reserve(self, resolved, image, operation_root):
        require(IMAGE.fullmatch(image))
        operation_root = Path(operation_root).absolute()
        owner = private(operation_root, directory=True)
        require(source_snapshot(self.source) == self.snapshot)
        expected = copy.deepcopy(self.model)
        expected['services'][self.service].update(image=image, pull_policy='never')
        require(canonical(expected) == canonical(resolved))
        raw = canonical(resolved)
        require(len(raw) <= MAX_BYTES)
        key = hashlib.sha256(raw).hexdigest()
        if not os.path.lexists(self.root):
            self._create()
        names = self._inventory()
        if key in names['registrations']:
            return self._registered(key)
        request = dict(schemaVersion=1, purpose='reserved-compose-execution', operationRoot=str(operation_root),
                       operationIdentity=[owner.st_dev, owner.st_ino],
                       source=self.snapshot, sourceBaseSha256=self.base, project=self.project, service=self.service,
                       image=image, resolvedSha256=key, resolvedBytes=len(raw))
        intent = self.root / 'reservations' / (key + '.json')
        if key in names['reservations']:
            require(canonical(record(intent)) == canonical(request))
        else:
            require(len(names['reservations']) < MAX_ENTRIES)
            once(intent, canonical(request))
        artifact = self.root / 'artifacts' / (key + '.json')
        if os.path.lexists(artifact):
            require(read(artifact)[0] == raw)
        else:
            once(artifact, raw)
        _, identity = read(artifact)
        once(self.root / 'registrations' / (key + '.json'), canonical(dict(schemaVersion=1,
             purpose='registered-compose-execution', reservationSha256=digest(request),
             artifactPath=str(artifact), artifactSha256=key, artifactIdentity=identity)))
        return self._registered(key)

    def verify(self, label, image):
        """No registry creation or recipe adoption during a read-only census."""
        self._inventory()
        path = Path(label)
        require(path.is_absolute() and path.parent == self.root / 'artifacts'
                and path.suffix == '.json' and HEX.fullmatch(path.stem))
        proof = self._registered(path.stem)
        require(proof['executed']['path'] == label and proof['executed']['image'] == image)
        return proof
