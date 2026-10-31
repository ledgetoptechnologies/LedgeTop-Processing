"""Cooperative per-engine lease for maintenance mutations.

The lease is separate from the node's job gate and the update supervisor's
lease. Hold all applicable leases when rebinding an installed updater. This is
not a fence against legacy code, another OS user, or direct Docker access.
"""
from contextlib import contextmanager
import os
import json
import hashlib
from pathlib import Path
import re
import stat
import subprocess

class MaintenanceAuthorityBusy(RuntimeError):
    pass


def local_engine_id():
    """Discover the local Docker identity before reading managed configuration."""
    context = os.environ.get('DOCKER_CONTEXT')
    endpoint = os.environ.get('DOCKER_HOST') if not context else None
    try:
        if not endpoint:
            command = ['docker', 'context', 'inspect', '--format', '{{json .Endpoints.docker.Host}}']
            if context:
                command.append(context)
            endpoint = json.loads(subprocess.run(command, capture_output=True, text=True, timeout=20, check=True).stdout)
        if not isinstance(endpoint, str) or not endpoint.startswith('unix:///'):
            raise MaintenanceAuthorityBusy('local_docker_endpoint_required')
        result = subprocess.run(['docker', 'info', '--format', '{{.ID}}'], capture_output=True,
                                text=True, timeout=20, check=True).stdout.strip()
        if not result:
            raise MaintenanceAuthorityBusy('docker_engine_identity_unavailable')
        return result
    except (OSError, subprocess.SubprocessError, ValueError):
        raise MaintenanceAuthorityBusy('docker_engine_identity_unavailable') from None


def authority_root(engine_id):
    if not isinstance(engine_id, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9:._-]{0,255}', engine_id):
        raise MaintenanceAuthorityBusy('invalid_engine_identity')
    import pwd
    home = Path(pwd.getpwuid(os.geteuid()).pw_dir)
    if not home.is_absolute():
        raise MaintenanceAuthorityBusy('absolute_home_required')
    # Do not honor XDG_STATE_HOME: two environments for the same OS user must
    # not silently create independent leases for one physical engine.
    return home / '.local' / 'state' / 'LedgeTop-Processing' / 'maintenance-authority' / hashlib.sha256(engine_id.encode()).hexdigest()


@contextmanager
def maintenance_authority(engine_id):
    if os.name == 'nt':
        raise MaintenanceAuthorityBusy('linux_maintenance_authority_required')
    import fcntl
    root = authority_root(engine_id).absolute()
    if any(path.is_symlink() for path in (root, *root.parents)):
        raise MaintenanceAuthorityBusy('private_authority_root_required')
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    if root.resolve() != root or any(path.is_symlink() for path in (root, *root.parents)):
        raise MaintenanceAuthorityBusy('private_authority_root_required')
    info = root.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise MaintenanceAuthorityBusy('private_authority_root_required')
    path = root / 'maintenance-authority.lock'
    fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    locked = False
    try:
        current = os.fstat(fd)
        linked = path.lstat()
        if (not stat.S_ISREG(current.st_mode) or current.st_nlink != 1
                or current.st_uid != os.geteuid() or current.st_mode & 0o077
                or (current.st_dev, current.st_ino) != (linked.st_dev, linked.st_ino)):
            raise MaintenanceAuthorityBusy('private_authority_lock_required')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise MaintenanceAuthorityBusy('maintenance_authority_busy') from None
        locked = True
        yield
    finally:
        if locked:
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
