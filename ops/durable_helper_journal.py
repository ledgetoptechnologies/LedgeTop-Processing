"""Experimental helper intent journal; not wired into the live updater.

The caller must hold the updater's existing operation lock before using this
journal. A create with an unknown outcome remains held: Docker may complete a
timed-out create after a single empty listing.
"""
import fcntl
import json
import os
from pathlib import Path
import re
import stat


class JournalError(ValueError):
    pass


class HelperJournal:
    def __init__(self, root):
        self.root = Path(root).absolute()
        info = self.root.lstat()
        if (self.root.resolve() != self.root or not stat.S_ISDIR(info.st_mode)
                or info.st_uid != os.geteuid() or info.st_mode & 0o077):
            raise JournalError('private_journal_root_required')
        self.lock = None

    def __enter__(self):
        flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW
        self.lock = os.open(self.root / 'helper-journal.lock', flags, 0o600)
        info = os.fstat(self.lock)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            os.close(self.lock); self.lock = None
            raise JournalError('invalid_helper_lock')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(self.lock); self.lock = None
            raise JournalError('helper_journal_busy') from None
        return self

    def __exit__(self, *_):
        os.close(self.lock); self.lock = None

    def _assert_locked(self):
        if self.lock is None:
            raise JournalError('helper_journal_lock_required')

    def write(self, operation, phase, container_id=None):
        self._assert_locked()
        if not re.fullmatch('[a-f0-9]{32}', operation) or phase not in ('create_pending', 'created', 'start_pending', 'resolved'):
            raise JournalError('invalid_helper_record')
        if (phase == 'create_pending' and container_id is not None or phase != 'create_pending'
                and not re.fullmatch('[a-f0-9]{64}', container_id or '')):
            raise JournalError('invalid_helper_record')
        try: previous = self.read()
        except FileNotFoundError: previous = None
        if previous is None or previous['phase'] == 'resolved':
            if phase != 'create_pending' or previous is not None and previous['operation'] == operation:
                raise JournalError('invalid_helper_transition')
        else:
            allowed = {'create_pending': ('created',), 'created': ('start_pending', 'resolved'),
                       'start_pending': ('resolved',)}
            if (operation != previous['operation'] or phase not in allowed[previous['phase']]
                    or previous['containerId'] is not None and previous['containerId'] != container_id):
                raise JournalError('invalid_helper_transition')
        payload = json.dumps({'operation': operation, 'phase': phase, 'containerId': container_id},
                             sort_keys=True, separators=(',', ':')).encode()
        # Unique temp names prevent accidental truncation or symlink following.
        import uuid
        temporary = self.root / ('.helper-record-' + uuid.uuid4().hex)
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, 'wb') as out:
                out.write(payload); out.flush(); os.fsync(out.fileno())
            os.replace(temporary, self.root / 'helper-record.json')
            directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(directory)
            finally: os.close(directory)
        finally:
            if temporary.exists(): temporary.unlink()

    def read(self):
        self._assert_locked()
        fd = os.open(self.root / 'helper-record.json', os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid() or info.st_mode & 0o077 or info.st_size > 4096:
                raise JournalError('invalid_helper_record')
            with os.fdopen(fd, 'rb', closefd=False) as src: record = json.load(src)
        finally: os.close(fd)
        if type(record) is not dict or set(record) != {'operation', 'phase', 'containerId'}:
            raise JournalError('invalid_helper_record')
        op, phase, cid = record['operation'], record['phase'], record['containerId']
        if (type(op) is not str or not re.fullmatch('[a-f0-9]{32}', op)
                or phase not in ('create_pending', 'created', 'start_pending', 'resolved')
                or (phase == 'create_pending' and cid is not None)
                or (phase != 'create_pending' and (type(cid) is not str or not re.fullmatch('[a-f0-9]{64}', cid)))):
            raise JournalError('invalid_helper_record')
        return record

    def recover(self, transport):
        """Remove only a positively identified helper; never replay a start."""
        record = self.read()
        if record['phase'] == 'create_pending':
            raise JournalError('create_outcome_requires_inspection')
        if record['phase'] == 'resolved':
            return
        cid, op = record['containerId'], record['operation']
        label = 'io.ledgetop.update-helper=' + op
        ids = transport.docker('ps', '-a', '--no-trunc', '--filter', 'label=' + label,
                               '--format', '{{.ID}}', timeout=30).splitlines()
        if ids not in ([], [cid]):
            raise JournalError('helper_identity_ambiguous')
        if ids:
            item = transport.inspected(cid, timeout=30)
            if (item.get('Id') != cid or item.get('Name') != '/ledgetop-update-helper-' + op
                    or (item.get('Config', {}).get('Labels') or {}).get('io.ledgetop.update-helper') != op):
                raise JournalError('helper_identity_ambiguous')
            transport.docker('rm', '-f', cid, timeout=30)
        if transport.docker('ps', '-a', '--no-trunc', '--filter', 'label=' + label,
                            '--format', '{{.ID}}', timeout=30).splitlines():
            raise JournalError('helper_cleanup_unconfirmed')
        self.write(op, 'resolved', cid)
