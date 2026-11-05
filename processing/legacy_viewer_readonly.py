"""Default-disabled delivery of already retained legacy terminal Viewer tasks.

No dispatcher, engine client, TaskStore constructor, cache repair or acceptance
authority lives here. The application owns its original Queue/leases and HTTP
freeze operation for the entire read. A missing/corrupt cache is unavailable.
"""
from contextlib import contextmanager
import hashlib
import hmac
import json
import os
from pathlib import Path, PurePosixPath
import re
import sqlite3
import stat
import uuid
import zipfile

from .viewer_http import ViewerFacade, RequestError


def _require(value):
    if not value:
        raise RequestError(503, 'legacy_retained_data_unavailable')


def _identity(value):
    return (value.st_dev, value.st_ino, value.st_mode, value.st_uid,
            value.st_gid, value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _json(value, maximum=1024 * 1024):
    _require(type(value) is str and len(value.encode('utf-8')) <= maximum)
    def pairs(items):
        result = {}
        for key, item in items:
            _require(key not in result)
            result[key] = item
        return result
    return json.loads(value, object_pairs_hook=pairs,
                      parse_constant=lambda _: _require(False))


class LegacyViewerReadonly:
    def __init__(self, queue, *, max_archive_bytes):
        _require(os.name == 'posix' and queue is not None and type(max_archive_bytes) is int
                 and 4 <= max_archive_bytes <= 10**15)
        self.queue, self.maximum = queue, max_archive_bytes
        self.root = Path(queue.path).absolute().parent

    @contextmanager
    def _db(self):
        # No new authority lease or normal state/service constructor.
        self.queue._coordinator_freeze._check()
        with self.queue._db():
            # The owning Queue context retains lifecycle accounting; a distinct
            # read-only connection cannot accidentally mutate retained rows.
            db = sqlite3.connect(Path(self.queue.path).as_uri() + '?mode=ro', uri=True)
            db.row_factory = sqlite3.Row
            try:
                db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 1024 * 1024)
                db.execute('BEGIN')
                yield db
            except sqlite3.Error:
                raise RequestError(503, 'legacy_retained_data_unavailable') from None
            finally:
                db.close()

    def _task(self, db, identifier, environment):
        task = db.execute('SELECT * FROM viewer_tasks WHERE uuid=? AND removed=0 AND environment=?',
                          (identifier, environment)).fetchone()
        if task is None:
            raise KeyError('unknown task')
        task = dict(task)
        job = db.execute('SELECT * FROM jobs WHERE id=?', (task['job_id'],)).fetchone()
        _require(job is not None)
        job = dict(job)
        generation = task['generation']
        _require(type(generation) is int and generation > 0 and str(uuid.UUID(identifier)) == identifier)
        request = identifier if generation == 1 else str(uuid.uuid5(uuid.UUID(identifier), 'generation:' + str(generation)))
        _require(job['profile'] == 'odm-viewer-v1' and job['request_id'] == request
                 and job['environment'] == environment and job['status'] in ('succeeded', 'failed')
                 and task['environment'] == environment and task['node_id'] in self.queue.node_ids
                 and task['node_id'] == job['node_id'] and type(task['claim_token']) is str
                 and type(job['claim_hash']) is str and hmac.compare_digest(
                     hashlib.sha256(task['claim_token'].encode()).hexdigest(), job['claim_hash'])
                 and type(task['engine_uuid']) is str and str(uuid.UUID(task['engine_uuid'])) == task['engine_uuid']
                 and type(task['directory']) is str and re.fullmatch(re.escape(identifier) + '-' + str(generation)
                     + '(?:-[a-f0-9]{16})?', task['directory']))
        _require((job['status'] == 'succeeded' and task['stage'] == 'succeeded' and task['phase'] == 'complete')
                 or (job['status'] == 'failed' and task['stage'] in ('failed', 'cancelled')))
        files, metadata_bytes = [], 0
        for row in db.execute('SELECT * FROM viewer_files WHERE uuid=? LIMIT 10001', (identifier,)):
            item = dict(row)
            metadata_bytes += sum(len(value.encode()) if type(value) is str else 16 for value in item.values())
            _require(len(files) < 10000 and metadata_bytes <= 1024 * 1024)
            files.append(item)
        paths = set()
        for item in files:
            _require(type(item['path']) is str and re.fullmatch('input-[a-f0-9]{32}', item['path'])
                     and item['path'] not in paths and type(item['bytes']) is int and item['bytes'] > 0
                     and type(item['sha256']) is str and re.fullmatch('[a-f0-9]{64}', item['sha256'])
                     and item['is_photo'] in (0, 1))
            paths.add(item['path'])
        task['_job'], task['_files'] = job, files
        return task

    def info(self, identifier, environment):
        with self._db() as db:
            task = self._task(db, identifier, environment)
        _require(type(task['name']) is str and len(task['name']) <= 256)
        code = 40 if task['_job']['status'] == 'succeeded' else 50 if task['stage'] == 'cancelled' else 30
        return dict(uuid=identifier, name=task['name'], status=dict(code=code),
                    progress=100 if code == 40 else 0,
                    imagesCount=sum(item['is_photo'] for item in task['_files']), dateCreated=task['created_at'])

    def output(self, identifier, environment, line):
        with self._db() as db:
            task = self._task(db, identifier, environment)
        lines = _json(task['output_lines'])
        _require(type(lines) is list and len(lines) <= 10000
                 and all(type(value) is str and len(value) <= 4096 for value in lines))
        return lines[line:line + 200]

    def list_tasks(self, environment):
        with self._db() as db:
            ids = [row[0] for row in db.execute('SELECT uuid FROM viewer_tasks WHERE removed=0 AND environment=? ORDER BY rowid LIMIT 1001', (environment,))]
            _require(len(ids) <= 1000)
            return [dict(uuid=identifier) for identifier in ids
                    if self._task(db, identifier, environment)]

    @staticmethod
    def _owned(fd, *, directory=False, root=False):
        value = os.fstat(fd)
        _require(value.st_uid == os.geteuid() and (stat.S_ISDIR(value.st_mode) if directory else
                 stat.S_ISREG(value.st_mode) and value.st_nlink == 1))
        _require(stat.S_IMODE(value.st_mode) in ((0o700, 0o755) if root else (0o700,))
                 if directory else not value.st_mode & 0o077)
        return value

    def download(self, identifier, environment):
        with self._db() as db:
            task = self._task(db, identifier, environment)
        _require(task['_job']['status'] == 'succeeded')
        directory = self.root / 'viewer-tasks' / task['directory']
        _require(task['archive_path'] in (None, str(directory / 'all.zip')))
        descriptors, stream = [], None
        try:
            _require(self.root.resolve() == self.root and not any(p.is_symlink() for p in self.root.parents))
            rootfd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            descriptors.append(rootfd); self._owned(rootfd, directory=True, root=True)
            for name in ('viewer-tasks', task['directory']):
                fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptors[-1])
                descriptors.append(fd); self._owned(fd, directory=True)
            folder = descriptors[-1]
            expected = {item['path'] for item in task['_files']} | {'all.zip', 'all.zip.receipt'}
            _require(set(os.listdir(folder)) == expected)
            for item in task['_files']:
                fd = os.open(item['path'], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=folder)
                try: _require(self._owned(fd).st_size == item['bytes'])
                finally: os.close(fd)
            fd = os.open('all.zip.receipt', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=folder)
            try:
                before = self._owned(fd); _require(before.st_size <= 65536)
                receipt = _json(os.read(fd, before.st_size + 1).decode(), 65536)
                _require(_identity(before) == _identity(os.fstat(fd)))
            finally: os.close(fd)
            identity = dict(version=1, task=identifier, generation=task['generation'], job=task['job_id'],
                            node=task['node_id'], engine=task['engine_uuid'])
            _require(type(receipt) is dict and set(receipt) == set(identity) | {'bytes', 'sha256'}
                     and all(type(receipt[key]) is type(value) and receipt[key] == value for key, value in identity.items())
                     and type(receipt['bytes']) is int and 4 <= receipt['bytes'] <= self.maximum
                     and type(receipt['sha256']) is str and re.fullmatch('[a-f0-9]{64}', receipt['sha256']))
            fd = os.open('all.zip', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=folder)
            stream = os.fdopen(fd, 'rb'); before = self._owned(fd)
            _require(before.st_size == receipt['bytes'])
            digest = hashlib.sha256(); remaining = receipt['bytes']
            while remaining:
                chunk = stream.read(min(1024 * 1024, remaining))
                _require(bool(chunk)); remaining -= len(chunk); digest.update(chunk)
            _require(hmac.compare_digest(digest.hexdigest(), receipt['sha256']))
            # Central directory only; no extraction or new inventory authority.
            end = zipfile._EndRecData(stream)
            _require(end is not None and 0 < end[zipfile._ECD_ENTRIES_TOTAL] <= 10000
                     and end[zipfile._ECD_SIZE] <= 8 * 1024 * 1024)
            with zipfile.ZipFile(stream) as archive:
                entries = archive.infolist(); _require(0 < len(entries) <= 10000)
                names = set(); expanded = 0
                for entry in entries:
                    name = entry.filename; path = PurePosixPath(name)
                    _require(name and len(name.encode()) <= 1024 and '\\' not in name and ':' not in name
                             and not path.is_absolute() and all(part not in ('', '.', '..') for part in name.rstrip('/').split('/'))
                             and name.casefold() not in names and not entry.flag_bits & 1
                             and stat.S_IFMT(entry.external_attr >> 16) in (0, stat.S_IFREG, stat.S_IFDIR))
                    names.add(name.casefold()); expanded += entry.file_size
                    _require(expanded <= self.maximum)
            _require(_identity(before) == _identity(os.fstat(stream.fileno())) ==
                     _identity(os.stat('all.zip', dir_fd=folder, follow_symlinks=False)))
            stream.seek(0)
            result, stream = stream, None
            return result, receipt['bytes']
        except (OSError, ValueError, sqlite3.Error, zipfile.BadZipFile):
            raise RequestError(503, 'legacy_retained_data_unavailable') from None
        finally:
            if stream is not None: stream.close()
            for fd in reversed(descriptors): os.close(fd)


class LegacyViewerReadonlyFacade(ViewerFacade):
    def _post(self, handler, path, environment):
        raise RequestError(405, 'legacy_read_only')

    def _get(self, handler, path, query, environment):
        # Reuse only the mature HTTP writer/authentication, never mutation code.
        if path in ('/info', '/options'):
            raise RequestError(405, 'legacy_read_only')
        if path == '/task/list':
            if handler.headers.get('Transfer-Encoding') or handler.headers.get_all('Content-Length', []) not in ([], ['0']):
                raise RequestError(400, 'unexpected_body')
            self._reply(handler, 200, self.service.list_tasks(environment)); return
        class Bound:
            def authorize(_, identifier, requested):
                if requested != environment: raise KeyError('unknown task')
                self.service.info(identifier, environment)
            def info(_, identifier): return self.service.info(identifier, environment)
            def output(_, identifier, line): return self.service.output(identifier, environment, line)
            def download(_, identifier): return self.service.download(identifier, environment)
        # A request-local facade avoids mutable shared environment state.
        ViewerFacade(Bound(), self.token, self.staging_token)._get(handler, path, query, environment)
