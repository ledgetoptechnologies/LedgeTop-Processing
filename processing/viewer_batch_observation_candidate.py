"""Inactive, coordinator-local indexed-upload observation; never authorizes replay.

Requires an existing per-task reservation. This observes database metadata only;
filesystem custody, authority/epoch, principal and writer fencing are not proven.
"""
import re
from .maintenance import Conflict


def inspect_indexed_batch(store, identifier, environment, capability, generation, files):
    if type(generation) is not int or generation < 1 or type(files) is not list or not 1 <= len(files) <= 100:
        raise ValueError('invalid batch')
    requested = []
    names = set()
    for item in files:
        if type(item) is not dict or set(item) != {'name', 'bytes', 'sha256'}:
            raise ValueError('invalid batch item')
        key = store._filename(item['name'])
        size, digest = item['bytes'], item['sha256']
        if key in names or type(size) is not int or size < 1 or not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest):
            raise ValueError('invalid batch item')
        names.add(key)
        requested.append((key, size, digest))
    with store._lock, store.queue._db() as db:
        db.execute('BEGIN')
        # Legacy environment-only jobs cannot use this observation.
        if store._reservation(db, identifier, environment, capability) is None:
            raise Conflict('reservation required')
        row = store._row(db, identifier)
        if row['environment'] != environment or row['generation'] != generation or row['stage'] != 'uploading' or row['job_id']:
            raise Conflict('task generation unavailable')
        states = []
        for key, size, digest in requested:
            found = db.execute('SELECT bytes,sha256 FROM viewer_files WHERE uuid=? AND name_key=?', (identifier, key)).fetchone()
            states.append('missing' if found is None else 'matched' if found['bytes'] == size and found['sha256'] == digest else 'conflict')
        return {'version': 1, 'state': 'held_index_only', 'generation': generation, 'files': states}
