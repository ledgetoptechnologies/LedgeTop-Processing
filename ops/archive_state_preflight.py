"""Read-only stopped-state comparison; not a writer fence or updater.

The caller must supply a trusted previous census and independently qualified
exact schema fingerprint map for the candidate. This module does not attest
the origin of either input, verify an image, reserve space, or authorize start.
"""
import os
from pathlib import Path
import re
import sqlite3

from ops import state_census


class PreflightHold(Exception):
    pass


def _partial_present(root):
    count = 1
    pending = [root]
    try:
        while pending:
            with os.scandir(pending.pop()) as entries:
                for entry in entries:
                    count += 1
                    if count > state_census.MAX_TREE_ENTRIES:
                        raise PreflightHold('tree_limit_exceeded')
                    name = entry.name
                    if (name.endswith('.partial') or name.startswith('.incoming-')
                            or name in ('.all.zip.restore', '.all.zip.restore-pending')):
                        return True
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(entry.path)
    except OSError:
        raise PreflightHold('tree_unreadable') from None
    return False


def inspect(root, key, baseline, qualified_schema):
    """Return a fresh exact census only when all conservative checks pass."""
    if not isinstance(key, bytes) or len(key) != 32:
        raise PreflightHold('invalid_key')
    if (not isinstance(qualified_schema, dict) or not qualified_schema
            or any(not isinstance(name, str) or not isinstance(value, str)
                   or not re.fullmatch(r'[0-9a-f]{64}', value)
                   for name, value in qualified_schema.items())):
        raise PreflightHold('invalid_qualified_schema')
    root = Path(root)
    try:
        if root.is_symlink() or not root.is_dir() or root.absolute() != root.resolve():
            raise PreflightHold('invalid_root')
        first = state_census.census(root, key)
        if _partial_present(root):
            raise PreflightHold('partial_requires_inspection')
        second = state_census.census(root, key)
        if first != second or first != baseline:
            raise PreflightHold('state_changed')
        tree = first['tree']
        database = first['database']
        if (any(tree['rejected'].values()) or tree['incomingPartials']
                or tree['spoolEntries'] not in (None, 0)):
            raise PreflightHold('tree_requires_inspection')
        if (database['quickCheck'] != 'ok' or database['sqliteHeaderMode'] != [1, 1]
                or database['sidecars'] or set(database['active']) != {'jobs', 'viewerTasks'}
                or any(database['active'].values())):
            raise PreflightHold('database_requires_inspection')
        if database['objects'] != qualified_schema:
            raise PreflightHold('schema_not_qualified')
        if not {'table:jobs', 'table:viewer_tasks', 'table:coordinator_identity'} <= set(qualified_schema):
            raise PreflightHold('schema_not_qualified')
        return first
    except PreflightHold:
        raise
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error, SystemExit):
        raise PreflightHold('state_unavailable') from None
