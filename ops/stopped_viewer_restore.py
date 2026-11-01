"""Restore a verified stopped backup to a new independent destination.

This is an offline prototype, not an automatic migration or a writer fence.
The operator must exclude all writers and independently provision source,
destination and trusted original inventory. Never restore into an existing
path. On failure or a lost response, retain the destination and independently
verify it before deciding on a separate recovery action.
"""
import argparse
import json
import os
import sqlite3
from pathlib import Path
import stat
import sys

from ops.stopped_viewer_backup import BackupRefused, backup

MAX_INVENTORY_BYTES = 64 * 1024 * 1024


def restore(source, destination, lease_directory, authority_id, key, expected_inventory, *,
            stopped_writers_confirmed=False, independent_destination_confirmed=False,
            original_inventory_trusted=False):
    """Copy only if the locked source matches its independently kept inventory."""
    if not original_inventory_trusted or not isinstance(expected_inventory, dict):
        raise BackupRefused('trusted_original_inventory_required')
    return backup(source, destination, lease_directory, authority_id, key,
                  stopped_writers_confirmed=stopped_writers_confirmed,
                  independent_destination_confirmed=independent_destination_confirmed,
                  expected_source_inventory=expected_inventory)


def read_inventory(path):
    """Read a bounded private regular file without following its final link."""
    path = Path(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) & 0o077
                or before.st_size > MAX_INVENTORY_BYTES):
            raise BackupRefused('inventory_file_invalid')
        data = bytearray()
        while len(data) <= MAX_INVENTORY_BYTES:
            chunk = os.read(fd, min(1048576, MAX_INVENTORY_BYTES + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
        after = os.fstat(fd)
        fields = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_uid, s.st_nlink,
                            s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        if len(data) != before.st_size or fields(before) != fields(after):
            raise BackupRefused('inventory_file_changed')
        value = json.loads(data)
        return value['inventory']
    finally:
        os.close(fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'destination', 'lease-directory', 'authority-id', 'inventory'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--stopped-writers-confirmed', action='store_true', required=True)
    parser.add_argument('--independent-destination-confirmed', action='store_true', required=True)
    parser.add_argument('--original-inventory-trusted', action='store_true', required=True)
    args = parser.parse_args(argv)
    try:
        inventory = read_inventory(args.inventory)
        key = bytes.fromhex(sys.stdin.readline().strip())
        restore(args.source, args.destination, args.lease_directory, args.authority_id, key, inventory,
                stopped_writers_confirmed=args.stopped_writers_confirmed,
                independent_destination_confirmed=args.independent_destination_confirmed,
                original_inventory_trusted=args.original_inventory_trusted)
        print(json.dumps({'status': 'verified', 'scope': 'offline restore; writer exclusion required'}))
        return 0
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error, BackupRefused) as error:
        print(json.dumps({'status': 'refused', 'reason': type(error).__name__}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
