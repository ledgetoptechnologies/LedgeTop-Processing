"""Unmounted fixture-only receipt journal; not an authorization or cleanup path.

The public entry point is disabled. This experiment assumes an isolated,
exclusively owned SQLite file and trusted inputs; it supplies no writer fence,
transport authentication, durable storage guarantees, or production migration.
"""
import sqlite3
import tempfile
import re
from pathlib import Path
from .viewer_copy_v2_candidate import CandidateUnavailable, _verify_v2_for_fixtures


class JournalConflict(ValueError):
    pass


def journal_verified_copy(*_args, **_kwargs):
    raise CandidateUnavailable('unagreed and unmounted journal')


def _open_fixture(path, owner_token):
    path=Path(path)
    parent=path.parent
    if (path.name!='journal.sqlite' or not parent.name.startswith('ltds-copy-journal-')
        or parent.is_symlink() or parent.resolve().parent!=Path(tempfile.gettempdir()).resolve()
        or not parent.is_dir() or path.is_symlink()
        or type(owner_token) is not str or not re.fullmatch(r'[0-9a-f]{32}',owner_token)):
        raise ValueError('fixture database must be inside an owned temporary directory')
    marker=parent/'.fixture-owner'
    if marker.is_symlink() or not marker.is_file() or marker.read_text(encoding='ascii')!=owner_token:
        raise ValueError('fixture ownership marker mismatch')
    db=sqlite3.connect(path, timeout=2, isolation_level=None)
    try:
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('PRAGMA synchronous=FULL')
        db.executescript('''
      CREATE TABLE IF NOT EXISTS fixture_copies (
        submission TEXT PRIMARY KEY, authority TEXT NOT NULL, principal TEXT NOT NULL,
        environment TEXT NOT NULL, activation TEXT NOT NULL, result_digest TEXT NOT NULL,
        raw BLOB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS fixture_ids (
        receipt_id TEXT PRIMARY KEY, submission TEXT NOT NULL REFERENCES fixture_copies(submission)
      );
        ''')
    except BaseException:
        db.close()
        raise
    return db


def _journal_fixture(db, raw, *, expected, result_manifest, trusted_keys, before_commit=None):
    receipt=_verify_v2_for_fixtures(raw,expected=expected,result_manifest=result_manifest,trusted_keys=trusted_keys)
    record=(receipt['submissionId'],receipt['coordinatorAuthorityId'],receipt['viewerPrincipalId'],
        receipt['environment'],receipt['activationId'],receipt['resultManifestSha256'],raw)
    ids=[receipt[k] for k in ('receiptId','storageReceiptId','acceptanceReceiptId','acceptanceStorageReceiptId')]
    try:
        db.execute('BEGIN IMMEDIATE')
        previous=db.execute('SELECT submission,authority,principal,environment,activation,result_digest,raw FROM fixture_copies WHERE submission=?',(record[0],)).fetchone()
        if previous is not None:
            if previous!=record:raise JournalConflict('submission conflict')
            db.execute('COMMIT')
            return 'replay'
        db.execute('INSERT INTO fixture_copies VALUES (?,?,?,?,?,?,?)',record)
        db.executemany('INSERT INTO fixture_ids VALUES (?,?)',[(i,record[0]) for i in ids])
        if before_commit is not None:before_commit()
        db.execute('COMMIT')
        return 'stored'
    except sqlite3.IntegrityError as error:
        if db.in_transaction:db.execute('ROLLBACK')
        raise JournalConflict('receipt id conflict') from error
    except BaseException:
        if db.in_transaction:db.execute('ROLLBACK')
        raise
