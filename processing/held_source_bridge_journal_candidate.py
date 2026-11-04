"""Dedicated held-only v2 journal. It never authorizes transfer or cleanup."""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from cryptography.hazmat.primitives import serialization
from . import reference_protocol as p
from .held_source_bridge_v2_candidate import verify
from .paged_frame_receiver_candidate import _durability

JOURNAL = 'CREATE TABLE held_source_bridge_v2 (issuer TEXT NOT NULL, snapshot TEXT NOT NULL, statement BLOB NOT NULL, proof_digest TEXT NOT NULL, PRIMARY KEY(issuer,snapshot))'
BLOBS = 'CREATE TABLE held_source_bridge_blobs_v2 (issuer TEXT NOT NULL, snapshot TEXT NOT NULL, kind TEXT NOT NULL, wire BLOB NOT NULL, PRIMARY KEY(issuer,snapshot,kind))'
MARKER = 'CREATE TABLE held_source_bridge_marker_v2 (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL CHECK(version=2))'


def _schema(db):
    for name, expected in (('held_source_bridge_v2', JOURNAL), ('held_source_bridge_blobs_v2', BLOBS),
                           ('held_source_bridge_marker_v2', MARKER)):
        row = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
        if row is None or row[0] != expected:
            raise ValueError()
    if [tuple(row) for row in db.execute('SELECT singleton,version FROM held_source_bridge_marker_v2 LIMIT 2')] != [(1, 2)]:
        raise ValueError()


def initialize(db):
    """Explicit initialization for a dedicated synthetic journal only."""
    if db.in_transaction:
        raise ValueError('held_bridge_journal_unavailable')
    try:
        _durability(db); db.execute('BEGIN IMMEDIATE')
        names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE name IN "
                    "('held_source_bridge_v2','held_source_bridge_blobs_v2','held_source_bridge_marker_v2')")}
        if not names:
            for ddl in (JOURNAL, BLOBS, MARKER): db.execute(ddl)
            db.execute('INSERT INTO held_source_bridge_marker_v2 VALUES(1,2)')
        elif len(names) != 3:
            raise ValueError()
        _schema(db); db.commit()
    except Exception:
        db.rollback(); raise ValueError('held_bridge_journal_unavailable') from None


def _stage(db, *, arguments, max_journal_bytes=128*1024*1024, max_snapshots=128, _fault=None, _owned=False):
    """Verify and atomically retain exact evidence; repeat only exact replays.

    `arguments` is supplied by a trusted caller, not a request decoder. This
    candidate does not bind to a coordinator job or confer any authorization.
    """
    if (db.in_transaction != _owned or type(arguments) is not dict or type(max_journal_bytes) is not int
            or not 1 <= max_journal_bytes <= 1024*1024*1024 or type(max_snapshots) is not int
            or not 1 <= max_snapshots <= 10000):
        raise ValueError('held_bridge_stage_unavailable')
    try:
        # Detach mutable structures so a caller cannot alter a verified proof
        # between verification and journal insertion.
        args = dict(arguments)
        for name in ('context', 'processing_closure', 'processing_pages'):
            args[name] = json.loads(p.canonical_json(args[name]).decode())
        for name, maximum in (('statement_bytes',65536), ('intent_bytes',65536), ('local_signature',64),
                              ('source_frames',12*1024*1024), ('selected_frames',12*1024*1024)):
            if type(args[name]) is not bytes or not 0 < len(args[name]) <= maximum:
                raise ValueError()
        for name in ('evidence_frames', 'file_frames'):
            if args[name] is not None and (type(args[name]) is not bytes or not 0 < len(args[name]) <= 24*1024*1024):
                raise ValueError()
        statement = json.loads(args['statement_bytes'].decode())
        key_bytes = lambda key: key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        proof = dict(context=args['context'], outerKeySha256=hashlib.sha256(key_bytes(args['outer_key'])).hexdigest(),
                     localKeySha256=hashlib.sha256(key_bytes(args['local_key'])).hexdigest(),
                     processingClosure=args['processing_closure'], processingPages=args['processing_pages'])
        proof_wire = p.canonical_json(proof)
        if len(proof_wire) > 12 * 1024 * 1024:
            raise ValueError()
        payloads = {'proof': proof_wire, 'intent': args['intent_bytes'], 'signature': args['local_signature'],
                    'source': args['source_frames'], 'selected': args['selected_frames']}
        for kind, name in (('evidence','evidence_frames'), ('file','file_frames')):
            if args[name] is not None: payloads[kind] = args[name]
        proof_hash = hashlib.sha256(proof_wire).hexdigest()
        issuer, snapshot = statement['issuerId'], statement['snapshotId']
        _durability(db)
        if not _owned: db.execute('BEGIN IMMEDIATE')
        _schema(db)
        old = db.execute('SELECT statement,proof_digest FROM held_source_bridge_v2 WHERE issuer=? AND snapshot=?',
                         (issuer, snapshot)).fetchone()
        if old:
            stored = {row[0]: bytes(row[1]) for row in db.execute(
                'SELECT kind,wire FROM held_source_bridge_blobs_v2 WHERE issuer=? AND snapshot=?', (issuer, snapshot))}
            if bytes(old[0]) != args['statement_bytes'] or old[1] != proof_hash or stored != payloads:
                raise ValueError()
            # Expiry prevents new staging, but exact previously committed
            # evidence remains inspectable after an outage. This path returns
            # held status only and re-verifies both signatures and closure.
            issued = datetime.strptime(statement['issuedAt'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
            verify(**{**args, 'now_ms': int(issued.timestamp() * 1000)})
            if not _owned: db.commit()
            return {'state':'held_bridge_journaled', 'created':False}
        verify(**args)
        # Bound this journal's logical bytes under the same write lock as the
        # insert. This is not a filesystem free-space or global storage quota.
        count, statements_bytes = db.execute(
            'SELECT COUNT(*),COALESCE(SUM(length(statement)),0) FROM held_source_bridge_v2').fetchone()
        blob_bytes = db.execute('SELECT COALESCE(SUM(length(wire)),0) FROM held_source_bridge_blobs_v2').fetchone()[0]
        if (count >= max_snapshots or statements_bytes + blob_bytes + len(args['statement_bytes'])
                + sum(len(wire) for wire in payloads.values()) > max_journal_bytes):
            raise ValueError()
        db.execute('INSERT INTO held_source_bridge_v2 VALUES(?,?,?,?)',
                   (issuer, snapshot, args['statement_bytes'], proof_hash))
        if _fault: _fault('statement')
        for kind, wire in payloads.items():
            db.execute('INSERT INTO held_source_bridge_blobs_v2 VALUES(?,?,?,?)', (issuer, snapshot, kind, wire))
            if _fault: _fault(kind)
        if not _owned: db.commit()
        return {'state':'held_bridge_journaled', 'created':True}
    except Exception:
        if not _owned and db.in_transaction: db.rollback()
        raise ValueError('held_bridge_stage_unavailable') from None


def stage(db, **kwargs):
    """Dedicated-transaction entry point for synthetic tests only."""
    return _stage(db, **kwargs)
