"""Disabled durable object-transfer checkpoints for the reference worker.

The caller owns the SQLite transaction, its process fence, filesystem checks and
scratch ledger. These records do not prove file integrity or ledger accounting.
"""
import hashlib
import json
import sqlite3

from .reference_protocol import canonical_json

QUALIFIED = False
NORMALIZED_QUALIFIED = False

class TransferJournalHeld(ValueError):
    pass


def install(db, *, enabled=False):
    if QUALIFIED is not True or enabled is not True:
        raise TransferJournalHeld('transfer journal is not qualified')
    db.execute('''CREATE TABLE transfer_journal_binding (
        job TEXT PRIMARY KEY, binding TEXT NOT NULL, verified_bytes INTEGER NOT NULL DEFAULT 0)''')
    db.execute('''CREATE TABLE transfer_journal_object (
        job TEXT NOT NULL, object_id TEXT NOT NULL, identity TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending','verified','charged')),
        PRIMARY KEY(job,object_id), FOREIGN KEY(job) REFERENCES transfer_journal_binding(job))''')
    db.execute('''CREATE TABLE transfer_journal_private (
        job TEXT PRIMARY KEY, payload TEXT NOT NULL, digest TEXT NOT NULL)''')
    db.execute('''CREATE TABLE transfer_fetch_pin_intent (
        job TEXT PRIMARY KEY, identity TEXT NOT NULL)''')
    db.execute('''CREATE TABLE transfer_fetch_batches (
        job TEXT NOT NULL, cursor INTEGER NOT NULL, identity TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('started','verified','charged')),
        PRIMARY KEY(job,cursor))''')
    db.execute('''CREATE TABLE transfer_fetch_batch_items (
        job TEXT NOT NULL, cursor INTEGER NOT NULL, object_id TEXT NOT NULL, identity TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('started','verified','charged')),
        PRIMARY KEY(job,cursor,object_id))''')
    db.execute("CREATE TRIGGER transfer_journal_private_no_update BEFORE UPDATE ON transfer_journal_private BEGIN SELECT RAISE(ABORT,'immutable private transfer input'); END")
    db.execute("CREATE TRIGGER transfer_journal_private_no_delete BEFORE DELETE ON transfer_journal_private BEGIN SELECT RAISE(ABORT,'immutable private transfer input'); END")


def pin_intent(db, *, job, generation, manifest_sha, reservation, directory_device, directory_inode):
    if QUALIFIED is not True:raise TransferJournalHeld('transfer journal is not qualified')
    identity=canonical_json(dict(job=job,generation=generation,manifestSha256=manifest_sha,
        reservation=reservation,directoryDevice=directory_device,directoryInode=directory_inode)).decode()
    db.execute('INSERT OR IGNORE INTO transfer_fetch_pin_intent VALUES(?,?)',(job,identity))
    row=db.execute('SELECT identity FROM transfer_fetch_pin_intent WHERE job=?',(job,)).fetchone()
    if row is None or row[0]!=identity:raise TransferJournalHeld('fetch pin intent changed')


def save_private(db, *, job, claim, grant):
    """Store one canonical input; caller atomically saves its cursor in same DB."""
    if QUALIFIED is not True: raise TransferJournalHeld('transfer journal is not qualified')
    payload=canonical_json(dict(claim=claim,grant=grant)).decode()
    digest=hashlib.sha256(payload.encode()).hexdigest()
    db.execute('INSERT OR IGNORE INTO transfer_journal_private VALUES(?,?,?)',(job,payload,digest))
    row=db.execute('SELECT payload,digest FROM transfer_journal_private WHERE job=?',(job,)).fetchone()
    if row is None or tuple(row)!=(payload,digest): raise TransferJournalHeld('private transfer input changed')
    return digest


def load_private(db, *, job, expected_digest):
    if QUALIFIED is not True: raise TransferJournalHeld('transfer journal is not qualified')
    row=db.execute('SELECT payload,digest FROM transfer_journal_private WHERE job=?',(job,)).fetchone()
    if (row is None or row[1]!=expected_digest or hashlib.sha256(row[0].encode()).hexdigest()!=expected_digest):
        raise TransferJournalHeld('private transfer input changed')
    try:
        value=json.loads(row[0])
        if type(value) is not dict or set(value)!={'claim','grant'} or canonical_json(value).decode()!=row[0]:
            raise ValueError()
        return value['claim'],value['grant']
    except (ValueError,TypeError):raise TransferJournalHeld('private transfer input invalid') from None


def bind(db, *, job, generation, manifest_sha, reservation, directory_device, directory_inode):
    if QUALIFIED is not True:
        raise TransferJournalHeld('transfer journal is not qualified')
    if (type(job) is not str or not job or type(generation) is not int or generation<1
            or type(manifest_sha) is not str or len(manifest_sha)!=64
            or any(c not in '0123456789abcdef' for c in manifest_sha)
            or type(reservation) is not str or not reservation
            or type(directory_device) is not int or type(directory_inode) is not int
            or directory_device<0 or directory_inode<0):
        raise TransferJournalHeld('invalid transfer journal binding')
    value=canonical_json(dict(job=job,generation=generation,manifestSha256=manifest_sha,
        reservation=reservation,directoryDevice=directory_device,directoryInode=directory_inode)).decode()
    db.execute('INSERT OR IGNORE INTO transfer_journal_binding(job,binding) VALUES(?,?)',(job,value))
    row=db.execute('SELECT binding FROM transfer_journal_binding WHERE job=?',(job,)).fetchone()
    if row is None or row[0]!=value:raise TransferJournalHeld('transfer journal binding changed')


def pending(db, *, job, object_id, size, sha256, path):
    if QUALIFIED is not True:raise TransferJournalHeld('transfer journal is not qualified')
    if (type(object_id) is not str or not object_id or type(size) is not int or size<0
            or type(sha256) is not str or len(sha256)!=64 or any(c not in '0123456789abcdef' for c in sha256)
            or type(path) is not str or not path):raise TransferJournalHeld('invalid transfer object')
    identity=canonical_json(dict(objectId=object_id,size=size,sha256=sha256,path=path)).decode()
    if db.execute('SELECT 1 FROM transfer_journal_binding WHERE job=?',(job,)).fetchone() is None:
        raise TransferJournalHeld('transfer journal binding unavailable')
    db.execute("INSERT OR IGNORE INTO transfer_journal_object VALUES(?,?,?,'pending')",(job,object_id,identity))
    row=db.execute('SELECT identity,state FROM transfer_journal_object WHERE job=? AND object_id=?',(job,object_id)).fetchone()
    if row is None or row[0]!=identity:raise TransferJournalHeld('transfer object identity changed')
    return row[1]


def verified(db, *, job, object_id, identity):
    """Caller must first revalidate full file bytes, inode binding and durability."""
    if QUALIFIED is not True:raise TransferJournalHeld('transfer journal is not qualified')
    row=db.execute('SELECT identity,state FROM transfer_journal_object WHERE job=? AND object_id=?',(job,object_id)).fetchone()
    if row is None or row[0]!=identity:raise TransferJournalHeld('transfer object identity changed')
    if row[1]=='pending':
        size=json.loads(identity)['size']
        db.execute("UPDATE transfer_journal_object SET state='verified' WHERE job=? AND object_id=? AND state='pending'",(job,object_id))
        db.execute('UPDATE transfer_journal_binding SET verified_bytes=verified_bytes+? WHERE job=?',(size,job))
    elif row[1] not in ('verified','charged'):raise TransferJournalHeld('transfer object state changed')
    return db.execute('SELECT verified_bytes FROM transfer_journal_binding WHERE job=?',(job,)).fetchone()[0]


def charged(db, *, job, object_id, identity, observed_allocated):
    """Caller proves its separate ledger durably records the exact absolute total."""
    if QUALIFIED is not True:raise TransferJournalHeld('transfer journal is not qualified')
    row=db.execute('SELECT identity,state FROM transfer_journal_object WHERE job=? AND object_id=?',(job,object_id)).fetchone()
    total=db.execute('SELECT verified_bytes FROM transfer_journal_binding WHERE job=?',(job,)).fetchone()
    if (row is None or row[0]!=identity or row[1] not in ('verified','charged') or total is None
            or type(observed_allocated) is not int or observed_allocated!=total[0]):
        raise TransferJournalHeld('transfer ledger reconciliation required')
    db.execute("UPDATE transfer_journal_object SET state='charged' WHERE job=? AND object_id=?",(job,object_id))
