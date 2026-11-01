"""Disabled fetch-specific capacity pin; no timeout releases a pin.

This is a cooperative empty-ledger prototype. It does not fence old binaries,
restored database copies, host administrators, or external filesystem writers.
"""
import json
import os
from .scratch_policy import PolicyConflict, _id, _sha, _integer, _json

QUALIFIED=False
RELEASE_QUALIFIED=False
DDL=('CREATE TABLE reference_fetch_pins (reservation TEXT PRIMARY KEY, identity TEXT NOT NULL, '
     "state TEXT NOT NULL CHECK(state IN ('active','released')))")


def attest(db):
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_fetch_pins'").fetchone()
    marker=db.execute("SELECT value FROM metadata WHERE key='reference_fetch_pin_required'").fetchone()
    if row is None or row[0]!=DDL or marker is None or marker[0]!='1':raise PolicyConflict('fetch pin schema changed')


def present(db):
    table=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_fetch_pins'").fetchone()
    marker=db.execute("SELECT value FROM metadata WHERE key='reference_fetch_pin_required'").fetchone()
    if bool(table)!=bool(marker):raise PolicyConflict('fetch pin schema changed')
    if table:
        if QUALIFIED is not True:raise PolicyConflict('fetch pin support cannot be silently disabled')
        attest(db)
    return bool(table)


def install(ledger, owner, *, enabled=False):
    from ops.reference_worker_daemon import DaemonStateLock
    if QUALIFIED is not True or enabled is not True or type(owner) is not DaemonStateLock:
        raise PolicyConflict('fetch pin is not qualified')
    owner.check(ledger.path.absolute().parent)
    with ledger._transaction() as db:
        if db.execute('SELECT 1 FROM reservations LIMIT 1').fetchone():raise PolicyConflict('fetch pin requires empty ledger')
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='reference_fetch_pins'").fetchone():raise PolicyConflict('fetch pin already exists')
        db.execute(DDL)
        db.execute("INSERT INTO metadata VALUES('reference_fetch_pin_required','1')")


def acquire(ledger, owner, *, reservation, job, generation, environment, manifest_sha256,
            directory_device, directory_inode):
    """Pin a reservation for its whole fetch lifetime; batch intents are separate."""
    from ops.reference_worker_daemon import DaemonStateLock
    import stat
    if QUALIFIED is not True or type(owner) is not DaemonStateLock or os.name!='posix':
        raise PolicyConflict('fetch pin is not qualified')
    owner.check(ledger.path.absolute().parent)
    _id(reservation);_id(job);_integer(generation,'generation',1);_sha(manifest_sha256)
    _integer(directory_device,'directory device');_integer(directory_inode,'directory inode')
    if environment not in ('production','staging'):raise PolicyConflict('fetch pin environment invalid')
    ledger._owned_path(reservation,'jobs/'+reservation+'/placeholder')
    directory=ledger.root/'jobs'/reservation
    try:descriptor=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    except OSError:raise PolicyConflict('fetch pin directory unavailable') from None
    try:
        opened=os.fstat(descriptor);named=directory.lstat()
        if (not stat.S_ISDIR(opened.st_mode) or opened.st_uid!=os.geteuid() or stat.S_IMODE(opened.st_mode)&0o077
                or (opened.st_dev,opened.st_ino)!=(directory_device,directory_inode)
                or (named.st_dev,named.st_ino)!=(opened.st_dev,opened.st_ino)):
            raise PolicyConflict('fetch pin directory changed')
        with ledger._transaction() as db:
            attest(db);now=ledger._now(db);ledger._expire(db,now)
            row=ledger._reservation(db,reservation);settings=json.loads(row['settings'])
            old=db.execute('SELECT identity,state FROM reference_fetch_pins WHERE reservation=?',(reservation,)).fetchone()
            if ((row['job'],row['generation'],row['environment'],settings['manifest_sha256']) !=
                    (job,generation,environment,manifest_sha256) or row['state']!='reserved'
                    or (old is None and row['expires']<=now)):
                raise PolicyConflict('fetch pin reservation changed')
            input_bytes=_integer(settings['input_bytes'],'input bytes')
            output_budget=_integer(settings['output_budget'],'output budget')
            if row['allocated']+row['remaining']!=input_bytes+output_budget:
                raise PolicyConflict('fetch pin reservation budget changed')
            identity=_json(dict(reservation=reservation,job=job,generation=generation,environment=environment,
                manifestSha256=manifest_sha256,directoryDevice=directory_device,directoryInode=directory_inode,
                inputBytes=input_bytes,outputBudget=output_budget))
            current=directory.lstat()
            if (current.st_dev,current.st_ino)!=(opened.st_dev,opened.st_ino):raise PolicyConflict('fetch pin directory changed')
            if old is None:db.execute("INSERT INTO reference_fetch_pins VALUES(?,?,'active')",(reservation,identity))
            elif tuple(old)!=(identity,'active'):raise PolicyConflict('fetch pin identity changed')
        return identity
    finally:os.close(descriptor)


def release_after_custody(ledger, owner, driver, *, reservation):
    """Reconcile exact charged inputs, terminal ledger and accepted outputs.

    Holds the owned driver transaction while checking each file and the ledger.
    This is cooperative; administrators, old binaries and database restore are
    outside its fence. Missing files or any uncertainty keep the pin active.
    """
    from ops.reference_worker_daemon import DaemonStateLock
    from .reference_driver import ReferenceDriver
    from .reference_protocol import manifest_sha256, canonical_json
    if (QUALIFIED is not True or RELEASE_QUALIFIED is not True or type(owner) is not DaemonStateLock
            or type(driver) is not ReferenceDriver or driver._state_owner is not owner
            or driver.ledger is not ledger or driver._normalized_transfer is not True):
        raise PolicyConflict('fetch pin release is not qualified')
    owner.check(ledger.path.absolute().parent);_id(reservation)
    with driver._db() as driver_db:
        with ledger._transaction() as db:
            attest(db)
            pin=db.execute('SELECT identity,state FROM reference_fetch_pins WHERE reservation=?',(reservation,)).fetchone()
            if pin is None or pin['state']!='active':raise PolicyConflict('active fetch pin required')
            identity=json.loads(pin['identity'])
            if _json(identity)!=pin['identity'] or identity['reservation']!=reservation:raise PolicyConflict('fetch pin identity changed')
            job=identity['job']
        record=driver_db.execute('SELECT stage,data FROM driver_jobs WHERE job=?',(job,)).fetchone()
        if record is None or record['stage'] not in ('awaiting_acceptance','completed'):
            raise PolicyConflict('fetch pin result custody incomplete')
        data=json.loads(record['data']);claim=data['claim'];manifest=claim['submission']['manifest']
        if ((claim['jobId'],claim['generation'],claim['environment'],claim['manifestSha256'],data['reservation']['reservationId']) !=
                (job,identity['generation'],identity['environment'],identity['manifestSha256'],reservation)
                or manifest_sha256(manifest)!=identity['manifestSha256'] or manifest['sha256']!=identity['manifestSha256']
                or manifest['totalBytes']!=identity['inputBytes']):
            raise PolicyConflict('fetch pin claim changed')
        directory=ledger.ensure_job_directory(reservation);info=directory.stat()
        if (info.st_dev,info.st_ino)!=(identity['directoryDevice'],identity['directoryInode']):
            raise PolicyConflict('fetch pin directory changed')
        rows={row['object_id']:row for row in driver_db.execute('SELECT object_id,identity,state FROM transfer_journal_object WHERE job=?',(job,))}
        if len(rows)!=len(manifest['objects']):raise PolicyConflict('fetch pin input journal incomplete')
        total=0
        for obj in manifest['objects']:
            relative='jobs/'+reservation+'/'+obj['id']+'.input'
            expected=canonical_json(dict(objectId=obj['id'],size=obj['byteSize'],sha256=obj['sha256'],path=relative)).decode()
            row=rows.get(obj['id'])
            if row is None or row['identity']!=expected or row['state']!='charged':raise PolicyConflict('fetch pin input journal changed')
            driver.worker._file(reservation,relative,obj['byteSize'],obj['sha256'])
            total+=obj['byteSize']
        if total!=identity['inputBytes']:raise PolicyConflict('fetch pin input total changed')
        with ledger._transaction() as db:
            attest(db);row=ledger._reservation(db,reservation)
            current=db.execute('SELECT identity,state FROM reference_fetch_pins WHERE reservation=?',(reservation,)).fetchone()
            settings=json.loads(row['settings'])
            if (current is None or tuple(current)!=(pin['identity'],'active') or row['state']!='terminal' or row['remaining']!=0
                    or (row['job'],row['generation'],row['environment'],settings['manifest_sha256'],settings['input_bytes'],settings['output_budget']) !=
                       (job,identity['generation'],identity['environment'],identity['manifestSha256'],identity['inputBytes'],identity['outputBudget'])
                    or row['allocated']<identity['inputBytes'] or db.execute('SELECT 1 FROM results WHERE reservation=?',(reservation,)).fetchone() is None
                    or db.execute('SELECT 1 FROM objects WHERE reservation=?',(reservation,)).fetchone() is None
                    or db.execute('SELECT 1 FROM objects WHERE reservation=? AND accepted IS NULL',(reservation,)).fetchone() is not None):
                raise PolicyConflict('fetch pin custody reconciliation required')
            db.execute("UPDATE reference_fetch_pins SET state='released' WHERE reservation=? AND state='active'",(reservation,))
