"""Disabled conservative small-batch reference transfer candidate.

A started batch with any missing target holds for operator reconciliation. The
pin/transactions are cooperative and do not fence external writers or mount swaps.
"""
import hashlib
import json
import os
import stat
from .reference_transfer import _now
from .reference_protocol import canonical_json
from . import reference_transfer_journal_candidate as journal
from . import reference_fetch_pin_candidate as pin

QUALIFIED=False


def run(driver, *, job_id, claim, grant, reservation, directory, cursor, objects, observe_claim):
    from .reference_driver import DriverHeld
    if QUALIFIED is not True or not 1<=len(objects)<=4:
        raise DriverHeld('fetch_batch_not_qualified')
    identity=canonical_json(dict(jobId=job_id,generation=claim['generation'],environment=claim['environment'],
        manifestSha256=claim['manifestSha256'],reservation=reservation,cursor=cursor,
        claimTokenSha256=hashlib.sha256(claim['claimToken'].encode()).hexdigest(),
        objects=[dict(objectId=o['id'],size=o['byteSize'],sha256=o['sha256']) for o in objects])).decode()
    directory_stat=directory.stat()
    pin.acquire(driver.ledger,driver._state_owner,reservation=reservation,job=job_id,generation=claim['generation'],
        environment=claim['environment'],manifest_sha256=claim['manifestSha256'],
        directory_device=directory_stat.st_dev,directory_inode=directory_stat.st_ino)
    def path_and_identity(obj):
        relative='jobs/'+reservation+'/'+obj['id']+'.input'
        data=canonical_json(dict(objectId=obj['id'],size=obj['byteSize'],sha256=obj['sha256'],path=relative)).decode()
        return relative,data,driver.ledger.root/relative
    with driver._db() as db:
        row=db.execute('SELECT identity,state FROM transfer_fetch_batches WHERE job=? AND cursor=?',(job_id,cursor)).fetchone()
        fresh=row is None
        if fresh:
            db.execute("INSERT INTO transfer_fetch_batches VALUES(?,?,?,'started')",(job_id,cursor,identity))
            for obj in objects:
                _,item,_=path_and_identity(obj)
                db.execute("INSERT INTO transfer_fetch_batch_items VALUES(?,?,?,?, 'started')",(job_id,cursor,obj['id'],item))
        elif row['identity']!=identity or row['state'] not in ('started','verified','charged'):
            raise DriverHeld('fetch_batch_identity_changed')
        items=db.execute('SELECT object_id,identity,state FROM transfer_fetch_batch_items WHERE job=? AND cursor=? ORDER BY object_id',(job_id,cursor)).fetchall()
        expected_state = 'started' if fresh else row['state']
        if (len(items)!=len(objects) or any(r['state']!=expected_state for r in items)
                or {(r['object_id'],r['identity']) for r in items}!={(o['id'],path_and_identity(o)[1]) for o in objects}):
            raise DriverHeld('fetch_batch_items_changed')
    # Pre-existing targets are never adopted for a new batch. A resumed batch
    # may reconcile only if every target exists; missing requests remain held.
    present=[os.path.lexists(path_and_identity(obj)[2]) for obj in objects]
    if fresh and any(present):raise DriverHeld('fetch_batch_preexisting_target')
    if not fresh and not all(present):raise DriverHeld('fetch_batch_incomplete_requires_reconciliation')
    for obj in objects:
        relative,_,target=path_and_identity(obj)
        if fresh:
            observe_claim()
            current=directory.stat()
            if (current.st_dev,current.st_ino)!=(directory_stat.st_dev,directory_stat.st_ino):raise DriverHeld('transfer_directory_changed')
            pin.acquire(driver.ledger,driver._state_owner,reservation=reservation,job=job_id,generation=claim['generation'],
                environment=claim['environment'],manifest_sha256=claim['manifestSha256'],
                directory_device=directory_stat.st_dev,directory_inode=directory_stat.st_ino)
            if grant.expires<=_now(driver.clock):raise DriverHeld('transfer_grant_expired')
            driver.ledger._owned_path(reservation,relative)
            driver.broker.download_object(grant,obj['id'],target)
        driver.worker._file(reservation,relative,obj['byteSize'],obj['sha256'])
        fd=os.open(target,os.O_RDONLY|os.O_NOFOLLOW)
        try:
            before=os.fstat(fd);digest=hashlib.sha256()
            if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_uid!=os.geteuid():raise DriverHeld('transfer_file_changed')
            while chunk:=os.read(fd,1024*1024):digest.update(chunk)
            after=os.fstat(fd);named=target.lstat()
            if ((before.st_dev,before.st_ino,before.st_size)!=(after.st_dev,after.st_ino,obj['byteSize'])
                    or (named.st_dev,named.st_ino)!=(before.st_dev,before.st_ino) or digest.hexdigest()!=obj['sha256']):
                raise DriverHeld('transfer_file_changed')
            os.fsync(fd)
        finally:os.close(fd)
    fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        observed=os.fstat(fd)
        if (observed.st_dev,observed.st_ino)!=(directory_stat.st_dev,directory_stat.st_ino):raise DriverHeld('transfer_directory_changed')
        os.fsync(fd)
    finally:os.close(fd)
    with driver._db() as db:
        for obj in objects:
            relative,item,_=path_and_identity(obj)
            journal.pending(db,job=job_id,object_id=obj['id'],size=obj['byteSize'],sha256=obj['sha256'],path=relative)
            allocated=journal.verified(db,job=job_id,object_id=obj['id'],identity=item)
        db.execute("UPDATE transfer_fetch_batches SET state='verified' WHERE job=? AND cursor=? AND state='started'",(job_id,cursor))
        db.execute("UPDATE transfer_fetch_batch_items SET state='verified' WHERE job=? AND cursor=? AND state='started'",(job_id,cursor))
    driver.ledger.record_allocated(reservation,allocated)
    with driver.ledger._transaction() as ledger_db:
        ledger_row=driver.ledger._reservation(ledger_db,reservation)
        settings=json.loads(ledger_row['settings'])
        if ((ledger_row['job'],ledger_row['generation'],ledger_row['environment'],settings['manifest_sha256'],ledger_row['allocated']) !=
                (job_id,claim['generation'],claim['environment'],claim['manifestSha256'],allocated)):
            raise DriverHeld('transfer_ledger_changed')
        with driver._db() as db:
            current=db.execute('SELECT stage,data FROM driver_jobs WHERE job=?',(job_id,)).fetchone()
            saved=json.loads(current['data']) if current is not None else None
            if current is None or current['stage']!='fetching' or saved.get('inputCursor')!=cursor:
                raise DriverHeld('transfer_cursor_changed')
            for obj in objects:
                journal.charged(db,job=job_id,object_id=obj['id'],identity=path_and_identity(obj)[1],observed_allocated=ledger_row['allocated'])
            db.execute("UPDATE transfer_fetch_batches SET state='charged' WHERE job=? AND cursor=?",(job_id,cursor))
            db.execute("UPDATE transfer_fetch_batch_items SET state='charged' WHERE job=? AND cursor=?",(job_id,cursor))
            saved['inputCursor']=cursor+len(objects)
            db.execute('UPDATE driver_jobs SET data=? WHERE job=?',(canonical_json(saved).decode(),job_id))
    return {'state':'fetching','jobId':job_id}
