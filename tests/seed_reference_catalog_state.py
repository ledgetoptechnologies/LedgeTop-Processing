"""Seed an exclusively empty LOCAL synthetic migration directory; no live I/O.

Uses real native HTTP/TLS fixture admission, not manually manufactured cache or
evidence rows. For the actual installed-image downgrade oracle only.
"""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from contextlib import closing

from tests import test_reference_catalog_cache as c
from processing.prototype import PROFILE

def seed(directory):
    target=Path(directory).absolute()
    if target.resolve()!=target or any(p.is_symlink() for p in (target,*target.parents)) or not target.is_dir():
        raise ValueError('owned empty synthetic target required')
    with os.scandir(target) as entries:
        if next(entries,None) is not None:raise ValueError('synthetic target must be empty')
    c.CatalogCacheTests.setUpClass();t=c.CatalogCacheTests()
    try:
        t.setUp();raw=t.raw()
        for index in range(12):
            t.queue.submit(dict(requestId='synthetic-historical-'+str(index),environment='staging',profile=PROFILE))
            claim=t.queue.claim(dict(workerId='synthetic-history-worker',nodeId='main'))
            t.queue.complete(claim['id'],dict(claimToken=claim['claimToken'],status='succeeded',result={'synthetic':True}))
        job=t.submit(raw)
        if job.status!=201:raise ValueError('actual metadata admission failed')
        t.runtime.close();t.queue.close()
        source=t.queue.path
        with closing(sqlite3.connect(source.as_uri()+'?mode=ro',uri=True)) as db:
            rows={row[0]:db.execute('SELECT * FROM "'+row[0]+'" ORDER BY rowid').fetchall()
                for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")}
        rawbytes=source.read_bytes();dest=target/'jobs.sqlite3'
        with dest.open('xb') as output:
            output.write(rawbytes);output.flush();os.fsync(output.fileno())
        descriptor=os.open(target,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
        try:os.fsync(descriptor)
        finally:os.close(descriptor)
        return dict(schemaVersion=1,synthetic=True,jobs=13,terminalJobs=12,queuedJobs=1,
            optionEvidenceRows=1,databaseSha256=hashlib.sha256(rawbytes).hexdigest(),
            logicalRowsSha256=hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
            authorityId=t.queue.authority_id)
    finally:
        t.doCleanups();c.CatalogCacheTests.tearDownClass()

if __name__=='__main__':
    import sys
    if len(sys.argv)!=2:raise SystemExit('one empty synthetic directory required')
    print(json.dumps(seed(sys.argv[1]),sort_keys=True))
