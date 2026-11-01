"""Disabled offline v2 response/owned-file candidate; no HTTP or auth.

Header names and status policy are tentative. Never treat this parser or a
verified local file as server authorization, custody or safe retry authority.
"""
import hashlib
import os
import re
import stat
import math
import time
from pathlib import Path
from .reference_transfer_v2_client_journal_candidate import V2ClientJournal, JournalHeld, _encode
from . import reference_transfer_v2_contract_candidate as contract

OFFLINE_SYNTHETIC_QUALIFIED=False
MAX_BODY=16*1024*1024
MAX_CHUNK=1024*1024
MAX_CHUNKS=16384


def _clock(clock):
    value=clock()
    if type(value) not in (int,float) or not math.isfinite(value):raise JournalHeld('invalid v2 offline clock')
    return value


def parse_tentative_response(status,headers,*,request_hash,delivery_id,expected_size):
    """Validate candidate whole/empty response metadata, no authority claim."""
    if OFFLINE_SYNTHETIC_QUALIFIED is not True:raise JournalHeld('offline v2 response is not qualified')
    if (type(status) is not int or status!=200 or type(headers) not in (list,tuple) or len(headers)>64
            or type(expected_size) is not int or not 0<=expected_size<=MAX_BODY):
        raise JournalHeld('v2 response metadata changed')
    found={}
    for pair in headers:
        if (type(pair) not in (tuple,list) or len(pair)!=2 or type(pair[0]) is not str or type(pair[1]) is not str
                or not pair[0].isascii() or not pair[1].isascii() or len(pair[0])>128 or len(pair[1])>8192
                or any(c in pair[0]+pair[1] for c in '\r\n\0')):raise JournalHeld('v2 response metadata changed')
        key=pair[0].lower()
        if key in found:raise JournalHeld('v2 duplicate response header')
        found[key]=pair[1]
    expected={'lt-reference-version':'2','lt-request-sha256':request_hash,'lt-delivery-id':delivery_id,
              'lt-delivery-admitted':'true','content-length':str(expected_size)}
    if (any(found.get(k)!=v for k,v in expected.items()) or found.get('content-encoding','identity').lower()!='identity'
            or 'content-range' in found or 'transfer-encoding' in found
            or 'no-store' not in {part.strip().lower() for part in found.get('cache-control','').split(',')}):
        raise JournalHeld('v2 response metadata changed')
    return dict(status=status,requestHash=request_hash,deliveryId=delivery_id,contentLength=expected_size)


def receive_offline(journal,*,request_id,delivery_id,status,headers,chunks,timeout_seconds=10,clock=time.monotonic):
    """Stage one test-only response; partial files and ambiguous intent are retained."""
    if OFFLINE_SYNTHETIC_QUALIFIED is not True or type(journal) is not V2ClientJournal:
        raise JournalHeld('offline v2 response is not qualified')
    if (type(timeout_seconds) not in (int,float) or not math.isfinite(timeout_seconds)
            or not 0<timeout_seconds<=10):raise JournalHeld('invalid v2 offline deadline')
    deadline=_clock(clock)+timeout_seconds
    with journal._tx() as db:
        journal._attest(db)
        row=db.execute('''SELECT d.request_id,d.state,d.admission,r.request_hash FROM v2_deliveries d
            JOIN v2_requests r ON r.request_id=d.request_id WHERE d.delivery_id=?''',(delivery_id,)).fetchone()
        if row is None or row['request_id']!=request_id or row['state']!='dispatch_started':
            raise JournalHeld('v2 local delivery is not started')
        if db.execute('SELECT 1 FROM v2_adoptions WHERE request_id=? LIMIT 1',(request_id,)).fetchone():
            raise JournalHeld('v2 original authority has been superseded')
        admission,digest=contract.parse_admission(row['admission'].encode('ascii'))
        if digest!=row['request_hash'] or admission['deliveryId']!=delivery_id:raise JournalHeld('v2 request identity changed')
        binding=admission['binding'];meta=parse_tentative_response(status,headers,request_hash=digest,
            delivery_id=delivery_id,expected_size=binding['objectLength'])
        name='v2-input-'+delivery_id+'.part';identity=_encode(dict(requestId=request_id,deliveryId=delivery_id,
            requestHash=digest,size=binding['objectLength'],sha256=binding['objectSha256'],response=meta))
        try:db.execute("INSERT INTO v2_local_streams VALUES(?,?,?,'started')",(delivery_id,identity,name))
        except Exception:raise JournalHeld('v2 local stream requires reconciliation') from None
    with journal._tx() as db:
        journal._attest(db)
        if db.execute('SELECT 1 FROM v2_adoptions WHERE request_id=? LIMIT 1',(request_id,)).fetchone():
            raise JournalHeld('v2 original authority has been superseded')
        row=db.execute('SELECT identity,state FROM v2_local_streams WHERE delivery_id=?',(delivery_id,)).fetchone()
        if row is None or (row['identity'],row['state'])!=(identity,'started'):
            raise JournalHeld('v2 local stream changed')
    root=journal.owner.root;target=root/name
    journal._check()
    if _clock(clock)>=deadline:raise JournalHeld('v2 offline deadline exceeded')
    try:fd=os.open(target,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
    except OSError:raise JournalHeld('v2 local stream target unavailable') from None
    try:
        first=os.fstat(fd);directory=root.stat();digest_state=hashlib.sha256();size=0;chunk_count=0
        for chunk in chunks:
            if _clock(clock)>=deadline:raise JournalHeld('v2 offline deadline exceeded')
            chunk_count+=1
            if type(chunk) is not bytes or not 0<len(chunk)<=MAX_CHUNK or chunk_count>MAX_CHUNKS:
                raise JournalHeld('v2 response chunk invalid')
            size+=len(chunk)
            if size>binding['objectLength']:raise JournalHeld('v2 response exceeds declared length')
            digest_state.update(chunk);view=memoryview(chunk)
            while view:
                if _clock(clock)>=deadline:raise JournalHeld('v2 offline deadline exceeded')
                written=os.write(fd,view)
                if written<=0:raise JournalHeld('v2 local write failed')
                view=view[written:]
        if size!=binding['objectLength'] or digest_state.hexdigest()!=binding['objectSha256']:
            raise JournalHeld('v2 response incomplete or changed')
        if _clock(clock)>=deadline:raise JournalHeld('v2 offline deadline exceeded')
        os.fsync(fd);current=os.fstat(fd);named=target.lstat();current_dir=root.stat()
        if (not stat.S_ISREG(current.st_mode) or current.st_nlink!=1 or current.st_uid!=os.geteuid()
                or (first.st_dev,first.st_ino)!=(current.st_dev,current.st_ino)
                or (named.st_dev,named.st_ino)!=(current.st_dev,current.st_ino)
                or (directory.st_dev,directory.st_ino)!=(current_dir.st_dev,current_dir.st_ino)
                or current.st_size!=size):raise JournalHeld('v2 local file identity changed')
        dir_fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            if (os.fstat(dir_fd).st_dev,os.fstat(dir_fd).st_ino)!=(directory.st_dev,directory.st_ino):
                raise JournalHeld('v2 local directory changed')
            os.fsync(dir_fd)
        finally:os.close(dir_fd)
        with journal._tx() as db:
            journal._attest(db)
            if _clock(clock)>=deadline:raise JournalHeld('v2 offline deadline exceeded')
            if db.execute('SELECT 1 FROM v2_adoptions WHERE request_id=? LIMIT 1',(request_id,)).fetchone():
                raise JournalHeld('v2 original authority has been superseded')
            if db.execute("UPDATE v2_local_streams SET state='verified' WHERE delivery_id=? AND identity=? AND state='started'",(delivery_id,identity)).rowcount!=1:
                raise JournalHeld('v2 local stream changed')
        return target
    finally:os.close(fd)
