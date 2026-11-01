"""Disabled bounded staged inventory framing; authentication is caller-owned."""
import hashlib
import hmac
import json
import re
import secrets
import time

from .node_authority_session import SessionHeld, _write

PAGE_BYTES=64*1024
RECORD_BYTES=8192
STAGED_BYTES=4*1024*1024
MAX_RECORDS=10000
DOMAIN=b'ledgetop-reference-pages-v1\0'
READY_DOMAIN=b'ledgetop-reference-pages-ready-v1\0'


def _encode(value):
    try:return json.dumps(value,sort_keys=True,ensure_ascii=True,allow_nan=False,separators=(',',':')).encode('ascii')
    except (TypeError,ValueError,RecursionError):raise SessionHeld('invalid page encoding') from None


def _hash_item(digest,data):
    digest.update(len(data).to_bytes(8,'big'));digest.update(data)


def _read_page(sock,deadline):
    """Enforce wire-byte limit before allocating or decoding JSON."""
    def exact(count):
        chunks=[]
        while count:
            remaining=deadline-time.monotonic()
            if remaining<=0:raise SessionHeld('page deadline exceeded')
            sock.settimeout(remaining);part=sock.recv(count)
            if not part:raise SessionHeld('page stream interrupted')
            chunks.append(part);count-=len(part)
        return b''.join(chunks)
    size=int.from_bytes(exact(4),'big')
    if not 0<size<=PAGE_BYTES:raise SessionHeld('page wire size exceeded')
    def unique(pairs):
        result={}
        for key,value in pairs:
            if key in result:raise ValueError()
            result[key]=value
        return result
    try:return json.loads(exact(size).decode('utf-8'),object_pairs_hook=unique,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError,UnicodeError,RecursionError):raise SessionHeld('invalid page wire JSON') from None


def send_pages(sock,context,records,*,count,deadline):
    """Send ordered records; caller pins claim and maintains durable intent."""
    if type(context) is not dict or type(count) is not int or not 0<count<=MAX_RECORDS:
        raise SessionHeld('invalid page context')
    context_bytes=_encode(context);context_hash=hashlib.sha256(context_bytes).hexdigest()
    digest=hashlib.sha256(DOMAIN);_hash_item(digest,context_bytes);_hash_item(digest,str(count).encode())
    sequence=0;seen=0;last=None;batch=[];batch_bytes=0
    def flush():
        frame={'type':'page','contextSha256':context_hash,'sequence':sequence,'records':batch}
        if len(_encode(frame))>PAGE_BYTES:raise SessionHeld('page size exceeded')
        _write(sock,frame,deadline)
    for record in records:
        if time.monotonic()>=deadline:raise SessionHeld('page deadline exceeded')
        if type(record) is not dict or type(record.get('objectId')) is not str or not record['objectId']:
            raise SessionHeld('invalid page record')
        identifier=record['objectId'];encoded=_encode(record)
        if (last is not None and identifier<=last) or len(encoded)>RECORD_BYTES or seen>=count:
            raise SessionHeld('page record order changed')
        # Reserve ample fixed envelope space; final encoding is checked again.
        if batch and batch_bytes+len(encoded)+1>PAGE_BYTES-1024:
            flush();sequence+=1;batch=[];batch_bytes=0
        # Detach from the producer's mutable object before retaining a page.
        batch.append(json.loads(encoded));batch_bytes+=len(encoded)+1;_hash_item(digest,encoded)
        seen+=1;last=identifier
    if seen!=count:raise SessionHeld('page count changed')
    if batch:flush();sequence+=1
    final=digest.hexdigest()
    _write(sock,{'type':'end','contextSha256':context_hash,'pages':sequence,'count':seen,'sha256':final},deadline)
    return final


def receive_pages(sock,context,*,count,deadline,require_eof=False):
    """Return immutable bytes after proof; duplex callers must fence trailing frames.

    For a one-way stream require_eof enforces exact termination. A duplex mTLS
    integration must implement and verify a protocol-specific final handshake
    before performing any engine mutation; this helper does not supply one.
    Python object overhead is additional to STAGED_BYTES.
    """
    if type(context) is not dict or type(count) is not int or not 0<count<=MAX_RECORDS:
        raise SessionHeld('invalid page context')
    context_bytes=_encode(context);context_hash=hashlib.sha256(context_bytes).hexdigest()
    digest=hashlib.sha256(DOMAIN);_hash_item(digest,context_bytes);_hash_item(digest,str(count).encode())
    staged=[];total=sequence=0;last=None
    while True:
        frame=_read_page(sock,deadline)
        if type(frame) is not dict or frame.get('contextSha256')!=context_hash:raise SessionHeld('page context changed')
        if frame.get('type')=='end':
            if (set(frame)!={'type','contextSha256','pages','count','sha256'} or type(frame['pages']) is not int
                    or frame['pages']!=sequence or type(frame['count']) is not int or frame['count']!=count
                    or len(staged)!=count or frame['sha256']!=digest.hexdigest()):raise SessionHeld('page completion changed')
            break
        if (set(frame)!={'type','contextSha256','sequence','records'} or frame.get('type')!='page'
                or type(frame['sequence']) is not int or frame['sequence']!=sequence
                or type(frame['records']) is not list or not frame['records']):raise SessionHeld('page sequence changed')
        for record in frame['records']:
            if time.monotonic()>=deadline:raise SessionHeld('page deadline exceeded')
            if type(record) is not dict or type(record.get('objectId')) is not str or not record['objectId']:
                raise SessionHeld('invalid page record')
            identifier=record['objectId'];encoded=_encode(record)
            if (last is not None and identifier<=last) or len(encoded)>RECORD_BYTES or len(staged)>=count:
                raise SessionHeld('page record changed')
            total+=len(encoded)
            if total>STAGED_BYTES:raise SessionHeld('staged page size exceeded')
            _hash_item(digest,encoded);staged.append(encoded);last=identifier
        sequence+=1
    if require_eof:
        remaining=deadline-time.monotonic()
        if remaining<=0:raise SessionHeld('page deadline exceeded')
        sock.settimeout(remaining)
        if sock.recv(1):raise SessionHeld('page stream has trailing bytes')
    return tuple(staged)


def _ready_signature(context_hash,stream_hash,challenge,secret):
    if type(secret) is not bytes or len(secret)<32:raise SessionHeld('page authentication unavailable')
    return hmac.new(secret,READY_DOMAIN+_encode(dict(contextSha256=context_hash,streamSha256=stream_hash,
                                                     challenge=challenge)),hashlib.sha256).hexdigest()


def send_duplex_pages(sock,context,records,*,count,deadline,secret):
    """Authenticated ready exchange; caller owns persistent operation fence."""
    stream_hash=send_pages(sock,context,records,count=count,deadline=deadline)
    context_hash=hashlib.sha256(_encode(context)).hexdigest()
    challenge=_read_page(sock,deadline)
    if (type(challenge) is not dict or set(challenge)!={'type','contextSha256','streamSha256','challenge'}
            or challenge['type']!='challenge' or challenge['contextSha256']!=context_hash
            or challenge['streamSha256']!=stream_hash or type(challenge['challenge']) is not str
            or not re.fullmatch('[a-f0-9]{64}',challenge['challenge'])):
        raise SessionHeld('page challenge changed')
    _write(sock,{'type':'ready','contextSha256':context_hash,'streamSha256':stream_hash,
        'challenge':challenge['challenge'],'signature':_ready_signature(context_hash,stream_hash,challenge['challenge'],secret)},deadline)


def receive_duplex_pages(sock,context,*,count,deadline,secret):
    """Return immutable bytes only after exact authenticated ready frame."""
    staged=receive_pages(sock,context,count=count,deadline=deadline)
    context_bytes=_encode(context);context_hash=hashlib.sha256(context_bytes).hexdigest()
    digest=hashlib.sha256(DOMAIN);_hash_item(digest,context_bytes);_hash_item(digest,str(count).encode())
    for item in staged:_hash_item(digest,item)
    stream_hash=digest.hexdigest();challenge=secrets.token_hex(32)
    _write(sock,{'type':'challenge','contextSha256':context_hash,'streamSha256':stream_hash,'challenge':challenge},deadline)
    ready=_read_page(sock,deadline)
    expected={'type':'ready','contextSha256':context_hash,'streamSha256':stream_hash,'challenge':challenge,
              'signature':_ready_signature(context_hash,stream_hash,challenge,secret)}
    if ready!=expected:raise SessionHeld('page ready proof changed')
    return staged
