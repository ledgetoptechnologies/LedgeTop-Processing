"""Disabled pinned mTLS reference-init authority experiment.

One coordinator transaction spans one externally forwarded initialization.
Upload, commit, daemon wiring and recovery are deliberately unsupported.
"""
import base64
from contextlib import contextmanager
import hashlib
import hmac
import os
import re
import secrets
import sqlite3
import time

from .node_authority_session import SessionHeld, _peer, _read, _write
from .node_mutation_broker import (ReferenceCoordinatorAuthority, _REFERENCE_INIT_FIELDS,
                                   _REFERENCE_COMMIT_DDL, _REFERENCE_UPLOAD_ISSUE_DDL, _REFERENCE_RECEIPT_DDL,
                                   _json, _reply, _upload_reply, _reference_receipt_signature,
                                   _reference_verified_image_only_uploads,
                                   sign_grant, sign_reference_issue, sign_reference_commit)

REFERENCE_REMOTE_QUALIFIED = False


def serve_reference_init(sock, *, witness, secret, peer_sha256, timeout_seconds=10):
    """Serve one pinned peer and one init; disconnect retains durable intent."""
    try:
        if (REFERENCE_REMOTE_QUALIFIED is not True or type(witness) is not ReferenceCoordinatorAuthority
                or type(timeout_seconds) not in (int,float) or not 0 < timeout_seconds <= 10):
            raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,True)
        deadline = time.monotonic()+timeout_seconds
        request = _read(sock,deadline)
        if type(request) is not dict or set(request) != {'grant','signature','body','nonce'}:
            raise SessionHeld('invalid reference request')
        grant, signature, nonce = request['grant'],request['signature'],request['nonce']
        if (type(grant) is not dict or set(grant) != _REFERENCE_INIT_FIELDS or grant.get('operation') != 'init'
                or type(signature) is not str or type(nonce) is not str or len(nonce) != 64
                or any(c not in '0123456789abcdef' for c in nonce)
                or not hmac.compare_digest(sign_grant(grant,secret),signature)):
            raise SessionHeld('reference request authentication failed')
        try: body = base64.b64decode(request['body'],validate=True)
        except Exception: raise SessionHeld('invalid reference body') from None
        if len(body)>65536 or hashlib.sha256(body).hexdigest() != grant['bodySha256']:
            raise SessionHeld('reference request body changed')
        digest = hashlib.sha256(_json(grant)).hexdigest()
        with witness.hold(grant,max(0,deadline-time.monotonic()),create=True,_remote_fenced=True):
            if witness._intent_state != 'started':
                raise SessionHeld('completed reference init requires inspection')
            _write(sock,{'state':'held','nonce':nonce,'grantSha256':digest},deadline)
            finish = _read(sock,deadline)
            if type(finish) is not dict or set(finish) != {'nonce','reply'} or finish['nonce'] != nonce:
                raise SessionHeld('reference completion identity changed')
            reply = _reply(finish['reply'],grant['engineTaskId'])
            witness.completed(grant,reply)
        _write(sock,{'state':'complete','nonce':nonce,'grantSha256':digest,
                     'replySha256':hashlib.sha256(_json(reply)).hexdigest()},deadline)
    finally:
        sock.close()


class RemoteReferenceInitAuthority:
    """One authenticated init, no reconnect/replay or other operations."""
    def __init__(self,sock,*,authority,epoch,peer_sha256,timeout_seconds=10):
        if REFERENCE_REMOTE_QUALIFIED is not True or type(timeout_seconds) not in (int,float) or not 0 < timeout_seconds <= 10:
            raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,False)
        self.sock,self.authority,self.epoch,self.timeout = sock,authority,epoch,timeout_seconds
        self.pid,self.used,self.active = os.getpid(),False,None

    @contextmanager
    def hold(self,grant,timeout,*,body,signature,create=False):
        if (self.pid != os.getpid() or self.used or create is not True or type(body) is not bytes
                or type(timeout) not in (int,float) or not 0 < timeout <= 10):
            raise SessionHeld('reference session unavailable')
        self.used = True
        deadline = time.monotonic()+min(timeout,self.timeout)
        nonce = secrets.token_hex(32)
        digest = hashlib.sha256(_json(grant)).hexdigest()
        try:
            _write(self.sock,dict(grant=grant,signature=signature,body=base64.b64encode(body).decode(),nonce=nonce),deadline)
            if _read(self.sock,deadline) != {'state':'held','nonce':nonce,'grantSha256':digest}:
                raise SessionHeld('reference held proof changed')
            self.active = (grant,nonce,digest,deadline)
            yield
            if self.active is not None:
                raise SessionHeld('reference completion not acknowledged')
        finally:
            self.active = None
            self.sock.close()

    def completed(self,grant,reply):
        if self.active is None or grant != self.active[0]:
            raise SessionHeld('reference session not held')
        _,nonce,digest,deadline = self.active
        reply = _reply(reply,grant['engineTaskId'])
        _write(self.sock,{'nonce':nonce,'reply':reply},deadline)
        expected = {'state':'complete','nonce':nonce,'grantSha256':digest,'replySha256':hashlib.sha256(_json(reply)).hexdigest()}
        if _read(self.sock,deadline) != expected:
            raise SessionHeld('reference completion proof changed')
        self.active = None


def serve_reference_upload(sock, *, witness, secret, peer_sha256, timeout_seconds=10):
    """One signed reference upload under a persisted coordinator intent.

    The pinned node is responsible for local gate, durable operation journal,
    actual streamed digest and sender verification. This session alone does not
    prove any of those and is not a deployable authorization boundary.
    """
    try:
        if (REFERENCE_REMOTE_QUALIFIED is not True or type(witness) is not ReferenceCoordinatorAuthority
                or type(timeout_seconds) not in (int,float) or not 0 < timeout_seconds <= 10):
            raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,True)
        deadline=time.monotonic()+timeout_seconds
        request=_read(sock,deadline)
        if type(request) is not dict or set(request) != {'grant','signature','objectId','operationId','issueSignature','nonce'}:
            raise SessionHeld('invalid reference upload request')
        grant,signature=request['grant'],request['signature']
        object_id,operation_id,issue_signature,nonce=(request[k] for k in ('objectId','operationId','issueSignature','nonce'))
        if (type(grant) is not dict or set(grant) != _REFERENCE_INIT_FIELDS or grant.get('operation') != 'init'
                or type(signature) is not str or not hmac.compare_digest(sign_grant(grant,secret),signature)
                or type(issue_signature) is not str or not hmac.compare_digest(sign_reference_issue(grant,object_id,operation_id,secret),issue_signature)
                or type(nonce) is not str or len(nonce)!=64 or any(c not in '0123456789abcdef' for c in nonce)):
            raise SessionHeld('reference upload authentication failed')
        issue=witness._issue_upload_candidate(grant,signature,object_id=object_id,operation_id=operation_id,
                                               secret=secret,timeout=max(0,deadline-time.monotonic()),remote_fenced=True)
        allowed=witness._start_upload_candidate(grant,object_id=object_id,operation_id=operation_id,
                                                timeout=max(0,deadline-time.monotonic()),remote_fenced=True)
        with witness.hold(grant,max(0,deadline-time.monotonic()),_allowed_upload=allowed,_remote_fenced=True):
            init_hash=hashlib.sha256(_json(grant)).hexdigest()
            proof=witness._active_db.execute('SELECT grant_hash,response_sha256 FROM reference_broker_completion_candidate WHERE operation_id=?',(grant['operationId'],)).fetchone()
            if proof is None or proof['grant_hash'] != init_hash:
                raise SessionHeld('reference initialization proof changed')
            _write(sock,{'state':'held','nonce':nonce,'issue':issue,'intentSha256':allowed[2],'initReplySha256':proof['response_sha256']},deadline)
            finish=_read(sock,deadline)
            if type(finish) is not dict or set(finish) != {'nonce','reply','receipt'} or finish['nonce']!=nonce:
                raise SessionHeld('reference upload completion changed')
            _upload_reply(finish['reply'])
            receipt=finish['receipt']
            expected = dict(objectId=object_id,fileName=issue['fileName'],fileBytes=issue['fileBytes'],
                sha256=issue['fileSha256'],operationId=operation_id,state='complete',
                scopeSha256=hashlib.sha256(_json(grant)).hexdigest())
            if (type(receipt) is not dict or set(receipt)!=set(expected)|{'signature'}
                    or any(type(receipt[k]) is not type(v) or receipt[k]!=v for k,v in expected.items())
                    or type(receipt['signature']) is not str
                    or not hmac.compare_digest(_reference_receipt_signature(receipt,secret),receipt['signature'])):
                raise SessionHeld('reference upload receipt changed')
            db=witness._active_db
            db.execute('INSERT INTO reference_broker_upload_receipt_candidate VALUES(?,?,?)',
                       (operation_id,allowed[2],_json(receipt).decode()))
            if db.execute("UPDATE broker_intents SET state='complete' WHERE operation_id=? AND job_id=? AND grant_hash=? AND state='started'",allowed).rowcount != 1:
                raise SessionHeld('reference upload intent changed')
        _write(sock,{'state':'complete','nonce':nonce,'intentSha256':allowed[2],
                     'receiptSha256':hashlib.sha256(_json(receipt)).hexdigest()},deadline)
    finally:
        sock.close()


class RemoteReferenceUploadAuthority:
    """One upload authority session; local gate and sender remain caller-owned."""
    def __init__(self,sock,*,authority,epoch,peer_sha256,timeout_seconds=10):
        if REFERENCE_REMOTE_QUALIFIED is not True or type(timeout_seconds) not in (int,float) or not 0 < timeout_seconds <= 10:
            raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,False)
        self.sock,self.authority,self.epoch,self.timeout=sock,authority,epoch,timeout_seconds
        self.pid,self.used,self.active=os.getpid(),False,None

    @contextmanager
    def hold(self,grant,signature,*,object_id,operation_id,issue_signature,expected_issue):
        if self.pid != os.getpid() or self.used or type(expected_issue) is not dict:
            raise SessionHeld('reference upload session unavailable')
        self.used=True
        deadline=time.monotonic()+self.timeout
        nonce=secrets.token_hex(32)
        try:
            _write(self.sock,dict(grant=grant,signature=signature,objectId=object_id,operationId=operation_id,
                                  issueSignature=issue_signature,nonce=nonce),deadline)
            held=_read(self.sock,deadline)
            if (type(held) is not dict or set(held)!={'state','nonce','issue','intentSha256','initReplySha256'}
                    or held['state']!='held' or held['nonce']!=nonce or _json(held['issue'])!=_json(expected_issue)
                    or type(held['intentSha256']) is not str or len(held['intentSha256'])!=64
                    or any(c not in '0123456789abcdef' for c in held['intentSha256'])
                    or type(held['initReplySha256']) is not str or len(held['initReplySha256'])!=64
                    or any(c not in '0123456789abcdef' for c in held['initReplySha256'])):
                raise SessionHeld('reference upload held proof changed')
            self.active=(nonce,held['intentSha256'],deadline)
            yield held
            if self.active is not None:raise SessionHeld('reference upload completion not acknowledged')
        finally:
            self.active=None
            self.sock.close()

    def completed(self,reply,receipt):
        if self.active is None:raise SessionHeld('reference upload session not held')
        nonce,digest,deadline=self.active
        _upload_reply(reply)
        _write(self.sock,{'nonce':nonce,'reply':reply,'receipt':receipt},deadline)
        expected={'state':'complete','nonce':nonce,'intentSha256':digest,'receiptSha256':hashlib.sha256(_json(receipt)).hexdigest()}
        if _read(self.sock,deadline)!=expected:raise SessionHeld('reference upload completion proof changed')
        self.active=None


def _coordinator_commit_inventory(db,grant,secret):
    """Coordinator side only; node must separately verify its gate and journal."""
    for table,ddl in (('reference_broker_commit_candidate',_REFERENCE_COMMIT_DDL),
                      ('reference_broker_upload_issue_candidate',_REFERENCE_UPLOAD_ISSUE_DDL),
                      ('reference_broker_upload_receipt_candidate',_REFERENCE_RECEIPT_DDL)):
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()
        if row is None or row[0]!=ddl:raise SessionHeld('remote reference inventory schema changed')
    init_hash=hashlib.sha256(_json(grant)).hexdigest()
    init=db.execute('SELECT job_id,grant_hash,state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()
    proof=db.execute('SELECT grant_hash,response_sha256 FROM reference_broker_completion_candidate WHERE operation_id=?',(grant['operationId'],)).fetchone()
    if init is None or tuple(init)!=(grant['jobId'],init_hash,'complete') or proof is None or proof['grant_hash']!=init_hash:
        raise SessionHeld('remote reference init proof changed')
    submission=__import__('json').loads(db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?',(grant['jobId'],)).fetchone()[0])
    rows=db.execute('SELECT * FROM reference_broker_upload_issue_candidate WHERE job_id=? AND generation=?',
                    (grant['jobId'],grant['taskGeneration'])).fetchall()
    issues={};receipts=[];local_proofs=[]
    for issue in rows:
        if (issue['manifest_sha']!=grant['inputManifestSha256'] or issue['claim_sha']!=grant['claimTokenSha256']
                or issue['init_operation_id']!=grant['operationId'] or issue['object_id'] in issues):
            raise SessionHeld('remote reference issue changed')
        digest=hashlib.sha256(_json(list(issue))).hexdigest()
        intent=db.execute('SELECT job_id,grant_hash,state FROM broker_intents WHERE operation_id=?',(issue['operation_id'],)).fetchone()
        receipt=db.execute('SELECT issue_sha256,receipt_json FROM reference_broker_upload_receipt_candidate WHERE operation_id=?',(issue['operation_id'],)).fetchone()
        if (intent is None or tuple(intent)!=(grant['jobId'],digest,'complete') or receipt is None or receipt['issue_sha256']!=digest):
            raise SessionHeld('remote reference upload proof unavailable')
        issues[issue['object_id']]=issue['operation_id'];receipts.append(__import__('json').loads(receipt['receipt_json']))
        local_proofs.append(dict(operationId=issue['operation_id'],fileName=issue['file_name'],issueSha256=digest))
    _reference_verified_image_only_uploads(submission['manifest'],grant['inputManifestSha256'],receipts,
                                            init_grant=grant,issues=issues,secret=secret)
    return dict(initReplySha256=proof['response_sha256'],uploads=local_proofs)


def serve_reference_commit(sock, *, witness, secret, peer_sha256, timeout_seconds=10):
    """Coordinator half of one photo-only remote commit; no retry on loss."""
    try:
        if (REFERENCE_REMOTE_QUALIFIED is not True or type(witness) is not ReferenceCoordinatorAuthority
                or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=10):
            raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,True);deadline=time.monotonic()+timeout_seconds
        request=_read(sock,deadline)
        if type(request) is not dict or set(request)!={'grant','signature','operationId','body','contentType','commitSignature','nonce'}:
            raise SessionHeld('invalid remote commit request')
        grant,signature=request['grant'],request['signature']
        operation,content_type,commit_signature,nonce=(request[k] for k in ('operationId','contentType','commitSignature','nonce'))
        try:body=base64.b64decode(request['body'],validate=True)
        except Exception:raise SessionHeld('invalid remote commit body') from None
        from .node_mutation_broker import _uuid
        if (type(grant) is not dict or set(grant)!=_REFERENCE_INIT_FIELDS or grant.get('operation')!='init'
                or type(signature) is not str or not hmac.compare_digest(sign_grant(grant,secret),signature)
                or not _uuid(operation) or operation==grant['operationId'] or len(body)>65536
                or type(content_type) is not str or not re.fullmatch(r'[ -~]{1,256}',content_type)
                or type(commit_signature) is not str or not hmac.compare_digest(sign_reference_commit(grant,operation,body,content_type,secret),commit_signature)
                or type(nonce) is not str or not re.fullmatch('[a-f0-9]{64}',nonce)):
            raise SessionHeld('remote commit authentication failed')
        commit=(operation,grant['jobId'],hashlib.sha256(_json(grant)).hexdigest(),hashlib.sha256(body).hexdigest(),content_type)
        allowed=(operation,grant['jobId'],hashlib.sha256(_json(list(commit))).hexdigest())
        with witness.hold(grant,max(0,deadline-time.monotonic()),_remote_fenced=True):
            db=witness._active_db
            _coordinator_commit_inventory(db,grant,secret)
            try:
                db.execute('INSERT INTO reference_broker_commit_candidate VALUES(?,?,?,?,?)',commit)
                db.execute("INSERT INTO broker_intents VALUES(?,?,?,'started')",allowed)
            except sqlite3.IntegrityError:raise SessionHeld('remote commit requires inspection') from None
        with witness.hold(grant,max(0,deadline-time.monotonic()),_allowed_commit=allowed,_remote_fenced=True):
            db=witness._active_db
            inventory=_coordinator_commit_inventory(db,grant,secret)
            _write(sock,{'state':'held','nonce':nonce,'intentSha256':allowed[2],'inventory':inventory},deadline)
            finish=_read(sock,deadline)
            if type(finish) is not dict or set(finish)!={'nonce','reply'} or finish['nonce']!=nonce:
                raise SessionHeld('remote commit completion changed')
            reply=_reply(finish['reply'],grant['engineTaskId'])
            db.execute('INSERT INTO reference_broker_completion_candidate VALUES(?,?,?)',
                       (operation,allowed[2],hashlib.sha256(_json(reply)).hexdigest()))
            if db.execute("UPDATE broker_intents SET state='complete' WHERE operation_id=? AND job_id=? AND grant_hash=? AND state='started'",allowed).rowcount!=1:
                raise SessionHeld('remote commit intent changed')
        _write(sock,{'state':'complete','nonce':nonce,'intentSha256':allowed[2],
                     'replySha256':hashlib.sha256(_json(reply)).hexdigest()},deadline)
    finally:sock.close()


class RemoteReferenceCommitAuthority:
    """One photo-only commit authority; caller must verify local inventory."""
    def __init__(self,sock,*,authority,epoch,peer_sha256,timeout_seconds=10):
        if REFERENCE_REMOTE_QUALIFIED is not True or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=10:
            raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,False)
        self.sock,self.authority,self.epoch,self.timeout=sock,authority,epoch,timeout_seconds
        self.pid,self.used,self.active=os.getpid(),False,None

    @contextmanager
    def hold(self,grant,signature,*,operation_id,body,content_type,commit_signature):
        if self.pid!=os.getpid() or self.used or type(body) is not bytes:
            raise SessionHeld('reference commit session unavailable')
        self.used=True;deadline=time.monotonic()+self.timeout;nonce=secrets.token_hex(32)
        try:
            _write(self.sock,dict(grant=grant,signature=signature,operationId=operation_id,
                body=base64.b64encode(body).decode(),contentType=content_type,commitSignature=commit_signature,nonce=nonce),deadline)
            held=_read(self.sock,deadline)
            if (type(held) is not dict or set(held)!={'state','nonce','intentSha256','inventory'}
                    or held['state']!='held' or held['nonce']!=nonce or type(held['inventory']) is not dict
                    or type(held['intentSha256']) is not str or not re.fullmatch('[a-f0-9]{64}',held['intentSha256'])):
                raise SessionHeld('remote commit held proof changed')
            self.active=(nonce,held['intentSha256'],deadline)
            yield held
            if self.active is not None:raise SessionHeld('remote commit completion not acknowledged')
        finally:
            self.active=None;self.sock.close()

    def completed(self,reply,task_id):
        if self.active is None:raise SessionHeld('reference commit session not held')
        nonce,digest,deadline=self.active
        reply=_reply(reply,task_id)
        _write(self.sock,{'nonce':nonce,'reply':reply},deadline)
        expected={'state':'complete','nonce':nonce,'intentSha256':digest,'replySha256':hashlib.sha256(_json(reply)).hexdigest()}
        if _read(self.sock,deadline)!=expected:raise SessionHeld('remote commit completion proof changed')
        self.active=None
