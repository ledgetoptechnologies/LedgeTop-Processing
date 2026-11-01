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
import socket
import ssl
import ipaddress
import sqlite3
import time

from .node_authority_session import MAX_FRAME, SessionHeld, _peer, _read, _write
from .node_mutation_broker import (ReferenceCoordinatorAuthority, _REFERENCE_INIT_FIELDS,
                                   _REFERENCE_COMMIT_DDL, _REFERENCE_UPLOAD_ISSUE_DDL, _REFERENCE_RECEIPT_DDL, _REFERENCE_PROOF_DDL,
                                   _json, _reply, _commit_reply, _upload_reply, _reference_receipt_signature,
                                   _reference_verified_image_only_uploads,
                                   sign_grant, sign_reference_issue, sign_reference_commit, sign_reference_proof)

REFERENCE_REMOTE_QUALIFIED = False


class RemoteReferenceSessionFactory:
    """Disabled per-operation pinned mTLS connector; owns every opened socket."""
    def __init__(self,*,host,port,authority,epoch,peer_sha256,ca_file,cert_file,key_file,timeout_seconds=5):
        if REFERENCE_REMOTE_QUALIFIED is not True:
            raise SessionHeld('reference remote authority unavailable')
        try: address=ipaddress.ip_address(host)
        except ValueError:raise SessionHeld('literal reference authority address required') from None
        if (type(port) is not int or not 1<=port<=65535 or type(peer_sha256) is not str
                or not re.fullmatch('[a-f0-9]{64}',peer_sha256) or type(timeout_seconds) not in (int,float)
                or not 0<timeout_seconds<=10):
            raise SessionHeld('invalid reference session factory')
        context=ssl.create_default_context(cafile=str(ca_file))
        context.load_cert_chain(str(cert_file),str(key_file))
        self.host,self.port,self.authority,self.epoch=str(address),port,authority,epoch
        self.pin,self.timeout,self.context=peer_sha256,timeout_seconds,context

    def open(self,kind):
        classes={'init':RemoteReferenceInitAuthority,'upload':RemoteReferenceUploadAuthority,
                 'commit':RemoteReferenceCommitAuthority,'plan':RemoteReferencePlanClient,
                 'proof':RemoteReferenceProofAuthority,'reconcile':RemoteReferenceReconcileAuthority}
        if kind not in classes:raise SessionHeld('reference operation unavailable')
        deadline=time.monotonic()+self.timeout
        raw=socket.create_connection((self.host,self.port),timeout=max(0,deadline-time.monotonic()))
        channel=None
        try:
            raw.settimeout(max(0,deadline-time.monotonic()))
            channel=self.context.wrap_socket(raw,server_hostname=self.host)
            _peer(channel,self.pin,False)
            _write(channel,{'operation':kind},deadline)
            return classes[kind](channel,authority=self.authority,epoch=self.epoch,
                                 peer_sha256=self.pin,timeout_seconds=max(0,deadline-time.monotonic()))
        except BaseException:
            if channel is not None:channel.close()
            else:raw.close()
            raise


def serve_reference_operation(sock,*,witness,secret,peer_sha256,timeout_seconds=10):
    """Dispatch exactly one pinned operation; caller owns listening socket."""
    try:
        if REFERENCE_REMOTE_QUALIFIED is not True:raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,True)
        deadline=time.monotonic()+timeout_seconds
        first=_read(sock,deadline)
        if type(first) is not dict or set(first)!={'operation'}:
            raise SessionHeld('invalid reference operation')
        functions={'init':serve_reference_init,'upload':serve_reference_upload,'commit':serve_reference_commit,
                   'plan':serve_reference_plan,'proof':serve_reference_proof,'reconcile':serve_reference_reconcile}
        function=functions.get(first['operation'])
        if function is None:raise SessionHeld('reference operation unavailable')
        function(sock,witness=witness,secret=secret,peer_sha256=peer_sha256,
                 timeout_seconds=max(0,deadline-time.monotonic()))
    finally:sock.close()


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


def serve_reference_plan(sock, *, witness, secret, peer_sha256, timeout_seconds=10):
    """Disabled pinned peer can request only its exact active claim plan."""
    from .node_reference_plan import issue_reference_plan
    try:
        if (REFERENCE_REMOTE_QUALIFIED is not True or type(witness) is not ReferenceCoordinatorAuthority
                or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=10):
            raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,True);deadline=time.monotonic()+timeout_seconds
        request=_read(sock,deadline)
        if (type(request) is not dict or set(request)!={'jobId','claimToken','workerId','workerInstanceId','claimRequestId','nonce'}
                or any(type(request[k]) is not str or not 1<=len(request[k])<=128 for k in request)
                or not re.fullmatch('[a-f0-9]{64}',request['nonce'])):
            raise SessionHeld('invalid reference plan request')
        with witness.queue._db() as db:
            row=db.execute('''SELECT j.status,r.job_id,r.node_id,r.generation,r.environment,r.manifest_sha,r.pipeline_digest,r.input_json
                FROM reference_jobs r JOIN jobs j ON j.id=r.job_id WHERE j.id=?''',(request['jobId'],)).fetchone()
            if row is None:raise SessionHeld('reference plan claim unavailable')
            job=dict(jobId=row['job_id'],status=row['status'],nodeId=row['node_id'],generation=row['generation'],
                environment=row['environment'],manifestSha256=row['manifest_sha'],pipelineDigest=row['pipeline_digest'],
                submission=__import__('json').loads(row['input_json']))
        plan=issue_reference_plan(witness,job,claim_token=request['claimToken'],worker_id=request['workerId'],
            worker_instance_id=request['workerInstanceId'],claim_request_id=request['claimRequestId'],secret=secret,
            timeout=max(0,deadline-time.monotonic()))
        response={'nonce':request['nonce'],'plan':plan}
        if len(_json(response))>MAX_FRAME:raise SessionHeld('reference plan exceeds frame bound')
        _write(sock,response,deadline)
    finally:sock.close()


class RemoteReferencePlanClient:
    """One pinned plan request; secret claim is sent only on mTLS."""
    def __init__(self,sock,*,authority,epoch,peer_sha256,timeout_seconds=10):
        if REFERENCE_REMOTE_QUALIFIED is not True:raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,False)
        self.sock,self.authority,self.epoch,self.timeout=sock,authority,epoch,timeout_seconds
        self.pid,self.used=os.getpid(),False

    def fetch(self,*,job_id,claim_token,worker_id,worker_instance_id,claim_request_id):
        if self.pid!=os.getpid() or self.used:raise SessionHeld('reference plan session unavailable')
        self.used=True;nonce=secrets.token_hex(32);deadline=time.monotonic()+self.timeout
        try:
            _write(self.sock,dict(jobId=job_id,claimToken=claim_token,workerId=worker_id,
                workerInstanceId=worker_instance_id,claimRequestId=claim_request_id,nonce=nonce),deadline)
            response=_read(self.sock,deadline)
            if type(response) is not dict or set(response)!={'nonce','plan'} or response['nonce']!=nonce or type(response['plan']) is not dict:
                raise SessionHeld('reference plan response changed')
            grant=response['plan'].get('initGrant')
            if (type(grant) is not dict or grant.get('authorityId')!=self.authority or grant.get('coordinatorEpoch')!=self.epoch
                    or grant.get('jobId')!=job_id or grant.get('workerId')!=worker_id
                    or grant.get('workerInstanceId')!=worker_instance_id or grant.get('claimRequestId')!=claim_request_id
                    or grant.get('claimTokenSha256')!=hashlib.sha256(claim_token.encode('ascii')).hexdigest()):
                raise SessionHeld('reference plan binding changed')
            return response['plan']
        finally:self.sock.close()


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


def _coordinator_complete_commit_proof(db,grant,commit_id,secret):
    inventory=_coordinator_commit_inventory(db,grant,secret)
    commit=db.execute('SELECT * FROM reference_broker_commit_candidate WHERE operation_id=?',(commit_id,)).fetchone()
    init_hash=hashlib.sha256(_json(grant)).hexdigest()
    if commit is None or commit['job_id']!=grant['jobId'] or commit['init_sha256']!=init_hash:
        raise SessionHeld('remote commit binding changed')
    digest=hashlib.sha256(_json(list(commit))).hexdigest()
    intent=db.execute('SELECT job_id,grant_hash,state FROM broker_intents WHERE operation_id=?',(commit_id,)).fetchone()
    proof=db.execute('SELECT grant_hash,response_sha256 FROM reference_broker_completion_candidate WHERE operation_id=?',(commit_id,)).fetchone()
    if intent is None or tuple(intent)!=(grant['jobId'],digest,'complete') or proof is None or proof['grant_hash']!=digest:
        raise SessionHeld('remote commit proof unavailable')
    return dict(inventory=inventory,commitIntentSha256=digest,commitReplySha256=proof['response_sha256'])


def serve_reference_proof(sock, *, witness, secret, peer_sha256, timeout_seconds=10):
    """Fence one remote poll or exact durable delivery acknowledgement."""
    try:
        if (REFERENCE_REMOTE_QUALIFIED is not True or type(witness) is not ReferenceCoordinatorAuthority
                or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=10):
            raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,True);deadline=time.monotonic()+timeout_seconds;request=_read(sock,deadline)
        if type(request) is not dict or set(request)!={'grant','signature','commitOperationId','operationId','delivery','proofSignature','nonce'}:
            raise SessionHeld('invalid remote proof request')
        grant,signature,commit_id,op,delivery,proof_signature,nonce=(request[k] for k in
            ('grant','signature','commitOperationId','operationId','delivery','proofSignature','nonce'))
        from .node_mutation_broker import _uuid
        if (type(grant) is not dict or set(grant)!=_REFERENCE_INIT_FIELDS or grant.get('operation')!='init'
                or type(signature) is not str or not hmac.compare_digest(sign_grant(grant,secret),signature)
                or not _uuid(op) or not _uuid(commit_id) or op in (grant['operationId'],commit_id)
                or not (delivery is None or (type(delivery) is list and len(delivery) in (2,5) and all(type(v) is str for v in delivery)))
                or type(proof_signature) is not str or not hmac.compare_digest(sign_reference_proof(grant,commit_id,op,delivery,secret),proof_signature)
                or type(nonce) is not str or not re.fullmatch('[a-f0-9]{64}',nonce)):
            raise SessionHeld('remote proof authentication failed')
        delivery_tuple=tuple(delivery) if delivery is not None and len(delivery)==2 else None
        failure_tuple=tuple(delivery) if delivery is not None and len(delivery)==5 else None
        proof_row=(op,grant['jobId'],hashlib.sha256(_json(grant)).hexdigest(),commit_id,_json(delivery).decode() if delivery is not None else '')
        allowed=(op,grant['jobId'],hashlib.sha256(_json(list(proof_row))).hexdigest())
        with witness.hold(grant,max(0,deadline-time.monotonic()),_delivery=delivery_tuple,_failure_delivery=failure_tuple,_remote_fenced=True):
            db=witness._active_db
            schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_broker_proof_candidate'").fetchone()
            if schema is None or schema[0]!=_REFERENCE_PROOF_DDL:raise SessionHeld('remote proof schema unavailable')
            evidence=_coordinator_complete_commit_proof(db,grant,commit_id,secret)
            if len(_json({'state':'held','nonce':nonce,'intentSha256':allowed[2],'evidence':evidence}))>MAX_FRAME:
                raise SessionHeld('remote proof inventory exceeds frame bound')
            if db.execute('SELECT COUNT(*) FROM reference_broker_proof_candidate').fetchone()[0]>=100000:
                raise SessionHeld('remote proof capacity reached')
            try:
                db.execute('INSERT INTO reference_broker_proof_candidate VALUES(?,?,?,?,?)',proof_row)
                db.execute("INSERT INTO broker_intents VALUES(?,?,?,'started')",allowed)
            except sqlite3.IntegrityError:raise SessionHeld('remote proof requires inspection') from None
        with witness.hold(grant,max(0,deadline-time.monotonic()),_delivery=delivery_tuple,_failure_delivery=failure_tuple,_remote_fenced=True,_allowed_proof=allowed):
            db=witness._active_db;evidence=_coordinator_complete_commit_proof(db,grant,commit_id,secret)
            _write(sock,{'state':'held','nonce':nonce,'intentSha256':allowed[2],'evidence':evidence},deadline)
            finish=_read(sock,deadline)
            if finish!={'nonce':nonce,'state':'finished'}:raise SessionHeld('remote proof completion changed')
            if db.execute("UPDATE broker_intents SET state='complete' WHERE operation_id=? AND job_id=? AND grant_hash=? AND state='started'",allowed).rowcount!=1:
                raise SessionHeld('remote proof intent changed')
        _write(sock,{'state':'complete','nonce':nonce,'intentSha256':allowed[2]},deadline)
    finally:sock.close()


class RemoteReferenceProofAuthority:
    """One fenced remote proof use; local gate/journal verification is required."""
    def __init__(self,sock,*,authority,epoch,peer_sha256,timeout_seconds=10):
        if REFERENCE_REMOTE_QUALIFIED is not True:raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,False)
        self.sock,self.authority,self.epoch,self.timeout=sock,authority,epoch,timeout_seconds
        self.pid,self.used,self.active=os.getpid(),False,None

    @contextmanager
    def hold(self,grant,signature,*,commit_operation_id,operation_id,delivery,proof_signature):
        if self.pid!=os.getpid() or self.used:raise SessionHeld('reference proof session unavailable')
        self.used=True;deadline=time.monotonic()+self.timeout;nonce=secrets.token_hex(32)
        try:
            _write(self.sock,dict(grant=grant,signature=signature,commitOperationId=commit_operation_id,operationId=operation_id,
                delivery=delivery,proofSignature=proof_signature,nonce=nonce),deadline)
            held=_read(self.sock,deadline)
            if (type(held) is not dict or set(held)!={'state','nonce','intentSha256','evidence'} or held['state']!='held'
                    or held['nonce']!=nonce or type(held['intentSha256']) is not str or not re.fullmatch('[a-f0-9]{64}',held['intentSha256'])
                    or type(held['evidence']) is not dict):raise SessionHeld('remote proof held response changed')
            self.active=(nonce,held['intentSha256'],deadline)
            yield held
            if self.active is not None:raise SessionHeld('remote proof completion not acknowledged')
        finally:self.active=None;self.sock.close()

    def completed(self):
        if self.active is None:raise SessionHeld('remote proof session not held')
        nonce,digest,deadline=self.active
        _write(self.sock,{'nonce':nonce,'state':'finished'},deadline)
        if _read(self.sock,deadline)!={'state':'complete','nonce':nonce,'intentSha256':digest}:
            raise SessionHeld('remote proof completion changed')
        self.active=None


def serve_reference_reconcile(sock, *, witness, secret, peer_sha256, timeout_seconds=10):
    """Read-only exact completed failure-release proof; never creates intent."""
    try:
        if (REFERENCE_REMOTE_QUALIFIED is not True or type(witness) is not ReferenceCoordinatorAuthority
                or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=10):
            raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,True);deadline=time.monotonic()+timeout_seconds;request=_read(sock,deadline)
        if type(request) is not dict or set(request)!={'grant','signature','commitOperationId','operationId','delivery','proofSignature','nonce'}:
            raise SessionHeld('invalid remote reconcile request')
        grant,signature,commit_id,op,delivery,proof_signature,nonce=(request[k] for k in
            ('grant','signature','commitOperationId','operationId','delivery','proofSignature','nonce'))
        from .node_mutation_broker import _uuid
        if (type(grant) is not dict or set(grant)!=_REFERENCE_INIT_FIELDS or grant.get('operation')!='init'
                or type(signature) is not str or not hmac.compare_digest(sign_grant(grant,secret),signature)
                or not _uuid(op) or not _uuid(commit_id) or type(delivery) is not list or len(delivery)!=5
                or delivery[0]!='failure' or any(type(v) is not str for v in delivery)
                or type(proof_signature) is not str or not hmac.compare_digest(sign_reference_proof(grant,commit_id,op,delivery,secret),proof_signature)
                or type(nonce) is not str or not re.fullmatch('[a-f0-9]{64}',nonce)):
            raise SessionHeld('remote reconcile authentication failed')
        with witness.hold(grant,max(0,deadline-time.monotonic()),_failure_delivery=tuple(delivery),_remote_fenced=True):
            db=witness._active_db
            _coordinator_complete_commit_proof(db,grant,commit_id,secret)
            schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_broker_proof_candidate'").fetchone()
            row=db.execute('SELECT * FROM reference_broker_proof_candidate WHERE operation_id=?',(op,)).fetchone() if schema is not None and schema[0]==_REFERENCE_PROOF_DDL else None
            if (row is None or row['job_id']!=grant['jobId'] or row['init_sha256']!=hashlib.sha256(_json(grant)).hexdigest()
                    or row['commit_operation_id']!=commit_id or row['delivery_json']!=_json(delivery).decode()):
                raise SessionHeld('remote completed proof binding changed')
            digest=hashlib.sha256(_json(list(row))).hexdigest()
            intent=db.execute('SELECT job_id,grant_hash,state FROM broker_intents WHERE operation_id=?',(op,)).fetchone()
            if intent is None or tuple(intent)!=(grant['jobId'],digest,'complete'):
                raise SessionHeld('remote completed proof unavailable')
            _write(sock,{'state':'held','nonce':nonce,'intentSha256':digest},deadline)
            if _read(sock,deadline)!={'nonce':nonce,'state':'finished'}:raise SessionHeld('remote reconcile completion changed')
        _write(sock,{'state':'complete','nonce':nonce,'intentSha256':digest},deadline)
    finally:sock.close()


class RemoteReferenceReconcileAuthority:
    """One bounded mTLS read-only completed failure proof session."""
    def __init__(self,sock,*,authority,epoch,peer_sha256,timeout_seconds=10):
        if REFERENCE_REMOTE_QUALIFIED is not True:raise SessionHeld('reference remote authority unavailable')
        _peer(sock,peer_sha256,False)
        self.sock,self.authority,self.epoch,self.timeout=sock,authority,epoch,timeout_seconds
        self.pid,self.used,self.active=os.getpid(),False,None

    @contextmanager
    def hold(self,grant,signature,*,commit_operation_id,operation_id,delivery,proof_signature):
        if self.pid!=os.getpid() or self.used:raise SessionHeld('reference reconcile session unavailable')
        self.used=True;deadline=time.monotonic()+self.timeout;nonce=secrets.token_hex(32)
        try:
            _write(self.sock,dict(grant=grant,signature=signature,commitOperationId=commit_operation_id,operationId=operation_id,
                delivery=delivery,proofSignature=proof_signature,nonce=nonce),deadline)
            held=_read(self.sock,deadline)
            if (type(held) is not dict or set(held)!={'state','nonce','intentSha256'} or held['state']!='held'
                    or held['nonce']!=nonce or type(held['intentSha256']) is not str or not re.fullmatch('[a-f0-9]{64}',held['intentSha256'])):
                raise SessionHeld('remote reconcile held response changed')
            self.active=(nonce,held['intentSha256'],deadline)
            yield held['intentSha256']
            if self.active is not None:raise SessionHeld('remote reconcile completion not acknowledged')
        finally:self.active=None;self.sock.close()

    def completed(self):
        if self.active is None:raise SessionHeld('remote reconcile session not held')
        nonce,digest,deadline=self.active
        _write(self.sock,{'nonce':nonce,'state':'finished'},deadline)
        if _read(self.sock,deadline)!={'state':'complete','nonce':nonce,'intentSha256':digest}:
            raise SessionHeld('remote reconcile completion changed')
        self.active=None


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
            inventory=_coordinator_commit_inventory(db,grant,secret)
            # Refuse oversized response before creating an irreversible started
            # commit intent; multipart inventory transport is not qualified.
            if len(_json({'state':'held','nonce':nonce,'intentSha256':allowed[2],'inventory':inventory}))>MAX_FRAME:
                raise SessionHeld('remote commit inventory exceeds frame bound')
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
            reply=_commit_reply(finish['reply'],grant['engineTaskId'])
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
        reply=_commit_reply(reply,task_id)
        _write(self.sock,{'nonce':nonce,'reply':reply},deadline)
        expected={'state':'complete','nonce':nonce,'intentSha256':digest,'replySha256':hashlib.sha256(_json(reply)).hexdigest()}
        if _read(self.sock,deadline)!=expected:raise SessionHeld('remote commit completion proof changed')
        self.active=None
