"""Private worker metadata boundary; never mount under Viewer credentials.

The listener establishes protected_transport from its actual socket. Bearer
configuration fixes worker identity; requests never choose a node/environment.
No filesystem paths are part of the worker wire protocol.
"""
import hmac
import json
import re
from types import MappingProxyType

from . import reference_protocol as protocol
from .maintenance import Conflict
from .reference_api import Response
from .reference_transfer import OBJECT_PREFIX, TransferError, validate_grant
from .source_range_transfer import OBJECT_PREFIX as SOURCE_RANGE_OBJECT_PREFIX, RENEWAL_PATH, RENEWAL_ACK_PATH


PREFIX = "/ltds/worker/v1"


class _SourceBrokerStream:
    """Small owned reader adapter for the HTTP binary response boundary."""
    def __init__(self, reader, *, byte_size):
        self.reader = reader
        self.byte_size = byte_size
        self.remaining = byte_size
        self.closed = False

    def chunks(self):
        try:
            while self.remaining:
                chunk = self.reader.read(min(64 * 1024, self.remaining))
                if not isinstance(chunk, bytes) or not chunk or len(chunk) > self.remaining:
                    raise OSError("source stream changed during delivery")
                self.remaining -= len(chunk)
                yield chunk
            if self.reader.read(1):
                raise OSError("source stream exceeded its receipt")
        finally:
            self.close()

    def close(self):
        if not self.closed:
            self.closed = True
            self.reader.close()


class _SourceRangeBrokerStream(_SourceBrokerStream):
    def __init__(self, reader, *, byte_size, file_identity):
        super().__init__(reader, byte_size=byte_size)
        self.file_identity = file_identity

    def chunks(self):
        try:
            while self.remaining:
                chunk=self.reader.read(min(64*1024,self.remaining))
                if not isinstance(chunk,bytes) or not chunk or len(chunk)>self.remaining:
                    raise OSError('source range changed during delivery')
                self.remaining-=len(chunk)
                yield chunk
            check_identity = getattr(self.reader, 'verify_identity', None)
            if check_identity is not None:
                check_identity(self.file_identity)
            else:
                # Plain file support is retained for isolated stream fixtures only;
                # Store-created production source responses are registered readers.
                info = __import__('os').fstat(self.reader.fileno())
                identity = (info.st_dev,info.st_ino,info.st_size,info.st_mode,info.st_nlink,
                            info.st_uid,info.st_mtime_ns,info.st_ctime_ns)
                if identity != self.file_identity:
                    raise OSError('source object changed during range delivery')
        finally:
            self.close()


class ReferenceWorkerAPI:
    def __init__(self, store, receiver, *, bearer_workers, recovery_bindings=None,
                 source_spool=None):
        if type(bearer_workers) is not dict or not bearer_workers:
            raise ValueError("explicit private worker bearer configuration required")
        workers = {}
        for token, identity in bearer_workers.items():
            if (type(token) is not str or not 16 <= len(token) <= 4096
                    or not all(0x21 <= ord(char) <= 0x7e for char in token)):
                raise ValueError("invalid worker bearer configuration")
            protocol._closed(identity, ("node_id", "worker_id", "worker_instance_id"), "worker identity")
            store.queue._known_node(identity["node_id"])
            store.queue.slug(identity["worker_id"])
            protocol._slug(identity["worker_instance_id"], "worker instance ID")
            workers[token] = MappingProxyType(dict(identity))
        self.store, self.receiver = store, receiver
        self.source_spool = source_spool
        from .reference_failure import FailureReceiver
        self.failure_receiver = FailureReceiver(store)
        from .reference_terminal import TerminalService
        self.terminal = TerminalService(store)
        self._workers = MappingProxyType(workers)
        self.recovery = None
        if recovery_bindings is not None:
            from .reference_recovery import ClaimRecoveryService
            self.recovery = ClaimRecoveryService(store, recovery_bindings)

    @staticmethod
    def _header(headers, name):
        if not hasattr(headers, "items"):
            return None
        values = [value for key, value in headers.items()
                  if type(key) is str and key.lower() == name.lower()]
        return values[0] if len(values) == 1 and type(values[0]) is str else None

    def _authenticate(self, headers):
        authorization = self._header(headers, "Authorization")
        if authorization is None or not authorization.startswith("Bearer "):
            return None
        token = authorization[7:]
        if not token.isascii() or not 16 <= len(token) <= 4096:
            return None
        identity = None
        for configured, worker in self._workers.items():
            if hmac.compare_digest(token, configured):
                identity = worker
        return identity

    @staticmethod
    def _error(status, code):
        return Response(status, {"error": code})

    def _owned_job(self, job_id, identity, claim_token):
        job = self.store.get(job_id, private=True)
        with self.store.queue._db() as db:
            claim = db.execute("SELECT * FROM reference_claims WHERE job_id=?", (job_id,)).fetchone()
        if (not claim or (claim["node_id"], claim["worker_id"], claim["instance_id"]) !=
                (identity["node_id"], identity["worker_id"], identity["worker_instance_id"])
                or type(claim_token) is not str or not claim_token.isascii()
                or not hmac.compare_digest(claim["token"], claim_token)):
            raise Conflict("worker grant ownership mismatch")
        return job

    def _grant(self, job_id, identity, claim_token):
        job = self._owned_job(job_id, identity, claim_token)
        if job["status"] != "running" or job["phase"] != "awaiting_transfer":
            raise Conflict("worker cannot fetch in this execution state")
        grant = self.store.private_transfer_grant(job_id)
        if grant is None and job.get("sourceIntakeVersion") == 2:
            if self.source_spool is None:
                raise Conflict("source broker is unavailable")
            grant = self.store.mint_source_transfer_grant(job_id, source_spool=self.source_spool)
        if grant is None:
            raise Conflict("transfer grant not ready")
        try:
            if (job.get('transferRequest') or {}).get('protocol') == 'source-range-v2':
                from .source_range_transfer import validate_source_grant
                validate_source_grant(grant, job=job)
            else:
                validate_grant(grant, job=job)
        except ValueError:
            raise Conflict("persisted transfer grant is unavailable") from None
        return grant

    def _source_object(self, path, headers):
        match = re.fullmatch(re.escape(OBJECT_PREFIX) + r"([A-Za-z0-9][A-Za-z0-9_-]{0,127})", path)
        if match is None or self.source_spool is None:
            return self._error(404, "not_found")
        authorization = self._header(headers, "Authorization")
        if authorization is None or not authorization.startswith("Bearer "):
            return self._error(401, "source_grant_required")
        token = authorization[7:]
        if not token.isascii() or not 32 <= len(token) <= 4096:
            return self._error(401, "source_grant_required")
        if self._header(headers, "Range") is not None:
            return self._error(400, "range_not_supported")
        try:
            reader, receipt = self.store.source_object_for_grant(token, match[1],
                source_spool=self.source_spool)
            from .reference_binary import BinaryResponse
            stream = _SourceBrokerStream(reader, byte_size=receipt["byteSize"])
            return BinaryResponse(stream=stream, status=200, headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(receipt["byteSize"]),
                "Cache-Control": "no-store"})
        except (Conflict, TransferError):
            return self._error(409, "source_grant_unavailable")
        except Exception:
            return self._error(503, "source_storage_unavailable")

    def _source_range_object(self, path, headers):
        import re
        from .reference_binary import BinaryResponse
        match = re.fullmatch(re.escape(SOURCE_RANGE_OBJECT_PREFIX) + r"([A-Za-z0-9][A-Za-z0-9_-]{0,127})", path)
        if match is None or self.source_spool is None:
            return self._error(404,"not_found")
        authorization = self._header(headers,'Authorization')
        if authorization is None or not authorization.startswith('Bearer '):
            return self._error(401,'source_grant_required')
        token = authorization[7:]
        raw_range = self._header(headers,'Range')
        range_match = re.fullmatch(r'bytes=(0|[1-9][0-9]{0,15})-(0|[1-9][0-9]{0,15})',raw_range or '')
        request_id = self._header(headers,'X-Processing-Request-ID')
        if (not token.isascii() or not 32 <= len(token) <= 4096 or range_match is None
                or not re.fullmatch(r'[a-f0-9]{64}',request_id or '')
                or self._header(headers,'Accept-Encoding') not in (None,'identity')):
            return self._error(400,'invalid_source_range')
        try:
            start,end=map(int,range_match.groups())
            if end < start or end > protocol.MAX_SAFE_INTEGER:
                return self._error(416,'invalid_source_range')
            reader, receipt = self.store.source_range_for_grant(token,match[1],range_start=start,
                range_length=end-start+1,request_id=request_id,if_match=self._header(headers,'If-Match'),
                source_spool=self.source_spool)
            stream=_SourceRangeBrokerStream(reader,byte_size=receipt['rangeLength'],
                file_identity=receipt['fileIdentity'])
            return BinaryResponse(stream=stream,status=206,headers={
                'Content-Type':'application/octet-stream','Content-Length':str(receipt['rangeLength']),
                'Content-Range':f"bytes {start}-{end}/{receipt['byteSize']}",
                'ETag':'"sha256-'+receipt['sha256']+'"','X-Processing-Request-ID':request_id,
                'Accept-Ranges':'bytes','Cache-Control':'no-store'})
        except (Conflict,TransferError):
            return self._error(409,'source_range_unavailable')
        except Exception:
            return self._error(503,'source_storage_unavailable')

    def _source_renewal(self, path, body, identity, claim_token):
        if self.source_spool is None:
            return self._error(503,'renewal_authority_unavailable')
        try:
            request=protocol.decode_submission(body)
            if path == RENEWAL_PATH:
                response=self.store.renew_source_transfer_grant(request,source_spool=self.source_spool,
                    worker=dict(identity,claim_token=claim_token))
            else:
                response=self.store.acknowledge_source_transfer_grant(request,worker=dict(identity))
            return Response(200,response)
        except Conflict as error:
            code=str(error)
            if code in ('grant_not_expired','renewal_authority_unavailable'):
                return self._error(409,code)
            return self._error(409,'source_renewal_unavailable')
        except (ValueError,TypeError):
            return self._error(400,'invalid_source_renewal')
        except Exception:
            return self._error(503,'renewal_authority_unavailable')

    def _next(self, identity):
        job = self.store.next_for_node(identity['node_id'])
        if job is None:
            return {'job': None, 'execution': None}
        # Discovery remains advisory. Read the immutable submission and bind it
        # to the same currently selected node/job pair within one snapshot.
        with self.store.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            candidate = self.store._candidate_row(db, identity['node_id'])
            if candidate is None or candidate['job_id'] != job['jobId']:
                raise Conflict('advisory node selection changed')
            row = self.store._row(db, job['jobId'])
            bound = ('jobId', 'generation', 'environment', 'manifestSha256', 'pipelineDigest')
            current = self.store._public(row)
            if any(current[key] != job[key] for key in bound) or current['status'] != 'queued':
                raise Conflict('advisory job binding changed')
            submission = json.loads(row['input_json'])
            if any(submission[key] != job[key] for key in ('environment', 'manifestSha256', 'pipelineDigest')):
                raise Conflict('persisted execution binding changed')
            odm = submission['pipeline']['odm']['requested']
            treedar = submission['pipeline']['treedar']['requested']
            if type(odm) is not bool or type(treedar) is not bool or not (odm or treedar):
                raise Conflict('persisted execution unavailable')
            kind = 'combined' if odm and treedar else 'odm' if odm else 'treedar'
            return {'job': job, 'execution': {'kind': kind, **{key: job[key] for key in bound}}}

    def handle(self, method, path, headers, body=b"", *, protected_transport=False):
        if protected_transport is not True:
            return self._error(503, "protected_transport_required")
        if isinstance(path,str) and path.startswith(SOURCE_RANGE_OBJECT_PREFIX):
            if method != 'GET' or body:
                return self._error(405,'method_not_allowed')
            return self._source_range_object(path,headers)
        if path in (RENEWAL_PATH,RENEWAL_ACK_PATH):
            if method != 'POST' or not body:
                return self._error(405,'method_not_allowed')
            # A grant is a capability, not an identity. Renewal happens after it
            # expired, so only the worker's own credentials can ask for another.
            identity=self._authenticate(headers)
            claim_token=self._header(headers,'X-Claim-Token')
            if identity is None or (path == RENEWAL_PATH and claim_token is None):
                return self._error(401,'unauthorized')
            return self._source_renewal(path,body,identity,claim_token)
        if isinstance(path, str) and path.startswith(OBJECT_PREFIX):
            if method != "GET" or body:
                return self._error(405, "method_not_allowed")
            return self._source_object(path, headers)
        identity = self._authenticate(headers)
        if identity is None:
            return self._error(401, "unauthorized")
        try:
            if method == 'GET' and path == PREFIX + '/authority':
                return Response(200, {'authorityId': self.store.queue.authority_id})
            if method == 'GET' and path == PREFIX + '/next':
                return Response(200, self._next(identity))
            if method == "POST" and path == PREFIX + "/claim":
                payload = protocol.decode_submission(body)
                protocol._closed(payload, ("jobId", "claimRequestId"), "claim")
                protocol._slug(payload["jobId"], "job ID")
                protocol._slug(payload["claimRequestId"], "claim request ID")
                claim = self.store.claim(payload["jobId"], **identity,
                                         claim_request_id=payload["claimRequestId"])
                if claim is None:
                    return self._error(409, "claim_unavailable")
                return Response(200, claim)
            if method == "POST" and path == PREFIX + "/results":
                payload = protocol.decode_submission(body)
                protocol._closed(payload, ("claimRequestId", "claimToken", "delivery"), "worker results")
                protocol._slug(payload["claimRequestId"], "claim request ID")
                token = payload["claimToken"]
                if (type(token) is not str or not 1 <= len(token) <= 128 or not token.isascii()):
                    raise ValueError("invalid claim token")
                receipt = self.receiver.receive(payload["delivery"], **identity,
                    claim_request_id=payload["claimRequestId"], claim_token=token)
                return Response(200, receipt)
            if method == 'POST' and path == PREFIX + '/terminal-outcomes':
                payload = protocol.decode_submission(body)
                protocol._closed(payload, ('claimRequestId', 'claimToken', 'delivery'), 'worker terminal outcome')
                receipt = self.failure_receiver.receive(payload['delivery'], **identity,
                    claim_request_id=payload['claimRequestId'], claim_token=payload['claimToken'])
                return Response(200, receipt)
            action = re.fullmatch(re.escape(PREFIX) + r'/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})/(reservation|phase)', path) if type(path) is str else None
            if method == 'POST' and action:
                job_id, kind = action.groups()
                payload = protocol.decode_submission(body)
                field = 'transferRequest' if kind == 'reservation' else 'phase'
                protocol._closed(payload, ('claimToken', field), 'worker control')
                job = self._owned_job(job_id, identity, payload['claimToken'])
                if job['status'] != 'running':
                    raise Conflict('worker execution is not running')
                if kind == 'reservation':
                    return Response(200, self.store.reserve(job_id, payload[field], node_id=identity['node_id']))
                if payload[field] not in ('fetching', 'processing'):
                    raise ValueError('worker may only advance transfer and processing phases')
                return Response(200, self.store.update_phase(job_id, payload[field]))
            recovery = re.fullmatch(re.escape(PREFIX) + r'/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})/(recovery-state|recover|recovery-receipts/([a-f0-9-]{36}))', path) if type(path) is str else None
            if recovery:
                if self.recovery is None:
                    return self._error(404, 'not_found')
                token = self._header(headers, 'X-Claim-Token')
                if token is None or not 1 <= len(token) <= 128:
                    return self._error(401, 'claim_authentication_required')
                job_id, operation, request_id = recovery.groups()
                if method == 'GET' and body:
                    return self._error(400, 'invalid_request')
                if method == 'GET' and operation == 'recovery-state':
                    return Response(200, self.recovery.state(job_id, identity=identity, claim_token=token))
                if method == 'GET' and request_id:
                    receipt = self.recovery.receipt(job_id, request_id, identity=identity, claim_token=token)
                    return Response(200, receipt) if receipt is not None else self._error(404, 'receipt_unavailable')
                if method == 'POST' and operation == 'recover':
                    return Response(200, self.recovery.resume(job_id, protocol.decode_submission(body), identity=identity, claim_token=token))
                return self._error(404, 'not_found')
            observation = re.fullmatch(re.escape(PREFIX) + r"/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})/acceptance-observed", path) if type(path) is str else None
            if method == 'POST' and observation:
                claim_token = self._header(headers, 'X-Claim-Token')
                if claim_token is None or not 1 <= len(claim_token) <= 128:
                    return self._error(401, 'claim_authentication_required')
                self._owned_job(observation[1], identity, claim_token)
                return Response(200, self.store.observe_worker_acceptance(observation[1],
                    protocol.decode_submission(body), identity=identity, claim_token=claim_token))
            match = re.fullmatch(re.escape(PREFIX) + r"/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})/(transfer-grant|acceptance|terminal-acceptance|claim-state)", path) if type(path) is str else None
            if method == "GET" and match:
                claim_token = self._header(headers, "X-Claim-Token")
                if claim_token is None or not 1 <= len(claim_token) <= 128:
                    return self._error(401, "claim_authentication_required")
                if match[2] == 'claim-state':
                    if body:
                        return self._error(400, 'invalid_request')
                    return Response(200, self.store.claim_snapshot(match[1], **identity, claim_token=claim_token))
                if match[2] == 'acceptance':
                    self._owned_job(match[1], identity, claim_token)
                    return Response(200, self.store.acceptance_receipts(match[1]))
                if match[2] == 'terminal-acceptance':
                    job = self._owned_job(match[1], identity, claim_token)
                    return Response(200, self.terminal.accepted(match[1], job['environment']))
                return Response(200, self._grant(match[1], identity, claim_token))
            return self._error(404, "not_found")
        except Conflict:
            return self._error(409, "worker_conflict")
        except (ValueError, TypeError):
            return self._error(400, "invalid_request")
        except Exception:
            return self._error(500, "internal_error")
