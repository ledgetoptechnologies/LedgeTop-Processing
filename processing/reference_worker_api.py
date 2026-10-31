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
from .reference_transfer import validate_grant


PREFIX = "/ltds/worker/v1"


class ReferenceWorkerAPI:
    def __init__(self, store, receiver, *, bearer_workers, recovery_bindings=None, viewer_claims=None, viewer_spool=None):
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
        from .reference_failure import FailureReceiver
        self.failure_receiver = FailureReceiver(store)
        from .reference_terminal import TerminalService
        self.terminal = TerminalService(store)
        self._workers = MappingProxyType(workers)
        self.viewer_claims = viewer_claims
        if viewer_spool is not None:
            from .viewer_archive_spool import ViewerArchiveSpool
            if type(viewer_spool) is not ViewerArchiveSpool or viewer_spool.claims is not viewer_claims:
                raise ValueError('bound viewer spool required')
        self.viewer_spool=viewer_spool
        self.viewer_chunks=None  # Explicit synthetic fixture injection only.
        self.viewer_restart_census=None  # Explicit synthetic fixture injection only.
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

    def handle_archive(self,path,headers,length,chunks,*,protected_transport=False):
        """Authenticate and reserve before iterating an archive request body."""
        if protected_transport is not True:return self._error(503,'protected_transport_required')
        identity=self._authenticate(headers)
        if identity is None:return self._error(401,'unauthorized')
        match=re.fullmatch(re.escape(PREFIX)+r'/viewer-claims/([a-f0-9-]{36})/archive',path)
        if not match or self.viewer_spool is None:return self._error(404,'not_found')
        if self._header(headers,'Content-Type')!='application/octet-stream':return self._error(415,'invalid_content_type')
        token=self._header(headers,'X-Claim-Token')
        try:
            receipt=self.viewer_spool.receive(match[1],identity,token,chunks,expected_length=length)
            return Response(200,receipt)
        except Exception:return self._error(409,'archive_requires_inspection')

    def handle_chunk(self,path,headers,length,chunks,*,tls_transport=False):
        """Synthetic TLS ingress: authenticate and admit before body iteration."""
        if tls_transport is not True:return self._error(503,'tls_required')
        identity=self._authenticate(headers)
        if identity is None:return self._error(401,'unauthorized')
        match=re.fullmatch(re.escape(PREFIX)+r'/viewer-claims/([a-f0-9-]{36})/chunks/([A-Za-z0-9_-]{1,128})',path)
        if not match or self.viewer_chunks is None:return self._error(404,'not_found')
        if self._header(headers,'Content-Type')!='application/octet-stream':return self._error(415,'invalid_content_type')
        token=self._header(headers,'X-Claim-Token');permit=None
        try:
            operation=match[2]
            if operation=='begin':
                size=self._header(headers,'X-Chunk-Size')
                if length!=0 or not isinstance(size,str) or not re.fullmatch(r'[1-9][0-9]{0,5}',size):raise ValueError('invalid chunk size')
                return Response(200,self.viewer_chunks.begin(match[1],identity,token,chunk_bytes=int(size)))
            if operation in ('finish','promote'):
                count=self._header(headers,'X-Chunk-Count')
                if length!=0 or not isinstance(count,str) or not re.fullmatch(r'[1-9][0-9]{0,3}',count):raise ValueError('invalid count')
                action=self.viewer_chunks.finish if operation=='finish' else self.viewer_chunks.promote
                return Response(200,action(match[1],identity,token,int(count)))
            index=self._header(headers,'X-Chunk-Index');offset=self._header(headers,'X-Chunk-Offset');digest=self._header(headers,'X-Chunk-SHA256')
            if not isinstance(index,str) or not re.fullmatch(r'0|[1-9][0-9]{0,3}',index) or not isinstance(offset,str) or not re.fullmatch(r'0|[1-9][0-9]{0,15}',offset):
                raise ValueError('invalid chunk headers')
            if operation.startswith('status-'):
                saved_length=self._header(headers,'X-Chunk-Length')
                if length!=0 or not isinstance(saved_length,str) or not re.fullmatch(r'[1-9][0-9]{0,5}',saved_length):raise ValueError('invalid status length')
                return Response(200,self.viewer_chunks.status_chunk(match[1],identity,token,operation[7:],int(index),int(offset),int(saved_length),digest,require_geometry=True))
            permit=self.viewer_chunks.admit_chunk(match[1],identity,token,operation,int(index),int(offset),length,digest,require_geometry=True)
            if permit['state']=='finished':return Response(200,{'state':'finished'})
            parts=[];total=0
            for part in chunks:
                total+=len(part)
                if total>length:raise ValueError('chunk overflow')
                parts.append(part)
            if total!=length:raise ValueError('truncated chunk')
            receipt=self.viewer_chunks.write_admitted(match[1],identity,token,match[2],permit['permit'],int(index),int(offset),b''.join(parts))
            return Response(200,{'state':'finished','receipt':receipt})
        except Exception:
            if permit and permit.get('state')=='active':
                self.viewer_chunks.abandon_admission(match[1],match[2],permit.get('permit'))
            return self._error(409,'chunk_requires_inspection')

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
        if grant is None:
            raise Conflict("transfer grant not ready")
        try:
            validate_grant(grant, job=job)
        except ValueError:
            raise Conflict("persisted transfer grant is unavailable") from None
        return grant

    def _next(self, identity):
        job = self.store.next_for_node(identity['node_id'])
        if job is None:
            return {'job': None, 'execution': None}
        # Discovery remains advisory. Read the immutable submission and bind it
        # to the same current global head within one SQLite read snapshot.
        with self.store.queue._db() as db:
            db.execute('BEGIN')
            head = db.execute("SELECT id FROM jobs WHERE status='queued' ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END,seq LIMIT 1").fetchone()
            if (not head or head['id'] != job['jobId']
                    or db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone()
                    or self.store.queue._node(db, identity['node_id'])['state'] not in ('active', 'scheduled')):
                raise Conflict('advisory queue head changed')
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
        identity = self._authenticate(headers)
        if identity is None:
            return self._error(401, "unauthorized")
        try:
            if method == 'POST' and path == PREFIX + '/viewer-claims':
                if self.viewer_claims is None:return self._error(404,'not_found')
                return Response(200,self.viewer_claims.claim(protocol.decode_submission(body),identity))
            enrollment=re.fullmatch(re.escape(PREFIX)+r'/viewer-claims/([a-f0-9-]{36})/enrollment',path) if type(path) is str else None
            if method=='POST' and enrollment:
                if self.viewer_claims is None:return self._error(404,'not_found')
                token=self._header(headers,'X-Claim-Token')
                return Response(200,self.viewer_claims.enroll(enrollment[1],protocol.decode_submission(body),identity,token))
            grant=re.fullmatch(re.escape(PREFIX)+r'/viewer-claims/([a-f0-9-]{36})/execution-grant',path) if type(path) is str else None
            if method=='POST' and grant:
                if self.viewer_claims is None:return self._error(404,'not_found')
                return Response(200,self.viewer_claims.execution_grant(grant[1],protocol.decode_submission(body),identity,
                                                                      self._header(headers,'X-Claim-Token')))
            metadata=re.fullmatch(re.escape(PREFIX)+r'/viewer-claims/([a-f0-9-]{36})/result-metadata',path) if type(path) is str else None
            if method=='POST' and metadata:
                if self.viewer_claims is None:return self._error(404,'not_found')
                return Response(200,self.viewer_claims.result_metadata(metadata[1],protocol.decode_submission(body),identity,
                                                                      self._header(headers,'X-Claim-Token')))
            completion=re.fullmatch(re.escape(PREFIX)+r'/viewer-claims/([a-f0-9-]{36})/completion-attestation',path) if type(path) is str else None
            if method=='POST' and completion:
                if self.viewer_claims is None or self.viewer_spool is None:return self._error(404,'not_found')
                token=self._header(headers,'X-Claim-Token');payload=protocol.decode_submission(body)
                state=self.viewer_spool.lookup(completion[1],identity,token)
                if state.get('state')!='sealed':raise Conflict('spool unavailable')
                return Response(200,self.viewer_claims.completion_attestation(completion[1],payload,identity,token,state['receipt']))
            census=re.fullmatch(re.escape(PREFIX)+r'/viewer-claims/([a-f0-9-]{36})/restart-evidence',path) if type(path) is str else None
            if method=='GET' and census:
                if self.viewer_restart_census is None:return self._error(404,'not_found')
                token=self._header(headers,'X-Claim-Token')
                try:return Response(200,self.viewer_restart_census.report(census[1],identity,token))
                except Exception:return self._error(409,'restart_evidence_unavailable')
            archive_status=re.fullmatch(re.escape(PREFIX)+r'/viewer-claims/([a-f0-9-]{36})/archive-receipt',path) if type(path) is str else None
            if method=='GET' and archive_status:
                if self.viewer_spool is None:return self._error(404,'not_found')
                if body:return self._error(400,'invalid_request')
                return Response(200,self.viewer_spool.lookup(archive_status[1],identity,self._header(headers,'X-Claim-Token')))
            fence=re.fullmatch(re.escape(PREFIX)+r'/viewer-claims/([a-f0-9-]{36})/(mutation-permit|mutation-finish|mutation-hold)',path) if type(path) is str else None
            if method=='POST' and fence:
                if self.viewer_claims is None:return self._error(404,'not_found')
                payload=protocol.decode_submission(body);token=self._header(headers,'X-Claim-Token')
                if fence[2]=='mutation-hold':
                    protocol._closed(payload,(), 'mutation hold')
                    return Response(200,self.viewer_claims.mutation_hold(fence[1],identity,token))
                operation=self.viewer_claims.mutation_permit if fence[2]=='mutation-permit' else self.viewer_claims.mutation_finish
                return Response(200,operation(fence[1],payload,identity,token))
            plan=re.fullmatch(re.escape(PREFIX)+r'/viewer-claims/([a-f0-9-]{36})/plan',path) if type(path) is str else None
            if method=='GET' and plan:
                if self.viewer_claims is None:return self._error(404,'not_found')
                if body:return self._error(400,'invalid_request')
                return Response(200,self.viewer_claims.plan(plan[1],identity,self._header(headers,'X-Claim-Token')))
            inventory=re.fullmatch(re.escape(PREFIX)+r'/viewer-claims/([a-f0-9-]{36})/inventory/(0|[1-9][0-9]{0,5})',path) if type(path) is str else None
            if method=='GET' and inventory:
                if self.viewer_claims is None:return self._error(404,'not_found')
                if body:return self._error(400,'invalid_request')
                token=self._header(headers,'X-Claim-Token')
                return Response(200,self.viewer_claims.inventory_page(inventory[1],int(inventory[2]),identity,token))
            source=re.fullmatch(re.escape(PREFIX)+r'/viewer-claims/([a-f0-9-]{36})/objects/([a-f0-9]{64})',path) if type(path) is str else None
            if method=='GET' and source:
                if self.viewer_claims is None:return self._error(404,'not_found')
                if body:return self._error(400,'invalid_request')
                token=self._header(headers,'X-Claim-Token')
                return self.viewer_claims.input_object(source[1],source[2],identity,token)
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
