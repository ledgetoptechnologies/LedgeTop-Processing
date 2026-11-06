"""Daemon-owned observation-only reconstruction of an already-consumed direct transfer."""
import json
import threading
from . import reference_protocol as p
from . import direct_worker_scratch_v2 as scratch
from . import direct_worker_inventory_retention as retention
from . import direct_v2_inventory as inventory
from .direct_worker_receiver_v2 import DirectWorkerBoundaryV2
from .direct_worker_grant_v2 import DirectWorkerGrant
from .direct_worker_http import create_direct_worker_server
from .direct_worker_service_v2 import ListenerOwner
from .direct_worker_receipt_v2 import WorkerClosureSignerV2
from .source_spool import SourceSpool

class ObservationOwner:
    """One worker's protected listener. No new writes or source ACK are enabled."""
    def __init__(self, *, ledger, worker, driver, mount_path, mount_id, marker_name,
                 signer, issuer_id, key_id, host, port, bearer, tls_context, clients):
        self.ledger,self.worker,self.driver=ledger,dict(worker),driver
        self.mount_path,self.mount_id,self.marker_name=mount_path,mount_id,marker_name
        self.signer,self.issuer_id,self.key_id=signer,issuer_id,key_id
        self.host,self.port,self.bearer,self.context=host,port,bearer,tls_context
        self.clients=dict(clients);self.lock=threading.Lock();self.owner=None;self.binding=None;self.closed=False

    def resume(self, *, item, challenge, principal, environment, verification):
        p._uuid(challenge,'challenge');p._slug(principal,'principal');p._environment(environment)
        normalized,digest,reservation=scratch.identity(item,expected_worker=self.worker)
        if normalized['environment']!=environment:raise ValueError('environment changed')
        matching={fp:scope['principal'] for fp,scope in self.clients.items()
                  if scope['principal']==principal and scope['environment']==environment}
        if not matching:raise ValueError('enrolled coordinator required')
        with self.lock:
            if self.closed:raise ValueError('listener owner closed')
            binding=(digest,challenge,principal)
            payload=retention.recover(self.ledger,item=normalized,worker=self.worker,verification=verification,
                                      coordinator_principal=principal)
            checked=inventory.prepare(payload,**verification)
            if self.owner is not None:
                if self.binding!=binding or self.owner.status()!='running':raise ValueError('listener held')
                return
            keys=('authorityId','principalId','environment','submissionId','viewerTaskId','providerTaskId',
                  'sourceManifestSha256','derivedManifestSha256','pipelineManifestSha256','pipelineDigest',
                  'attestationSha256','sourceSnapshotSha256','preparedEnvelopeSha256','activationId')
            expected={k:checked[k] for k in keys}
            reservation_binding={k:normalized[k] for k in ('reservationId','jobId','attemptId','generation',
                'authorityId','environment','nodeId','workerId','workerInstanceId')}
            index=inventory.prepare_worker_index(checked,expected_binding=expected,reservation_binding=reservation_binding,**verification)
            with self.ledger._transaction() as db:
                issue=db.execute("SELECT state,proof,reservation FROM direct_readiness_issues WHERE challenge=?",(challenge,)).fetchone()
                if not issue or issue['state']!='issued' or issue['reservation']!=reservation:raise ValueError('readiness unavailable')
                proof=json.loads(issue['proof'])
            path=self.ledger.root/'jobs'/reservation/'direct-source'
            if path.is_symlink() or not path.is_dir():raise ValueError('retained spool unavailable')
            spool=SourceSpool(path,mount_path=self.mount_path,mount_id=self.mount_id,marker_name=self.marker_name,
                              safety_floor_bytes=0,require_mount=True)
            signer=WorkerClosureSignerV2(issuer_id=self.issuer_id,key_id=self.key_id,identity=self.worker,private_key=self.signer)
            boundary=DirectWorkerBoundaryV2(ledger=self.ledger,spool=spool,inventory=index,item=normalized,
                reservation_binding=reservation_binding,expected_worker=self.worker,engine_id=self.driver.worker.engine_id,
                coordinator_origin=self.driver.worker.origin,gate_generation=self.driver.worker.gate_generation,bearer=self.bearer,
                readiness_proof=proof,challenge_id=challenge,issuer_id=self.issuer_id,key_id=self.key_id,
                public_key=self.signer.public_key(),closure_expected_binding=expected,closure_verification=verification,
                worker_signer=signer,resume_observation_only=True)
            grant=DirectWorkerGrant(boundary,coordinator_principal=principal)
            server=create_direct_worker_server(boundary,self.host,self.port,ssl_context=self.context,
                                                client_principals=matching,reservation_grant=grant)
            owner=ListenerOwner(server)
            try:owner.start()
            except BaseException:owner.close(1);raise
            self.owner,self.binding=owner,binding

    def status(self):
        with self.lock:return 'closed' if self.closed else 'running' if self.owner is None else self.owner.status()
    def close(self,timeout):
        with self.lock:
            self.closed=True
            return self.owner is None or self.owner.close(timeout)

class CombinedOwner:
    """Keep daemon ownership until both listeners finish draining."""
    def __init__(self,control,observation):self.control,self.observation=control,observation
    def start(self):self.control.start()
    def status(self):
        a,b=self.control.status(),self.observation.status()
        return 'failed' if a=='failed' or (a=='running' and b!='running') else a
    def close(self,timeout):
        return self.control.close(timeout) and self.observation.close(timeout)
