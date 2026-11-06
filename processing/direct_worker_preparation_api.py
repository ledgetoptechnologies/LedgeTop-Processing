"""mTLS-bound worker source preparation adapter, disabled by fleet policy.

Only the private listener calls this method with its certificate-derived scope.
No Viewer custody, direct upload, or engine execution is authorized here.
"""
from . import reference_protocol as p
from .reference_api import Response
from .direct_worker_prepare_v2 import prepare_signed_job


class DirectPreparationAPI:
    def __init__(self, *, driver, ledger, worker, mount_path, mount_id, marker_name,
                 source_keys, signer, issuer_id, key_id):
        self.driver,self.ledger,self.worker=driver,ledger,dict(worker)
        self.mount_path,self.mount_id,self.marker_name=mount_path,mount_id,marker_name
        self.source_keys,self.signer,self.issuer_id,self.key_id=source_keys,signer,issuer_id,key_id

    def _prepare_from_listener(self, scope, raw):
        try:
            if type(scope) is not tuple or len(scope)!=2:raise ValueError()
            principal,environment=scope;p._slug(principal,'coordinator principal');p._environment(environment)
            payload=p.decode_submission(raw,max_bytes=16*1024*1024)
            p._closed(payload,('source','item','challengeId'),'worker preparation request')
            p._uuid(payload['challengeId'],'challenge')
            # Policy originates from the authenticated coordinator response,
            # never from a request header or caller-provided policy field.
            policy=self.driver.direct_source_policy()
            if policy['directPreparationEnabled'] is not True:
                return Response(503,{'error':'direct_preparation_disabled'})
            source=payload['source'];item=payload['item']
            viewer_principal=source['sourceAttestation']['proof']['principalId']
            options=dict(authenticated_principal=viewer_principal,environment=environment,
                capability_fingerprint=policy['capabilityFingerprint'],options_fingerprint=policy['optionsFingerprint'],
                allowed_odm_options=set(policy['allowedOdmOptions']),trusted_source_keys=self.source_keys,
                authority_id=self.worker['authorityId'],available_engines=('odm',))
            _,_,proof=prepare_signed_job(self.ledger,payload=source,item=item,principal=principal,
                worker=self.worker,engine_id=self.driver.worker.engine_id,
                coordinator_origin=self.driver.worker.origin,
                gate_generation=self.driver.worker.gate_generation,source_verification=options,
                mount_path=self.mount_path,mount_id=self.mount_id,marker_name=self.marker_name,
                challenge_id=payload['challengeId'],issuer_id=self.issuer_id,key_id=self.key_id,private_key=self.signer)
            return Response(200,proof)
        except Exception:
            # The gate or readiness pin may already exist after any retry.
            return Response(503,{'error':'worker_preparation_uncertain'})
