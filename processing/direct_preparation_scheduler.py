"""Explicit coordinator scheduling step; never source custody or execution.

Construction requires trusted local enrollment. No automatic task or listener
is started here, and incomplete or uncertain worker holds are never released.
"""
import json
import hashlib
import uuid
from . import reference_protocol as p
from .direct_preparation_client import DirectPreparationClient
from .direct_preparation_journal import prepare_once
from .direct_v2_inventory import prepare as prepare_inventory
from .direct_v2_reservation import DirectV2Reservations
from .maintenance import Conflict
from .reference_store import PROFILE


class DirectPreparationScheduler:
    def __init__(self, store, *, trusted_workers, enrollment):
        self.store,self.queue=store,store.queue
        self.reservations=DirectV2Reservations(store,trusted_workers=trusted_workers)
        if type(enrollment) is not dict or set(enrollment)!=set(self.queue.node_ids):
            raise ValueError('complete protected scheduler enrollment required')
        clients={};policies={}
        for node in self.queue.node_ids:
            entry=enrollment[node]
            p._closed(entry,('configPath','configSha256','credentialSha256','gateGeneration','outputBudget','ttlSeconds'),
                      'protected scheduler enrollment')
            p._integer(entry['gateGeneration'],'gate generation')
            p._integer(entry['outputBudget'],'output budget',allow_zero=True)
            p._integer(entry['ttlSeconds'],'reservation ttl')
            clients[node]=DirectPreparationClient(config_path=entry['configPath'],config_sha256=entry['configSha256'],
                credential_sha256=entry['credentialSha256'],node=node,reservations=self.reservations)
            policies[node]=(entry['gateGeneration'],entry['outputBudget'],entry['ttlSeconds'])
        self._clients,self._policies=clients,policies
        self._enrollment_path=None
        self._enrollment_sha=None
        self._enrollment=json.loads(json.dumps(enrollment))

    @classmethod
    def from_protected_file(cls,store,path):
        """Build only from an owner-protected local enrollment file."""
        from .direct_protected_transport import _read
        from cryptography.hazmat.primitives import serialization
        raw=_read(path,private=True,maximum=1024*1024)
        value=p.decode_submission(raw,max_bytes=1024*1024)
        p._closed(value,('schemaVersion','trustedWorkers','enrollment'),'direct scheduler enrollment')
        if type(value['schemaVersion']) is not int or value['schemaVersion']!=1 or type(value['trustedWorkers']) is not dict:
            raise ValueError('direct scheduler enrollment invalid')
        workers={}
        for node,entry in value['trustedWorkers'].items():
            p._closed(entry,('identity','issuerId','keyId','publicKeyPem'),'protected worker pin')
            workers[node]=dict(identity=entry['identity'],issuerId=entry['issuerId'],keyId=entry['keyId'],
                               publicKey=serialization.load_pem_public_key(entry['publicKeyPem'].encode('ascii')))
        scheduler=cls(store,trusted_workers=workers,enrollment=value['enrollment'])
        scheduler._enrollment_path=path;scheduler._enrollment_sha=hashlib.sha256(raw).hexdigest()
        return scheduler

    def revalidate_enrollment(self,node):
        """Reopen exact protected bytes and credentials before use."""
        from .direct_protected_transport import _read
        if self._enrollment_path is None or node not in self._clients:
            raise Conflict('protected scheduler enrollment unavailable')
        if hashlib.sha256(_read(self._enrollment_path,private=True,maximum=1024*1024)).hexdigest()!=self._enrollment_sha:
            raise Conflict('protected scheduler enrollment changed')
        entry=self._enrollment[node]
        fresh=DirectPreparationClient(config_path=entry['configPath'],config_sha256=entry['configSha256'],
            credential_sha256=entry['credentialSha256'],node=node,reservations=self.reservations)
        current=self._clients[node]
        if (fresh._worker!=current._worker or (fresh._issuer,fresh._key)!=(current._issuer,current._key)
                or (fresh._host,fresh._port,fresh._address)!=(current._host,current._port,current._address)):
            raise Conflict('protected scheduler endpoint changed')

    def policy_enabled_for(self,identity):
        try:
            node=identity['node_id'];worker=self._clients[node]._worker
            if (identity['worker_id'],identity['worker_instance_id'])!=(worker['workerId'],worker['workerInstanceId']):return False
            if self.queue._coordinator_freeze.status()['phase']!='open':return False
            self.revalidate_enrollment(node)
            return True
        except Exception:
            return False

    def run_once(self, *, capability_fingerprint, options_fingerprint, allowed_odm_options, revalidate_policy):
        """Select the first eligible pending direct job and prepare that node."""
        if not callable(revalidate_policy):raise ValueError('fresh policy check required')
        selected=None
        with self.queue.coordinator_operation('source-io-admission'),self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            for node in self.queue.node_ids:
                row=self.store._candidate_any_row(db,node)
                if row is not None and row['profile']==PROFILE:
                    job=self.store._row(db,row['id'])
                    if job['status']=='held' and job['held_reason']=='direct_admission_pending':
                        selected=(node,dict(job));break
            if selected is None:return None
        node,job=selected
        self.revalidate_enrollment(node)
        if revalidate_policy(node,capability_fingerprint,options_fingerprint,allowed_odm_options) is not True:
            raise Conflict('current worker policy changed')
        submission=json.loads(job['input_json'])
        source=json.loads(job['source_manifest_json']);derived=json.loads(job['derived_manifest_json'])
        attestation=json.loads(job['source_attestation_json'])
        payload=dict(schemaVersion=2,submissionId=submission['submissionId'],providerTaskId=job['provider_task_id'],
            viewerTaskId=job['viewer_task_id'],sourceManifest=source,derivedManifest=derived,
            manifest=submission['manifest'],pipeline=submission['pipeline'],
            capabilityFingerprint=submission['capabilityFingerprint'],sourceAttestation=attestation)
        principal=attestation['proof']['principalId']
        if (submission['capabilityFingerprint']!=capability_fingerprint
                or submission['pipeline']['odm']['optionsFingerprint']!=options_fingerprint):
            raise Conflict('current worker capability changed')
        checked=prepare_inventory(payload,authenticated_principal=principal,environment=job['environment'],
            capability_fingerprint=submission['capabilityFingerprint'],options_fingerprint=options_fingerprint,
            allowed_odm_options=allowed_odm_options,trusted_source_keys=self.store._source_routing_keys,
            authority_id=self.queue.authority_id)
        worker=self._clients[node]._worker;gate,output,ttl=self._policies[node]
        item=dict(reservationId=str(uuid.uuid4()),jobId=job['job_id'],attemptId=submission['submissionId'],
            generation=job['generation'],environment=job['environment'],**worker,
            sourceManifestSha256=checked['sourceManifestSha256'],derivedManifestSha256=checked['derivedManifestSha256'],
            pipelineManifestSha256=checked['pipelineManifestSha256'],inventorySha256=checked['inventorySha256'],
            inputBytes=sum(o['byteSize'] for k in ('sourceManifest','derivedManifest') for o in checked[k]['objects']),
            objectCount=sum(len(checked[k]['objects']) for k in ('sourceManifest','derivedManifest')),
            overheadBytesPerObject=65536,outputBudget=output,ttlSeconds=ttl)
        challenge=str(uuid.uuid4())
        prepared=prepare_once(self.reservations,self._clients[node],payload=payload,item=item,principal=principal,
            challenge_id=challenge,gate_generation=gate,capability_fingerprint=submission['capabilityFingerprint'],
            options_fingerprint=options_fingerprint,allowed_odm_options=allowed_odm_options)
        with self.queue._db() as db:
            saved=db.execute('SELECT proof_json FROM direct_preparation_intents WHERE reservation_id=? AND state=?',
                             (item['reservationId'],'proof_observed')).fetchone()
            if saved is None:raise Conflict('prepared proof unavailable; reconciliation required')
            proof=json.loads(saved['proof_json'])
        # If enrollment was rotated after worker preparation, preserve the
        # held intent and do not reserve under stale worker keys.
        self.revalidate_enrollment(node)
        if revalidate_policy(node,capability_fingerprint,options_fingerprint,allowed_odm_options) is not True:
            raise Conflict('worker policy changed; preparation remains held')
        pin=self.reservations._workers[node]
        reserved=self.reservations.reserve(payload=payload,authenticated_principal=principal,item=item,
            expected_worker=worker,readiness_proof=proof,challenge_id=challenge,gate_generation=gate,
            issuer_id=pin[1],key_id=pin[2],worker_public_key=pin[3],trusted_source_keys=self.store._source_routing_keys,
            capability_fingerprint=submission['capabilityFingerprint'],options_fingerprint=options_fingerprint,
            allowed_odm_options=allowed_odm_options)
        return {'reservationId':reserved['reservationId'],'nodeId':node,'state':'receiving',
                'sourceCustodyAccepted':False,'processingAuthorized':False}
