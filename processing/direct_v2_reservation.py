"""Unwired V2 coordinator node reservation; never a custody acknowledgement."""
import json
import time
from types import MappingProxyType
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from . import direct_v2_inventory, direct_worker_fence_proof_v2
from . import reference_protocol as p
from .maintenance import Conflict
from .source_intake_mode import require as require_mode


class V2ReservationUncertain(RuntimeError):
    """A reservation may have committed and requires exact reconciliation."""


class DirectV2Reservations:
    def __init__(self, store, *, trusted_workers):
        self.store,self.queue=store,store.queue
        if type(trusted_workers) is not dict or set(trusted_workers)!=set(self.queue.node_ids):
            raise ValueError('complete pinned V2 worker inventory required')
        pinned={}
        for node,value in trusted_workers.items():
            p._closed(value,('identity','issuerId','keyId','publicKey'),'pinned V2 worker')
            identity=value['identity'];p._closed(identity,('authorityId','nodeId','workerId','workerInstanceId'),'worker identity')
            p._require(identity['authorityId']==self.queue.authority_id and identity['nodeId']==node,
                       'pinned V2 worker authority changed')
            for key in ('nodeId','workerId','workerInstanceId'):p._slug(identity[key],key)
            p._slug(value['issuerId'],'issuer');p._slug(value['keyId'],'key')
            if not isinstance(value['publicKey'],Ed25519PublicKey):raise ValueError('pinned V2 worker key required')
            pinned[node]=(dict(identity),value['issuerId'],value['keyId'],value['publicKey'])
        self._workers=MappingProxyType(pinned)
        with self.queue.coordinator_operation('source-io-admission'),self.queue._db() as db:
            from .direct_v2_reservation_schema import ensure_schema
            db.execute('BEGIN IMMEDIATE')
            ensure_schema(db)

    def reserve(self, **request):
        """Classify any possibly committed failure as uncertain, never release."""
        try:
            return self._reserve(**request)
        except V2ReservationUncertain:
            raise
        except Exception as error:
            item=request.get('item')
            if type(item) is not dict or type(item.get('reservationId')) is not str or type(item.get('jobId')) is not str:
                raise
            try:
                with self.queue._db() as db:
                    saved=db.execute('SELECT 1 FROM direct_v2_reservations WHERE reservation_id=? OR job_id=?',
                                     (item['reservationId'],item['jobId'])).fetchone()
            except Exception:
                raise V2ReservationUncertain('V2 reservation outcome requires reconciliation') from error
            if saved:
                raise V2ReservationUncertain('V2 reservation outcome requires reconciliation') from error
            raise

    def _reserve(self, *, payload, authenticated_principal, item, expected_worker,
                readiness_proof, challenge_id, gate_generation, issuer_id, key_id, worker_public_key,
                trusted_source_keys, capability_fingerprint, options_fingerprint, allowed_odm_options):
        """Hold an eligible node for this exact verified V2 source and worker proof."""
        from .direct_worker_scratch_v2 import identity
        if self.store._source_routing_keys is None:
            raise Conflict('protected source routing trust unavailable')
        # Use only the immutable registry loaded with the store; request
        # arguments must never substitute routing/signing authority.
        trusted_source_keys=self.store._source_routing_keys
        item,_,_=identity(item,expected_worker=expected_worker)
        pinned=self._workers.get(item['nodeId'])
        def key_bytes(key):
            return key.public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)
        if (pinned is None or pinned[0]!=expected_worker or pinned[1]!=issuer_id or pinned[2]!=key_id
                or not isinstance(worker_public_key,Ed25519PublicKey)
                or key_bytes(pinned[3])!=key_bytes(worker_public_key)):
            raise Conflict('V2 worker identity or key is not pinned')
        checked=direct_v2_inventory.prepare(payload,authenticated_principal=authenticated_principal,
            environment=item['environment'],capability_fingerprint=capability_fingerprint,
            options_fingerprint=options_fingerprint,allowed_odm_options=allowed_odm_options,
            trusted_source_keys=trusted_source_keys,authority_id=self.queue.authority_id)
        reservation={key:item[key] for key in ('reservationId','jobId','attemptId','generation',
            'authorityId','environment','nodeId','workerId','workerInstanceId')}
        expected_keys=('authorityId','principalId','environment','submissionId','viewerTaskId','providerTaskId',
            'sourceManifestSha256','derivedManifestSha256','pipelineManifestSha256','pipelineDigest',
            'attestationSha256','sourceSnapshotSha256','preparedEnvelopeSha256','activationId')
        expected={key:checked[key] for key in expected_keys}
        direct_v2_inventory.prepare_worker_index(checked,expected_binding=expected,reservation_binding=reservation,
            authenticated_principal=authenticated_principal,environment=item['environment'],
            capability_fingerprint=capability_fingerprint,options_fingerprint=options_fingerprint,
            allowed_odm_options=allowed_odm_options,trusted_source_keys=trusted_source_keys,authority_id=self.queue.authority_id)
        if (any(item[key]!=checked[key] for key in ('sourceManifestSha256','derivedManifestSha256',
                'pipelineManifestSha256','inventorySha256')) or item['objectCount']!=sum(
                len(checked[key]['objects']) for key in ('sourceManifest','derivedManifest'))
                or item['inputBytes']!=sum(obj['byteSize'] for key in ('sourceManifest','derivedManifest')
                                         for obj in checked[key]['objects'])):
            raise Conflict('V2 reservation inventory changed')
        mono=time.monotonic();wall=time.time()
        proof_args=dict(expected_item=item,expected_worker=expected_worker,challenge_id=challenge_id,
            gate_generation=gate_generation,issuer_id=issuer_id,key_id=key_id,public_key=worker_public_key)
        proof=direct_worker_fence_proof_v2.verify(readiness_proof,now=int(wall),**proof_args)
        deadline=mono+max(0,proof['expiresAt']-wall)
        request_digest=p.digest(payload);item_bytes=p.canonical_json(item);proof_bytes=p.canonical_json(readiness_proof)
        from .reference_source_intake_canonical import pipeline_digest as v2_pipeline_digest
        from .reference_limits import V2_SOURCE_INTAKE_BYTES
        pipeline_payload={key:payload[key] for key in ('submissionId','manifest','pipeline','capabilityFingerprint')}
        pipeline_payload['schemaVersion']=1
        normalized=p.validate_submission(pipeline_payload,environment=item['environment'],
            capability_fingerprint=capability_fingerprint,options_fingerprint=options_fingerprint,
            allowed_odm_options=allowed_odm_options,available_engines=('odm','treedar'),
            max_body_bytes=V2_SOURCE_INTAKE_BYTES)
        normalized['pipelineDigest']=v2_pipeline_digest(normalized['pipeline'])
        normalized['sourceIntakeVersion']=2
        if len(item_bytes)>8192 or len(proof_bytes)>8192:raise Conflict('V2 reservation metadata exceeds bounds')
        with self.queue.coordinator_operation('source-io-admission'),self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            from .direct_v2_reservation_schema import verify_fence,verify_schema,validate_receiving
            verify_fence(db);verify_schema(db)
            if time.monotonic()>=deadline:raise Conflict('V2 worker readiness expired')
            direct_worker_fence_proof_v2.verify(readiness_proof,now=int(time.time()),**proof_args)
            require_mode(db,item['jobId'],'direct-v2')
            row=self.store._row(db,item['jobId'])
            source=db.execute('''SELECT request_digest,source_manifest_json,derived_manifest_json,source_attestation_json
                FROM reference_source_intakes WHERE job_id=?''',(item['jobId'],)).fetchone()
            if (row is None or source is None or source['request_digest']!=request_digest
                    or row['generation']!=item['generation'] or row['environment']!=item['environment']
                    or row['source_state']!='receiving' or row['derived_state']!='receiving'):
                raise Conflict('V2 direct admission changed')
            try:
                same=(p.canonical_json(json.loads(row['input_json']))==p.canonical_json(normalized)
                    and row['manifest_sha']==checked['pipelineManifestSha256']
                    and row['pipeline_digest']==checked['pipelineDigest']
                    and p.canonical_json(json.loads(source['source_manifest_json']))==p.canonical_json(payload['sourceManifest'])
                    and p.canonical_json(json.loads(source['derived_manifest_json']))==p.canonical_json(payload['derivedManifest'])
                    and p.canonical_json(json.loads(source['source_attestation_json']))==p.canonical_json(payload['sourceAttestation']))
            except (ValueError,TypeError,KeyError):same=False
            if not same:raise Conflict('V2 signed admission metadata changed')
            old=db.execute('SELECT * FROM direct_v2_reservations WHERE reservation_id=? OR job_id=?',
                           (item['reservationId'],item['jobId'])).fetchall()
            values=(item['reservationId'],item['jobId'],item['nodeId'],authenticated_principal,
                    request_digest,item_bytes,proof_bytes,challenge_id,'receiving')
            if old:
                validate_receiving(db,item['reservationId'])
                saved_node=db.execute('SELECT node_id FROM reference_jobs WHERE job_id=?',(item['jobId'],)).fetchone()
                if (len(old)!=1 or tuple(old[0])!=values or row['status']!='held'
                        or row['held_reason']!='direct_v2_receiving' or row['node_id']!=item['nodeId']
                        or saved_node is None or saved_node['node_id']!=item['nodeId']):
                    raise Conflict('V2 reservation replay changed')
                replayed=True
            else:
                if row['status']!='held' or row['held_reason']!='direct_admission_pending':
                    raise Conflict('V2 direct job not pending')
                candidate=self.store._candidate_any_row(db,item['nodeId'])
                if candidate is None or candidate['id']!=item['jobId']:
                    raise Conflict('V2 node not eligible or earlier job takes priority')
                self.store._quota(db,sum(len(value) for value in (item_bytes,proof_bytes))+
                                  sum(len(value.encode()) for value in values[:5]+values[7:]))
                if time.monotonic()>=deadline:raise Conflict('V2 worker readiness expired')
                direct_worker_fence_proof_v2.verify(readiness_proof,now=int(time.time()),**proof_args)
                try:db.execute('INSERT INTO direct_v2_reservations VALUES(?,?,?,?,?,?,?,?,?)',values)
                except Exception as error:
                    import sqlite3
                    if isinstance(error,sqlite3.IntegrityError):raise Conflict('V2 node or job already reserved') from None
                    raise
                changed=db.execute("UPDATE jobs SET node_id=?,held_reason='direct_v2_receiving' WHERE id=? AND status='held' AND held_reason='direct_admission_pending'",
                                   (item['nodeId'],item['jobId'])).rowcount
                if changed!=1:raise Conflict('V2 direct admission changed')
                db.execute('UPDATE reference_jobs SET node_id=? WHERE job_id=?',(item['nodeId'],item['jobId']))
                validate_receiving(db,item['reservationId'])
                replayed=False
        # The transaction has committed: expiration now is uncertain, not a
        # safe rejection or reason to release its durable reservation.
        try:
            if time.monotonic()>=deadline:raise ValueError('expired')
            direct_worker_fence_proof_v2.verify(readiness_proof,now=int(time.time()),**proof_args)
        except (ValueError,TypeError):
            raise V2ReservationUncertain('V2 reservation committed; reconcile exact state') from None
        return dict(reservationId=item['reservationId'],nodeId=item['nodeId'],state='receiving',
                    replayed=replayed,sourceCustodyAccepted=False,transferAuthorized=False)
