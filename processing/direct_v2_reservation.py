"""Unwired V2 coordinator node reservation; never a custody acknowledgement."""
import json
import time

from . import direct_v2_inventory, direct_worker_fence_proof_v2
from . import reference_protocol as p
from .maintenance import Conflict
from .source_intake_mode import require as require_mode


class DirectV2Reservations:
    def __init__(self, store):
        self.store,self.queue=store,store.queue
        with self.queue.coordinator_operation('source-io-admission'),self.queue._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS direct_v2_reservations (
                reservation_id TEXT PRIMARY KEY,job_id TEXT UNIQUE NOT NULL,node_id TEXT UNIQUE NOT NULL,
                principal TEXT NOT NULL,request_digest TEXT NOT NULL,item_json BLOB NOT NULL,
                proof_json BLOB NOT NULL,challenge_id TEXT NOT NULL,state TEXT NOT NULL)''')

    def reserve(self, *, payload, authenticated_principal, item, expected_worker,
                readiness_proof, challenge_id, gate_generation, issuer_id, key_id, worker_public_key,
                trusted_source_keys, capability_fingerprint, options_fingerprint, allowed_odm_options):
        """Hold an eligible node for this exact verified V2 source and worker proof."""
        from .direct_worker_scratch_v2 import identity
        item,_,_=identity(item,expected_worker=expected_worker)
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
        if len(item_bytes)>8192 or len(proof_bytes)>8192:raise Conflict('V2 reservation metadata exceeds bounds')
        with self.queue.coordinator_operation('source-io-admission'),self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            if time.monotonic()>=deadline:raise Conflict('V2 worker readiness expired')
            direct_worker_fence_proof_v2.verify(readiness_proof,now=int(time.time()),**proof_args)
            require_mode(db,item['jobId'],'direct-v2')
            row=self.store._row(db,item['jobId'])
            source=db.execute('SELECT request_digest FROM reference_source_intakes WHERE job_id=?',(item['jobId'],)).fetchone()
            if (row is None or source is None or source['request_digest']!=request_digest
                    or row['generation']!=item['generation'] or row['environment']!=item['environment']
                    or row['source_state']!='receiving' or row['derived_state']!='receiving'):
                raise Conflict('V2 direct admission changed')
            old=db.execute('SELECT * FROM direct_v2_reservations WHERE reservation_id=? OR job_id=?',
                           (item['reservationId'],item['jobId'])).fetchall()
            values=(item['reservationId'],item['jobId'],item['nodeId'],authenticated_principal,
                    request_digest,item_bytes,proof_bytes,challenge_id,'receiving')
            if old:
                if len(old)!=1 or tuple(old[0])!=values or row['status']!='held' or row['node_id']!=item['nodeId']:
                    raise Conflict('V2 reservation replay changed')
                return dict(reservationId=item['reservationId'],nodeId=item['nodeId'],state='receiving',
                            replayed=True,sourceCustodyAccepted=False,transferAuthorized=False)
            if row['status']!='held' or row['held_reason']!='direct_admission_pending':
                raise Conflict('V2 direct job not pending')
            candidate=self.store._candidate_any_row(db,item['nodeId'])
            if candidate is None or candidate['id']!=item['jobId']:
                raise Conflict('V2 node not eligible or earlier job takes priority')
            self.store._quota(db,sum(len(value) for value in (item_bytes,proof_bytes))+
                              sum(len(value.encode()) for value in values[:5]+values[7:]))
            try:db.execute('INSERT INTO direct_v2_reservations VALUES(?,?,?,?,?,?,?,?,?)',values)
            except Exception as error:
                import sqlite3
                if isinstance(error,sqlite3.IntegrityError):raise Conflict('V2 node or job already reserved') from None
                raise
            changed=db.execute("UPDATE jobs SET node_id=?,held_reason='direct_v2_receiving' WHERE id=? AND status='held' AND held_reason='direct_admission_pending'",
                               (item['nodeId'],item['jobId'])).rowcount
            if changed!=1:raise Conflict('V2 direct admission changed')
            db.execute('UPDATE reference_jobs SET node_id=? WHERE job_id=?',(item['nodeId'],item['jobId']))
            return dict(reservationId=item['reservationId'],nodeId=item['nodeId'],state='receiving',
                        replayed=False,sourceCustodyAccepted=False,transferAuthorized=False)
