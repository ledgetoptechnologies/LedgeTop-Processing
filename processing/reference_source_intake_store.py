"""Durable source intake and verified-spool operations for ReferenceStore."""
import hashlib
import hmac
import json
import math
import secrets
import time
import uuid
from datetime import datetime, timezone

from .maintenance import Conflict
from . import reference_protocol as protocol

PROFILE = "reference-pipeline-v1"

_SOURCE_INTAKE_FIELDS = {"schemaVersion", "submissionId", "providerTaskId", "sourceManifest",
                         "viewerTaskId", "derivedManifest", "manifest", "pipeline", "capabilityFingerprint",
                         "sourceAttestation",
                         "environment", "requestDigest", "manifestSha256", "sourceManifestSha256",
                         "derivedManifestSha256", "pipelineDigest"}


def _json(value):
    return protocol.canonical_json(value).decode("utf-8")


class ReferenceSourceIntakeMethods:
    def submit_source_intake(self, validated, *, capability_fingerprint,
                             options_fingerprint, allowed_odm_options,
                             source_spool, trusted_source_keys, authority_id,
                             available_engines=('odm', 'treedar')):
        """Atomically queue a v2 source envelope in a non-claimable state.

        This records metadata only. A separate verified spool writer must
        publish every object and durable receipt before a future finalizer can
        mark either closure accepted. Until then this job remains unclaimable.
        """
        from .reference_source_intake import validate_source_intake
        if type(validated) is not dict or set(validated) != _SOURCE_INTAKE_FIELDS:
            raise ValueError('expected a normalized source-intake submission')
        raw = {key: validated[key] for key in (
            'schemaVersion', 'submissionId', 'providerTaskId', 'viewerTaskId', 'sourceManifest',
            'derivedManifest', 'manifest', 'pipeline', 'capabilityFingerprint', 'sourceAttestation')}
        checked = validate_source_intake(raw, environment=validated['environment'],
            capability_fingerprint=capability_fingerprint,
            options_fingerprint=options_fingerprint,
            allowed_odm_options=allowed_odm_options, available_engines=available_engines,
            trusted_source_keys=trusted_source_keys, authority_id=authority_id)
        if _json(checked) != _json({key: validated[key] for key in checked}):
            raise ValueError('normalized source-intake identity or digest mismatch')
        if source_spool is None or not callable(getattr(source_spool, 'check_capacity', None)):
            raise ValueError('configured source spool required for source-intake admission')
        pipeline_raw = {key: raw[key] for key in (
            'schemaVersion', 'submissionId', 'manifest', 'pipeline', 'capabilityFingerprint')}
        pipeline_raw['schemaVersion'] = 1
        from .reference_limits import V2_SOURCE_INTAKE_BYTES
        submission = protocol.validate_submission(pipeline_raw,
            environment=validated['environment'],
            capability_fingerprint=capability_fingerprint,
            options_fingerprint=options_fingerprint,
            allowed_odm_options=allowed_odm_options, available_engines=available_engines,
            max_body_bytes=V2_SOURCE_INTAKE_BYTES)
        from .reference_source_intake_canonical import pipeline_digest as v2_pipeline_digest
        submission['pipelineDigest'] = v2_pipeline_digest(submission['pipeline'])
        if submission['pipelineDigest'] != validated['pipelineDigest']:
            raise Conflict('source-intake pipeline digest changed')
        submission['sourceIntakeVersion'] = 2
        encoded = _json(submission)
        source_encoded = _json(validated['sourceManifest'])
        derived_encoded = _json(validated['derivedManifest'])
        reserved_bytes = validated['sourceManifest']['totalBytes'] + sum(
            item['byteSize'] for item in validated['derivedManifest']['objects'])
        protocol._integer(reserved_bytes, 'source spool reservation', allow_zero=True)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('''SELECT r.job_id,r.environment,s.request_digest
                FROM reference_jobs r LEFT JOIN reference_source_intakes s ON s.job_id=r.job_id
                WHERE r.submission_id=?''', (submission['submissionId'],)).fetchone()
            if old:
                if (old['environment'] != submission['environment']
                        or old['request_digest'] != validated['requestDigest']):
                    raise Conflict('submissionId belongs to a different reference request')
                return self._public(self._row(db, old['job_id'])), False
            pending = db.execute("SELECT COUNT(*) FROM reference_jobs WHERE phase NOT IN ('completed','cancelled')").fetchone()[0]
            if pending >= self.max_queued_jobs:
                raise Conflict('reference pending job quota reached')
            existing_reservations = db.execute("""SELECT COALESCE(SUM(reserved_bytes),0)
                FROM reference_source_intakes""").fetchone()[0]
            source_spool.check_capacity(reserved_bytes, reserved_bytes=existing_reservations)
            self._quota(db, len(encoded.encode('utf-8')) + len(source_encoded.encode('utf-8'))
                        + len(derived_encoded.encode('utf-8')))
            job_id = validated['providerTaskId'] or str(uuid.uuid4())
            if db.execute('SELECT 1 FROM jobs WHERE id=?', (job_id,)).fetchone():
                raise Conflict('providerTaskId already belongs to another queue job')
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,?,?,'queued')",
                (job_id, 'reference:' + submission['submissionId'], submission['environment'], PROFILE))
            db.execute("""INSERT INTO reference_jobs(job_id,submission_id,request_digest,environment,input_json,
                manifest_sha,pipeline_digest,phase) VALUES(?,?,?,?,?,?,?,'queued')""",
                (job_id, submission['submissionId'], submission['requestDigest'], submission['environment'],
                 encoded, submission['manifestSha256'], submission['pipelineDigest']))
            db.execute("""INSERT INTO reference_source_intakes(job_id,request_digest,viewer_task_id,provider_task_id,
                source_attestation_json,source_attestation_sha,source_manifest_json,
                derived_manifest_json,source_manifest_sha,derived_manifest_sha,pipeline_manifest_sha,reserved_bytes,
                source_state,derived_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'receiving','receiving')""",
                (job_id, validated['requestDigest'], validated['viewerTaskId'], validated['providerTaskId'],
                 _json(validated['sourceAttestation']), validated['sourceAttestation']['attestationSha256'],
                 source_encoded, derived_encoded,
                 validated['sourceManifestSha256'], validated['derivedManifestSha256'],
                 validated['manifestSha256'], reserved_bytes))
            return self._public(self._row(db, job_id)), True

    def source_capacity(self, source_spool):
        """Return current admission limits with existing byte reservations deducted."""
        if source_spool is None:
            return {"enabled": False}
        with self.queue._db() as db:
            reserved = db.execute('SELECT COALESCE(SUM(reserved_bytes),0) FROM reference_source_intakes').fetchone()[0]
        available = source_spool.available_bytes()
        maximum = max(0, available - source_spool.safety_floor_bytes - reserved)
        from .reference_limits import V2_SOURCE_INTAKE_BYTES
        return {"enabled": True, "maxObjects": protocol.MAX_OBJECTS,
            "maxManifestBytes": V2_SOURCE_INTAKE_BYTES,
            "maxObjectBytes": source_spool.max_object_bytes,
            "maxSourceBytes": maximum, "chunkBytes": source_spool.chunk_bytes}

    def has_source_intakes(self):
        with self.queue._db() as db:
            return db.execute('SELECT 1 FROM reference_source_intakes LIMIT 1').fetchone() is not None

    def _hold_source_intake(self, job_id):
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('''UPDATE reference_source_intakes SET source_state='held',derived_state='held',
                updated_at=CURRENT_TIMESTAMP WHERE job_id=?''', (job_id,))
            db.execute('''UPDATE jobs SET status='held',held_reason='source_receipt_held',
                updated_at=CURRENT_TIMESTAMP WHERE id=? AND profile=?''', (job_id, PROFILE))

    def reconcile_source_spool(self, source_spool):
        """Reverify files at startup, adopt exact files lacking a DB ACK, and hold drift."""
        with self.queue._db() as db:
            rows = db.execute('''SELECT r.job_id,r.generation,s.source_manifest_json,s.derived_manifest_json,
                s.source_state,s.derived_state,s.source_manifest_sha,s.derived_manifest_sha,
                s.pipeline_manifest_sha FROM reference_jobs r JOIN reference_source_intakes s
                ON s.job_id=r.job_id ORDER BY r.created_at,r.job_id''').fetchall()
            recorded = {row['job_id']: db.execute('''SELECT generation,object_id,object_kind,
                byte_size,sha256,mount_id,storage_key FROM reference_source_object_receipts
                WHERE job_id=? AND generation=?''', (row['job_id'], row['generation'])).fetchall()
                for row in rows}
        if source_spool is None:
            if rows:
                raise RuntimeError('source spool required for persisted source intakes')
            return {"jobs": 0, "adopted": 0, "held": 0}
        source_spool.audit_root({row['job_id'] for row in rows})
        adopted = held = 0
        for row in rows:
            job_id, generation = row['job_id'], row['generation']
            if row['source_state'] == 'held' or row['derived_state'] == 'held':
                held += 1
                continue
            source = json.loads(row['source_manifest_json'])
            derived = json.loads(row['derived_manifest_json'])
            objects = [{"id": obj['id'], "byteSize": obj['byteSize'], "sha256": obj['sha256']}
                       for obj in source['objects'] + derived['objects']]
            try:
                found = source_spool.audit_job(job_id=job_id, generation=generation,
                    expected_objects=objects)
                by_id = {item['object_id']: item for item in recorded[job_id]}
                if len(by_id) != len(recorded[job_id]):
                    raise Conflict('duplicate source receipt')
                if any(object_id not in found for object_id in by_id):
                    raise Conflict('durable source receipt has no exact file')
                for object_id, receipt in found.items():
                    old = by_id.get(object_id)
                    if old is not None and (old['generation'] != receipt['generation']
                            or old['byte_size'] != receipt['byteSize'] or old['sha256'] != receipt['sha256']
                            or old['mount_id'] != receipt['mountId'] or old['storage_key'] != receipt['storageKey']):
                        raise Conflict('durable source receipt changed')
                    if old is None:
                        self.record_source_object_receipt(job_id, receipt, source_spool=source_spool)
                        adopted += 1
                expected_count = len(objects)
                if (row['source_state'] == 'accepted' or row['derived_state'] == 'accepted') and len(found) != expected_count:
                    raise Conflict('retained source closure is incomplete')
            except Exception as error:
                from .source_spool import SourceSpoolError
                if isinstance(error, SourceSpoolError) and error.code in (
                        'spool_mount_unavailable', 'spool_mount_changed'):
                    raise RuntimeError('source spool mount unavailable during reconciliation') from None
                self._hold_source_intake(job_id)
                held += 1
        return {"jobs": len(rows), "adopted": adopted, "held": held}

    def source_object_expectation(self, job_id, *, generation, object_id):
        """Return one closed manifest object for a streaming source PUT."""
        protocol._integer(generation, 'generation')
        protocol._slug(object_id, 'source object ID')
        with self.queue._db() as db:
            row = self._row(db, job_id)
            if row['generation'] != generation:
                raise Conflict('source generation changed')
            if row['source_state'] != 'receiving' or row['derived_state'] != 'receiving':
                raise Conflict('source intake is not receiving')
            source = json.loads(row['source_manifest_json'])
            derived = json.loads(row['derived_manifest_json'])
            matches = [(obj, 'source') for obj in source['objects'] if obj['id'] == object_id]
            matches.extend((obj, 'derived') for obj in derived['objects'] if obj['id'] == object_id)
            if len(matches) != 1:
                raise Conflict('unknown source object')
            obj, kind = matches[0]
            total_reserved = db.execute('SELECT COALESCE(SUM(reserved_bytes),0) '
                'FROM reference_source_intakes').fetchone()[0]
            already_received = db.execute('''SELECT 1 FROM reference_source_object_receipts
                WHERE job_id=? AND generation=? AND object_id=?''',
                (job_id, generation, object_id)).fetchone() is not None
            current_reservation = 0 if already_received else obj['byteSize']
            if not already_received and row['reserved_bytes'] < current_reservation:
                raise Conflict('source byte reservation is unavailable')
            if total_reserved < current_reservation:
                raise Conflict('source reservation ledger is inconsistent')
            return {"jobId": job_id, "generation": generation, "objectId": object_id,
                    "objectKind": kind, "byteSize": obj['byteSize'], "sha256": obj['sha256'],
                    "environment": row['environment'],
                    "otherReservedBytes": total_reserved - current_reservation}

    def record_source_object_receipt(self, job_id, receipt, *, source_spool):
        """Persist a spool receipt after re-opening and rehashing its object."""
        fields = {"schemaVersion", "jobId", "generation", "objectId", "storageKey",
                  "byteSize", "sha256", "mountId"}
        if (type(receipt) is not dict or set(receipt) != fields
                or type(receipt['schemaVersion']) is not int or receipt['schemaVersion'] != 1):
            raise ValueError('invalid source object receipt')
        protocol._slug(receipt['jobId'], 'job ID')
        if receipt['jobId'] != job_id:
            raise Conflict('source receipt job binding changed')
        protocol._integer(receipt['generation'], 'generation')
        protocol._slug(receipt['objectId'], 'source object ID')
        protocol._integer(receipt['byteSize'], 'source object size', allow_zero=True)
        protocol._hash(receipt['sha256'], 'source object digest')
        if (source_spool is None or receipt['mountId'] != source_spool.mount_id
                or receipt['storageKey'] != source_spool.storage_key(
                    receipt['jobId'], receipt['generation'], receipt['objectId'])):
            raise Conflict('source storage binding changed')
        source_spool.verify_object(job_id=receipt['jobId'], generation=receipt['generation'],
            object_id=receipt['objectId'], byte_size=receipt['byteSize'], sha256=receipt['sha256'])
        encoded = _json(receipt)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, job_id)
            expected_matches = []
            for obj in json.loads(row['source_manifest_json'])['objects']:
                if obj['id'] == receipt['objectId']:
                    expected_matches.append((obj, 'source'))
            for obj in json.loads(row['derived_manifest_json'])['objects']:
                if obj['id'] == receipt['objectId']:
                    expected_matches.append((obj, 'derived'))
            if (row['generation'] != receipt['generation'] or row['source_state'] != 'receiving'
                    or row['derived_state'] != 'receiving' or len(expected_matches) != 1):
                raise Conflict('source receipt is stale or outside intake closure')
            expected, kind = expected_matches[0]
            if (expected['byteSize'], expected['sha256']) != (receipt['byteSize'], receipt['sha256']):
                raise Conflict('source receipt differs from intake manifest')
            existing = db.execute('''SELECT receipt_json FROM reference_source_object_receipts
                WHERE job_id=? AND generation=? AND object_id=?''',
                (job_id, receipt['generation'], receipt['objectId'])).fetchone()
            if existing:
                if existing['receipt_json'] != encoded:
                    raise Conflict('source object identity has a different durable receipt')
                return False
            if row['reserved_bytes'] < receipt['byteSize']:
                raise Conflict('source byte reservation is unavailable')
            self._quota(db, len(encoded.encode('utf-8')))
            db.execute('''INSERT INTO reference_source_object_receipts(job_id,generation,object_id,
                object_kind,storage_key,byte_size,sha256,mount_id,receipt_json)
                VALUES(?,?,?,?,?,?,?,?,?)''', (job_id, receipt['generation'], receipt['objectId'],
                kind, receipt['storageKey'], receipt['byteSize'], receipt['sha256'], receipt['mountId'], encoded))
            db.execute('''UPDATE reference_source_intakes SET reserved_bytes=reserved_bytes-?,
                updated_at=CURRENT_TIMESTAMP WHERE job_id=?''', (receipt['byteSize'], job_id))
            return True

    def complete_source_intake(self, job_id, *, generation, source_manifest_sha256,
                               derived_manifest_sha256, pipeline_manifest_sha256, source_spool):
        """Retain both closed manifests only after every persisted object re-verifies."""
        protocol._integer(generation, 'generation')
        for digest in (source_manifest_sha256, derived_manifest_sha256, pipeline_manifest_sha256):
            protocol._hash(digest, 'source completion digest')
        if source_spool is None:
            raise ValueError('configured source spool required')
        with self.queue._db() as db:
            row = self._row(db, job_id)
            if row['generation'] != generation:
                raise Conflict('source generation changed')
            if (row['source_manifest_sha'] != source_manifest_sha256
                    or row['derived_manifest_sha'] != derived_manifest_sha256
                    or row['pipeline_manifest_sha'] != pipeline_manifest_sha256):
                raise Conflict('source completion manifest binding changed')
            if row['source_state'] == 'accepted' and row['derived_state'] == 'accepted':
                source = json.loads(row['source_manifest_json'])
                derived = json.loads(row['derived_manifest_json'])
            elif row['source_state'] != 'receiving' or row['derived_state'] != 'receiving':
                raise Conflict('source intake is not receiving')
            else:
                source = json.loads(row['source_manifest_json'])
                derived = json.loads(row['derived_manifest_json'])
            expected = [(obj, 'source') for obj in source['objects']]
            expected.extend((obj, 'derived') for obj in derived['objects'])
            receipts = db.execute('''SELECT * FROM reference_source_object_receipts
                WHERE job_id=? AND generation=? ORDER BY object_kind,object_id''',
                (job_id, generation)).fetchall()
            if len(receipts) != len(expected) or row['reserved_bytes'] != 0:
                raise Conflict('source intake is incomplete')
            by_id = {receipt['object_id']: receipt for receipt in receipts}
            if len(by_id) != len(receipts):
                raise Conflict('source object receipt set is invalid')
            verified = []
            for obj, kind in expected:
                receipt_row = by_id.get(obj['id'])
                if (receipt_row is None or receipt_row['object_kind'] != kind
                        or receipt_row['byte_size'] != obj['byteSize']
                        or receipt_row['sha256'] != obj['sha256']
                        or receipt_row['mount_id'] != source_spool.mount_id):
                    raise Conflict('source receipt set differs from manifest')
                receipt = json.loads(receipt_row['receipt_json'])
                expected_receipt = source_spool.verify_object(job_id=job_id,
                    generation=generation, object_id=obj['id'], byte_size=obj['byteSize'],
                    sha256=obj['sha256'])
                if _json(receipt) != _json(expected_receipt) or receipt_row['storage_key'] != expected_receipt['storageKey']:
                    raise Conflict('source object receipt no longer verifies')
                verified.append(receipt['objectId'])
        from datetime import datetime, timezone
        retained_at = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        source_receipt = {
            'schemaVersion': 2, 'receiptType': 'source_retention',
            'receiptId': str(uuid.uuid4()), 'storageReceiptId': str(uuid.uuid4()),
            'jobId': job_id, 'attemptId': row['submission_id'], 'generation': generation,
            'environment': row['environment'], 'pipelineManifestSha256': pipeline_manifest_sha256,
            'sourceManifestSha256': source_manifest_sha256, 'pipelineDigest': row['pipeline_digest'],
            'objects': [{key: obj[key] for key in ('id', 'byteSize', 'sha256', 'sourceType',
                'retentionClass', 'retentionHoldId')} for obj in source['objects']],
            'retainedAt': retained_at,
            'retentionGuarantee': 'until_explicit_source_release_v1'}
        derived_receipt = {
            'schemaVersion': 1, 'receiptType': 'attempt_derived_input',
            'receiptId': str(uuid.uuid4()), 'jobId': job_id, 'attemptId': row['submission_id'],
            'generation': generation, 'environment': row['environment'],
            'derivedManifestSha256': derived_manifest_sha256,
            'objects': [{key: obj[key] for key in ('id', 'name', 'byteSize', 'sha256',
                'snapshotAttemptId', 'snapshotContentSha256', 'snapshotEvidenceSha256',
                'sourceType', 'retentionHoldId', 'retentionClass')} for obj in derived['objects']],
            'retainedAt': retained_at}
        source_receipt_json, derived_receipt_json = _json(source_receipt), _json(derived_receipt)
        source_receipt_sha, derived_receipt_sha = protocol.digest(source_receipt), protocol.digest(derived_receipt)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, job_id)
            if (row['generation'] != generation or row['source_state'] not in ('receiving', 'accepted')
                    or row['derived_state'] not in ('receiving', 'accepted') or row['reserved_bytes'] != 0):
                raise Conflict('source intake changed during verification')
            receipt_count = db.execute('''SELECT COUNT(*) FROM reference_source_object_receipts
                WHERE job_id=? AND generation=?''', (job_id, generation)).fetchone()[0]
            if receipt_count != len(verified):
                raise Conflict('source receipt set changed during verification')
            if row['source_receipt_json'] or row['derived_receipt_json']:
                if (row['source_receipt_json'] != source_receipt_json
                        or row['derived_receipt_json'] != derived_receipt_json):
                    # The first completion response may have been lost. Its exact
                    # persisted receipts, not newly generated IDs/timestamps, win.
                    if (row['source_state'] == 'accepted' and row['derived_state'] == 'accepted'
                            and row['source_receipt_json'] and row['derived_receipt_json']):
                        return self._record(row, False), False
                    raise Conflict('source completion receipt binding changed')
                return self._record(row, False), False
            self._quota(db, len(source_receipt_json.encode('utf-8'))
                + len(derived_receipt_json.encode('utf-8')))
            db.execute('''UPDATE reference_source_intakes SET source_state='accepted',
                derived_state='accepted',source_receipt_json=?,source_receipt_sha=?,
                derived_receipt_json=?,derived_receipt_sha=?,updated_at=CURRENT_TIMESTAMP WHERE job_id=?''',
                (source_receipt_json, source_receipt_sha, derived_receipt_json, derived_receipt_sha, job_id))
            result = self._record(self._row(db, job_id), False)
        return result, True

    def mint_source_transfer_grant(self, job_id, *, source_spool, clock=None):
        """Seal a claim-bound broker grant only after durable source intake acceptance."""
        from datetime import datetime
        from .reference_transfer import validate_grant
        read_clock = time.time if clock is None else clock
        previous_clock = None

        def checked_now():
            nonlocal previous_clock
            value = read_clock()
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value)):
                raise Conflict('invalid source grant clock')
            if previous_clock is not None and value < previous_clock:
                raise Conflict('source grant clock moved backwards')
            previous_clock = value
            return value

        def capture(db):
            row = self._row(db, job_id)
            claim = db.execute('''SELECT request_id,job_id,node_id,worker_id,instance_id,token
                FROM reference_claims WHERE job_id=?''', (job_id,)).fetchone()
            receipts = db.execute('''SELECT generation,object_id,object_kind,byte_size,sha256,mount_id,storage_key,receipt_json
                FROM reference_source_object_receipts WHERE job_id=? ORDER BY generation,object_id''',
                (job_id,)).fetchall()
            grant_row = db.execute('SELECT grant_json,digest FROM reference_transfer_grants WHERE job_id=?',
                                   (job_id,)).fetchone()
            token_rows = db.execute('''SELECT token_sha256,job_id,generation FROM reference_source_grant_tokens
                WHERE job_id=? ORDER BY token_sha256''', (job_id,)).fetchall()
            claim_value = dict(claim) if claim is not None else None
            snapshot = {
                'row': {key: row[key] for key in (
                    'job_id','submission_id','request_digest','environment','input_json','manifest_sha',
                    'pipeline_digest','generation','phase','node_id','transfer_json','status','claim_hash',
                    'held_reason','private_claim_token','source_request_digest','viewer_task_id','provider_task_id',
                    'source_attestation_json','source_attestation_sha','source_manifest_json','derived_manifest_json',
                    'source_manifest_sha','derived_manifest_sha','pipeline_manifest_sha','source_state',
                    'derived_state','reserved_bytes','source_receipt_json','source_receipt_sha',
                    'derived_receipt_json','derived_receipt_sha')},
                'claim': claim_value,
                'receipts': [dict(item) for item in receipts],
                'grant': dict(grant_row) if grant_row is not None else None,
                'tokens': [dict(item) for item in token_rows],
            }
            return row, claim_value, snapshot

        # Read a consistent, immutable metadata snapshot briefly. In particular,
        # leave Queue._db (and its lifecycle lock) before touching the NAS.
        with self.queue._db() as db:
            db.execute('BEGIN')
            row, claim, snapshot = capture(db)
            db.commit()

        now = checked_now()
        if snapshot['grant'] is None and snapshot['tokens']:
            raise Conflict('orphan source grant token history')
        if row['status'] != 'running' or row['phase'] != 'awaiting_transfer':
            raise Conflict('source grant requires a running transfer reservation')
        if (row['source_state'] != 'accepted' or row['derived_state'] != 'accepted'
                or not row['source_receipt_json'] or not row['derived_receipt_json']):
            raise Conflict('source retention receipts are not accepted')
        if (claim is None or claim['token'] != row['private_claim_token']
                or not row['claim_hash']
                or not hmac.compare_digest(hashlib.sha256(claim['token'].encode('utf-8')).hexdigest(), row['claim_hash'])):
            raise Conflict('active source grant claim is unavailable')
        request = json.loads(row['transfer_json']) if row['transfer_json'] else None
        if not isinstance(request, dict):
            raise Conflict('worker reservation is unavailable')
        try:
            expiry = datetime.fromisoformat(request['expiresAt'].replace('Z', '+00:00')).timestamp()
        except (KeyError, ValueError, AttributeError, OverflowError, OSError):
            raise Conflict('worker reservation expiry is invalid') from None
        if not math.isfinite(expiry) or expiry <= now:
            raise Conflict('worker reservation expired')
        source_receipt = json.loads(row['source_receipt_json'])
        derived_receipt = json.loads(row['derived_receipt_json'])
        if (type(source_receipt) is not dict or type(derived_receipt) is not dict
                or protocol.digest(source_receipt) != row['source_receipt_sha']
                or protocol.digest(derived_receipt) != row['derived_receipt_sha']
                or source_receipt.get('receiptType') != 'source_retention'
                or derived_receipt.get('receiptType') != 'attempt_derived_input'
                or (source_receipt.get('jobId'), source_receipt.get('attemptId'),
                    source_receipt.get('generation'), source_receipt.get('environment'),
                    source_receipt.get('pipelineManifestSha256'), source_receipt.get('sourceManifestSha256')) !=
                   (job_id, row['submission_id'], row['generation'], row['environment'],
                    row['pipeline_manifest_sha'], row['source_manifest_sha'])
                or (derived_receipt.get('jobId'), derived_receipt.get('attemptId'),
                    derived_receipt.get('generation'), derived_receipt.get('environment'),
                    derived_receipt.get('derivedManifestSha256')) !=
                   (job_id, row['submission_id'], row['generation'], row['environment'],
                    row['derived_manifest_sha'])):
            raise Conflict('source retention receipt binding changed')
        try:
            source = json.loads(row['source_manifest_json'])
            derived = json.loads(row['derived_manifest_json'])
            submission = json.loads(row['input_json'])
            objects = submission['manifest']['objects']
            expected = {item['id']: (item, 'source') for item in source['objects']}
            expected.update({item['id']: (item, 'derived') for item in derived['objects']})
            receipt_rows = snapshot['receipts']
            if len(receipt_rows) != len(expected):
                raise Conflict('accepted source receipt closure is incomplete')
            by_id = {item['object_id']: item for item in receipt_rows
                     if item['generation'] == row['generation']}
            if len(by_id) != len(receipt_rows) or set(by_id) != set(expected):
                raise Conflict('accepted source receipt closure changed')
            for obj in objects:
                stored = by_id.get(obj['id'])
                retained = expected.get(obj['id'])
                if (stored is None or retained is None
                        or (stored['byte_size'], stored['sha256'], stored['object_kind']) !=
                           (obj['byteSize'], obj['sha256'], retained[1])
                        or stored['mount_id'] != source_spool.mount_id
                        or stored['storage_key'] != source_spool.storage_key(
                            job_id, row['generation'], obj['id'])):
                    raise Conflict('pipeline source receipt binding changed')
                receipt = json.loads(stored['receipt_json'])
                actual = source_spool.verify_object(job_id=job_id, generation=row['generation'],
                    object_id=obj['id'], byte_size=obj['byteSize'], sha256=obj['sha256'])
                if _json(actual) != _json(receipt):
                    raise Conflict('pipeline source object is no longer retained')
        except (KeyError, TypeError, ValueError):
            raise Conflict('source transfer grant could not be validated') from None

        verified_at = checked_now()
        if expiry <= verified_at:
            raise Conflict('worker reservation expired during source verification')

        # Hashing has finished. Take the write lock, re-read every authority and
        # receipt binding, then mint or reconcile only against this exact state.
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            current, current_claim, current_snapshot = capture(db)
            before_cas = {key: value for key, value in snapshot.items() if key not in ('grant', 'tokens')}
            after_cas = {key: value for key, value in current_snapshot.items() if key not in ('grant', 'tokens')}
            if _json(after_cas) != _json(before_cas):
                raise Conflict('source grant authority changed during verification')
            if (current['status'] != 'running' or current['phase'] != 'awaiting_transfer'
                    or current_claim is None or current_claim['token'] != current['private_claim_token']
                    or current['generation'] != row['generation']
                    or current['transfer_json'] != row['transfer_json']):
                raise Conflict('source grant reservation changed during verification')
            if current_snapshot['grant'] is None and current_snapshot['tokens']:
                raise Conflict('orphan source grant token history')
            # Another identical verifier may have won the CAS while this thread
            # was hashing. Return only that exact persisted, still-live grant.
            if current_snapshot['grant'] is not None:
                persisted = json.loads(current_snapshot['grant']['grant_json'])
                token_hash = hashlib.sha256(persisted['token'].encode('ascii')).hexdigest()
                if (current_snapshot['grant']['digest'] != protocol.digest(persisted)
                        or len(current_snapshot['tokens']) != 1
                        or current_snapshot['tokens'][0] != {
                            'token_sha256': token_hash, 'job_id': job_id,
                            'generation': current['generation']}
                        or any(persisted.get(key) != request.get(key) for key in (
                            'reservationId','generation','expiresAt','manifestSha256','byteBudget','nonce'))
                        or persisted.get('jobId') != job_id
                        or persisted.get('environment') != current['environment']
                        or persisted.get('objectIds') != [item['id'] for item in objects]):
                    raise Conflict('persisted source grant binding changed')
                commit_now = checked_now()
                validate_grant(persisted, job=self._record(current, True), clock=lambda: commit_now)
                return persisted
            commit_now = checked_now()
            if commit_now >= expiry:
                raise Conflict('worker reservation expired during source verification')
            grant = {key: request[key] for key in
                     ('reservationId', 'generation', 'expiresAt', 'manifestSha256', 'byteBudget', 'nonce')}
            grant.update(jobId=job_id, environment=current['environment'],
                         objectIds=[item['id'] for item in objects], token=secrets.token_urlsafe(32))
            validate_grant(grant, job=self._record(current, True), clock=lambda: commit_now)
            encoded = _json(grant)
            digest = protocol.digest(grant)
            token_sha = hashlib.sha256(grant['token'].encode('ascii')).hexdigest()
            self._quota(db, len(encoded.encode('utf-8')))
            db.execute('INSERT INTO reference_transfer_grants VALUES(?,?,?)', (job_id, encoded, digest))
            db.execute('INSERT INTO reference_source_grant_tokens(token_sha256,job_id,generation) VALUES(?,?,?)',
                       (token_sha, job_id, current['generation']))
            return grant

    def source_object_for_grant(self, token, object_id, *, source_spool, clock=None):
        """Authorize one exact pipeline input and return its already verified file handle."""
        from .reference_transfer import validate_grant
        if type(token) is not str or not token:
            raise Conflict('source grant unavailable')
        token_sha = hashlib.sha256(token.encode('utf-8')).hexdigest()
        with self.queue._db() as db:
            row = db.execute('''SELECT job_id,generation FROM reference_source_grant_tokens
                WHERE token_sha256=?''', (token_sha,)).fetchone()
            if row is None:
                raise Conflict('source grant unavailable')
            job = self._row(db, row['job_id'])
            grant_row = db.execute('SELECT grant_json FROM reference_transfer_grants WHERE job_id=?',
                                    (row['job_id'],)).fetchone()
            if grant_row is None:
                raise Conflict('source grant unavailable')
            grant = json.loads(grant_row['grant_json'])
            private = self._record(job, True)
            if (row['generation'] != job['generation'] or job['status'] != 'running'
                    or job['phase'] not in ('fetching', 'processing')
                    or job['source_state'] != 'accepted' or job['derived_state'] != 'accepted'):
                raise Conflict('source grant execution is not active')
            if (type(grant) is not dict or type(grant.get('token')) is not str
                    or not hmac.compare_digest(token, grant['token'])):
                raise Conflict('source grant unavailable')
            checked = validate_grant(grant, job=private, clock=clock or time.time)
            item = next((entry for entry in checked.objects if entry.id == object_id), None)
            if item is None:
                raise Conflict('source object outside pipeline grant')
            source_rows = db.execute('''SELECT object_id,object_kind,byte_size,sha256,mount_id,storage_key
                FROM reference_source_object_receipts WHERE job_id=? AND generation=? AND object_id=?''',
                (row['job_id'], row['generation'], object_id)).fetchall()
            if len(source_rows) != 1:
                raise Conflict('source object receipt unavailable')
            receipt = source_rows[0]
            if (receipt['byte_size'], receipt['sha256'], receipt['mount_id']) != (
                    item.byte_size, item.sha256, source_spool.mount_id):
                raise Conflict('source object receipt changed')
            return source_spool.open_verified_object(job_id=row['job_id'], generation=row['generation'],
                object_id=object_id, byte_size=item.byte_size, sha256=item.sha256)


