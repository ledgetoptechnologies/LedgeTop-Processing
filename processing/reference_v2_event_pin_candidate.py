"""Disabled coordinator-owned intent pin; not Viewer event authentication."""
import hashlib
import json
import re
from datetime import datetime
from . import reference_protocol as p
from .reference_store import ReferenceStore
from . import reference_v2_envelope_candidate as envelope
from . import reference_v2_attempt_candidate as attempts

QUALIFIED=False
SIGNED_QUALIFIED=False
DDL=('CREATE TABLE v2_event_pins (operation TEXT PRIMARY KEY, job_id TEXT NOT NULL UNIQUE, '
     'claim_sha TEXT NOT NULL, claim_binding TEXT NOT NULL, event_bytes BLOB NOT NULL, '
     'event_sha TEXT NOT NULL, local_identity TEXT NOT NULL, local_identity_sha TEXT NOT NULL, signed_binding TEXT NOT NULL, state TEXT NOT NULL CHECK(state=\'held\'))')
LOCAL_IDENTITY_DOMAIN=b'ltds-v2-local-claim-observation-experiment-v1\0'
CLAIM_BINDING_DOMAIN=b'ltds-v2-claim-binding-experiment-v1\0'
EVENT_SIGNATURE_DOMAIN=b'ltds-viewer-ledger-event-envelope-experiment-v3\0'
TRIGGERS={
 'v2_pin_record_update':"CREATE TRIGGER v2_pin_record_update BEFORE UPDATE ON v2_event_pins BEGIN SELECT RAISE(ABORT,'v2 event pin immutable'); END",
 'v2_pin_record_delete':"CREATE TRIGGER v2_pin_record_delete BEFORE DELETE ON v2_event_pins BEGIN SELECT RAISE(ABORT,'v2 event pin immutable'); END",
 'v2_pin_record_replace':"CREATE TRIGGER v2_pin_record_replace BEFORE INSERT ON v2_event_pins WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE operation=NEW.operation OR job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'v2 event pin immutable'); END",
 'v2_pin_job_insert':"CREATE TRIGGER v2_pin_job_insert BEFORE INSERT ON jobs WHEN EXISTS (SELECT 1 FROM v2_event_pins p JOIN jobs j ON j.id=p.job_id WHERE p.job_id=NEW.id OR j.seq=NEW.seq OR j.request_id=NEW.request_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_job_update':"CREATE TRIGGER v2_pin_job_update BEFORE UPDATE ON jobs WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE job_id=OLD.id OR job_id=NEW.id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_job_delete':"CREATE TRIGGER v2_pin_job_delete BEFORE DELETE ON jobs WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE job_id=OLD.id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_reference_update':"CREATE TRIGGER v2_pin_reference_update BEFORE UPDATE ON reference_jobs WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE job_id=OLD.job_id OR job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_reference_insert':"CREATE TRIGGER v2_pin_reference_insert BEFORE INSERT ON reference_jobs WHEN EXISTS (SELECT 1 FROM v2_event_pins p JOIN reference_jobs j ON j.job_id=p.job_id WHERE p.job_id=NEW.job_id OR j.submission_id=NEW.submission_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_reference_delete':"CREATE TRIGGER v2_pin_reference_delete BEFORE DELETE ON reference_jobs WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE job_id=OLD.job_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_claim_update':"CREATE TRIGGER v2_pin_claim_update BEFORE UPDATE ON reference_claims WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE job_id=OLD.job_id OR job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_claim_delete':"CREATE TRIGGER v2_pin_claim_delete BEFORE DELETE ON reference_claims WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE job_id=OLD.job_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_claim_insert':"CREATE TRIGGER v2_pin_claim_insert BEFORE INSERT ON reference_claims WHEN EXISTS (SELECT 1 FROM v2_event_pins p JOIN reference_claims c ON c.job_id=p.job_id WHERE p.job_id=NEW.job_id OR c.request_id=NEW.request_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
}
class PinHeld(ValueError):pass

REFERENCE_DDL={
 'reference_jobs':'''CREATE TABLE reference_jobs (
                  job_id TEXT PRIMARY KEY REFERENCES jobs(id), submission_id TEXT UNIQUE NOT NULL,
                  request_digest TEXT NOT NULL, environment TEXT NOT NULL, input_json TEXT NOT NULL,
                  manifest_sha TEXT NOT NULL, pipeline_digest TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 1,
                  phase TEXT NOT NULL, node_id TEXT, transfer_json TEXT, result_json TEXT, error TEXT,
                  created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)''',
 'reference_claims':'''CREATE TABLE reference_claims (
                  request_id TEXT PRIMARY KEY, job_id TEXT UNIQUE NOT NULL REFERENCES reference_jobs(job_id),
                  node_id TEXT NOT NULL, worker_id TEXT NOT NULL, instance_id TEXT NOT NULL,
                  token TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)'''}

def _unique_schema(db):
    from .transfer_fence import JOBS_DDL
    for table,sql in dict(REFERENCE_DDL,jobs=JOBS_DDL).items():
        actual=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()
        if actual is None or actual[0]!=sql:raise PinHeld('source schema changed')
    expected={'jobs':{('id',),('request_id',)},'reference_jobs':{('job_id',),('submission_id',)},'reference_claims':{('request_id',),('job_id',)}}
    for table,keys in expected.items():
        indexes=db.execute('PRAGMA index_list('+table+')').fetchall()
        observed=set()
        for row in indexes:
            if row[2]:
                if row[4]:raise PinHeld('partial unique index unsupported')
                name=row[1]
                if not isinstance(name,str) or not name.replace('_','').isalnum():raise PinHeld('invalid index name')
                columns=tuple(v[2] for v in db.execute('PRAGMA index_info("'+name+'")'))
                if None in columns or columns in observed:raise PinHeld('unexpected unique index')
                observed.add(columns)
        if observed!=keys:raise PinHeld('source unique schema changed')

def install_offline(store,*,enabled=False):
    if QUALIFIED is not True or enabled is not True or type(store) is not ReferenceStore:raise PinHeld('unqualified')
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone():raise PinHeld('offline queue required')
        _unique_schema(db)
        db.execute(DDL)
        for sql in TRIGGERS.values():db.execute(sql)

def attest(db):
    _unique_schema(db)
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='v2_event_pins'").fetchone()
    if row is None or row[0]!=DDL:raise PinHeld('pin schema missing')
    for name,sql in TRIGGERS.items():
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(name,)).fetchone()
        if row is None or row[0]!=sql:raise PinHeld('pin trigger missing')

def pin(store,*,claim,event_raw,_signed_pair=None):
    """Persist an authenticated existing claim's held intent, not event authority."""
    if QUALIFIED is not True or type(store) is not ReferenceStore or type(claim) is not dict or set(claim)!={'jobId','nodeId','workerId','workerInstanceId','claimToken'}:raise PinHeld('unqualified claim')
    if type(event_raw) is not bytes or not 0<len(event_raw)<=4096:raise PinHeld('invalid event')
    try:
        event=json.loads(event_raw.decode('ascii'))
        if (type(event) is not dict or set(event)!={'domain','ledger','epoch','operation','previousRevision','previousDigest','nextRevision','nextDigest'}
                or envelope._canonical(event)!=event_raw):raise ValueError()
        for field in ('domain','ledger','epoch','operation'):
            if type(event[field]) is not str or not envelope.ID.fullmatch(event[field]):raise ValueError()
        for field in ('previousDigest','nextDigest'):
            if type(event[field]) is not str or not envelope.HEX.fullmatch(event[field]):raise ValueError()
        if type(event['previousRevision']) is not int or type(event['nextRevision']) is not int or not 0<=event['previousRevision']<2**53-1 or event['nextRevision']!=event['previousRevision']+1:raise ValueError()
    except Exception:raise PinHeld('invalid event') from None
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE');attest(db)
        snapshot=store._claim_snapshot_uncached(db,claim['jobId'],node_id=claim['nodeId'],worker_id=claim['workerId'],worker_instance_id=claim['workerInstanceId'],claim_token=claim['claimToken'])
        if snapshot['status']!='running' or snapshot['executionPhase'] not in ('fetching','processing'):raise PinHeld('running claim required')
        try:attempts.attest(db)
        except attempts.AttemptHeld:raise PinHeld('attempt guard missing') from None
        attempt=db.execute('SELECT request_id,attempt_id,claim_sha,identity FROM v2_attempts WHERE job_id=?',(claim['jobId'],)).fetchone()
        expected_attempt=dict(schemaVersion=1,scope='local_attempt_not_authority',**{k:snapshot[k] for k in ('authorityId','jobId','generation','environment','nodeId','workerId','workerInstanceId','claimRequestId','manifestSha256','pipelineDigest')})
        if attempt is None or attempt['claim_sha']!=snapshot['claimTokenSha256'] or attempt['identity']!=p.canonical_json(expected_attempt).decode():raise PinHeld('attempt binding changed')
        p._slug(attempt['request_id'],'attempt request');p._uuid(attempt['attempt_id'],'attempt')
        raw_transfer=db.execute('SELECT transfer_json,input_json FROM reference_jobs WHERE job_id=?',(claim['jobId'],)).fetchone()
        try:
            transfer=json.loads(raw_transfer[0]);submission=json.loads(raw_transfer[1])
            p._closed(transfer,('reservationId','generation','expiresAt','manifestSha256','byteBudget','nonce'),'transfer request')
            for field in ('reservationId','nonce'):p._slug(transfer[field],field)
            p._integer(transfer['generation'],'generation');p._integer(transfer['byteBudget'],'byteBudget');p._hash(transfer['manifestSha256'],'manifest')
            expiry=transfer['expiresAt']
            if type(expiry) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z',expiry):raise ValueError()
            datetime.fromisoformat(expiry.replace('Z','+00:00'))
            total=submission['manifest']['totalBytes'];p._integer(total,'total')
            if ((transfer['generation'],transfer['manifestSha256'])!=(snapshot['generation'],snapshot['manifestSha256'])
                    or not total<=transfer['byteBudget']<=min(p.MAX_SAFE_INTEGER,3*total)):raise ValueError()
            reservation=transfer['reservationId']
        except Exception:raise PinHeld('reserved claim required') from None
        if _signed_pair is not None:
            if SIGNED_QUALIFIED is not True or type(_signed_pair) is not dict or set(_signed_pair)!={'intentRaw','intentSignature','receiptRaw','receiptSignature','registry','expectedIntent','expectedReceipt','eventBody','eventEnvelopeRaw','eventSignature','admissionRaw','policyDomain','now'}:
                raise PinHeld('signed reservation is unqualified')
            from . import reference_v2_reservation_candidate as signed
            try:
                proof=signed.verify_pair(_signed_pair['intentRaw'],_signed_pair['intentSignature'],_signed_pair['receiptRaw'],_signed_pair['receiptSignature'],registry=_signed_pair['registry'],expected_intent=_signed_pair['expectedIntent'],expected_receipt=_signed_pair['expectedReceipt'],now=_signed_pair['now'])
                intent=_signed_pair['expectedIntent'];receipt=_signed_pair['expectedReceipt']
                if type(_signed_pair['policyDomain']) is not str or event['domain']!=_signed_pair['policyDomain'] or not envelope.ID.fullmatch(_signed_pair['policyDomain']):raise ValueError('checkpoint domain')
                # Epoch is still caller-provided; this is an experimental
                # digest, not proof of a current independently anchored epoch.
                claim_binding=dict(identity=expected_attempt,attemptId=attempt['attempt_id'],authorityEpoch=intent['coordinatorEpoch'])
                claim_binding_sha=hashlib.sha256(CLAIM_BINDING_DOMAIN+p.canonical_json(claim_binding)).hexdigest()
                if ((intent['jobId'],intent['generation'],intent['environment'],intent['attemptId'],intent['inputManifestSha256'],intent['coordinatorAuthorityId'],intent['byteBudget'],intent['objectCount'],intent['totalSourceBytes'],receipt['reservationId']) !=
                        (snapshot['jobId'],snapshot['generation'],snapshot['environment'],attempt['attempt_id'],snapshot['manifestSha256'],snapshot['authorityId'],transfer['byteBudget'],len(submission['manifest']['objects']),total,reservation)):
                    raise ValueError('claim mismatch')
                if intent['claimBindingSha256']!=claim_binding_sha:raise ValueError('claim binding mismatch')
                from . import reference_v2_event_transition_candidate as transition
                body_raw=_signed_pair['eventBody'];derived=transition.calculate_receipt_bound(event['previousDigest'],body_raw);body=json.loads(body_raw)
                envelope_raw=_signed_pair['eventEnvelopeRaw']
                if type(envelope_raw) is not bytes or not 0<len(envelope_raw)<=4096:raise ValueError('event envelope')
                event_envelope=json.loads(envelope_raw.decode('ascii'))
                fields={'version','issuer','keyId','audience','operation','issuedAt','expiresAt','nonce','domain','ledger','epoch','eventOperation','previousRevision','previousDigest','nextRevision','nextDigest','bodySha256','bodyBytes','requestId','requestSha256','deliveryId','claimBindingSha256','receiptBodyDigest'}
                if type(event_envelope) is not dict or set(event_envelope)!=fields or p.canonical_json(event_envelope)!=envelope_raw or type(event_envelope['version']) is not int or event_envelope['version']!=1:raise ValueError('event envelope')
                registry_value=_signed_pair['registry'];key=registry_value.entries.get((event_envelope['issuer'],event_envelope['keyId'],'observation'))
                now=_signed_pair['now']
                if (type(registry_value) is not envelope.Registry or event_envelope['issuer']!=receipt['issuer'] or key is None or key.revoked
                        or (key.audience,key.operation,event_envelope['audience'],event_envelope['operation'])!=(intent['coordinatorDeploymentId'],'ledger-event',intent['coordinatorDeploymentId'],'ledger-event')
                        or type(now) is not int or type(event_envelope['issuedAt']) is not int or type(event_envelope['expiresAt']) is not int
                        or not key.not_before<=event_envelope['issuedAt']<=now<event_envelope['expiresAt']<=key.not_after
                        or event_envelope['expiresAt']-event_envelope['issuedAt']>300):raise ValueError('event key')
                envelope._bytes64(event_envelope['nonce'],32)
                expected_envelope=dict(domain=event['domain'],ledger=event['ledger'],epoch=event['epoch'],eventOperation=event['operation'],previousRevision=event['previousRevision'],previousDigest=event['previousDigest'],nextRevision=event['nextRevision'],nextDigest=event['nextDigest'],bodySha256=hashlib.sha256(body_raw).hexdigest(),bodyBytes=len(body_raw),requestId=body['requestId'],requestSha256=body['requestSha256'],deliveryId=body['deliveryId'],claimBindingSha256=body['claimLineageSha256'],receiptBodyDigest=body['receiptBodyDigest'])
                if any(type(event_envelope[k]) is not type(v) or event_envelope[k]!=v for k,v in expected_envelope.items()):raise ValueError('event binding')
                from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
                Ed25519PublicKey.from_public_bytes(key.public_key).verify(envelope._bytes64(_signed_pair['eventSignature'],64),EVENT_SIGNATURE_DOMAIN+envelope_raw)
                expected_event=dict(operationId=event['operation'],ledgerId=receipt['viewerLedgerId'],ledgerEpoch=receipt['viewerLedgerEpoch'],previousRevision=event['previousRevision'],nextRevision=event['nextRevision'],jobId=snapshot['jobId'],generation=snapshot['generation'],environment=snapshot['environment'],workerInstanceId=snapshot['workerInstanceId'],attemptId=attempt['attempt_id'],inputManifestSha256=snapshot['manifestSha256'],claimLineageSha256=claim_binding_sha,authorityEpoch=intent['coordinatorEpoch'],trustDomain=intent['trustDomain'],receiptId=receipt['receiptId'],reservationId=receipt['reservationId'],intentDigest=receipt['intentDigest'],receiptBodyDigest=hashlib.sha256(transition.RECEIPT_DIGEST_DOMAIN+_signed_pair['receiptRaw']).hexdigest())
                if ((event['ledger'],event['epoch'])!=(receipt['viewerLedgerId'],receipt['viewerLedgerEpoch']) or derived!=event['nextDigest'] or any(body[k]!=v or type(body[k]) is not type(v) for k,v in expected_event.items())):raise ValueError('event/receipt mismatch')
                objects=[obj for obj in submission['manifest']['objects'] if obj['id']==body['objectId']]
                if len(objects)!=1 or (objects[0]['sha256'],objects[0]['byteSize'])!=(body['objectSha256'],body['objectLength']):raise ValueError('event object mismatch')
                from . import reference_transfer_v2_contract_candidate as request_contract
                admission,request_sha=request_contract.parse_admission(_signed_pair['admissionRaw']);request_binding=admission['binding']
                if ((body['requestId'],body['deliveryId'],body['requestSha256'])!=(admission['requestId'],admission['deliveryId'],request_sha)
                        or any(request_binding[k]!=v for k,v in dict(jobId=snapshot['jobId'],attemptId=attempt['attempt_id'],generation=snapshot['generation'],environment=snapshot['environment'],manifestSha256=snapshot['manifestSha256'],reservationId=reservation,workerInstanceId=snapshot['workerInstanceId'],claimLineageSha256=claim_binding_sha,authorityId=snapshot['authorityId'],epoch=intent['coordinatorEpoch'],objectId=body['objectId'],objectLength=body['objectLength'],objectSha256=body['objectSha256'],range=body['range']).items())):
                    raise ValueError('admission binding mismatch')
            except Exception:raise PinHeld('signed reservation mismatch') from None
        else:proof=None
        local_identity=dict(schemaVersion=1,scope='local_observation_not_authority',reservationId=reservation,attemptId=attempt['attempt_id'],signedReservation=proof,
            **{k:snapshot[k] for k in ('authorityId','jobId','generation','environment','nodeId','workerId','workerInstanceId','claimRequestId','manifestSha256','pipelineDigest')})
        local_bytes=p.canonical_json(local_identity);local_sha=hashlib.sha256(LOCAL_IDENTITY_DOMAIN+local_bytes).hexdigest()
        binding=p.canonical_json(snapshot).decode('utf-8');digest=hashlib.sha256(event_raw).hexdigest()
        signed_binding=p.canonical_json(None if _signed_pair is None else {k:(_signed_pair[k].decode('ascii') if k.endswith('Raw') or k=='eventBody' else _signed_pair[k]) for k in ('intentRaw','intentSignature','receiptRaw','receiptSignature','eventBody','eventEnvelopeRaw','eventSignature','admissionRaw')}).decode()
        prior=db.execute('SELECT job_id,claim_sha,claim_binding,event_bytes,event_sha,local_identity,local_identity_sha,signed_binding,state FROM v2_event_pins WHERE operation=?',(event['operation'],)).fetchone()
        expected=(claim['jobId'],snapshot['claimTokenSha256'],binding,event_raw,digest,local_bytes.decode(),local_sha,signed_binding,'held')
        if prior is None:
            if db.execute('SELECT 1 FROM v2_event_pins WHERE job_id=?',(claim['jobId'],)).fetchone():raise PinHeld('parent already pinned')
            db.execute('INSERT INTO v2_event_pins VALUES(?,?,?,?,?,?,?,?,?,?)',(event['operation'],*expected))
        elif tuple(prior)!=expected:raise PinHeld('conflicting pinned event')
        return dict(operation=event['operation'],eventSha256=digest,localIdentitySha256=local_sha,state='held',authorization='unqualified')
