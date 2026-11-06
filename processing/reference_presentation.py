"""Worker start witness and the public ltds-job-presentation-v1 projection.

Display only. Nothing here reads or writes phase, claim, gate, custody, budgets or
recovery, and no report lifts a hold. The one durable effect is a single
acknowledged reference_outbox row per job, which is never sent. It is historical
start evidence: it says this attempt was once seen started, not that it is
running now, so it carries no heartbeat, engine incarnation or maintenance state.

Two clocks stay apart. observedAt is the worker's own observation, kept attributed
and never published. acceptedAt is this coordinator's first durable acceptance,
and is the only time shown publicly. Neither is checked against the other, so a
report delayed by a coordinator outage is still accepted.

See docs/job-presentation-v1.md.
"""
from datetime import datetime, timezone
import hashlib
import hmac
import json
import re

from . import reference_protocol as p
from .maintenance import Conflict

PROTOCOL = 'ltds-worker-start-witness-v1'
ACK_PROTOCOL = 'ltds-worker-start-witness-ack-v1'
PRESENTATION_PROTOCOL = 'ltds-job-presentation-v1'
KIND = 'presentation_start'
IDENTITY_PREFIX = 'presentation-start:'
MAX_REQUEST_BYTES = 2048
MAX_RECORD_BYTES = 1024
MAX_OUTPUT_LINES = 8
STARTED_CODE = 20
# A start can only be reported once the coordinator has seen processing.
STARTED_PHASES = frozenset(('processing', 'waiting_for_delivery', 'awaiting_acceptance', 'completed'))
WITNESS_FIELDS = ('schemaVersion', 'protocol', 'witnessId', 'sequence', 'coordinatorAuthorityId', 'claimRequestId',
    'jobId', 'executionUuid', 'generation', 'environment', 'inputManifestSha256', 'pipelineDigest',
    'claimTokenSha256', 'gateGeneration', 'gateReceiptId', 'nativeStatusCode', 'outputLineCount',
    'outputPrefixSha256', 'observedAt')
FIELDS = WITNESS_FIELDS + ('witnessSha256',)
RECORD_FIELDS = ('witnessId', 'sequence', 'requestSha256', 'witnessSha256', 'executionUuid', 'generation',
    'claimRequestId', 'gateGeneration', 'gateReceiptId', 'workerObservedAt', 'acceptedAt')
ACK_FIELDS = ('schemaVersion', 'protocol', 'jobId', 'generation', 'witnessId', 'sequence', 'requestSha256',
    'witnessSha256', 'workerObservedAt', 'acceptedAt', 'ackSha256')
_TIME = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z\Z')
_RECEIPT = re.compile(r'[0-9a-f]{32}\Z')


def timestamp(seconds):
    """UTC milliseconds in the one format this contract allows."""
    moment = datetime.fromtimestamp(seconds, timezone.utc)
    return moment.strftime('%Y-%m-%dT%H:%M:%S.') + '%03dZ' % (moment.microsecond // 1000)


def _coordinator_now():
    return datetime.now(timezone.utc).timestamp()


def _timestamp(value, label):
    p._require(type(value) is str and _TIME.fullmatch(value) is not None, label + ' must be a UTC millisecond time')
    try:
        datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        raise p.ProtocolError(label + ' must be a real time') from None


def _gate(generation, receipt):
    p._integer(generation, 'gate generation')
    p._require(type(receipt) is str and _RECEIPT.fullmatch(receipt) is not None, 'invalid gate receipt')


def validate_request(value):
    p._closed(value, FIELDS, 'start witness')
    p._require(type(value['schemaVersion']) is int and value['schemaVersion'] == 1
        and value['protocol'] == PROTOCOL, 'unsupported start witness')
    p._uuid(value['witnessId'], 'witness ID')
    p._require(type(value['sequence']) is int and value['sequence'] == 1, 'invalid witness sequence')
    for name in ('coordinatorAuthorityId', 'claimRequestId'):
        p._slug(value[name], name)
    p._uuid(value['jobId'], 'job ID')
    p._uuid(value['executionUuid'], 'execution UUID')
    p._require(value['executionUuid'] == value['jobId'], 'execution UUID must be the job ID')
    p._integer(value['generation'], 'witness generation')
    p._environment(value['environment'])
    for name in ('inputManifestSha256', 'pipelineDigest', 'claimTokenSha256', 'outputPrefixSha256', 'witnessSha256'):
        p._hash(value[name], name)
    _gate(value['gateGeneration'], value['gateReceiptId'])
    p._require(type(value['nativeStatusCode']) is int and value['nativeStatusCode'] == STARTED_CODE,
        'a start witness needs native status 20')
    p._require(type(value['outputLineCount']) is int and 1 <= value['outputLineCount'] <= MAX_OUTPUT_LINES,
        'a start witness needs bounded output')
    _timestamp(value['observedAt'], 'observedAt')
    p._require(value['witnessSha256'] == p.digest({key: value[key] for key in WITNESS_FIELDS}),
        'start witness digest mismatch')
    return json.loads(p.canonical_json(value))


def make_request(claim, *, authority_id, claim_request_id, witness_id, observed, observed_at):
    value = dict(schemaVersion=1, protocol=PROTOCOL, witnessId=witness_id, sequence=1,
        coordinatorAuthorityId=authority_id, claimRequestId=claim_request_id, jobId=claim['jobId'],
        executionUuid=claim['jobId'], generation=claim['generation'], environment=claim['environment'],
        inputManifestSha256=claim['manifestSha256'], pipelineDigest=claim['pipelineDigest'],
        claimTokenSha256=hashlib.sha256(claim['claimToken'].encode('ascii')).hexdigest(),
        gateGeneration=observed['gateGeneration'], gateReceiptId=observed['gateReceiptId'],
        nativeStatusCode=observed['nativeStatusCode'], outputLineCount=observed['outputLineCount'],
        outputPrefixSha256=observed['outputPrefixSha256'], observedAt=observed_at)
    value['witnessSha256'] = p.digest(value)
    return validate_request(value)


def make_ack(job_id, generation, record):
    ack = dict(schemaVersion=1, protocol=ACK_PROTOCOL, jobId=job_id, generation=generation,
        witnessId=record['witnessId'], sequence=record['sequence'], requestSha256=record['requestSha256'],
        witnessSha256=record['witnessSha256'], workerObservedAt=record['workerObservedAt'],
        acceptedAt=record['acceptedAt'])
    ack['ackSha256'] = p.digest(ack)
    return ack


def validate_ack(value, request):
    p._closed(value, ACK_FIELDS, 'start witness acknowledgement')
    _timestamp(value['acceptedAt'], 'acceptedAt')
    expected = make_ack(request['jobId'], request['generation'], dict(witnessId=request['witnessId'],
        sequence=request['sequence'], requestSha256=p.digest(request), witnessSha256=request['witnessSha256'],
        workerObservedAt=request['observedAt'], acceptedAt=value['acceptedAt']))
    p._require(p.canonical_json(value) == p.canonical_json(expected), 'start witness acknowledgement binding mismatch')
    return value


def stored_record(raw, job_id, generation):
    """The validated latch for this exact attempt, else None. Never raises on bad bytes."""
    try:
        value = json.loads(raw)
        p._closed(value, RECORD_FIELDS, 'stored start witness')
        p._uuid(value['witnessId'], 'witness ID')
        p._require(type(value['sequence']) is int and value['sequence'] == 1, 'invalid witness sequence')
        for name in ('requestSha256', 'witnessSha256'):
            p._hash(value[name], name)
        p._require(value['executionUuid'] == job_id and type(value['generation']) is int
            and value['generation'] == generation, 'stored start witness names another attempt')
        p._slug(value['claimRequestId'], 'claim request ID')
        _gate(value['gateGeneration'], value['gateReceiptId'])
        _timestamp(value['workerObservedAt'], 'workerObservedAt')
        _timestamp(value['acceptedAt'], 'acceptedAt')
    except (TypeError, ValueError):
        return None
    return value


def report(store, job_id, request, *, identity, claim_token):
    """Record the first witness; exact replay writes nothing and returns one ack."""
    request = validate_request(request)
    digest = p.digest(request)
    if type(claim_token) is not str or not claim_token.isascii() or not 1 <= len(claim_token) <= 128:
        raise Conflict('start witness ownership unavailable')
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        row = store._row(db, job_id)
        claim = db.execute('SELECT * FROM reference_claims WHERE job_id=?', (job_id,)).fetchone()
        token_sha = hashlib.sha256(claim_token.encode('ascii')).hexdigest()
        if (not claim or (claim['node_id'], claim['worker_id'], claim['instance_id']) !=
                (identity['node_id'], identity['worker_id'], identity['worker_instance_id'])
                or claim['request_id'] != request['claimRequestId']
                or not hmac.compare_digest(claim['token'], claim_token)
                or type(row['claim_hash']) is not str or not hmac.compare_digest(token_sha, row['claim_hash'])):
            raise Conflict('start witness ownership mismatch')
        expected = dict(coordinatorAuthorityId=store.queue.authority_id, jobId=job_id, generation=row['generation'],
            environment=row['environment'], inputManifestSha256=row['manifest_sha'],
            pipelineDigest=row['pipeline_digest'], claimTokenSha256=token_sha)
        if any(request[key] != value for key, value in expected.items()):
            raise Conflict('start witness binding mismatch')
        identifier = IDENTITY_PREFIX + job_id
        old = db.execute('SELECT kind,payload_json,acknowledged FROM reference_outbox WHERE identity=?',
            (identifier,)).fetchone()
        if old is not None:
            record = stored_record(old['payload_json'], job_id, row['generation'])
            if (old['kind'], old['acknowledged']) != (KIND, 1) or record is None:
                raise Conflict('stored start witness unavailable')
            if record['requestSha256'] != digest:
                raise Conflict('job already has a different start witness')
            return make_ack(job_id, row['generation'], record)
        pipeline = json.loads(row['input_json'])['pipeline']
        if pipeline['odm']['requested'] is not True or pipeline['treedar']['requested'] is not False:
            raise Conflict('only an ODM job has a start witness')
        if row['phase'] not in STARTED_PHASES:
            raise Conflict('job has not reached processing')
        record = dict(witnessId=request['witnessId'], sequence=request['sequence'], requestSha256=digest,
            witnessSha256=request['witnessSha256'], executionUuid=request['executionUuid'],
            generation=request['generation'], claimRequestId=request['claimRequestId'],
            gateGeneration=request['gateGeneration'], gateReceiptId=request['gateReceiptId'],
            workerObservedAt=request['observedAt'], acceptedAt=timestamp(_coordinator_now()))
        encoded = p.canonical_json(record).decode('ascii')
        if len(encoded) > MAX_RECORD_BYTES:
            raise Conflict('start witness record exceeds its bound')
        store._quota(db, len(encoded))
        db.execute('INSERT INTO reference_outbox(identity,kind,payload_json,digest,acknowledged) VALUES(?,?,?,?,1)',
            (identifier, KIND, encoded, p.digest(dict(kind=KIND, payload=record))))
        return make_ack(job_id, row['generation'], record)


def latch(db, job_id):
    """Raw stored latch for one job. An unacknowledged or other-kind row is no latch."""
    row = db.execute('SELECT payload_json FROM reference_outbox WHERE identity=? AND kind=? AND acknowledged=1',
        (IDENTITY_PREFIX + job_id, KIND)).fetchone()
    return None if row is None else row[0]


def _state(public, witnessed):
    """First match wins: every dominating state yields null, and so does anything unknown."""
    status, phase = public['status'], public['phase']
    if status not in ('queued', 'running'):
        return None
    if public.get('queueReason') is not None or public['error'] is not None:
        return None
    if status == 'queued':
        return 'queued' if phase in ('queued', 'waiting_capacity') else None
    if phase in ('queued', 'waiting_capacity', 'awaiting_transfer', 'fetching'):
        return 'preparing'
    if phase == 'processing':
        return 'processing' if witnessed else 'preparing'
    return None


def project(public, raw):
    """The public DTO. None omits it, because a damaged latch proves nothing either way."""
    record = None
    if raw is not None:
        record = stored_record(raw, public['jobId'], public['generation'])
        if record is None:
            return None
    return dict(schemaVersion=1, protocol=PRESENTATION_PROTOCOL, state=_state(public, record is not None),
        processingConfirmedAt=record['acceptedAt'] if record is not None else None)
