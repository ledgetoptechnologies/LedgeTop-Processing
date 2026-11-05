"""Worker status reports; a visibility overlay on jobs.status, never a release.

Only irreversible local evidence (HOLD_REASONS) sets jobs.status='held'; a job
held that way leaves only by Queue.operator. Everything transient is OBSERVING:
the job stays as it was, retries and result delivery are unchanged, and the
report is shown beside it. A coordinator_restart or operator hold is another
authority, so a report never replaces it: ClaimRecoveryService and the
restart-tolerant result path stay exactly as eligible as they were.

Uses existing columns only: jobs.status/held_reason, jobs.result for the bounded
record and reference_jobs.error for an escalated code. Reports carry allowlisted
codes, never free text or paths. Nothing here requeues, moves, resumes or
deletes a job.
"""
from datetime import datetime, timezone
import hashlib
import hmac
import json

from . import reference_protocol as p
from .maintenance import Conflict

PROTOCOL = 'ltds-worker-hold-v1'
ACK_PROTOCOL = 'ltds-worker-hold-ack-v1'
HOLD_REASONS = frozenset(('engine_init_uncertain', 'reservation_expired', 'result_over_budget'))
OBSERVING_REASONS = frozenset(('engine_start_refused', 'engine_reconciliation_required', 'engine_task_missing',
    'node_lost', 'scratch_capacity_unavailable', 'source_transfer_unavailable', 'worker_reconciliation_required'))
# An operator ended the job while this node's physical fence is still live.
TERMINAL_REASONS = frozenset(('physical_reservation_retained',))
REASONS = HOLD_REASONS | OBSERVING_REASONS | TERMINAL_REASONS
OBSERVATIONS = frozenset(('engine_task_present', 'engine_task_missing', 'engine_unreachable'))
ENGINE_STATUS_CODES = frozenset((10, 20, 30, 40, 50))
FIELDS = ('schemaVersion', 'protocol', 'holdId', 'sequence', 'coordinatorAuthorityId', 'jobId', 'generation',
    'environment', 'inputManifestSha256', 'pipelineDigest', 'claimRequestId', 'reasonCode', 'observation',
    'engineStatusCode')
ACK_FIELDS = ('schemaVersion', 'protocol', 'jobId', 'generation', 'holdId', 'sequence', 'requestSha256',
    'reasonCode', 'heldAt', 'ackSha256')
RECORD_FIELDS = ('holdId', 'sequence', 'requestSha256', 'reasonCode', 'observation', 'engineStatusCode', 'heldAt')


def validate_request(value):
    p._closed(value, FIELDS, 'worker hold')
    p._require(type(value['schemaVersion']) is int and value['schemaVersion'] == 1
        and value['protocol'] == PROTOCOL, 'unsupported worker hold')
    p._uuid(value['holdId'], 'hold ID')
    p._require(type(value['sequence']) is int and 0 <= value['sequence'] <= p.MAX_SAFE_INTEGER, 'invalid hold sequence')
    for name in ('coordinatorAuthorityId', 'jobId', 'claimRequestId'):
        p._slug(value[name], name)
    p._integer(value['generation'], 'hold generation')
    p._environment(value['environment'])
    for name in ('inputManifestSha256', 'pipelineDigest'):
        p._hash(value[name], name)
    observation, code = value['observation'], value['engineStatusCode']
    # A null reason retires an observation; it can carry nothing else.
    p._require(type(value['reasonCode']) is str and value['reasonCode'] in REASONS
        or value['reasonCode'] is None and observation is None, 'unknown hold reason')
    p._require(observation is None or type(observation) is str and observation in OBSERVATIONS, 'unknown hold observation')
    p._require(code is None or type(code) is int and code in ENGINE_STATUS_CODES and observation == 'engine_task_present',
        'invalid engine status observation')
    return json.loads(p.canonical_json(value))


def make_request(claim, *, authority_id, claim_request_id, hold):
    return validate_request(dict(schemaVersion=1, protocol=PROTOCOL, holdId=hold['holdId'], sequence=hold['sequence'],
        coordinatorAuthorityId=authority_id, jobId=claim['jobId'], generation=claim['generation'],
        environment=claim['environment'], inputManifestSha256=claim['manifestSha256'],
        pipelineDigest=claim['pipelineDigest'], claimRequestId=claim_request_id, reasonCode=hold['reasonCode'],
        observation=hold['observation'], engineStatusCode=hold['engineStatusCode']))


def make_ack(job_id, generation, record):
    ack = dict(schemaVersion=1, protocol=ACK_PROTOCOL, jobId=job_id, generation=generation, holdId=record['holdId'],
        sequence=record['sequence'], requestSha256=record['requestSha256'], reasonCode=record['reasonCode'],
        heldAt=record['heldAt'])
    ack['ackSha256'] = p.digest(ack)
    return ack


def validate_ack(value, request):
    p._closed(value, ACK_FIELDS, 'worker hold acknowledgement')
    p._require(type(value['heldAt']) is str and len(value['heldAt']) <= 40, 'invalid hold time')
    record = dict(value, requestSha256=p.digest(request), holdId=request['holdId'], sequence=request['sequence'],
        reasonCode=request['reasonCode'])
    expected = make_ack(request['jobId'], request['generation'], record)
    p._require(p.canonical_json(value) == p.canonical_json(expected), 'hold acknowledgement binding mismatch')
    return value


def stored_record(row):
    """The bounded report behind a job's visible worker status, else None."""
    if not row['queue_result'] or row['result_json'] or row['terminal_failure_json']:
        return None
    value = json.loads(row['queue_result'])
    record = value.get('workerHold') if type(value) is dict else None
    if record is None:
        return None
    p._closed(record, RECORD_FIELDS, 'stored worker hold')
    return record


def queue_status(record):
    """Additive public fields for one report; None once it has been retired."""
    reason = record['reasonCode'] if record is not None else None
    if reason is None:
        return None
    message = ('Waiting for operator resolution of a worker hold.' if reason in HOLD_REASONS
        else 'Node stays fenced until its physical reservation is reconciled.' if reason in TERMINAL_REASONS
        else 'Worker is retrying; the job keeps its claim.')
    return dict(queueReason=reason, queueMessage=message, holdObservation=record['observation'],
        holdEngineStatusCode=record['engineStatusCode'])


def _permitted(row, reason):
    """Whether this report may be recorded against the job's current authority."""
    if row['result_json'] or row['terminal_failure_json']:
        return False
    status, held = row['status'], row['held_reason']
    if reason in TERMINAL_REASONS:
        return status in ('failed', 'cancelled')
    if status == 'running':
        return True
    # Same-reason updates only: an escalated hold keeps its code, and any other
    # hold (restart, operator) is not this worker's to replace.
    return status == 'held' and (held == 'coordinator_restart' or held in HOLD_REASONS and held == reason)


def report(store, job_id, request, *, identity, claim_token):
    """Record one report; exact replay writes nothing and returns one ack."""
    request = validate_request(request)
    digest = p.digest(request)
    if type(claim_token) is not str or not claim_token.isascii() or not 1 <= len(claim_token) <= 128:
        raise Conflict('worker hold ownership unavailable')
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        row = store._row(db, job_id)
        claim = db.execute('SELECT * FROM reference_claims WHERE job_id=?', (job_id,)).fetchone()
        if (not claim or (claim['node_id'], claim['worker_id'], claim['instance_id']) !=
                (identity['node_id'], identity['worker_id'], identity['worker_instance_id'])
                or claim['request_id'] != request['claimRequestId']
                or not hmac.compare_digest(claim['token'], claim_token)
                or type(row['claim_hash']) is not str or not hmac.compare_digest(
                    hashlib.sha256(claim_token.encode('ascii')).hexdigest(), row['claim_hash'])):
            raise Conflict('worker hold ownership mismatch')
        expected = dict(coordinatorAuthorityId=store.queue.authority_id, jobId=job_id, generation=row['generation'],
            environment=row['environment'], inputManifestSha256=row['manifest_sha'], pipelineDigest=row['pipeline_digest'])
        if any(request[key] != value for key, value in expected.items()):
            raise Conflict('worker hold binding mismatch')
        reason = request['reasonCode']
        old = stored_record(row)
        if old is not None:
            if old['holdId'] != request['holdId']:
                raise Conflict('job already holds a different worker report')
            if request['sequence'] < old['sequence'] or (request['sequence'] == old['sequence'] and digest != old['requestSha256']):
                raise Conflict('stale worker hold report')
            if request['sequence'] == old['sequence']:
                return make_ack(job_id, row['generation'], old)
        if not _permitted(row, reason):
            # Operator holds stay as they are and delivered work is not reopened.
            raise Conflict('job cannot take a worker hold')
        held_at = (old['heldAt'] if old is not None and old['reasonCode'] == reason else
            datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'))
        record = dict(holdId=request['holdId'], sequence=request['sequence'], requestSha256=digest,
            reasonCode=reason, observation=request['observation'],
            engineStatusCode=request['engineStatusCode'], heldAt=held_at)
        # The operator's own terminal note stays beside the report.
        value = json.loads(row['queue_result']) if reason in TERMINAL_REASONS and row['queue_result'] else {}
        result = p.canonical_json(dict(value, workerHold=record)).decode('ascii')
        if reason in HOLD_REASONS and row['status'] == 'running':
            db.execute("UPDATE jobs SET status='held',held_reason=?,result=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (reason, result, job_id))
            db.execute('UPDATE reference_jobs SET error=?,updated_at=CURRENT_TIMESTAMP WHERE job_id=?', (reason, job_id))
        else:
            db.execute('UPDATE jobs SET result=?,updated_at=CURRENT_TIMESTAMP WHERE id=?', (result, job_id))
        return make_ack(job_id, row['generation'], record)


def claim_dropped(store, job_id, claim_request_id):
    """The job status when this unanswered claim can never succeed, else None.

    A job leaves queued only by claim or cancellation and never returns, so any
    other status without this request's claim proves nothing started here.
    """
    with store.queue._db() as db:
        db.execute('BEGIN')
        row = db.execute('SELECT status FROM jobs WHERE id=?', (job_id,)).fetchone()
        if row is None or row['status'] == 'queued':
            return None
        if db.execute('SELECT 1 FROM reference_claims WHERE request_id=?', (claim_request_id,)).fetchone():
            return None
        return row['status']
