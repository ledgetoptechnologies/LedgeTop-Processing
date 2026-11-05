"""Worker observation of verified Viewer copies; never source release authority."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import hmac
import json

from . import reference_protocol as p
from .maintenance import Conflict

PROTOCOL = 'ltds-worker-acceptance-observed-v1'
ACK_PROTOCOL = 'ltds-worker-acceptance-observed-ack-v1'
FIELDS = ('schemaVersion', 'protocol', 'observationId', 'coordinatorAuthorityId',
    'jobId', 'generation', 'environment', 'inputManifestSha256', 'pipelineDigest',
    'resultManifestSha256', 'receipts')
PAIR_FIELDS = ('acceptanceReceiptId', 'acceptanceSha256', 'viewerReceiptId', 'viewerReceiptSha256')
ACK_FIELDS = ('schemaVersion', 'protocol', 'coordinatorAuthorityId', 'jobId', 'generation',
    'observationId', 'requestSha256', 'nodeId', 'workerId', 'workerInstanceId', 'observedAt', 'ackSha256')


def validate_request(value):
    p._closed(value, FIELDS, 'acceptance observation')
    p._require(type(value['schemaVersion']) is int and value['schemaVersion'] == 1
        and value['protocol'] == PROTOCOL, 'unsupported acceptance observation')
    p._uuid(value['observationId'], 'observation ID')
    for name in ('coordinatorAuthorityId', 'jobId'):
        p._slug(value[name], name)
    p._integer(value['generation'], 'observation generation')
    p._environment(value['environment'])
    for name in ('inputManifestSha256', 'pipelineDigest', 'resultManifestSha256'):
        p._hash(value[name], name)
    receipts = value['receipts']
    p._require(type(receipts) is list and 1 <= len(receipts) <= p.MAX_OBJECTS, 'bounded observation receipts required')
    for pair in receipts:
        p._closed(pair, PAIR_FIELDS, 'observed receipt pair')
        for name in ('acceptanceReceiptId', 'viewerReceiptId'):
            p._slug(pair[name], name)
        for name in ('acceptanceSha256', 'viewerReceiptSha256'):
            p._hash(pair[name], name)
    ids = [pair['acceptanceReceiptId'] for pair in receipts]
    viewer_ids = [pair['viewerReceiptId'] for pair in receipts]
    p._require(ids == sorted(set(ids)) and len(viewer_ids) == len(set(viewer_ids)),
        'observation receipts must be unique and sorted')
    p._require(len(p.canonical_json(value)) <= p.MAX_BODY_BYTES, 'observation metadata too large')
    return deepcopy(value)


def receipt_pairs(receipts):
    if type(receipts) is not list or not 1 <= len(receipts) <= p.MAX_OBJECTS:
        raise ValueError('verified V2 receipt list required')
    pairs = []
    for envelope in receipts:
        p._closed(envelope, ('acceptance', 'viewerReceipt'), 'observed receipt envelope')
        acceptance, viewer = envelope['acceptance'], envelope['viewerReceipt']
        if (type(acceptance) is not dict or type(viewer) is not dict
                or viewer.get('protocol') != 'ltds-viewer-accepted-copy-v2'
                or viewer.get('acceptanceReceiptId') != acceptance.get('receiptId')):
            raise ValueError('verified V2 receipt pair required')
        pairs.append(dict(acceptanceReceiptId=acceptance['receiptId'], acceptanceSha256=p.digest(acceptance),
            viewerReceiptId=viewer['receiptId'], viewerReceiptSha256=p.digest(viewer)))
    return sorted(pairs, key=lambda item: item['acceptanceReceiptId'])


def make_request(claim, result, receipts, observation_id):
    p.validate_result_manifest(result, job_id=claim['jobId'], generation=claim['generation'],
        environment=claim['environment'], pipeline_digest=claim['pipelineDigest'])
    return validate_request(dict(schemaVersion=1, protocol=PROTOCOL, observationId=observation_id,
        coordinatorAuthorityId=claim['sourceAcceptanceBinding']['coordinatorAuthorityId'], jobId=claim['jobId'],
        generation=claim['generation'], environment=claim['environment'], inputManifestSha256=claim['manifestSha256'],
        pipelineDigest=claim['pipelineDigest'], resultManifestSha256=result['resultManifestSha256'],
        receipts=receipt_pairs(receipts)))


def make_ack(request, identity, observed_at):
    ack = dict(schemaVersion=1, protocol=ACK_PROTOCOL,
        coordinatorAuthorityId=request['coordinatorAuthorityId'], jobId=request['jobId'],
        generation=request['generation'], observationId=request['observationId'], requestSha256=p.digest(request),
        nodeId=identity['node_id'], workerId=identity['worker_id'], workerInstanceId=identity['worker_instance_id'],
        observedAt=observed_at)
    ack['ackSha256'] = p.digest(ack)
    return ack


def validate_ack(value, request, identity):
    p._closed(value, ACK_FIELDS, 'acceptance observation acknowledgement')
    observed_at = value['observedAt']
    p._require(type(observed_at) is str and len(observed_at) <= 40 and observed_at.endswith('Z'), 'invalid observed time')
    parsed = datetime.fromisoformat(observed_at.replace('Z', '+00:00'))
    p._require(parsed.utcoffset() == timezone.utc.utcoffset(None), 'observed time must be UTC')
    expected = make_ack(request, identity, observed_at)
    p._require(p.canonical_json(value) == p.canonical_json(expected), 'observation acknowledgement binding mismatch')
    return deepcopy(value)


def ensure_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS reference_worker_acceptance_observations (
        job_id TEXT NOT NULL REFERENCES reference_jobs(job_id),generation INTEGER NOT NULL,
        observation_id TEXT NOT NULL UNIQUE,request_sha256 TEXT NOT NULL,request_json TEXT NOT NULL,
        ack_json TEXT NOT NULL,node_id TEXT NOT NULL,worker_id TEXT NOT NULL,instance_id TEXT NOT NULL,
        claim_sha256 TEXT NOT NULL,PRIMARY KEY(job_id,generation))''')


def metadata_bytes(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='reference_worker_acceptance_observations'").fetchone():
        return 0
    return db.execute('''SELECT COALESCE(SUM(length(job_id)+length(CAST(generation AS TEXT))+
        length(observation_id)+length(request_sha256)+length(CAST(request_json AS BLOB))+
        length(CAST(ack_json AS BLOB))+length(node_id)+length(worker_id)+length(instance_id)+length(claim_sha256)),0)
        FROM reference_worker_acceptance_observations''').fetchone()[0]


def observe(store, job_id, request, *, identity, claim_token):
    request = validate_request(request)
    identity = dict(identity)
    p._closed(identity, ('node_id', 'worker_id', 'worker_instance_id'), 'observation worker identity')
    for value in identity.values():
        p._slug(value, 'observation worker identity')
    encoded, digest = p.canonical_json(request).decode('ascii'), p.digest(request)
    if (type(claim_token) is not str or not claim_token.isascii() or not 1 <= len(claim_token) <= 128):
        raise Conflict('worker observation ownership unavailable')
    token_digest = hashlib.sha256(claim_token.encode('ascii')).hexdigest()
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        ensure_schema(db)
        row = store._row(db, job_id)
        claim = db.execute('SELECT * FROM reference_claims WHERE job_id=?', (job_id,)).fetchone()
        if (not claim or (claim['node_id'], claim['worker_id'], claim['instance_id']) !=
                (identity['node_id'], identity['worker_id'], identity['worker_instance_id'])
                or not hmac.compare_digest(claim['token'], claim_token)):
            raise Conflict('worker observation ownership mismatch')
        expected = dict(coordinatorAuthorityId=store.queue.authority_id, jobId=job_id,
            generation=row['generation'], environment=row['environment'], inputManifestSha256=row['manifest_sha'],
            pipelineDigest=row['pipeline_digest'])
        submission = json.loads(row['input_json'])
        old = db.execute('SELECT * FROM reference_worker_acceptance_observations WHERE job_id=? AND generation=?',
            (job_id, row['generation'])).fetchone()
        if (type(submission.get('sourceIntakeVersion')) is not int or submission['sourceIntakeVersion'] != 2
                or (old is None and (row['source_state'] != 'accepted' or row['derived_state'] != 'accepted'))
                or row['phase'] != 'completed' or not row['result_json']
                or any(request[key] != value for key, value in expected.items())):
            raise Conflict('V2 observation requires exact accepted job')
        result = p.validate_result_manifest(json.loads(row['result_json']), job_id=job_id,
            generation=row['generation'], environment=row['environment'], pipeline_digest=row['pipeline_digest'])
        if request['resultManifestSha256'] != result['resultManifestSha256']:
            raise Conflict('observed result manifest changed')
        accepted_ids = set()
        for pair in request['receipts']:
            stored = db.execute('''SELECT a.receipt_json,a.digest,v.receipt_id,v.receipt_json AS viewer_json,v.digest AS viewer_digest
                FROM reference_acceptances a JOIN reference_viewer_receipts v ON v.acceptance_receipt_id=a.receipt_id
                WHERE a.job_id=? AND v.job_id=? AND a.receipt_id=?''',
                (job_id, job_id, pair['acceptanceReceiptId'])).fetchone()
            if not stored:
                raise Conflict('observed receipt unavailable')
            acceptance, viewer = json.loads(stored['receipt_json']), json.loads(stored['viewer_json'])
            if (stored['digest'] != p.digest(acceptance) or stored['viewer_digest'] != p.digest(viewer)
                    or pair != dict(acceptanceReceiptId=acceptance['receiptId'], acceptanceSha256=p.digest(acceptance),
                        viewerReceiptId=stored['receipt_id'], viewerReceiptSha256=p.digest(viewer))
                    or viewer.get('protocol') != 'ltds-viewer-accepted-copy-v2'
                    or viewer.get('acceptanceReceiptId') != acceptance['receiptId']):
                raise Conflict('observed receipt binding changed')
            checked = p.validate_acceptance(acceptance, result_manifest=result, input_manifest_sha256=row['manifest_sha'])
            if (viewer.get('acceptedObjectIds') != checked['acceptedObjectIds']
                    or viewer.get('resultManifestSha256') != result['resultManifestSha256']):
                raise Conflict('observed receipt coverage changed')
            accepted_ids.update(checked['acceptedObjectIds'])
        if accepted_ids != {item['id'] for item in result['objects']}:
            raise Conflict('observed receipts do not cover complete result')
        if old:
            if (old['observation_id'] != request['observationId'] or old['request_sha256'] != digest
                    or old['request_json'] != encoded or (old['node_id'], old['worker_id'], old['instance_id'], old['claim_sha256']) !=
                        (identity['node_id'], identity['worker_id'], identity['worker_instance_id'], token_digest)):
                raise Conflict('worker observation replay conflict')
            return validate_ack(json.loads(old['ack_json']), request, identity)
        if db.execute('SELECT 1 FROM reference_worker_acceptance_observations WHERE observation_id=?', (request['observationId'],)).fetchone():
            raise Conflict('worker observation ID already bound')
        observed_at = store.queue._now().isoformat(timespec='microseconds').replace('+00:00', 'Z')
        ack = make_ack(request, identity, observed_at)
        ack_json = p.canonical_json(ack).decode('ascii')
        values = (job_id, row['generation'], request['observationId'], digest, encoded, ack_json,
            identity['node_id'], identity['worker_id'], identity['worker_instance_id'], token_digest)
        store._quota(db, sum(len(str(item).encode('utf-8')) for item in values))
        db.execute('INSERT INTO reference_worker_acceptance_observations VALUES(?,?,?,?,?,?,?,?,?,?)', values)
        return json.loads(ack_json)


def lookup(store, job_id):
    """Read only the stable acknowledgement; caller establishes Viewer scope."""
    with store.queue._db() as db:
        row = store._row(db, job_id)
        submission = json.loads(row['input_json'])
        if submission.get('sourceIntakeVersion') != 2:
            return None
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='reference_worker_acceptance_observations'").fetchone():
            return None
        saved = db.execute('SELECT * FROM reference_worker_acceptance_observations WHERE job_id=? AND generation=?',
            (job_id, row['generation'])).fetchone()
        if saved is None:
            return None
        request = validate_request(json.loads(saved['request_json']))
        if (request['jobId'] != job_id or request['generation'] != row['generation']
                or request['coordinatorAuthorityId'] != store.queue.authority_id
                or saved['request_sha256'] != p.digest(request)
                or saved['observation_id'] != request['observationId']):
            raise Conflict('worker observation journal changed')
        identity = dict(node_id=saved['node_id'], worker_id=saved['worker_id'], worker_instance_id=saved['instance_id'])
        return validate_ack(json.loads(saved['ack_json']), request, identity)
