"""Inactive, non-authorizing metadata request transition model. No durable store or I/O."""
from dataclasses import dataclass
from datetime import datetime
from . import reference_v2_candidate as c

STATUS = 'INACTIVE_NON_AUTHORIZING_NO_DURABLE_STORE'
MAX_BYTES = c.MAX_BYTES
GATES = ('issuer_authentication', 'real_original_custody_and_claim_mapping',
         'typed_metadata_control_capacity', 'durable_owner_store', 'issuer_replay_contract')

class MetadataError(ValueError):
    pass

def _fail(message):
    raise MetadataError(message)

def _raw(value):
    return c.canonical_metadata(value)

def _closed(value, keys):
    if not isinstance(value, dict) or set(value) != set(keys):
        _fail('closed fields')

def _time(value):
    c.validate_candidate(_raw(value), record_type='Time')
    return datetime.fromisoformat(value.replace('Z', '+00:00'))

def _record(raw, kind, limit=MAX_BYTES):
    if not isinstance(raw, bytes) or len(raw) > min(limit, MAX_BYTES):
        _fail('record byte bound')
    return c.validate_candidate(raw, record_type=kind).value

def _seeds(intent, policy, payload, actor):
    i = _record(intent, 'ImmutableMetadataIntent')
    p = _record(policy, 'ControlBudgetPolicy')
    b = c._decode(payload)
    _closed(b, ('originalExpiresAt', 'cumulativeByteBudget', 'totalInputBytes',
                'consumedBytes', 'remainingBytes'))
    _time(b['originalExpiresAt'])
    for key in ('cumulativeByteBudget', 'totalInputBytes', 'consumedBytes', 'remainingBytes'):
        if type(b[key]) is not int or not 0 <= b[key] <= c.MAX_INTEGER:
            _fail('payload integer')
    if not (0 < b['totalInputBytes'] <= b['cumulativeByteBudget'] <= 3*b['totalInputBytes']
            and b['consumedBytes'] + b['remainingBytes'] == b['cumulativeByteBudget']):
        _fail('payload ledger totals')
    c.validate_candidate(_raw(actor), record_type='Sha256')
    if i['round'] != 1 or len(_raw(i)) > p['maxIntentBytes']:
        _fail('initial round or intent bound')
    if i['stage']['binding'] != i['parentBinding']:
        _fail('stage parent binding')
    objects = i['originalObjects']
    ids = [o['objectId'] for o in objects]
    if ids != sorted(set(ids)):
        _fail('original object uniqueness/order')
    if any(o['acquisitionReceiptId'] != i['originalAcquisition']['receiptId'] for o in objects):
        _fail('original acquisition binding')
    roles = [i['originalAcquisition']['receiptId'], i['originalCustody']['receiptId'],
             p['reservedControlDiskReceiptId'], i['stage']['reservationReceiptId']]
    if len(set(roles)) != len(roles) or set(roles) & {o['custodyPhysicalReceiptId'] for o in objects}:
        _fail('receipt role alias')
    if (i['stage']['stageOrdinal'] == 1) != (i['predecessorProofSha256'] is None):
        _fail('predecessor scope')
    return dict(intent=i, policy=p, payload=b, expectedActorBindingSha256=actor)

@dataclass(frozen=True)
class MetadataJournal:
    """Bounded immutable bytes, not persisted state, a capacity receipt, or authority."""
    serialized: bytes

    def __bool__(self):
        raise TypeError('metadata model is not authorization')

    @property
    def value(self):
        return c._decode(self.serialized)

    def require_engine_permission(self):
        _fail('issuer authentication and runtime integration absent')

def _evaluate(seed, events):
    i, p = seed['intent'], seed['policy']
    phase, attempts, last_time = 'prepared', 0, None
    current = None
    high_custody = high_revocation = 0
    observations = {}
    for e in events:
        _closed(e, ('kind', 'observedAt', 'response'))
        now = _time(e['observedAt'])
        if now < _time(i['requestedAt']):
            _fail('observation precedes original request')
        if last_time is not None and now < last_time:
            _fail('observation clock regression')
        last_time = now
        kind = e['kind']
        if kind in ('issue', 'lookup'):
            if e['response'] is not None or phase == 'issued':
                _fail('concurrent or malformed attempt')
            if kind == 'issue' and (phase != 'prepared' or attempts):
                _fail('issuer replay contract absent')
            if kind == 'lookup' and phase not in ('outcome_unknown', 'response_observed_unverified'):
                _fail('lookup without uncertain original request')
            attempts += 1
            if attempts > p['maxAttemptsPerRound']:
                _fail('attempt bound')
            current, phase = None, 'issued'
        elif kind == 'unknown':
            if e['response'] is not None or phase != 'issued':
                _fail('unknown transition')
            current, phase = None, 'outcome_unknown'
        elif kind == 'response':
            if phase not in ('issued', 'outcome_unknown', 'response_observed_unverified'):
                _fail('response without original issue')
            raw = _raw(e['response'])
            decision = e['response'].get('decision') if isinstance(e['response'], dict) else None
            if decision not in ('authorized', 'denied'):
                _fail('response decision')
            r = _record(raw, 'AuthorizedResponse' if decision == 'authorized' else 'DeniedResponse', p['maxResponseBytes'])
            # All request fields also present in the response are immutable bindings.
            excluded = {'schemaVersion', 'protocol', 'receiptType', 'requestedAt', 'stage',
                        'controlBudgetLedgerId'}
            for key, expected in i.items():
                if key not in excluded and r.get(key) != expected:
                    _fail('response request binding: '+key)
            if r['authenticatedActorBindingSha256'] != seed['expectedActorBindingSha256']:
                _fail('response actor binding')
            original_ids = {i['originalAcquisition']['receiptId'], i['originalCustody']['receiptId'],
                            p['reservedControlDiskReceiptId'], i['stage']['reservationReceiptId']}
            original_ids.update(o['custodyPhysicalReceiptId'] for o in i['originalObjects'])
            if r['receiptId'] in original_ids:
                _fail('response receipt role alias')
            checked = _time(r['checkedAt'])
            if not _time(i['requestedAt']) <= checked <= now:
                _fail('metadata observation outside original request interval')
            if decision == 'authorized':
                expires = _time(r['metadataExpiresAt'])
                if not checked < expires or expires <= now or (expires-checked).total_seconds() > p['metadataFreshnessSeconds']:
                    _fail('metadata freshness window')
            for key, previous in (('custodyRevision', high_custody), ('revocationRevision', high_revocation)):
                if r[key] and r[key] < previous:
                    _fail('metadata revision regression')
            high_custody = max(high_custody, r['custodyRevision'])
            high_revocation = max(high_revocation, r['revocationRevision'])
            receipt = r['receiptId']
            if receipt in observations and observations[receipt] != raw:
                _fail('receipt identity changed')
            # A second distinct receipt cannot resolve the uncertain same operation.
            if observations and receipt not in observations:
                _fail('same operation different receipt')
            observations[receipt] = raw
            current, phase = r['receiptSha256'], 'response_observed_unverified'
        else:
            _fail('unknown transition kind')
    return dict(phase=phase, attemptCount=attempts, latestUnverifiedReceiptSha256=current,
                currentAuthority=None, custodyRevision=high_custody, revocationRevision=high_revocation)

def _build(seed, events):
    if len(events)+3 > min(seed['policy']['maxHistoryRecords'], c.MAX_ARRAY):
        _fail('history record bound')
    summary = _evaluate(seed, events)
    value = dict(schemaVersion=1, status=STATUS, seed=seed, events=events, summary=summary)
    raw = _raw(value)
    p = seed['policy']
    # Conservative local envelope only; NOT allocated capacity or a control charge.
    envelope = len(raw) + summary['attemptCount']*p['maxResponseBytes']
    if len(raw) > MAX_BYTES or envelope > p['maxHistoryBytes']:
        _fail('history byte envelope bound')
    return MetadataJournal(raw)

def prepare(intent: bytes, policy: bytes, payload_snapshot: bytes, expected_actor_sha256: str):
    """Pin one original stage/purpose; every response remains unverified."""
    return _build(_seeds(intent, policy, payload_snapshot, expected_actor_sha256), [])

def restore(intent: bytes, policy: bytes, payload_snapshot: bytes, expected_actor_sha256: str, serialized: bytes):
    """Recompute all counters/transitions against independently supplied originals.

    This is structural replay, not persistence, rollback protection or authentication.
    """
    seed = _seeds(intent, policy, payload_snapshot, expected_actor_sha256)
    value = c._decode(serialized)
    _closed(value, ('schemaVersion', 'status', 'seed', 'events', 'summary'))
    if type(value['schemaVersion']) is not int or value['schemaVersion'] != 1 or value['status'] != STATUS or value['seed'] != seed or not isinstance(value['events'], list):
        _fail('journal original context')
    rebuilt = _build(seed, value['events'])
    if rebuilt.serialized != _raw(value):
        _fail('journal counters or transition evidence changed')
    return rebuilt

def transition(journal: MetadataJournal, kind: str, observed_at: str, response: bytes | None = None):
    """Model a bounded attempted request BEFORE I/O; this function itself cannot issue I/O."""
    if type(journal) is not MetadataJournal:
        _fail('journal type')
    value = journal.value
    _closed(value, ('schemaVersion', 'status', 'seed', 'events', 'summary'))
    _closed(value['seed'], ('intent', 'policy', 'payload', 'expectedActorBindingSha256'))
    # Revalidate even a directly constructed dataclass; no trusted summary bypass.
    seed = value['seed']
    old = restore(_raw(seed['intent']), _raw(seed['policy']), _raw(seed['payload']),
                  seed['expectedActorBindingSha256'], journal.serialized).value
    if response is not None and (not isinstance(response, bytes) or len(response) > min(MAX_BYTES, seed['policy']['maxResponseBytes'])):
        _fail('response byte bound')
    parsed = c._decode(response) if response is not None else None
    return _build(old['seed'], old['events'] + [dict(kind=kind, observedAt=observed_at, response=parsed)])
