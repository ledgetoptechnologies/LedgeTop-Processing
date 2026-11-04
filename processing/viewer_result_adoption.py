"""Evidence that lets a lost Viewer task's finished result be adopted.

A node back from a loss can report code 40 while only it holds the result, and
NodeODX cleans up finished tasks. Copying those bytes destroys nothing, so the
coordinator fetches them unprompted; only an operator changes the job. This file
beside the task's inputs records what the fetch was bound to. It is a sidecar
because the task directory is already inside the update census, whose schema
allowlist is strict, and ``refused`` is a latch: only resolving the hold ends it.

States: ``bound`` holds the engine identity first seen while running, ``observed``
a code 40 being fetched, ``verified`` a result in custody, ``refused`` a reason.
"""

import hashlib
import json

ADOPTION_NAME = '.ledgetop-adopt.json'
ADOPTION_VERSION = 1
ADOPTION_MAX_FILE_BYTES = 16 * 1024
ADOPTION_STATES = ('bound', 'observed', 'verified', 'refused')
IDENTITY_MISMATCH = 'engine_identity_mismatch'
RESULT_CHANGED = 'engine_result_changed'
_BOUND_FIELDS = ('dateCreated', 'imagesCount', 'options')
CUSTODY_NAME = '.ledgetop-custody.json'
CUSTODY_VERSION = 1
CUSTODY_STATES = ('pending', 'verified', 'deferred', 'unavailable')


def new_adoption(identity):
    return {'version': ADOPTION_VERSION, 'identity': identity, 'state': 'bound', 'reason': None,
            'bound': None, 'result': None, 'archive': None, 'adoption': None}


def valid_adoption(record, identity):
    return (type(record) is dict and set(record) == set(new_adoption(identity))
            and record['version'] == ADOPTION_VERSION and record['identity'] == identity
            and record['state'] in ADOPTION_STATES
            and (record['reason'] is None or type(record['reason']) is str)
            and (record['bound'] is None or (type(record['bound']) is dict and set(record['bound']) == set(_BOUND_FIELDS)))
            and (record['result'] is None or type(record['result']) is dict)
            and (record['archive'] is None or type(record['archive']) is dict)
            and (record['adoption'] is None or type(record['adoption']) is dict))


def new_custody(identity, info):
    """Eager custody of a normal success. No operator decides anything here, so
    this is not an adoption record: ``result`` is what the engine reported at code
    40, and a transfer publishes only while the engine still reports exactly that.
    """
    return {'version': CUSTODY_VERSION, 'identity': identity, 'state': 'pending', 'reason': None,
            'result': engine_evidence(info), 'archive': None}


def valid_custody(record, identity):
    return (type(record) is dict and set(record) == {'version', 'identity', 'state', 'reason', 'result', 'archive'}
            and record['version'] == CUSTODY_VERSION and record['identity'] == identity
            and record['state'] in CUSTODY_STATES
            and (record['reason'] is None or type(record['reason']) is str)
            and type(record['result']) is dict
            and (record['archive'] is None or type(record['archive']) is dict))


def _options_digest(options):
    if (type(options) is not list or any(type(item) is not dict or type(item.get('name')) is not str for item in options)):
        return None
    ordered = sorted(options, key=lambda item: item['name'])
    return hashlib.sha256(json.dumps(ordered, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def engine_evidence(info):
    """What the engine says about itself, reduced to bounded comparable values."""
    scalar = lambda value: value if type(value) in (int, float) or (type(value) is str and len(value) <= 64) else None
    return {'dateCreated': scalar(info.get('dateCreated')), 'imagesCount': scalar(info.get('imagesCount')),
            'options': _options_digest(info.get('options')), 'processingTime': scalar(info.get('processingTime'))}


def bound_evidence(info):
    return {key: value for key, value in engine_evidence(info).items() if key in _BOUND_FIELDS}


def identity_refusal(task, record, info):
    """A fixed reason if this engine task is not provably the claimed run.

    The uuid and node are already exact: the read used them. What was recorded
    while it ran is exact too. Without that the task's own files and options
    are the only binding, and NodeODM may count non-photo inputs as images.
    """
    current, bound = engine_evidence(info), record['bound'] or {}
    if any(bound.get(key) is not None and bound[key] != current[key] for key in _BOUND_FIELDS):
        return IDENTITY_MISMATCH
    if bound.get('imagesCount') is None and current['imagesCount'] not in (task['images_count'], len(task['files'])):
        return IDENTITY_MISMATCH
    reported = info.get('options')
    if bound.get('options') is None and type(reported) is list:
        values = {item.get('name'): item.get('value') for item in reported if type(item) is dict}
        same = lambda left, right: str(left).lower() == str(right).lower()
        if any(item['name'] not in values or not same(values[item['name']], item['value']) for item in task['options']):
            return IDENTITY_MISMATCH
    return None
