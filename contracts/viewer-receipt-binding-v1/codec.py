"""Review-only canonical wire codec. Not an endpoint or authentication."""
import base64
import hashlib
import json
import re
from processing import reference_protocol as p

_UUID4 = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
_B64 = re.compile(r'^[A-Za-z0-9_-]+$')
MAX_ENVELOPE = p.MAX_BODY_BYTES
MAX_RECEIPT = 2 * 1024 * 1024
MAX_DEPTH = 8
MAX_NODES = 50_000

def _fail():
    raise ValueError('invalid candidate receipt wire')

def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result: _fail()
        result[key] = value
    return result

def _int(value):
    if value == '-0': _fail()
    result = int(value)
    if not 0 <= result <= p.MAX_SAFE_INTEGER: _fail()
    return result

def _keys(value, depth=0, budget=None):
    if budget is None: budget = [MAX_NODES]
    budget[0] -= 1
    if budget[0] < 0 or depth > MAX_DEPTH: _fail()
    if type(value) is dict:
        for key, item in value.items():
            if not key.isascii(): _fail()
            _keys(item, depth + 1, budget)
    elif type(value) is list:
        for item in value: _keys(item, depth + 1, budget)

def _parse(raw):
    try:
        value = json.loads(raw.decode('utf-8', 'strict'), object_pairs_hook=_pairs,
                           parse_int=_int, parse_float=lambda _: _fail(),
                           parse_constant=lambda _: _fail())
        _keys(value)
        if p.canonical_json(value) != raw: _fail()
        return value
    except (UnicodeError, ValueError, TypeError, RecursionError):
        _fail()

def decode(raw):
    if type(raw) is not bytes or not 1 <= len(raw) <= MAX_ENVELOPE: _fail()
    env = _parse(raw)
    if type(env) is not dict or set(env) != {'schemaVersion','bindingId','attemptId','receiptSha256','receiptBase64url'}: _fail()
    if type(env['schemaVersion']) is not int or env['schemaVersion'] != 1: _fail()
    if any(type(env[x]) is not str or not _UUID4.fullmatch(env[x]) for x in ('bindingId','attemptId')): _fail()
    encoded = env['receiptBase64url']
    if type(encoded) is not str or not _B64.fullmatch(encoded) or len(encoded) > (MAX_RECEIPT * 4 + 2)//3: _fail()
    try:
        payload = base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4))
    except (ValueError, TypeError): _fail()
    if not 1 <= len(payload) <= MAX_RECEIPT or base64.urlsafe_b64encode(payload).rstrip(b'=').decode('ascii') != encoded: _fail()
    if type(env['receiptSha256']) is not str or not re.fullmatch(r'[0-9a-f]{64}', env['receiptSha256']) or hashlib.sha256(payload).hexdigest() != env['receiptSha256']: _fail()
    receipt = _parse(payload)
    if type(receipt) is not dict: _fail()
    return env, receipt, payload
