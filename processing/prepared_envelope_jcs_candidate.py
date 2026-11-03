"""Unmounted experimental JCS envelope. No authentication, activation or cleanup."""
import hashlib
import json
import math

from . import reference_protocol as p

DOMAIN = b'ltds-viewer-prepared-envelope-jcs-v3\x00'
PROTOCOL = 'ltds-viewer-prepared-envelope-jcs-v3'
MAX_DEPTH = 24
MAX_NODES = 100000


def _parse(raw):
    if type(raw) is not bytes or len(raw) > p.MAX_BODY_BYTES:
        raise ValueError('bounded raw bytes required')
    # Bound nesting before JSON allocates recursive structures. Quoted brackets
    # and escaped quotes do not contribute to depth; JSON performs syntax checks.
    depth = 0; quoted = False; escaped = False
    for byte in raw:
        if quoted:
            if escaped: escaped = False
            elif byte == 92: escaped = True
            elif byte == 34: quoted = False
        elif byte == 34: quoted = True
        elif byte in (91, 123):
            depth += 1
            if depth > MAX_DEPTH: raise ValueError('JSON nesting limit')
        elif byte in (93, 125): depth -= 1
    def pairs(items):
        out = {}
        for k, v in items:
            if k in out: raise ValueError('duplicate JSON key')
            out[k] = v
        return out
    try:
        value = json.loads(raw.decode('utf8'), object_pairs_hook=pairs,
            parse_constant=lambda x: (_ for _ in ()).throw(ValueError('nonfinite number')))
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise ValueError('invalid JSON') from None
    stack = [value]; count = 0
    while stack:
        item = stack.pop(); count += 1
        if count > MAX_NODES: raise ValueError('JSON node limit')
        if type(item) is int and abs(item) > p.MAX_SAFE_INTEGER: raise ValueError('unsafe integer')
        if type(item) is float and (not math.isfinite(item) or abs(item) > p.MAX_SAFE_INTEGER):
            raise ValueError('invalid numeric value')
        if type(item) is dict: stack.extend(item.values())
        elif type(item) is list: stack.extend(item)
    return value


def validate(raw, *, trusted_policy, environment, previous_canonical=None):
    """Validate raw wire with independently selected policy. Exact replay is caller-owned.

    This pure candidate cannot authenticate its caller, persist replay evidence,
    or authorize a worker. The pinned optional dependency is imported only here.
    """
    import rfc8785
    envelope = _parse(raw)
    try: canonical = rfc8785.dumps(envelope)
    except (ValueError, TypeError, UnicodeError): raise ValueError('invalid JCS data') from None
    # Normalize JCS-equivalent forms before producing the server-owned legacy
    # digests, so e.g. -0 and 0 cannot share a wire identity but diverge there.
    envelope = _parse(canonical)
    p._closed(envelope, ('schemaVersion','protocol','submission','viewerTaskId','viewerAttemptId','activationId'), 'envelope')
    if type(envelope['schemaVersion']) is not int or envelope['schemaVersion'] != 3 or envelope['protocol'] != PROTOCOL:
        raise ValueError('unsupported envelope')
    for name in ('viewerTaskId','viewerAttemptId','activationId'): p._uuid(envelope[name], name)
    p._closed(trusted_policy, ('capabilityFingerprint','optionsFingerprint','availableEngines','optionDefinitions'), 'policy')
    defs = trusted_policy['optionDefinitions']
    if type(defs) is not dict: raise ValueError('invalid option definitions')
    submission = envelope['submission']
    normalized = p.validate_submission(submission, environment=environment,
        capability_fingerprint=trusted_policy['capabilityFingerprint'],
        options_fingerprint=trusted_policy['optionsFingerprint'],
        available_engines=trusted_policy['availableEngines'], allowed_odm_options=set(defs))
    for name, value in submission['pipeline']['odm']['options'].items():
        spec = defs[name]
        p._closed(spec, ('type','domain'), 'option definition')
        kind, domain = spec['type'], spec['domain']
        if kind == 'bool': valid = type(value) is bool
        elif kind == 'int': valid = type(value) is int
        elif kind == 'float': valid = type(value) in (int,float)
        elif kind in ('enum','string'): valid = type(value) is str
        else: raise ValueError('unsupported option type')
        if not valid: raise ValueError('option type mismatch')
        if domain is None: continue
        if type(domain) is list:
            if not domain or not any(type(value) is type(choice) and value == choice for choice in domain):
                raise ValueError('option outside domain')
        elif type(domain) is dict and kind in ('int','float'):
            p._closed(domain, ('min','max'), 'numeric domain')
            lo, hi = domain['min'], domain['max']
            if (type(lo) not in (int,float) or type(hi) not in (int,float)
                    or not math.isfinite(lo) or not math.isfinite(hi) or lo > hi or not lo <= value <= hi):
                raise ValueError('option outside domain')
        else: raise ValueError('invalid option domain')
    if previous_canonical is not None and (type(previous_canonical) is not bytes or canonical != previous_canonical):
        raise ValueError('prepared envelope replay changed')
    return canonical, hashlib.sha256(DOMAIN + canonical).hexdigest(), normalized
