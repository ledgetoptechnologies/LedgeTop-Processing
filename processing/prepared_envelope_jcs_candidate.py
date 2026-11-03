"""Unmounted experimental JCS envelope. No authentication, activation or cleanup."""
import hashlib
import json
import math

from . import reference_protocol as p

DOMAIN = b'ltds-viewer-prepared-envelope-jcs-v3\x00'
PROTOCOL = 'ltds-viewer-prepared-envelope-jcs-v3'
MAX_DEPTH = 24
MAX_NODES = 100000


def _validate_definition(spec):
    """Validate independent policy values without Python's bool/int equality."""
    p._closed(spec, ('type','domain'), 'option definition')
    kind, domain = spec['type'], spec['domain']
    if kind not in ('bool','int','float','enum','string'):
        raise ValueError('unsupported option type')
    def valid(value):
        if kind == 'bool': return type(value) is bool
        if kind == 'int': return type(value) is int and abs(value) <= p.MAX_SAFE_INTEGER
        if kind == 'float': return type(value) in (int, float) and abs(value) <= p.MAX_SAFE_INTEGER and math.isfinite(value)
        return type(value) is str
    if domain is None:
        if kind == 'enum': raise ValueError('enum domain required')
    elif type(domain) is list:
        if not domain or not all(valid(choice) for choice in domain):
            raise ValueError('invalid option domain')
    elif type(domain) is dict and kind in ('int','float'):
        p._closed(domain, ('min','max'), 'numeric domain')
        if not valid(domain['min']) or not valid(domain['max']) or domain['min'] > domain['max']:
            raise ValueError('invalid numeric domain')
    else:
        raise ValueError('invalid option domain')
    return kind, domain, valid


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
    definitions = {name: _validate_definition(spec) for name, spec in defs.items()}
    submission = envelope['submission']
    normalized = p.validate_submission(submission, environment=environment,
        capability_fingerprint=trusted_policy['capabilityFingerprint'],
        options_fingerprint=trusted_policy['optionsFingerprint'],
        available_engines=trusted_policy['availableEngines'], allowed_odm_options=set(defs))
    for name, value in submission['pipeline']['odm']['options'].items():
        kind, domain, valid = definitions[name]
        if not valid(value): raise ValueError('option type mismatch')
        if domain is None: continue
        if type(domain) is list:
            if not any(value == choice for choice in domain):
                raise ValueError('option outside domain')
        else:
            if not domain['min'] <= value <= domain['max']:
                raise ValueError('option outside domain')
    if previous_canonical is not None and (type(previous_canonical) is not bytes or canonical != previous_canonical):
        raise ValueError('prepared envelope replay changed')
    return canonical, hashlib.sha256(DOMAIN + canonical).hexdigest(), normalized
