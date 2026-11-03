"""Non-authorizing compatibility proposal for different ODM catalogs.

This module is intentionally not called by admission or dispatch. A reviewed,
versioned policy and durable handoff are prerequisites for using its output.
"""
import math
import re

_NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z')
_INT = re.compile(r'-?(?:0|[1-9][0-9]*)\Z')
_FLOAT = re.compile(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z')
_SPECIAL = frozenset(('boundary', 'cameras', 'gcp', 'rerun-from', 'end-with', 'sm-cluster'))
_KINDS = frozenset(('bool', 'int', 'float', 'enum', 'string'))


def _number(value, kind, *, default):
    if default and type(value) is str:
        if not (_INT if kind == 'int' else _FLOAT).fullmatch(value):
            raise ValueError('ambiguous numeric default')
        value = int(value) if kind == 'int' else float(value)
    if kind == 'int':
        if type(value) is not int or abs(value) > 9007199254740991:
            raise ValueError('invalid integer')
    elif type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError('invalid number')
    return value


def _value(spec, value, *, default=False):
    kind, domain = spec['type'], spec.get('domain', '')
    if kind == 'bool':
        if default and value in ('true', 'false'):
            value = value == 'true'
        if type(value) is not bool or domain not in ('', None):
            raise ValueError('invalid boolean')
    elif kind in ('int', 'float'):
        value = _number(value, kind, default=default)
        numeric_domains = ('', 'integer', 'positive integer', 'float', 'positive float',
                           'float > 0.0', 'integer: 1 <= x <= 14')
        if domain not in numeric_domains:
            raise ValueError('unknown numeric domain')
        if domain.startswith('positive ') and value < 0:
            raise ValueError('negative value')
        if domain == 'positive integer' and not float(value).is_integer():
            raise ValueError('integer value required')
        if domain == 'float > 0.0' and value <= 0:
            raise ValueError('positive value required')
        if domain == 'integer: 1 <= x <= 14' and not (1 <= value <= 14 and float(value).is_integer()):
            raise ValueError('value outside domain')
    elif kind == 'enum':
        if (type(value) is not str or type(domain) is not list or not domain
                or any(type(choice) is not str for choice in domain) or value not in domain):
            raise ValueError('invalid enum')
    elif kind == 'string':
        if type(value) is not str or len(value) > 4000 or domain not in ('', 'string', 'json'):
            raise ValueError('invalid string')
    else:
        raise ValueError('unknown type')
    return value


def _catalog(items):
    if type(items) is not list or not items or len(items) > 2048:
        raise ValueError('invalid catalog')
    result = {}
    for item in items:
        if (type(item) is not dict or type(item.get('name')) is not str
                or not _NAME.fullmatch(item['name']) or item['name'] in result
                or item.get('type') not in _KINDS or 'value' not in item):
            raise ValueError('invalid catalog definition')
        result[item['name']] = item
    return result


def plan_effective_options(source, target, requested, policy):
    """Return a non-authorizing proposal or a held result; never mutate input.

    `policy` is an exact per-name review input: `pin-default` permits adding a
    validated, representable enum/numeric source default; `node-local` permits
    differing numeric hardware defaults. No missing or unknown option can be
    waived. This function does not attest policy ownership or evidence.
    """
    try:
        a, b = _catalog(source), _catalog(target)
        if type(requested) is not list or type(policy) is not dict:
            raise ValueError('invalid request or policy')
        if any(type(k) is not str or not _NAME.fullmatch(k) or v not in ('pin-default', 'node-local') for k, v in policy.items()):
            raise ValueError('invalid policy')
        explicit = {}
        for item in requested:
            if (type(item) is not dict or set(item) != {'name', 'value'}
                    or type(item['name']) is not str or item['name'] in explicit):
                raise ValueError('invalid request')
            explicit[item['name']] = item['value']
        if set(explicit) - (set(a) & set(b)):
            raise ValueError('requested option missing')
        pins, reasons = [], []
        for name in sorted(set(a) | set(b)):
            if name not in a or name not in b:
                reasons.append(f'{name}:missing_definition'); continue
            left, right = a[name], b[name]
            if left['type'] != right['type']:
                reasons.append(f'{name}:type_changed'); continue
            if name in explicit:
                if name in _SPECIAL:
                    reasons.append(f'{name}:special_option'); continue
                _value(left, explicit[name]); _value(right, explicit[name]); continue
            first, second = _value(left, left['value'], default=True), _value(right, right['value'], default=True)
            if first == second and type(first) is type(second):
                continue
            rule = policy.get(name)
            if name in _SPECIAL:
                reasons.append(f'{name}:special_default_changed')
            elif rule == 'node-local' and left['type'] in ('int', 'float'):
                continue
            elif rule == 'pin-default' and left['type'] in ('enum', 'int', 'float'):
                _value(right, first)
                pins.append({'name': name, 'value': first})
            else:
                reasons.append(f'{name}:unreviewed_default_change')
        return {'state': 'held' if reasons else 'proposal', 'reasons': reasons,
                'pins': [] if reasons else pins, 'authorizesDispatch': False}
    except (ValueError, TypeError, KeyError, OverflowError):
        return {'state': 'held', 'reasons': ['invalid_or_ambiguous_input'],
                'pins': [], 'authorizesDispatch': False}
