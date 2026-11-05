"""Shared fresh ODM definition and value policy; no runtime or engine imports."""
import json
import math
import re

from . import reference_protocol as p

_UNSAFE = re.compile(r"\b(file|filename|path|directory|folder|url|uri|endpoint|host|hostname|"
                     r"token|credential|secret|password|key|script|command|upload|download)\b", re.I)
_OPTION_BOUND = re.compile(
    r"^(?:int|integer|float|number|x)\s*(>=|>|<=|<)\s*"
    r"(-?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?)$", re.I)
_OPTION_RANGE = re.compile(
    r"^(-?(?:\d+(?:\.\d*)?|\.\d+))\s*(<=|<)\s*"
    r"(?:x|int|integer|float|number)\s*(<=|<)\s*"
    r"(-?(?:\d+(?:\.\d*)?|\.\d+))$", re.I)
_FILE_OPTIONS = frozenset(("align", "cameras", "boundary", "geo", "gcp", "image-groups",
                           "input", "output", "project-path"))


def _option_domain(option):
    """Return a normalized Viewer-compatible domain, or None when malformed."""
    domain = option.get("domain")
    if domain is None or (type(domain) is str and not domain.strip()):
        return {}
    if type(domain) is str:
        try:
            domain = json.loads(domain)
        except (ValueError, TypeError):
            text = domain.strip().lower()
            if re.fullmatch(r"positive (?:integer|int|float|number)", text):
                return {"min": 0, "minExclusive": True}
            if re.fullmatch(r"non[- ]?negative (?:integer|int|float|number)", text):
                return {"min": 0}
            bound = _OPTION_BOUND.fullmatch(text)
            if bound:
                value = float(bound[2])
                if not math.isfinite(value):
                    return None
                return ({"min": value, "minExclusive": bound[1] == ">"}
                        if bound[1].startswith(">") else
                        {"max": value, "maxExclusive": bound[1] == "<"})
            interval = _OPTION_RANGE.fullmatch(text)
            if interval:
                low, high = float(interval[1]), float(interval[4])
                if math.isfinite(low) and math.isfinite(high) and low <= high:
                    return {"min": low, "max": high,
                            "minExclusive": interval[2] == "<",
                            "maxExclusive": interval[3] == "<"}
            # Unrecognized prose is display metadata, never an expression to
            # evaluate. As in the Viewer, it adds no constraints; path-like
            # definitions are rejected separately by the option restriction.
            return {}
    if type(domain) is list:
        if len(domain) > 1000 or any(type(value) not in (str, bool, int, float)
                or (type(value) is float and not math.isfinite(value)) for value in domain):
            return None
        if option.get("type") in ("string", "enum") and any(type(value) is not str for value in domain):
            return None
        return {"values": domain}
    if type(domain) is dict and set(domain) <= {"min", "max"}:
        result = {}
        for key in ("min", "max"):
            if key in domain:
                value = domain[key]
                if type(value) not in (int, float):
                    return None
                try:
                    if not math.isfinite(value):
                        return None
                except OverflowError:
                    return None
                result[key] = value
        if "min" in result and "max" in result and result["min"] > result["max"]:
            return None
        return result
    return None


def _value_matches_domain(value, kind, domain):
    if "values" in domain:
        def same_choice(choice):
            if type(value) in (int, float) and type(choice) in (int, float):
                return value == choice
            return type(value) is type(choice) and value == choice
        if not any(same_choice(choice) for choice in domain["values"]):
            return False
    if not ("min" in domain or "max" in domain):
        return True
    numeric = value
    if kind == "string":
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return False
    if type(numeric) not in (int, float) or not math.isfinite(numeric):
        return False
    if "min" in domain and (numeric <= domain["min"] if domain.get("minExclusive") else numeric < domain["min"]):
        return False
    if "max" in domain and (numeric >= domain["max"] if domain.get("maxExclusive") else numeric > domain["max"]):
        return False
    return True


_KINDS = {'boolean': 'bool', 'integer': 'int', 'double': 'float', 'number': 'float'}


def _safe_text(value):
    return (type(value) is str and len(value) <= 4000 and '\\' not in value
            and not re.search(r'[A-Za-z][A-Za-z0-9+.-]*://|(?:^|[\s=])[A-Za-z]:[\\/]|(?:^|[\s=])[\\/]|(?:^|[\\/])\.\.(?:[\\/]|$)', value))


def option_kind(definition):
    kind = definition.get('type')
    return _KINDS.get(kind, kind) if type(kind) is str else None


def safe_option_definitions(catalog):
    """Keep original metadata for fingerprints while sharing one safe allowlist."""
    if type(catalog) is not list:
        raise ValueError('invalid ODM catalog')
    selected = {}
    for definition in catalog:
        if type(definition) is not dict:
            raise ValueError('invalid ODM definition')
        name, kind = definition.get('name'), option_kind(definition)
        domain = _option_domain(dict(definition, type=kind))
        supported = kind in ('bool', 'int', 'float', 'enum', 'string') and domain is not None
        if kind in ('enum', 'string') and domain is not None and 'values' in domain:
            choices = domain['values']
            supported = supported and bool(choices) and len(choices) <= 1000 and all(_safe_text(value) for value in choices)
        if kind in ('string', 'enum'):
            description = json.dumps(definition.get('domain', ''), ensure_ascii=False) + ' ' + str(definition.get('help', ''))
            supported = supported and not (name in _FILE_OPTIONS or _UNSAFE.search(description))
        if (not p.valid_odm_option_name(name) or not supported
                or _UNSAFE.search(p.canonical_json(definition).decode('utf-8'))):
            continue
        if name in selected:
            raise ValueError('duplicate ODM definition')
        selected[name] = definition
    return selected


def advertised_option_definitions(catalog):
    """Project the public canonical types; fingerprint these exact definitions."""
    return {name: dict(definition, type=option_kind(definition))
            for name, definition in safe_option_definitions(catalog).items()}


def validate_option_values(values, definitions):
    """Validate exact scalar types and current domains before any mutation."""
    if type(values) is not dict:
        raise ValueError('ODM options must be an object')
    for name, value in values.items():
        definition = definitions.get(name)
        if definition is None:
            raise ValueError('ODM option is no longer available')
        kind = option_kind(definition)
        domain = _option_domain(dict(definition, type=kind))
        valid = False
        if kind == 'bool':
            valid = type(value) is bool
        elif kind == 'int':
            valid = type(value) is int and abs(value) <= p.MAX_SAFE_INTEGER
        elif kind == 'float':
            valid = type(value) in (int, float) and math.isfinite(value) and abs(value) <= p.MAX_SAFE_INTEGER
        elif kind == 'enum':
            valid = _safe_text(value) and domain is not None and value in domain.get('values', [])
        elif kind == 'string':
            valid = _safe_text(value)
        if domain is None or not valid or not _value_matches_domain(value, kind, domain):
            raise ValueError('ODM option value does not match the current engine definition')

