"""Unwired direct-transfer profiles; never alters shared V2 limits."""
from . import reference_protocol as p

PROFILE = 'ltds-direct-10k-each-v1'
MAX_SOURCE = 10_000
MAX_DERIVED = 10_000
MAX_MANIFEST_BYTES = 16 * 1024 * 1024


def counts(profile, source, derived):
    p._integer(source, 'source count')
    p._integer(derived, 'derived count', allow_zero=True)
    if profile is None:
        if source + derived > p.MAX_OBJECTS:
            raise ValueError('legacy direct count exceeded')
    elif profile == PROFILE:
        if source > MAX_SOURCE or derived > MAX_DERIVED:
            raise ValueError('direct profile count exceeded')
    else:
        raise ValueError('unknown direct profile')


def manifest(profile, value, source, derived):
    counts(profile, source, derived)
    bound = MAX_MANIFEST_BYTES if profile == PROFILE else p.MAX_BODY_BYTES
    if len(p.canonical_json(value)) > bound:
        raise ValueError('direct manifest metadata limit exceeded')
