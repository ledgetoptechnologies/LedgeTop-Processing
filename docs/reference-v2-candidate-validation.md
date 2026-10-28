# Inactive candidate lexical, structural and named-digest layer

`processing.reference_v2_candidate` implements the lexical, used-schema and named
metadata self-digest layer for the exact frozen outer-v2 candidate schema SHA
`61467d589c36efae9b130fc179a454868ea1242a96918d8143ca949c79ed933c`.
The module is inactive and uses Python's standard library only. No v1 code,
request route, worker/issuer factory, deployment or advertised capability changes.

## API and result scope

```python
record = validate_candidate(raw_bytes, record_type="ResultManifest")
```

`raw_bytes` must be immutable bytes. The type must name one of the 60 pinned
definitions; there is no caller-supplied schema, path, URL, trust flag or option
to skip digest checking. The only filesystem reads are bounded fixed local
candidate resources whose exact raw SHA is verified. All references are local.
The compiled schema and rule set are cached after verified loading. Hash pinning
remains mandatory on Windows, where stat/fstat ctime meanings can differ.

Success returns an immutable `StructuralCandidateRecord` with status
`UNAGREED_INACTIVE_STRUCTURAL_ONLY`, exact raw bytes, verified named digest paths
and explicit unimplemented gates. `value` returns a fresh decoded copy; modifying
it does not change the checked record. Raw contents are hidden from repr and
errors expose only bounded reason codes and schema-owned/dynamic-placeholder
paths. Boolean conversion raises, so the object cannot act as a true/false
acceptance or authority flag.

This is not an admission, authenticated issuer receipt, semantic product check,
scientific qualification, physical-file verifier, Viewer acceptance or permission
to publish or clean up data. A structurally correct self-declared custody record
still has an unimplemented authority gate. No caller boolean can supply it.

## Implemented checks

A lexical scan precedes `json.loads`, bounding the 4 MiB raw message, 32 container
levels and 10,000 members per array. It also enforces a 256-member object ceiling:
this is derived from the largest object allowed anywhere in the pinned schema,
not a new permissive fallback. Oversized/deep/wide inputs are refused before
materializing their JSON tree. Strings containing brackets or escaped quotes do
not consume container depth. UTF-8/BOM, duplicate and escaped duplicate keys,
unpaired surrogates, unsafe keys, trailing input, nonfinite values and negative
zero are rejected. Integer tokens must remain in the JavaScript safe range.

JSON float/exponent tokens retain a distinct internal type. Thus `1.0` and `1e0`
cannot satisfy integer fields or numeric version constants, while finite floats
are permitted by the candidate's actual `JsonValue`/`Options` number schemas.
Negative safe integers are likewise permitted only where that schema admits
them. Options are still subject to later server catalog/admission checks.
The metadata canonicalizer rejects floating-point values and is not a public
algorithm for deriving the opaque server-owned pipeline digest.

The evaluator implements only the vocabulary actually used by this pinned
schema: local references, scalar types, constants/enums, closed properties,
pattern/additional properties, required fields, array bounds/JSON equality
uniqueness, string bounds/patterns, numeric ranges, date-time round trips,
oneOf/anyOf/allOf/not and if/then. Booleans remain distinct from numbers;
`uniqueItems` treats numeric 1 and 1.0 as equal where options allow both.
Unknown keywords, reference forms, formats and unused schema forms fail closed.
This is not a generic JSON Schema implementation or a promise of future-draft
compatibility. The current schema's conditional roles, nullable fields, requested
modes and engine-version shape are enforced.

All nine named self-digest domains are checked, including nested applicable
records such as `Submission.inputManifest`. The restricted canonicalizer matches
the existing Node metadata codec: UTF-8, ASCII-key ordering, array order preserved,
standard string escaping and safe nonnegative integer metadata. Only the
designated own top-level digest field is omitted; child physical hashes remain
in the digest. Opaque pipeline/catalog/actor/policy/receipt references are not
invented or derived. Physical artifact hashes, actual archive members and the
cross-record requested-products digest need their separate future gates.

## Coverage and tests

`coverage_report()` classifies the pinned 57 negative vectors. Its 25 lexical or
schema cases are applicable to this layer. The other 32 remain
`UNIMPLEMENTED_GATE`: cross-record binding/membership, archive safety/semantics,
issuer/actor/custody/revocation, capacity, execution settlement, delivery,
publication and cleanup. Some such inputs may be rejected earlier by structure;
that does not establish semantic conformance or close the original full goal.

Tests exercise all 58 typed goldens, all 60 definitions, all applicable negative
vectors, every named digest domain, canonical vectors, integer/float distinctions,
Unicode, resource boundaries, closed/conditional forms and explicit deferred
authority. The optional test oracle uses an already installed Draft 2020-12
`jsonschema` library with remote retrieval disabled. It is never a runtime
dependency and nothing is installed. Linux's ordinary cached image skips only
that optional oracle; a separate existing Windows oracle environment can run
the whole same suite.

Run `python -B -W error::ResourceWarning -m unittest
tests.test_reference_v2_candidate`. Final test counts and source pins belong in
the qualification record; no production-ready or full 57-vector semantic
acceptance claim is made by this component.
