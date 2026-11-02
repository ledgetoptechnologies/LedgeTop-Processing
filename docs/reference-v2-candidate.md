# Reference outer-v2 candidate review package

This package is **unagreed, incompatible with v1, and inactive**. It contains
review data and a deterministic artifact generator, with no production parser,
validator, adapter, issuer, factory, migration, capability advertisement or runtime
configuration. Existing v1 files and contracts remain unchanged.

The exact inputs are Viewer proposal v3, SHA
`23918d2263a91ca6c91501fd35bdf52fb60826c2ab6c33e06596b37688671be5`,
and metadata addendum v1, SHA
`502491feee2f8e52df49e87ce51e0f26a2fa3c2e0b6ed9431b8423f794358242`.
Pinned copies are in `contracts/reference-v2-candidate/sources/`. The generator
asserts exact field coverage for all 31 outer and eight addendum record types.

## Contents and limits

`candidate.json` is the package index, technical choices, review questions and
remaining gates. `contract.schema.json` provides closed JSON Schema 2020-12-style
record definitions with all fields required, explicit nullable values, exact
discriminants and concrete scalar bounds. The root union lists proposed messages;
private/support records remain available through `$defs`.

`semantic-rules.json` supplies required lexical, cross-record, authentication,
budget and lifecycle rules that JSON Schema cannot enforce by itself.
`hash-domains.json` distinguishes exact physical bytes, named structured
self-digests and opaque authenticated references. The general pipeline digest
remains server-owned and opaque; there is no new cross-language algorithm for
dynamic option floats. Structured metadata digest domains preserve nested hashes
and omit only their designated own top-level digest field. A proposed products
digest covers the original stage-ordered `{stageId,engine,requestedProducts}`
array, which contains no options.

Whole messages are capped at 4 MiB, arrays at 10,000 members, integers at the
JavaScript safe maximum, stages at two and lexical depth at a proposed 32.
Actual archive/raw/expansion/retained-copy and metadata history policies remain
explicit per-admission positive limits. The fixtures give finite invented test
values, not approved deployment limits. TreeDar retains its six separate limits,
64-file ceiling and 1 MiB manifest ceiling. Physical sources/artifacts remain
positive; zero expanded EPT/archive entries need explicit semantic justification.

Schema integer typing alone cannot distinguish `1` from `1.0` or `1e0`. A future
strict decoder must enforce integer token grammar, UTF-8, scalar Unicode,
duplicate/escaped duplicate keys, unsafe keys, negative zero, finite values,
trailing bytes and depth before schema validation. Timestamp format annotations
also require real date parsing and canonical round-trip checks.

## Golden fixtures

The `valid/` directory contains complete ODM-only, TreeDar-only and combined
workflows, plus ODM pointcloud-only and optional-unavailable-product variants.
Each workflow carries original source bytes, actual deterministic synthetic ZIP
bytes and hashes, closure descriptors, producer custody/result records, distinct
NAS accepted-copy records, execution settlement, heavy capacity release, parent
delivery completion and producer cleanup snapshots. The support-record fixture
adds a denied metadata round, finite control policy/journal, continuation and
structural EPT example.

These are **structural/hash/relationship goldens under an explicitly invented
expected context**. They are not genuine acquisition, engine execution,
geospatial/product validation, authenticated issuer proof, NAS ingestion or
publication. No MAC, signature, real credential or fabricated authentication
proof is generated. Claimed genuine flags and receipt IDs inside synthetic
records require real independent verification before they could authorize
anything. The synthetic source and native bodies intentionally make no LAS,
COG, CRS, EPT, tree detection or other scientific qualification claim.

Product targets reference validated members/closures of one ODM ZIP; there is no
mandatory duplicate standalone orthophoto/pointcloud/report artifact. Preupload
worker product evidence and producer custody IDs differ from later NAS accepted
copy validation/custody IDs. Optional unavailable products remain unavailable.
TreeDar bundles retain the existing closed inner bundle contract and its native
vocabulary without adding outer claims to it.

The outage snapshot separates safely settled heavy execution from pending
delivery: heavy capacity may release only after all requested engines actually
settle, while original custody, disk/control reservations and delivery outbox
remain held. A combined ODM intermediate alone is not parent execution settled.

`invalid/index.json` names concrete expected-rejection inputs and their trusted
context/rule. Raw cases cover duplicate/escaped keys, malformed UTF-8, BOM,
surrogates, bool/float/exponent/unsafe integers, negative zero, unsafe keys,
trailing data and excess depth. Structured cases cover roles, parent claims,
original custody, partial identity/membership, archive member closure, premature
settlement, cleanup and metadata actor/purpose/expiry/ledger drift. Actual small
ZIP negatives cover traversal, case collision, unlisted members, symlinks,
encryption flags and empty product containers. Expected rejection labels are
test specifications, not claims that an unimplemented validator was run.

## Source authority versus historical execution

Fresh metadata-only authorization, current parent permission and independently
checked revocation/custody are required immediately before a new heavy stage.
The original payload grant may remain expired; its immutable identities and
cumulative byte ledger are not reset. Stable operation/round/intent and finite
control budgets govern retries and authoritative lookup of unknown outcomes.

Once an admitted stage starts, its sealed at-start metadata authorization and
original acquisition/custody identities remain historical evidence. Metadata TTL
expiry alone does not invalidate truthful output, rewrite provenance or strand
native byte validation behind the expired payload token. Current cancel/revoke
policy remains separate. Publication/catalog mutation requires its own current
authority and accepted product closure; historical admission alone cannot bypass
a present revocation. During source/Viewer outage, retain results and outboxes
without inventing a claim, receipt freshness, acceptance or cleanup authority.

## Paired decisions and implementation gates

The following technical candidate choices require owner acceptance: depth/path
bounds, the requested-products digest domain, the producer `retainUntil` spelling,
and the typed ODM archive closure sidecar. The sidecar appears only in fixture
context; actual embedding/fetch transport and semantic validation are unresolved.
The old compact `SourceMetadataReauthorization` definition is retained solely for
source-field coverage, annotated superseded and excluded from the root authority
message union. The full addendum receipt supplies the proposed authority shape;
owners should remove or explicitly pair any future derived summary view.

No endpoint, authentication mechanism, issuer/purge-pin enforcement, deployment
capacity policy, execution/delivery factory, product semantic validator, current
publication/ACK/cleanup API or v1 migration/rollback is selected by these files.
All such gates are listed in the package index. Gaussian splats remain false.

Run `python -B contracts/reference-v2-candidate/generate.py` to reproduce review
artifacts using only the vendored, exact-SHA-checked `sources/` files. The generator
does not read a developer's artifact directory or rewrite those source copies.
The package-local `.gitattributes` uses `* -text` to preserve every fixture and
vendored source byte across Windows/Linux Git checkouts, while retaining normal
text diffs. `inventory.json` records exact
file bytes and a review inventory digest; it is **not** a negotiated capability
or contract fingerprint. Artifact QA checks may verify deterministic generation,
field/ref coverage, self-digests and synthetic ZIP members. They do not replace
paired parser/semantic/security/authority conformance or real engine tests.

Offline structural evaluation used the existing MCP connector environment's
`jsonschema 4.26.0` Draft 2020-12 evaluator with remote retrieval disabled.
`check_schema`, all 58 typed golden records and the 10 schema-phase negative
vectors passed that scoped evaluation. This did not execute lexical, hash,
cross-record, issuer/actor authority, archive or scientific conformance checks;
the separately described artifact/codec QA has its own narrower scope.

## Paired Viewer fixture inspection (November 1, 2026)

Viewer added a test-only exact-vector inspector in commit
`54556f9a34b330bd21c93813441c5c67385aeae2` on
`codex/viewer-v2-delivery-ledger`. Its bounded one-CPU container run inspected
the pinned `treedar_only.json` and `combined.json` vectors from Processing
commit `15d053ac976da6aabc6e3759f40620778bad8f6b`; Viewer reported that the
Processing candidate subtree was byte-unchanged before and after. The check
verified those fixtures' bundle hashes, listed member closure, native vocabulary
and selected source/result bindings, rejected byte tampering, accepted the v1
positive fixture and confirmed that the unchanged closed v1 validator rejects
the v2 TreeDar record. All successful candidate results remain `hold`.

This is paired evidence for those exact public synthetic fixtures only. It is
not general v2 schema or lexical conformance, arbitrary untrusted ZIP safety,
genuine LAS provenance, authenticated custody, Viewer acceptance/ACK/outbox,
capability negotiation or runtime qualification. Viewer production remains on
the existing v1 bridge until those separate gates are paired and implemented.
