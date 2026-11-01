# Native ODM input manifest: validator candidate

This is an unwired validator and a provisional cross-language vector set. It
does not add an endpoint, authenticate an operator, accept uploads, authorize
dispatch, allocate or reserve storage, prove source bytes, or establish result
custody. Do not enable a runtime based on validation alone.

The closed manifest shape and SHA-256 digest reuse reference-v1: hash canonical
UTF-8 sorted-key compact JSON excluding top-level `sha256`. The received full
JSON must itself be canonical, with no duplicate keys, noncanonical numeric
forms, whitespace or alternate escaping. This profile admits only printable
ASCII identifiers/names/media types under both reference and native filename
rules; integers are positive safe integers except the explicit zero counts.

The provisional jointly discussed mapping uses an opaque Viewer-attempt slug as
`id`, fixed revision `1`, and `o000001`, `o000002`, ... in exact transmitted
order, with photos first and then auxiliary files (including generated GCP).
Each object includes reference-v1 `id`, `name`, `byteSize`, `sha256`, `kind`
and `mediaType`. Only native ODM image/auxiliary suffix and media combinations
are allowed; basename duplicates are case-insensitive. The caller supplies the
expected opaque manifest ID, which is not authenticated by this helper. Any
later runtime must separately bind environment, coordinator authority, task
UUID, coordinator-issued generation and the task capability, and must define
durable exact replay, conflict, cancellation, retention, old-writer exclusion,
and rollback semantics before accepting bytes.

This candidate preserves the reference maximum of 10,000 **total** objects,
the 4 MiB metadata body limit and a 20 GiB maximum total input ceiling. A
10,000-photo set plus generated GCP is 10,001 objects and is rejected; a
roughly 50 GB input is also rejected. Tests at these boundaries use metadata
only and allocate no corresponding data. The two vectors contain synthetic
bytes and do not claim valid photos or GCPs. Their canonical strings and SHA-256
digests are shared; the JSON file's Windows checkout may use CRLF while its Git
blob uses LF, so compare exact blob bytes or the canonical vector fields.

The separate rejection fixture covers UTF-8 BOM and invalid bytes, duplicate keys, alternate number lexemes,
zero-sized objects, unsupported names, a changed digest, count mismatch,
duplicate basenames and a generated 10,000-photo-plus-GCP case. The Node test
independently implements a test-only candidate checker for these fixtures; it
is not the Viewer owner's validator or an implementation of physical admission.
Cross-repository agreement and independent runtime work are
still needed for the versioned binding, pre-body storage allocation and spool
handling, old-writer fence, and crash recovery.
