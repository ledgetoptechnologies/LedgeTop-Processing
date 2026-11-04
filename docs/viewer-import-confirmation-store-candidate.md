# Inactive Viewer import confirmation store

This isolated Processing candidate is not a production route, a Viewer
contract, a trusted binding source, or permission to delete data. Both engine
and scratch cleanup holds remain in place. It accepts an **already allocated**
trusted job ID, generation and environment; it does not generate or predict a
Processing job ID. Admission, pre-submission Viewer task/attempt mapping and
activation after job binding remain separate work.

The caller must provide trusted application code that reads a coherent,
independently authenticated binding, accepted-copy envelopes, and public-key
pins from the same SQLite transaction. The loader is trusted code; the SQL
authorizer prevents accidental SQL writes or transaction control during its
call but does not sandbox arbitrary Python or establish data provenance. A
loader that copies expected values from the incoming confirmation invalidates
the security model. **No production trusted source currently supplies Viewer
model/version/output IDs or output/inventory digests independently**, so this
store cannot safely be wired yet.

The trusted loader must itself bound raw database values before materializing
or parsing them and reject invalid UTF-8 and duplicate JSON keys. The store's
bounded snapshot starts only after that loader has returned; it cannot limit
allocations already performed inside the loader.

`record` starts `BEGIN IMMEDIATE`, reads that trusted binding and current pins,
re-verifies the decoded confirmation and accepted-copy signatures, then
atomically records the exact signed canonical bytes and digest keyed by
job/generation/environment. A unique confirmation ID and a separately hashed
trusted job/copy binding prevent a changed proof or binding from overwriting the
first record. Identical replays re-verify against current pins. `load_verified`
also reloads trusted state and re-verifies stored bytes, refusing malformed or
oversized records. Added trusted pins do not themselves change the job binding;
removing the signing pin makes future reads/replays hold. Neither result type
is an authentication capability and caller-supplied verified-result objects are
not accepted as input.

The database must be in a private, exclusive service-owned directory. The
candidate requires POSIX `O_DIRECTORY`, `O_NOFOLLOW`, owner/mode checks, and a
successful verified parent-directory fsync; unsupported platforms, including
Windows, fail closed before creating a database. The newly created file is
fsynced; the parent is fsynced after creation, after SQLite commits, and after
connection close. SQLite uses a durable DELETE or WAL journal and
`synchronous=FULL`. A post-commit sync error is an uncertain outcome: the
caller must retry the exact proof, which re-verifies and re-syncs. These calls
request durability; they do not prove filesystem, hardware, or power-loss
behavior. Same-account malicious filesystem/SQLite writers are outside the
boundary. The immediate transaction includes bounded hashing/signature work,
so deployment needs lock-contention and large-input qualification.

Before decoding retained bytes, a bounded structural scan that does not build
a JSON tree checks the
existing verifier's 16 MiB raw limit, 500,000 value-node limit, depth 12,
10,000 array items and 64 object members, plus conservative escaped-string and
safe-integer token bounds. The stdlib decoder then checks JSON and duplicate
keys; the verifier applies its aggregate UTF-8/tree bounds after decoding. The
scanner bounds retained-proof parsing, not the trusted loader's own database
reads or any future HTTP body. A future network boundary must bound raw bytes
before JSON parsing and reject duplicate keys, as described in the verifier
proposal.

The current verifier is provisional and broader than the agreed first ODM-only
v1 scope: exactly one `odm_archive` object, with TreeDar-only/mixed results
rejected until their actual intake exists. The agreed proposed `archive` fields
are exactly `{objectId,byteSize,sha256,inventoryFormat,inventorySha256}` with
`inventoryFormat='viewer-processing-archive-evidence-v1'`. The inventory digest
is SHA-256 of UTF-8 JavaScript `JSON.stringify` over the path-sorted inventory,
where each `{relativePath,byteSize,sha256}` object's properties are inserted in
that order; it is not Python's generic sorted-key canonical JSON. Model/version/
output IDs and output/inventory digests are post-ingest signed Viewer assertions,
not values Processing can independently pre-know. Processing must independently
bind job/generation/environment/input/pipeline/result, prebound Viewer task and
attempt, pinned issuer, signed accepted-copy closure, and archive object ID,
size and SHA against its own durable result manifest. A separately versioned
verifier and shared vectors must be agreed before integration; this store still
uses the older provisional verifier and must not be enabled.

Synthetic tests exercise first write, exact replay after restart, simultaneous
identical and conflicting proofs, process death before commit, lost response
after commit, malformed or changed durable bytes, changed bindings, key
rotation/revocation, reused confirmation ID and denial of loader SQL writes.
They use synthetic signing keys and a test-only binding table; they do not
prove deployment, Viewer ingestion, or permission to delete worker results.
