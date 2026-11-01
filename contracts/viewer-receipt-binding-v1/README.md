# Candidate Viewer receipt binding wire profile

This is a review-only **new profile**, not the existing `ltds-reference-v1`
endpoint, and is not enabled or imported into runtime. The Python and JavaScript
codecs test only wire encoding. They do not authenticate a principal, validate
Viewer custody, verify a claimed attempt binding, or authorize cleanup.

The canonical UTF-8 outer object is closed and contains `schemaVersion: 1`,
lowercase canonical UUIDv4 strings `bindingId` and `attemptId`, a lowercase
64-character `receiptSha256`, and `receiptBase64url`: unpadded canonical
base64url of the canonical UTF-8 **unchanged v1 receipt object**. The declared
digest must equal SHA-256 of those decoded bytes. The draft bounds are 4 MiB
for the encoded outer object and 2 MiB for the decoded receipt; these require
agreement with Viewer and do not alter any existing protocol limit.
The Processing v1 validator permits at most 10,000 unique ASCII object IDs of
up to 128 bytes, three further IDs of at most 128 bytes, a safe integer
generation, fixed digest fields and a timestamp of at most 40 Unicode
characters. Allowing a conservative four UTF-8 bytes per timestamp character,
the current closed receipt's syntactic upper bound is 1,311,009 decoded bytes,
or 1,748,238 bytes in this fixed-length-ID envelope. Recalculate if fields,
validation version or limits change. A 10,000-object valid-shaped vector is
included in the test; the bound intentionally overestimates the timestamp.

Canonical JSON uses sorted ASCII object keys, compact separators, literal UTF-8
Unicode scalar values and nonnegative safe integers. It rejects whitespace,
noncanonical escaping, duplicate or escaped-duplicate keys, lone surrogates,
invalid UTF-8, -0, floats/exponents, out-of-range integers and noncanonical
base64url. Array order is significant. V1 receipt semantic validation, exact
manifest comparison and authorization are separately required after decoding.
Each JSON document is limited to 50,000 value nodes and depth 8, counting the
root as depth 0. The outer and decoded documents have separate budgets. This is
a candidate profile restriction, not an existing v1 parser guarantee.
The codecs receive an already-buffered outer document and allocate parsed JSON
before checking its depth and node count. Before any endpoint is enabled, its
listener must enforce the outer byte limit while reading (including chunked
bodies), bound concurrent requests and reserve enough memory for the encoded
body, decoded receipt and both parsed representations. Deadlines and safe
failure under memory pressure require separate qualification. These codecs do
not provide streaming admission or bound aggregate process memory.
The explicit digest establishes byte consistency only; an authenticated
request/binding must cover the profile version, attempt, binding and computed
digest. A TLS bearer alone is not a durable signature or proof of custody.

Viewer's existing insertion-ordered v1 JSON must not be reinterpreted after an
uncertain send. A new adapter must durably persist its own canonical snapshot
before the first new-profile send, then retry that exact snapshot and identity.
Old uncertain v1 receipts are held for explicit migration, not silently rebound.
`golden.json` is derived from the existing v1 fixture and can be independently
checked by both languages. `python -m unittest tests.test_candidate_receipt_wire`
runs Python and Node conformance checks and separate v1 semantic validation.
On the authoring Windows host the combined candidate and v1 protocol suite ran
17 tests successfully. Ubuntu/WSL has no Node binary, so its cross-language
suite could not complete there; no packages were installed.

Before an endpoint exists, the participating teams must qualify server-owned,
non-reusable principal and credential revision provisioning, atomic revocation
and acceptance, replay after rotation, anti-rollback, attempt/job registration,
nonempty Viewer migration and durable recipient custody. No client-supplied
principal, revision, claimed receipt, hash, or opaque proof ID establishes any
of those properties. Cleanup remains prohibited until independently qualified.
