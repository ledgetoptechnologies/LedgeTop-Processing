# Draft v2 input transfer contract for joint review

**Status: PROPOSED, INACTIVE, NOT A WIRE AGREEMENT.** This document asks the
Viewer and Processing owners to agree on an input-only contract. It does not
change v1, grant access, establish deployment readiness, or settle result
handoff, cleanup, TreeDar, or engine-start authority. Values below are examples,
not approved production limits. The job queue may hold metadata on the
coordinator while source bytes remain on NAS-backed Viewer storage.

## Scope and immutable identity

Proposed transfer identity: `{protocolVersion:2, jobId, generation, environment,
sourceId, manifestSha256, capabilityFingerprint, transferEpoch}`. `jobId`,
`generation`, `environment`, source and manifest bind to an authenticated
coordinator claim and one immutable input manifest. `transferEpoch` is a
non-reused, durable random identifier issued once for this input transfer.
Capability fingerprint commits the negotiated broker limits. Neither a job ID
nor manifest hash is a credential; sources must authorize the requesting
worker and claimed job, including tenant/environment. Durable authority and
exclusive-worker fencing are prerequisites, not established by this document.

A proposed short-lived grant adds `{grantId, issuedAt, expiresAt, manifestSha256,
rangeBytes, totalRequestBudget, totalByteBudget, token}`. A compact manifest
commitment binds the complete permitted object set; do not copy tens of
thousands of object IDs into each grant. The broker must independently verify
that each requested ID belongs to that immutable manifest. The token is secret,
scoped to the exact transfer identity and committed object set; it never enters
logs or public receipts. The manifestâ€”not the grant aloneâ€”defines object size,
hash and type. Objects are read-only, immutable and hash-verified. The candidate canonical grant encoding and commitment domain below are normative
for this proposal; issuer authentication, key management and authorization remain
open. The encoding is not the v1 manifest encoding or a deployed protocol. A grant is not proof of disk reservation,
engine capacity or permission to delete data.

## Candidate canonical grant bytes and digest

This section is a precise proposal for cross-language review, not yet agreed.
The grant commitment is `SHA-256(domain || canonical_bytes)`, where `domain` is
exactly the UTF-8 bytes `ledgetop.reference.input-grant.v2` followed by one NUL
byte (`00`). The commitment payload is a closed JSON object with exactly:
`protocolVersion`, `jobId`, `generation`, `environment`, `sourceId`,
`manifestSha256`, `capabilityFingerprint`, `transferEpoch`, `grantId`,
`issuedAt`, `expiresAt`, `rangeBytes`, `totalRequestBudget`, and
`totalByteBudget`. The bearer token and commitment itself are not in these bytes;
issuer MAC/signature scheme, key identity and envelope remain open. An issuer
must bind the token and all payload bytes to the same authenticated grant.

`jobId` is a lowercase canonical UUID (`8-4-4-4-12` hex digits, matching
Processing `reference_protocol._uuid`); the other identifiers are nonempty safe
ASCII `[A-Za-z0-9][A-Za-z0-9_-]{0,127}`. SHA-256 strings are 64 lowercase hex characters; environment is exactly
`production` or `staging`. `protocolVersion` is integer 2; generation and
rangeBytes are positive integers and both budgets are nonnegative integers.
Every integer is at most 9007199254740991 (`2^53-1`); booleans, fractional or
exponent tokens, negative values and negative zero are invalid. Dates are real
UTC calendar instants in years 0001–9999 exactly `YYYY-MM-DDTHH:mm:ss.SSSZ`, and expiry must be
later than issuance. Reject UTF-8 BOM, duplicate or unknown keys, missing keys,
invalid Unicode, floats and extra whitespace; input must equal its canonical
re-encoding. Canonical bytes are compact UTF-8 JSON with object keys ordered by
ASCII code point, no spaces, ASCII-only payload strings and JSON string escapes
for quotes, backslashes and control characters. Exact canonical examples and a
hex digest are in the vector file. No float option or non-ASCII name contract
is implied by this grant format.

The source validates the authenticated grant and exact transfer identity before
looking up the requested object; membership is checked against the immutable
manifest for that source and generation. A source cannot grant an object from a
different job or source merely because its ID or hash matches. This compact
commitment avoids embedding all object IDs in every grant; broker storage,
manifest lookup and metadata limits still require measured qualification.

## Request, charging and range semantics

Proposed read request: an authenticated object GET with a new unpredictable
`requestId` and a single half-open byte range `[start,end)` where
`0 <= start < end <= size`, and `end-start <= rangeBytes`. For a zero-byte
object propose a distinct whole-object empty request, charged as one request and
zero bytes. For nonempty objects, encode `[start,end)` as HTTP `Range: bytes=start-(end-1)`;
require status 206, exact `Content-Range: bytes start-(end-1)/size`,
`Content-Length: end-start`, and identity encoding. The empty-object request
has no Range header and requires status 200, `Content-Length: 0`, and an empty
body. Exact header parsing, integer bounds and malformed/multiple ranges fail
closed. A successful response binds
object ID, exact range, total object size and bytes; redirects, content encoding,
truncation and incorrect range are errors. The worker verifies whole-object
size and SHA-256 before publishing a local input. Range verification alone does
not establish whole-object integrity.

Before opening or streaming any source bytes, the broker atomically validates
identity, expiry, object/range and remaining budget; durably inserts the unique
request ID and charges one request and the full intended response length. A
committed charge is never refunded solely because a response, stream, process,
network connection or acknowledgment is lost. A rejected ledger write emits no bytes. If commit outcome is unknown, hold the
request and send no bytes until durable reconciliation confirms its exact ID
and charge; the caller reconciles rather than assumes zero charge. Reconciliation alone
does not authorize sending the original body: without a separately specified
and proven emission protocol, any subsequent body requires a fresh request ID
and a new charge.
Reusing any charged request ID never emits a body, even when the original
body was never sent: an exact replay may return a bounded status/receipt, while
mismatched use is refused. A retry that
might stream bytes uses a fresh request ID and spends another charge. This rule
avoids a request-ID replay becoming an unmetered re-download. Atomicity and
crash recovery apply across all broker processes; direct-object routes that
bypass the ledger must be closed before activation.

The source maintains cumulative counters by transfer identity, independently of
individual grants and over restarts and rotations. Initial budgets include
first attempts plus an explicit bounded retry allowance; renewal must not reset
or silently increase them. Deny an attempt if request or byte charge exceeds
remaining budget, and retain prior charges if the worker abandons a partial
file. Concurrent request precharges serialize against the same cumulative
budget; each accepted request has its own unique ID, and only the number whose
charges fit may stream. A commit error is not evidence that no charge occurred. Worker scratch capacity and source broker budget are separate holds.

## Expiry, replacement and unknown outcomes

At request admission the broker compares against a durable nondecreasing clock
observation; persist an expiry observation before returning the denial, in a
transaction that is not rolled back by the denial. Clock rollback must not
revive an already observed expired grant.
Proposed initial policy rejects new request admission after expiry but does not
retroactively release a stream slot or undo a charge. Whether an already-admitted
stream may finish after expiry, its maximum duration, and how to recover its
concurrency slot after broker process death require a bounded durable in-flight
record, reconciliation and jointly agreed limits and tests. A restart must not
assume a still-running stream has stopped merely because its grant expired.
Never create a new transfer epoch merely because a broker or worker restarts.

Renewal requires a separately authenticated, fenced operation bound to the same
job, generation, source, environment, manifest, epoch and negotiated limits.
A replacement grant has a unique grant ID and expiry; it observes the durable
cumulative balances, and issuance or revocation cannot make an uncertain read
free. Whether prior grants are revoked atomically or overlap, and whether the
budget may ever be increased through a separate audited operation, are open
choices; implement neither until jointly specified. If a request response is
lost, a status lookup authenticated to the same worker and transfer identity may
report committed charge and outcome; it must compare the exact request ID and
identity and reject a changed ID, job, source, manifest, epoch or environment.
The status response is non-authorizing and
it cannot assert file delivery or release scratch. If status is unavailable,
the worker retains partial work and the spent-or-unknown budget and does not
assume a new request is free.

## Synthetic vectors

[`input-v2-transfer-vectors.json`](../contracts/input-v2-transfer-vectors.json)
contains numerical examples for ranges, empty input, cumulative retry charging,
expiry and clock rollback. These are contract-review examples, not an
implementation or interoperability result. The 16 MiB example follows the
current provisional planner and does not approve a production cap.

## Decisions and qualification still needed

1. Review and agree these proposed canonical identities and raw bytes, grant issuer and worker authentication,
   tenant/source isolation, and capability negotiation in both languages.
2. Agree per-range HTTP framing, zero-byte behavior, expiry-in-flight and
   concurrency-slot recovery; set measured object count, byte and request caps.
3. Agree audited renewal/revocation and an external epoch/restore fence;
   implement durable broker transactions with process-crash and rollback tests.
4. Test the actual Viewer broker with the Processing worker, including lost
   response, duplicate request ID, concurrent charges, expiry and restart;
   verify files and retain custody until explicit downstream acknowledgment.
5. Separately qualify NAS capacity/retention, exclusive worker and engine
   fencing, result handoff, and small then large end-to-end jobs. No v2 runtime
   or live client data is enabled by this proposal.
