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
logs or public receipts. The manifest—not the grant alone—defines object size,
hash and type. Objects are read-only, immutable and hash-verified. There must
be a documented, interoperable canonical encoding and signature/MAC domain,
but this draft does not choose one. A grant is not proof of disk reservation,
engine capacity or permission to delete data.

## Request, charging and range semantics

Proposed read request: an authenticated object GET with a new unpredictable
`requestId` and a single half-open byte range `[start,end)` where
`0 <= start < end <= size`, and `end-start <= rangeBytes`. For a zero-byte
object propose a distinct whole-object empty request, charged as one request and
zero bytes. The final HTTP representation, exact header encoding and zero-byte
response must be agreed before implementation. A successful response binds
object ID, exact range, total object size and bytes; redirects, content encoding,
truncation and incorrect range are errors. The worker verifies whole-object
size and SHA-256 before publishing a local input. Range verification alone does
not establish whole-object integrity.

Before opening or streaming any source bytes, the broker atomically validates
identity, expiry, object/range and remaining budget; durably inserts the unique
request ID and charges one request and the full intended response length. A
committed charge is never refunded solely because a response, stream, process,
network connection or acknowledgment is lost. A rejected or uncertain ledger
write emits no bytes; the caller reconciles rather than assumes zero charge.
Reusing a request ID must not emit a second response body: an exact replay may
return a bounded status/receipt, while mismatched use is refused. A retry that
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
budget. Worker scratch capacity and source broker budget are separate holds.

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
lost, a non-authorizing status lookup may report committed charge and outcome;
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

1. Agree identities, signed raw framing, grant issuer and worker authentication,
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
